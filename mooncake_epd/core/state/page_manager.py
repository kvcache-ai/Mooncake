"""Paged KV manager: block-level allocation, reference counting, copy-on-write.

The manager owns a pool of physical KV pages. Each page holds a fixed
number of tokens' worth of key/value states for every transformer layer.
Logical ``MultimodalState`` instances reference pages through ``BlockRef``
pointers; forking a state increments reference counts in O(num_pages) and
never copies data. Write-on-copy is triggered only on the single page that
a decode step is about to mutate, reducing fork cost from O(seq_len) to
O(page_size).

The tensor layout of a page is:

    key:   (num_layers, num_kv_heads, page_size, head_dim)
    value: (num_layers, num_kv_heads, page_size, head_dim)

which mirrors the HuggingFace ``past_key_values`` tuple (after stacking
across layers) sliced along the sequence dimension. This lets the LLM
consume reassembled pages without any format conversion.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple, Union

import torch

from ..control import LocalKVDirectory


PageId = int


def _normalize_kv_cache(past_key_values):
    """Convert an HF ``past_key_values`` (DynamicCache or tuple/list of
    (key, value)) into ``[(key_tensor, value_tensor), ...]`` per layer.
    """
    # DynamicCache path (transformers >= 4.47). Prefer the legacy form
    # because its layout is stable across HF versions.
    if hasattr(past_key_values, "to_legacy_cache"):
        legacy = past_key_values.to_legacy_cache()
        if legacy is not None:
            return [(k, v) for k, v in legacy]
    # Iterable of per-layer entries. Some HF versions yield (k, v);
    # newer ones yield (k, v, sliding_flag). Accept both.
    out = []
    try:
        for layer in past_key_values:
            if isinstance(layer, (tuple, list)):
                out.append((layer[0], layer[1]))
            elif hasattr(layer, "keys") and hasattr(layer, "values"):
                out.append((layer.keys, layer.values))
            else:
                raise TypeError(f"unsupported layer type: {type(layer)}")
    except TypeError:
        return []
    return out

@dataclass
class BlockRef:
    """Logical reference to a physical KV page plus the slot occupancy.

    ``filled`` is the number of valid token slots in the page (0 < filled
    <= page_size). Partially-filled pages only exist at the tail of a
    sequence; all earlier pages have ``filled == page_size``.
    """

    physical_id: PageId
    filled: int
    global_block_id: str = ""
    physical_node_id: str = "local"
    logical_index: int = 0
    virtual_offset: int = 0


@dataclass
class _PhysicalPage:
    key: torch.Tensor        # (num_layers, num_kv_heads, page_size, head_dim)
    value: torch.Tensor      # (num_layers, num_kv_heads, page_size, head_dim)
    refcount: int = 1
    created_at: float = field(default_factory=time.monotonic)
    device: torch.device = torch.device("cpu")


class PagedKVManager:
    """Page-granularity KV allocator with reference counting and CoW.

    The manager is thread-safe: all mutations go through a re-entrant lock
    so that concurrent fork / release / cow_page calls stay consistent.
    """

    def __init__(
        self,
        page_size: int = 16,
        num_layers: int = 36,
        num_kv_heads: int = 8,
        head_dim: int = 128,
        dtype: torch.dtype = torch.bfloat16,
        device: torch.device = torch.device("cpu"),
        num_lock_shards: int = 16,
        kv_directory: Optional[LocalKVDirectory] = None,
        node_id: Optional[str] = None,
    ):
        self.page_size = page_size
        self.num_layers = num_layers
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.dtype = dtype
        self.device = device
        self.node_id = str(node_id or device)
        self.kv_directory = kv_directory or LocalKVDirectory(node_id=self.node_id)

        self._pages: Dict[PageId, _PhysicalPage] = {}
        self._next_id: PageId = 0
        # Sharded locks: pid % num_shards -> lock. This spreads contention
        # across independent shards for high-throughput CoW / refcount ops.
        self._num_shards = num_lock_shards
        self._shards: List[threading.RLock] = [
            threading.RLock() for _ in range(num_lock_shards)
        ]
        self._global_lock = threading.RLock()  # for allocation / resize

    def _shard_for(self, pid: PageId) -> threading.RLock:
        return self._shards[pid % self._num_shards]

    # ------------------------------------------------------------------
    # Allocation
    # ------------------------------------------------------------------
    def _register_new_page(
        self,
        key: torch.Tensor,
        value: torch.Tensor,
        filled: int = 0,
        workflow_id: str = "",
    ) -> BlockRef:
        with self._global_lock:
            pid = self._next_id
            self._next_id += 1
            self._pages[pid] = _PhysicalPage(
                key=key,
                value=value,
                device=self.device,
            )
        gid = self.kv_directory.register_block(
            local_pid=pid,
            workflow_id=workflow_id,
            node_id=self.node_id,
        )
        return BlockRef(
            physical_id=pid,
            filled=max(0, min(filled, self.page_size)),
            global_block_id=gid,
            physical_node_id=self.node_id,
        )

    def allocate_page(self, filled: int = 0) -> BlockRef:
        """Allocate an empty physical page and return its BlockRef."""
        shape = (self.num_layers, self.num_kv_heads, self.page_size, self.head_dim)
        key = torch.zeros(shape, dtype=self.dtype, device=self.device)
        value = torch.zeros(shape, dtype=self.dtype, device=self.device)
        return self._register_new_page(key=key, value=value, filled=filled)

    def allocate_pages(self, count: int, filled: int = 0) -> List[BlockRef]:
        return [self.allocate_page(filled=filled) for _ in range(count)]

    # ------------------------------------------------------------------
    # Read / write helpers
    # ------------------------------------------------------------------
    def get_page(self, ref: BlockRef) -> Tuple[torch.Tensor, torch.Tensor]:
        with self._shard_for(ref.physical_id):
            page = self._pages[ref.physical_id]
        return page.key, page.value

    def get_page_slice(self, ref: BlockRef) -> Tuple[torch.Tensor, torch.Tensor]:
        key, value = self.get_page(ref)
        start = max(0, int(ref.virtual_offset))
        stop = min(start + int(ref.filled), key.shape[-2])
        return key[:, :, start:stop, :], value[:, :, start:stop, :]

    def write_page_slots(
        self,
        ref: BlockRef,
        key_chunk: torch.Tensor,
        value_chunk: torch.Tensor,
        offset: int = 0,
    ) -> BlockRef:
        """Write ``key_chunk/value_chunk`` into slots ``[offset, offset+n)``.

        If the page is shared (refcount > 1) the caller must first call
        ``cow_page``; writing into a shared page raises ``RuntimeError``.
        """
        n = key_chunk.shape[-2]
        if n > self.page_size - offset:
            raise ValueError(
                f"chunk length {n} exceeds free slots {self.page_size - offset}"
            )
        with self._shard_for(ref.physical_id):
            page = self._pages[ref.physical_id]
            if page.refcount > 1:
                raise RuntimeError(
                    f"page {ref.physical_id} has refcount={page.refcount}; "
                    "cow_page() required before mutation"
                )
            page.key[:, :, offset : offset + n, :].copy_(key_chunk)
            page.value[:, :, offset : offset + n, :].copy_(value_chunk)
        new_filled = max(ref.filled, offset + n)
        return BlockRef(
            physical_id=ref.physical_id,
            filled=new_filled,
            global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
            physical_node_id=ref.physical_node_id,
            logical_index=ref.logical_index,
            virtual_offset=ref.virtual_offset,
        )

    def write_page_layer_slots(
        self,
        ref: BlockRef,
        key_chunk: torch.Tensor,
        value_chunk: torch.Tensor,
        *,
        layer_start: int,
        offset: int = 0,
    ) -> BlockRef:
        """Write a contiguous layer group into a page.

        ``key_chunk`` / ``value_chunk`` are shaped
        ``(num_group_layers, num_kv_heads, n_tokens, head_dim)``.
        """
        n = key_chunk.shape[-2]
        if n > self.page_size - offset:
            raise ValueError(
                f"chunk length {n} exceeds free slots {self.page_size - offset}"
            )
        layer_stop = layer_start + key_chunk.shape[0]
        if layer_start < 0 or layer_stop > self.num_layers:
            raise ValueError(
                f"layer range [{layer_start}, {layer_stop}) exceeds num_layers={self.num_layers}"
            )
        with self._shard_for(ref.physical_id):
            page = self._pages[ref.physical_id]
            if page.refcount > 1:
                raise RuntimeError(
                    f"page {ref.physical_id} has refcount={page.refcount}; "
                    "cow_page() required before mutation"
                )
            page.key[layer_start:layer_stop, :, offset : offset + n, :].copy_(key_chunk)
            page.value[layer_start:layer_stop, :, offset : offset + n, :].copy_(value_chunk)
        new_filled = max(ref.filled, offset + n)
        return BlockRef(
            physical_id=ref.physical_id,
            filled=new_filled,
            global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
            physical_node_id=ref.physical_node_id,
            logical_index=ref.logical_index,
            virtual_offset=ref.virtual_offset,
        )

    # ------------------------------------------------------------------
    # Reference counting
    # ------------------------------------------------------------------
    def _normalize_block_identifier(self, block: Union[PageId, str]) -> Tuple[PageId, str]:
        if isinstance(block, str):
            gid = block
            pid = self.kv_directory.resolve_physical_id(gid)
            if pid is None:
                raise KeyError(f"unknown global block id: {gid}")
            return pid, gid
        pid = int(block)
        gid = self.kv_directory.gid_for_physical_id(pid)
        if gid is None:
            raise KeyError(f"unknown physical page id: {pid}")
        return pid, gid

    def incref(self, pid: Union[PageId, str]) -> None:
        physical_id, gid = self._normalize_block_identifier(pid)
        new_refcount = self.kv_directory.incref(gid)
        with self._shard_for(physical_id):
            self._pages[physical_id].refcount = new_refcount

    def decref(self, pid: Union[PageId, str]) -> bool:
        """Decrement reference count; return True if the page was freed."""
        physical_id, gid = self._normalize_block_identifier(pid)
        new_refcount = self.kv_directory.decref(gid)
        with self._shard_for(physical_id):
            page = self._pages.get(physical_id)
            if page is None:
                return False
            page.refcount = new_refcount
            if new_refcount <= 0:
                del self._pages[physical_id]
                self.kv_directory.release_physical(gid)
                return True
            return False

    def refcount(self, pid: Union[PageId, str]) -> int:
        try:
            _, gid = self._normalize_block_identifier(pid)
        except KeyError:
            return 0
        return self.kv_directory.refcount(gid)

    def release_refs(self, refs: Sequence[BlockRef]) -> int:
        """Release a sequence of block refs; return number of pages freed."""
        freed = 0
        for ref in refs:
            if self.decref(ref.physical_id):
                freed += 1
        return freed

    # ------------------------------------------------------------------
    # Copy-on-write
    # ------------------------------------------------------------------
    def cow_page(self, ref: BlockRef) -> BlockRef:
        """Clone a shared page into a private one (refcount=1).

        If the source page is already private (refcount == 1), returns the
        same ref unchanged -- no copy is performed.

        Only the filled slice ``[:,:,:filled,:]`` is cloned; the remaining
        (uninitialized) slots are zero-filled on the new page. This cuts
        copy cost linearly with ``filled / page_size``.
        """
        with self._shard_for(ref.physical_id):
            src = self._pages[ref.physical_id]
            if src.refcount == 1:
                return ref
            # Slice-only copy: only clone the actually-filled slots.
            f = ref.filled
            if f < self.page_size:
                new_key_slice = src.key[:, :, :f, :].clone()
                new_val_slice = src.value[:, :, :f, :].clone()
                new_key = torch.zeros_like(src.key)
                new_value = torch.zeros_like(src.value)
                new_key[:, :, :f, :].copy_(new_key_slice)
                new_value[:, :, :f, :].copy_(new_val_slice)
            else:
                new_key = src.key.clone()
                new_value = src.value.clone()
        self.decref(ref.global_block_id or ref.physical_id)
        new_ref = self._register_new_page(
            key=new_key,
            value=new_value,
            filled=ref.filled,
        )
        return BlockRef(
            physical_id=new_ref.physical_id,
            filled=ref.filled,
            global_block_id=new_ref.global_block_id,
            physical_node_id=self.node_id,
            logical_index=ref.logical_index,
            virtual_offset=ref.virtual_offset,
        )

    # ------------------------------------------------------------------
    # Bulk operations used by fork / advance_step
    # ------------------------------------------------------------------
    def fork_refs(self, refs: Sequence[BlockRef]) -> List[BlockRef]:
        """Fork a sequence of block refs: incref each physical page.

        Uses per-page sharded locks so independent pages can be incref'd
        in parallel.
        """
        out: List[BlockRef] = []
        for ref in refs:
            self.incref(ref.global_block_id or ref.physical_id)
            out.append(BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or self.kv_directory.gid_for_physical_id(ref.physical_id) or f"local:{ref.physical_id}",
                physical_node_id=ref.physical_node_id,
                logical_index=ref.logical_index,
                virtual_offset=ref.virtual_offset,
            ))
        return out

    def reference_token_span(
        self,
        refs: Sequence[BlockRef],
        *,
        token_start: int,
        token_count: int,
    ) -> List[BlockRef]:
        """Create zero-copy logical refs covering a token span across refs.

        The returned refs point to the existing physical pages and honor
        ``virtual_offset``/``filled`` for partial-page overlaps.
        """
        token_start = max(0, int(token_start))
        token_count = max(0, int(token_count))
        if token_count == 0 or not refs:
            return []
        target_end = token_start + token_count
        cursor = 0
        out: List[BlockRef] = []
        for ref in refs:
            ref_start = cursor
            ref_end = cursor + int(ref.filled)
            overlap_start = max(token_start, ref_start)
            overlap_end = min(target_end, ref_end)
            if overlap_end > overlap_start:
                local_start = overlap_start - ref_start
                span = overlap_end - overlap_start
                self.incref(ref.global_block_id or ref.physical_id)
                out.append(
                    BlockRef(
                        physical_id=ref.physical_id,
                        filled=span,
                        global_block_id=ref.global_block_id or self.kv_directory.gid_for_physical_id(ref.physical_id) or f"local:{ref.physical_id}",
                        physical_node_id=ref.physical_node_id,
                        logical_index=len(out),
                        virtual_offset=int(ref.virtual_offset) + local_start,
                    )
                )
            cursor = ref_end
            if cursor >= target_end:
                break
        return out

    def reference_token_chunks(
        self,
        refs: Sequence[BlockRef],
        *,
        token_start: int,
        chunk_sizes: Sequence[int],
    ) -> List[BlockRef]:
        """Build refs for chunked token spans using zero-copy or fused pages.

        - If a chunk lives inside one source ref, return a sub-page view ref.
        - If a chunk spans multiple source refs, allocate one fused page and copy
          only that chunk's KV slices into it.
        """
        token_start = max(0, int(token_start))
        normalized_sizes = [max(0, int(size)) for size in chunk_sizes]
        if not refs or not normalized_sizes or any(size <= 0 for size in normalized_sizes):
            return []

        out: List[BlockRef] = []
        absolute_cursor = token_start
        try:
            for chunk_size in normalized_sizes:
                chunk_end = absolute_cursor + chunk_size
                ref_cursor = 0
                spans: List[Tuple[BlockRef, int, int]] = []
                for ref in refs:
                    ref_start = ref_cursor
                    ref_end = ref_cursor + int(ref.filled)
                    overlap_start = max(absolute_cursor, ref_start)
                    overlap_end = min(chunk_end, ref_end)
                    if overlap_end > overlap_start:
                        spans.append((ref, overlap_start - ref_start, overlap_end - overlap_start))
                    ref_cursor = ref_end
                    if ref_cursor >= chunk_end:
                        break
                if sum(span for _, _, span in spans) != chunk_size:
                    raise ValueError(
                        f"unable to cover chunk_size={chunk_size} at token_start={absolute_cursor}"
                    )
                if len(spans) == 1:
                    ref, local_start, span = spans[0]
                    self.incref(ref.global_block_id or ref.physical_id)
                    out.append(
                        BlockRef(
                            physical_id=ref.physical_id,
                            filled=span,
                            global_block_id=ref.global_block_id or self.kv_directory.gid_for_physical_id(ref.physical_id) or f"local:{ref.physical_id}",
                            physical_node_id=ref.physical_node_id,
                            logical_index=len(out),
                            virtual_offset=int(ref.virtual_offset) + int(local_start),
                        )
                    )
                else:
                    key_parts: List[torch.Tensor] = []
                    value_parts: List[torch.Tensor] = []
                    for ref, local_start, span in spans:
                        key, value = self.get_page(ref)
                        start = int(ref.virtual_offset) + int(local_start)
                        stop = start + int(span)
                        key_parts.append(key[:, :, start:stop, :].clone())
                        value_parts.append(value[:, :, start:stop, :].clone())
                    fused_ref = self.allocate_page(filled=chunk_size)
                    fused_ref.logical_index = len(out)
                    self.write_page_slots(
                        fused_ref,
                        torch.cat(key_parts, dim=-2),
                        torch.cat(value_parts, dim=-2),
                        offset=0,
                    )
                    out.append(fused_ref)
                absolute_cursor = chunk_end
        except Exception:
            self.release_refs(out)
            raise
        return out

    def ingest_kv_cache(
        self,
        past_key_values,
        *,
        token_start: int = 0,
    ) -> List[BlockRef]:
        """Pack an HF-style ``past_key_values`` into managed pages.

        Accepts either:
        - a tuple/list of ``(key, value)`` per layer, where each is shaped
          ``(B, num_kv_heads, seq_len, head_dim)``
        - an HF ``DynamicCache`` (transformers >= 4.47), whose ``.key_cache``
          and ``.value_cache`` are lists of per-layer tensors.

        The batch dimension is squeezed. Returns a list of ``BlockRef``
        (all pages fully filled except possibly the last).
        """
        if past_key_values is None:
            return []
        layers = _normalize_kv_cache(past_key_values)
        if not layers:
            return []
        # Stack across layers: (num_layers, 2, num_kv_heads, seq_len, head_dim)
        keys = torch.stack([k.squeeze(0) for k, _ in layers], dim=0)
        values = torch.stack([v.squeeze(0) for _, v in layers], dim=0)
        seq_len = keys.shape[-2]
        token_start = max(0, min(int(token_start), seq_len))
        refs: List[BlockRef] = []
        for start in range(token_start, seq_len, self.page_size):
            end = min(start + self.page_size, seq_len)
            k_chunk = keys[:, :, start:end, :]
            v_chunk = values[:, :, start:end, :]
            ref = self.allocate_page(filled=end - start)
            ref.logical_index = len(refs)
            self.write_page_slots(ref, k_chunk, v_chunk, offset=0)
            refs.append(ref)
        return refs

    def materialize_kv_cache(
        self, refs: Sequence[BlockRef]
    ) -> Tuple[Tuple[torch.Tensor, torch.Tensor], ...]:
        """Reassemble pages into the HF ``past_key_values`` tuple format."""
        if not refs:
            return tuple()
        full_keys, full_values = [], []
        for ref in refs:
            k, v = self.get_page_slice(ref)
            full_keys.append(k)
            full_values.append(v)
        key_cat = torch.cat(full_keys, dim=-2)
        val_cat = torch.cat(full_values, dim=-2)
        return tuple(
            (key_cat[i].unsqueeze(0), val_cat[i].unsqueeze(0))
            for i in range(key_cat.shape[0])
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, float]:
        with self._global_lock:
            total = len(self._pages)
            shared = sum(1 for p in self._pages.values() if p.refcount > 1)
            bytes_per_page = (
                2
                * self.num_layers
                * self.num_kv_heads
                * self.page_size
                * self.head_dim
                * torch.tensor([], dtype=self.dtype).element_size()
            )
            total_bytes = total * bytes_per_page
            return {
                "total_pages": total,
                "shared_pages": shared,
                "bytes_per_page": bytes_per_page,
                "total_bytes": total_bytes,
                "directory_blocks": self.kv_directory.stats()["blocks"],
                "directory_orphans": self.kv_directory.stats()["orphan_blocks"],
            }
