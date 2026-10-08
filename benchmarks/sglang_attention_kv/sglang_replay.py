# SGLang's own KV cache structures for one attention step, and the branch of
# FlashInferAttnBackend each mode takes.
#
# Nothing here re-implements what SGLang already does:
#
#   - the request to token table is sglang.srt.mem_cache.memory_pool.ReqToTokenPool;
#   - the slots a step writes come from PagedTokenToKVPoolAllocator, the allocator
#     a server with page_size > 1 builds, through its own alloc_extend and
#     alloc_decode entry points;
#   - the KV write is MHATokenToKVPool.set_kv_buffer, the writer the attention
#     layer calls;
#   - the index mapping is create_flashinfer_kv_indices_triton, the kernel
#     KVIndexTranslator.fill_packed_read_stream launches to turn the token table
#     into the CSR stream FlashInfer reads;
#   - the read is what FlashInferAttnBackend.forward_extend / forward_decode do,
#     in the same order and with the same arguments.
#
# The branches come from that backend. SGLANG_FLASHINFER_USE_PAGED defaults to
# False, so use_ragged is True and a paged prefill wrapper is not the path an
# extend step takes: the new tokens go through the ragged wrapper, the cached
# history through the paged wrapper, the two states are merged, and the KV cache
# is written *after* the attention. A prefill with no cached history is one
# ragged forward, and decode is the paged decode wrapper. A server with
# SGLANG_FLASHINFER_USE_PAGED=1 replaces the extend branch with a single paged
# prefill call, which --extend-branch can measure as well.

from dataclasses import dataclass

import flashinfer
import torch
from flashinfer.cascade import merge_state

from sglang.kernels.ops.kvcache.kv_indices import create_flashinfer_kv_indices_triton
from sglang.srt.mem_cache.allocator import PagedTokenToKVPoolAllocator
from sglang.srt.mem_cache.memory_pool import (
    KVWriteLoc,
    MHATokenToKVPool,
    ReqToTokenPool,
)

from .cases import (
    BRANCH_DETAIL,
    BRANCH_PAGED_DECODE,
    BRANCH_PAGED_EXTEND,
    BRANCH_RAGGED_NO_PREFIX,
    BRANCH_RAGGED_PREFIX_MERGE,
    BRANCHES_THAT_WRITE_FIRST,
    use_paged_default,
)

# The wrapper backend and the page size SGLang plans the wrappers with: it hands
# FlashInfer a token-level CSR stream, so every wrapper is planned as page_size=1
# and every last_page_len is 1. The pool's page size is a separate figure, and it
# reaches the kernels only through the addresses in that stream.
FLASHINFER_BACKEND = "fa2"
WRAPPER_PAGE_SIZE = 1
WORKSPACE_BYTES = 384 * 1024 * 1024

# The decode wrapper's tensor-core path, which is what FlashInferAttnBackend
# builds for a decode step.
DECODE_USE_TENSOR_CORES = True

# Spare pages beyond what the case needs, so a layout can leave gaps.
SPARE_PAGE_FRACTION = 8

# Whether the KV writer runs on the pool's alternate stream. A server leaves it
# on and lets the write overlap with the attention, which makes a phase-by-phase
# breakdown meaningless: the write's cost then lands in whichever window syncs
# next. The benchmark writes on the step's own stream, so every phase is that
# phase's own cost, and each record states which of the two it measured.
KV_WRITE_ALT_STREAM = False

# The two sources this benchmark writes: the cached history and the K/V of the
# step itself. Regenerated on demand from these seeds, so a check can compare
# against them without holding every layer of the context in memory.
PREFIX_SOURCE = 10_000
STEP_SOURCE = 20_000


class LayerStandIn:
    """The attributes SGLang reads off an attention layer: the writer reads
    layer_id, and the kernel calls take the head count, the head width and the
    softmax scale."""

    def __init__(self, layer_id, qo_heads, head_dim):
        self.layer_id = layer_id
        self.tp_q_head_num = qo_heads
        self.head_dim = head_dim
        self.scaling = head_dim**-0.5
        self.logit_cap = None
        self.k_scale_float = None
        self.v_scale_float = None


@dataclass
class StepIndices:
    """What one step hands to the attention kernels."""

    qo_indptr: torch.Tensor
    out_cache_loc: torch.Tensor
    paged_indptr: torch.Tensor = None
    paged_indices: torch.Tensor = None
    paged_last_page_len: torch.Tensor = None


class SglangStep:
    """One step driven through SGLang's own structures. Head counts are per rank."""

    def __init__(self, case, device, seed=0, extend_branch=BRANCH_RAGGED_PREFIX_MERGE):
        self.case = case
        self.device = device
        self.seed = seed
        self.dtype = getattr(torch, case.dtype)
        self.branch = self._branch_for(case.mode, extend_branch)
        self.branch_detail = BRANCH_DETAIL[self.branch]
        self.reads_before_write = self.branch not in BRANCHES_THAT_WRITE_FIRST
        self.layers = [
            LayerStandIn(layer_id, case.num_qo_heads, case.head_dim)
            for layer_id in range(case.num_layers)
        ]
        self.prefix_lens = torch.tensor(
            list(case.prefix_lens), dtype=torch.int32, device=device
        )
        self.new_lens = torch.tensor(
            list(case.new_lens), dtype=torch.int32, device=device
        )
        self.context_lens = torch.tensor(
            list(case.context_lens), dtype=torch.int32, device=device
        )
        self.req_pool_indices = torch.arange(
            1, case.batch_size + 1, dtype=torch.int32, device=device
        )
        self.workspace = torch.empty(WORKSPACE_BYTES, dtype=torch.uint8, device=device)

    @staticmethod
    def _branch_for(mode, extend_branch):
        if mode == "prefill":
            return BRANCH_RAGGED_NO_PREFIX
        if mode == "extend":
            return extend_branch
        return BRANCH_PAGED_DECODE

    # ---------------------------------------------------------------- setup

    def prepare(self):
        """Build the pools a server would build for these sequences, allocate the
        slots through SGLang's allocator, and fill the cached history with known
        K/V."""
        case = self.case
        # A churned pool holds as many pages as the step needs, scattered over a
        # pool twice that size.
        self.pages_needed = case.pages + 2 * case.batch_size + 2
        spare = max(16, case.pages // SPARE_PAGE_FRACTION)
        if case.layout == "random":
            self.num_pages_allocated = 2 * self.pages_needed + spare
        else:
            self.num_pages_allocated = self.pages_needed + spare
        pool_size = self.num_pages_allocated * case.page_size

        self.token_pool = ReqToTokenPool(
            size=case.batch_size,
            max_context_len=max(case.context_lens),
            device=self.device,
            enable_memory_saver=False,
        )
        self.kv_pool = MHATokenToKVPool(
            size=pool_size,
            page_size=case.page_size,
            dtype=self.dtype,
            head_num=case.num_kv_heads,
            head_dim=case.head_dim,
            layer_num=case.num_layers,
            device=self.device,
            enable_memory_saver=False,
            enable_alt_stream=KV_WRITE_ALT_STREAM,
        )
        self.allocator = PagedTokenToKVPoolAllocator(
            size=pool_size,
            page_size=case.page_size,
            dtype=self.dtype,
            device=self.device,
            kvcache=self.kv_pool,
            need_sort=False,
        )
        key_buffer, _ = self.kv_pool.get_kv_buffer(0)
        self.kv_cache_bytes_per_rank = (
            key_buffer.numel() * key_buffer.element_size() * 2 * case.num_layers
        )

        self._shape_the_free_pages()
        self._allocate_slots()
        self._allocate_step_slots()
        self.new_kv = self.make_new_kv()
        self._fill_prefix()
        self._build_wrappers()
        return self

    def _shape_the_free_pages(self):
        """The order the allocator hands pages out in, which is the layout the
        step is measured in. Both layouts come from SGLang's allocator; the
        benchmark only decides which pages are free when the step asks.

        contiguous: the pool has not churned, so the step gets ascending pages.
        random: the step gets a seeded random subset of the pool in a random
        order, which is a pool that churned. The subset is drawn once per case, so
        the mapping is the same for every timed iteration.
        """
        if self.case.layout == "contiguous":
            return
        generator = torch.Generator(device="cpu")
        generator.manual_seed(self.seed)
        self._held_pages = self.allocator.alloc(
            self.num_pages_allocated * self.case.page_size
        )
        # Page 0 is reserved and is never handed out, so the draw starts at 1.
        available = torch.arange(
            1, self.num_pages_allocated, dtype=torch.int64, device=self.device
        )
        order = torch.randperm(available.numel(), generator=generator).to(self.device)
        shuffled = available[order]
        self.free_pages = shuffled[: self.pages_needed]
        spare = shuffled[self.pages_needed :]
        self.allocator.free_page_ids(torch.cat([self.free_pages, spare]))

    def _allocate_slots(self):
        """The history slots of every sequence, page aligned, through the
        allocator a server with this page size builds."""
        page_size = self.case.page_size
        self.prefix_slots = []
        for prefix_len in self.case.prefix_lens:
            if prefix_len == 0:
                self.prefix_slots.append(
                    torch.empty(0, dtype=torch.int64, device=self.device)
                )
                continue
            aligned = -(-prefix_len // page_size) * page_size
            slots = self.allocator.alloc(aligned)
            if slots is None:
                raise RuntimeError(
                    f"the allocator ran out of pages for a {prefix_len}-token history"
                )
            self.prefix_slots.append(slots[:prefix_len])

    def _last_prefix_slot(self):
        """The slot the last cached token of each sequence sits in, which is what
        the allocator's step entry points take: it is the slot, not the token
        index, that says whether the new tokens continue a page or start one."""
        slots = []
        for index, prefix_len in enumerate(self.case.prefix_lens):
            slots.append(-1 if prefix_len == 0 else int(self.prefix_slots[index][-1]))
        return torch.tensor(slots, dtype=torch.int64, device=self.device)

    def _allocate_step_slots(self):
        """The slots this step writes, through the allocator's own step entry
        points: alloc_extend for a step that computes a chunk, alloc_decode for a
        single token."""
        case = self.case
        last_loc = self._last_prefix_slot()
        if case.mode == "decode":
            slots = self.allocator.alloc_decode(
                seq_lens=self.context_lens,
                seq_lens_cpu=self.context_lens.cpu(),
                last_loc=last_loc,
            )
        else:
            slots = self.allocator.alloc_extend(
                prefix_lens=self.prefix_lens,
                prefix_lens_cpu=self.prefix_lens.cpu(),
                seq_lens=self.context_lens,
                seq_lens_cpu=self.context_lens.cpu(),
                last_loc=last_loc,
                extend_num_tokens=case.new_tokens,
            )
        if slots is None:
            raise RuntimeError("the allocator ran out of pages for this step")
        self.step_slots = slots[: case.new_tokens].to(torch.int64)

        # The token table the scheduler keeps up to date
        offset = 0
        for index, (prefix_len, new_len) in enumerate(
            zip(case.prefix_lens, case.new_lens)
        ):
            row = int(self.req_pool_indices[index])
            if prefix_len:
                self.token_pool.write(
                    (row, slice(0, prefix_len)), self.prefix_slots[index]
                )
            self.token_pool.write(
                (row, slice(prefix_len, prefix_len + new_len)),
                self.step_slots[offset : offset + new_len],
            )
            offset += new_len

    def _random(self, shape, base, tag=0):
        generator = torch.Generator(device=self.device)
        generator.manual_seed(1_000_003 * base + 7_919 * tag + self.seed)
        return torch.randn(
            shape, dtype=self.dtype, device=self.device, generator=generator
        )

    def _source(self, base, tag, count):
        return self._random(
            (count, self.case.num_kv_heads, self.case.head_dim), base, tag
        )

    def prefix_source(self, layer_id, sequence_index, count):
        return self._source(PREFIX_SOURCE, layer_id * 64 + sequence_index, count)

    def prefix_value_source(self, layer_id, sequence_index, count):
        return self._source(
            PREFIX_SOURCE,
            (self.case.num_layers + layer_id) * 64 + sequence_index,
            count,
        )

    def make_new_kv(self):
        """The K and V this step computes, one pair per layer."""
        case = self.case
        return {
            layer_id: (
                self._source(STEP_SOURCE, layer_id, case.new_tokens),
                self._source(
                    STEP_SOURCE, self.case.num_layers + layer_id, case.new_tokens
                ),
            )
            for layer_id in range(case.num_layers)
        }

    def make_query(self):
        return self._random(
            (self.case.new_tokens, self.case.num_qo_heads, self.case.head_dim), 30_000
        )

    def _fill_prefix(self):
        """Write the cached history the step reads, layer by layer, through
        SGLang's writer."""
        for index, prefix_len in enumerate(self.case.prefix_lens):
            if prefix_len == 0:
                continue
            slots = self.prefix_slots[index]
            for layer_id, layer in enumerate(self.layers):
                k = self.prefix_source(layer_id, index, prefix_len)
                v = self.prefix_value_source(layer_id, index, prefix_len)
                self.kv_pool.set_kv_buffer(layer, KVWriteLoc(slots), k, v)
        torch.cuda.synchronize()

    def _build_wrappers(self):
        self.ragged_wrapper = None
        self.paged_wrapper = None
        self.decode_wrapper = None
        if self.branch == BRANCH_RAGGED_NO_PREFIX:
            self.ragged_wrapper = flashinfer.BatchPrefillWithRaggedKVCacheWrapper(
                self.workspace, kv_layout="NHD", backend=FLASHINFER_BACKEND
            )
        elif self.branch == BRANCH_RAGGED_PREFIX_MERGE:
            # The suffix goes through the ragged wrapper and the history through
            # the paged one; FlashInferAttnBackend builds both for this branch.
            self.ragged_wrapper = flashinfer.BatchPrefillWithRaggedKVCacheWrapper(
                self.workspace, kv_layout="NHD", backend=FLASHINFER_BACKEND
            )
            self.paged_wrapper = flashinfer.BatchPrefillWithPagedKVCacheWrapper(
                self.workspace, "NHD", backend=FLASHINFER_BACKEND
            )
        elif self.branch == BRANCH_PAGED_EXTEND:
            self.paged_wrapper = flashinfer.BatchPrefillWithPagedKVCacheWrapper(
                self.workspace, "NHD", backend=FLASHINFER_BACKEND
            )
        else:
            self.decode_wrapper = flashinfer.BatchDecodeWithPagedKVCacheWrapper(
                self.workspace, "NHD", use_tensor_cores=DECODE_USE_TENSOR_CORES
            )

    # ------------------------------------------------------------ the step

    def _history_lens(self):
        """The tokens the paged side of this branch reads: the cached history in
        the merge branch, the whole context in the paged branches."""
        if self.branch == BRANCH_RAGGED_PREFIX_MERGE:
            return tuple(self.case.prefix_lens)
        return self.case.context_lens

    def _history_tokens(self):
        return sum(self._history_lens())

    def build_indices(self):
        """SGLang's index stage: the query offsets and the CSR stream of the
        tokens the paged side reads.

        Every length is known before the step runs, so the buffers are allocated
        from the case and the stream is filled by SGLang's kernel with no host
        synchronisation inside the timed window.
        """
        case = self.case
        qo_indptr = torch.zeros(
            case.batch_size + 1, dtype=torch.int32, device=self.device
        )
        qo_indptr[1:] = torch.cumsum(self.new_lens, dim=0).to(torch.int32)

        indices = StepIndices(qo_indptr=qo_indptr, out_cache_loc=self.step_slots)
        if self.branch == BRANCH_RAGGED_NO_PREFIX:
            return indices

        history_lens = torch.tensor(
            list(self._history_lens()), dtype=torch.int32, device=self.device
        )
        paged_indptr = torch.zeros(
            case.batch_size + 1, dtype=torch.int32, device=self.device
        )
        paged_indptr[1:] = torch.cumsum(history_lens, dim=0).to(torch.int32)
        paged_indices = torch.empty(
            self._history_tokens(), dtype=torch.int32, device=self.device
        )
        create_flashinfer_kv_indices_triton[(case.batch_size,)](
            self.token_pool.req_to_token,
            self.req_pool_indices,
            history_lens,
            paged_indptr,
            None,
            paged_indices,
            self.token_pool.req_to_token.stride(0),
        )
        indices.paged_indptr = paged_indptr
        indices.paged_indices = paged_indices
        indices.paged_last_page_len = torch.ones(
            case.batch_size, dtype=torch.int32, device=self.device
        )
        return indices

    def write_layer(self, layer_id, indices):
        """SGLang's writer for one layer."""
        k, v = self.new_kv[layer_id]
        self.kv_pool.set_kv_buffer(
            self.layers[layer_id], KVWriteLoc(indices.out_cache_loc), k, v
        )

    def write_kv(self, indices):
        """SGLang's writer, once per layer."""
        for layer_id in range(self.case.num_layers):
            self.write_layer(layer_id, indices)

    def plan(self, indices):
        case = self.case
        if self.ragged_wrapper is not None:
            self.ragged_wrapper.plan(
                qo_indptr=indices.qo_indptr,
                kv_indptr=indices.qo_indptr,
                num_qo_heads=case.num_qo_heads,
                num_kv_heads=case.num_kv_heads,
                head_dim_qk=case.head_dim,
                causal=True,
                q_data_type=self.dtype,
                kv_data_type=self.dtype,
            )
        if self.paged_wrapper is not None:
            # SGLang plans this wrapper without a causal mask: the history of the
            # merge branch is entirely before the queries, and the paged extend
            # branch is planned the same way.
            self.paged_wrapper.plan(
                qo_indptr=indices.qo_indptr,
                paged_kv_indptr=indices.paged_indptr,
                paged_kv_indices=indices.paged_indices,
                paged_kv_last_page_len=indices.paged_last_page_len,
                num_qo_heads=case.num_qo_heads,
                num_kv_heads=case.num_kv_heads,
                head_dim_qk=case.head_dim,
                page_size=WRAPPER_PAGE_SIZE,
                q_data_type=self.dtype,
                kv_data_type=self.dtype,
            )
        if self.decode_wrapper is not None:
            self.decode_wrapper.plan(
                indptr=indices.paged_indptr,
                indices=indices.paged_indices,
                last_page_len=indices.paged_last_page_len,
                num_qo_heads=case.num_qo_heads,
                num_kv_heads=case.num_kv_heads,
                head_dim=case.head_dim,
                page_size=WRAPPER_PAGE_SIZE,
                q_data_type=self.dtype,
                kv_data_type=self.dtype,
            )

    def run_layer(self, layer_id, query):
        """One layer's kernel calls, as FlashInferAttnBackend makes them."""
        scale = self.layers[layer_id].scaling
        if self.branch == BRANCH_RAGGED_NO_PREFIX:
            k, v = self.new_kv[layer_id]
            return self.ragged_wrapper.forward(query, k, v, causal=True, sm_scale=scale)
        if self.branch == BRANCH_RAGGED_PREFIX_MERGE:
            k, v = self.new_kv[layer_id]
            suffix, suffix_lse = self.ragged_wrapper.forward_return_lse(
                query, k, v, causal=True, sm_scale=scale
            )
            history, history_lse = self.paged_wrapper.forward_return_lse(
                query,
                self.kv_pool.get_kv_buffer(layer_id),
                causal=False,
                sm_scale=scale,
            )
            merged, _ = merge_state(suffix, suffix_lse, history, history_lse)
            return merged
        if self.branch == BRANCH_PAGED_EXTEND:
            # SGLang passes causal=True to this wrapper's forward, which is what
            # turns the mask on: the queries must not see the tokens after them.
            return self.paged_wrapper.forward(
                query, self.kv_pool.get_kv_buffer(layer_id), causal=True, sm_scale=scale
            )
        return self.decode_wrapper.forward(
            query, self.kv_pool.get_kv_buffer(layer_id), sm_scale=scale
        )

    def read(self, query):
        """SGLang's read, once per layer."""
        output = None
        for layer_id in range(self.case.num_layers):
            output = self.run_layer(layer_id, query)
        return output

    def gather_rows(self, indices):
        """The rows the read-only probe moves: the rows the paged side of this
        branch reads, or the slots this step wrote when the branch reads no paged
        KV."""
        if indices.paged_indices is not None:
            return indices.paged_indices.long()
        return indices.out_cache_loc.long()

    def gather_bytes(self, indices):
        """The bytes the probe moves: both K and V of the rows it reads, for every
        layer."""
        return self.case.kv_bytes(self.gather_rows(indices).numel())

    def read_tokens(self):
        """The tokens of KV the branch's attention reads, as (paged, ragged).

        The paged side reads the sequence's cached history and the ragged side
        reads the K/V this step computes, so which of the two is counted depends on
        the branch: the two ragged branches hand their own tokens to the ragged
        wrapper and let the paged wrapper see the history only, while the paged
        branches read the whole context through the paged wrapper.
        """
        if self.branch == BRANCH_RAGGED_NO_PREFIX:
            return 0, self.case.new_tokens
        if self.branch == BRANCH_RAGGED_PREFIX_MERGE:
            history = self.case.context_tokens - self.case.new_tokens
            return history, self.case.new_tokens
        return self.case.context_tokens, 0

    def read_bytes(self):
        """The KV bytes the attention window covers, split by the path that reads
        them: the paged side's logical bytes, the ragged side's, and their sum.
        That sum is what the window's bandwidth and its arithmetic intensity divide
        by; the page capacity is a separate figure."""
        paged_tokens, ragged_tokens = self.read_tokens()
        paged = self.case.kv_bytes(paged_tokens)
        ragged = self.case.kv_bytes(ragged_tokens)
        return {"paged": paged, "ragged": ragged, "attention": paged + ragged}

    def gather(self, indices):
        """A read-only probe over the same rows: the same bytes, both tensors,
        moved with index_select and no arithmetic."""
        rows = self.gather_rows(indices)
        output = None
        for layer_id in range(self.case.num_layers):
            key_buffer, value_buffer = self.kv_pool.get_kv_buffer(layer_id)
            output = key_buffer.index_select(0, rows)
            output = value_buffer.index_select(0, rows)
        return output

    # -------------------------------------------------------------- checks

    def _expected_rows(self):
        """The token table rows the paged side of this branch should read."""
        rows = []
        for index, history_len in enumerate(self._history_lens()):
            row = int(self.req_pool_indices[index])
            rows.append(self.token_pool.req_to_token[row, :history_len])
        return torch.cat(rows).long()

    def check_indices(self, indices):
        """The CSR stream has to name exactly the token table rows the paged side
        reads, in order. A stream built from a different page table fails here."""
        if indices.paged_indices is None:
            return {
                "stream_entries": 0,
                "mismatched_slots": 0,
                "passed": True,
                "note": "this branch reads no paged KV",
            }
        expected = self._expected_rows()
        actual = indices.paged_indices.long()
        mismatched = int((expected != actual).sum().item())
        return {
            "stream_entries": int(actual.numel()),
            "mismatched_slots": mismatched,
            "passed": bool(mismatched == 0 and expected.numel() == actual.numel()),
        }

    def _expected_kv(self, layer_id, sequence_index):
        """The K and V the token table should point at for one sequence: its
        cached history followed by the tokens this step wrote."""
        case = self.case
        prefix_len = case.prefix_lens[sequence_index]
        start = sum(case.new_lens[:sequence_index])
        stop = start + case.new_lens[sequence_index]
        new_k = self.new_kv[layer_id][0][start:stop]
        new_v = self.new_kv[layer_id][1][start:stop]
        if prefix_len == 0:
            return new_k, new_v
        history_k = self.prefix_source(layer_id, sequence_index, prefix_len)
        history_v = self.prefix_value_source(layer_id, sequence_index, prefix_len)
        return torch.cat([history_k, new_k]), torch.cat([history_v, new_v])

    def check_history(self, indices):
        """Every token the step reads has to hold what was written, K and V, for
        every layer: the cached history and the tokens this step wrote."""
        case = self.case
        mismatched_k = 0
        mismatched_v = 0
        checked = 0
        for index, context_len in enumerate(case.context_lens):
            row = int(self.req_pool_indices[index])
            rows = self.token_pool.req_to_token[row, :context_len].long()
            for layer_id in range(case.num_layers):
                key_buffer, value_buffer = self.kv_pool.get_kv_buffer(layer_id)
                expected_k, expected_v = self._expected_kv(layer_id, index)
                mismatched_k += int(
                    (key_buffer.index_select(0, rows) != expected_k).sum().item()
                )
                mismatched_v += int(
                    (value_buffer.index_select(0, rows) != expected_v).sum().item()
                )
                checked += expected_k.numel()
        elements = max(1, case.num_kv_heads * case.head_dim)
        return {
            "tokens_checked": checked // elements,
            "mismatched_k_elements": mismatched_k,
            "mismatched_v_elements": mismatched_v,
            "passed": bool(mismatched_k == 0 and mismatched_v == 0),
        }

    def check_attention(self, query):
        """The kernel output against a plain tensor reference over the same
        content, in float32.

        The reference is causal for every branch: a step's queries must not see
        the tokens after them, and in the merge branch the full causal attention
        over history and queries is what its two kernels and merge_state have to
        add up to. A reference without the mask would pass a kernel that sees the
        future, so the mask is not optional here.
        """
        case = self.case
        produced = self.run_layer(0, query)
        torch.cuda.synchronize()

        scale = 1.0 / (case.head_dim**0.5)
        repeat = case.num_qo_heads // case.num_kv_heads
        outputs = []
        q_offset = 0
        for index, new_len in enumerate(case.new_lens):
            prefix_len = case.prefix_lens[index]
            key_source, value_source = self._expected_kv(0, index)
            k_seq = key_source.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
            v_seq = (
                value_source.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
            )
            q_seq = query[q_offset : q_offset + new_len].transpose(0, 1).float()
            q_offset += new_len
            scores = torch.matmul(q_seq, k_seq.transpose(-1, -2)) * scale
            mask = torch.ones(
                (new_len, prefix_len + new_len),
                dtype=torch.bool,
                device=query.device,
            ).tril(diagonal=prefix_len)
            scores = scores.masked_fill(~mask, float("-inf"))
            weights = torch.softmax(scores, dim=-1)
            outputs.append(torch.matmul(weights, v_seq).transpose(0, 1))
        reference = torch.cat(outputs)
        difference = (produced.float() - reference).abs()
        largest = reference.abs().max().item()
        return {
            "max_abs_diff": difference.max().item(),
            "reference_abs_max": largest,
            "max_rel_diff": difference.max().item() / max(largest, 1e-6),
            "mask": "causal",
            "passed": bool(difference.max().item() / max(largest, 1e-6) < 1e-2),
        }

    def unmasked_reference(self, query):
        """The same comparison with the mask left out, so a test can show that the
        causal check above is sensitive to a kernel that sees the future."""
        case = self.case
        produced = self.run_layer(0, query)
        torch.cuda.synchronize()

        scale = 1.0 / (case.head_dim**0.5)
        repeat = case.num_qo_heads // case.num_kv_heads
        outputs = []
        q_offset = 0
        for index, new_len in enumerate(case.new_lens):
            key_source, value_source = self._expected_kv(0, index)
            k_seq = key_source.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
            v_seq = (
                value_source.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
            )
            q_seq = query[q_offset : q_offset + new_len].transpose(0, 1).float()
            q_offset += new_len
            scores = torch.matmul(q_seq, k_seq.transpose(-1, -2)) * scale
            weights = torch.softmax(scores, dim=-1)
            outputs.append(torch.matmul(weights, v_seq).transpose(0, 1))
        reference = torch.cat(outputs)
        difference = (produced.float() - reference).abs()
        largest = reference.abs().max().item()
        return {
            "max_rel_diff": difference.max().item() / max(largest, 1e-6),
            "passed": bool(difference.max().item() / max(largest, 1e-6) < 1e-2),
        }

    def as_dict(self):
        """What a record states about this step's configuration."""
        return {
            "branch": self.branch,
            "branch_detail": self.branch_detail,
            "reads_before_write": self.reads_before_write,
            "attention_backend": f"flashinfer-{FLASHINFER_BACKEND}",
            "wrapper_page_size": WRAPPER_PAGE_SIZE,
            "decode_use_tensor_cores": DECODE_USE_TENSOR_CORES,
            "kv_layout": "NHD",
            "kv_write_stream": "alternate" if KV_WRITE_ALT_STREAM else "step",
            "flashinfer_use_paged_env": use_paged_default(),
        }
