# Kernel-level benchmark: builds a real paged KV cache, page table, slot mapping
# and seq_lens, and times the phases of one attention pass over the KV cache.
# A synthetic load is not equivalent to end-to-end serving; the cache reuse
# benefit is measured by e2e.py.

import json
import time
from dataclasses import asdict, dataclass

import flashinfer
import torch

from .config import canonical_torch_dtype
from .stats import summarize

# Attention step phases, in the order the step runs them. attention_plan is the
# attention backend compiling the page table into a kernel schedule, attention is
# the per-layer kernel calls. They are recorded apart so the plan cost is not
# folded into the kernel.
STEP_PHASES = ("metadata", "kv_write", "attention_plan", "attention")

# A read-only comparison, not part of a step: it copies every layer's KV through
# the page table to measure what the read path alone costs. Adding it to the step
# total would report a throughput no forward pass has.
DIAGNOSTIC_PHASES = ("kv_gather",)

MEASURED_PHASES = STEP_PHASES + DIAGNOSTIC_PHASES
PHASES = MEASURED_PHASES + ("total_step",)

DEFAULT_WARMUP = 10
DEFAULT_TIMED = 100

# FlashInfer paged attention backend in use
FLASHINFER_BACKEND = "fa2"

# KV precisions that backend supports. A model declaring fp8 has to fail while
# the cases are built, otherwise the byte counts and throughput would be
# computed against a layout the model does not have.
FLASHINFER_KV_DTYPES = ("bfloat16", "float16")


@dataclass(frozen=True)
class KernelCase:
    """One kernel-level measurement point. num_kv_heads is per rank."""

    mode: str
    seq_len: int
    batch_size: int
    page_size: int
    num_layers: int
    num_qo_heads: int
    num_kv_heads: int
    head_dim: int
    dtype: str

    @property
    def label(self):
        if self.mode == "prefill":
            return f"prefill_len{self.seq_len}_bs{self.batch_size}"
        return f"decode_ctx{self.seq_len}_bs{self.batch_size}"

    @property
    def dtype_bytes(self):
        return torch.empty((), dtype=getattr(torch, self.dtype)).element_size()

    @property
    def new_tokens(self):
        if self.mode == "prefill":
            return self.seq_len * self.batch_size
        return self.batch_size

    @property
    def context_tokens(self):
        """Tokens covered when attention reads historical KV."""
        if self.mode == "prefill":
            return self.seq_len * self.batch_size
        return (self.seq_len + 1) * self.batch_size

    @property
    def pages_per_seq(self):
        total = self.seq_len if self.mode == "prefill" else self.seq_len + 1
        return (total + self.page_size - 1) // self.page_size

    @property
    def kv_cache_bytes(self):
        """KV cache bytes this point allocates in the real engine layout."""
        return (
            self.num_layers
            * 2
            * self.pages_per_seq
            * self.batch_size
            * self.page_size
            * self.num_kv_heads
            * self.head_dim
            * self.dtype_bytes
        )

    def kv_bytes_written(self):
        return (
            self.num_layers
            * 2
            * self.new_tokens
            * self.num_kv_heads
            * self.head_dim
            * self.dtype_bytes
        )

    @property
    def pages_read(self):
        """Pages the attention kernel and the gather actually touch."""
        return self.pages_per_seq * self.batch_size

    def kv_bytes_read(self):
        """KV bytes of the valid tokens, for any per-token figure."""
        return (
            self.num_layers
            * 2
            * self.context_tokens
            * self.num_kv_heads
            * self.head_dim
            * self.dtype_bytes
        )

    def kv_bytes_read_moved(self):
        """Bytes the kernel actually moves.

        A paged read touches whole pages, so a short last page drags its padding
        along. Bandwidth and arithmetic intensity have to use this count; the
        valid-token count would skew both.
        """
        return (
            self.num_layers
            * 2
            * self.pages_read
            * self.page_size
            * self.num_kv_heads
            * self.head_dim
            * self.dtype_bytes
        )

    @property
    def padding_tokens_read(self):
        return self.pages_read * self.page_size - self.context_tokens

    def attention_flops(self):
        """Estimated attention work. QK^T and PV are one multiply-add each, so a
        query-key pair costs 4 floating point operations; a causal prefill mask
        leaves roughly half of the q_len times kv_len pairs."""
        if self.mode == "prefill":
            pairs_per_seq = self.seq_len * (self.seq_len + 1) / 2.0
        else:
            pairs_per_seq = float(self.seq_len + 1)
        return (
            2 * 2 * pairs_per_seq * self.num_qo_heads * self.head_dim * self.batch_size
        )

    def as_dict(self):
        body = asdict(self)
        body.update(
            {
                "label": self.label,
                "new_tokens": self.new_tokens,
                "context_tokens": self.context_tokens,
                "kv_layout": "NHD",
                "attention_backend": f"flashinfer-{FLASHINFER_BACKEND}",
                "kv_bytes_written": self.kv_bytes_written(),
                "kv_bytes_read_valid": self.kv_bytes_read(),
                "kv_bytes_read_moved": self.kv_bytes_read_moved(),
                "padding_tokens_read": self.padding_tokens_read,
                "kv_cache_bytes": self.kv_cache_bytes,
                "attention_flops": self.attention_flops(),
            }
        )
        return body


@dataclass
class PagedMeta:
    """Every piece of metadata one paged attention pass needs."""

    qo_indptr: torch.Tensor
    paged_kv_indptr: torch.Tensor
    paged_kv_indices: torch.Tensor
    paged_kv_last_page_len: torch.Tensor
    batch_indices: torch.Tensor
    positions: torch.Tensor
    slot_mapping: torch.Tensor
    page_table: torch.Tensor
    seq_lens: torch.Tensor
    metadata_cpu_ms: float = 0.0
    metadata_h2d_ms: float = 0.0


class PagedKVCache:
    """Allocate a multi-layer paged KV cache in the real engine layout."""

    def __init__(self, case: KernelCase, device, seed_offset=0):
        self.case = case
        self.device = device
        self.dtype = getattr(torch, case.dtype)
        self.num_heads = case.num_kv_heads
        self.head_dim = case.head_dim
        self.page_size = case.page_size

        required_pages = case.pages_per_seq * case.batch_size
        # Keep some pages spare so the page table can point at scattered
        # physical pages instead of degenerating into a sequential walk
        self.num_pages = required_pages + max(16, required_pages // 8)
        self._block_offsets = torch.arange(
            case.page_size, dtype=torch.int64, device=device
        )

        shape = (
            case.num_layers,
            self.num_pages,
            case.page_size,
            case.num_kv_heads,
            case.head_dim,
        )
        self.k_cache = torch.zeros(shape, dtype=self.dtype, device=device)
        self.v_cache = torch.zeros(shape, dtype=self.dtype, device=device)

        self._page_rng = torch.Generator(device="cpu")
        self._page_rng.manual_seed(
            case.seq_len * 131 + case.batch_size * 17 + seed_offset
        )

    @property
    def bytes_per_rank(self):
        return (
            self.k_cache.numel() + self.v_cache.numel()
        ) * self.k_cache.element_size()

    def rows_for(self, page_table):
        """Expand the page table into rows of the flattened cache; index mapping."""
        physical = page_table.reshape(-1).to(torch.int64)
        rows = physical.unsqueeze(1) * self.page_size + self._block_offsets
        return rows.reshape(-1)

    def build_metadata(self, history=False):
        """Build the page table, slot mapping and paged KV indices. The CPU side
        and the H2D copy are timed apart and both count towards metadata.

        history=True builds what is needed to write the historical KV for
        positions 0..seq_len-1, since decode needs real history to read.
        """
        case = self.case
        batch = case.batch_size
        pages_per_seq = case.pages_per_seq
        page_size = case.page_size

        cpu_begin = time.perf_counter()

        # Shuffle the physical page numbers so the page table is a real indirection
        perm = torch.randperm(self.num_pages, generator=self._page_rng)
        page_table = torch.empty((batch, pages_per_seq), dtype=torch.int32)
        for index in range(batch):
            start = index * pages_per_seq
            page_table[index] = perm[start : start + pages_per_seq]

        if history:
            new_per_seq = case.seq_len
            base_of_seq = [0] * batch
            total_ctx = case.seq_len
        elif case.mode == "prefill":
            new_per_seq = case.seq_len
            base_of_seq = [0] * batch
            total_ctx = case.seq_len
        else:
            new_per_seq = 1
            base_of_seq = [case.seq_len] * batch
            total_ctx = case.seq_len + 1

        batch_indices = torch.cat(
            [
                torch.full((new_per_seq,), index, dtype=torch.int32)
                for index in range(batch)
            ]
        )
        positions = torch.cat(
            [
                torch.arange(base, base + new_per_seq, dtype=torch.int32)
                for base in base_of_seq
            ]
        )

        # slot_mapping maps each new token to an element offset in the flat cache
        page_of_token = positions.to(torch.int64) // page_size
        offset_in_page = positions.to(torch.int64) % page_size
        physical = page_table.to(torch.int64)[
            batch_indices.to(torch.int64), page_of_token
        ]
        slot_mapping = (
            (physical * page_size + offset_in_page) * (self.num_heads * self.head_dim)
        ).to(torch.int64)

        remainder = total_ctx % page_size
        last_page_len = page_size if remainder == 0 else remainder

        meta_cpu = PagedMeta(
            qo_indptr=torch.arange(
                0, (batch + 1) * new_per_seq, new_per_seq, dtype=torch.int32
            ),
            paged_kv_indptr=torch.arange(
                0, (batch + 1) * pages_per_seq, pages_per_seq, dtype=torch.int32
            ),
            paged_kv_indices=page_table.reshape(-1).contiguous(),
            paged_kv_last_page_len=torch.full(
                (batch,), last_page_len, dtype=torch.int32
            ),
            batch_indices=batch_indices,
            positions=positions,
            slot_mapping=slot_mapping,
            page_table=page_table,
            seq_lens=torch.full(
                (batch,),
                case.seq_len
                if (history or case.mode == "prefill")
                else case.seq_len + 1,
                dtype=torch.int32,
            ),
        )
        meta_cpu.metadata_cpu_ms = (time.perf_counter() - cpu_begin) * 1000.0

        h2d_begin = torch.cuda.Event(enable_timing=True)
        h2d_end = torch.cuda.Event(enable_timing=True)
        h2d_begin.record()
        on_device = PagedMeta(
            qo_indptr=meta_cpu.qo_indptr.to(self.device),
            paged_kv_indptr=meta_cpu.paged_kv_indptr.to(self.device),
            paged_kv_indices=meta_cpu.paged_kv_indices.to(self.device),
            paged_kv_last_page_len=meta_cpu.paged_kv_last_page_len.to(self.device),
            batch_indices=meta_cpu.batch_indices.to(self.device),
            positions=meta_cpu.positions.to(self.device),
            slot_mapping=meta_cpu.slot_mapping.to(self.device),
            page_table=meta_cpu.page_table.to(self.device),
            seq_lens=meta_cpu.seq_lens.to(self.device),
            metadata_cpu_ms=meta_cpu.metadata_cpu_ms,
        )
        h2d_end.record()
        torch.cuda.synchronize()
        on_device.metadata_h2d_ms = h2d_begin.elapsed_time(h2d_end)
        return on_device

    def append_kv(self, layer, k, v, meta):
        """Write newly produced K/V into the paged KV cache."""
        flashinfer.append_paged_kv_cache(
            k,
            v,
            meta.batch_indices,
            meta.positions,
            (self.k_cache[layer], self.v_cache[layer]),
            meta.paged_kv_indices,
            meta.paged_kv_indptr,
            meta.paged_kv_last_page_len,
            kv_layout="NHD",
        )

    def prefill_history(self, generator):
        """Write the historical KV for 0..seq_len-1 into the cache.

        Decode appends a single token. Without filling the history first,
        attention reads a stretch of zeros: the same number of bytes is read,
        but neither the data path nor the correctness check ever touches real
        historical KV.
        """
        case = self.case
        if case.mode != "decode":
            return None
        meta = self.build_metadata(history=True)
        total = case.seq_len * case.batch_size
        k = torch.randn(
            (total, case.num_kv_heads, case.head_dim),
            dtype=self.dtype,
            device=self.device,
            generator=generator,
        )
        v = torch.randn(
            (total, case.num_kv_heads, case.head_dim),
            dtype=self.dtype,
            device=self.device,
            generator=generator,
        )
        for layer in range(case.num_layers):
            self.append_kv(layer, k, v, meta)
        torch.cuda.synchronize()
        return meta

    def gather_kv(self, layer, rows):
        """Read historical KV through the rows the page table gives, moving only."""
        k_flat = self.k_cache[layer].reshape(-1, self.num_heads, self.head_dim)
        v_flat = self.v_cache[layer].reshape(-1, self.num_heads, self.head_dim)
        return k_flat.index_select(0, rows), v_flat.index_select(0, rows)


class PagedAttentionRunner:
    """Read historical KV through FlashInfer's paged attention kernels."""

    def __init__(self, case: KernelCase, workspace):
        self.case = case
        self.workspace = workspace
        self.dtype = getattr(torch, case.dtype)
        if case.mode == "prefill":
            self.wrapper = flashinfer.BatchPrefillWithPagedKVCacheWrapper(
                workspace, "NHD", backend=FLASHINFER_BACKEND
            )
        else:
            self.wrapper = flashinfer.BatchDecodeWithPagedKVCacheWrapper(
                workspace, "NHD", use_tensor_cores=True
            )

    def plan(self, meta):
        case = self.case
        if case.mode == "prefill":
            self.wrapper.plan(
                qo_indptr=meta.qo_indptr,
                paged_kv_indptr=meta.paged_kv_indptr,
                paged_kv_indices=meta.paged_kv_indices,
                paged_kv_last_page_len=meta.paged_kv_last_page_len,
                num_qo_heads=case.num_qo_heads,
                num_kv_heads=case.num_kv_heads,
                head_dim_qk=case.head_dim,
                page_size=case.page_size,
                causal=True,
                q_data_type=self.dtype,
            )
        else:
            self.wrapper.plan(
                indptr=meta.paged_kv_indptr,
                indices=meta.paged_kv_indices,
                last_page_len=meta.paged_kv_last_page_len,
                num_qo_heads=case.num_qo_heads,
                num_kv_heads=case.num_kv_heads,
                head_dim=case.head_dim,
                page_size=case.page_size,
                q_data_type=self.dtype,
                kv_data_type=self.dtype,
            )

    def run(self, q, layer_cache):
        return self.wrapper.run(q, layer_cache)


def make_workspace(device, megabytes=256):
    return torch.empty(megabytes * 1024 * 1024, dtype=torch.uint8, device=device)


def _rows_for_sequence(cache, page_table_row, case, device):
    physical = page_table_row.to(torch.int64)
    offsets = torch.arange(case.page_size, dtype=torch.int64, device=device)
    rows = (physical.unsqueeze(1) * case.page_size + offsets).reshape(-1)
    flat = cache.reshape(-1, case.num_kv_heads, case.head_dim)
    return flat.index_select(0, rows)


def reference_attention(q, k_cache, v_cache, meta, case, layer=0):
    """Reference attention over KV gathered through the page table, using plain
    tensor ops. Only for the correctness comparison on short sequences; it is
    not a performance measurement."""
    batch = case.batch_size
    total = case.new_tokens
    per_seq = total // batch
    scale = 1.0 / (case.head_dim**0.5)
    repeat = case.num_qo_heads // case.num_kv_heads
    outputs = []
    for index in range(batch):
        k_seq = _rows_for_sequence(
            k_cache[layer], meta.page_table[index], case, q.device
        )
        v_seq = _rows_for_sequence(
            v_cache[layer], meta.page_table[index], case, q.device
        )
        kv_len = case.seq_len if case.mode == "prefill" else case.seq_len + 1
        k_seq = k_seq[:kv_len].transpose(0, 1).repeat_interleave(repeat, dim=0)
        v_seq = v_seq[:kv_len].transpose(0, 1).repeat_interleave(repeat, dim=0)
        q_seq = q[index * per_seq : (index + 1) * per_seq].transpose(0, 1)
        scores = torch.matmul(q_seq.float(), k_seq.float().transpose(-1, -2)) * scale
        if case.mode == "prefill":
            causal = torch.ones(
                (per_seq, kv_len), dtype=torch.bool, device=q.device
            ).tril(diagonal=kv_len - per_seq)
            scores = scores.masked_fill(~causal, float("-inf"))
        weights = torch.softmax(scores, dim=-1)
        outputs.append(torch.matmul(weights, v_seq.float()).transpose(0, 1))
    return torch.cat(outputs).to(q.dtype)


def verify_scatter(case, device, layer=0):
    """Check that the KV cache holds exactly what was scattered into it."""
    store = PagedKVCache(case, device)
    meta = store.build_metadata()
    generator = torch.Generator(device=device)
    generator.manual_seed(1234)
    k = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    v = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    store.append_kv(layer, k, v, meta)
    torch.cuda.synchronize()

    flat_k = store.k_cache[layer].reshape(-1, case.num_kv_heads, case.head_dim)
    flat_v = store.v_cache[layer].reshape(-1, case.num_kv_heads, case.head_dim)
    rows = meta.slot_mapping // (case.num_kv_heads * case.head_dim)
    read_k = flat_k.index_select(0, rows)
    read_v = flat_v.index_select(0, rows)

    k_diff = (read_k.float() - k.float()).abs().max().item()
    v_diff = (read_v.float() - v.float()).abs().max().item()

    written = torch.zeros(flat_k.shape[0], dtype=torch.bool, device=device)
    written[rows] = True
    untouched = flat_k[~written]
    untouched_max = untouched.abs().max().item() if untouched.numel() else 0.0

    return {
        "k_max_abs_diff": k_diff,
        "v_max_abs_diff": v_diff,
        "untouched_max_abs": untouched_max,
        "passed": bool(k_diff == 0.0 and v_diff == 0.0 and untouched_max == 0.0),
    }


def verify_attention(case, device, layer=0):
    """Check the paged attention output against the reference implementation."""
    store = PagedKVCache(case, device)
    generator = torch.Generator(device=device)
    generator.manual_seed(4321)
    # Decode fills its history first so the reference comparison covers the real
    # historical read path
    store.prefill_history(generator)
    meta = store.build_metadata()
    runner = PagedAttentionRunner(case, make_workspace(device))
    runner.plan(meta)

    q = torch.randn(
        (case.new_tokens, case.num_qo_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    k = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    v = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    store.append_kv(layer, k, v, meta)
    torch.cuda.synchronize()

    reference = reference_attention(q, store.k_cache, store.v_cache, meta, case, layer)
    with torch.no_grad():
        produced = runner.run(q, (store.k_cache[layer], store.v_cache[layer]))
    torch.cuda.synchronize()

    diff = (produced.float() - reference.float()).abs()
    scale = max(reference.float().abs().max().item(), 1e-6)
    return {
        "max_abs_diff": diff.max().item(),
        "reference_abs_max": scale,
        "max_rel_diff": diff.max().item() / scale,
        "passed": bool(diff.max().item() / scale < 1e-2),
    }


def _short_case(case):
    return KernelCase(
        mode=case.mode,
        seq_len=min(case.seq_len, 128),
        batch_size=min(case.batch_size, 2),
        page_size=case.page_size,
        num_layers=1,
        num_qo_heads=case.num_qo_heads,
        num_kv_heads=case.num_kv_heads,
        head_dim=case.head_dim,
        dtype=case.dtype,
    )


def run_case(case, device, warmup=DEFAULT_WARMUP, timed=DEFAULT_TIMED, verify=True):
    """Run one kernel measurement point and return per-phase percentiles plus
    the derived figures."""
    store = PagedKVCache(case, device)
    runner = PagedAttentionRunner(case, make_workspace(device))
    generator = torch.Generator(device=device)
    generator.manual_seed(case.seq_len + case.batch_size)

    # Every layer reuses one set of q/k/v buffers, so the timing loop contains
    # no random number generation
    q = torch.randn(
        (case.new_tokens, case.num_qo_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    k = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )
    v = torch.randn(
        (case.new_tokens, case.num_kv_heads, case.head_dim),
        dtype=store.dtype,
        device=device,
        generator=generator,
    )

    # Decode fills 0..seq_len-1 first, then lets the timing loop append the new token
    store.prefill_history(generator)

    meta = store.build_metadata()
    for layer in range(case.num_layers):
        store.append_kv(layer, k, v, meta)
    runner.plan(meta)
    torch.cuda.synchronize()

    def one_iteration(store, runner, q, k, v):
        events = {
            name: (
                torch.cuda.Event(enable_timing=True),
                torch.cuda.Event(enable_timing=True),
            )
            for name in MEASURED_PHASES
        }

        events["metadata"][0].record()
        local_meta = store.build_metadata()
        rows = store.rows_for(local_meta.page_table)
        events["metadata"][1].record()

        events["kv_write"][0].record()
        for layer in range(case.num_layers):
            store.append_kv(layer, k, v, local_meta)
        events["kv_write"][1].record()

        events["kv_gather"][0].record()
        for layer in range(case.num_layers):
            store.gather_kv(layer, rows)
        events["kv_gather"][1].record()

        events["attention_plan"][0].record()
        runner.plan(local_meta)
        events["attention_plan"][1].record()

        events["attention"][0].record()
        for layer in range(case.num_layers):
            runner.run(q, (store.k_cache[layer], store.v_cache[layer]))
        events["attention"][1].record()

        torch.cuda.synchronize()
        measured = {
            name: events[name][0].elapsed_time(events[name][1])
            for name in MEASURED_PHASES
        }
        # The event window around build_metadata() already covers the host to
        # device copy it performs, so the copy is only reported as a breakdown of
        # that window and is never added to it
        measured["metadata_cpu"] = local_meta.metadata_cpu_ms
        measured["metadata_h2d"] = local_meta.metadata_h2d_ms
        return measured

    for _ in range(warmup):
        one_iteration(store, runner, q, k, v)

    samples = {name: [] for name in MEASURED_PHASES}
    samples["total_step"] = []
    samples["metadata_cpu"] = []
    samples["metadata_h2d"] = []
    for _ in range(timed):
        measured = one_iteration(store, runner, q, k, v)
        total = 0.0
        for name in STEP_PHASES:
            samples[name].append(measured[name])
            total += measured[name]
        for name in DIAGNOSTIC_PHASES:
            samples[name].append(measured[name])
        samples["total_step"].append(total)
        samples["metadata_cpu"].append(measured["metadata_cpu"])
        samples["metadata_h2d"].append(measured["metadata_h2d"])

    result = {
        "kind": "kernel",
        "case": case.as_dict(),
        "warmup": warmup,
        "timed": timed,
        "device_name": torch.cuda.get_device_name(device),
        "kv_cache_bytes_per_rank": store.bytes_per_rank,
        "num_pages_allocated": store.num_pages,
        "phases": {name: summarize(samples[name]) for name in samples},
        "per_layer_p50_ms": {
            name: summarize(samples[name])["p50"] / case.num_layers
            for name in MEASURED_PHASES + ("total_step",)
        },
    }

    write_ms = result["phases"]["kv_write"]["p50"]
    gather_ms = result["phases"]["kv_gather"]["p50"]
    attn_ms = result["phases"]["attention"]["p50"]
    plan_ms = result["phases"]["attention_plan"]["p50"]
    total_ms = result["phases"]["total_step"]["p50"]
    meta_ms = result["phases"]["metadata"]["p50"]

    # Bandwidth and arithmetic intensity use the bytes the kernel actually moves:
    # a paged read takes whole pages, padding included, and the valid-token count
    # would skew both
    moved_bytes = case.kv_bytes_read_moved()

    result["derived"] = {
        # The attention step is metadata + kv_write + attention_plan + attention;
        # the kv_gather probe is compared against it, never added to it, so the
        # per-token figures describe a step a forward pass could actually run
        "kv_write_tokens_per_s": case.new_tokens / (write_ms / 1000.0),
        "kv_write_effective_gbps": case.kv_bytes_written() / (write_ms / 1000.0) / 1e9,
        "kv_gather_effective_gbps": moved_bytes / (gather_ms / 1000.0) / 1e9,
        "kv_gather_valid_token_gbps": case.kv_bytes_read() / (gather_ms / 1000.0) / 1e9,
        "attention_effective_gbps": moved_bytes / (attn_ms / 1000.0) / 1e9,
        "attention_tflops": case.attention_flops() / (attn_ms / 1000.0) / 1e12,
        "attention_arithmetic_intensity": case.attention_flops() / moved_bytes,
        "attention_total_ms": plan_ms + attn_ms,
        "attention_plan_us_per_layer": plan_ms * 1000.0 / case.num_layers,
        "attention_run_us_per_layer": attn_ms * 1000.0 / case.num_layers,
        "kv_gather_share_of_attention_run": gather_ms / attn_ms,
        "kv_write_us_per_token": write_ms * 1000.0 / case.new_tokens,
        "kv_gather_us_per_context_token": gather_ms * 1000.0 / case.context_tokens,
        "attention_us_per_new_token": attn_ms * 1000.0 / case.new_tokens,
        "metadata_us_per_new_token": meta_ms * 1000.0 / case.new_tokens,
        "metadata_h2d_share_of_metadata": result["phases"]["metadata_h2d"]["p50"]
        / meta_ms,
        "total_us_per_new_token": total_ms * 1000.0 / case.new_tokens,
        "total_tokens_per_s": case.new_tokens / (total_ms / 1000.0),
        "attention_share_of_total": attn_ms / total_ms,
        "attention_plan_share_of_total": plan_ms / total_ms,
        "kv_write_share_of_total": write_ms / total_ms,
        "metadata_share_of_total": meta_ms / total_ms,
    }

    if verify:
        short = _short_case(case)
        result["correctness"] = {
            "scatter": verify_scatter(short, device),
            "attention": verify_attention(short, device),
        }
    else:
        result["correctness"] = {
            "scatter": "unavailable",
            "attention": "unavailable",
        }

    del store, runner, q, k, v
    torch.cuda.empty_cache()
    return result


def assert_tp_sharding(model_config, tp_size):
    """Check the sharding divides evenly and return the per-rank head counts.

    Both head counts have to divide by tp_size: with num_attention_heads not
    dividing, integer division silently drops heads and the measurement covers a
    shape the model does not have.
    """
    if model_config.is_mla:
        raise ValueError("the kernel benchmark only supports standard GQA, not MLA")
    if model_config.num_key_value_heads % tp_size != 0:
        raise ValueError(
            f"num_key_value_heads={model_config.num_key_value_heads} "
            f"is not divisible by tp_size={tp_size}"
        )
    if model_config.num_attention_heads % tp_size != 0:
        raise ValueError(
            f"num_attention_heads={model_config.num_attention_heads} "
            f"is not divisible by tp_size={tp_size}; integer division would "
            f"silently drop heads"
        )
    return (
        model_config.num_attention_heads // tp_size,
        model_config.num_key_value_heads // tp_size,
    )


def build_kernel_cases(
    model_config,
    tp_size,
    page_size,
    input_lens,
    batch_sizes=(1,),
    modes=("prefill", "decode"),
):
    """Build the kernel measurement points; head counts and dtype come from the
    model config."""
    dtype = canonical_torch_dtype(model_config.torch_dtype)
    if dtype not in FLASHINFER_KV_DTYPES:
        raise ValueError(
            f"the kernel benchmark's attention backend only supports "
            f"{FLASHINFER_KV_DTYPES}; the model declares {dtype}"
        )
    qo_heads_per_rank, kv_heads_per_rank = assert_tp_sharding(model_config, tp_size)
    cases = []
    for mode in modes:
        for seq_len in input_lens:
            for batch_size in batch_sizes:
                cases.append(
                    KernelCase(
                        mode=mode,
                        seq_len=seq_len,
                        batch_size=batch_size,
                        page_size=page_size,
                        num_layers=model_config.num_layers,
                        num_qo_heads=qo_heads_per_rank,
                        num_kv_heads=kv_heads_per_rank,
                        head_dim=model_config.head_dim,
                        dtype=dtype,
                    )
                )
    return cases


def cases_to_json(cases):
    return json.dumps([case.as_dict() for case in cases], indent=2)
