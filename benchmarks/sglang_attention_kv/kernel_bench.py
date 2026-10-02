# The measured attention step.
#
# Every call inside the timed windows is SGLang's own: its per-request token
# table, its KV pool writer, its index kernel and the FlashInfer paged attention
# wrappers its FlashInfer backend issues. What this module adds is the timing, the
# checks that the KV read is the KV that was written, and the ledger the derived
# figures divide by.

import zlib

import torch

from .cases import DEFAULT_TIMED, DEFAULT_WARMUP
from .stats import summarize

# The step, in the order one forward pass runs it. The index mapping is the stage
# that turns the token table into the paged index tensors the attention kernel
# reads; the write is SGLang's own KV pool writer.
STEP_PHASES = ("indices", "kv_write", "attention_plan", "attention")

# A read-only probe over the same pages, run in a pass of its own so it never
# shares a window with the step. It is compared against the read inside the
# attention kernel and is never added to the step.
DIAGNOSTIC_PHASES = ("kv_gather",)

MEASURED_PHASES = STEP_PHASES + DIAGNOSTIC_PHASES
PHASES = MEASURED_PHASES + ("total_step",)


def _case_seed(case, seed):
    """A per-case seed, so every case draws its own page layout and a rerun of
    the same matrix draws the same one. crc32 rather than hash(), which is
    randomised per process for strings."""
    return zlib.crc32(f"{case.label}|{seed}".encode()) % (2**31)


def one_iteration(step, query):
    """One timed step, phase by phase."""
    events = {
        name: (
            torch.cuda.Event(enable_timing=True),
            torch.cuda.Event(enable_timing=True),
        )
        for name in MEASURED_PHASES
    }

    events["indices"][0].record()
    indices = step.build_indices()
    events["indices"][1].record()

    # SGLang plans once per step, in init_forward_metadata, before its layer loop
    # runs. The branch then decides what that loop does first: the two ragged
    # branches compute the attention and save the KV cache afterwards, the paged
    # branches write first.
    events["attention_plan"][0].record()
    step.plan(indices)
    events["attention_plan"][1].record()

    if step.reads_before_write:
        events["attention"][0].record()
        step.read(query)
        events["attention"][1].record()
        events["kv_write"][0].record()
        step.write_kv(indices)
        events["kv_write"][1].record()
    else:
        events["kv_write"][0].record()
        step.write_kv(indices)
        events["kv_write"][1].record()
        events["attention"][0].record()
        step.read(query)
        events["attention"][1].record()

    torch.cuda.synchronize()
    return {name: events[name][0].elapsed_time(events[name][1]) for name in STEP_PHASES}


def one_gather(step, indices):
    """One read-only probe, in a window of its own."""
    begin = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    begin.record()
    step.gather(indices)
    end.record()
    torch.cuda.synchronize()
    return begin.elapsed_time(end)


def run_case(
    case, device, warmup=DEFAULT_WARMUP, timed=DEFAULT_TIMED, seed=0, extend_branch=None
):
    """Measure one step and return per-phase percentiles plus the derived
    figures."""
    from .sglang_replay import BRANCH_RAGGED_PREFIX_MERGE, SglangStep

    if extend_branch is None:
        extend_branch = BRANCH_RAGGED_PREFIX_MERGE
    step = SglangStep(
        case, device, seed=_case_seed(case, seed), extend_branch=extend_branch
    )
    step.prepare()

    query = step.make_query()

    for _ in range(warmup):
        one_iteration(step, query)

    samples = {name: [] for name in STEP_PHASES}
    samples["total_step"] = []
    for _ in range(timed):
        measured = one_iteration(step, query)
        total = 0.0
        for name in STEP_PHASES:
            samples[name].append(measured[name])
            total += measured[name]
        samples["total_step"].append(total)

    # The gather runs after the step loop, in its own windows, so the step is
    # never timed with a probe inside it
    gather_indices = step.build_indices()
    samples["kv_gather"] = [one_gather(step, gather_indices) for _ in range(timed)]

    result = {
        "kind": "kernel",
        "case": case.as_dict(),
        "configuration": step.as_dict(),
        "warmup": warmup,
        "timed": timed,
        "device_name": torch.cuda.get_device_name(device),
        "kv_cache_bytes_per_rank": step.kv_cache_bytes_per_rank,
        "num_pages_allocated": step.num_pages_allocated,
        "phases": {name: summarize(samples[name]) for name in PHASES},
        "per_layer_p50_ms": {
            name: summarize(samples[name])["p50"] / case.num_layers
            for name in STEP_PHASES + ("total_step",)
        },
    }
    gather_bytes = step.gather_bytes(gather_indices)
    result["gather_bytes"] = gather_bytes
    result["gather_rows"] = int(step.gather_rows(gather_indices).numel())
    result["derived"] = derive(case, result["phases"], gather_bytes)
    result["correctness"] = {
        "indices": step.check_indices(gather_indices),
        "history": step.check_history(gather_indices),
        "attention": short_step_attention(case, device, seed, extend_branch),
    }
    # The next step builds its own pools; a step's pool and its tensors are the
    # largest allocations in the run, so release them before moving on.
    del step, query, gather_indices
    torch.cuda.empty_cache()
    return result


def short_step_attention(case, device, seed, extend_branch):
    """The arithmetic reference on a short step of the same shape: a full-size
    reference would materialise a q_len by kv_len score matrix per head, which a
    long context cannot fit. The mapping and content checks run at full size."""
    from .cases import short_case
    from .sglang_replay import SglangStep

    short = short_case(case)
    check_step = SglangStep(
        short, device, seed=_case_seed(short, seed), extend_branch=extend_branch
    )
    check_step.prepare()
    indices = check_step.build_indices()
    query = check_step.make_query()
    check_step.plan(indices)
    if check_step.reads_before_write:
        check_step.read(query)
        check_step.write_kv(indices)
    else:
        check_step.write_kv(indices)
        check_step.read(query)
    result = check_step.check_attention(query)
    result["case"] = short.label
    return result


def derive(case, phases, gather_bytes):
    """The figures the report quotes, divided by the step's own ledger."""
    index_ms = phases["indices"]["p50"]
    write_ms = phases["kv_write"]["p50"]
    plan_ms = phases["attention_plan"]["p50"]
    attention_ms = phases["attention"]["p50"]
    gather_ms = phases["kv_gather"]["p50"]
    step_ms = phases["total_step"]["p50"]

    written = case.kv_bytes_written()
    read_pages = case.kv_bytes_read_pages()
    read_valid = case.kv_bytes_read_valid()
    flops = case.attention_flops()

    def per_second(milliseconds):
        return milliseconds / 1000.0

    return {
        "attention_plan_us_per_layer": plan_ms * 1000.0 / case.num_layers,
        "attention_run_us_per_layer": attention_ms * 1000.0 / case.num_layers,
        "kv_write_us_per_new_token": write_ms * 1000.0 / case.new_tokens,
        "indices_us_per_new_token": index_ms * 1000.0 / case.new_tokens,
        "attention_us_per_context_token": attention_ms * 1000.0 / case.context_tokens,
        "kv_write_effective_gbps": written / per_second(write_ms) / 1e9,
        # The probe's own ledger: both tensors of the rows it reads, which is what
        # it actually moves, rather than the kernel's paged read.
        "kv_gather_effective_gbps": gather_bytes / per_second(gather_ms) / 1e9,
        "attention_effective_gbps": read_pages / per_second(attention_ms) / 1e9,
        "attention_valid_token_gbps": read_valid / per_second(attention_ms) / 1e9,
        "attention_tflops": flops / per_second(attention_ms) / 1e12,
        "attention_arithmetic_intensity": flops / read_pages,
        "attention_share_of_step": attention_ms / step_ms,
        "kv_write_share_of_step": write_ms / step_ms,
        "indices_share_of_step": index_ms / step_ms,
        "attention_plan_share_of_step": plan_ms / step_ms,
        "kv_gather_share_of_attention_run": gather_ms / attention_ms,
        "total_tokens_per_s": case.new_tokens / per_second(step_ms),
    }
