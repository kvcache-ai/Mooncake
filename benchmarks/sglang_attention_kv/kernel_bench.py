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
# reads; the write is SGLang's own KV pool writer. layer_loop is the whole
# per-layer loop, timed as one window so the events that would break it down do
# not sit inside the step.
STEP_PHASES = ("indices", "attention_plan", "layer_loop")

# Passes of their own: the KV write and the attention read each run over all the
# layers in one window, as components rather than as the schedule the step runs,
# and the read-only gather probe over the same rows. None of them is added to the
# step.
DIAGNOSTIC_PHASES = ("kv_write_component", "attention_component", "kv_gather")

MEASURED_PHASES = STEP_PHASES + DIAGNOSTIC_PHASES
PHASES = MEASURED_PHASES + ("total_step",)

# The phases total_step adds up: the step's three windows, in order. The
# components are measured in passes of their own and are never added in.
TOTAL_PHASES = STEP_PHASES


def _case_seed(case, seed):
    """A per-case seed, so every case draws its own page layout and a rerun of
    the same matrix draws the same one. crc32 rather than hash(), which is
    randomised per process for strings."""
    return zlib.crc32(f"{case.label}|{seed}".encode()) % (2**31)


def _event_pair():
    return (
        torch.cuda.Event(enable_timing=True),
        torch.cuda.Event(enable_timing=True),
    )


def one_iteration(step, query):
    """One timed step in the order the branch runs it.

    The layer loop is timed as one window: a forward pass runs one layer's
    attention and one layer's KV write, layer by layer, in the order the branch
    puts them, and putting an event pair around each of those would measure the
    events as much as the work. The write and the read are priced separately, in
    passes of their own, as components.
    """
    indices_events = _event_pair()
    plan_events = _event_pair()
    loop_events = _event_pair()

    indices_events[0].record()
    indices = step.build_indices()
    indices_events[1].record()

    # SGLang plans once per step, in init_forward_metadata, before its layer loop.
    plan_events[0].record()
    step.plan(indices)
    plan_events[1].record()

    loop_events[0].record()
    for layer_id in range(step.case.num_layers):
        if step.reads_before_write:
            step.run_layer(layer_id, query)
            step.write_layer(layer_id, indices)
        else:
            step.write_layer(layer_id, indices)
            step.run_layer(layer_id, query)
    loop_events[1].record()

    torch.cuda.synchronize()
    return {
        "indices": indices_events[0].elapsed_time(indices_events[1]),
        "attention_plan": plan_events[0].elapsed_time(plan_events[1]),
        "layer_loop": loop_events[0].elapsed_time(loop_events[1]),
    }


def one_write_component(step, indices):
    """All the write calls of one step in one window, without the attention they
    interleave with in a real step."""
    begin, end = _event_pair()
    begin.record()
    step.write_kv(indices)
    end.record()
    torch.cuda.synchronize()
    return begin.elapsed_time(end)


def one_attention_component(step, query):
    """All the attention calls of one step in one window, without the writes."""
    begin, end = _event_pair()
    begin.record()
    step.read(query)
    end.record()
    torch.cuda.synchronize()
    return begin.elapsed_time(end)


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
        for name in STEP_PHASES:
            samples[name].append(measured[name])
        samples["total_step"].append(sum(measured[name] for name in TOTAL_PHASES))

    # The components and the gather run after the step loop, each in its own
    # windows, so no step iteration is ever timed with a probe inside it. They run
    # against a plan of the indices they use, taken here rather than inside a
    # window: the wrappers' metadata has to describe the indices the component pass
    # reads, and planning is not part of what a component measures.
    probe_indices = step.build_indices()
    step.plan(probe_indices)
    samples["kv_write_component"] = [
        one_write_component(step, probe_indices) for _ in range(timed)
    ]
    samples["attention_component"] = [
        one_attention_component(step, query) for _ in range(timed)
    ]
    samples["kv_gather"] = [one_gather(step, probe_indices) for _ in range(timed)]

    read_bytes = step.read_bytes()
    result = {
        "kind": "kernel",
        "case": case.as_dict(),
        "configuration": step.as_dict(),
        "read_bytes": read_bytes,
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
    gather_bytes = step.gather_bytes(probe_indices)
    result["gather_bytes"] = gather_bytes
    result["gather_rows"] = int(step.gather_rows(probe_indices).numel())
    result["derived"] = derive(
        case, result["phases"], gather_bytes, read_bytes["attention"]
    )
    result["correctness"] = {
        "indices": step.check_indices(probe_indices),
        "history": step.check_history(probe_indices),
        "attention": short_step_attention(case, device, seed, extend_branch),
    }
    # The next step builds its own pools; a step's pool and its tensors are the
    # largest allocations in the run, so release them before moving on.
    del step, query, probe_indices
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


def derive(case, phases, gather_bytes, read_bytes):
    """The figures the report quotes, divided by the step's own ledger.

    Bandwidth and the arithmetic rate divide logical bytes by the window those
    bytes belong to: the KV the branch's attention reads (the paged side's bytes
    plus the ragged side's) and the rows the probe moves. The page capacity is an
    allocation figure and is labelled as one; what the memory system actually
    transfers is not observable without a profiler, so no figure here claims it.

    The write window carries no rate. It reads two regimes for the same calls —
    around 0.1 ms and around 1.16 ms — so what it covers is not determined by the
    measurement, and a quotient of it would report whichever regime the window
    caught.

    The write and the attention are components, measured in passes of their own.
    Their share of the step is named as a component share, because the step
    interleaves the two per layer and their serial sum is not what it runs.
    """
    index_ms = phases["indices"]["p50"]
    plan_ms = phases["attention_plan"]["p50"]
    loop_ms = phases["layer_loop"]["p50"]
    write_ms = phases["kv_write_component"]["p50"]
    attention_ms = phases["attention_component"]["p50"]
    gather_ms = phases["kv_gather"]["p50"]
    step_ms = phases["total_step"]["p50"]

    flops = case.attention_flops()

    def per_second(milliseconds):
        return milliseconds / 1000.0

    return {
        # The plan runs once per step, so a per-layer figure for it is a
        # normalisation by the layer count and not a measured per-layer window.
        "attention_plan_us_per_step": plan_ms * 1000.0,
        "attention_component_us_per_layer": attention_ms * 1000.0 / case.num_layers,
        "kv_write_component_us_per_layer": write_ms * 1000.0 / case.num_layers,
        "indices_us_per_new_token": index_ms * 1000.0 / case.new_tokens,
        "attention_us_per_context_token": attention_ms * 1000.0 / case.context_tokens,
        "kv_gather_effective_gbps": gather_bytes / per_second(gather_ms) / 1e9,
        "attention_effective_gbps": read_bytes / per_second(attention_ms) / 1e9,
        "attention_tflops": flops / per_second(attention_ms) / 1e12,
        "attention_arithmetic_intensity": flops / read_bytes,
        "indices_share_of_step": index_ms / step_ms,
        "attention_plan_share_of_step": plan_ms / step_ms,
        "layer_loop_share_of_step": loop_ms / step_ms,
        "attention_component_share_of_step": attention_ms / step_ms,
        "kv_write_component_share_of_step": write_ms / step_ms,
        "components_share_of_step": (write_ms + attention_ms) / step_ms,
        "total_tokens_per_s": case.new_tokens / per_second(step_ms),
    }
