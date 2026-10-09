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
# step_window is one window over the whole step; phase_sum is the sum of the three
# windows above. step_window minus phase_sum is the between-window interval, which
# the three windows do not cover; the measurement does not say what it holds.
PHASES = MEASURED_PHASES + ("step_window", "phase_sum")

# What the derived ratios divide by, and what phase_sum adds up.
TOTAL_PHASES = STEP_PHASES


def _case_seed(case, seed):
    """A per-case seed, so every case draws its own page layout and a rerun of
    the same matrix draws the same one. crc32 rather than hash(), which is
    randomised per process for strings."""
    return zlib.crc32(f"{case.label}|{seed}".encode()) % (2**31)


def _event_pair():
    """Two events on the current device. Every caller runs inside the device
    context run_case holds, so the events, the synchronise that follows them and
    the step's own tensors are on the same device."""
    return (
        torch.cuda.Event(enable_timing=True),
        torch.cuda.Event(enable_timing=True),
    )


def one_iteration(step, query):
    """One timed step in the order the branch runs it, and the indices it read.

    The layer loop is timed as one window: a forward pass runs one layer's
    attention and one layer's KV write, layer by layer, in the order the branch
    puts them, and putting an event pair around each of those would measure the
    events as much as the work. The write and the read are priced separately, in
    passes of their own, as components.

    A pair around all three windows gives step_window, the step as one span. The
    three windows sum to phase_sum, which is below it by the between-window
    interval: what the events of two windows do not cover. The indices come back
    with the measurement, so the checks can run against the state this iteration
    left rather than against a rebuild.
    """
    step_events = _event_pair()
    indices_events = _event_pair()
    plan_events = _event_pair()
    loop_events = _event_pair()

    step_events[0].record()
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
    step_events[1].record()

    torch.cuda.synchronize()
    measured = {
        "indices": indices_events[0].elapsed_time(indices_events[1]),
        "attention_plan": plan_events[0].elapsed_time(plan_events[1]),
        "layer_loop": loop_events[0].elapsed_time(loop_events[1]),
        "step_window": step_events[0].elapsed_time(step_events[1]),
    }
    measured["phase_sum"] = sum(measured[name] for name in TOTAL_PHASES)
    return indices, measured


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
    figures.

    The whole case runs inside `torch.cuda.device(device)`: the step's tensors are
    built on that device, and `Event.record()`, `torch.cuda.synchronize()` and the
    wrapper workspaces all act on the process's current device. A case on `cuda:1`
    measured while the current device is left on `cuda:0` would record its windows
    on device 0 and return from its synchronise without waiting for device 1, so
    the windows would be read off a timeline the case had not finished running.

    The checks run on the state the timed loop left, before the component passes
    rewrite the KV and replan the wrappers, so a step that wrote to the wrong
    slots or read through a stale page table fails on its own iteration and not on
    a state the diagnostics rebuilt.
    """
    from .sglang_replay import BRANCH_RAGGED_PREFIX_MERGE, SglangStep

    if extend_branch is None:
        extend_branch = BRANCH_RAGGED_PREFIX_MERGE
    with torch.cuda.device(device):
        step = SglangStep(
            case, device, seed=_case_seed(case, seed), extend_branch=extend_branch
        )
        step.prepare()

        query = step.make_query()

        for _ in range(warmup):
            one_iteration(step, query)

        samples = {name: [] for name in STEP_PHASES}
        samples["step_window"] = []
        samples["phase_sum"] = []
        timed_indices = None
        for _ in range(timed):
            timed_indices, measured = one_iteration(step, query)
            for name in STEP_PHASES + ("step_window", "phase_sum"):
                samples[name].append(measured[name])
        if timed_indices is None:
            # No timed iteration to check against, so the checks read a state built
            # for them; every run that measures anything has at least one.
            timed_indices = step.build_indices()

        correctness = {
            "indices": step.check_indices(timed_indices),
            "history": step.check_history(timed_indices),
            "attention": short_step_attention(case, device, seed, extend_branch),
        }

        # The components and the gather run after the checks, each in its own
        # windows, so no step iteration is ever timed with a probe inside it and no
        # check reads a state a probe built. They run against a plan of the indices
        # they use, taken here rather than inside a window: the wrappers' metadata
        # has to describe the indices the component pass reads, and planning is not
        # part of what a component measures.
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
            "device": str(device),
            "device_name": torch.cuda.get_device_name(device),
            "kv_cache_bytes_per_rank": step.kv_cache_bytes_per_rank,
            "num_pages_allocated": step.num_pages_allocated,
            "phases": {name: summarize(samples[name]) for name in PHASES},
            "per_layer_p50_ms": {
                name: summarize(samples[name])["p50"] / case.num_layers
                for name in STEP_PHASES + ("step_window",)
            },
        }
        gather_bytes = step.gather_bytes(probe_indices)
        result["gather_bytes"] = gather_bytes
        result["gather_rows"] = int(step.gather_rows(probe_indices).numel())
        result["derived"] = derive(
            case, result["phases"], gather_bytes, read_bytes["attention"]
        )
        result["correctness"] = correctness
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

    The rates are unique-payload normalisations: they divide the bytes one path
    reads, counted once per layer however many queries read them, by the window
    that path ran in. The repeated operand a query costs is in the FLOP count, not
    in those bytes, and what the memory system actually transfers is not observable
    without a profiler, so no figure here is a measured bandwidth. The page
    capacity is an allocation figure and is labelled as one.

    The write window carries no rate. It does not follow the bytes the step writes,
    so what it covers is not determined by the measurement and a quotient of it
    would report whatever the window caught.

    The ratios divide phase_sum, the sum of the step's three windows, and are named
    for it: they are not shares of a total that the parts add up to, and the whole
    step is measured separately as step_window.
    """
    index_ms = phases["indices"]["p50"]
    plan_ms = phases["attention_plan"]["p50"]
    loop_ms = phases["layer_loop"]["p50"]
    write_ms = phases["kv_write_component"]["p50"]
    attention_ms = phases["attention_component"]["p50"]
    gather_ms = phases["kv_gather"]["p50"]
    sum_ms = phases["phase_sum"]["p50"]
    window_ms = phases["step_window"]["p50"]

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
        "kv_gather_unique_payload_gbps": gather_bytes / per_second(gather_ms) / 1e9,
        "attention_unique_payload_gbps": read_bytes / per_second(attention_ms) / 1e9,
        "attention_tflops": flops / per_second(attention_ms) / 1e12,
        "attention_flops_per_unique_payload_byte": flops / read_bytes,
        "indices_ratio_of_phase_sum": index_ms / sum_ms,
        "attention_plan_ratio_of_phase_sum": plan_ms / sum_ms,
        "layer_loop_ratio_of_phase_sum": loop_ms / sum_ms,
        "attention_component_ratio_of_phase_sum": attention_ms / sum_ms,
        "kv_write_component_ratio_of_phase_sum": write_ms / sum_ms,
        "components_ratio_of_phase_sum": (write_ms + attention_ms) / sum_ms,
        "new_tokens_per_s": case.new_tokens / per_second(window_ms),
    }
