# The measured attention step.
#
# The calls inside the timed windows are the ones this benchmark replays from
# SGLang: its per-request token table, its KV pool writer, its index kernel and the
# FlashInfer wrappers its FlashInfer backend issues. What this module adds is the
# CUDA timing and the sampling; the Ledger arithmetic lives in stats.py.

import zlib

import torch

from .cases import BRANCH_RAGGED_PREFIX_MERGE, DEFAULT_TIMED, DEFAULT_WARMUP
from .stats import PHASES, STEP_PHASES, derive, summarize


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
    measured["phase_sum"] = sum(measured[name] for name in STEP_PHASES)
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

    The checks read the state the last timed iteration left: the mapping in the
    token table and the KV the step wrote, taken before the component passes
    rewrite the KV and replan the wrappers. They see that state and not an earlier
    iteration's, so a write the step itself later overwrote is not what they
    inspect. The arithmetic check runs on a short case of the same shape instead,
    because the reference materialises a query by context score matrix.
    """
    from .sglang_replay import SglangStep

    if timed <= 0 or warmup < 0:
        raise ValueError("a case needs timed > 0 and warmup >= 0")
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
        correctness = {
            "indices": step.check_indices(timed_indices),
            "history": step.check_history(),
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
