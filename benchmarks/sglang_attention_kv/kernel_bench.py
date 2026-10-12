# The measured attention step.
#
# The calls inside the timed windows are the ones this benchmark replays from
# SGLang: its per-request token table, its KV pool writer, its index kernel and the
# FlashInfer wrappers its FlashInfer backend issues. What this module adds is the
# CUDA timing and the sampling; the ledger arithmetic lives in stats.py.

import zlib

import torch

from .cases import (
    BRANCH_RAGGED_PREFIX_MERGE,
    DEFAULT_KV_SOURCE_PROTOCOL,
    DEFAULT_TIMED,
    DEFAULT_WARMUP,
)
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


# Iterations of the host-ahead comparison pass: the step timed with the host free to
# run ahead of the device, so its windows hold the kernels they cover instead of the
# time the device spent idle waiting for the next launch. The difference against the
# primary windows is that idle time, which a step whose kernels are smaller than its
# host work is made of almost entirely.
HOST_AHEAD_ITERATIONS = 20

# Moves of the cached history measured for a step whose history Mooncake holds. The
# step's own iterations read the history the last of them left in the pool, so the
# content check that follows is a check on a move that really happened.
FETCH_ITERATIONS = 20


def _windows(events):
    """The windows of one iteration, read off its event pairs."""
    measured = {
        "indices": events["indices"][0].elapsed_time(events["indices"][1]),
        "attention_plan": events["plan"][0].elapsed_time(events["plan"][1]),
        "layer_loop": events["loop"][0].elapsed_time(events["loop"][1]),
        "step_window": events["step"][0].elapsed_time(events["step"][1]),
    }
    measured["phase_sum"] = sum(measured[name] for name in STEP_PHASES)
    return measured


def one_iteration(step, query, defer=None):
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
    left rather than against a rebuild, and the last layer's output comes back with
    them, so the arithmetic check can sample the output a timed iteration produced.

    With `defer` a list, the event pairs are appended to it and nothing
    synchronises here: the caller resolves them after a synchronise of its own,
    which lets it enqueue iteration after iteration without the device going idle
    between them. A window then holds the kernels it covers rather than the time
    the device waited for the next launch.
    """
    events = {
        "step": _event_pair(),
        "indices": _event_pair(),
        "plan": _event_pair(),
        "loop": _event_pair(),
    }

    events["step"][0].record()
    events["indices"][0].record()
    indices = step.build_indices()
    events["indices"][1].record()

    # SGLang plans once per step, in init_forward_metadata, before its layer loop.
    events["plan"][0].record()
    step.plan(indices)
    events["plan"][1].record()

    events["loop"][0].record()
    output = None
    for layer_id in range(step.case.num_layers):
        if step.reads_before_write:
            output = step.run_layer(layer_id, query)
            step.write_layer(layer_id, indices)
        else:
            step.write_layer(layer_id, indices)
            output = step.run_layer(layer_id, query)
    events["loop"][1].record()
    events["step"][1].record()

    if defer is not None:
        defer.append(events)
        return indices, None, output
    torch.cuda.synchronize()
    return indices, _windows(events), output


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


def one_fetch(step):
    """One Mooncake move of the cached history into the pool, in a window of its
    own. A step whose history is already in the pool moves nothing and reports no
    window rather than a window around nothing."""
    if step.history is None:
        return 0.0
    begin = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    begin.record()
    fetched = step.fetch_kv()
    end.record()
    torch.cuda.synchronize()
    step.fetched_bytes = fetched
    return begin.elapsed_time(end)


def run_case(
    case,
    device,
    warmup=DEFAULT_WARMUP,
    timed=DEFAULT_TIMED,
    seed=0,
    extend_branch=None,
    kv_protocol=DEFAULT_KV_SOURCE_PROTOCOL,
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
            case,
            device,
            seed=_case_seed(case, seed),
            extend_branch=extend_branch,
            kv_protocol=kv_protocol,
        )
        step.prepare()
        # A step whose history Mooncake holds starts with the pool empty, so one move
        # happens before anything reads it: the warmup and every iteration below then
        # read the history the way a step whose history was in the pool all along
        # does. Its window is measured with the other moves, below.
        step.fetch_kv()

        query = step.make_query()

        for _ in range(warmup):
            one_iteration(step, query)

        samples = {name: [] for name in STEP_PHASES}
        samples["step_window"] = []
        samples["phase_sum"] = []
        timed_indices = None
        timed_output = None
        writes_before = step.write_calls
        fills_before = step.index_fills
        for _ in range(timed):
            timed_indices, measured, timed_output = one_iteration(step, query)
            for name in STEP_PHASES + ("step_window", "phase_sum"):
                samples[name].append(measured[name])

        # Every timed iteration writes one layer's K/V per layer and fills one index
        # stream. Without this the checks below could pass on the state the warmup
        # left, because every iteration writes the same content to the same slots.
        witness = {
            "write_calls": step.write_calls - writes_before,
            "index_fills": step.index_fills - fills_before,
            "expected_write_calls": timed * case.num_layers,
            "expected_index_fills": timed,
        }
        if (
            witness["write_calls"] != witness["expected_write_calls"]
            or witness["index_fills"] != witness["expected_index_fills"]
        ):
            raise RuntimeError(
                f"{case.label}: the timed loop made {witness['write_calls']} write "
                f"calls and {witness['index_fills']} index fills, expected "
                f"{witness['expected_write_calls']} and "
                f"{witness['expected_index_fills']}"
            )

        # The same step timed with the host free to run ahead. The primary windows
        # above include the device waiting for the next launch, which a step whose
        # kernels are small is made of almost entirely; the difference between the
        # two passes is that wait, so a reader can tell kernel time from idle time.
        host_ahead_events = []
        for _ in range(HOST_AHEAD_ITERATIONS):
            one_iteration(step, query, defer=host_ahead_events)
        torch.cuda.synchronize()
        host_ahead = [_windows(events) for events in host_ahead_events]

        correctness = {
            "indices": step.check_indices(timed_indices),
            "history": step.check_history(),
            "attention_rows": step.check_attention_rows(
                query, timed_output, case.num_layers - 1
            ),
            "attention": short_step_attention(case, device, seed, extend_branch),
        }

        # The Mooncake move of the history, in a window of its own beside the step
        # and before it: the pool the step's iterations read is the one the last of
        # these moves filled, so the content check below is a check on a move rather
        # than on a pool that was never filled. A step whose history the pool already
        # holds moves nothing and states no window.
        if step.history is None:
            samples["kv_fetch"] = [0.0] * FETCH_ITERATIONS
        else:
            samples["kv_fetch"] = [one_fetch(step) for _ in range(FETCH_ITERATIONS)]
        fetch_bytes = step.fetched_bytes if step.history is not None else 0

        # The write and the read are priced in passes of their own, after the checks,
        # each in its own window, so no step iteration is ever timed with a probe
        # inside it and no check reads a state a probe built. They run against a plan
        # of the indices they use, taken here rather than inside a window: the
        # wrappers' metadata has to describe the indices the component pass reads, and
        # planning is not part of what a component measures.
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
            "witness": witness,
            "host_ahead": {
                "iterations": HOST_AHEAD_ITERATIONS,
                "step_window_p50_ms": summarize(
                    [entry["step_window"] for entry in host_ahead]
                )["p50"],
                "layer_loop_p50_ms": summarize(
                    [entry["layer_loop"] for entry in host_ahead]
                )["p50"],
            },
            "device": str(device),
            "device_name": torch.cuda.get_device_name(device),
            "kv_cache_bytes_per_rank": step.kv_cache_bytes_per_rank,
            "num_pages_allocated": step.num_pages_allocated,
            "phases": {name: summarize(samples[name]) for name in PHASES},
        }
        gather_bytes = step.gather_bytes(probe_indices)
        result["gather_bytes"] = gather_bytes
        result["gather_rows"] = int(step.gather_rows(probe_indices).numel())
        result["kv_bytes_fetched"] = fetch_bytes
        result["derived"] = derive(
            case, result["phases"], gather_bytes, read_bytes["attention"], fetch_bytes
        )
        result["correctness"] = correctness
        # The history's buffers are registered with Mooncake's engine and the record
        # above names what it moved, so they are released here rather than left
        # registered until the process ends.
        step.close()
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
