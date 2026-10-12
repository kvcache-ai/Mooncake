# The windows a step is timed in, the components measured beside it, and the pure
# arithmetic the report quotes. No torch dependency, so dry runs, reporting and the
# tests of both work on a machine without a GPU.

# The step, in the order one forward pass runs it. layer_loop is the whole per-layer
# loop, timed as one window so the events that would break it down do not sit
# inside the step.
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


def percentile(samples, q):
    """Linearly interpolated percentile over a sequence of floats."""
    ordered = sorted(float(value) for value in samples)
    if not ordered:
        raise ValueError("cannot take a percentile of an empty sample")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize(samples):
    ordered = sorted(float(value) for value in samples)
    if not ordered:
        raise ValueError("cannot summarize an empty sample")
    return {
        "count": len(ordered),
        "mean": sum(ordered) / len(ordered),
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "min": ordered[0],
        "max": ordered[-1],
    }


def derive(case, phases, gather_bytes, read_bytes):
    """The figures the report quotes, divided by the step's own ledger.

    The rates are unique-payload normalisations: they divide the bytes one path
    reads, counted once per layer however many queries read them, by the window
    that path ran in. The repeated operand a query costs is in the FLOP count, not
    in those bytes, and what the memory system actually transfers is not observable
    without a profiler, so no figure here is a measured bandwidth. The page
    capacity is an allocation figure and is labelled as one.

    The write window is the latency of the step's `set_kv_buffer` calls, one per
    layer, in a window of its own. It is not a measurement of the bytes the pool
    transfers, which needs a profiler, so no rate is derived from it.

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
