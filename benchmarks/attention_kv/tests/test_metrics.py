from benchmarks.attention_kv.metrics import (
    diff_counters,
    parse_prometheus,
    split_metric,
    sum_metric,
)

SAMPLE = """# HELP sglang:cache_hit_rate The prefix cache hit rate.
# TYPE sglang:cache_hit_rate gauge
sglang:cache_hit_rate{engine_type="unified",tp_rank="0"} 0.5
sglang:prompt_tokens_total{engine_type="unified",tp_rank="0"} 100.0
sglang:prompt_tokens_total{engine_type="unified",tp_rank="1"} 100.0
sglang:prefill_effective_tokens_total{engine_type="unified",mode="host_hit",tp_rank="0"} 30.0
sglang:prefill_effective_tokens_total{engine_type="unified",mode="host_hit",tp_rank="1"} 30.0
sglang:prefill_effective_tokens_total{engine_type="unified",mode="storage_hit",tp_rank="0"} 70.0
sglang:prefill_effective_tokens_total{engine_type="unified",mode="storage_hit",tp_rank="1"} 70.0
"""


def test_parse_prometheus_keeps_labels_in_name():
    values = parse_prometheus(SAMPLE)
    assert (
        values['sglang:prompt_tokens_total{engine_type="unified",tp_rank="0"}'] == 100.0
    )
    assert len(values) == 7


def test_split_metric_separates_base_and_labels():
    base, labels = split_metric('sglang:x{a="b",c="d"}')
    assert base == "sglang:x"
    assert labels == 'a="b",c="d"'
    assert split_metric("sglang:y") == ("sglang:y", "")


def test_sum_metric_aggregates_ranks_and_filters_labels():
    values = parse_prometheus(SAMPLE)
    assert sum_metric(values, "sglang:prompt_tokens_total") == 200.0

    host = sum_metric(values, "sglang:prefill_effective_tokens_total", ("host_hit",))
    storage = sum_metric(
        values, "sglang:prefill_effective_tokens_total", ("storage_hit",)
    )
    assert host == 60.0
    assert storage == 140.0
    assert host + storage == 200.0


def test_sum_metric_returns_none_when_absent():
    values = parse_prometheus(SAMPLE)
    assert sum_metric(values, "sglang:not_present") is None


def test_diff_counters_includes_labeled_counters():
    """Labelled counters have to reach the delta too, or statistics come out
    silently empty."""
    before = parse_prometheus(SAMPLE)
    after_text = SAMPLE.replace(
        'sglang:prompt_tokens_total{engine_type="unified",tp_rank="0"} 100.0',
        'sglang:prompt_tokens_total{engine_type="unified",tp_rank="0"} 150.0',
    )
    after = parse_prometheus(after_text)
    delta, appeared = diff_counters(before, after)
    assert (
        delta['sglang:prompt_tokens_total{engine_type="unified",tp_rank="0"}'] == 50.0
    )
    assert len(delta) == 1
    assert appeared == []


def test_diff_counters_counts_series_created_inside_the_window():
    """A series created inside the window starts at zero, so its delta is the
    current value."""
    before = parse_prometheus(SAMPLE)
    after_text = SAMPLE + (
        'sglang:hicache_storage_hit_tokens_total{tp_rank="0"} 40.0\n'
        'sglang:hicache_storage_hit_tokens_total{tp_rank="1"} 40.0\n'
    )
    after = parse_prometheus(after_text)
    delta, appeared = diff_counters(before, after)
    assert (
        sum(
            value
            for name, value in delta.items()
            if name.startswith("sglang:hicache_storage_hit_tokens_total")
        )
        == 80.0
    )
    assert len(appeared) == 2


def test_diff_counters_skips_gauges():
    before = parse_prometheus(SAMPLE)
    after_text = SAMPLE.replace(
        'sglang:cache_hit_rate{engine_type="unified",tp_rank="0"} 0.5',
        'sglang:cache_hit_rate{engine_type="unified",tp_rank="0"} 0.9',
    )
    after = parse_prometheus(after_text)
    delta, _ = diff_counters(before, after)
    assert all("cache_hit_rate" not in name for name in delta)
