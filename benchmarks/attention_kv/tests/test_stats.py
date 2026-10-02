import pytest

from benchmarks.attention_kv.stats import percentile, summarize


def test_percentile_single_sample():
    assert percentile([3.0], 0.5) == 3.0


def test_percentile_median_of_even_count():
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.5) == pytest.approx(2.5)


def test_percentile_extremes():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(values, 0.0) == 1.0
    assert percentile(values, 1.0) == 5.0


def test_percentile_interpolates():
    values = [0.0, 10.0]
    assert percentile(values, 0.95) == pytest.approx(9.5)


def test_percentile_rejects_empty():
    with pytest.raises(ValueError):
        percentile([], 0.5)


def test_summarize_reports_all_required_fields():
    stats = summarize([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
    assert stats["count"] == 10
    assert stats["p50"] == pytest.approx(5.5)
    assert stats["min"] == 1.0
    assert stats["max"] == 10.0
    assert stats["p50"] <= stats["p95"] <= stats["p99"]


def test_summarize_rejects_empty():
    with pytest.raises(ValueError):
        summarize([])
