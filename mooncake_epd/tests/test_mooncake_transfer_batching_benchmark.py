from __future__ import annotations

import pytest

from mooncake_epd.scripts.benchmark_mooncake_transfer_batching import (
    _groups,
    _parse_positive_ints,
    _percentile,
)


def test_batching_benchmark_groups_cover_each_descriptor_once():
    groups = _groups(10, 4)

    assert [(group.start, group.stop) for group in groups] == [(0, 4), (4, 8), (8, 10)]


def test_batching_benchmark_parses_and_deduplicates_batch_sizes():
    assert _parse_positive_ints("64, 16,64, 32") == [16, 32, 64]
    with pytest.raises(ValueError, match="positive"):
        _parse_positive_ints("0,16")


def test_batching_benchmark_percentile_uses_nearest_rank():
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.50) == 2.0
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.95) == 4.0
    assert _percentile([], 0.95) is None
