from __future__ import annotations

import pytest

from mooncake_epd.scripts.benchmark_encoder_cache_pressure import (
    _resolve_expected_hot_promotion_hits,
)


def test_expected_hot_promotion_hits_defaults_to_full_hit_phase() -> None:
    assert (
        _resolve_expected_hot_promotion_hits(
            hot_requests=8,
            hot_promotions=2,
            configured=None,
        )
        == 16
    )


def test_expected_hot_promotion_hits_allows_exact_capacity_control_count() -> None:
    assert (
        _resolve_expected_hot_promotion_hits(
            hot_requests=64,
            hot_promotions=1,
            configured=0,
        )
        == 0
    )


@pytest.mark.parametrize("configured", [-1, 17])
def test_expected_hot_promotion_hits_rejects_out_of_phase_count(
    configured: int,
) -> None:
    with pytest.raises(ValueError, match="within the measured phase"):
        _resolve_expected_hot_promotion_hits(
            hot_requests=8,
            hot_promotions=2,
            configured=configured,
        )
