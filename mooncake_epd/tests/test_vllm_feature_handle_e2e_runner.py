from __future__ import annotations

import pytest

from mooncake_epd.scripts.run_vllm_feature_handle_e2e import (
    summarize_feature_handle_metrics,
    validate_feature_handle_summary,
)


def _summary(*, precomputed_hits: int = 1, full_miss: int = 0, vision_ms: float = 0.0):
    metrics_payload = {
        "metrics": {
            "requests_total": 1,
            "requests_multimodal": 1,
            "mm_prefetch_attempted": 0,
            "mm_prefetch_completed": 0,
            "mm_hidden_cache_precomputed_image_embeds_hits": precomputed_hits,
            "mm_hidden_cache_errors": 0,
            "mm_hidden_cache_full_miss_batches": full_miss,
            "mm_hidden_cache_vision_compute_ms_avg": vision_ms,
            "fallback_batches": 0,
            "fallback_bytes": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "peer_buffer_batches": 3,
            "peer_buffer_bytes": 1024,
            "remote_transfer_backend_counts": {"peer_buffer_direct": 3},
        }
    }
    return {
        "response": {
            "status_code": 200,
            "headers": {"x-epd-routing-path": "EPD"},
            "response_content_len": 8,
        },
        "metrics": metrics_payload,
        "feature_handle_metric_summary": summarize_feature_handle_metrics(metrics_payload),
    }


def test_feature_handle_e2e_runner_validation_accepts_real_skip_metrics():
    validate_feature_handle_summary(_summary())


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"precomputed_hits": 0}, "precomputed"),
        ({"full_miss": 1}, "full miss"),
        ({"vision_ms": 12.5}, "vision_compute"),
    ],
)
def test_feature_handle_e2e_runner_rejects_vision_encoder_fallback(kwargs, expected):
    with pytest.raises(AssertionError, match=expected):
        validate_feature_handle_summary(_summary(**kwargs))
