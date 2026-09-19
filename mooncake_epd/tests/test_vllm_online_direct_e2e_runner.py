from __future__ import annotations

import pytest

from mooncake_epd.scripts.run_vllm_online_direct_e2e import (
    summarize_online_direct_metrics,
    validate_online_direct_summary,
)


def _summary(*, allocations: int = 0, backend: str = "peer_buffer_direct"):
    metrics_payload = {
        "metrics": {
            "requests_total": 1,
            "requests_multimodal": 1,
            "mm_hidden_cache_precomputed_image_embeds_hits": 1,
            "mm_hidden_cache_errors": 0,
            "mm_hidden_cache_full_miss_batches": 0,
            "mm_hidden_cache_vision_compute_ms_avg": 0.0,
            "fallback_batches": 0,
            "fallback_bytes": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "peer_buffer_batches": 4,
            "peer_buffer_bytes": 2048,
            "remote_transfer_backend_counts": {backend: 4},
        }
    }
    return {
        "response": {
            "status_code": 200,
            "headers": {"x-epd-routing-path": "EPD"},
            "response_content_len": 8,
        },
        "metrics": metrics_payload,
        "direct_buffer_stats_after_release": {"allocations": allocations, "bytes": 0},
        "online_direct_metric_summary": summarize_online_direct_metrics(
            metrics_payload,
            direct_buffer_stats={"allocations": allocations, "bytes": 0},
        ),
    }


def test_online_direct_e2e_runner_validation_accepts_strict_direct_path():
    validate_online_direct_summary(_summary())


def test_online_direct_e2e_runner_rejects_unreleased_prefill_buffers():
    with pytest.raises(AssertionError, match="not released"):
        validate_online_direct_summary(_summary(allocations=1))


def test_online_direct_e2e_runner_rejects_non_direct_backend():
    with pytest.raises(AssertionError, match="peer_buffer_direct"):
        validate_online_direct_summary(_summary(backend="batch_transfer_fallback"))
