from __future__ import annotations

from mooncake_epd.scripts.run_feature_handle_paired_benchmark import (
    _feature_request,
    _strip_feature_handles,
    build_paired_summary,
)


def test_strip_feature_handles_keeps_image_request_but_removes_control_plane_handles():
    request = {
        "model": "m",
        "messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,xx"}}]}],
        "metadata": {
            "workflow_id": "wf",
            "mooncake_epd_feature_handles": [{"uri": "file:///tmp/a.pt"}],
            "feature_handle_builder": {"source_mm_hash": "abc"},
        },
    }

    asset = _strip_feature_handles(request)
    feature = _feature_request(request)

    assert asset["messages"] == request["messages"]
    assert "mooncake_epd_feature_handles" not in asset["metadata"]
    assert "feature_handle_builder" not in asset["metadata"]
    assert asset["metadata"]["workflow_id"].endswith("-asset-bytes")
    assert feature["metadata"]["mooncake_epd_feature_handles"] == [{"uri": "file:///tmp/a.pt"}]
    assert feature["metadata"]["workflow_id"].endswith("-feature-handle")


def test_build_paired_summary_reports_skip_and_performance_delta():
    asset = {
        "result": {
            "status_code": 200,
            "routing_path": "EPD",
            "ttft_ms": 2000.0,
            "elapsed_ms": 2600.0,
            "finish_reason": "length",
            "response_text": "a brown room with a chair",
        },
        "goodput_rps": 0.4,
        "metric_summary": {
            "precomputed_hits": 0,
            "hidden_cache_vision_compute_ms_avg": 123.0,
        },
    }
    feature = {
        "result": {
            "status_code": 200,
            "routing_path": "EPD",
            "ttft_ms": 1500.0,
            "elapsed_ms": 2100.0,
            "finish_reason": "length",
            "response_text": "a brown room with a chair",
        },
        "goodput_rps": 0.5,
        "metric_summary": {
            "precomputed_hits": 1,
            "hidden_cache_vision_compute_ms_avg": 0.0,
            "hidden_cache_full_miss_batches": 0,
            "fallback_batches": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "backend_counts": {"peer_buffer_direct": 12},
        },
    }

    summary = build_paired_summary(asset, feature)

    assert summary["pass"] is True
    assert summary["ttft_delta_ms"] == -500.0
    assert summary["ttft_reduction_pct"] == 25.0
    assert abs(summary["goodput_gain_pct"] - 25.0) < 1e-9
    assert summary["text_metrics"]["normalized_exact_match"] is True
    assert summary["gates"]["feature_vision_compute_skipped"] is True
