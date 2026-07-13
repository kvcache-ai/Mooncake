from __future__ import annotations

import json
from types import SimpleNamespace

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
            "layered_receive_kv_requests": 1,
            "layered_receive_kv_worker_roundtrips": 1,
            "layered_receive_kv_worker_ms_avg": 7.5,
            "layered_receive_kv_first_group_count": 1,
            "layered_receive_kv_first_group_ms_avg": 2.5,
            "layered_receive_kv_finished_count": 1,
            "layered_receive_kv_finished_ms_avg": 6.5,
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


def test_online_direct_e2e_runner_validation_accepts_text_only_pd_path():
    summary = _summary()
    summary["text_only"] = True
    summary["response"]["headers"]["x-epd-routing-path"] = "PD"
    summary["metrics"]["metrics"]["requests_multimodal"] = 0
    summary["metrics"]["metrics"]["requests_text"] = 1
    summary["online_direct_metric_summary"] = summarize_online_direct_metrics(
        summary["metrics"],
        direct_buffer_stats={"allocations": 0, "bytes": 0},
    )

    validate_online_direct_summary(summary)


def test_text_only_request_builders_do_not_include_image_payload(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _write_online_request
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _write_request

    args = SimpleNamespace(
        text_only=True,
        image_url=None,
        demo_image="room",
        prompt="Explain KV transfer in one sentence.",
        model="qwen-vl",
        max_tokens=16,
        temperature=0.0,
        workflow_id="text-only",
    )
    online_path = tmp_path / "online.json"
    baseline_path = tmp_path / "baseline.json"
    _write_online_request(args, online_path)
    _write_request(args, baseline_path)

    online_content = json.loads(online_path.read_text(encoding="utf-8"))["messages"][0]["content"]
    baseline_content = json.loads(baseline_path.read_text(encoding="utf-8"))["messages"][0]["content"]
    assert online_content == baseline_content == [{"type": "text", "text": args.prompt}]


def test_paired_request_variation_breaks_prefix_cache_with_same_payload_shape(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import (
        _online_direct_entry,
        _write_online_request,
    )
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _baseline_entry

    args = SimpleNamespace(
        text_only=True,
        image_url=None,
        demo_image="room",
        prompt="Explain KV transfer in one sentence.",
        prompt_file=None,
        model="qwen-vl",
        max_tokens=16,
        temperature=0.0,
        workflow_id="text-only",
    )
    request_path = tmp_path / "request.json"
    _write_online_request(args, request_path)

    online = _online_direct_entry(
        request_path,
        repeat_idx=7,
        workflow_prefix="paired",
        request_variation="unique_prefix",
        phase="measure",
    )
    baseline = _baseline_entry(
        request_path,
        repeat_idx=7,
        workflow_prefix="paired",
        request_variation="unique_prefix",
        phase="measure",
    )
    online_text = online["request"]["messages"][0]["content"][0]["text"]
    baseline_text = baseline["request"]["messages"][0]["content"][0]["text"]

    assert online_text == baseline_text
    assert online_text.startswith("[Benchmark nonce: epd-bench-v1-measure-7")
    other = _online_direct_entry(
        request_path,
        repeat_idx=8,
        workflow_prefix="paired",
        request_variation="unique_prefix",
        phase="measure",
    )
    other_text = other["request"]["messages"][0]["content"][0]["text"]
    assert other_text != online_text


def test_prompt_file_is_used_by_both_paired_runners(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _write_online_request
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _write_request

    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("Long prompt from a dataset file.", encoding="utf-8")
    args = SimpleNamespace(
        text_only=True,
        image_url=None,
        demo_image="room",
        prompt="ignored inline prompt",
        prompt_file=str(prompt_file),
        model="qwen-vl",
        max_tokens=16,
        temperature=0.0,
        workflow_id="text-only",
    )
    online_path = tmp_path / "online.json"
    baseline_path = tmp_path / "baseline.json"
    _write_online_request(args, online_path)
    _write_request(args, baseline_path)

    online_text = json.loads(online_path.read_text(encoding="utf-8"))["messages"][0]["content"][0]["text"]
    baseline_text = json.loads(baseline_path.read_text(encoding="utf-8"))["messages"][0]["content"][0]["text"]
    assert online_text == baseline_text == "Long prompt from a dataset file."


def test_online_direct_e2e_runner_rejects_unreleased_prefill_buffers():
    with pytest.raises(AssertionError, match="not released"):
        validate_online_direct_summary(_summary(allocations=1))


def test_online_direct_e2e_runner_rejects_non_direct_backend():
    with pytest.raises(AssertionError, match="peer_buffer_direct"):
        validate_online_direct_summary(_summary(backend="batch_transfer_fallback"))


def test_stream_packet_timings_are_merged_into_result():
    from mooncake_epd.scripts.run_vllm_serving_e2e import _merge_packet_timings

    result = {"epd_timing_ms": {"prefill_ms": 12.0}}
    _merge_packet_timings(
        result,
        {
            "choices": [],
            "_mooncake_epd_proxy_timings_ms": {
                "decode_first_event_ms": "3.5",
                "decode_first_content_ms": 4,
            },
        },
    )
    assert result["epd_timing_ms"]["prefill_ms"] == 12.0
    assert result["epd_timing_ms"]["decode_first_event_ms"] == 3.5
    assert result["epd_timing_ms"]["decode_first_content_ms"] == 4.0
    assert "decode_first_content_ms" in result["epd_timing_ms_header"]


def test_metrics_settle_waits_for_a_quiescent_post_transfer_snapshot(monkeypatch):
    from mooncake_epd.scripts.run_vllm_serving_e2e import _wait_for_metrics_settle

    def payload(peer_bytes: int):
        return {
            "metrics": {
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 1,
                "layered_transfer_grouped_bytes": peer_bytes,
                "layered_transfer_grouped_descriptors": 1,
                "layered_receive_group_batches": 1,
                "layered_receive_finished_reqs": 1,
                "layered_receive_failures": 0,
                "layered_transfer_failed_batches": 0,
                "fallback_batches": 0,
                "fallback_bytes": 0,
                "peer_buffer_batches": 1,
                "peer_buffer_bytes": peer_bytes,
                "requests_total": 1,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 1},
                "connector_path_stats": {
                    "PD": {
                        "grouped_batches": 1,
                        "grouped_bytes": peer_bytes,
                        "grouped_descriptors": 1,
                        "peer_buffer_batches": 1,
                        "peer_buffer_bytes": peer_bytes,
                        "received_group_batches": 1,
                        "received_finished_reqs": 1,
                    }
                },
            }
        }

    class Response:
        def __init__(self, value):
            self.value = value

        def json(self):
            return self.value

    class Session:
        def __init__(self):
            self.values = [payload(1024), payload(2048), payload(2048)]
            self.calls = 0

        def get(self, *_args, **_kwargs):
            value = self.values[min(self.calls, len(self.values) - 1)]
            self.calls += 1
            return Response(value)

    session = Session()
    monkeypatch.setattr("mooncake_epd.scripts.run_vllm_serving_e2e.time.sleep", lambda *_: None)
    result = _wait_for_metrics_settle(
        session,
        "http://metrics",
        timeout_s=1.0,
        poll_s=0.0,
        stable_polls=2,
    )

    assert session.calls == 3
    assert result["metrics"]["peer_buffer_bytes"] == 2048


def test_online_direct_metric_summary_includes_receive_kv_breakdown():
    summary = _summary()["online_direct_metric_summary"]

    assert summary["layered_receive_kv_requests"] == 1
    assert summary["layered_receive_kv_worker_roundtrips"] == 1
    assert summary["layered_receive_kv_worker_ms_avg"] == 7.5
    assert summary["layered_receive_kv_first_group_count"] == 1
    assert summary["layered_receive_kv_first_group_ms_avg"] == 2.5
    assert summary["layered_receive_kv_finished_count"] == 1
    assert summary["layered_receive_kv_finished_ms_avg"] == 6.5


def test_online_direct_latency_stats_interpolate_percentiles():
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _latency_stats

    stats = _latency_stats([100.0, 200.0, 300.0, 400.0])

    assert stats["count"] == 4
    assert stats["avg"] == 250.0
    assert stats["p50"] == 250.0
    assert stats["p95"] == 385.0
    assert stats["max"] == 400.0


def test_single_baseline_latency_stats_match_online_runner_shape():
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _latency_stats as baseline_stats

    stats = baseline_stats([100.0, 200.0, 300.0, 400.0])

    assert set(stats) == {"count", "avg", "p50", "p95", "p99", "max"}
    assert stats["p95"] == 385.0


def test_merge_direct_buffer_stats_sums_multiple_prefill_workers():
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _merge_direct_buffer_stats

    merged = _merge_direct_buffer_stats(
        {
            "prefill-0": {"allocations": 1, "bytes": 10, "managed_buffers": 2, "persistent_cache": True},
            "prefill-1": {"allocations": 3, "bytes": 30, "managed_buffers": 4, "persistent_cache": True},
        }
    )

    assert merged["allocations"] == 4
    assert merged["bytes"] == 40
    assert merged["managed_buffers"] == 6
    assert merged["persistent_cache"] is True
    assert set(merged["workers"]) == {"prefill-0", "prefill-1"}


def test_warmup_concurrency_auto_covers_worker_fanout_and_measured_concurrency():
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _resolve_warmup_concurrency

    assert _resolve_warmup_concurrency(
        requested=0,
        effective_warmups=12,
        measured_concurrency=8,
        prefill_workers=2,
        decode_workers=4,
    ) == 8
    assert _resolve_warmup_concurrency(
        requested=0,
        effective_warmups=4,
        measured_concurrency=8,
        prefill_workers=2,
        decode_workers=4,
    ) == 4


def test_warmup_concurrency_explicit_value_is_respected_and_capped():
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _resolve_warmup_concurrency

    assert _resolve_warmup_concurrency(
        requested=2,
        effective_warmups=12,
        measured_concurrency=8,
        prefill_workers=2,
        decode_workers=4,
    ) == 2
    assert _resolve_warmup_concurrency(
        requested=99,
        effective_warmups=3,
        measured_concurrency=8,
        prefill_workers=2,
        decode_workers=4,
    ) == 3
    assert _resolve_warmup_concurrency(
        requested=0,
        effective_warmups=0,
        measured_concurrency=8,
        prefill_workers=2,
        decode_workers=4,
    ) == 0


def test_numeric_metric_delta_recomputes_window_averages():
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _numeric_metric_delta

    before = {
        "metrics": {
            "layered_receive_kv_worker_roundtrips": 1,
            "layered_receive_kv_worker_ms": 80.0,
            "layered_receive_kv_worker_ms_avg": 80.0,
            "decode_engine_kv_first_token_requests": 1,
            "decode_engine_kv_first_token_latency_ms_total": 700.0,
            "decode_engine_kv_first_token_latency_ms_avg": 700.0,
            "connector_metric_workers": 2,
            "connector_metrics_updated_at": 100.0,
            "mm_hidden_cache_entries": 4,
            "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
        }
    }
    after = {
        "metrics": {
            "layered_receive_kv_worker_roundtrips": 2,
            "layered_receive_kv_worker_ms": 170.0,
            "layered_receive_kv_worker_ms_avg": 85.0,
            "decode_engine_kv_first_token_requests": 2,
            "decode_engine_kv_first_token_latency_ms_total": 810.0,
            "decode_engine_kv_first_token_latency_ms_avg": 405.0,
            "connector_metric_workers": 2,
            "connector_metrics_updated_at": 105.0,
            "mm_hidden_cache_entries": 6,
            "remote_transfer_backend_counts": {"peer_buffer_direct": 5},
        }
    }

    delta = _numeric_metric_delta(after, before)["metrics"]

    assert delta["layered_receive_kv_worker_roundtrips"] == 1
    assert delta["layered_receive_kv_worker_ms"] == 90.0
    assert delta["layered_receive_kv_worker_ms_avg"] == 90.0
    assert delta["decode_engine_kv_first_token_requests"] == 1
    assert delta["decode_engine_kv_first_token_latency_ms_total"] == 110.0
    assert delta["decode_engine_kv_first_token_latency_ms_avg"] == 110.0
    assert delta["connector_metric_workers"] == 2
    assert delta["mm_hidden_cache_entries"] == 6
    assert "connector_metrics_updated_at" not in delta
    assert delta["remote_transfer_backend_counts"] == {"peer_buffer_direct": 3}
