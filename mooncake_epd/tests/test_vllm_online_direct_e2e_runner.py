from __future__ import annotations

import json
import signal
import sys
from types import SimpleNamespace

import pytest

import mooncake_epd.scripts.run_vllm_online_direct_e2e as online_direct_runner
from mooncake_epd.scripts.run_vllm_online_direct_e2e import (
    _effective_direct_engine_protocol,
    _load_reference_response_summary,
    _measurement_quiescence_failures,
    _numeric_metric_delta,
    _record_runtime_transport_evidence,
    _summarize_deterministic_response_consistency,
    _summarize_direct_cache_timing,
    _summarize_proxy_wire_metrics,
    _summarize_rendered_prefill_cache_timing,
    _summarize_direct_feature_delivery,
    _summarize_reference_response_equivalence,
    _summarize_worker_dispatch_balance,
    _summarize_worker_dispatches,
    _run_online_direct_request,
    _validate_requested_transport_capabilities,
    _wait_for_measurement_purity,
    summarize_online_direct_metrics,
    validate_online_direct_summary,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (
    _dataset_warmup_coverage,
    _resolve_dataset_warmup_requests,
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
        "direct_buffer_stats_after_release": {
            "allocations": allocations,
            "bytes": 0,
            "persistent_cache": False,
            "ref_count": 0,
            "workers": {
                "prefill-0": {
                    "allocations": allocations,
                    "bytes": 0,
                    "persistent_cache": False,
                    "ref_count": 0,
                }
            },
        },
        "online_direct_metric_summary": summarize_online_direct_metrics(
            metrics_payload,
            direct_buffer_stats={"allocations": allocations, "bytes": 0},
        ),
    }


def _reference_benchmark_config():
    return {
        "schema_version": 1,
        "request_fingerprint": "request-fingerprint",
        "request": {
            "model": "Qwen3-VL-8B-Instruct",
            "max_tokens": 16,
            "temperature": 0.0,
            "prompt_source": "dataset_chat_split",
            "selected_samples": ["W3:0"],
        },
        "request_variation": {"mode": "none"},
        "vllm_prefix_cache": {"mode": "isolate"},
        "load": {
            "repeat_requests": 12,
            "concurrency": 4,
            "requested_warmup_requests": 8,
            "requested_warmup_concurrency": 0,
            "warmup_cover_dataset_cycle": True,
            "between_repeat_sleep_s": 0.0,
        },
        "serving": {
            "max_model_len": 2048,
            "gpu_memory_utilization": 0.55,
            "max_num_batched_tokens": 2048,
            "max_num_seqs": 16,
            "generation_config": "vllm",
            "tensor_parallel_size": 1,
        },
    }


def _write_reference_summary(tmp_path, *, config=None):
    path = tmp_path / "single_baseline_summary.json"
    path.write_text(
        json.dumps(
            {
                "mode": "single_vllm_baseline",
                "benchmark_config": config or _reference_benchmark_config(),
                "warmup_responses": [
                    {
                        "sample_id": "W3:0",
                        "status_code": 200,
                        "response_text": "known completion A",
                    }
                ],
                "responses": [
                    {
                        "sample_id": "W3:0",
                        "status_code": 200,
                        "response_text": "known completion B",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_online_direct_e2e_runner_validation_accepts_strict_direct_path():
    validate_online_direct_summary(_summary())


def test_online_runner_encoder_script_enables_checksum_on_encoder_and_prefill(tmp_path):
    """The E-stage descriptor must carry the checksum that P validates."""

    args = SimpleNamespace(
        model="test-model",
        encoder_device="cuda:0",
        encoder_dtype="bfloat16",
        encoder_family="auto",
        encoder_runtime="vllm_native",
        direct_source_mode="registered_tensor",
        encoder_request_timeout_s=30.0,
        feature_handle_checksum=True,
    )
    config = online_direct_runner.VLLMDisaggConfig(
        model="test-model",
        local_hostname="127.0.0.1",
        feature_handle_require_checksum=True,
    )
    script = online_direct_runner._write_encoder_script(
        workdir=tmp_path,
        cfg=config,
        encoder_port=8330,
        args=args,
        mooncake_json=tmp_path / "mooncake.json",
    )

    contents = script.read_text(encoding="utf-8")
    assert "--checksum --request-timeout-s 30.0" in contents
    assert "MOONCAKE_EPD_FEATURE_HANDLE_REQUIRE_CHECKSUM=1" in contents


def test_online_direct_runner_termination_handler_preserves_cleanup_path():
    with pytest.raises(SystemExit) as exc_info:
        online_direct_runner._termination_signal_to_system_exit(
            signal.SIGTERM,
            None,
        )

    assert exc_info.value.code == 128 + signal.SIGTERM


def test_online_direct_request_can_preserve_failed_response_for_concurrent_evidence(monkeypatch, tmp_path):
    def _failed_request(**_kwargs):
        return {
            "index": 3,
            "status_code": 502,
            "error": "HTTPError: 502 Server Error",
            "error_response_detail": "prefill request failed: ReadError",
    }

    monkeypatch.setattr(online_direct_runner, "_execute_dataset_request", _failed_request)
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps({"model": "test-model", "messages": [{"role": "user", "content": "hello"}]}),
        encoding="utf-8",
    )
    response = _run_online_direct_request(
        proxy_url="http://proxy.invalid/v1/chat/completions",
        request_path=request_path,
        repeat_idx=3,
        workflow_prefix="test",
        request_timeout=1.0,
        raise_on_failure=False,
    )

    assert response["online_direct_request_failed"] is True
    assert response["status_code"] == 502
    assert response["headers"]["x-epd-routing-path"] is None
    with pytest.raises(RuntimeError, match="online direct request failed"):
        _run_online_direct_request(
            proxy_url="http://proxy.invalid/v1/chat/completions",
            request_path=request_path,
            repeat_idx=3,
            workflow_prefix="test",
            request_timeout=1.0,
        )


def test_deterministic_response_consistency_records_hash_only_verdict():
    verdict = _summarize_deterministic_response_consistency(
        [
            {"sample_id": "sample-a", "response_text": "same completion"},
            {"sample_id": "sample-a", "response_text": "same completion"},
            {"sample_id": "sample-b", "response_text": "one-off completion"},
        ],
        required=True,
    )

    assert verdict["mode"] == "exact_response_sha256"
    assert verdict["applicable"] is True
    assert verdict["pass"] is True
    assert verdict["comparable_group_count"] == 1
    assert verdict["inconsistent_group_ids"] == []
    sample_hashes = verdict["groups"]["sample-a"]["hash_counts"]
    assert list(sample_hashes.values()) == [2]
    assert all(len(digest) == 64 for digest in sample_hashes)


def test_online_direct_validation_rejects_inconsistent_required_outputs():
    summary = _summary()
    summary["response_consistency"] = _summarize_deterministic_response_consistency(
        [
            {"sample_id": "sample-a", "response_text": "correct answer"},
            {"sample_id": "sample-a", "response_text": "corrupted answer"},
        ],
        required=True,
    )

    with pytest.raises(AssertionError, match="response-consistency failure"):
        validate_online_direct_summary(summary)


def test_online_direct_validation_rejects_warmup_only_consistency_failure():
    summary = _summary()
    summary["response_consistency"] = _summarize_deterministic_response_consistency(
        [
            {"sample_id": "sample-a", "response_text": "correct answer"},
            {"sample_id": "sample-a", "response_text": "correct answer"},
        ],
        required=True,
    )
    summary["all_response_consistency"] = _summarize_deterministic_response_consistency(
        [
            {"sample_id": "sample-a", "response_text": "correct answer"},
            {"sample_id": "sample-a", "response_text": "cold-path corruption"},
        ],
        required=True,
    )

    with pytest.raises(AssertionError, match="response-consistency failure"):
        validate_online_direct_summary(summary)


def test_reference_response_equivalence_accepts_known_concurrent_hashes(tmp_path):
    reference = _load_reference_response_summary(
        _write_reference_summary(tmp_path),
        benchmark_config=_reference_benchmark_config(),
    )

    verdict = _summarize_reference_response_equivalence(
        [
            {"sample_id": "W3:0", "response_text": "known completion B"},
            {"sample_id": "W3:0", "response_text": "known completion A"},
        ],
        reference=reference,
        required=True,
    )

    assert verdict["applicable"] is True
    assert verdict["pass"] is True
    assert verdict["unexpected_candidate_hashes_by_group"] == {}
    assert verdict["groups"]["W3:0"]["candidate_hashes"] == sorted(
        verdict["groups"]["W3:0"]["reference_hashes"]
    )
    assert "known completion" not in json.dumps(verdict)


def test_reference_response_equivalence_rejects_unknown_completion(tmp_path):
    reference = _load_reference_response_summary(
        _write_reference_summary(tmp_path),
        benchmark_config=_reference_benchmark_config(),
    )
    verdict = _summarize_reference_response_equivalence(
        [{"sample_id": "W3:0", "response_text": "unexpected completion"}],
        reference=reference,
        required=True,
    )
    summary = _summary()
    summary["reference_response_equivalence"] = verdict

    assert verdict["pass"] is False
    assert "W3:0" in verdict["unexpected_candidate_hashes_by_group"]
    with pytest.raises(AssertionError, match="reference response-equivalence failure"):
        validate_online_direct_summary(summary)


def test_reference_response_summary_rejects_incompatible_request_surface(tmp_path):
    incompatible = _reference_benchmark_config()
    incompatible["request_fingerprint"] = "different-request"

    with pytest.raises(ValueError, match="mismatched request_fingerprint"):
        _load_reference_response_summary(
            _write_reference_summary(tmp_path, config=incompatible),
            benchmark_config=_reference_benchmark_config(),
        )


def test_online_direct_metric_summary_preserves_direct_cache_snapshots():
    metrics_payload = {
        "direct_feature_handle_cache": {"hits": 2, "entries": 1},
        "direct_feature_singleflight": {"created": 1, "active": 0},
        "direct_feature_release": {"scheduled": 3, "completed": 3},
        "rendered_prefill_cache": {"hits": 5, "misses": 1, "entries": 1},
        "metrics": {
            "requests_total": 1,
        }
    }

    summary = summarize_online_direct_metrics(metrics_payload)

    assert summary["direct_feature_handle_cache"] == {"hits": 2, "entries": 1}
    assert summary["direct_feature_singleflight"] == {"created": 1, "active": 0}
    assert summary["direct_feature_release"] == {"scheduled": 3, "completed": 3}
    assert summary["rendered_prefill_cache"] == {
        "hits": 5,
        "misses": 1,
        "entries": 1,
    }


def test_online_direct_cache_timing_summary_counts_only_measured_cache_hits():
    summary = _summarize_direct_cache_timing(
        [
            {"epd_timing_ms": {"direct_cache_hits": 1, "direct_cache_lookup_ms": 2.5}},
            {"epd_timing_ms": {"direct_cache_hits": 2, "direct_cache_lookup_ms": 3.5}},
            {"epd_timing_ms": {}},
        ]
    )

    assert summary["direct_feature_cache_hits"] == 3
    assert summary["direct_feature_cache_hit_requests"] == 2
    assert summary["direct_feature_cache_lookup_ms"] == 6.0
    assert summary["direct_feature_cache_lookup_ms_avg"] == 3.0
    assert summary["direct_feature_cache_lookup_ms_stats"] == {
        "count": 2,
        "avg": 3.0,
        "p50": 3.0,
        "p95": 3.45,
        "p99": 3.49,
        "max": 3.5,
    }


def test_rendered_prefill_cache_timing_summary_keeps_measured_hits_separate():
    summary = _summarize_rendered_prefill_cache_timing(
        [
            {"epd_timing_ms": {"prefill_render_cache_hit": 1.0}},
            {"epd_timing_ms": {"prefill_render_cache_hit": 0.0}},
            {"epd_timing_ms": {}},
        ]
    )

    assert summary == {
        "rendered_prefill_cache_hit_requests": 1,
        "rendered_prefill_cache_miss_requests": 2,
    }


def test_proxy_wire_metrics_report_totals_averages_and_distribution():
    summary = _summarize_proxy_wire_metrics(
        [
            {
                "epd_timing_ms": {
                    "proxy_request_body_bytes": 100.0,
                    "prefill_generate_request_bytes": 900.0,
                    "decode_request_bytes": 120.0,
                }
            },
            {
                "epd_timing_ms": {
                    "proxy_request_body_bytes": 140.0,
                    "prefill_generate_request_bytes": 1100.0,
                    "decode_request_bytes": 160.0,
                }
            },
        ]
    )

    assert summary["proxy_request_body_bytes_total"] == 240.0
    assert summary["proxy_request_body_bytes_avg"] == 120.0
    assert summary["prefill_generate_request_bytes_avg"] == 1000.0
    assert summary["decode_request_bytes_stats"]["max"] == 160.0


def test_measurement_quiescence_reports_active_stage_and_release_work():
    failures = _measurement_quiescence_failures(
        {
            "workers": {
                "prefill": [
                    {
                        "worker_id": "prefill-0",
                        "current_load": 1,
                        "queue_size": 2,
                    }
                ],
                "decode": [],
            },
            "active_requests": ["request-1"],
            "direct_feature_release": {"inflight": 3},
            "mm_store": {"queue_size": 4, "inflight_events": 5},
        }
    )

    assert "prefill-0.current_load=1" in failures
    assert "prefill-0.queue_size=2" in failures
    assert "active_requests=1" in failures
    assert "direct_feature_release.inflight=3" in failures
    assert "mm_store.queue_size=4" in failures
    assert "mm_store.inflight_events=5" in failures


def test_measurement_purity_waits_for_compile_quiet_windows(tmp_path):
    log_path = tmp_path / "decode.log"
    log_path.write_text("ready\n", encoding="utf-8")
    settled = {
        "workers": {"prefill": [], "decode": []},
        "active_requests": [],
        "direct_feature_release": {"inflight": 0},
        "mm_store": {"queue_size": 0, "inflight_events": 0},
    }

    class _Response:
        def json(self):
            return settled

    class _Session:
        calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        "Triton kernel JIT compilation during inference: kernel\n"
                    )
            return _Response()

    evidence, metrics = _wait_for_measurement_purity(
        _Session(),
        "http://proxy/metrics",
        log_paths={"decode-0": log_path},
        initial_metrics=settled,
        timeout_s=1.0,
        poll_s=0.01,
        stable_polls=2,
    )

    assert evidence["pass"] is True
    assert evidence["polls"] == 3
    assert evidence["stable_polls"] == 2
    assert len(evidence["compile_events_after_warmup"]) == 1
    assert metrics == settled


def test_measured_metric_delta_subtracts_render_cache_counters_not_footprint():
    delta = _numeric_metric_delta(
        {
            "metrics": {"requests_total": 8},
            "rendered_prefill_cache": {
                "hits": 7,
                "misses": 1,
                "stores": 1,
                "entries": 1,
                "bytes": 4096,
            },
        },
        {
            "metrics": {"requests_total": 2},
            "rendered_prefill_cache": {
                "hits": 1,
                "misses": 1,
                "stores": 1,
                "entries": 1,
                "bytes": 4096,
            },
        },
    )

    assert delta["metrics"]["requests_total"] == 6
    assert delta["rendered_prefill_cache"] == {
        "hits": 6,
        "misses": 0,
        "stores": 0,
        "entries": 1,
        "bytes": 4096,
    }


def test_direct_feature_delivery_evidence_covers_publish_and_cache_hit_paths():
    summary = _summarize_direct_feature_delivery(
        [
            {
                "epd_timing_ms": {
                    "direct_describe_ms": 1.0,
                    "direct_allocate_ms": 2.0,
                    "direct_publish_ms": 3.0,
                    "direct_mark_ready_ms": 1.0,
                    "direct_publish_engine_nbytes": 4096.0,
                }
            },
            {"epd_timing_ms": {"direct_cache_hits": 1.0}},
            {"epd_timing_ms": {}},
        ]
    )

    assert summary == {
        "measured_requests": 3,
        "direct_publish_requests": 1,
        "direct_cache_hit_requests": 1,
        "direct_feature_delivery_requests": 2,
    }


def test_online_direct_validation_accepts_no_cache_delivery_evidence():
    summary = _summary()
    summary["online_direct_metric_summary"]["precomputed_hits"] = 0
    response = {
        "status_code": 200,
        "headers": {"x-epd-routing-path": "EPD"},
        "routing_path": "EPD",
        "response_content_len": 8,
    }
    summary["response"] = response
    summary["responses"] = [response]
    summary["repeat_requests"] = 1
    summary["direct_feature_delivery"] = {
        "measured_requests": 1,
        "direct_publish_requests": 1,
        "direct_cache_hit_requests": 0,
        "direct_feature_delivery_requests": 1,
    }

    validate_online_direct_summary(summary)


def test_online_direct_worker_dispatch_summary_uses_response_headers():
    summary = _summarize_worker_dispatches(
        [
            {"prefill_worker_id": "prefill-0", "decode_worker_id": "decode-0"},
            {"prefill_worker_id": "prefill-1", "decode_worker_id": "decode-0"},
            {"prefill_worker_id": "prefill-0", "decode_worker_id": "decode-1"},
            {},
        ]
    )

    assert summary == {
        "prefill_worker_dispatches": {"prefill-0": 2, "prefill-1": 1},
        "decode_worker_dispatches": {"decode-0": 2, "decode-1": 1},
    }


def test_online_direct_worker_dispatch_balance_includes_idle_configured_workers():
    balance = _summarize_worker_dispatch_balance(
        {
            "prefill_worker_dispatches": {"prefill-0": 20, "prefill-1": 12},
            "decode_worker_dispatches": {"decode-0": 32},
        },
        prefill_worker_ids=["prefill-0", "prefill-1"],
        decode_worker_ids=["decode-0", "decode-1"],
    )

    assert balance["prefill"]["counts"] == {"prefill-0": 20, "prefill-1": 12}
    assert balance["prefill"]["normalized_entropy"] < 1.0
    assert balance["decode"]["active_workers"] == 1
    assert balance["decode"]["max_share"] == 1.0


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


def test_baseline_startup_timeout_matches_paired_runner_contract():
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _startup_timeout_s

    # An explicit cold-start budget must not be truncated to the historical
    # fixed 300 s cap; the EPD and baseline launchers can now use the same
    # model-compilation allowance in a paired real-GPU run.
    assert _startup_timeout_s(SimpleNamespace(timeout=900.0, startup_timeout=600.0)) == 600.0
    assert _startup_timeout_s(SimpleNamespace(timeout=900.0, startup_timeout=None)) == 900.0
    assert _startup_timeout_s(SimpleNamespace(timeout=1.0, startup_timeout=1.0)) == 60.0


def test_dataset_warmup_cycle_coverage_primes_each_selected_shape():
    entries = [
        {"sample": {"sample_id": "W0:0"}},
        {"sample": {"sample_id": "W1:0"}},
        {"sample": {"sample_id": "W4:0"}},
    ]

    assert _resolve_dataset_warmup_requests(
        requested_warmups=1,
        dataset_entries=entries,
        cover_dataset_cycle=False,
    ) == 1
    assert _resolve_dataset_warmup_requests(
        requested_warmups=1,
        dataset_entries=entries,
        cover_dataset_cycle=True,
    ) == 3

    coverage = _dataset_warmup_coverage(
        entries,
        [
            {"sample_id": "W0:0", "status_code": 200},
            {"sample_id": "W1:0", "status_code": 200},
            {"sample_id": "W4:0", "status_code": 200},
        ],
        cover_dataset_cycle=True,
    )
    assert coverage == {
        "applicable": True,
        "coverage_requested": True,
        "selected_sample_count": 3,
        "warmed_sample_count": 3,
        "warmup_response_count": 3,
        "complete": True,
        "missing_sample_ids": [],
    }


def test_dataset_warmup_coverage_requires_successful_response():
    coverage = _dataset_warmup_coverage(
        [{"sample": {"sample_id": "W0:0"}}],
        [{"sample_id": "W0:0", "status_code": 503}],
        cover_dataset_cycle=True,
    )

    assert coverage["complete"] is False
    assert coverage["missing_sample_ids"] == ["W0:0"]


def test_online_runner_uses_in_memory_registry_unless_durability_is_requested(
    tmp_path, monkeypatch
):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"run-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    hot_path = capture([])
    assert hot_path.enable_workflow_registry_wal is False
    assert hot_path.workflow_registry_wal_path is None

    durable = capture(["--durable-workflow-registry"])
    assert durable.enable_workflow_registry_wal is True
    assert durable.workflow_registry_wal_path.endswith("proxy_workflow_registry.jsonl")


def test_online_runner_defaults_to_isolated_vllm_prefix_cache(monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    monkeypatch.setattr(
        sys,
        "argv",
        ["run_vllm_online_direct_e2e.py", "--text-only"],
    )

    assert runner.parse_args().vllm_prefix_cache_mode == "isolate"


def test_online_runner_forwards_selected_scheduler_policy(tmp_path, monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    captured = {}

    def fake_generate(_workdir, config):
        captured["config"] = config
        raise StopAfterConfig()

    monkeypatch.setattr(runner, "generate_configs", fake_generate)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_vllm_online_direct_e2e.py",
            "--workdir",
            str(tmp_path / "least-loaded"),
            "--text-only",
            "--scheduler-policy",
            "least_loaded",
        ],
    )

    with pytest.raises(StopAfterConfig):
        runner.run(runner.parse_args())

    assert captured["config"].scheduler_policy == "least_loaded"


def test_online_runner_requires_explicit_tcp_connection_pool_opt_in(tmp_path, monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"tcp-pool-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    assert capture([]).tcp_connection_pool is False
    assert capture(["--tcp-connection-pool"]).tcp_connection_pool is True


def test_online_runner_requires_explicit_prefill_http_keepalive_opt_in(
    tmp_path, monkeypatch
):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"prefill-http-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    assert capture([]).prefill_http_keepalive is False
    assert capture(["--prefill-http-keepalive"]).prefill_http_keepalive is True


def test_online_runner_forwards_bounded_direct_release_batch_size(tmp_path, monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"release-batch-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    assert capture([]).direct_feature_release_batch_max_jobs == 16
    assert (
        capture(["--direct-feature-release-batch-max-jobs", "3"])
        .direct_feature_release_batch_max_jobs
        == 3
    )


def test_online_runner_forwards_adaptive_direct_feature_lease_controls(
    tmp_path, monkeypatch
):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"adaptive-lease-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    default = capture([])
    assert default.direct_feature_adaptive_lease_prefetch is False
    assert default.direct_feature_adaptive_lease_prefetch_max == 2

    enabled = capture(
        [
            "--enable-direct-feature-adaptive-lease-prefetch",
            "--direct-feature-adaptive-lease-prefetch-max",
            "2",
        ]
    )
    assert enabled.direct_feature_adaptive_lease_prefetch is True
    assert enabled.direct_feature_adaptive_lease_prefetch_max == 2


def test_online_runner_only_enables_proxy_handle_cache_when_leases_are_retained(
    tmp_path, monkeypatch
):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class StopAfterConfig(Exception):
        pass

    def capture(extra_args):
        captured = {}

        def fake_generate(_workdir, config):
            captured["config"] = config
            raise StopAfterConfig()

        monkeypatch.setattr(runner, "generate_configs", fake_generate)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_vllm_online_direct_e2e.py",
                "--workdir",
                str(tmp_path / f"proxy-cache-{len(extra_args)}"),
                "--text-only",
                *extra_args,
            ],
        )
        with pytest.raises(StopAfterConfig):
            runner.run(runner.parse_args())
        return captured["config"]

    # Text-only mode never enables a multimodal FeatureHandle cache.
    released = capture(["--direct-proxy-handle-cache"])
    assert released.release_direct_feature_buffers_after_prefill is True
    assert released.enable_direct_feature_handle_cache is False

    # Exercise the same rule with a multimodal configuration by recording the
    # VLLM config before the generated services are launched.
    captured = {}

    def fake_generate(_workdir, config):
        captured["config"] = config
        raise StopAfterConfig()

    monkeypatch.setattr(runner, "generate_configs", fake_generate)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_vllm_online_direct_e2e.py",
            "--workdir",
            str(tmp_path / "proxy-cache-multimodal-release"),
            "--direct-proxy-handle-cache",
        ],
    )
    with pytest.raises(StopAfterConfig):
        runner.run(runner.parse_args())
    assert captured["config"].release_direct_feature_buffers_after_prefill is True
    assert captured["config"].enable_direct_feature_handle_cache is False

    retained = {}

    def fake_generate_retained(_workdir, config):
        retained["config"] = config
        raise StopAfterConfig()

    monkeypatch.setattr(runner, "generate_configs", fake_generate_retained)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_vllm_online_direct_e2e.py",
            "--workdir",
            str(tmp_path / "proxy-cache-multimodal-retained"),
            "--direct-proxy-handle-cache",
            "--no-release-direct-feature-buffers-after-prefill",
        ],
    )
    with pytest.raises(StopAfterConfig):
        runner.run(runner.parse_args())
    assert retained["config"].release_direct_feature_buffers_after_prefill is False
    assert retained["config"].enable_direct_feature_handle_cache is True


def test_online_runner_transport_preflight_uses_effective_direct_protocol(monkeypatch):
    import mooncake_epd.scripts.run_vllm_online_direct_e2e as runner

    observed = []
    monkeypatch.setattr(
        runner,
        "require_mooncake_protocol_support",
        lambda protocol: observed.append(protocol),
    )
    args = SimpleNamespace(
        mooncake_protocol="nvlink_intra",
        direct_engine_protocol=None,
        text_only=False,
    )

    _validate_requested_transport_capabilities(args)

    assert _effective_direct_engine_protocol(args) == "nvlink_intra"
    assert observed == ["nvlink_intra"]


def test_online_runner_transport_preflight_rejects_mixed_nvlink_protocols(monkeypatch):
    import mooncake_epd.scripts.run_vllm_online_direct_e2e as runner
    from mooncake_epd.core.transfer import MooncakeProtocolError

    monkeypatch.setattr(runner, "require_mooncake_protocol_support", lambda _protocol: None)
    args = SimpleNamespace(
        mooncake_protocol="tcp",
        direct_engine_protocol="nvlink_intra",
        text_only=False,
    )

    with pytest.raises(MooncakeProtocolError, match="process-scoped"):
        _validate_requested_transport_capabilities(args)


def test_online_runner_records_native_nvlink_worker_evidence(tmp_path):
    prefill_log = tmp_path / "prefill.log"
    decode_log = tmp_path / "decode.log"
    marker = "Using Intra-Node NVLink transport (MC_INTRANODE_NVLINK set)"
    prefill_log.write_text(marker, encoding="utf-8")
    decode_log.write_text(marker, encoding="utf-8")
    summary = {}

    _record_runtime_transport_evidence(
        summary,
        logs={"prefill_0": prefill_log, "decode_0": decode_log},
        requested="nvlink_intra",
    )

    assert summary["transport_runtime_evidence"]["pass"] is True
    assert summary["transport_runtime_evidence"]["actual_transport"] == "nvlink_intra"


def test_online_runner_accepts_native_tcp_listener_evidence(tmp_path):
    """TCP selected directly need not set MC_FORCE_TCP in the child worker."""

    from mooncake_epd.scripts.run_vllm_online_direct_e2e import (
        _record_runtime_transport_evidence,
    )

    prefill_log = tmp_path / "prefill.log"
    decode_log = tmp_path / "decode.log"
    marker = "I0714 tcp_transport.cpp:553] TcpTransport: listen on port 15008"
    prefill_log.write_text(marker, encoding="utf-8")
    decode_log.write_text(marker, encoding="utf-8")
    summary = {}

    _record_runtime_transport_evidence(
        summary,
        logs={"prefill_0": prefill_log, "decode_0": decode_log},
        requested="tcp",
    )

    evidence = summary["transport_runtime_evidence"]
    assert evidence["pass"] is True
    assert evidence["actual_transport"] == "tcp"


def test_online_runner_records_separate_encoder_to_prefill_native_evidence(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import (
        _direct_engine_transport_logs,
    )

    marker = "TcpTransport: listen on port 15008"
    encoder = tmp_path / "encoder.log"
    prefill = tmp_path / "prefill.log"
    decode = tmp_path / "decode.log"
    for path in (encoder, prefill, decode):
        path.write_text(marker, encoding="utf-8")
    logs = {"encoder": encoder, "prefill_0": prefill, "decode_0": decode}
    summary = {}

    _record_runtime_transport_evidence(
        summary,
        logs=logs,
        requested="tcp",
        evidence_key="direct_transport_runtime_evidence",
        worker_logs=_direct_engine_transport_logs(logs),
    )

    evidence = summary["direct_transport_runtime_evidence"]
    assert evidence["pass"] is True
    assert [worker["worker"] for worker in evidence["workers"]] == ["encoder", "prefill-0"]


def test_online_runner_rejects_native_transport_fallback_marker(tmp_path):
    prefill_log = tmp_path / "prefill.log"
    decode_log = tmp_path / "decode.log"
    marker = "Using Intra-Node NVLink transport (MC_INTRANODE_NVLINK set)"
    prefill_log.write_text(marker, encoding="utf-8")
    decode_log.write_text(
        marker + "\nMC_FORCE_TCP is set, using TCP transport only",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="native Mooncake transport evidence failed"):
        _record_runtime_transport_evidence(
            {},
            logs={"prefill_0": prefill_log, "decode_0": decode_log},
            requested="nvlink_intra",
        )


def test_multi_image_request_builders_keep_epd_and_baseline_workloads_identical(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _write_online_request
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _write_request

    args = SimpleNamespace(
        text_only=False,
        image_url=None,
        image_urls=["data:image/png;base64,first", "data:image/png;base64,second"],
        demo_image="room",
        prompt="Compare both images.",
        prompt_file=None,
        model="qwen-vl",
        max_tokens=16,
        temperature=0.0,
        workflow_id="multi-image",
    )
    online_path = tmp_path / "online-multi.json"
    baseline_path = tmp_path / "baseline-multi.json"
    _write_online_request(args, online_path)
    _write_request(args, baseline_path)

    online_content = json.loads(online_path.read_text(encoding="utf-8"))["messages"][0]["content"]
    baseline_content = json.loads(baseline_path.read_text(encoding="utf-8"))["messages"][0]["content"]
    assert online_content == baseline_content
    assert [item["image_url"]["url"] for item in online_content[:2]] == args.image_urls
    assert online_content[2] == {"type": "text", "text": args.prompt}


def test_multi_image_request_builders_reject_ambiguous_image_flags(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _write_online_request
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _write_request

    args = SimpleNamespace(
        text_only=False,
        image_url="data:image/png;base64,one",
        image_urls=["data:image/png;base64,two"],
        demo_image="room",
        prompt="ambiguous",
        prompt_file=None,
        model="qwen-vl",
        max_tokens=16,
        temperature=0.0,
        workflow_id="ambiguous-images",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        _write_online_request(args, tmp_path / "online-ambiguous.json")
    with pytest.raises(ValueError, match="mutually exclusive"):
        _write_request(args, tmp_path / "baseline-ambiguous.json")


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


def test_paired_vllm_prefix_cache_isolation_preserves_model_visible_prompt(tmp_path):
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
        request_variation="none",
        vllm_prefix_cache_mode="isolate",
        phase="measure",
    )
    baseline = _baseline_entry(
        request_path,
        repeat_idx=7,
        workflow_prefix="paired",
        request_variation="none",
        vllm_prefix_cache_mode="isolate",
        phase="measure",
    )

    assert online["request"] == baseline["request"]
    assert online["request"]["messages"][0]["content"][0]["text"] == args.prompt
    assert online["vllm_prefix_cache_id"] == "epd-vllm-cache-salt-v1-measure-7"
    assert len(online["request"]["cache_salt"]) == 64

    other = _online_direct_entry(
        request_path,
        repeat_idx=8,
        workflow_prefix="paired",
        request_variation="none",
        vllm_prefix_cache_mode="isolate",
        phase="measure",
    )
    assert other["request"]["messages"][0]["content"][0]["text"] == args.prompt
    assert other["request"]["cache_salt"] != online["request"]["cache_salt"]


def test_paired_dataset_replay_is_identical_and_preserves_source_entry(tmp_path):
    from mooncake_epd.scripts.run_vllm_online_direct_e2e import _online_direct_entry
    from mooncake_epd.scripts.run_vllm_serving_e2e import (
        _dataset_replay_context,
        _dataset_request_evidence_row,
    )
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _baseline_entry

    source_entry = {
        "family": "W1",
        "sample": {
            "sample_id": "docvqa-001",
            "workflow_id": "source-workflow-001",
            "source_dataset": "docvqa",
        },
        "request": {
            "model": "qwen-vl",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,AAAA"},
                        },
                        {"type": "text", "text": "Read this document."},
                    ],
                }
            ],
            "max_tokens": 32,
            "temperature": 0.0,
            "metadata": {
                "workflow_id": "source-workflow-001",
                "estimated_prompt_len": 384,
                "admission_method": "processor",
            },
        },
    }
    source_snapshot = json.loads(json.dumps(source_entry))

    online = _online_direct_entry(
        None,
        repeat_idx=3,
        workflow_prefix="paired-dataset",
        request_variation="unique_prefix",
        phase="measure",
        dataset_entries=[source_entry],
        dataset_workflow_id_mode="unique",
    )
    baseline = _baseline_entry(
        None,
        repeat_idx=3,
        workflow_prefix="paired-dataset",
        request_variation="unique_prefix",
        phase="measure",
        dataset_entries=[source_entry],
        dataset_workflow_id_mode="unique",
    )

    assert online["request"] == baseline["request"]
    assert online["sample"] == baseline["sample"]
    assert online["request"]["metadata"]["workflow_id"] == "paired-dataset-r3"
    assert online["sample"]["source_workflow_id"] == "source-workflow-001"
    assert online["dataset_cycle_index"] == 3
    assert source_entry == source_snapshot

    root = tmp_path / "dataset"
    split = root / "chat_splits" / "dev-small.jsonl"
    split.parent.mkdir(parents=True)
    split.write_text('{"sample_id":"docvqa-001"}\n', encoding="utf-8")
    context = _dataset_replay_context(
        [source_entry],
        dataset_root=str(root),
        chat_split="dev-small",
        families=["W1"],
        max_input_len=2048,
        request_max_tokens=32,
        skip_oversized=True,
        image_max_pixels=0,
        agent_pd_labels=False,
        workflow_id_mode="unique",
    )
    assert len(context["request_fingerprint"]) == 64
    assert context["selected_samples"][0]["sample_id"] == "docvqa-001"
    row = _dataset_request_evidence_row(online, index=3, phase="measure")
    assert row["source_workflow_id"] == "source-workflow-001"
    assert "base64" not in repr(row)


def test_dataset_replay_source_workflow_mode_is_explicit():
    from mooncake_epd.scripts.run_vllm_serving_e2e import _materialize_dataset_replay_entry

    source = {
        "family": "W2",
        "sample": {"sample_id": "sample", "workflow_id": "wf-source"},
        "request": {
            "model": "model",
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "continue"}]}
            ],
            "metadata": {"workflow_id": "wf-source"},
        },
    }
    warmup = _materialize_dataset_replay_entry(
        [source],
        repeat_idx=-1,
        workflow_prefix="ignored",
        workflow_id_mode="source",
        request_variation="none",
        phase="warmup",
    )

    assert warmup["dataset_cycle_index"] == 0
    assert warmup["request"]["metadata"]["workflow_id"] == "wf-source"
    assert warmup["sample"]["workflow_id"] == "wf-source"


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


def test_online_direct_e2e_runner_rejects_missing_direct_buffer_stats():
    summary = _summary()
    summary["direct_buffer_stats_after_release"] = {"workers": {"prefill-0": {}}}
    with pytest.raises(AssertionError, match="stats unavailable"):
        validate_online_direct_summary(summary)


def test_direct_buffer_stats_uses_internal_control_token(monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"allocations": 0, "ref_count": 0, "persistent_cache": False}

    class Session:
        def __init__(self):
            self.headers = {}
            self.urls = []

        def get(self, url, timeout):
            self.urls.append((url, timeout))
            return Response()

        def close(self):
            return None

    session = Session()
    monkeypatch.setattr(runner, "_session", lambda: session)

    assert runner._direct_buffer_stats("http://prefill", auth_token="internal-token") == {
        "allocations": 0,
        "ref_count": 0,
        "persistent_cache": False,
    }
    assert session.headers["X-Mooncake-EPD-Token"] == "internal-token"
    assert session.urls == [("http://prefill/mooncake_epd/direct_feature_buffer/stats", 10)]


def test_direct_buffer_release_settle_waits_for_async_refcount(monkeypatch):
    from mooncake_epd.scripts import run_vllm_online_direct_e2e as runner

    values = iter(
        [
            {"persistent_cache": False, "allocations": 1, "ref_count": 1},
            {"persistent_cache": False, "allocations": 0, "ref_count": 0},
        ]
    )
    monkeypatch.setattr(runner, "_direct_buffer_stats", lambda _url: next(values))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    settled = runner._wait_for_direct_buffer_release_settle(
        ["http://prefill-0"],
        timeout_s=1.0,
        poll_s=0.0,
    )
    assert settled["http://prefill-0"]["allocations"] == 0
    assert settled["http://prefill-0"]["ref_count"] == 0


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


def test_single_baseline_server_script_uses_selected_vllm_cache_root(
    tmp_path, monkeypatch
):
    from mooncake_epd.scripts.run_vllm_single_baseline_e2e import _write_server_script

    cache_root = tmp_path / "fast-cache"
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_CACHE_ROOT", str(cache_root))
    script = _write_server_script(
        workdir=tmp_path,
        port=18400,
        args=SimpleNamespace(
            model="/models/qwen-vl",
            gpu=1,
            max_model_len=2048,
            gpu_memory_utilization=0.55,
            max_num_batched_tokens=0,
            max_num_seqs=0,
            generation_config="vllm",
            trust_remote_code=True,
        ),
    ).read_text(encoding="utf-8")

    assert f"export VLLM_CACHE_ROOT={cache_root}" in script
    assert "export TORCHINDUCTOR_CACHE_DIR=${VLLM_CACHE_ROOT}/torch_compile_cache" in script
    assert "export TRITON_CACHE_DIR=${VLLM_CACHE_ROOT}/triton" in script
    assert 'mkdir -p "${VLLM_CACHE_ROOT}" "${TORCHINDUCTOR_CACHE_DIR}" "${TRITON_CACHE_DIR}"' in script
    assert "--trust-remote-code" in script


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
            "layered_source_ready_event_waits": 2,
            "layered_source_ready_event_wait_ms": 14.0,
            "layered_source_ready_event_wait_ms_avg": 7.0,
            "layered_source_ready_sync_fallbacks": 0,
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
            "layered_source_ready_event_waits": 5,
            "layered_source_ready_event_wait_ms": 32.0,
            "layered_source_ready_event_wait_ms_avg": 6.4,
            "layered_source_ready_sync_fallbacks": 1,
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
    assert delta["layered_source_ready_event_waits"] == 3
    assert delta["layered_source_ready_event_wait_ms"] == 18.0
    assert delta["layered_source_ready_event_wait_ms_avg"] == 6.0
    assert delta["layered_source_ready_sync_fallbacks"] == 1
    assert delta["decode_engine_kv_first_token_requests"] == 1
    assert delta["decode_engine_kv_first_token_latency_ms_total"] == 110.0
    assert delta["decode_engine_kv_first_token_latency_ms_avg"] == 110.0
    assert delta["connector_metric_workers"] == 2
    assert delta["mm_hidden_cache_entries"] == 6
    assert "connector_metrics_updated_at" not in delta
    assert delta["remote_transfer_backend_counts"] == {"peer_buffer_direct": 3}
