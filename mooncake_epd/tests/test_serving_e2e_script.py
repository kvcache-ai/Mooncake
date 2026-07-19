from __future__ import annotations

import sys

import pytest
import requests

from mooncake_epd.scripts.run_vllm_serving_e2e import (
    _http_error_evidence,
    _agent_pd_metadata_for_dataset,
    _connector_metrics_settled,
    _dataset_request_evidence_row,
    _data_url_for_demo_image,
    _ensure_process_running,
    _load_dataset_requests,
    _proc_env,
    _terminate_all,
    _wait_for_metrics_settle,
    _worker_dispatch_balance,
    _validate_summary,
    parse_args,
)
from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs


def test_data_url_for_demo_image_uses_inline_png_payload():
    payload = _data_url_for_demo_image("room")
    assert payload.startswith("data:image/png;base64,")
    assert len(payload.split(",", 1)[1]) > 128


def test_http_error_evidence_retains_bounded_proxy_detail_without_request_data():
    response = requests.Response()
    response.status_code = 502
    response._content = b'{"detail":"prefill response missing required KV handoff fields"}'
    response.url = "http://proxy.local/v1/chat/completions"
    error = requests.HTTPError("502 Server Error", response=response)

    evidence = _http_error_evidence(error)

    assert evidence == {
        "status_code": 502,
        "error_response_detail": "prefill response missing required KV handoff fields",
    }


def test_worker_launch_environment_removes_all_proxy_spellings(monkeypatch):
    for key in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        monkeypatch.setenv(key, "socks5://127.0.0.1:7890")

    env = _proc_env()

    for key in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        assert key not in env
    assert env["NO_PROXY"] == "127.0.0.1,localhost,::1"
    assert env["no_proxy"] == env["NO_PROXY"]


def test_serving_runner_defaults_use_measured_transfer_grouping(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_vllm_serving_e2e.py"])

    args = parse_args()

    assert args.layers_per_group == 32
    assert args.max_group_bytes == 64 * 1024 * 1024
    assert args.max_transfer_descriptors == 512
    assert args.max_transfer_bytes == 64 * 1024 * 1024
    assert args.prefill_http_keepalive is False


def test_generate_configs_supports_real_multi_worker_pools(tmp_path):
    files = generate_configs(
        str(tmp_path),
        VLLMDisaggConfig(prefill_gpus=(0, 1), decode_gpus=(2, 3)),
    )

    assert len(files["prefill_scripts"]) == 2
    assert len(files["decode_scripts"]) == 2
    proxy_cmd = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    assert "--prefiller-hosts 127.0.0.1 127.0.0.1" in proxy_cmd
    assert "--decoder-hosts 127.0.0.1 127.0.0.1" in proxy_cmd
    assert "--high-prefill-worker-ids prefill-0" in proxy_cmd
    assert "--standard-prefill-worker-ids prefill-1" in proxy_cmd
    assert "--low-latency-decode-worker-ids decode-0" in proxy_cmd
    assert "--standard-decode-worker-ids decode-1" in proxy_cmd


def test_dataset_agent_pd_metadata_uses_workload_shape():
    thinking = _agent_pd_metadata_for_dataset(
        {"prompt_token_len": 669, "source_dataset": "mmmu"},
        "W3",
        669,
    )
    hybrid = _agent_pd_metadata_for_dataset(
        {"prompt_token_len": 1009, "source_dataset": "docvqa"},
        "W0",
        1009,
    )
    interactive = _agent_pd_metadata_for_dataset(
        {"prompt_token_len": 128, "source_dataset": "docvqa"},
        "W0",
        128,
    )

    assert thinking["agent_type"] == "thinking"
    assert thinking["routing_target"] == "high_prefill_pool"
    assert hybrid["agent_type"] == "hybrid"
    assert hybrid["routing_target"] == "mixed"
    assert interactive["agent_type"] == "interactive"
    assert interactive["routing_target"] == "low_latency_decode_pool"


def test_dataset_request_evidence_is_reproducible_without_inline_image_payload():
    entry = {
        "family": "W3",
        "sample": {
            "sample_id": "sample-1",
            "workflow_id": "workflow-1",
            "source_dataset": "mmmu",
        },
        "request": {
            "model": "model",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64," + "a" * 4096},
                        }
                    ],
                }
            ],
            "max_tokens": 32,
            "temperature": 0.0,
            "metadata": {
                "workflow_id": "workflow-1",
                "estimated_prompt_len": 512,
                "admission_method": "processor",
            },
        },
    }

    row = _dataset_request_evidence_row(entry, index=3, phase="measure")

    assert row["sample_id"] == "sample-1"
    assert row["workflow_id"] == "workflow-1"
    assert len(row["request_sha256"]) == 64
    assert "messages" not in row
    assert "base64" not in repr(row)


def test_worker_dispatch_balance_includes_idle_configured_workers():
    balance = _worker_dispatch_balance(
        [
            {"decode_worker_id": "decode-0"},
            {"decode_worker_id": "decode-0"},
            {"decode_worker_id": "decode-1"},
        ],
        stage="decode",
        configured_workers=3,
    )

    assert balance["counts"] == {"decode-0": 2, "decode-1": 1, "decode-2": 0}
    assert balance["active_workers"] == 2
    assert balance["configured_workers"] == 3
    assert 0.0 < balance["normalized_entropy"] < 1.0


def _sample_summary(tmp_path):
    metrics_dir = tmp_path / "connector_metrics"
    metrics_dir.mkdir()
    (metrics_dir / "worker.json").write_text("{}", encoding="utf-8")
    return {
        "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
        "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
        "workflow_registry_events": 3,
        "connector_metrics_dir": str(metrics_dir),
        "metrics": {
            "metrics": {
                "requests_total": 2,
                "requests_multimodal": 1,
                "handoff_committed": 2,
                "path_stats": {
                    "PD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                    },
                    "EPD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                        "mm_prefetch_announced": 1,
                    },
                },
                "connector_path_stats": {
                    "PD": {
                        "grouped_batches": 1,
                        "grouped_descriptors": 8,
                        "grouped_bytes": 512,
                        "peer_buffer_batches": 1,
                        "peer_buffer_bytes": 512,
                        "received_group_batches": 1,
                        "received_finished_reqs": 1,
                    },
                    "EPD": {
                        "grouped_batches": 1,
                        "grouped_descriptors": 16,
                        "grouped_bytes": 1024,
                        "peer_buffer_batches": 1,
                        "peer_buffer_bytes": 1024,
                        "received_group_batches": 1,
                        "received_finished_reqs": 1,
                    },
                },
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 2,
                "layered_transfer_grouped_bytes": 1536,
                "layered_transfer_grouped_descriptors": 24,
                "layered_receive_group_batches": 2,
                "layered_receive_finished_reqs": 2,
                "layer_load_wait_calls": 1,
                "peer_buffer_batches": 2,
                "peer_buffer_bytes": 1536,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
            },
            "workflow_registry": {
                "enabled": True,
                "tracked_states": 2,
            },
        },
    }


def test_validate_summary_accepts_real_success_shape(tmp_path):
    _validate_summary(_sample_summary(tmp_path))


def test_validate_summary_accepts_progressive_receive_without_legacy_layer_wait(tmp_path):
    summary = _sample_summary(tmp_path)
    summary["metrics"]["metrics"]["layer_load_wait_calls"] = 0

    # Grouped producer/consumer parity and direct-backend evidence, not a
    # legacy blocking wait counter, prove this native receive completed.
    _validate_summary(summary)


def test_validate_summary_rejects_missing_connector_metrics(tmp_path):
    summary = _sample_summary(tmp_path)
    summary["metrics"]["metrics"]["connector_metric_workers"] = 0
    summary["metrics"]["metrics"]["layered_transfer_grouped_batches"] = 0
    summary["metrics"]["metrics"]["layered_receive_group_batches"] = 0
    with pytest.raises(AssertionError):
        _validate_summary(summary)


def test_connector_metrics_settled_detects_transient_parity_gap():
    ok, failures = _connector_metrics_settled(
        {
            "metrics": {
                "requests_total": 2,
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 14,
                "layered_receive_group_batches": 18,
                "layered_receive_finished_reqs": 2,
                "peer_buffer_batches": 14,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 14},
                "connector_path_stats": {},
            }
        }
    )

    assert ok is False
    assert "producer/consumer grouped batch parity broken" in failures


def test_connector_metrics_settled_accepts_producer_ahead_of_consumer_when_requests_finish():
    ok, failures = _connector_metrics_settled(
        {
            "metrics": {
                "requests_total": 2,
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 27,
                "layered_transfer_grouped_bytes": 2700,
                "layered_transfer_grouped_descriptors": 270,
                "layered_receive_group_batches": 18,
                "layered_receive_finished_reqs": 2,
                "peer_buffer_batches": 27,
                "peer_buffer_bytes": 2700,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 27},
                "connector_path_stats": {
                    "PD": {
                        "grouped_batches": 9,
                        "grouped_bytes": 900,
                        "grouped_descriptors": 90,
                        "peer_buffer_batches": 9,
                        "peer_buffer_bytes": 900,
                        "received_group_batches": 9,
                        "received_finished_reqs": 1,
                    },
                    "EPD": {
                        "grouped_batches": 18,
                        "grouped_bytes": 1800,
                        "grouped_descriptors": 180,
                        "peer_buffer_batches": 18,
                        "peer_buffer_bytes": 1800,
                        "received_group_batches": 9,
                        "received_finished_reqs": 1,
                    },
                },
            }
        }
    )

    assert ok is True
    assert failures == []


def test_wait_for_metrics_settle_polls_until_parity_restored():
    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    class _Session:
        def __init__(self, payloads):
            self._payloads = list(payloads)
            self.calls = 0

        def get(self, url, timeout=30):
            del url, timeout
            idx = min(self.calls, len(self._payloads) - 1)
            self.calls += 1
            return _Resp(self._payloads[idx])

    payload_bad = {
        "metrics": {
            "requests_total": 2,
            "connector_metric_workers": 2,
            "layered_transfer_grouped_batches": 14,
            "layered_receive_group_batches": 18,
            "layered_receive_finished_reqs": 2,
            "peer_buffer_batches": 14,
            "remote_transfer_backend_counts": {"peer_buffer_direct": 14},
            "connector_path_stats": {},
        }
    }
    payload_good = {
        "metrics": {
            "requests_total": 2,
            "connector_metric_workers": 2,
            "layered_transfer_grouped_batches": 18,
            "layered_transfer_grouped_bytes": 1800,
            "layered_transfer_grouped_descriptors": 180,
            "layered_receive_group_batches": 18,
            "layered_receive_finished_reqs": 2,
            "peer_buffer_batches": 18,
            "peer_buffer_bytes": 1800,
            "remote_transfer_backend_counts": {"peer_buffer_direct": 18},
            "connector_path_stats": {
                "PD": {
                    "grouped_batches": 9,
                    "grouped_bytes": 900,
                    "grouped_descriptors": 90,
                    "peer_buffer_batches": 9,
                    "peer_buffer_bytes": 900,
                    "received_group_batches": 9,
                    "received_finished_reqs": 1,
                },
                "EPD": {
                    "grouped_batches": 9,
                    "grouped_bytes": 900,
                    "grouped_descriptors": 90,
                    "peer_buffer_batches": 9,
                    "peer_buffer_bytes": 900,
                    "received_group_batches": 9,
                    "received_finished_reqs": 1,
                },
            },
        }
    }
    session = _Session([payload_bad, payload_good])

    settled = _wait_for_metrics_settle(
        session,
        "http://127.0.0.1:8000/metrics",
        timeout_s=1.0,
        poll_s=0.0,
    )

    assert settled["metrics"]["layered_transfer_grouped_batches"] == 18
    assert session.calls >= 2


def test_ensure_process_running_raises_with_log_tail(tmp_path):
    log_path = tmp_path / "service.log"
    log_path.write_text("line-1\nline-2\nfatal error\n", encoding="utf-8")

    class _Proc:
        def poll(self):
            return 17

    with pytest.raises(RuntimeError) as exc:
        _ensure_process_running("prefill", _Proc(), log_path)

    assert "prefill exited early with code=17" in str(exc.value)
    assert "fatal error" in str(exc.value)


def test_terminate_all_signals_service_group_after_shell_leader_exits(monkeypatch):
    """An orphan EngineCore must not survive an E2E failure cleanup."""

    from mooncake_epd.scripts import run_vllm_serving_e2e as runner

    class _ExitedShell:
        pid = 4242

        @staticmethod
        def poll():
            return 1

    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(runner.os, "killpg", lambda pgid, sig: calls.append((pgid, sig)))
    monkeypatch.setattr(runner, "_process_group_alive", lambda _pgid: False)

    _terminate_all([_ExitedShell()])

    assert calls == [(4242, runner.signal.SIGTERM)]


def test_connector_metrics_settled_accepts_extra_internal_finished_reqs(tmp_path):
    payload = _sample_summary(tmp_path)["metrics"]
    payload["metrics"]["layered_receive_finished_reqs"] = 3
    payload["metrics"]["connector_path_stats"]["PD"]["received_finished_reqs"] = 2

    ok, failures = _connector_metrics_settled(payload)

    assert ok is True
    assert failures == []


def test_connector_metrics_settled_rejects_receive_failure(tmp_path):
    payload = _sample_summary(tmp_path)["metrics"]
    payload["metrics"]["layered_receive_failures"] = 1

    ok, failures = _connector_metrics_settled(payload)

    assert ok is False
    assert "layered_receive_failures != 0" in failures


def test_connector_metrics_settled_rejects_fallback_batches(tmp_path):
    payload = _sample_summary(tmp_path)["metrics"]
    payload["metrics"]["fallback_batches"] = 1
    payload["metrics"]["fallback_bytes"] = 128
    payload["metrics"]["remote_transfer_backend_counts"]["batch_transfer_fallback"] = 1

    ok, failures = _connector_metrics_settled(payload)

    assert ok is False
    assert "fallback_batches != 0" in failures
    assert "fallback_bytes != 0" in failures
    assert any("unexpected remote transfer backends" in item for item in failures)


def test_validate_summary_requires_requested_dataset_probe_count(tmp_path):
    summary = _sample_summary(tmp_path)
    summary["dataset_probe_requested"] = 2
    summary["dataset_probe_count"] = 1
    summary["dataset_probe_success"] = 1
    summary["dataset_probe_results"] = [{"status_code": 200, "routing_path": "EPD"}]

    with pytest.raises(AssertionError) as exc:
        _validate_summary(summary)

    assert "dataset_probe_count 1 != requested 2" in str(exc.value)


def test_validate_summary_requires_requested_family_coverage(tmp_path):
    summary = _sample_summary(tmp_path)
    summary["dataset_probe_requested"] = 2
    summary["dataset_probe_count"] = 2
    summary["dataset_probe_success"] = 2
    summary["dataset_families_requested"] = ["W0", "W1"]
    summary["dataset_probe_family_counts"] = {"W0": 2}
    summary["dataset_probe_results"] = [
        {"status_code": 200, "routing_path": "EPD", "response_content_len": 1},
        {"status_code": 200, "routing_path": "EPD", "response_content_len": 1},
    ]

    with pytest.raises(AssertionError) as exc:
        _validate_summary(summary)

    assert "dataset requested families not covered" in str(exc.value)


def test_load_dataset_requests_skips_oversized_samples(monkeypatch, tmp_path):
    root = tmp_path / "dataset"
    (root / "chat_splits").mkdir(parents=True)
    (root / "images").mkdir()
    (root / "images" / "a.png").write_bytes(b"fake")
    split = root / "chat_splits" / "dev-small.jsonl"
    samples = [
        {
            "sample_id": "too-long",
            "workflow_id": "wf-long",
            "workload_family": "W0",
            "source_dataset": "unit",
            "prompt_token_len": 1,
            "messages": [
                {"role": "user", "content": [{"type": "image", "image": "images/a.png"}]},
            ],
            "images": [{"path_or_url": "images/a.png"}],
        },
        {
            "sample_id": "ok",
            "workflow_id": "wf-ok",
            "workload_family": "W0",
            "source_dataset": "unit",
            "prompt_token_len": 1,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "hello"}]},
            ],
            "images": [],
        },
    ]
    split.write_text("\n".join(__import__("json").dumps(item) for item in samples), encoding="utf-8")

    estimates = iter(
        [
            (5000, "unit", {"method": "unit"}),
            (100, "unit", {"method": "unit"}),
        ]
    )
    monkeypatch.setattr(
        "mooncake_epd.scripts.run_vllm_serving_e2e._estimate_dataset_prompt_len",
        lambda *args, **kwargs: next(estimates),
    )

    selected, skipped = _load_dataset_requests(
        dataset_root=str(root),
        chat_split="dev-small",
        max_requests=1,
        families=["W0"],
        model="model",
        max_input_len=4096,
        request_max_tokens=32,
        skip_oversized=True,
        image_max_pixels=0,
    )

    assert [entry["sample"]["sample_id"] for entry in selected] == ["ok"]
    assert skipped[0]["sample_id"] == "too-long"
    assert skipped[0]["reason"] == "estimated_context_too_long"


def test_load_dataset_requests_prioritizes_family_coverage(monkeypatch, tmp_path):
    root = tmp_path / "dataset"
    (root / "chat_splits").mkdir(parents=True)
    split = root / "chat_splits" / "dev-small.jsonl"
    samples = []
    for family, count in (("W0", 3), ("W1", 2), ("W2", 2)):
        for idx in range(count):
            samples.append(
                {
                    "sample_id": f"{family}-{idx}",
                    "workflow_id": f"wf-{family}-{idx}",
                    "workload_family": family,
                    "source_dataset": "unit",
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": family}]},
                    ],
                    "images": [],
                }
            )
    split.write_text("\n".join(__import__("json").dumps(item) for item in samples), encoding="utf-8")
    monkeypatch.setattr(
        "mooncake_epd.scripts.run_vllm_serving_e2e._estimate_dataset_prompt_len",
        lambda *args, **kwargs: (100, "unit", {"method": "unit"}),
    )

    selected, skipped = _load_dataset_requests(
        dataset_root=str(root),
        chat_split="dev-small",
        max_requests=3,
        families=["W0", "W1", "W2"],
        model="model",
        max_input_len=4096,
        request_max_tokens=32,
        skip_oversized=True,
        image_max_pixels=0,
    )

    assert skipped == []
    assert [entry["family"] for entry in selected] == ["W0", "W1", "W2"]
