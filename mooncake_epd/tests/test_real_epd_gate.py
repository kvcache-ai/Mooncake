from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from mooncake_epd.scripts.check_real_epd_gate import RealEPDGateError, validate_real_epd_summary


def _valid_summary(**overrides):
    summary = {
        "uses_mock": False,
        "strict_no_fallback": True,
        "response": {
            "status_code": 200,
            "headers": {"x-epd-routing-path": "EPD"},
            "response_content_len": 16,
        },
        "direct_buffer_stats_after_release": {"allocations": 0, "bytes": 0},
        "real_epd_metric_summary": {
            "requests_total": 1,
            "requests_multimodal": 1,
            "precomputed_hits": 1,
            "hidden_cache_errors": 0,
            "hidden_cache_full_miss_batches": 0,
            "hidden_cache_vision_compute_ms_avg": 0.0,
            "fallback_batches": 0,
            "fallback_bytes": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "peer_buffer_batches": 2,
            "peer_buffer_bytes": 4096,
            "backend_counts": {"peer_buffer_direct": 2},
        },
    }
    for key, value in overrides.items():
        if key == "metrics":
            summary["real_epd_metric_summary"].update(value)
        else:
            summary[key] = value
    return summary


def test_real_epd_gate_accepts_strict_direct_summary():
    validate_real_epd_summary(_valid_summary())


def test_real_epd_gate_rejects_recorded_deterministic_output_mismatch():
    with pytest.raises(RealEPDGateError, match="response-consistency failure"):
        validate_real_epd_summary(
            _valid_summary(
                response_consistency={
                    "required": True,
                    "applicable": True,
                    "pass": False,
                    "inconsistent_group_ids": ["sample-a"],
                }
            )
        )


def test_real_epd_gate_rejects_warmup_only_deterministic_output_mismatch():
    with pytest.raises(RealEPDGateError, match="response-consistency failure"):
        validate_real_epd_summary(
            _valid_summary(
                response_consistency={
                    "required": True,
                    "applicable": True,
                    "pass": True,
                    "inconsistent_group_ids": [],
                },
                all_response_consistency={
                    "required": True,
                    "applicable": True,
                    "pass": False,
                    "inconsistent_group_ids": ["sample-a"],
                },
            )
        )


def test_real_epd_gate_rejects_required_reference_response_mismatch():
    with pytest.raises(RealEPDGateError, match="reference response-equivalence failure"):
        validate_real_epd_summary(
            _valid_summary(
                reference_response_equivalence={
                    "required": True,
                    "applicable": True,
                    "pass": False,
                    "missing_reference_group_ids": [],
                    "unexpected_candidate_hashes_by_group": {"W3:0": ["a" * 64]},
                }
            )
        )


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"uses_mock": True}, "mock"),
        ({"strict_no_fallback": False}, "strict_no_fallback"),
        ({"metrics": {"precomputed_hits": 0}}, "precomputed"),
        ({"metrics": {"hidden_cache_vision_compute_ms_avg": 12.5}}, "vision encoder"),
        ({"metrics": {"fallback_batches": 1}}, "fallback"),
        ({"metrics": {"backend_counts": {"batch_transfer_fallback": 1}}}, "peer_buffer_direct"),
        ({"direct_buffer_stats_after_release": {"allocations": 1}}, "not released"),
    ],
)
def test_real_epd_gate_rejects_unsupported_claims(patch, message):
    with pytest.raises(RealEPDGateError, match=message):
        validate_real_epd_summary(_valid_summary(**patch))


def test_check_real_epd_gate_cli_json(tmp_path):
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(_valid_summary()), encoding="utf-8")
    proc = subprocess.run(
        [
            sys.executable,
            "mooncake_epd/scripts/check_real_epd_gate.py",
            "--summary",
            str(summary_path),
            "--json",
        ],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=True,
    )
    assert json.loads(proc.stdout) == {"ok": True}


def test_real_qwenvl_demo_help_is_file_executable():
    proc = subprocess.run(
        [sys.executable, "mooncake_epd/scripts/run_real_qwenvl_epd_demo.py", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=True,
    )
    assert "real strict Qwen-VL EPD demo" in proc.stdout


def test_real_qwenvl_demo_help_exposes_multi_decode_and_direct_options():
    proc = subprocess.run(
        [sys.executable, "mooncake_epd/scripts/run_real_qwenvl_epd_demo.py", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=True,
    )
    assert "--decode-gpus" in proc.stdout
    assert "--min-warmup-per-decode" in proc.stdout
    assert "--direct-persistent-cache" in proc.stdout
    assert "--direct-proxy-handle-cache-max-entries" in proc.stdout
    assert "--direct-proxy-handle-cache-ttl-s" in proc.stdout
    assert "--release-direct-feature-buffers-after-prefill" in proc.stdout
    assert "--vllm-mm-hidden-cache" in proc.stdout
    assert "--image-urls" in proc.stdout
    assert "--prefill-dispatch-mode" in proc.stdout
    assert "--enable-rendered-prefill-cache" in proc.stdout
    assert "--vllm-prefix-cache-mode" in proc.stdout
    assert "--reference-response-summary" in proc.stdout
    assert "--scheduler-policy" in proc.stdout
    assert "--transfer-workers" in proc.stdout


def test_real_qwenvl_demo_parser_exposes_every_online_direct_cache_option(monkeypatch):
    from mooncake_epd.scripts.run_real_qwenvl_epd_demo import parse_args

    monkeypatch.setattr(sys, "argv", ["run_real_qwenvl_epd_demo.py"])
    args = parse_args()

    assert args.direct_proxy_handle_cache is True
    assert args.direct_proxy_handle_cache_max_entries == 4096
    assert args.direct_proxy_handle_cache_ttl_s == 600.0
    assert args.enable_rendered_prefill_cache is True
    assert args.rendered_prefill_cache_max_entries == 64
    assert args.rendered_prefill_cache_max_bytes == 256 * 1024 * 1024
    assert args.rendered_prefill_cache_ttl_s == 300.0
    assert args.generation_config == "vllm"
    assert args.request_variation == "none"
    assert args.vllm_prefix_cache_mode == "isolate"
    assert args.release_direct_feature_buffers_after_prefill is True
    assert args.vllm_mm_hidden_cache is None
    assert args.image_urls is None
    assert args.scheduler_policy == "agent_aware"


def test_real_qwenvl_demo_forwards_online_runner_required_options(monkeypatch, tmp_path):
    """Keep the public real-demo facade compatible with its lower-level runner."""

    from mooncake_epd.scripts import run_real_qwenvl_epd_demo as demo

    captured = {}

    def fake_run_online_direct(args):
        captured["scheduler_policy"] = args.scheduler_policy
        captured["release_after_prefill"] = args.release_direct_feature_buffers_after_prefill
        captured["hidden_cache"] = args.vllm_mm_hidden_cache
        captured["image_urls"] = args.image_urls
        summary = _valid_summary()
        summary["online_direct_metric_summary"] = dict(
            summary["real_epd_metric_summary"]
        )
        return summary

    monkeypatch.setattr(demo, "run_online_direct", fake_run_online_direct)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_real_qwenvl_epd_demo.py",
            "--workdir",
            str(tmp_path),
            "--scheduler-policy",
            "least_loaded",
            "--no-release-direct-feature-buffers-after-prefill",
            "--no-vllm-mm-hidden-cache",
            "--image-urls",
            "data:image/png;base64,AA==",
            "data:image/png;base64,BB==",
        ],
    )

    demo.run(demo.parse_args())

    assert captured == {
        "scheduler_policy": "least_loaded",
        "release_after_prefill": False,
        "hidden_cache": False,
        "image_urls": [
            "data:image/png;base64,AA==",
            "data:image/png;base64,BB==",
        ],
    }


def test_real_epd_gate_accepts_released_persistent_direct_cache():
    validate_real_epd_summary(
        _valid_summary(
            direct_buffer_stats_after_release={
                "persistent_cache": True,
                "allocations": 1,
                "bytes": 1024,
                "ref_count": 0,
            }
        )
    )


def test_real_epd_gate_rejects_active_persistent_direct_cache_ref():
    with pytest.raises(RealEPDGateError, match="active refs"):
        validate_real_epd_summary(
            _valid_summary(
                direct_buffer_stats_after_release={
                    "persistent_cache": True,
                    "allocations": 1,
                    "bytes": 1024,
                    "ref_count": 1,
                }
            )
        )
