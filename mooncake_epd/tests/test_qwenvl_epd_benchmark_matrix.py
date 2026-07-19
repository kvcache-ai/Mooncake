from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from mooncake_epd.scripts.build_qwenvl_epd_eval_dataset import build_dataset
from mooncake_epd.scripts.check_epd_performance_claims import evaluate_claims
from mooncake_epd.scripts.run_qwenvl_epd_benchmark_matrix import build_matrix


def _benchmark_config(*, total_gpus: int = 2, fingerprint: str = "same-request"):
    return {
        "schema_version": 1,
        "request_fingerprint": fingerprint,
        "request": {
            "model": "qwen-vl",
            "max_tokens": 16,
            "temperature": 0.0,
            "prompt_source": "inline",
        },
        "request_variation": {"mode": "none", "schema_version": 1},
        "vllm_prefix_cache": {"mode": "reuse", "schema_version": 1},
        "load": {
            "repeat_requests": 2,
            "concurrency": 2,
            "requested_warmup_requests": 1,
            "requested_warmup_concurrency": 1,
            "warmup_cover_dataset_cycle": False,
            "between_repeat_sleep_s": 0.0,
        },
        "serving": {
            "max_model_len": 4096,
            "gpu_memory_utilization": 0.8,
            "max_num_batched_tokens": 4096,
            "max_num_seqs": 16,
            "tensor_parallel_size": 1,
        },
        "topology": {"total_gpus": total_gpus},
    }


def _responses(*, text: str = "same", tokens: int = 3):
    return [
        {
            "index": index,
            "status_code": 200,
            "response_text": text,
            "completion_tokens": tokens,
        }
        for index in range(2)
    ]


def _raw_artifacts(tmp_path: Path, label: str):
    paths = {}
    for field in ("requests_jsonl", "responses_jsonl", "service_logs", "metrics", "environment"):
        path = tmp_path / f"{label}-{field}.txt"
        path.write_text("evidence", encoding="utf-8")
        paths[field] = str(path)
    return paths


def test_build_qwenvl_dataset_uses_real_image_assets(tmp_path):
    img = tmp_path / "demo.png"
    img.write_bytes(b"not-real-png-but-real-file")
    out = tmp_path / "tasks.jsonl"

    summary = build_dataset(dataset_root=tmp_path, output=out, samples=12, seed=1)

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["samples"] == 12
    assert summary["image_assets"] == 1
    assert rows[0]["messages"][0]["content"][0]["type"] == "image_url"
    assert {row["agent_type"] for row in rows} >= {"thinking", "interactive", "hybrid"}


def test_qwenvl_benchmark_matrix_and_claim_gate(tmp_path):
    dataset = tmp_path / "tasks.jsonl"
    dataset.write_text(json.dumps({"task_id": "x"}) + "\n", encoding="utf-8")
    baseline = tmp_path / "baseline.json"
    epd = tmp_path / "epd.json"
    raw_baseline = _raw_artifacts(tmp_path, "baseline")
    raw_epd = _raw_artifacts(tmp_path, "epd")
    baseline.write_text(
        json.dumps(
            {
                "ttft_ms": 1000,
                "goodput_rps": 2.0,
                "benchmark_config": _benchmark_config(),
                "responses": _responses(),
                "raw_artifacts": raw_baseline,
            }
        ),
        encoding="utf-8",
    )
    epd.write_text(
        json.dumps(
            {
                "ttft_ms": 700,
                "goodput_rps": 2.5,
                "precomputed_hits": 3,
                "fallback_batches": 0,
                "layered_receive_failures": 0,
                "benchmark_config": _benchmark_config(),
                "responses": _responses(),
                "raw_artifacts": raw_epd,
            }
        ),
        encoding="utf-8",
    )

    matrix = build_matrix(
        dataset=str(dataset),
        baseline_summary=str(baseline),
        epd_summary=str(epd),
        output=str(tmp_path / "matrix.json"),
        protocol="shm",
    )

    assert matrix["protocol"] == "shm"
    assert matrix["dataset"]["rows"] == 1
    assert matrix["claim_gate"]["pass"] is True
    assert evaluate_claims({"baseline": {"ttft_ms": 1}, "epd": {"ttft_ms": 2}})["pass"] is False


def test_claim_gate_reads_nested_real_serving_artifacts():
    result = evaluate_claims(
        {
            "baseline": {
                "latency_stats_ms": {"avg": 1000.0},
                "benchmark_config": _benchmark_config(),
                "responses": _responses(),
            },
            "epd": {
                "ttft_stats_ms": {"avg": 800.0},
                "request_throughput_rps": 2.0,
                "feature_handle_metric_summary": {
                    "precomputed_hits": 2,
                    "fallback_batches": 0,
                    "layered_receive_failures": 0,
                    "layered_transfer_failed_batches": 0,
                    "hidden_cache_full_miss_batches": 0,
                    "hidden_cache_vision_compute_ms_avg": 0.0,
                },
                "benchmark_config": _benchmark_config(),
                "responses": _responses(),
            },
        }
    )

    assert result["claims"]["ttft_improvement"] is True
    assert result["claims"]["goodput_improvement"] is True
    assert result["claims"]["cache_reuse"] is True
    assert result["claims"]["direct_transfer"] is True


def test_claim_gate_rejects_resource_or_deterministic_output_mismatch():
    result = evaluate_claims(
        {
            "baseline": {
                "ttft_ms": 100.0,
                "goodput_rps": 2.0,
                "benchmark_config": _benchmark_config(total_gpus=1),
                "responses": _responses(text="baseline", tokens=3),
            },
            "epd": {
                "ttft_ms": 50.0,
                "goodput_rps": 3.0,
                "precomputed_hits": 1,
                "fallback_batches": 0,
                "layered_receive_failures": 0,
                "benchmark_config": _benchmark_config(total_gpus=4),
                "responses": _responses(text="epd", tokens=4),
            },
        }
    )

    assert result["pass"] is False
    assert result["claims"]["fair_comparison"] is False
    assert result["claims"]["output_equivalence"] is False
    assert "resource_unmatched" in result["failures"]
    assert "completion_token_distribution_mismatch" in result["failures"]
    assert "deterministic_response_mismatch" in result["failures"]


def test_qwenvl_benchmark_matrix_script_runs(tmp_path):
    dataset = tmp_path / "tasks.jsonl"
    baseline = tmp_path / "baseline.json"
    epd = tmp_path / "epd.json"
    dataset.write_text(json.dumps({"task_id": "x"}) + "\n", encoding="utf-8")
    baseline.write_text(json.dumps({"ttft_ms": 10, "goodput_rps": 1}), encoding="utf-8")
    epd.write_text(json.dumps({"ttft_ms": 8, "goodput_rps": 2, "precomputed_hits": 1}), encoding="utf-8")
    script = Path(__file__).resolve().parent.parent / "scripts" / "run_qwenvl_epd_benchmark_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--dataset",
            str(dataset),
            "--baseline-summary",
            str(baseline),
            "--epd-summary",
            str(epd),
            "--output",
            str(tmp_path / "matrix.json"),
            "--protocol",
            "rdma",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "claim_gate" in proc.stdout
    assert json.loads((tmp_path / "matrix.json").read_text(encoding="utf-8"))["protocol"] == "rdma"


def test_matrix_refuses_claims_without_complete_raw_evidence(tmp_path):
    dataset = tmp_path / "tasks.jsonl"
    baseline = tmp_path / "baseline.json"
    epd = tmp_path / "epd.json"
    dataset.write_text(json.dumps({"task_id": "x"}) + "\n", encoding="utf-8")
    baseline.write_text(json.dumps({"benchmark_config": _benchmark_config(), "responses": _responses()}), encoding="utf-8")
    epd.write_text(json.dumps({"benchmark_config": _benchmark_config(), "responses": _responses()}), encoding="utf-8")
    matrix = build_matrix(
        dataset=str(dataset),
        baseline_summary=str(baseline),
        epd_summary=str(epd),
        output=str(tmp_path / "matrix.json"),
    )
    assert matrix["claim_gate"]["pass"] is False
    assert "baseline:missing_raw_artifacts" in matrix["claim_gate"]["failures"]


def test_matrix_executes_commands_and_discovers_pretty_runner_summaries(tmp_path):
    dataset = tmp_path / "tasks.jsonl"
    dataset.write_text(json.dumps({"task_id": "x"}) + "\n", encoding="utf-8")
    baseline = tmp_path / "baseline-from-command.json"
    epd = tmp_path / "epd-from-command.json"
    baseline.write_text(json.dumps({"ttft_ms": 100, "goodput_rps": 1}), encoding="utf-8")
    epd.write_text(json.dumps({"ttft_ms": 80, "goodput_rps": 2}), encoding="utf-8")

    def command(path: Path) -> str:
        # Emit the same multi-line summary envelope used by real runners.
        payload = json.dumps({"summary": str(path)}, indent=2)
        return f"{sys.executable} -c {json.dumps('print(' + repr(payload) + ')')}"

    matrix = build_matrix(
        dataset=str(dataset),
        baseline_summary=None,
        epd_summary=None,
        output=str(tmp_path / "matrix.json"),
        run_baseline_cmd=command(baseline),
        run_epd_cmd=command(epd),
    )

    assert matrix["executed_steps"]["baseline"]["summary_source"] == "runner_stdout"
    assert matrix["executed_steps"]["epd"]["summary_source"] == "runner_stdout"
    assert matrix["baseline"]["ttft_ms"] == 100
    assert matrix["epd"]["goodput_rps"] == 2


def test_claim_gate_prefers_average_ttft_over_last_request_ttft():
    result = evaluate_claims(
        {
            "baseline": {"avg_ttft_ms": 100.0, "ttft_ms": 1000.0, "goodput_rps": 2.0},
            "epd": {
                "avg_ttft_ms": 300.0,
                "ttft_ms": 50.0,
                "goodput_rps": 3.0,
                "precomputed_hits": 1,
                "fallback_batches": 0,
                "layered_receive_failures": 0,
                "layered_transfer_failed_batches": 0,
            },
        }
    )

    assert result["metrics"]["baseline_ttft_ms"] == 100.0
    assert result["metrics"]["epd_ttft_ms"] == 300.0
    assert result["claims"]["ttft_improvement"] is False
