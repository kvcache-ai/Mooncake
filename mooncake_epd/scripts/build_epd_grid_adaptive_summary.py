#!/usr/bin/env python3
"""Build a compact, reproducible summary for semantic-grid/adaptive-read experiments."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts" / "perf302_gpu_epd_20260830"
TRACE_DIR = ARTIFACTS / "online_direct" / "connector_metrics"


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def _http_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=5.0) as response:  # noqa: S310
        payload = response.read()
    return json.loads(payload)


def _http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5.0) as response:  # noqa: S310
            return response.status == 200
    except Exception:
        return False


def _benchmark_summary(filename: str) -> dict[str, Any]:
    path = ARTIFACTS / filename
    document = _read_json(path)
    runs: list[dict[str, Any]] = []
    for run in document.get("runs", []):
        runs.append(
            {
                "requested": run.get("requested"),
                "successful": run.get("successful"),
                "failed": run.get("failed"),
                "success_rate": run.get("success_rate"),
                "goodput_count": run.get("goodput_count"),
                "goodput_rps": run.get("goodput_rps"),
                "request_throughput_rps": run.get("request_throughput_rps"),
                "ttft_ms": run.get("ttft_ms"),
                "tpot_ms": run.get("tpot_ms"),
                "decode_worker_counts": run.get("decode_worker_counts"),
            }
        )
    strict = document.get("gate", {}).get("strict_epd_metrics", {})
    return {
        "artifact": str(path.relative_to(ROOT)),
        "benchmark_id": document.get("benchmark_id"),
        "created_at": document.get("created_at"),
        "gate_passed": document.get("gate", {}).get("passed"),
        "gate_failures": document.get("gate", {}).get("failures", []),
        "mock_used": document.get("methodology", {}).get("mock_used"),
        "arrival_model": document.get("methodology", {}).get("arrival_model"),
        "client_mm_uuid_compaction": document.get("methodology", {}).get(
            "client_mm_uuid_compaction"
        ),
        "runs": runs,
        "strict_counter_deltas": strict.get("counter_deltas", {}),
    }


def _trace_events(filename: str) -> list[dict[str, Any]]:
    path = TRACE_DIR / filename
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") != "feature_handle_direct_remote_resolved":
                continue
            events.append(
                {
                    key: event.get(key)
                    for key in (
                        "pid",
                        "feature_id",
                        "nbytes",
                        "tensor_count",
                        "direct_read_requested_mode",
                        "direct_read_mode",
                        "direct_read_adaptive_slow_ms",
                        "direct_read_adaptive_managed_remaining",
                        "direct_read_adaptive_transition",
                        "direct_read_timings_ms",
                        "grid_thw_direct_read_skipped",
                        "grid_thw_source",
                        "grid_thw_values",
                    )
                    if key in event
                }
            )
    return events


def _gpu_snapshot() -> list[dict[str, Any]]:
    output = subprocess.check_output(
        [
            "nvidia-smi",
            "--query-gpu=index,uuid,memory.used,utilization.gpu",
            "--format=csv,noheader,nounits",
        ],
        text=True,
    )
    rows: list[dict[str, Any]] = []
    for line in output.splitlines():
        index, uuid, memory_mib, utilization = (part.strip() for part in line.split(","))
        if int(index) not in {1, 2, 5, 6}:
            continue
        rows.append(
            {
                "gpu": int(index),
                "uuid": uuid,
                "memory_used_mib": int(memory_mib),
                "utilization_percent": int(utilization),
            }
        )
    return rows


def _pid_for_port(port: int) -> int | None:
    result = subprocess.run(
        ["fuser", "-n", "tcp", str(port)],
        check=False,
        capture_output=True,
        text=True,
    )
    tokens = (result.stdout + " " + result.stderr).split()
    for token in tokens:
        if token.isdigit():
            return int(token)
    return None


def main() -> None:
    metrics = _http_json("http://127.0.0.1:39572/metrics")
    cache = metrics["direct_feature_handle_cache"]
    summary: dict[str, Any] = {
        "schema_version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "objective": (
            "Validate semantic grid isolation and interference-aware adaptive E->P "
            "direct reads on a real EPD deployment."
        ),
        "truth_boundary": {
            "model": "/data01/LWX/Qwen3-VL-8B-Instruct",
            "dataset": "/data/songbinbin/Proj/Proj_LWX/mooncake_test_dataset",
            "mock_used": False,
            "epd_separated": True,
            "data_plane_protocol": "tcp",
            "transport_backend": "mooncake_engine_direct",
            "rdma_claimed": False,
        },
        "implementation": {
            "semantic_grid": {
                "descriptor_sideband": "grid_thw_values",
                "validated_fields": [
                    "integer",
                    "finite",
                    "positive",
                    "shape_and_nbytes",
                    "split_size_total",
                    "spatial_merge_row_count",
                ],
                "model_input_device": "cpu",
                "remote_grid_read_skipped": True,
                "representative_descriptor_count_before": 5,
                "representative_descriptor_count_after": 4,
                "representative_bytes_before": 122093592,
                "representative_bytes_after": 122093568,
            },
            "registered_read_lifetime": {
                "cuda_visibility_barrier_before_unregister": True,
                "trace_field": "visibility_sync_ms",
            },
            "adaptive_policy": {
                "requested_mode": "adaptive",
                "production_slow_threshold_ms": 750.0,
                "production_managed_cooldown_reads": 8,
                "same_request_retry": False,
                "algorithm": (
                    "registered fast path; future reads use managed buffer after a slow "
                    "registered completion; registered probe resumes after cooldown"
                ),
            },
            "files": [
                "core/state/feature_store.py",
                "core/epd_workers.py",
                "core/transfer/engine.py",
                "core/state/vllm_feature_handle_provider.py",
                "artifacts/perf302_gpu_epd_20260830/online_direct/start_prefill.sh",
            ],
        },
        "benchmarks": {
            "semantic_grid_full_repair": _benchmark_summary(
                "epd_semantic_grid_repair_real_full.json"
            ),
            "semantic_grid_compact_c2": _benchmark_summary(
                "epd_semantic_grid_repair_compact_hot_c2_r2.json"
            ),
            "managed_hot_full": _benchmark_summary(
                "epd_managed_cpu_grid_full_unique2_hot.json"
            ),
            "registered_clean_hot_full": _benchmark_summary(
                "epd_registered_cpu_grid_clean_full_unique2_hot.json"
            ),
            "adaptive_forced_transition": _benchmark_summary(
                "epd_adaptive_forced_registered_to_managed_real.json"
            ),
            "adaptive_production": _benchmark_summary(
                "epd_adaptive_production750_real_unique2.json"
            ),
        },
        "direct_read_traces": {
            "semantic_grid_registered": _trace_events(
                "mm-hidden-trace-epd-prefill-kv_producer-pid819799.jsonl"
            ),
            "managed": _trace_events(
                "mm-hidden-trace-epd-prefill-kv_producer-pid1013804.jsonl"
            ),
            "registered_clean": _trace_events(
                "mm-hidden-trace-epd-prefill-kv_producer-pid3185266.jsonl"
            ),
            "adaptive_forced": _trace_events(
                "mm-hidden-trace-epd-prefill-kv_producer-pid3556713.jsonl"
            ),
            "adaptive_production": _trace_events(
                "mm-hidden-trace-epd-prefill-kv_producer-pid3726650.jsonl"
            ),
        },
        "invalid_or_failed_experiments": [
            {
                "artifact": "artifacts/perf302_gpu_epd_20260830/epd_direct_read_managed_ab_full1.json",
                "log": "artifacts/perf302_gpu_epd_20260830/online_direct/prefill_managed_read_ab.log",
                "classification": "runner_lifecycle_setup_error",
                "reason": (
                    "manual restart omitted metadata cleanup; duplicate rpc_meta PUT "
                    "caused a native crash, so this is not a valid mode comparison"
                ),
            },
            {
                "artifact": "artifacts/perf302_gpu_epd_20260830/epd_direct_read_managed_valid_ab_full1.json",
                "log": "artifacts/perf302_gpu_epd_20260830/online_direct/prefill_managed_read_valid_ab.log",
                "classification": "product_defect_reproduced",
                "reason": (
                    "managed read completed, but CUDA-resident semantic grid later became "
                    "an invalid split; this led to the CPU semantic-grid repair"
                ),
            },
            {
                "artifact": "artifacts/perf302_gpu_epd_20260830/nonreal_grid_semantic_adaptive_final.log",
                "classification": "test_environment_bootstrap_error",
                "reason": (
                    "project parent was absent from PYTHONPATH, so Python imported the "
                    "system sitecustomize instead of the project lifecycle shim"
                ),
            },
        ],
        "software_verification": {
            "pytest_command": (
                "PYTHONNOUSERSITE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 "
                "PYTHONPATH=/data/songbinbin/Proj/Proj_LWX "
                "/data/songbinbin/Proj/Proj_LWX/venv_mooncake/bin/python "
                "-m pytest -q -m 'not real_model'"
            ),
            "pytest_result": {
                "passed": 432,
                "skipped": 8,
                "deselected": 16,
                "warnings": 1,
                "elapsed_s": 165.37,
                "artifact": "artifacts/perf302_gpu_epd_20260830/nonreal_grid_semantic_adaptive_final_retry.log",
            },
            "py_compile": "passed",
            "start_prefill_bash_n": "passed",
        },
        "runtime_snapshot": {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "health": {
                str(port): _http_ok(f"http://127.0.0.1:{port}/health")
                for port in (8100, 8200, 8330, 8201, 39572)
            },
            "api_pids": {
                "prefill_gpu1": _pid_for_port(8100),
                "decode0_gpu2": _pid_for_port(8200),
                "encoder_gpu5": _pid_for_port(8330),
                "decode1_gpu6": _pid_for_port(8201),
                "proxy": _pid_for_port(39572),
            },
            "known_engine_core_pids": {
                "prefill_gpu1": 3726650,
                "decode0_gpu2": 337484,
                "decode1_gpu6": 622785,
            },
            "gpu": _gpu_snapshot(),
            "direct_feature_handle_cache": {
                key: cache.get(key)
                for key in (
                    "entries",
                    "bytes",
                    "hits",
                    "misses",
                    "worker_unavailable_attempts",
                    "worker_unavailable_transitions",
                    "worker_recovery_transitions",
                    "incarnation_probe_responses",
                    "incarnation_probe_failures",
                    "incarnation_poll_jitter_ratio",
                    "incarnation_poll_delay_min_s",
                    "incarnation_poll_delay_max_s",
                )
            },
        },
        "conclusion": {
            "semantic_grid_repair": (
                "Small semantic tensors must not share the registration/unregistration "
                "lifetime of bulk CUDA DMA buffers."
            ),
            "clean_path": (
                "Registered-tensor reads remained faster than managed-buffer reads in the "
                "low-interference comparison."
            ),
            "interference_path": (
                "Adaptive mode preserves the registered fast path and moves only future "
                "reads to a bounded managed-buffer cooldown after a slow completion."
            ),
            "limitations": [
                "Cold/lazy-compile TTFT values are not steady-state speed comparisons.",
                "Earlier congested registered readings are diagnostic, not isolated A/B evidence.",
                "All data-plane evidence here is TCP; no RDMA conclusion is made.",
            ],
        },
    }
    output = ARTIFACTS / "epd_grid_semantic_adaptive_read_summary.json"
    with output.open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
    print(output)


if __name__ == "__main__":
    os.chdir(ROOT)
    main()
