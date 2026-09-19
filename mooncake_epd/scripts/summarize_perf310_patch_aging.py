#!/usr/bin/env python3
"""Summarize real mixed-patch FIFO versus patch-aging EPD capability arms."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
from typing import Any, Iterable


def _stats(values: Iterable[float]) -> dict[str, Any]:
    rows = sorted(float(value) for value in values)
    if not rows:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def percentile(q: float) -> float:
        position = (len(rows) - 1) * q
        lower = int(position)
        upper = min(len(rows) - 1, lower + 1)
        weight = position - lower
        return rows[lower] * (1.0 - weight) + rows[upper] * weight

    return {
        "count": len(rows),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": rows[-1],
    }


def _pct(treatment: float, control: float) -> float:
    return (float(treatment) / float(control) - 1.0) * 100.0


def _load_arm(
    root: Path,
    tag: str,
    concurrency: int,
    *,
    expected_requests: int,
    patch_by_sample: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    benchmark = json.loads((root / tag / f"c{concurrency}.json").read_text())
    health = json.loads(
        (root / tag / f"encoder_health_c{concurrency}.json").read_text()
    )
    run = benchmark["runs"][0]
    assert benchmark["gate"]["passed"], (tag, concurrency, benchmark["gate"])
    assert run["successful"] == expected_requests and run["failed"] == 0
    assert benchmark["gate"]["strict_epd_metrics"]["strict_no_fallback"] is True
    assert health["qwen3_preprocess_pipeline"]["vision_only_processor"] == {
        "enabled": True,
        "calls": expected_requests,
        "fallbacks": 0,
    }
    assert health["qwen3_predicted_descriptor_overlap"]["enabled"] is False
    pending = health["pending_direct_bundles"]
    assert pending["tickets"] == 0 and pending["retained_bytes"] == 0
    batcher = health["qwen3_dynamic_batcher"]
    admission = health["qwen3_batch_admission"]
    assert batcher["enabled"] is True
    assert batcher["max_batch_size"] == 1 and batcher["max_wait_ms"] == 0.0
    assert batcher["pending"] == 0 and batcher["native_encode_inflight"] == 0
    assert batcher["multi_item_batches"] == 0
    assert admission["min_inflight"] == 6
    assert admission["direct_credits_per_burst"] == 3
    if concurrency < 6:
        assert admission["burst_activations"] == 0
        assert admission["batched_decisions"] == 0
        assert batcher["submitted"] == 0 and batcher["completed"] == 0
    else:
        assert admission["burst_activations"] >= 1
        assert admission["direct_credit_decisions"] >= 3
        assert (
            int(admission["direct_decisions"])
            + int(admission["batched_decisions"])
            == expected_requests
        )
        assert batcher["submitted"] == admission["batched_decisions"]
        assert batcher["completed"] == admission["batched_decisions"]
        assert batcher["batch_size_counts"] == {
            "1": admission["batched_decisions"]
        }

    bucket_rows: dict[str, list[dict[str, Any]]] = {"small": [], "large": []}
    missing_samples = []
    for row in list(run.get("raw_results") or []):
        sample_id = str(row.get("sample_id") or "")
        patch = patch_by_sample.get(sample_id)
        if patch is None:
            missing_samples.append(sample_id)
            continue
        bucket_rows[str(patch["patch_bucket"])].append(row)
    assert not missing_samples, (tag, concurrency, missing_samples)
    assert sum(len(rows) for rows in bucket_rows.values()) == expected_requests
    per_bucket = {}
    for bucket, rows in bucket_rows.items():
        assert rows, (tag, concurrency, bucket)
        per_bucket[bucket] = {
            "count": len(rows),
            "patches": _stats(
                patch_by_sample[str(row["sample_id"])]["patches"] for row in rows
            ),
            "ttft_ms": _stats(row["ttft_ms"] for row in rows),
            "latency_ms": _stats(row["elapsed_ms"] for row in rows),
            "tpot_ms": _stats(row["tpot_ms"] for row in rows),
        }
    return {
        "gate_passed": True,
        "successful": run["successful"],
        "request_throughput_rps": run["request_throughput_rps"],
        "goodput_count": run["goodput_count"],
        "goodput_rps": run["goodput_rps"],
        "ttft_p50_ms": run["ttft_ms"]["p50"],
        "ttft_p95_ms": run["ttft_ms"]["p95"],
        "latency_p95_ms": run["latency_ms"]["p95"],
        "batcher": batcher,
        "admission": admission,
        "per_patch_bucket": per_bucket,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--expected-requests", type=int, default=16)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    manifest = json.loads(Path(args.manifest).resolve().read_text())
    patch_by_sample = {
        str(row["sample_id"]): row for row in list(manifest.get("rows") or [])
    }
    tags = ("control_before", "treatment_patch_aging", "control_after")
    payload: dict[str, Any] = {
        "schema_version": "perf310-real-mixed-patch-capability-v1",
        "model": "/data01/LWX/Qwen3-VL-8B-Instruct",
        "dataset": (
            "/data/songbinbin/Proj/Proj_LWX/mooncake_test_dataset/"
            "chat_splits/perf310-w0-patch-mix.jsonl"
        ),
        "gpus": [1, 2, 5, 6],
        "mock": False,
        "data_plane": "tcp",
        "rdma_claimed": False,
        "expected_requests": args.expected_requests,
        "runs": {},
        "comparisons": {},
    }
    for concurrency in (1, 4, 8):
        rows = {
            tag: _load_arm(
                root,
                tag,
                concurrency,
                expected_requests=args.expected_requests,
                patch_by_sample=patch_by_sample,
            )
            for tag in tags
        }
        controls = [rows["control_before"], rows["control_after"]]
        treatment = rows["treatment_patch_aging"]
        for tag, row in rows.items():
            expected_policy = "patch_aging" if tag == "treatment_patch_aging" else "fifo"
            assert row["batcher"]["scheduler_policy"] == expected_policy
            if expected_policy == "fifo":
                assert row["batcher"]["reordered_selections"] == 0
        if concurrency == 8:
            assert treatment["batcher"]["reordered_selections"] > 0

        scalar_keys = (
            "request_throughput_rps",
            "goodput_rps",
            "ttft_p50_ms",
            "ttft_p95_ms",
            "latency_p95_ms",
        )
        control_median = {
            key: statistics.median(float(row[key]) for row in controls)
            for key in scalar_keys
        }
        comparison: dict[str, Any] = {
            "control_median": control_median,
            "treatment": {key: treatment[key] for key in scalar_keys},
            "treatment_vs_control_median_pct": {
                key: _pct(treatment[key], control_median[key]) for key in scalar_keys
            },
            "per_patch_bucket": {},
        }
        for bucket in ("small", "large"):
            comparison["per_patch_bucket"][bucket] = {}
            for metric in ("ttft_ms", "latency_ms", "tpot_ms"):
                for percentile in ("p50", "p95"):
                    control_value = statistics.median(
                        float(row["per_patch_bucket"][bucket][metric][percentile])
                        for row in controls
                    )
                    treatment_value = float(
                        treatment["per_patch_bucket"][bucket][metric][percentile]
                    )
                    comparison["per_patch_bucket"][bucket][
                        f"{metric}_{percentile}"
                    ] = {
                        "control_median": control_value,
                        "treatment": treatment_value,
                        "treatment_vs_control_median_pct": _pct(
                            treatment_value, control_value
                        ),
                    }
        payload["runs"][str(concurrency)] = rows
        payload["comparisons"][str(concurrency)] = comparison

    output = Path(args.output).resolve()
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload["comparisons"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
