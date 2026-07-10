#!/usr/bin/env python3
"""Validate Mooncake EPD real-run artifacts against no-mock/no-fallback gates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


def _load(path: str) -> Dict[str, Any]:
    p = Path(path).expanduser()
    if not p.exists():
        raise FileNotFoundError(str(p))
    return json.loads(p.read_text(encoding="utf-8"))


def _nested(data: Dict[str, Any], *keys: str, default=None):
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def check_serving_summary(path: str) -> List[str]:
    data = _load(path)
    metrics = _nested(data, "metrics", "metrics", default={}) or {}
    backend_counts = metrics.get("remote_transfer_backend_counts") or {}
    failures: List[str] = []
    if int(metrics.get("layered_receive_failures", 0) or 0) != 0:
        failures.append("layered_receive_failures != 0")
    if int(metrics.get("layered_transfer_failed_batches", 0) or 0) != 0:
        failures.append("layered_transfer_failed_batches != 0")
    if int(metrics.get("fallback_batches", 0) or 0) != 0:
        failures.append("fallback_batches != 0")
    if int(metrics.get("peer_buffer_batches", 0) or 0) <= 0:
        failures.append("peer_buffer_batches <= 0")
    if int(backend_counts.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("peer_buffer_direct <= 0")
    if int(data.get("dataset_probe_requested", 0) or 0) > 0:
        if int(data.get("dataset_probe_success", 0) or 0) != int(data.get("dataset_probe_requested", 0) or 0):
            failures.append("dataset_probe_success != dataset_probe_requested")
    return [f"{path}: {item}" for item in failures]


def check_feature_summary(path: str) -> List[str]:
    data = _load(path)
    metric = data.get("feature_handle_metric_summary") or data.get("feature_metric_summary") or {}
    gates = data.get("correctness_gates") or {}
    failures: List[str] = []
    if int(metric.get("fallback_batches", 0) or 0) != 0:
        failures.append("feature fallback_batches != 0")
    if int(metric.get("layered_receive_failures", 0) or 0) != 0:
        failures.append("feature layered_receive_failures != 0")
    if int(metric.get("layered_transfer_failed_batches", 0) or 0) != 0:
        failures.append("feature layered_transfer_failed_batches != 0")
    backend = metric.get("backend_counts") or {}
    if int(backend.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("feature peer_buffer_direct <= 0")
    if gates:
        for key in ("all_http_200", "all_epd_routing", "fallback_zero", "transfer_failures_zero", "peer_buffer_direct"):
            if key in gates and not bool(gates[key]):
                failures.append(f"correctness_gates.{key} is false")
    return [f"{path}: {item}" for item in failures]


def check_agent_clone_summary(path: str) -> List[str]:
    data = _load(path)
    failures: List[str] = []
    if int(data.get("zero_copy_clone_bytes", -1)) != 0:
        failures.append("zero_copy_clone_bytes != 0")
    if int(_nested(data, "store_stats_after_release", "states", default=-1)) != 0:
        failures.append("store states remain after release")
    if int(_nested(data, "page_manager_stats_after_release", "directory_orphans", default=-1)) != 0:
        failures.append("directory_orphans != 0")
    if float(data.get("speedup_avg", 0.0) or 0.0) <= 1.0:
        failures.append("zero-copy clone speedup_avg <= 1.0")
    return [f"{path}: {item}" for item in failures]


def main() -> None:
    ap = argparse.ArgumentParser(description="Check real Mooncake EPD artifact gates")
    ap.add_argument("--serving-summary", action="append", default=[])
    ap.add_argument("--feature-summary", action="append", default=[])
    ap.add_argument("--agent-clone-summary", action="append", default=[])
    args = ap.parse_args()

    failures: List[str] = []
    for path in args.serving_summary:
        failures.extend(check_serving_summary(path))
    for path in args.feature_summary:
        failures.extend(check_feature_summary(path))
    for path in args.agent_clone_summary:
        failures.extend(check_agent_clone_summary(path))
    result = {"ok": not failures, "failures": failures}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
