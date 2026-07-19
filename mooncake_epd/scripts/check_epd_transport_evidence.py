#!/usr/bin/env python3
"""Reject transport claims whose artifact lacks matching backend evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _walk(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key), child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def evaluate_transport_evidence(
    payload: Dict[str, Any],
    *,
    requested: str,
    runtime_evidence_key: str = "transport_runtime_evidence",
) -> Dict[str, Any]:
    requested = str(requested).lower()
    labels: List[str] = []
    runtime_evidence = payload.get(runtime_evidence_key)
    runtime_failures: List[str] = []
    if isinstance(runtime_evidence, dict):
        runtime_requested = str(runtime_evidence.get("requested_transport", "")).lower()
        runtime_actual = str(runtime_evidence.get("actual_transport", "")).lower()
        if runtime_actual:
            labels.append(runtime_actual)
        if runtime_requested != requested:
            runtime_failures.append(
                "runtime_transport_evidence_requested_transport_mismatch"
            )
        if not bool(runtime_evidence.get("pass", False)):
            runtime_failures.append("runtime_transport_evidence_failed")
        if runtime_actual != requested:
            runtime_failures.append(
                f"runtime_transport_evidence_actual_transport_is_not_{requested}"
            )
    for key, value in _walk(payload):
        # A requested CLI ``protocol`` is not proof of the backend that the
        # native engine selected.  Only explicit actual/backend fields count.
        if key.lower() in {
            "backend",
            "actual_backend",
            "actual_transport",
            "backend_label",
        }:
            labels.append(str(value).lower())
    labels = sorted(set(labels))
    failures: List[str] = list(runtime_failures)
    if requested == "rdma":
        if not any("rdma" in label for label in labels):
            failures.append("artifact_has_no_actual_rdma_backend_label")
        required = ("nic", "gid", "remote_host")
        text = json.dumps(payload, sort_keys=True).lower()
        for field in required:
            if field not in text:
                failures.append(f"rdma_metadata_missing:{field}")
    elif requested in {"tcp", "posix_shm", "cuda_ipc", "nvlink_intra"}:
        if not any(requested in label for label in labels):
            failures.append(f"artifact_has_no_actual_{requested}_backend_label")
    return {
        "pass": not failures,
        "requested_transport": requested,
        "runtime_evidence_key": runtime_evidence_key,
        "observed_labels": labels,
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--requested", required=True, choices=["tcp", "rdma", "posix_shm", "cuda_ipc", "nvlink_intra"])
    parser.add_argument(
        "--runtime-evidence-key",
        default="transport_runtime_evidence",
        help="Summary field containing the worker-side native evidence.",
    )
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args()
    result = evaluate_transport_evidence(
        json.loads(Path(args.artifact).read_text(encoding="utf-8")),
        requested=args.requested,
        runtime_evidence_key=args.runtime_evidence_key,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.enforce and not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
