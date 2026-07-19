"""Fail-closed runtime evidence for native Mooncake transport selection.

The vLLM connector records the *requested* protocol in its configuration, but
an older native ``engine.so`` can log an initialization error and still select
another transport.  Benchmark summaries therefore need proof from each actual
Prefill/Decode worker, not merely the command-line value that launched it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


_NATIVE_TRANSPORT_MARKERS: dict[str, dict[str, tuple[str, ...]]] = {
    "tcp": {
        # Mooncake has two legitimate TCP initialization paths.  The explicit
        # ``MC_FORCE_TCP`` path emits the first message; a connector that
        # selects TCP directly emits the native ``TcpTransport`` listener
        # message instead.  Both are emitted by the C++ data-plane rather
        # than by the vLLM configuration layer, so either is suitable proof
        # that the worker installed TCP rather than merely *requesting* it.
        "expected": (
            "MC_FORCE_TCP is set, using TCP transport only",
            "TcpTransport: listen on port",
        ),
        "forbidden": (
            "Using Intra-Node NVLink transport",
            "Using RDMA transport (RoCE/iWARP)",
            "Using cross-node NVLink transport",
        ),
    },
    "rdma": {
        "expected": ("Using RDMA transport (RoCE/iWARP)",),
        "forbidden": (
            "MC_FORCE_TCP is set, using TCP transport only",
            "Using Intra-Node NVLink transport",
            "Using cross-node NVLink transport",
        ),
    },
    "nvlink_intra": {
        "expected": ("Using Intra-Node NVLink transport",),
        "forbidden": (
            "MC_FORCE_TCP is set",
            "Using TCP transport only",
            "Using RDMA transport (RoCE/iWARP)",
            "Using cross-node NVLink transport",
            "Protocol 'nvlink_intra' requires",
        ),
    },
}


def collect_mooncake_worker_transport_evidence(
    worker_logs: Mapping[str, str | Path],
    *,
    requested: str,
) -> dict[str, Any]:
    """Collect strict native transport proof from Prefill and Decode logs.

    ``worker_logs`` must contain the actual worker log for each expected stage,
    normally ``prefill-0``/``decode-0`` (and their additional worker indices).
    ``shm``/local paths do not use the native selector and are deliberately
    reported as not applicable rather than guessed from an unrelated log.
    """

    protocol = str(requested or "").strip().lower()
    marker_spec = _NATIVE_TRANSPORT_MARKERS.get(protocol)
    if marker_spec is None:
        return {
            "schema_version": 1,
            "requested_transport": protocol,
            "actual_transport": None,
            "applicable": False,
            "pass": True,
            "workers": [],
            "failures": [],
        }

    workers: list[dict[str, Any]] = []
    failures: list[str] = []
    seen_paths: set[Path] = set()
    expected_markers = marker_spec["expected"]
    forbidden_markers = marker_spec["forbidden"]

    for worker, raw_path in worker_logs.items():
        path = Path(raw_path)
        if path in seen_paths:
            continue
        seen_paths.add(path)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            workers.append(
                {
                    "worker": str(worker),
                    "log": str(path),
                    "expected_marker_found": False,
                    "forbidden_markers": [],
                    "read_error": f"{type(exc).__name__}: {exc}",
                }
            )
            failures.append(f"{worker}: unable to read native transport log")
            continue

        matched_markers = [marker for marker in expected_markers if marker in text]
        expected_found = bool(matched_markers)
        found_forbidden = [marker for marker in forbidden_markers if marker in text]
        workers.append(
            {
                "worker": str(worker),
                "log": str(path),
                "expected_marker_found": expected_found,
                "matched_markers": matched_markers,
                "forbidden_markers": found_forbidden,
            }
        )
        if not expected_found:
            failures.append(f"{worker}: missing native {protocol} marker")
        if found_forbidden:
            failures.append(
                f"{worker}: conflicting native transport marker(s): "
                + ", ".join(found_forbidden)
            )

    if not workers:
        failures.append("no Prefill/Decode worker logs supplied")
    passed = not failures
    return {
        "schema_version": 1,
        "requested_transport": protocol,
        "actual_transport": protocol if passed else None,
        "applicable": True,
        "pass": passed,
        "workers": workers,
        "failures": failures,
    }
