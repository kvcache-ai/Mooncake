#!/usr/bin/env python3
"""Validate a real Mooncake EPD vLLM demo/eval summary.

This gate is intentionally strict: it is for claims that the request used the
real E→P FeatureHandle/direct-engine path plus P→D Mooncake KV path.  It should
fail rather than silently accept a normal vLLM multimodal request that happened
to return HTTP 200.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping


class RealEPDGateError(AssertionError):
    """Raised when a real EPD summary does not support the claimed hot path."""


def _as_int(payload: Mapping[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(payload.get(key, default) or default)
    except Exception:
        return default


def _as_float(payload: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(payload.get(key, default) or default)
    except Exception:
        return default


def _metric_summary(summary: Mapping[str, Any]) -> Dict[str, Any]:
    for key in (
        "real_epd_metric_summary",
        "online_direct_metric_summary",
        "feature_handle_metric_summary",
    ):
        value = summary.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    metrics_payload = summary.get("metrics")
    if isinstance(metrics_payload, Mapping):
        metrics = dict(metrics_payload.get("metrics") or {})
        return {
            "requests_total": _as_int(metrics, "requests_total"),
            "requests_multimodal": _as_int(metrics, "requests_multimodal"),
            "precomputed_hits": _as_int(metrics, "mm_hidden_cache_precomputed_image_embeds_hits"),
            "hidden_cache_errors": _as_int(metrics, "mm_hidden_cache_errors"),
            "hidden_cache_full_miss_batches": _as_int(metrics, "mm_hidden_cache_full_miss_batches"),
            "hidden_cache_vision_compute_ms_avg": _as_float(metrics, "mm_hidden_cache_vision_compute_ms_avg"),
            "fallback_batches": _as_int(metrics, "fallback_batches"),
            "fallback_bytes": _as_int(metrics, "fallback_bytes"),
            "layered_receive_failures": _as_int(metrics, "layered_receive_failures"),
            "layered_transfer_failed_batches": _as_int(metrics, "layered_transfer_failed_batches"),
            "peer_buffer_batches": _as_int(metrics, "peer_buffer_batches"),
            "peer_buffer_bytes": _as_int(metrics, "peer_buffer_bytes"),
            "backend_counts": dict(metrics.get("remote_transfer_backend_counts") or {}),
        }
    return {}


def validate_real_epd_summary(summary: Mapping[str, Any]) -> None:
    """Fail unless summary proves the strict real EPD hidden-state path ran."""

    failures: List[str] = []
    response = dict(summary.get("response") or {})
    headers = {str(k).lower(): v for k, v in dict(response.get("headers") or {}).items()}
    metrics = _metric_summary(summary)
    backend_counts = dict(metrics.get("backend_counts") or {})

    if bool(summary.get("uses_mock", False)):
        failures.append("summary was produced by a mock path")
    if summary.get("strict_no_fallback") is not True:
        failures.append("strict_no_fallback is not true")
    if int(response.get("status_code", 0) or 0) != 200:
        failures.append(f"HTTP status is not 200: {response.get('status_code')}")
    if headers.get("x-epd-routing-path") != "EPD":
        failures.append(f"request did not route EPD: {headers.get('x-epd-routing-path')}")
    if int(response.get("response_content_len", 0) or 0) <= 0:
        failures.append("response content is empty")

    # A transport or source-lifetime fault can yield a non-empty HTTP 200 and
    # clean byte-count metrics while changing the model completion.  The online
    # runner enables this exact-output check only for repeated deterministic
    # request surfaces; honor that recorded requirement here so a later gate
    # invocation cannot relabel a quality-failed artifact as a real EPD pass.
    # New artifacts cover both warmup and measured requests. A cold-start
    # corruption is a correctness failure even when a warmed cache happens to
    # produce stable completions; retain the measured-only fallback for older
    # artifacts.
    response_consistency = summary.get(
        "all_response_consistency", summary.get("response_consistency")
    )
    if isinstance(response_consistency, Mapping) and bool(
        response_consistency.get("required", False)
    ):
        if not bool(response_consistency.get("applicable", False)):
            failures.append(
                "deterministic response-consistency gate was required but no request repeated"
            )
        elif not bool(response_consistency.get("pass", False)):
            inconsistent = list(
                response_consistency.get("inconsistent_group_ids") or []
            )
            failures.append(
                "deterministic response-consistency failure"
                + (f": groups={inconsistent}" if inconsistent else "")
            )

    reference_equivalence = summary.get("reference_response_equivalence")
    if isinstance(reference_equivalence, Mapping) and bool(
        reference_equivalence.get("required", False)
    ):
        if not bool(reference_equivalence.get("applicable", False)):
            failures.append(
                "reference response-equivalence gate was required but no comparable responses exist"
            )
        elif not bool(reference_equivalence.get("pass", False)):
            unexpected = dict(
                reference_equivalence.get("unexpected_candidate_hashes_by_group")
                or {}
            )
            missing = list(
                reference_equivalence.get("missing_reference_group_ids") or []
            )
            details: List[str] = []
            if missing:
                details.append(f"missing_groups={missing}")
            if unexpected:
                details.append(f"unexpected_hash_groups={sorted(unexpected)}")
            failures.append(
                "reference response-equivalence failure"
                + (f": {', '.join(details)}" if details else "")
            )

    if _as_int(metrics, "requests_multimodal") < 1:
        failures.append("requests_multimodal < 1")
    if _as_int(metrics, "precomputed_hits") < 1:
        failures.append("Prefill did not consume precomputed image embeds")
    if _as_float(metrics, "hidden_cache_vision_compute_ms_avg") != 0.0:
        failures.append("Prefill vision encoder ran instead of using precomputed hidden states")
    if _as_int(metrics, "hidden_cache_errors") != 0:
        failures.append("hidden cache errors observed")
    if _as_int(metrics, "hidden_cache_full_miss_batches") != 0:
        failures.append("hidden cache full misses observed")
    if _as_int(metrics, "fallback_batches") != 0 or _as_int(metrics, "fallback_bytes") != 0:
        failures.append("Mooncake transfer fallback observed")
    if _as_int(metrics, "layered_receive_failures") != 0:
        failures.append("layered receive failures observed")
    if _as_int(metrics, "layered_transfer_failed_batches") != 0:
        failures.append("layered transfer failed batches observed")
    if _as_int(metrics, "peer_buffer_batches") <= 0:
        failures.append("peer_buffer_batches <= 0")
    if _as_int(metrics, "peer_buffer_bytes") <= 0:
        failures.append("peer_buffer_bytes <= 0")
    if int(backend_counts.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("peer_buffer_direct backend did not run")
    unexpected = {
        str(k): int(v or 0)
        for k, v in backend_counts.items()
        if str(k) != "peer_buffer_direct" and int(v or 0) != 0
    }
    if unexpected:
        failures.append(f"unexpected transfer backends: {unexpected}")

    direct_stats = dict(summary.get("direct_buffer_stats_after_release") or {})
    persistent_cache = bool(direct_stats.get("persistent_cache", False))
    # In persistent direct-buffer mode the production hot path intentionally
    # keeps registered peer-buffer tensors cached after request release so the
    # next same-feature request avoids CUDA allocation/registration.  That is
    # not a leak as long as no request still holds a ref.  Non-persistent mode
    # must still release all allocations.
    if persistent_cache:
        if _as_int(direct_stats, "ref_count") != 0:
            failures.append("Prefill direct feature persistent cache still has active refs")
    elif _as_int(direct_stats, "allocations") != 0:
        failures.append("Prefill direct feature buffers were not released")

    if failures:
        raise RealEPDGateError("; ".join(failures))


def load_summary(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser(description="Gate a real strict Mooncake EPD summary.")
    ap.add_argument("--summary", required=True, help="Path to real_qwenvl_epd_demo_summary.json or equivalent")
    ap.add_argument("--json", action="store_true", help="Print machine-readable verdict")
    args = ap.parse_args()

    summary = load_summary(args.summary)
    try:
        validate_real_epd_summary(summary)
    except RealEPDGateError as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        raise SystemExit(str(exc))
    if args.json:
        print(json.dumps({"ok": True}, ensure_ascii=False))
    else:
        print("real EPD gate passed")


if __name__ == "__main__":
    main()
