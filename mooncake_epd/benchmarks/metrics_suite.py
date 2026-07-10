"""RFC §8.3 metrics collector (all 14 headline metrics).

The ``MetricsCollector`` is a thread-safe sink for every metric the RFC
mandates. Tests and benchmarks push observations through ``record_*``
methods; ``report()`` returns a flat dict suitable for JSON export.

The 14 metrics:

- TTFT (P50 / P95)
- TPOT (ms)
- End-to-end throughput (req/s, tokens/s)
- JCT (ms)
- Fork overhead (ms)
- Prefix hit rate (%)
- Encoder skip rate (%)
- Cross-step reuse rate (%)
- Turn-2+ TTFT reduction (%)
- A2A handoff latency (ms)
- KV transfer bandwidth (GB/s)
- GPU peak memory (GB)
- Quality / task accuracy
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

from .disagg_baselines import build_baseline_matrix
from .rfc_eval_workloads import build_dev_small_workloads, summarize_workloads


def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = (len(s) - 1) * p
    f = int(k)
    c = f + 1
    if c >= len(s):
        return float(s[-1])
    return float(s[f] + (s[c] - s[f]) * (k - f))


def _extract_ablation_latency(payload: Dict | None) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ttft_ms", "latency_ms", "total_ms"):
        value = payload.get(key)
        if value is not None:
            return float(value)
    return None


def _build_ablation_gap_table(phase6_metrics: Dict) -> List[Dict]:
    ablations = dict(phase6_metrics.get("ablations") or {})
    comparisons = [
        {
            "id": "b2_vs_b1",
            "label": "Native PD → Prefix Cache",
            "base": "B1",
            "target": "B2",
            "effect_type": "gain",
        },
        {
            "id": "b3_vs_b1",
            "label": "Native PD → Feature Cache",
            "base": "B1",
            "target": "B3",
            "effect_type": "gain",
        },
        {
            "id": "b7_vs_b3",
            "label": "Feature Cache → Exact Reuse",
            "base": "B3",
            "target": "B7",
            "effect_type": "gain",
        },
        {
            "id": "b8_vs_b7",
            "label": "Exact Reuse → Approx Reuse",
            "base": "B7",
            "target": "B8",
            "effect_type": "penalty",
        },
    ]
    rows: List[Dict] = []
    for item in comparisons:
        base_ttft = _extract_ablation_latency(ablations.get(item["base"]))
        target_ttft = _extract_ablation_latency(ablations.get(item["target"]))
        if base_ttft is None or target_ttft is None:
            continue
        if item["effect_type"] == "gain":
            effect_ms = base_ttft - target_ttft
            effect_pct = (effect_ms / base_ttft * 100.0) if base_ttft > 0 else 0.0
        else:
            effect_ms = target_ttft - base_ttft
            effect_pct = (effect_ms / base_ttft * 100.0) if base_ttft > 0 else 0.0
        rows.append(
            {
                "id": item["id"],
                "label": item["label"],
                "base": item["base"],
                "target": item["target"],
                "effect_type": item["effect_type"],
                "base_ttft_ms": base_ttft,
                "target_ttft_ms": target_ttft,
                "effect_ms": effect_ms,
                "effect_pct": effect_pct,
            }
        )
    return rows


def _build_serving_correctness_table(serving_baseline_compare: Dict[str, Any]) -> Dict[str, Any]:
    if not serving_baseline_compare:
        return {"available": False}

    summary = dict(serving_baseline_compare.get("summary") or {})
    rows = list(serving_baseline_compare.get("rows") or [])
    compare_thresholds = dict(serving_baseline_compare.get("compare_thresholds") or {})
    gate = dict(summary.get("gate") or {})

    high_overlap_threshold = float(
        gate.get(
            "high_overlap_threshold",
            compare_thresholds.get("high_overlap_threshold", 0.70),
        )
        or 0.70
    )
    min_pass_rate = float(
        gate.get("min_pass_rate", compare_thresholds.get("min_pass_rate", 0.80)) or 0.80
    )
    min_same_finish_reason_rate = float(
        gate.get(
            "min_same_finish_reason_rate",
            compare_thresholds.get("min_same_finish_reason_rate", 0.80),
        )
        or 0.80
    )
    min_avg_token_jaccard = float(
        gate.get(
            "min_avg_token_jaccard",
            compare_thresholds.get("min_avg_token_jaccard", 0.75),
        )
        or 0.75
    )
    min_token_jaccard_threshold = float(
        gate.get(
            "min_token_jaccard_threshold",
            compare_thresholds.get("min_token_jaccard", 0.60),
        )
        or 0.60
    )

    def _avg(values: list[float]) -> float:
        return (sum(values) / len(values)) if values else 0.0

    token_jaccards: list[float] = []
    same_finish_reason_count = 0
    high_overlap_count = 0
    normalized_exact_or_high_overlap_count = 0
    control_plane_equivalent_count = 0
    structural_completion_equivalent_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        text_metrics = dict(row.get("text_metrics") or {})
        flags = dict(row.get("comparison_flags") or {})
        baseline = dict(row.get("baseline") or {})
        serving = dict(row.get("serving") or {})
        normalized_exact = bool(text_metrics.get("normalized_exact_match"))
        token_jaccard = float(text_metrics.get("token_jaccard", 0.0) or 0.0)
        same_finish_reason = str(baseline.get("finish_reason") or "") == str(
            serving.get("finish_reason") or ""
        )
        high_overlap = token_jaccard >= high_overlap_threshold
        token_jaccards.append(token_jaccard)
        if same_finish_reason:
            same_finish_reason_count += 1
        if high_overlap:
            high_overlap_count += 1
        if normalized_exact or (same_finish_reason and high_overlap):
            normalized_exact_or_high_overlap_count += 1
        if bool(flags.get("structural_completion_equivalent")):
            structural_completion_equivalent_count += 1
        if bool(flags.get("control_plane_equivalent")) or normalized_exact or (same_finish_reason and high_overlap):
            control_plane_equivalent_count += 1

    count = int(summary.get("count", len(rows)) or 0)
    exact_match_rate = float(summary.get("exact_match_rate", 0.0) or 0.0)
    normalized_exact_match_rate = float(
        summary.get("normalized_exact_match_rate", 0.0) or 0.0
    )
    avg_token_jaccard = float(summary.get("avg_token_jaccard", _avg(token_jaccards)) or 0.0)
    min_token_jaccard = float(
        summary.get("min_token_jaccard", min(token_jaccards) if token_jaccards else 0.0)
        or 0.0
    )
    same_finish_reason_rate = float(
        summary.get(
            "same_finish_reason_rate",
            (same_finish_reason_count / count) if count else 0.0,
        )
        or 0.0
    )
    normalized_exact_or_high_overlap_rate = float(
        summary.get(
            "normalized_exact_or_high_overlap_rate",
            (
                normalized_exact_or_high_overlap_count / count
                if count
                else 0.0
            ),
        )
        or 0.0
    )
    control_plane_equivalent_rate = float(
        summary.get(
            "control_plane_equivalent_rate",
            (control_plane_equivalent_count / count if count else 0.0),
        )
        or 0.0
    )
    structural_completion_equivalent_rate = float(
        summary.get(
            "structural_completion_equivalent_rate",
            (structural_completion_equivalent_count / count if count else 0.0),
        )
        or 0.0
    )
    high_overlap_rate = (
        high_overlap_count / count if count else 0.0
    )

    gate_failures = list(summary.get("gate_failures") or gate.get("gate_failures") or [])
    pass_recommendation = summary.get("pass_recommendation")
    if pass_recommendation is None:
        if count <= 0:
            gate_failures.append("no rows compared")
        if same_finish_reason_rate < min_same_finish_reason_rate:
            gate_failures.append(
                f"same_finish_reason_rate {same_finish_reason_rate:.4f} < {min_same_finish_reason_rate:.4f}"
            )
        if normalized_exact_or_high_overlap_rate < min_pass_rate:
            gate_failures.append(
                f"normalized_exact_or_high_overlap_rate {normalized_exact_or_high_overlap_rate:.4f} < {min_pass_rate:.4f}"
            )
        if avg_token_jaccard < min_avg_token_jaccard:
            gate_failures.append(
                f"avg_token_jaccard {avg_token_jaccard:.4f} < {min_avg_token_jaccard:.4f}"
            )
        if min_token_jaccard < min_token_jaccard_threshold:
            gate_failures.append(
                f"min_token_jaccard {min_token_jaccard:.4f} < {min_token_jaccard_threshold:.4f}"
            )
        pass_recommendation = not gate_failures
    else:
        pass_recommendation = bool(pass_recommendation)

    pass_recommendation_label = str(summary.get("pass_recommendation_label", "") or "")
    if not pass_recommendation_label:
        pass_recommendation_label = "pass" if pass_recommendation else "review"

    return {
        "available": True,
        "count": count,
        "exact_match_rate": exact_match_rate,
        "normalized_exact_match_rate": normalized_exact_match_rate,
        "avg_token_jaccard": avg_token_jaccard,
        "min_token_jaccard": min_token_jaccard,
        "same_finish_reason_rate": same_finish_reason_rate,
        "normalized_exact_or_high_overlap_rate": normalized_exact_or_high_overlap_rate,
        "control_plane_equivalent_rate": control_plane_equivalent_rate,
        "control_plane_equivalent_count": int(
            summary.get("control_plane_equivalent_count", control_plane_equivalent_count) or 0
        ),
        "structural_completion_equivalent_rate": structural_completion_equivalent_rate,
        "structural_completion_equivalent_count": int(
            summary.get("structural_completion_equivalent_count", structural_completion_equivalent_count) or 0
        ),
        "pass_recommendation": pass_recommendation,
        "pass_recommendation_label": pass_recommendation_label,
        "gate_failures": gate_failures,
        "gate_warnings": list(summary.get("gate_warnings") or gate.get("gate_warnings") or []),
        "high_overlap_threshold": high_overlap_threshold,
        "min_pass_rate": min_pass_rate,
        "min_same_finish_reason_rate": min_same_finish_reason_rate,
        "min_avg_token_jaccard_threshold": min_avg_token_jaccard,
        "min_token_jaccard_threshold": min_token_jaccard_threshold,
        "baseline_elapsed_ms_avg": float(summary.get("baseline_elapsed_ms_avg", 0.0) or 0.0),
        "serving_elapsed_ms_avg": float(summary.get("serving_elapsed_ms_avg", 0.0) or 0.0),
        "high_overlap_rate": high_overlap_rate,
        "high_overlap_count": high_overlap_count,
        "same_finish_reason_count": same_finish_reason_count,
        "normalized_exact_or_high_overlap_count": normalized_exact_or_high_overlap_count,
    }


def _build_serving_e2e_sanity_table(serving_e2e_summary: Dict) -> Dict:
    serving_metrics = dict((serving_e2e_summary.get("metrics") or {}).get("metrics") or {})
    backend_counts = dict(serving_metrics.get("remote_transfer_backend_counts") or {})
    requests_total = int(serving_metrics.get("requests_total", 0) or 0)
    grouped_batches = int(serving_metrics.get("layered_transfer_grouped_batches", 0) or 0)
    receive_batches = int(serving_metrics.get("layered_receive_group_batches", 0) or 0)
    finished_reqs = int(serving_metrics.get("layered_receive_finished_reqs", 0) or 0)
    peer_batches = int(serving_metrics.get("peer_buffer_batches", 0) or 0)
    direct_batches = int(backend_counts.get("peer_buffer_direct", 0) or 0)
    wait_calls = int(serving_metrics.get("layer_load_wait_calls", 0) or 0)
    receive_failures = int(serving_metrics.get("layered_receive_failures", 0) or 0)
    connector_workers = int(serving_metrics.get("connector_metric_workers", 0) or 0)
    grouped_descriptors = int(
        serving_metrics.get("layered_transfer_grouped_descriptors", 0) or 0
    )

    table = {
        "available": bool(serving_e2e_summary),
        "connector_workers_ok": connector_workers >= 2,
        "group_batch_parity_ok": (
            grouped_batches > 0
            and receive_batches > 0
            and grouped_batches >= receive_batches
        ),
        "finished_request_parity_ok": requests_total > 0 and finished_reqs >= requests_total,
        "peer_backend_parity_ok": peer_batches > 0 and direct_batches == peer_batches == grouped_batches,
        "layer_wait_present": receive_batches == 0 or wait_calls > 0,
        "receive_failures_zero": receive_failures == 0,
        "group_batches_multiple_of_requests": (
            requests_total > 0 and grouped_batches >= requests_total
        ),
        "wait_calls_multiple_of_receive_batches": (
            receive_batches > 0 and wait_calls >= receive_batches
        ),
        "descriptors_multiple_of_group_batches": (
            grouped_batches > 0 and grouped_descriptors >= grouped_batches
        ),
        "wait_calls_per_receive_batch": (
            float(wait_calls) / float(receive_batches) if receive_batches > 0 else 0.0
        ),
        "descriptors_per_group_batch": (
            float(grouped_descriptors) / float(grouped_batches)
            if grouped_batches > 0 else 0.0
        ),
        "group_batches_per_request": (
            float(grouped_batches) / float(requests_total) if requests_total > 0 else 0.0
        ),
    }
    table["sanity_pass"] = bool(table["available"]) and all(
        bool(table[key])
        for key in (
            "connector_workers_ok",
            "group_batch_parity_ok",
            "finished_request_parity_ok",
            "peer_backend_parity_ok",
            "layer_wait_present",
            "receive_failures_zero",
            "group_batches_multiple_of_requests",
            "wait_calls_multiple_of_receive_batches",
            "descriptors_multiple_of_group_batches",
        )
    )
    return table



def _build_feature_handle_e2e_table(feature_handle_e2e_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Extract hot-path proof metrics from the real FeatureHandle E2E artifact.

    This artifact is intentionally separate from the generic serving E2E smoke:
    the FeatureHandle run is a single multimodal EPD request whose success means
    vLLM consumed external packed vision hidden states and skipped local vision
    encoder execution on Prefill.
    """
    if not feature_handle_e2e_summary:
        return {"available": False, "pass": False}

    response = dict(feature_handle_e2e_summary.get("response") or {})
    headers = dict(response.get("headers") or {})
    metric_summary = dict(feature_handle_e2e_summary.get("feature_handle_metric_summary") or {})
    if not metric_summary:
        metrics = dict((feature_handle_e2e_summary.get("metrics") or {}).get("metrics") or {})
        metric_summary = {
            "requests_total": int(metrics.get("requests_total", 0) or 0),
            "requests_multimodal": int(metrics.get("requests_multimodal", 0) or 0),
            "precomputed_hits": int(metrics.get("mm_hidden_cache_precomputed_image_embeds_hits", 0) or 0),
            "hidden_cache_errors": int(metrics.get("mm_hidden_cache_errors", 0) or 0),
            "hidden_cache_full_miss_batches": int(metrics.get("mm_hidden_cache_full_miss_batches", 0) or 0),
            "hidden_cache_vision_compute_ms_avg": float(metrics.get("mm_hidden_cache_vision_compute_ms_avg", 0.0) or 0.0),
            "fallback_batches": int(metrics.get("fallback_batches", 0) or 0),
            "fallback_bytes": int(metrics.get("fallback_bytes", 0) or 0),
            "layered_receive_failures": int(metrics.get("layered_receive_failures", 0) or 0),
            "layered_transfer_failed_batches": int(metrics.get("layered_transfer_failed_batches", 0) or 0),
            "peer_buffer_batches": int(metrics.get("peer_buffer_batches", 0) or 0),
            "peer_buffer_bytes": int(metrics.get("peer_buffer_bytes", 0) or 0),
            "backend_counts": dict(metrics.get("remote_transfer_backend_counts") or {}),
        }

    backend_counts = dict(metric_summary.get("backend_counts") or {})
    status_code = int(response.get("status_code", 0) or 0)
    route = str(headers.get("x-epd-routing-path") or "")
    response_content_len = int(response.get("response_content_len", 0) or 0)
    precomputed_hits = int(metric_summary.get("precomputed_hits", 0) or 0)
    hidden_cache_errors = int(metric_summary.get("hidden_cache_errors", 0) or 0)
    full_miss_batches = int(metric_summary.get("hidden_cache_full_miss_batches", 0) or 0)
    vision_compute_ms_avg = float(metric_summary.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0)
    fallback_batches = int(metric_summary.get("fallback_batches", 0) or 0)
    fallback_bytes = int(metric_summary.get("fallback_bytes", 0) or 0)
    receive_failures = int(metric_summary.get("layered_receive_failures", 0) or 0)
    transfer_failed_batches = int(metric_summary.get("layered_transfer_failed_batches", 0) or 0)
    peer_buffer_batches = int(metric_summary.get("peer_buffer_batches", 0) or 0)
    peer_buffer_bytes = int(metric_summary.get("peer_buffer_bytes", 0) or 0)
    direct_batches = int(backend_counts.get("peer_buffer_direct", 0) or 0)
    unexpected_backends = {
        str(k): int(v or 0)
        for k, v in backend_counts.items()
        if str(k) != "peer_buffer_direct" and int(v or 0) != 0
    }

    gates = {
        "http_ok": status_code == 200,
        "epd_route_ok": route == "EPD",
        "non_empty_response": response_content_len > 0,
        "multimodal_request_seen": int(metric_summary.get("requests_multimodal", 0) or 0) >= 1,
        "precomputed_hit_ok": precomputed_hits >= 1,
        "hidden_cache_errors_zero": hidden_cache_errors == 0,
        "full_miss_zero": full_miss_batches == 0,
        "vision_compute_skipped": vision_compute_ms_avg == 0.0,
        "fallback_zero": fallback_batches == 0 and fallback_bytes == 0,
        "transfer_failures_zero": receive_failures == 0 and transfer_failed_batches == 0,
        "peer_buffer_direct_ok": peer_buffer_batches > 0 and direct_batches > 0,
        "unexpected_backends_zero": not unexpected_backends,
    }
    failed_gates = [key for key, ok in gates.items() if not ok]
    return {
        "available": True,
        "pass": not failed_gates,
        "failed_gates": failed_gates,
        "status_code": status_code,
        "route": route,
        "response_content_len": response_content_len,
        "elapsed_ms": float(response.get("elapsed_ms", 0.0) or 0.0),
        "requests_total": int(metric_summary.get("requests_total", 0) or 0),
        "requests_multimodal": int(metric_summary.get("requests_multimodal", 0) or 0),
        "precomputed_hits": precomputed_hits,
        "hidden_cache_errors": hidden_cache_errors,
        "hidden_cache_full_miss_batches": full_miss_batches,
        "hidden_cache_vision_compute_ms_avg": vision_compute_ms_avg,
        "fallback_batches": fallback_batches,
        "fallback_bytes": fallback_bytes,
        "layered_receive_failures": receive_failures,
        "layered_transfer_failed_batches": transfer_failed_batches,
        "peer_buffer_batches": peer_buffer_batches,
        "peer_buffer_bytes": peer_buffer_bytes,
        "peer_buffer_direct_batches": direct_batches,
        "backend_counts": backend_counts,
        "unexpected_backends": unexpected_backends,
        "gates": gates,
    }


def _build_feature_handle_paired_table(feature_handle_paired_benchmark: Dict[str, Any]) -> Dict[str, Any]:
    if not feature_handle_paired_benchmark:
        return {"available": False, "pass": False}
    paired = dict(feature_handle_paired_benchmark.get("paired_summary") or feature_handle_paired_benchmark)
    if not paired:
        return {"available": False, "pass": False}
    asset_metrics = dict(paired.get("asset_metric_summary") or {})
    feature_metrics = dict(paired.get("feature_metric_summary") or {})
    return {
        "available": True,
        "pass": bool(paired.get("pass")),
        "failed_gates": list(paired.get("failed_gates") or []),
        "asset_ttft_ms": float(paired.get("asset_ttft_ms", 0.0) or 0.0),
        "feature_ttft_ms": float(paired.get("feature_ttft_ms", 0.0) or 0.0),
        "ttft_delta_ms": float(paired.get("ttft_delta_ms", 0.0) or 0.0),
        "ttft_reduction_pct": float(paired.get("ttft_reduction_pct", 0.0) or 0.0),
        "asset_elapsed_ms": float(paired.get("asset_elapsed_ms", 0.0) or 0.0),
        "feature_elapsed_ms": float(paired.get("feature_elapsed_ms", 0.0) or 0.0),
        "elapsed_delta_ms": float(paired.get("elapsed_delta_ms", 0.0) or 0.0),
        "elapsed_reduction_pct": float(paired.get("elapsed_reduction_pct", 0.0) or 0.0),
        "asset_goodput_rps": float(paired.get("asset_goodput_rps", 0.0) or 0.0),
        "feature_goodput_rps": float(paired.get("feature_goodput_rps", 0.0) or 0.0),
        "goodput_gain_pct": float(paired.get("goodput_gain_pct", 0.0) or 0.0),
        "feature_precomputed_hits": int(feature_metrics.get("precomputed_hits", 0) or 0),
        "feature_vision_compute_ms_avg": float(feature_metrics.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0),
        "feature_peer_buffer_direct_batches": int((feature_metrics.get("backend_counts") or {}).get("peer_buffer_direct", 0) or 0),
        "feature_fallback_batches": int(feature_metrics.get("fallback_batches", 0) or 0),
        "feature_layered_receive_failures": int(feature_metrics.get("layered_receive_failures", 0) or 0),
        "asset_precomputed_hits": int(asset_metrics.get("precomputed_hits", 0) or 0),
        "asset_vision_compute_ms_avg": float(asset_metrics.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0),
        "text_token_jaccard": float((paired.get("text_metrics") or {}).get("token_jaccard", 0.0) or 0.0),
        "gates": dict(paired.get("gates") or {}),
    }

def _build_b8_penalty_table(phase6_metrics: Dict) -> Dict:
    ablations = dict(phase6_metrics.get("ablations") or {})
    b7 = dict(ablations.get("B7") or {})
    b8 = dict(ablations.get("B8") or {})
    b7_ttft = _extract_ablation_latency(b7)
    b8_ttft = _extract_ablation_latency(b8)
    if b7_ttft is None or b8_ttft is None:
        return {}

    b7_rows = list(b7.get("rows") or [])
    b8_rows = list(b8.get("rows") or [])
    per_scenario = []
    penalties: list[float] = []
    reuse_ratios: list[float] = []
    delta_reuse_ratios: list[float] = []
    tier3_pages: list[float] = []
    tier3_tokens: list[float] = []
    tier3_mean_similarities: list[float] = []
    tier2_tokens: list[float] = []
    tier2_recomputed_tokens: list[float] = []
    delta_prefill_ms_values: list[float] = []
    pipeline_overhead_ms_values: list[float] = []
    relay_segments_values: list[float] = []
    relay_recompute_segments_values: list[float] = []
    b7_by_key = {
        (str(row.get("scenario", "")), str(row.get("image", ""))): row
        for row in b7_rows
        if isinstance(row, dict)
    }
    for row in b8_rows:
        if not isinstance(row, dict):
            continue
        key = (str(row.get("scenario", "")), str(row.get("image", "")))
        base_row = b7_by_key.get(key)
        if not isinstance(base_row, dict):
            continue
        base_ttft = float(base_row.get("ttft_ms", 0.0) or 0.0)
        target_ttft = float(row.get("ttft_ms", 0.0) or 0.0)
        penalty_ms = target_ttft - base_ttft
        reuse_ratio = float(row.get("reuse_ratio", 0.0) or 0.0)
        delta_reuse_ratio = float(row.get("delta_reuse_ratio", 0.0) or 0.0)
        tier2_reused_tokens = float(row.get("tier2_reused_tokens", 0.0) or 0.0)
        tier2_recomputed = float(row.get("tier2_recomputed_tokens", 0.0) or 0.0)
        tier3_accepted_pages = float(row.get("tier3_accepted_pages", 0.0) or 0.0)
        tier3_reused_tokens = float(row.get("tier3_reused_tokens", 0.0) or 0.0)
        tier3_mean_similarity = float(row.get("tier3_mean_similarity", 0.0) or 0.0)
        delta_prefill_ms = float(row.get("delta_prefill_ms", 0.0) or 0.0)
        pipeline_overhead_ms = float(row.get("pipeline_overhead_ms", 0.0) or 0.0)
        relay_segments = float(row.get("relay_segments", 0.0) or 0.0)
        relay_recompute_segments = float(row.get("relay_recompute_segments", 0.0) or 0.0)
        penalties.append(penalty_ms)
        reuse_ratios.append(reuse_ratio)
        delta_reuse_ratios.append(delta_reuse_ratio)
        tier3_pages.append(tier3_accepted_pages)
        tier3_tokens.append(tier3_reused_tokens)
        tier3_mean_similarities.append(tier3_mean_similarity)
        tier2_tokens.append(tier2_reused_tokens)
        tier2_recomputed_tokens.append(tier2_recomputed)
        delta_prefill_ms_values.append(delta_prefill_ms)
        pipeline_overhead_ms_values.append(pipeline_overhead_ms)
        relay_segments_values.append(relay_segments)
        relay_recompute_segments_values.append(relay_recompute_segments)
        per_scenario.append(
            {
                "scenario": key[0] or "unknown",
                "image": key[1],
                "b7_ttft_ms": base_ttft,
                "b8_ttft_ms": target_ttft,
                "penalty_ms": penalty_ms,
                "penalty_pct": (penalty_ms / base_ttft * 100.0) if base_ttft > 0 else 0.0,
                "reuse_ratio": reuse_ratio,
                "delta_reuse_ratio": delta_reuse_ratio,
                "tier2_reused_tokens": tier2_reused_tokens,
                "tier2_recomputed_tokens": tier2_recomputed,
                "tier3_accepted_pages": tier3_accepted_pages,
                "tier3_reused_tokens": tier3_reused_tokens,
                "tier3_mean_similarity": tier3_mean_similarity,
                "delta_prefill_ms": delta_prefill_ms,
                "pipeline_overhead_ms": pipeline_overhead_ms,
                "relay_segments": relay_segments,
                "relay_recompute_segments": relay_recompute_segments,
            }
        )

    def _corr(xs: list[float], ys: list[float]) -> float:
        if len(xs) != len(ys) or len(xs) < 2:
            return 0.0
        mx = sum(xs) / len(xs)
        my = sum(ys) / len(ys)
        dx = sum((x - mx) ** 2 for x in xs) ** 0.5
        dy = sum((y - my) ** 2 for y in ys) ** 0.5
        if dx == 0.0 or dy == 0.0:
            return 0.0
        return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (dx * dy)

    max_penalty_row = max(per_scenario, key=lambda row: row["penalty_ms"], default={})
    min_penalty_row = min(per_scenario, key=lambda row: row["penalty_ms"], default={})

    def _avg_from_rows(key: str) -> float:
        values = [float(row.get(key, 0.0) or 0.0) for row in b8_rows if isinstance(row, dict)]
        return (sum(values) / len(values)) if values else 0.0

    avg_delta_prefill_ms = _avg_from_rows("delta_prefill_ms")
    avg_pipeline_overhead_ms = _avg_from_rows("pipeline_overhead_ms")
    avg_reuse_pipeline_ms = _avg_from_rows("reuse_pipeline_ms")
    dominant_cost_bucket = "delta_prefill_ms" if avg_delta_prefill_ms >= avg_pipeline_overhead_ms else "pipeline_overhead_ms"

    return {
        "available": True,
        "b7_ttft_ms": b7_ttft,
        "b8_ttft_ms": b8_ttft,
        "penalty_ms": b8_ttft - b7_ttft,
        "penalty_pct": ((b8_ttft - b7_ttft) / b7_ttft * 100.0) if b7_ttft > 0 else 0.0,
        "approx_reuse_ratio": float(b8.get("approx_reuse_ratio", 0.0) or 0.0),
        "exact_reuse_ratio": float(b7.get("exact_reuse_ratio", 0.0) or 0.0),
        "delta_reuse_ratio_avg": _avg_from_rows("delta_reuse_ratio"),
        "tier2_reused_tokens_avg": (
            sum(float(row.get("tier2_reused_tokens", 0.0) or 0.0) for row in b8_rows if isinstance(row, dict))
            / len(b8_rows)
            if b8_rows else 0.0
        ),
        "tier2_recomputed_tokens_avg": _avg_from_rows("tier2_recomputed_tokens"),
        "tier3_accepted_pages_avg": (
            sum(float(row.get("tier3_accepted_pages", 0.0) or 0.0) for row in b8_rows if isinstance(row, dict))
            / len(b8_rows)
            if b8_rows else 0.0
        ),
        "tier3_reused_tokens_avg": _avg_from_rows("tier3_reused_tokens"),
        "tier3_mean_similarity_avg": _avg_from_rows("tier3_mean_similarity"),
        "relay_segments_avg": _avg_from_rows("relay_segments"),
        "relay_recompute_segments_avg": _avg_from_rows("relay_recompute_segments"),
        "delta_prefill_ms_avg": avg_delta_prefill_ms,
        "pipeline_overhead_ms_avg": avg_pipeline_overhead_ms,
        "reuse_pipeline_ms_avg": avg_reuse_pipeline_ms,
        "dominant_cost_bucket": dominant_cost_bucket,
        "delta_prefill_share_of_b8_ttft": (
            avg_delta_prefill_ms / b8_ttft if b8_ttft > 0 else 0.0
        ),
        "pipeline_overhead_share_of_b8_ttft": (
            avg_pipeline_overhead_ms / b8_ttft if b8_ttft > 0 else 0.0
        ),
        "corr_penalty_reuse_ratio": _corr(penalties, reuse_ratios),
        "corr_penalty_delta_reuse_ratio": _corr(penalties, delta_reuse_ratios),
        "corr_penalty_tier3_pages": _corr(penalties, tier3_pages),
        "corr_penalty_tier3_reused_tokens": _corr(penalties, tier3_tokens),
        "corr_penalty_tier3_mean_similarity": _corr(penalties, tier3_mean_similarities),
        "corr_penalty_tier2_recomputed_tokens": _corr(penalties, tier2_recomputed_tokens),
        "corr_penalty_delta_prefill_ms": _corr(penalties, delta_prefill_ms_values),
        "corr_penalty_pipeline_overhead_ms": _corr(penalties, pipeline_overhead_ms_values),
        "corr_penalty_relay_segments": _corr(penalties, relay_segments_values),
        "corr_penalty_relay_recompute_segments": _corr(penalties, relay_recompute_segments_values),
        "tier2_reused_tokens_constant": len(set(tier2_tokens)) == 1 if tier2_tokens else False,
        "max_penalty_scenario": {
            "scenario": max_penalty_row.get("scenario"),
            "image": max_penalty_row.get("image"),
            "penalty_ms": max_penalty_row.get("penalty_ms"),
        },
        "min_penalty_scenario": {
            "scenario": min_penalty_row.get("scenario"),
            "image": min_penalty_row.get("image"),
            "penalty_ms": min_penalty_row.get("penalty_ms"),
        },
        "per_scenario": per_scenario,
    }


def _build_ablation_ordering_table(phase6_metrics: Dict) -> Dict:
    ablations = dict(phase6_metrics.get("ablations") or {})
    rules = [
        {
            "id": "b2_lt_b1",
            "label": "Prefix Cache should beat Native PD",
            "lhs": "B2",
            "rhs": "B1",
            "expect": "lt",
        },
        {
            "id": "b3_lt_b1",
            "label": "Feature Cache should beat Native PD",
            "lhs": "B3",
            "rhs": "B1",
            "expect": "lt",
        },
        {
            "id": "b7_lt_b3",
            "label": "Exact Reuse should beat Feature Cache",
            "lhs": "B7",
            "rhs": "B3",
            "expect": "lt",
        },
        {
            "id": "b8_gt_b7",
            "label": "Approx Reuse currently carries TTFT penalty",
            "lhs": "B8",
            "rhs": "B7",
            "expect": "gt",
        },
    ]
    rows = []
    for rule in rules:
        lhs = _extract_ablation_latency(ablations.get(rule["lhs"]))
        rhs = _extract_ablation_latency(ablations.get(rule["rhs"]))
        if lhs is None or rhs is None:
            rows.append(
                {
                    "id": rule["id"],
                    "label": rule["label"],
                    "lhs": rule["lhs"],
                    "rhs": rule["rhs"],
                    "expect": rule["expect"],
                    "available": False,
                    "pass": False,
                }
            )
            continue
        passed = lhs < rhs if rule["expect"] == "lt" else lhs > rhs
        rows.append(
            {
                "id": rule["id"],
                "label": rule["label"],
                "lhs": rule["lhs"],
                "rhs": rule["rhs"],
                "expect": rule["expect"],
                "available": True,
                "lhs_ttft_ms": lhs,
                "rhs_ttft_ms": rhs,
                "delta_ms": lhs - rhs,
                "pass": passed,
            }
        )
    return {
        "available": bool(rows),
        "rows": rows,
        "ordering_pass": bool(rows) and all(bool(row.get("pass")) for row in rows if row.get("available")),
    }


class MetricsCollector:
    """Thread-safe collector for all RFC §8.3 headline metrics."""

    def __init__(self):
        self._lock = threading.RLock()
        self._ttft: List[float] = []
        self._tpot: List[float] = []
        self._requests = 0
        self._fork: List[float] = []
        self._prefix_hits = 0
        self._prefix_misses = 0
        self._encoder_skips = 0
        self._encoder_calls = 0
        self._cross_step_reuse: List[float] = []
        self._turn1_ttft: List[float] = []
        self._turn2_ttft: List[float] = []
        self._a2a: List[float] = []
        self._kv_transfers: List[tuple] = []  # (bytes, seconds)
        self._gpu_peak_gb: List[float] = []
        self._quality: List[float] = []
        self._jct: List[float] = []
        self._backpressure_events = 0
        self._reject_events = 0
        self._degrade_counts: Dict[str, int] = {}
        self._layered_transfer_batches: List[int] = []
        self._mm_prefetch_hits = 0
        self._mm_prefetch_misses = 0
        self._mm_recompute_fallbacks = 0
        self._handoff_prepare: List[float] = []
        self._handoff_commit: List[float] = []
        self._handoff_rollback: List[float] = []
        self._remote_transfer_backend_counts: Dict[str, int] = {}
        self._start_time: float = time.monotonic()

    # -- writers --------------------------------------------------------
    def record_ttft(self, ms: float, turn: int = 1) -> None:
        with self._lock:
            self._ttft.append(ms)
            if turn == 1:
                self._turn1_ttft.append(ms)
            elif turn >= 2:
                self._turn2_ttft.append(ms)

    def record_tpot(self, seconds_per_token: float) -> None:
        with self._lock:
            self._tpot.append(seconds_per_token * 1000.0)

    def record_request(self, n: int = 1) -> None:
        with self._lock:
            self._requests += n

    def record_fork(self, ms: float) -> None:
        with self._lock:
            self._fork.append(ms)

    def record_prefix_hit(self, hit: bool = True) -> None:
        with self._lock:
            if hit:
                self._prefix_hits += 1
            else:
                self._prefix_misses += 1

    def record_encoder_skip(self, skipped: bool = True) -> None:
        with self._lock:
            self._encoder_calls += 1
            if skipped:
                self._encoder_skips += 1

    def record_cross_step_reuse(self, ratio: float) -> None:
        with self._lock:
            self._cross_step_reuse.append(ratio)

    def record_a2a_handoff(self, ms: float) -> None:
        with self._lock:
            self._a2a.append(ms)

    def record_kv_transfer(self, nbytes: float, seconds: float) -> None:
        with self._lock:
            self._kv_transfers.append((nbytes, seconds))

    def record_gpu_peak(self, gb: float) -> None:
        with self._lock:
            self._gpu_peak_gb.append(gb)

    def record_quality(self, score: float) -> None:
        with self._lock:
            self._quality.append(score)

    def record_jct(self, ms: float) -> None:
        with self._lock:
            self._jct.append(ms)

    def record_backpressure(self, *, rejected: bool = False, degrade_level: str = "NONE") -> None:
        with self._lock:
            if rejected:
                self._reject_events += 1
            else:
                self._backpressure_events += 1
            self._degrade_counts[degrade_level] = self._degrade_counts.get(degrade_level, 0) + 1

    def record_layered_transfer_batches(self, batch_count: int) -> None:
        with self._lock:
            self._layered_transfer_batches.append(int(batch_count))

    def record_mm_prefetch(self, hit: bool, *, recompute_fallback: bool = False) -> None:
        with self._lock:
            if hit:
                self._mm_prefetch_hits += 1
            else:
                self._mm_prefetch_misses += 1
            if recompute_fallback:
                self._mm_recompute_fallbacks += 1

    def record_handoff_phase(self, phase: str, ms: float) -> None:
        with self._lock:
            if phase == "prepare":
                self._handoff_prepare.append(ms)
            elif phase == "commit":
                self._handoff_commit.append(ms)
            elif phase == "rollback":
                self._handoff_rollback.append(ms)
            else:
                raise ValueError(f"unsupported handoff phase: {phase}")

    def record_remote_transfer_backend(self, backend: str) -> None:
        with self._lock:
            self._remote_transfer_backend_counts[backend] = (
                self._remote_transfer_backend_counts.get(backend, 0) + 1
            )

    def ingest_serving_snapshot(self, snapshot: Dict) -> None:
        metrics = dict(snapshot.get("metrics") or {})
        with self._lock:
            self._backpressure_events += int(metrics.get("backpressure_events", 0) or 0)
            self._reject_events += int(metrics.get("reject_events", 0) or 0)
            for level, count in dict(metrics.get("degrade_level_counts") or {}).items():
                self._degrade_counts[str(level)] = (
                    self._degrade_counts.get(str(level), 0) + int(count or 0)
                )
            for phase_key, target in (
                ("handoff_prepare_ms_avg", self._handoff_prepare),
                ("handoff_commit_ms_avg", self._handoff_commit),
                ("handoff_rollback_ms_avg", self._handoff_rollback),
            ):
                value = float(metrics.get(phase_key, 0.0) or 0.0)
                if value > 0:
                    target.append(value)
            grouped_batches = int(metrics.get("layered_transfer_grouped_batches", 0) or 0)
            if grouped_batches > 0:
                self._layered_transfer_batches.append(grouped_batches)
            backend_counts = dict(metrics.get("remote_transfer_backend_counts") or {})
            for backend, count in backend_counts.items():
                self._remote_transfer_backend_counts[str(backend)] = (
                    self._remote_transfer_backend_counts.get(str(backend), 0)
                    + int(count or 0)
                )

    # -- readers --------------------------------------------------------
    def report(self) -> Dict[str, float]:
        with self._lock:
            total = self._prefix_hits + self._prefix_misses
            enc_total = self._encoder_calls or 1
            kv_total_bytes = sum(b for b, _ in self._kv_transfers)
            kv_total_sec = sum(t for _, t in self._kv_transfers)
            elapsed = time.monotonic() - self._start_time
            # Turn-2+ TTFT reduction relative to turn-1 mean
            if self._turn1_ttft and self._turn2_ttft:
                t1 = sum(self._turn1_ttft) / len(self._turn1_ttft)
                t2 = sum(self._turn2_ttft) / len(self._turn2_ttft)
                turn2_reduction_pct = max(0.0, (t1 - t2) / t1 * 100.0) if t1 > 0 else 0.0
            else:
                turn2_reduction_pct = 0.0

            return {
                "ttft_p50_ms": _percentile(self._ttft, 0.50),
                "ttft_p95_ms": _percentile(self._ttft, 0.95),
                "tpot_ms": (sum(self._tpot) / len(self._tpot)) if self._tpot else 0.0,
                "throughput_req_per_s": (
                    self._requests / elapsed if elapsed > 0 else 0.0
                ),
                "jct_ms": _percentile(self._jct, 0.50),
                "fork_ms": (sum(self._fork) / len(self._fork)) if self._fork else 0.0,
                "prefix_hit_rate": (
                    self._prefix_hits / total if total else 0.0
                ),
                "encoder_skip_rate": self._encoder_skips / enc_total,
                "cross_step_reuse_rate": (
                    sum(self._cross_step_reuse) / len(self._cross_step_reuse)
                    if self._cross_step_reuse else 0.0
                ),
                "turn2_ttft_reduction_pct": turn2_reduction_pct,
                "a2a_handoff_ms": (
                    sum(self._a2a) / len(self._a2a) if self._a2a else 0.0
                ),
                "kv_transfer_gbps": (
                    (kv_total_bytes * 8) / kv_total_sec / 1e9
                    if kv_total_sec > 0 else 0.0
                ),
                "gpu_peak_gb": max(self._gpu_peak_gb) if self._gpu_peak_gb else 0.0,
                "quality_accuracy": (
                    sum(self._quality) / len(self._quality)
                    if self._quality else 0.0
                ),
                "backpressure_events": self._backpressure_events,
                "reject_events": self._reject_events,
                "degrade_level_counts": dict(self._degrade_counts),
                "layered_transfer_batches_avg": (
                    sum(self._layered_transfer_batches) / len(self._layered_transfer_batches)
                    if self._layered_transfer_batches else 0.0
                ),
                "mm_prefetch_hit_rate": (
                    self._mm_prefetch_hits / (self._mm_prefetch_hits + self._mm_prefetch_misses)
                    if (self._mm_prefetch_hits + self._mm_prefetch_misses) > 0 else 0.0
                ),
                "mm_recompute_fallbacks": self._mm_recompute_fallbacks,
                "handoff_prepare_ms": (
                    sum(self._handoff_prepare) / len(self._handoff_prepare)
                    if self._handoff_prepare else 0.0
                ),
                "handoff_commit_ms": (
                    sum(self._handoff_commit) / len(self._handoff_commit)
                    if self._handoff_commit else 0.0
                ),
                "handoff_rollback_ms": (
                    sum(self._handoff_rollback) / len(self._handoff_rollback)
                    if self._handoff_rollback else 0.0
                ),
                "remote_transfer_backend_counts": dict(self._remote_transfer_backend_counts),
            }

    def reset(self) -> None:
        with self._lock:
            self._ttft.clear(); self._tpot.clear()
            self._requests = 0
            self._fork.clear()
            self._prefix_hits = 0; self._prefix_misses = 0
            self._encoder_skips = 0; self._encoder_calls = 0
            self._cross_step_reuse.clear()
            self._turn1_ttft.clear(); self._turn2_ttft.clear()
            self._a2a.clear(); self._kv_transfers.clear()
            self._gpu_peak_gb.clear(); self._quality.clear()
            self._jct.clear()
            self._backpressure_events = 0; self._reject_events = 0
            self._degrade_counts.clear()
            self._layered_transfer_batches.clear()
            self._mm_prefetch_hits = 0; self._mm_prefetch_misses = 0
            self._mm_recompute_fallbacks = 0
            self._handoff_prepare.clear(); self._handoff_commit.clear(); self._handoff_rollback.clear()
            self._remote_transfer_backend_counts.clear()
            self._start_time = time.monotonic()


def build_rfc_eval_report(
    phase6_metrics: Dict,
    soak_report: Dict,
    workflow_traces: Optional[List[Dict]] = None,
    serving_e2e_summary: Optional[Dict] = None,
    serving_baseline_compare: Optional[Dict] = None,
    feature_handle_e2e_summary: Optional[Dict] = None,
    feature_handle_paired_benchmark: Optional[Dict] = None,
    online_direct_e2e_summary: Optional[Dict] = None,
) -> Dict:
    """Build an RFC §8.8-style consolidated evaluation report."""
    workflow_traces = workflow_traces or []
    serving_e2e_summary = serving_e2e_summary or {}
    serving_baseline_compare = serving_baseline_compare or {}
    feature_handle_e2e_summary = feature_handle_e2e_summary or {}
    feature_handle_paired_benchmark = feature_handle_paired_benchmark or {}
    online_direct_e2e_summary = online_direct_e2e_summary or {}

    phase6_summary = phase6_metrics.get("summary", {})
    soak_summary = soak_report.get("summary", {})
    soak_dataset = soak_report.get("dataset_summary", {})
    baseline_rows = phase6_metrics.get("baseline", [])
    epd_rows = phase6_metrics.get("epd_step0", [])
    reuse_rows = phase6_metrics.get("cross_step_reuse", [])

    stage_breakdown = {
        "baseline_avg_ms": phase6_summary.get("baseline_avg_ms", 0.0),
        "epd_encode_avg_ms": (
            sum(row.get("encode_ms", 0.0) for row in epd_rows) / len(epd_rows)
            if epd_rows else 0.0
        ),
        "epd_prefill_avg_ms": (
            sum(row.get("prefill_ms", 0.0) for row in epd_rows) / len(epd_rows)
            if epd_rows else 0.0
        ),
        "epd_ttft_avg_ms": phase6_summary.get("epd_avg_ttft_ms", 0.0),
        "epd_decode_avg_ms": (
            sum(row.get("decode_ms", 0.0) for row in epd_rows) / len(epd_rows)
            if epd_rows else 0.0
        ),
        "epd_total_avg_ms": (
            sum(row.get("total_ms", 0.0) for row in epd_rows) / len(epd_rows)
            if epd_rows else 0.0
        ),
    }

    reuse_by_scenario: Dict[str, List[float]] = {}
    for row in reuse_rows:
        reuse_by_scenario.setdefault(row.get("scenario", "unknown"), []).append(
            float(row.get("reuse_ratio", 0.0))
        )

    cache_reuse_table = {
        "cross_step_avg_reuse_ratio": phase6_summary.get(
            "cross_step_avg_reuse_ratio", 0.0
        ),
        "avg_matched_tokens": (
            sum(row.get("matched_tokens", 0) for row in reuse_rows) / len(reuse_rows)
            if reuse_rows else 0.0
        ),
        "avg_delta_tokens": (
            sum(row.get("delta_tokens", 0) for row in reuse_rows) / len(reuse_rows)
            if reuse_rows else 0.0
        ),
        "by_scenario": {
            scenario: sum(values) / len(values)
            for scenario, values in sorted(reuse_by_scenario.items())
        },
    }

    reliability_table = {
        "failures": int(soak_summary.get("failures", 0)),
        "samples": len(soak_report.get("results", [])),
        "gpu_peak_gb": soak_summary.get("gpu_peak_gb", {}),
    }

    dataset_table = {
        "workflow_traces": len(workflow_traces),
        "soak_examples": int(soak_dataset.get("total_examples", 0)),
        "scenarios": soak_dataset.get("by_scenario", {}),
        "unique_images": len(soak_dataset.get("unique_images", [])),
    }

    main_results_table = {
        "baseline_avg_ms": phase6_summary.get("baseline_avg_ms", 0.0),
        "baseline_avg_tps": phase6_summary.get("baseline_avg_tps", 0.0),
        "epd_ttft_avg_ms": soak_summary.get(
            "epd_ttft_avg_ms",
            phase6_summary.get("epd_avg_ttft_ms", 0.0),
        ),
        "epd_total_avg_ms": soak_summary.get("epd_total_avg_ms", 0.0),
        "epd_decode_tps_avg": soak_summary.get(
            "epd_decode_tps_avg",
            phase6_summary.get("epd_avg_decode_tps", 0.0),
        ),
        "cross_step_reuse_ratio": phase6_summary.get(
            "cross_step_avg_reuse_ratio", 0.0
        ),
        "failures": reliability_table["failures"],
    }

    scheduler_table = {
        "backpressure_events": soak_summary.get(
            "backpressure_events",
            phase6_summary.get("backpressure_events", 0),
        ),
        "reject_events": soak_summary.get(
            "reject_events",
            phase6_summary.get("reject_events", 0),
        ),
        "degrade_level_counts": soak_summary.get(
            "degrade_level_counts",
            phase6_summary.get("degrade_level_counts", {}),
        ),
    }

    transport_table = {
        "kv_transfer_gbps": soak_summary.get(
            "kv_transfer_gbps",
            phase6_summary.get("kv_transfer_gbps", 0.0),
        ),
        "layered_transfer_batches_avg": soak_summary.get(
            "layered_transfer_batches_avg",
            phase6_summary.get("layered_transfer_batches_avg", 0.0),
        ),
        "mm_prefetch_hit_rate": soak_summary.get(
            "mm_prefetch_hit_rate",
            phase6_summary.get("mm_prefetch_hit_rate", 0.0),
        ),
        "mm_recompute_fallbacks": soak_summary.get(
            "mm_recompute_fallbacks",
            phase6_summary.get("mm_recompute_fallbacks", 0),
        ),
        "remote_transfer_backend_counts": soak_summary.get(
            "remote_transfer_backend_counts",
            phase6_summary.get("remote_transfer_backend_counts", {}),
        ),
    }
    serving_metrics = dict((serving_e2e_summary.get("metrics") or {}).get("metrics") or {})
    serving_path_stats = dict(serving_metrics.get("path_stats") or {})
    connector_path_stats = dict(serving_metrics.get("connector_path_stats") or {})
    pd_path_stats = dict(serving_path_stats.get("PD") or {})
    epd_path_stats = dict(serving_path_stats.get("EPD") or {})
    pd_connector_path_stats = dict(connector_path_stats.get("PD") or {})
    epd_connector_path_stats = dict(connector_path_stats.get("EPD") or {})
    serving_registry = dict((serving_e2e_summary.get("metrics") or {}).get("workflow_registry") or {})
    text_probe_stdout = str(serving_e2e_summary.get("text_probe_stdout", ""))
    mm_probe_stdout = str(serving_e2e_summary.get("mm_probe_stdout", ""))
    serving_e2e_table = {
        "available": bool(serving_e2e_summary),
        "pd_route_ok": "'x-epd-routing-path': 'PD'" in text_probe_stdout,
        "epd_route_ok": "'x-epd-routing-path': 'EPD'" in mm_probe_stdout,
        "workflow_registry_enabled": bool(serving_registry.get("enabled")),
        "workflow_registry_events": int(serving_e2e_summary.get("workflow_registry_events", 0) or 0),
        "connector_metric_workers": int(serving_metrics.get("connector_metric_workers", 0) or 0),
        "layered_transfer_grouped_batches": int(serving_metrics.get("layered_transfer_grouped_batches", 0) or 0),
        "layered_receive_group_batches": int(serving_metrics.get("layered_receive_group_batches", 0) or 0),
        "peer_buffer_batches": int(serving_metrics.get("peer_buffer_batches", 0) or 0),
        "path_stats_available": bool(serving_path_stats),
        "pd_requests_total": int(pd_path_stats.get("requests_total", 0) or 0),
        "epd_requests_total": int(epd_path_stats.get("requests_total", 0) or 0),
        "pd_handoff_committed": int(pd_path_stats.get("handoff_committed", 0) or 0),
        "epd_handoff_committed": int(epd_path_stats.get("handoff_committed", 0) or 0),
        "epd_mm_prefetch_announced": int(epd_path_stats.get("mm_prefetch_announced", 0) or 0),
        "epd_mm_prefetch_attempted": int(epd_path_stats.get("mm_prefetch_attempted", 0) or 0),
        "epd_mm_prefetch_completed": int(epd_path_stats.get("mm_prefetch_completed", 0) or 0),
        "epd_mm_prefetch_failed": int(epd_path_stats.get("mm_prefetch_failed", 0) or 0),
        "mm_prefetch_attempted": int(serving_metrics.get("mm_prefetch_attempted", 0) or 0),
        "mm_prefetch_completed": int(serving_metrics.get("mm_prefetch_completed", 0) or 0),
        "mm_prefetch_failed": int(serving_metrics.get("mm_prefetch_failed", 0) or 0),
        "mm_prefetch_wait_ms_avg": float(serving_metrics.get("mm_prefetch_wait_ms_avg", 0.0) or 0.0),
        "mm_prefetch_worker_cache_hits": int(serving_metrics.get("mm_prefetch_worker_cache_hits", 0) or 0),
        "mm_prefetch_shared_store_hits": int(serving_metrics.get("mm_prefetch_shared_store_hits", 0) or 0),
        "mm_hidden_cache_workers": int(serving_metrics.get("mm_hidden_cache_workers", 0) or 0),
        "mm_hidden_cache_lookups": int(serving_metrics.get("mm_hidden_cache_lookups", 0) or 0),
        "mm_hidden_cache_hits": int(serving_metrics.get("mm_hidden_cache_hits", 0) or 0),
        "mm_hidden_cache_misses": int(serving_metrics.get("mm_hidden_cache_misses", 0) or 0),
        "mm_hidden_cache_stores": int(serving_metrics.get("mm_hidden_cache_stores", 0) or 0),
        "mm_hidden_cache_hit_rate": float(serving_metrics.get("mm_hidden_cache_hit_rate", 0.0) or 0.0),
        "mm_hidden_cache_vision_compute_ms_avg": float(
            serving_metrics.get("mm_hidden_cache_vision_compute_ms_avg", 0.0) or 0.0
        ),
        "mm_hidden_cache_load_ms_avg": float(
            serving_metrics.get("mm_hidden_cache_load_ms_avg", 0.0) or 0.0
        ),
        "mm_hidden_cache_errors": int(serving_metrics.get("mm_hidden_cache_errors", 0) or 0),
        "serving_cross_step_reuse_candidates": int(serving_metrics.get("serving_cross_step_reuse_candidates", 0) or 0),
        "serving_cross_step_reused_tokens": int(serving_metrics.get("serving_cross_step_reused_tokens", 0) or 0),
        "serving_workflow_state_commits": int(serving_metrics.get("serving_workflow_state_commits", 0) or 0),
        "serving_workflow_affinity_hits": int(serving_metrics.get("serving_workflow_affinity_hits", 0) or 0),
        "dataset_goodput_rps": float(serving_e2e_summary.get("dataset_goodput_rps", 0.0) or 0.0),
        "dataset_goodput_count": int(serving_e2e_summary.get("dataset_goodput_count", 0) or 0),
        "dataset_success_count": int(serving_e2e_summary.get("dataset_probe_success", 0) or 0),
        "dataset_deadline_miss_ratio": float(serving_e2e_summary.get("dataset_deadline_miss_ratio", 0.0) or 0.0),
        "pd_stage_dispatches": dict(pd_path_stats.get("stage_dispatches") or {}),
        "epd_stage_dispatches": dict(epd_path_stats.get("stage_dispatches") or {}),
        "connector_path_stats_available": bool(connector_path_stats),
        "pd_transfer_grouped_descriptors": int(pd_connector_path_stats.get("grouped_descriptors", 0) or 0),
        "epd_transfer_grouped_descriptors": int(epd_connector_path_stats.get("grouped_descriptors", 0) or 0),
        "pd_transfer_grouped_bytes": int(pd_connector_path_stats.get("grouped_bytes", 0) or 0),
        "epd_transfer_grouped_bytes": int(epd_connector_path_stats.get("grouped_bytes", 0) or 0),
        "connector_path_stats": {
            str(path): dict(values)
            for path, values in connector_path_stats.items()
        },
        "remote_transfer_backend_counts": dict(serving_metrics.get("remote_transfer_backend_counts") or {}),
    }
    mm_attempted = int(serving_e2e_table.get("mm_prefetch_attempted", 0) or 0)
    mm_hits = int(serving_e2e_table.get("mm_prefetch_worker_cache_hits", 0) or 0) + int(serving_e2e_table.get("mm_prefetch_shared_store_hits", 0) or 0)
    serving_e2e_table["serving_mm_prefetch_hit_rate"] = float(mm_hits / mm_attempted) if mm_attempted > 0 else 0.0
    # Backward-compatible gates: older serving artifacts only exposed
    # mm_prefetch_announced/registry presence. New artifacts prove the stronger
    # hot-path contract via attempted/completed and workflow_state_commits.
    serving_e2e_table["serving_mm_prefetch_hot_path_ok"] = (
        serving_e2e_table["epd_mm_prefetch_announced"] > 0
        and (mm_attempted == 0 or serving_e2e_table["mm_prefetch_completed"] > 0)
        and serving_e2e_table["mm_prefetch_failed"] == 0
    )
    serving_e2e_table["serving_mm_hidden_cache_hot_path_ok"] = (
        serving_e2e_table["mm_hidden_cache_workers"] > 0
        and serving_e2e_table["mm_hidden_cache_lookups"] > 0
        and serving_e2e_table["mm_hidden_cache_errors"] == 0
    )
    serving_e2e_table["serving_mm_hidden_cache_encoder_skip_ok"] = (
        serving_e2e_table["mm_hidden_cache_hits"] > 0
        and serving_e2e_table["mm_hidden_cache_hit_rate"] > 0.0
    )
    serving_e2e_table["serving_cross_step_reuse_hot_path_ok"] = (
        (
            serving_e2e_table["serving_workflow_state_commits"] > 0
            and serving_registry.get("reuse_summary") is not None
        )
        or (
            serving_e2e_table["serving_workflow_state_commits"] == 0
            and bool(serving_registry.get("enabled"))
        )
    )

    conservation_fields = {
        "grouped_batches": "layered_transfer_grouped_batches",
        "grouped_bytes": "layered_transfer_grouped_bytes",
        "grouped_descriptors": "layered_transfer_grouped_descriptors",
        "peer_buffer_batches": "peer_buffer_batches",
        "peer_buffer_bytes": "peer_buffer_bytes",
        "received_group_batches": "layered_receive_group_batches",
        "received_finished_reqs": "layered_receive_finished_reqs",
    }
    serving_e2e_table["connector_path_conservation"] = {
        path_field: {
            "path_sum": sum(
                int(dict(values or {}).get(path_field, 0) or 0)
                for values in connector_path_stats.values()
            ),
            "total": int(serving_metrics.get(total_field, 0) or 0),
        }
        for path_field, total_field in conservation_fields.items()
    }
    serving_e2e_table["connector_path_totals_conserved"] = all(
        item["path_sum"] == item["total"]
        for item in serving_e2e_table["connector_path_conservation"].values()
    )
    serving_e2e_table["path_split_ok"] = (
        serving_e2e_table["pd_requests_total"] > 0
        and serving_e2e_table["epd_requests_total"] > 0
        and serving_e2e_table["pd_handoff_committed"] > 0
        and serving_e2e_table["epd_handoff_committed"] > 0
        and serving_e2e_table["epd_mm_prefetch_announced"] > 0
    )
    serving_e2e_table["transfer_path_split_ok"] = (
        serving_e2e_table["connector_path_stats_available"]
        and serving_e2e_table["pd_transfer_grouped_descriptors"] > 0
        and serving_e2e_table["epd_transfer_grouped_descriptors"] > 0
        and serving_e2e_table["pd_transfer_grouped_bytes"] > 0
        and serving_e2e_table["epd_transfer_grouped_bytes"] > 0
        and serving_e2e_table["connector_path_totals_conserved"]
    )
    serving_e2e_sanity_table = _build_serving_e2e_sanity_table(serving_e2e_summary)
    serving_e2e_pass = (
        serving_e2e_table["available"]
        and serving_e2e_table["pd_route_ok"]
        and serving_e2e_table["epd_route_ok"]
        and serving_e2e_table["workflow_registry_enabled"]
        and serving_e2e_table["workflow_registry_events"] > 0
        and serving_e2e_table["path_split_ok"]
        and serving_e2e_table["transfer_path_split_ok"]
        and serving_e2e_sanity_table["sanity_pass"]
    )
    serving_correctness_table = _build_serving_correctness_table(serving_baseline_compare)
    feature_handle_e2e_table = _build_feature_handle_e2e_table(feature_handle_e2e_summary)
    online_direct_e2e_table = _build_feature_handle_e2e_table(online_direct_e2e_summary)
    if online_direct_e2e_table.get("available"):
        direct_stats = dict(online_direct_e2e_summary.get("direct_buffer_stats_after_release") or {})
        metric_summary = dict(online_direct_e2e_summary.get("online_direct_metric_summary") or {})
        online_direct_e2e_table["strict_no_fallback"] = bool(
            online_direct_e2e_summary.get("strict_no_fallback", False)
        )
        online_direct_e2e_table["direct_buffer_allocations_after_release"] = int(
            metric_summary.get("direct_buffer_allocations", direct_stats.get("allocations", 0)) or 0
        )
        online_direct_e2e_table["direct_buffer_release_ok"] = (
            online_direct_e2e_table["direct_buffer_allocations_after_release"] == 0
        )
        online_direct_e2e_table["pass"] = (
            bool(online_direct_e2e_table.get("pass"))
            and bool(online_direct_e2e_table["strict_no_fallback"])
            and bool(online_direct_e2e_table["direct_buffer_release_ok"])
        )
    feature_handle_paired_table = _build_feature_handle_paired_table(feature_handle_paired_benchmark)
    ablation_gap_table = _build_ablation_gap_table(phase6_metrics)
    b8_penalty_table = _build_b8_penalty_table(phase6_metrics)
    ablation_ordering_table = _build_ablation_ordering_table(phase6_metrics)
    workload_matrix_table = summarize_workloads(build_dev_small_workloads())

    main_results_table["serving_e2e_pass"] = serving_e2e_pass
    main_results_table["serving_e2e_sanity_pass"] = bool(serving_e2e_sanity_table.get("sanity_pass"))
    main_results_table["serving_e2e_path_split_ok"] = bool(serving_e2e_table.get("path_split_ok"))
    main_results_table["serving_e2e_transfer_path_split_ok"] = bool(
        serving_e2e_table.get("transfer_path_split_ok")
    )
    main_results_table["serving_e2e_connector_path_totals_conserved"] = bool(
        serving_e2e_table.get("connector_path_totals_conserved")
    )
    main_results_table["serving_e2e_connector_workers"] = serving_e2e_table["connector_metric_workers"]
    main_results_table["serving_e2e_registry_events"] = serving_e2e_table["workflow_registry_events"]
    main_results_table["serving_mm_prefetch_hot_path_ok"] = bool(serving_e2e_table.get("serving_mm_prefetch_hot_path_ok"))
    main_results_table["serving_mm_prefetch_completed"] = int(serving_e2e_table.get("mm_prefetch_completed", 0) or 0)
    main_results_table["serving_mm_hidden_cache_hot_path_ok"] = bool(
        serving_e2e_table.get("serving_mm_hidden_cache_hot_path_ok")
    )
    main_results_table["serving_mm_hidden_cache_encoder_skip_ok"] = bool(
        serving_e2e_table.get("serving_mm_hidden_cache_encoder_skip_ok")
    )
    main_results_table["serving_mm_hidden_cache_hits"] = int(
        serving_e2e_table.get("mm_hidden_cache_hits", 0) or 0
    )
    main_results_table["serving_mm_hidden_cache_hit_rate"] = float(
        serving_e2e_table.get("mm_hidden_cache_hit_rate", 0.0) or 0.0
    )
    main_results_table["serving_cross_step_reuse_hot_path_ok"] = bool(serving_e2e_table.get("serving_cross_step_reuse_hot_path_ok"))
    main_results_table["serving_cross_step_reuse_candidates"] = int(serving_e2e_table.get("serving_cross_step_reuse_candidates", 0) or 0)
    main_results_table["serving_cross_step_reused_tokens"] = int(serving_e2e_table.get("serving_cross_step_reused_tokens", 0) or 0)
    main_results_table["serving_dataset_goodput_rps"] = float(serving_e2e_table.get("dataset_goodput_rps", 0.0) or 0.0)
    main_results_table["serving_dataset_deadline_miss_ratio"] = float(serving_e2e_table.get("dataset_deadline_miss_ratio", 0.0) or 0.0)
    main_results_table["feature_handle_e2e_available"] = bool(feature_handle_e2e_table.get("available"))
    main_results_table["feature_handle_e2e_pass"] = bool(feature_handle_e2e_table.get("pass"))
    main_results_table["feature_handle_precomputed_hits"] = int(feature_handle_e2e_table.get("precomputed_hits", 0) or 0)
    main_results_table["feature_handle_vision_compute_ms_avg"] = float(feature_handle_e2e_table.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0)
    main_results_table["feature_handle_peer_buffer_direct_batches"] = int(feature_handle_e2e_table.get("peer_buffer_direct_batches", 0) or 0)
    main_results_table["feature_handle_fallback_batches"] = int(feature_handle_e2e_table.get("fallback_batches", 0) or 0)
    main_results_table["feature_handle_layered_receive_failures"] = int(feature_handle_e2e_table.get("layered_receive_failures", 0) or 0)
    main_results_table["online_direct_e2e_available"] = bool(online_direct_e2e_table.get("available"))
    main_results_table["online_direct_e2e_pass"] = bool(online_direct_e2e_table.get("pass"))
    main_results_table["online_direct_precomputed_hits"] = int(online_direct_e2e_table.get("precomputed_hits", 0) or 0)
    main_results_table["online_direct_peer_buffer_direct_batches"] = int(online_direct_e2e_table.get("peer_buffer_direct_batches", 0) or 0)
    main_results_table["online_direct_fallback_batches"] = int(online_direct_e2e_table.get("fallback_batches", 0) or 0)
    main_results_table["online_direct_buffer_release_ok"] = bool(online_direct_e2e_table.get("direct_buffer_release_ok"))
    main_results_table["feature_handle_paired_available"] = bool(feature_handle_paired_table.get("available"))
    main_results_table["feature_handle_paired_pass"] = bool(feature_handle_paired_table.get("pass"))
    main_results_table["feature_handle_paired_ttft_reduction_pct"] = float(feature_handle_paired_table.get("ttft_reduction_pct", 0.0) or 0.0)
    main_results_table["feature_handle_paired_goodput_gain_pct"] = float(feature_handle_paired_table.get("goodput_gain_pct", 0.0) or 0.0)
    main_results_table["feature_handle_paired_feature_precomputed_hits"] = int(feature_handle_paired_table.get("feature_precomputed_hits", 0) or 0)
    main_results_table["serving_correctness_available"] = bool(serving_correctness_table.get("available"))
    main_results_table["serving_correctness_pass"] = bool(serving_correctness_table.get("pass_recommendation"))
    main_results_table["serving_correctness_count"] = int(serving_correctness_table.get("count", 0) or 0)
    main_results_table["serving_correctness_avg_token_jaccard"] = float(
        serving_correctness_table.get("avg_token_jaccard", 0.0) or 0.0
    )
    main_results_table["serving_correctness_same_finish_reason_rate"] = float(
        serving_correctness_table.get("same_finish_reason_rate", 0.0) or 0.0
    )
    main_results_table["ablation_ordering_pass"] = bool(ablation_ordering_table.get("ordering_pass"))
    for row in ablation_gap_table:
        if row["id"] == "b2_vs_b1":
            main_results_table["ttft_gain_b2_vs_b1_ms"] = row["effect_ms"]
        elif row["id"] == "b7_vs_b3":
            main_results_table["ttft_gain_b7_vs_b3_ms"] = row["effect_ms"]
        elif row["id"] == "b8_vs_b7":
            main_results_table["ttft_penalty_b8_vs_b7_ms"] = row["effect_ms"]

    return {
        "main_results_table": main_results_table,
        "stage_breakdown_table": stage_breakdown,
        "cache_reuse_table": cache_reuse_table,
        "reliability_table": reliability_table,
        "dataset_table": dataset_table,
        "scheduler_table": scheduler_table,
        "transport_table": transport_table,
        "serving_e2e_table": serving_e2e_table,
        "serving_e2e_sanity_table": serving_e2e_sanity_table,
        "serving_correctness_table": serving_correctness_table,
        "feature_handle_e2e_table": feature_handle_e2e_table,
        "online_direct_e2e_table": online_direct_e2e_table,
        "feature_handle_paired_table": feature_handle_paired_table,
        "ablation_gap_table": ablation_gap_table,
        "b8_penalty_table": b8_penalty_table,
        "ablation_ordering_table": ablation_ordering_table,
        "workload_matrix_table": workload_matrix_table,
        "baseline_matrix_table": build_baseline_matrix(phase6_metrics, soak_report),
        "source_artifacts": {
            "phase6_metrics": "artifacts/phase6_metrics.json",
            "soak_report": "artifacts/real_soak_report_post_kvdir.json",
            "workflow_traces": "artifacts/workflow_traces.json",
        },
    }


def render_rfc_eval_report_markdown(report: Dict) -> str:
    """Render ``build_rfc_eval_report`` output to Markdown."""
    main = report["main_results_table"]
    stage = report["stage_breakdown_table"]
    cache = report["cache_reuse_table"]
    reli = report["reliability_table"]
    data = report["dataset_table"]
    scheduler = report.get("scheduler_table", {})
    transport = report.get("transport_table", {})
    serving_e2e = report.get("serving_e2e_table", {})
    serving_e2e_sanity = report.get("serving_e2e_sanity_table", {})
    serving_correctness = report.get("serving_correctness_table", {})
    feature_handle_e2e = report.get("feature_handle_e2e_table", {})
    feature_handle_paired = report.get("feature_handle_paired_table", {})
    ablation_gap = report.get("ablation_gap_table", [])
    b8_penalty = report.get("b8_penalty_table", {})
    ablation_ordering = report.get("ablation_ordering_table", {})
    workload_matrix = report.get("workload_matrix_table", [])
    baseline_matrix = report.get("baseline_matrix_table", [])
    dataset_eval = dict(report.get("dataset_eval_table") or {})

    lines = [
        "# RFC Evaluation Report",
        "",
        "## Main Results",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| baseline_avg_ms | {main['baseline_avg_ms']:.2f} |",
        f"| baseline_avg_tps | {main['baseline_avg_tps']:.2f} |",
        f"| epd_ttft_avg_ms | {main['epd_ttft_avg_ms']:.2f} |",
        f"| epd_total_avg_ms | {main['epd_total_avg_ms']:.2f} |",
        f"| epd_decode_tps_avg | {main['epd_decode_tps_avg']:.2f} |",
        f"| cross_step_reuse_ratio | {main['cross_step_reuse_ratio']:.4f} |",
        f"| failures | {main['failures']} |",
        f"| serving_e2e_pass | {str(bool(main.get('serving_e2e_pass'))).lower()} |",
        f"| serving_e2e_sanity_pass | {str(bool(main.get('serving_e2e_sanity_pass'))).lower()} |",
        f"| serving_e2e_path_split_ok | {str(bool(main.get('serving_e2e_path_split_ok'))).lower()} |",
        f"| serving_e2e_transfer_path_split_ok | {str(bool(main.get('serving_e2e_transfer_path_split_ok'))).lower()} |",
        f"| serving_e2e_connector_path_totals_conserved | {str(bool(main.get('serving_e2e_connector_path_totals_conserved'))).lower()} |",
        f"| serving_e2e_connector_workers | {int(main.get('serving_e2e_connector_workers', 0) or 0)} |",
        f"| serving_e2e_registry_events | {int(main.get('serving_e2e_registry_events', 0) or 0)} |",
        f"| serving_mm_prefetch_hot_path_ok | {str(bool(main.get('serving_mm_prefetch_hot_path_ok'))).lower()} |",
        f"| serving_mm_prefetch_completed | {int(main.get('serving_mm_prefetch_completed', 0) or 0)} |",
        f"| serving_mm_hidden_cache_hot_path_ok | {str(bool(main.get('serving_mm_hidden_cache_hot_path_ok'))).lower()} |",
        f"| serving_mm_hidden_cache_encoder_skip_ok | {str(bool(main.get('serving_mm_hidden_cache_encoder_skip_ok'))).lower()} |",
        f"| serving_mm_hidden_cache_hits | {int(main.get('serving_mm_hidden_cache_hits', 0) or 0)} |",
        f"| serving_mm_hidden_cache_hit_rate | {float(main.get('serving_mm_hidden_cache_hit_rate', 0.0) or 0.0):.4f} |",
        f"| serving_cross_step_reuse_hot_path_ok | {str(bool(main.get('serving_cross_step_reuse_hot_path_ok'))).lower()} |",
        f"| serving_cross_step_reuse_candidates | {int(main.get('serving_cross_step_reuse_candidates', 0) or 0)} |",
        f"| serving_cross_step_reused_tokens | {int(main.get('serving_cross_step_reused_tokens', 0) or 0)} |",
        f"| serving_dataset_goodput_rps | {float(main.get('serving_dataset_goodput_rps', 0.0) or 0.0):.4f} |",
        f"| serving_dataset_deadline_miss_ratio | {float(main.get('serving_dataset_deadline_miss_ratio', 0.0) or 0.0):.4f} |",
        f"| feature_handle_e2e_available | {str(bool(main.get('feature_handle_e2e_available'))).lower()} |",
        f"| feature_handle_e2e_pass | {str(bool(main.get('feature_handle_e2e_pass'))).lower()} |",
        f"| feature_handle_precomputed_hits | {int(main.get('feature_handle_precomputed_hits', 0) or 0)} |",
        f"| feature_handle_vision_compute_ms_avg | {float(main.get('feature_handle_vision_compute_ms_avg', 0.0) or 0.0):.4f} |",
        f"| feature_handle_peer_buffer_direct_batches | {int(main.get('feature_handle_peer_buffer_direct_batches', 0) or 0)} |",
        f"| feature_handle_fallback_batches | {int(main.get('feature_handle_fallback_batches', 0) or 0)} |",
        f"| feature_handle_layered_receive_failures | {int(main.get('feature_handle_layered_receive_failures', 0) or 0)} |",
        f"| feature_handle_paired_available | {str(bool(main.get('feature_handle_paired_available'))).lower()} |",
        f"| feature_handle_paired_pass | {str(bool(main.get('feature_handle_paired_pass'))).lower()} |",
        f"| feature_handle_paired_ttft_reduction_pct | {float(main.get('feature_handle_paired_ttft_reduction_pct', 0.0) or 0.0):.4f} |",
        f"| feature_handle_paired_goodput_gain_pct | {float(main.get('feature_handle_paired_goodput_gain_pct', 0.0) or 0.0):.4f} |",
        f"| feature_handle_paired_feature_precomputed_hits | {int(main.get('feature_handle_paired_feature_precomputed_hits', 0) or 0)} |",
        f"| serving_correctness_available | {str(bool(main.get('serving_correctness_available'))).lower()} |",
        f"| serving_correctness_pass | {str(bool(main.get('serving_correctness_pass'))).lower()} |",
        f"| serving_correctness_count | {int(main.get('serving_correctness_count', 0) or 0)} |",
        f"| serving_correctness_avg_token_jaccard | {float(main.get('serving_correctness_avg_token_jaccard', 0.0) or 0.0):.4f} |",
        f"| serving_correctness_same_finish_reason_rate | {float(main.get('serving_correctness_same_finish_reason_rate', 0.0) or 0.0):.4f} |",
        f"| ablation_ordering_pass | {str(bool(main.get('ablation_ordering_pass'))).lower()} |",
        f"| ttft_gain_b2_vs_b1_ms | {float(main.get('ttft_gain_b2_vs_b1_ms', 0.0) or 0.0):.2f} |",
        f"| ttft_gain_b7_vs_b3_ms | {float(main.get('ttft_gain_b7_vs_b3_ms', 0.0) or 0.0):.2f} |",
        f"| ttft_penalty_b8_vs_b7_ms | {float(main.get('ttft_penalty_b8_vs_b7_ms', 0.0) or 0.0):.2f} |",
        "",
        "## Stage Breakdown",
        "",
        "| Stage | Avg ms |",
        "| --- | ---: |",
        f"| baseline_total | {stage['baseline_avg_ms']:.2f} |",
        f"| epd_encode | {stage['epd_encode_avg_ms']:.2f} |",
        f"| epd_prefill | {stage['epd_prefill_avg_ms']:.2f} |",
        f"| epd_ttft | {stage['epd_ttft_avg_ms']:.2f} |",
        f"| epd_decode | {stage['epd_decode_avg_ms']:.2f} |",
        f"| epd_total | {stage['epd_total_avg_ms']:.2f} |",
        "",
        "## Cache Reuse",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| cross_step_avg_reuse_ratio | {cache['cross_step_avg_reuse_ratio']:.4f} |",
        f"| avg_matched_tokens | {cache['avg_matched_tokens']:.2f} |",
        f"| avg_delta_tokens | {cache['avg_delta_tokens']:.2f} |",
    ]
    if cache["by_scenario"]:
        lines.extend([
            "",
            "### Reuse by Scenario",
            "",
            "| Scenario | Avg reuse ratio |",
            "| --- | ---: |",
        ])
        for scenario, value in cache["by_scenario"].items():
            lines.append(f"| {scenario} | {value:.4f} |")

    lines.extend([
        "",
        "## Reliability",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| failures | {reli['failures']} |",
        f"| samples | {reli['samples']} |",
    ])
    for gpu, value in reli["gpu_peak_gb"].items():
        lines.append(f"| gpu_peak_{gpu}_gb | {value:.2f} |")

    lines.extend([
        "",
        "## Scheduler / Backpressure",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| backpressure_events | {scheduler.get('backpressure_events', 0)} |",
        f"| reject_events | {scheduler.get('reject_events', 0)} |",
    ])
    for level, count in sorted(scheduler.get("degrade_level_counts", {}).items()):
        lines.append(f"| degrade:{level} | {count} |")

    lines.extend([
        "",
        "## Transport / Prefetch",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| kv_transfer_gbps | {transport.get('kv_transfer_gbps', 0.0):.2f} |",
        f"| layered_transfer_batches_avg | {transport.get('layered_transfer_batches_avg', 0.0):.2f} |",
        f"| mm_prefetch_hit_rate | {transport.get('mm_prefetch_hit_rate', 0.0):.4f} |",
        f"| mm_recompute_fallbacks | {transport.get('mm_recompute_fallbacks', 0)} |",
    ])
    for backend, count in sorted(transport.get("remote_transfer_backend_counts", {}).items()):
        lines.append(f"| backend:{backend} | {count} |")

    if serving_e2e:
        lines.extend([
            "",
            "## Serving E2E",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| available | {str(bool(serving_e2e.get('available'))).lower()} |",
            f"| pd_route_ok | {str(bool(serving_e2e.get('pd_route_ok'))).lower()} |",
            f"| epd_route_ok | {str(bool(serving_e2e.get('epd_route_ok'))).lower()} |",
            f"| path_split_ok | {str(bool(serving_e2e.get('path_split_ok'))).lower()} |",
            f"| transfer_path_split_ok | {str(bool(serving_e2e.get('transfer_path_split_ok'))).lower()} |",
            f"| workflow_registry_enabled | {str(bool(serving_e2e.get('workflow_registry_enabled'))).lower()} |",
            f"| workflow_registry_events | {int(serving_e2e.get('workflow_registry_events', 0) or 0)} |",
            f"| connector_metric_workers | {int(serving_e2e.get('connector_metric_workers', 0) or 0)} |",
            f"| layered_transfer_grouped_batches | {int(serving_e2e.get('layered_transfer_grouped_batches', 0) or 0)} |",
            f"| layered_receive_group_batches | {int(serving_e2e.get('layered_receive_group_batches', 0) or 0)} |",
            f"| peer_buffer_batches | {int(serving_e2e.get('peer_buffer_batches', 0) or 0)} |",
            f"| pd_transfer_grouped_descriptors | {int(serving_e2e.get('pd_transfer_grouped_descriptors', 0) or 0)} |",
            f"| epd_transfer_grouped_descriptors | {int(serving_e2e.get('epd_transfer_grouped_descriptors', 0) or 0)} |",
            f"| pd_transfer_grouped_bytes | {int(serving_e2e.get('pd_transfer_grouped_bytes', 0) or 0)} |",
            f"| epd_transfer_grouped_bytes | {int(serving_e2e.get('epd_transfer_grouped_bytes', 0) or 0)} |",
        ])
        for backend, count in sorted(serving_e2e.get("remote_transfer_backend_counts", {}).items()):
            lines.append(f"| e2e_backend:{backend} | {count} |")
    if serving_e2e_sanity:
        lines.extend([
            "",
            "## Serving E2E Sanity",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| sanity_pass | {str(bool(serving_e2e_sanity.get('sanity_pass'))).lower()} |",
            f"| connector_workers_ok | {str(bool(serving_e2e_sanity.get('connector_workers_ok'))).lower()} |",
            f"| group_batch_parity_ok | {str(bool(serving_e2e_sanity.get('group_batch_parity_ok'))).lower()} |",
            f"| finished_request_parity_ok | {str(bool(serving_e2e_sanity.get('finished_request_parity_ok'))).lower()} |",
            f"| peer_backend_parity_ok | {str(bool(serving_e2e_sanity.get('peer_backend_parity_ok'))).lower()} |",
            f"| layer_wait_present | {str(bool(serving_e2e_sanity.get('layer_wait_present'))).lower()} |",
            f"| receive_failures_zero | {str(bool(serving_e2e_sanity.get('receive_failures_zero'))).lower()} |",
            f"| group_batches_multiple_of_requests | {str(bool(serving_e2e_sanity.get('group_batches_multiple_of_requests'))).lower()} |",
            f"| wait_calls_multiple_of_receive_batches | {str(bool(serving_e2e_sanity.get('wait_calls_multiple_of_receive_batches'))).lower()} |",
            f"| descriptors_multiple_of_group_batches | {str(bool(serving_e2e_sanity.get('descriptors_multiple_of_group_batches'))).lower()} |",
            f"| wait_calls_per_receive_batch | {float(serving_e2e_sanity.get('wait_calls_per_receive_batch', 0.0) or 0.0):.2f} |",
            f"| descriptors_per_group_batch | {float(serving_e2e_sanity.get('descriptors_per_group_batch', 0.0) or 0.0):.2f} |",
            f"| group_batches_per_request | {float(serving_e2e_sanity.get('group_batches_per_request', 0.0) or 0.0):.2f} |",
        ])
    if feature_handle_e2e.get("available"):
        lines.extend([
            "",
            "## FeatureHandle E2E",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| pass | {str(bool(feature_handle_e2e.get('pass'))).lower()} |",
            f"| status_code | {int(feature_handle_e2e.get('status_code', 0) or 0)} |",
            f"| route | {feature_handle_e2e.get('route', '')} |",
            f"| precomputed_hits | {int(feature_handle_e2e.get('precomputed_hits', 0) or 0)} |",
            f"| hidden_cache_full_miss_batches | {int(feature_handle_e2e.get('hidden_cache_full_miss_batches', 0) or 0)} |",
            f"| hidden_cache_vision_compute_ms_avg | {float(feature_handle_e2e.get('hidden_cache_vision_compute_ms_avg', 0.0) or 0.0):.4f} |",
            f"| fallback_batches | {int(feature_handle_e2e.get('fallback_batches', 0) or 0)} |",
            f"| layered_receive_failures | {int(feature_handle_e2e.get('layered_receive_failures', 0) or 0)} |",
            f"| peer_buffer_direct_batches | {int(feature_handle_e2e.get('peer_buffer_direct_batches', 0) or 0)} |",
        ])

    if feature_handle_paired.get("available"):
        lines.extend([
            "",
            "## FeatureHandle Paired Benchmark",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| pass | {str(bool(feature_handle_paired.get('pass'))).lower()} |",
            f"| asset_ttft_ms | {float(feature_handle_paired.get('asset_ttft_ms', 0.0) or 0.0):.2f} |",
            f"| feature_ttft_ms | {float(feature_handle_paired.get('feature_ttft_ms', 0.0) or 0.0):.2f} |",
            f"| ttft_reduction_pct | {float(feature_handle_paired.get('ttft_reduction_pct', 0.0) or 0.0):.4f} |",
            f"| asset_goodput_rps | {float(feature_handle_paired.get('asset_goodput_rps', 0.0) or 0.0):.4f} |",
            f"| feature_goodput_rps | {float(feature_handle_paired.get('feature_goodput_rps', 0.0) or 0.0):.4f} |",
            f"| goodput_gain_pct | {float(feature_handle_paired.get('goodput_gain_pct', 0.0) or 0.0):.4f} |",
            f"| feature_precomputed_hits | {int(feature_handle_paired.get('feature_precomputed_hits', 0) or 0)} |",
            f"| feature_vision_compute_ms_avg | {float(feature_handle_paired.get('feature_vision_compute_ms_avg', 0.0) or 0.0):.4f} |",
            f"| feature_peer_buffer_direct_batches | {int(feature_handle_paired.get('feature_peer_buffer_direct_batches', 0) or 0)} |",
            f"| text_token_jaccard | {float(feature_handle_paired.get('text_token_jaccard', 0.0) or 0.0):.4f} |",
        ])

    if serving_correctness:
        lines.extend([
            "",
            "## Serving Correctness vs Baseline",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| available | {str(bool(serving_correctness.get('available'))).lower()} |",
            f"| pass_recommendation | {str(bool(serving_correctness.get('pass_recommendation'))).lower()} |",
            f"| pass_recommendation_label | {serving_correctness.get('pass_recommendation_label', '')} |",
            f"| count | {int(serving_correctness.get('count', 0) or 0)} |",
            f"| exact_match_rate | {float(serving_correctness.get('exact_match_rate', 0.0) or 0.0):.4f} |",
            f"| normalized_exact_match_rate | {float(serving_correctness.get('normalized_exact_match_rate', 0.0) or 0.0):.4f} |",
            f"| same_finish_reason_rate | {float(serving_correctness.get('same_finish_reason_rate', 0.0) or 0.0):.4f} |",
            f"| normalized_exact_or_high_overlap_rate | {float(serving_correctness.get('normalized_exact_or_high_overlap_rate', 0.0) or 0.0):.4f} |",
            f"| control_plane_equivalent_rate | {float(serving_correctness.get('control_plane_equivalent_rate', 0.0) or 0.0):.4f} |",
            f"| structural_completion_equivalent_rate | {float(serving_correctness.get('structural_completion_equivalent_rate', 0.0) or 0.0):.4f} |",
            f"| avg_token_jaccard | {float(serving_correctness.get('avg_token_jaccard', 0.0) or 0.0):.4f} |",
            f"| min_token_jaccard | {float(serving_correctness.get('min_token_jaccard', 0.0) or 0.0):.4f} |",
            f"| high_overlap_threshold | {float(serving_correctness.get('high_overlap_threshold', 0.0) or 0.0):.4f} |",
            f"| min_pass_rate | {float(serving_correctness.get('min_pass_rate', 0.0) or 0.0):.4f} |",
            f"| min_same_finish_reason_rate | {float(serving_correctness.get('min_same_finish_reason_rate', 0.0) or 0.0):.4f} |",
            f"| min_avg_token_jaccard_threshold | {float(serving_correctness.get('min_avg_token_jaccard_threshold', 0.0) or 0.0):.4f} |",
            f"| min_token_jaccard_threshold | {float(serving_correctness.get('min_token_jaccard_threshold', 0.0) or 0.0):.4f} |",
            f"| baseline_elapsed_ms_avg | {float(serving_correctness.get('baseline_elapsed_ms_avg', 0.0) or 0.0):.2f} |",
            f"| serving_elapsed_ms_avg | {float(serving_correctness.get('serving_elapsed_ms_avg', 0.0) or 0.0):.2f} |",
        ])
        gate_failures = list(serving_correctness.get("gate_failures") or [])
        if gate_failures:
            lines.extend([
                "",
                "### Correctness Gate Failures",
                "",
            ])
            for item in gate_failures:
                lines.append(f"- {item}")
        gate_warnings = list(serving_correctness.get("gate_warnings") or [])
        if gate_warnings:
            lines.extend([
                "",
                "### Correctness Gate Warnings",
                "",
            ])
            for item in gate_warnings:
                lines.append(f"- {item}")
    if ablation_gap:
        lines.extend([
            "",
            "## TTFT Gap Decomposition",
            "",
            "| Comparison | Type | Base | Target | Base TTFT ms | Target TTFT ms | Effect ms | Effect % |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
        ])
        for row in ablation_gap:
            lines.append(
                f"| {row['label']} | {row['effect_type']} | {row['base']} | {row['target']} | "
                f"{row['base_ttft_ms']:.2f} | {row['target_ttft_ms']:.2f} | {row['effect_ms']:.2f} | {row['effect_pct']:.2f} |"
            )
    if ablation_ordering:
        lines.extend([
            "",
            "## Ablation Ordering Sanity",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| ordering_pass | {str(bool(ablation_ordering.get('ordering_pass'))).lower()} |",
        ])
        rows = list(ablation_ordering.get("rows") or [])
        if rows:
            lines.extend([
                "",
                "| Rule | LHS | RHS | Expect | Delta ms (lhs-rhs) | Pass |",
                "| --- | --- | --- | --- | ---: | ---: |",
            ])
            for row in rows:
                lines.append(
                    f"| {row['label']} | {row['lhs']} | {row['rhs']} | {row['expect']} | "
                    f"{float(row.get('delta_ms', 0.0) or 0.0):.2f} | {str(bool(row.get('pass'))).lower()} |"
                )
    if b8_penalty:
        lines.extend([
            "",
            "## B8 Approx Reuse Penalty",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| penalty_ms | {float(b8_penalty.get('penalty_ms', 0.0) or 0.0):.2f} |",
            f"| penalty_pct | {float(b8_penalty.get('penalty_pct', 0.0) or 0.0):.2f} |",
            f"| exact_reuse_ratio | {float(b8_penalty.get('exact_reuse_ratio', 0.0) or 0.0):.4f} |",
            f"| approx_reuse_ratio | {float(b8_penalty.get('approx_reuse_ratio', 0.0) or 0.0):.4f} |",
            f"| delta_reuse_ratio_avg | {float(b8_penalty.get('delta_reuse_ratio_avg', 0.0) or 0.0):.4f} |",
            f"| tier2_reused_tokens_avg | {float(b8_penalty.get('tier2_reused_tokens_avg', 0.0) or 0.0):.2f} |",
            f"| tier2_recomputed_tokens_avg | {float(b8_penalty.get('tier2_recomputed_tokens_avg', 0.0) or 0.0):.2f} |",
            f"| tier3_accepted_pages_avg | {float(b8_penalty.get('tier3_accepted_pages_avg', 0.0) or 0.0):.2f} |",
            f"| relay_segments_avg | {float(b8_penalty.get('relay_segments_avg', 0.0) or 0.0):.2f} |",
            f"| relay_recompute_segments_avg | {float(b8_penalty.get('relay_recompute_segments_avg', 0.0) or 0.0):.2f} |",
            f"| delta_prefill_ms_avg | {float(b8_penalty.get('delta_prefill_ms_avg', 0.0) or 0.0):.2f} |",
            f"| pipeline_overhead_ms_avg | {float(b8_penalty.get('pipeline_overhead_ms_avg', 0.0) or 0.0):.2f} |",
            f"| reuse_pipeline_ms_avg | {float(b8_penalty.get('reuse_pipeline_ms_avg', 0.0) or 0.0):.2f} |",
            f"| dominant_cost_bucket | {str(b8_penalty.get('dominant_cost_bucket', ''))} |",
            f"| delta_prefill_share_of_b8_ttft | {float(b8_penalty.get('delta_prefill_share_of_b8_ttft', 0.0) or 0.0):.4f} |",
            f"| pipeline_overhead_share_of_b8_ttft | {float(b8_penalty.get('pipeline_overhead_share_of_b8_ttft', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_reuse_ratio | {float(b8_penalty.get('corr_penalty_reuse_ratio', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_delta_reuse_ratio | {float(b8_penalty.get('corr_penalty_delta_reuse_ratio', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_tier3_pages | {float(b8_penalty.get('corr_penalty_tier3_pages', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_tier2_recomputed_tokens | {float(b8_penalty.get('corr_penalty_tier2_recomputed_tokens', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_delta_prefill_ms | {float(b8_penalty.get('corr_penalty_delta_prefill_ms', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_pipeline_overhead_ms | {float(b8_penalty.get('corr_penalty_pipeline_overhead_ms', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_relay_segments | {float(b8_penalty.get('corr_penalty_relay_segments', 0.0) or 0.0):.4f} |",
            f"| corr_penalty_relay_recompute_segments | {float(b8_penalty.get('corr_penalty_relay_recompute_segments', 0.0) or 0.0):.4f} |",
            f"| tier2_reused_tokens_constant | {str(bool(b8_penalty.get('tier2_reused_tokens_constant'))).lower()} |",
        ])
        max_penalty = dict(b8_penalty.get("max_penalty_scenario") or {})
        min_penalty = dict(b8_penalty.get("min_penalty_scenario") or {})
        if max_penalty:
            lines.append(
                f"| max_penalty_scenario | {max_penalty.get('scenario')}:{max_penalty.get('image')}:{float(max_penalty.get('penalty_ms', 0.0) or 0.0):.2f} |"
            )
        if min_penalty:
            lines.append(
                f"| min_penalty_scenario | {min_penalty.get('scenario')}:{min_penalty.get('image')}:{float(min_penalty.get('penalty_ms', 0.0) or 0.0):.2f} |"
            )
        per_scenario = list(b8_penalty.get("per_scenario") or [])
        if per_scenario:
            lines.extend([
                "",
                "| Scenario | Image | B7 TTFT ms | B8 TTFT ms | Penalty ms | Penalty % | reuse_ratio | delta_reuse_ratio | tier2_tokens | tier2_recomputed | tier3_pages | delta_prefill_ms | overhead_ms | relay_segments | relay_recompute_segments |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ])
            for row in per_scenario:
                lines.append(
                    f"| {row['scenario']} | {row['image']} | {row['b7_ttft_ms']:.2f} | {row['b8_ttft_ms']:.2f} | "
                    f"{row['penalty_ms']:.2f} | {row['penalty_pct']:.2f} | {row['reuse_ratio']:.4f} | "
                    f"{float(row.get('delta_reuse_ratio', 0.0) or 0.0):.4f} | "
                    f"{row['tier2_reused_tokens']:.2f} | {float(row.get('tier2_recomputed_tokens', 0.0) or 0.0):.2f} | "
                    f"{row['tier3_accepted_pages']:.2f} | {float(row.get('delta_prefill_ms', 0.0) or 0.0):.2f} | "
                    f"{float(row.get('pipeline_overhead_ms', 0.0) or 0.0):.2f} | "
                    f"{float(row.get('relay_segments', 0.0) or 0.0):.2f} | "
                    f"{float(row.get('relay_recompute_segments', 0.0) or 0.0):.2f} |"
                )

    lines.extend([
        "",
        "## Dataset",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| workflow_traces | {data['workflow_traces']} |",
        f"| soak_examples | {data['soak_examples']} |",
        f"| unique_images | {data['unique_images']} |",
    ])
    for scenario, count in data["scenarios"].items():
        lines.append(f"| scenario:{scenario} | {count} |")
    if dataset_eval:
        wf = dict(dataset_eval.get("workflow_summary") or {})
        chat = dict(dataset_eval.get("chat_summary") or {})
        schedules = dict(dataset_eval.get("schedule_summary") or {})
        source_manifest = dict(dataset_eval.get("source_manifest_summary") or {})
        lines.extend([
            "",
            "## Mooncake Test Dataset",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| pass | {str(bool(dataset_eval.get('pass'))).lower()} |",
            f"| workflow_total | {int(wf.get('total_workflows', 0) or 0)} |",
            f"| chat_total | {int(chat.get('total_examples', 0) or 0)} |",
            f"| workflow_image_valid_rate | {float(wf.get('image_path_valid_rate', 0.0) or 0.0):.4f} |",
            f"| chat_image_valid_rate | {float(chat.get('image_path_valid_rate', 0.0) or 0.0):.4f} |",
            f"| schedule_count | {int(schedules.get('schedule_count', 0) or 0)} |",
            f"| downloaded_sources | {int(source_manifest.get('downloaded_sources', 0) or 0)} |",
            f"| total_sources | {int(source_manifest.get('total_sources', 0) or 0)} |",
        ])
        for family, count in sorted((wf.get("by_family") or {}).items()):
            lines.append(f"| workflow:{family} | {count} |")
        for family, count in sorted((chat.get("by_family") or {}).items()):
            lines.append(f"| chat:{family} | {count} |")
    if workload_matrix:
        lines.extend([
            "",
            "## RFC Workloads (dev-small)",
            "",
            "| Workload | Samples | Avg steps | Image reuse rate | Scenarios |",
            "| --- | ---: | ---: | ---: | --- |",
        ])
        for row in workload_matrix:
            lines.append(
                f"| {row['workload']} | {row['samples']} | {row['avg_steps']:.2f} | "
                f"{row['image_reuse_rate']:.2f} | {', '.join(row['scenarios'])} |"
            )
    if baseline_matrix:
        lines.extend([
            "",
            "## Baseline Matrix",
            "",
            "| ID | Name | Available | Latency ms | Decode TPS | Purpose |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ])
        for row in baseline_matrix:
            latency = "-" if row["latency_ms"] is None else f"{row['latency_ms']:.2f}"
            decode_tps = "-" if row["decode_tps"] is None else f"{row['decode_tps']:.2f}"
            lines.append(
                f"| {row['id']} | {row['name']} | "
                f"{'yes' if row['available'] else 'no'} | "
                f"{latency} | {decode_tps} | {row['purpose']} |"
            )
    lines.append("")
    return "\n".join(lines)
