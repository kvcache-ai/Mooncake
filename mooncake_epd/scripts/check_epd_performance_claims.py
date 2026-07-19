"""Fail-closed performance-claim checker for EPD benchmark summaries."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _nested(payload: Dict[str, Any], path: Tuple[str, ...]) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur.get(key)
    return cur


def _num(payload: Dict[str, Any], *keys: str | Tuple[str, ...], default: float = 0.0) -> float:
    for key in keys:
        value = _nested(payload, key) if isinstance(key, tuple) else payload.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    return default


_FAIR_CONFIG_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("request", "model"),
    ("request", "max_tokens"),
    ("request", "temperature"),
    ("request", "prompt_source"),
    ("request_variation", "mode"),
    ("request_variation", "schema_version"),
    ("vllm_prefix_cache", "mode"),
    ("vllm_prefix_cache", "schema_version"),
    ("load", "repeat_requests"),
    ("load", "concurrency"),
    ("load", "requested_warmup_requests"),
    ("load", "requested_warmup_concurrency"),
    ("load", "warmup_cover_dataset_cycle"),
    ("load", "between_repeat_sleep_s"),
    ("serving", "max_model_len"),
    ("serving", "gpu_memory_utilization"),
    ("serving", "max_num_batched_tokens"),
    ("serving", "max_num_seqs"),
    ("serving", "tensor_parallel_size"),
)


def _path_label(path: Tuple[str, ...]) -> str:
    return ".".join(path)


def _benchmark_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = payload.get("benchmark_config")
    return dict(config) if isinstance(config, dict) else {}


def _fairness_gate(baseline: Dict[str, Any], epd: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
    """Reject comparisons that do not use equivalent workload and resources.

    A disaggregated topology often consumes more GPUs than a colocated baseline.
    That can still be useful operational data, but it is not evidence of a
    same-resource performance win. Keep this check intentionally strict so a
    benchmark artifact cannot accidentally make that claim.
    """

    base_config = _benchmark_config(baseline)
    epd_config = _benchmark_config(epd)
    failures: List[str] = []
    details: Dict[str, Any] = {
        "baseline_has_benchmark_config": bool(base_config),
        "epd_has_benchmark_config": bool(epd_config),
    }
    if not base_config or not epd_config:
        return False, ["missing_benchmark_config"], details

    base_fingerprint = str(base_config.get("request_fingerprint") or "")
    epd_fingerprint = str(epd_config.get("request_fingerprint") or "")
    details["baseline_request_fingerprint"] = base_fingerprint
    details["epd_request_fingerprint"] = epd_fingerprint
    if not base_fingerprint or not epd_fingerprint:
        failures.append("missing_request_fingerprint")
    elif base_fingerprint != epd_fingerprint:
        failures.append("request_fingerprint_mismatch")

    for path in _FAIR_CONFIG_PATHS:
        base_value = _nested(base_config, path)
        epd_value = _nested(epd_config, path)
        if base_value is None or epd_value is None:
            failures.append(f"missing_benchmark_config:{_path_label(path)}")
        elif base_value != epd_value:
            failures.append(f"benchmark_config_mismatch:{_path_label(path)}")

    base_gpus = _nested(base_config, ("topology", "total_gpus"))
    epd_gpus = _nested(epd_config, ("topology", "total_gpus"))
    details["baseline_total_gpus"] = base_gpus
    details["epd_total_gpus"] = epd_gpus
    try:
        resource_matched = int(base_gpus) == int(epd_gpus) and int(base_gpus) > 0
    except (TypeError, ValueError):
        resource_matched = False
    details["resource_matched"] = resource_matched
    if not resource_matched:
        failures.append("resource_unmatched")
    return not failures, failures, details


def _response_equivalence_gate(
    baseline: Dict[str, Any],
    epd: Dict[str, Any],
) -> Tuple[bool, List[str], Dict[str, Any]]:
    """Require output evidence before treating throughput as comparable."""

    base_config = _benchmark_config(baseline)
    temperature = _nested(base_config, ("request", "temperature"))
    details: Dict[str, Any] = {"temperature": temperature}
    baseline_responses = baseline.get("responses")
    epd_responses = epd.get("responses")
    if not isinstance(baseline_responses, list) or not isinstance(epd_responses, list):
        return False, ["missing_response_equivalence_evidence"], details
    if not baseline_responses or not epd_responses:
        return False, ["empty_response_equivalence_evidence"], details
    if len(baseline_responses) != len(epd_responses):
        details["baseline_response_count"] = len(baseline_responses)
        details["epd_response_count"] = len(epd_responses)
        return False, ["response_count_mismatch"], details
    if not all(isinstance(item, dict) for item in baseline_responses + epd_responses):
        return False, ["invalid_response_equivalence_evidence"], details
    if any(int(item.get("status_code", 0) or 0) >= 400 or item.get("error") for item in baseline_responses + epd_responses):
        return False, ["response_error_present"], details

    try:
        is_deterministic = float(temperature) == 0.0
    except (TypeError, ValueError):
        is_deterministic = False
    details["deterministic_request"] = is_deterministic
    if not is_deterministic:
        # Stochastic workloads require an explicit quality evaluation artifact;
        # token throughput alone says nothing about output quality.
        quality_gate = epd.get("output_quality_gate")
        if not isinstance(quality_gate, dict) or quality_gate.get("verified") is not True:
            return False, ["nondeterministic_output_requires_quality_evidence"], details
        return True, [], details

    base_texts = [str(item.get("response_text") or "") for item in baseline_responses]
    epd_texts = [str(item.get("response_text") or "") for item in epd_responses]
    base_tokens = [item.get("completion_tokens") for item in baseline_responses]
    epd_tokens = [item.get("completion_tokens") for item in epd_responses]
    if any(value is None for value in base_tokens + epd_tokens):
        return False, ["missing_completion_token_evidence"], details
    details["baseline_response_text_variants"] = len(set(base_texts))
    details["epd_response_text_variants"] = len(set(epd_texts))
    details["baseline_completion_token_distribution"] = dict(Counter(int(value) for value in base_tokens))
    details["epd_completion_token_distribution"] = dict(Counter(int(value) for value in epd_tokens))
    failures: List[str] = []
    if Counter(base_tokens) != Counter(epd_tokens):
        failures.append("completion_token_distribution_mismatch")
    if Counter(base_texts) != Counter(epd_texts):
        failures.append("deterministic_response_mismatch")
    return not failures, failures, details


def evaluate_claims(summary: Dict[str, Any]) -> Dict[str, Any]:
    baseline = dict(summary.get("baseline") or {})
    epd = dict(summary.get("epd") or {})
    # Prefer explicit streaming TTFT/goodput fields.  HF local baseline artifacts
    # cannot observe first-token streaming, so use full generation latency only
    # as a conservative proxy and report that choice in metrics below.
    base_ttft = _num(
        baseline,
        "avg_ttft_ms",
        ("ttft_stats_ms", "avg"),
        "epd_avg_ttft_ms",
        "ttft_ms",
        ("latency_stats_ms", "avg"),
    )
    epd_ttft = _num(epd, "avg_ttft_ms", ("ttft_stats_ms", "avg"), "epd_avg_ttft_ms", "ttft_ms")
    base_goodput = _num(
        baseline,
        "goodput_rps",
        "throughput_rps",
        "requests_per_second",
        "request_throughput_rps",
    )
    if base_goodput <= 0:
        avg_latency_ms = _num(baseline, ("latency_stats_ms", "avg"))
        base_goodput = 1000.0 / avg_latency_ms if avg_latency_ms > 0 else 0.0
    epd_goodput = _num(
        epd,
        "goodput_rps",
        "throughput_rps",
        "requests_per_second",
        "request_throughput_rps",
    )
    metric_summary = dict(
        epd.get("feature_handle_metric_summary")
        or epd.get("real_epd_metric_summary")
        or epd.get("metrics")
        or {}
    )
    fallback = _num(epd, "fallback_batches", "fallback_count", ("feature_handle_metric_summary", "fallback_batches"), default=0.0)
    cache_hits = _num(
        epd,
        "feature_cache_hits",
        "precomputed_hits",
        "mm_hidden_cache_hits",
        ("feature_handle_metric_summary", "vision_skip_hits"),
        ("feature_handle_metric_summary", "precomputed_hits"),
        default=0.0,
    )
    transfer_failures = max(
        _num(epd, "layered_receive_failures", ("feature_handle_metric_summary", "layered_receive_failures"), default=0.0),
        _num(epd, "layered_transfer_failed_batches", ("feature_handle_metric_summary", "layered_transfer_failed_batches"), default=0.0),
    )
    vision_miss_batches = _num(metric_summary, "hidden_cache_full_miss_batches", default=0.0)
    vision_compute_ms = _num(metric_summary, "hidden_cache_vision_compute_ms_avg", default=0.0)

    fairness_ok, fairness_failures, fairness_details = _fairness_gate(baseline, epd)
    outputs_ok, output_failures, output_details = _response_equivalence_gate(baseline, epd)
    ttft_improved = bool(fairness_ok and outputs_ok and base_ttft > 0 and epd_ttft > 0 and epd_ttft < base_ttft)
    goodput_improved = bool(fairness_ok and outputs_ok and base_goodput > 0 and epd_goodput > base_goodput)
    direct_path_ok = bool(fallback == 0 and transfer_failures == 0)
    cache_reuse_ok = bool(cache_hits > 0 and vision_miss_batches == 0 and vision_compute_ms == 0)
    claims = {
        "ttft_improvement": ttft_improved,
        "goodput_improvement": goodput_improved,
        "cache_reuse": cache_reuse_ok,
        "direct_transfer": direct_path_ok,
        "fair_comparison": fairness_ok,
        "output_equivalence": outputs_ok,
    }
    failures = list(fairness_failures) + list(output_failures)
    if not ttft_improved:
        failures.append("ttft_not_improved")
    if not goodput_improved:
        failures.append("goodput_not_improved")
    if not cache_reuse_ok:
        failures.append("cache_reuse_not_observed")
    if not direct_path_ok:
        failures.append("direct_transfer_not_clean")
    return {
        "pass": not failures,
        "failures": failures,
        "claims": claims,
        "metrics": {
            "baseline_ttft_ms": base_ttft,
            "epd_ttft_ms": epd_ttft,
            "baseline_goodput_rps": base_goodput,
            "epd_goodput_rps": epd_goodput,
            "epd_cache_hits": cache_hits,
            "epd_fallback_batches": fallback,
            "epd_transfer_failures": transfer_failures,
            "epd_hidden_cache_full_miss_batches": vision_miss_batches,
            "epd_hidden_cache_vision_compute_ms_avg": vision_compute_ms,
            "fairness": fairness_details,
            "output_equivalence": output_details,
            "baseline_ttft_source": "explicit_or_latency_proxy",
            "baseline_goodput_source": (
                "explicit" if _num(baseline, "goodput_rps", "throughput_rps", "requests_per_second", "request_throughput_rps") > 0
                else "latency_avg_proxy"
            ),
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Check whether EPD benchmark artifacts justify performance claims")
    ap.add_argument("--summary", required=True)
    ap.add_argument("--output", default=None)
    ap.add_argument("--enforce", action="store_true")
    args = ap.parse_args()
    result = evaluate_claims(json.loads(Path(args.summary).read_text(encoding="utf-8")))
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    print(text)
    if args.enforce and not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
