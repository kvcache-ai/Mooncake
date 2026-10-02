# Reporting: aggregate the raw JSONL records into JSON/CSV summaries and report
# unavailable for any staged metric that cannot be obtained.

import csv
import json
import math
from dataclasses import dataclass

from .metrics import sum_metric
from .stats import summarize


# Cache path metrics that SGLang exposes, matched by exact base name with the
# declared unit and histogram component. Substring matching would pull in
# metrics with a different meaning: hicache_storage_prefetch_hit_tokens_total is
# a hit count rather than a fetch latency, and hicache_backup_duration_seconds is
# the GPU to host DRAM copy rather than the write to the storage backend.
#
# SGLang 0.5.20 times the two local cache movements and the device eviction, and
# for the storage backend it counts the tokens written and the tokens a prefetch
# served but never times the transfer. The L3 path therefore has token counters
# only, and no L3 latency or bandwidth is derived from them.
@dataclass(frozen=True)
class MetricSpec:
    base_name: str
    unit: str = "milliseconds"
    component: str = "sum"

    @property
    def suffix(self):
        return "" if self.component == "value" else f"_{self.component}"

    @property
    def full_name(self):
        return f"{self.base_name}{self.suffix}"


DURATION_METRICS = {
    "l2_to_gpu_h2d_time": MetricSpec("sglang:load_back_duration_seconds", "seconds"),
    "device_to_l2_backup_time": MetricSpec(
        "sglang:hicache_backup_duration_seconds", "seconds"
    ),
    "device_eviction_time": MetricSpec("sglang:eviction_duration_seconds", "seconds"),
}

BYTE_COUNTERS = {
    "l2_to_gpu_bytes": "sglang:load_back_bytes_total",
    "device_to_l2_bytes": "sglang:hicache_backup_bytes_total",
}

# Counters that measure the storage tier and the two local cache movements in
# tokens. A family that is absent from the scrape means the tier never moved
# anything, which is not the same as the metric being unavailable.
TOKEN_COUNTERS = {
    "l2_to_gpu_tokens": "sglang:load_back_tokens_total",
    "device_to_l2_tokens": "sglang:hicache_backup_tokens_total",
    "l3_write_tokens": "sglang:backuped_tokens_total",
    "l3_prefetch_hit_tokens": "sglang:storage_prefetch_hit_tokens_total",
    "l3_prefetch_unfulfilled_tokens": (
        "sglang:storage_prefetch_unfulfilled_tokens_total"
    ),
}

UNIT_TO_MS = {"seconds": 1000.0, "milliseconds": 1.0}

CACHE_HIT_TOKENS_METRIC = "sglang:cached_tokens_total"
PROMPT_TOKENS_METRIC = "sglang:prompt_tokens_total"

# SGLang reports the tokens each tier served for a request in
# meta_info.cached_tokens_details, which is what tells an L1 hit from an L2 or L3
# hit. The same split is counted in sglang:cached_tokens_total under the
# cache_source label, and both are collected so they can be checked against each
# other.
CACHE_SOURCES = ("device", "host", "storage")

CSV_COLUMNS = [
    "config_id",
    "cache_tier",
    "cache_tier_description",
    "hit_pattern",
    "case_id",
    "input_len",
    "output_len",
    "num_requests",
    "max_concurrency",
    "repeat_index",
    "round_index",
    "rounds",
    "num_samples",
    "ttft_p50_ms",
    "ttft_p95_ms",
    "ttft_p99_ms",
    "itl_p50_ms",
    "itl_p95_ms",
    "itl_p99_ms",
    "tpot_p50_ms",
    "tpot_p95_ms",
    "tpot_p99_ms",
    "e2e_p50_ms",
    "e2e_p95_ms",
    "e2e_p99_ms",
    "mean_ttft_ms",
    "input_tokens_per_s",
    "output_tokens_per_s",
    "total_tokens_per_s",
    "prompt_tokens",
    "output_tokens",
    "cache_hit_rate",
    "hit_tokens",
    "miss_tokens",
    "hit_source",
    "hit_pages",
    "miss_pages",
    "pages_source",
    "hit_tier",
    "device_hit_tokens",
    "host_hit_tokens",
    "storage_hit_tokens",
    "storage_backend",
    "storage_key_present",
    "device_hit_tokens_metric",
    "host_hit_tokens_metric",
    "storage_hit_tokens_metric",
    "tier_attribution_agrees",
    "l1_pool_tokens",
    "l2_pool_tokens",
    "working_set_tokens",
    "l1_can_hold_working_set",
    "l2_can_hold_working_set",
    "kv_bytes_per_token",
    "total_kv_bytes_prompt",
    "l2_to_gpu_h2d_time_ms",
    "device_to_l2_backup_time_ms",
    "device_eviction_time_ms",
    "l2_to_gpu_bytes",
    "device_to_l2_bytes",
    "l2_to_gpu_tokens",
    "device_to_l2_tokens",
    "l3_write_tokens",
    "l3_prefetch_hit_tokens",
    "l3_prefetch_unfulfilled_tokens",
    "l2_to_gpu_bandwidth_gbps",
    "device_to_l2_bandwidth_gbps",
    "mean_ttft_ms",
    "run_wall_ms",
    "errors",
    "timeouts",
    "failed_requests",
]


def load_records(path):
    records = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _percentiles(values):
    if not values:
        return None, None, None
    stats = summarize(values)
    return stats["p50"], stats["p95"], stats["p99"]


def _counter_or_zero(metrics_delta, metrics_present, base_name, label_filters=()):
    """Return the delta when the metric exists (no delta means 0), None when the
    metric is absent."""
    value = sum_metric(metrics_delta, base_name, label_filters)
    if value is not None:
        return value
    if base_name in metrics_present:
        return 0.0
    return None


def _bandwidth(counters, durations, byte_key, duration_key):
    """Bytes of a window divided by the duration of that same window."""
    byte_value = counters.get(byte_key)
    duration = durations.get(duration_key)
    if byte_value is None or duration == "unavailable" or duration is None:
        return None
    if duration["value"] <= 0:
        return None
    return byte_value / (duration["value"] / 1000.0) / 1e9


def _tier_hit_tokens(records):
    """Sum the tokens each cache tier served, from
    meta_info.cached_tokens_details.

    SGLang reports this per request: device is the GPU pool (L1), host is the
    host DRAM pool (L2) and storage is the storage backend (L3). A request with
    no cached tokens carries no details at all, and the storage key is only
    present when a storage backend is configured.
    """
    totals = dict.fromkeys(CACHE_SOURCES, 0)
    saw_details = False
    saw_storage_key = False
    for record in records:
        details = (record.get("meta_info") or {}).get("cached_tokens_details")
        if not isinstance(details, dict):
            continue
        saw_details = True
        if "storage" in details:
            saw_storage_key = True
        for tier in CACHE_SOURCES:
            value = details.get(tier)
            if isinstance(value, (int, float)):
                totals[tier] += value
    return totals, saw_details, saw_storage_key


def _flatten_itl(records):
    values = []
    for record in records:
        values.extend(record.get("itl_ms") or [])
    return values


def _tpot_values(records):
    values = []
    for record in records:
        tokens = record.get("output_tokens") or 0
        if tokens <= 1:
            continue
        values.append((record["e2e_ms"] - record["ttft_ms"]) / (tokens - 1))
    return values


def _sum_meta(records, field):
    total = 0
    found = False
    for record in records:
        value = (record.get("meta_info") or {}).get(field)
        if isinstance(value, (int, float)):
            total += value
            found = True
    return total if found else None


def aggregate_e2e(records, page_size):
    """Aggregate the e2e records by (setup, load point, repeat index)."""
    groups = {}
    for record in records:
        if record.get("kind") != "e2e":
            continue
        config = record["config"]
        case = record["case"]
        key = (
            config["config_id"],
            case["case_id"],
            record["repeat_index"],
            record["round_index"],
        )
        groups.setdefault(key, []).append(record)

    run_metrics = {}
    for record in records:
        if record.get("kind") != "e2e_run_metrics":
            continue
        config = record["config"]
        case = record["case"]
        key = (
            config["config_id"],
            case["case_id"],
            record["repeat_index"],
            record["round_index"],
        )
        run_metrics[key] = record

    rows = []
    for key, group in sorted(groups.items()):
        config_id, case_id, repeat_index, round_index = key
        config = group[0]["config"]
        case = group[0]["case"]
        run_record = run_metrics.get(key, {})
        metrics_delta = run_record.get("metrics_delta", {})

        ttft_p50, ttft_p95, ttft_p99 = _percentiles(
            [record["ttft_ms"] for record in group]
        )
        e2e_p50, e2e_p95, e2e_p99 = _percentiles([record["e2e_ms"] for record in group])
        itl_p50, itl_p95, itl_p99 = _percentiles(_flatten_itl(group))
        tpot_p50, tpot_p95, tpot_p99 = _percentiles(_tpot_values(group))

        prompt_tokens = _sum_meta(group, "prompt_tokens")
        if prompt_tokens is None:
            prompt_tokens = sum_metric(metrics_delta, PROMPT_TOKENS_METRIC)
        output_tokens = sum(record.get("output_tokens") or 0 for record in group)

        wall_ms = run_record.get("run_wall_ms")
        if wall_ms and wall_ms > 0:
            wall_s = wall_ms / 1000.0
            input_tps = (prompt_tokens / wall_s) if prompt_tokens else None
            output_tps = output_tokens / wall_s
            total_tps = ((prompt_tokens or 0) + output_tokens) / wall_s
        else:
            input_tps = output_tps = total_tps = None

        cached_tokens = _sum_meta(group, "cached_tokens")
        hit_value = sum_metric(metrics_delta, CACHE_HIT_TOKENS_METRIC)

        if cached_tokens is not None and prompt_tokens:
            hit_tokens = cached_tokens
            hit_source = "response meta_info cached_tokens"
        elif hit_value is not None and prompt_tokens:
            hit_tokens = hit_value
            hit_source = f"metrics delta {CACHE_HIT_TOKENS_METRIC}"
        else:
            hit_tokens = None
            hit_source = "unavailable"

        miss_tokens = (
            max(0, prompt_tokens - hit_tokens)
            if hit_tokens is not None and prompt_tokens
            else None
        )

        cache_hit_rate = (
            hit_tokens / prompt_tokens
            if hit_tokens is not None and prompt_tokens
            else None
        )

        # Page counts come from the token count and the page size; the report marks
        # them as derived
        if hit_tokens is not None:
            hit_pages = math.ceil(hit_tokens / page_size)
            miss_pages = (
                math.ceil(miss_tokens / page_size) if miss_tokens is not None else None
            )
            pages_source = "derived_from_tokens"
        else:
            hit_pages = miss_pages = None
            pages_source = "unavailable"

        # Which tier served the reuse decides what a tier comparison may claim.
        # The per-request breakdown in meta_info is the direct evidence; the
        # counter with the cache_source label is collected next to it so the two
        # can be checked against each other.
        metrics_present = set(run_record.get("metrics_present") or [])
        tier_tokens, saw_details, saw_storage_key = _tier_hit_tokens(group)
        if not saw_details and not hit_tokens:
            # No request reported any cached tokens, so every tier served zero
            tier_tokens = dict.fromkeys(CACHE_SOURCES, 0)
        served = [tier for tier in CACHE_SOURCES if tier_tokens[tier] > 0]
        if not saw_details and hit_tokens:
            hit_tier = "unknown"
        elif len(served) == 1:
            hit_tier = served[0]
        elif served:
            hit_tier = "mixed"
        else:
            hit_tier = "none"
        # The storage key is present in the response whenever an L3 backend is
        # configured, so its absence means no L3 backend rather than zero tokens
        if case.get("storage_backend"):
            storage_hit_tokens = tier_tokens["storage"]
        else:
            storage_hit_tokens = None

        cached_tokens_per_source = {
            source: _counter_or_zero(
                metrics_delta,
                metrics_present,
                CACHE_HIT_TOKENS_METRIC,
                (f'cache_source="{source}"',),
            )
            for source in CACHE_SOURCES
        }
        if cached_tokens_per_source["device"] is not None and saw_details:
            tier_attribution_agrees = (
                cached_tokens_per_source["device"] == tier_tokens["device"]
                and cached_tokens_per_source["host"] == tier_tokens["host"]
            )
        else:
            tier_attribution_agrees = None

        durations = {}
        for concept, spec in DURATION_METRICS.items():
            value = sum_metric(metrics_delta, spec.full_name)
            if value is None:
                # The family exists but saw no observation this window, so the true
                # value is 0; only an absent family is unavailable
                value = 0.0 if spec.base_name in metrics_present else None
            if value is None:
                durations[concept] = "unavailable"
                continue
            # A histogram _sum is in seconds; the report is in milliseconds
            durations[concept] = {
                "value": value * UNIT_TO_MS[spec.unit],
                "unit": "milliseconds",
                "metric": spec.full_name,
                "source_unit": spec.unit,
            }

        counters = {
            concept: _counter_or_zero(metrics_delta, metrics_present, base_name)
            for concept, base_name in {**BYTE_COUNTERS, **TOKEN_COUNTERS}.items()
        }

        # The pool sizes are known once the server has started, and the working
        # set of a round is the prompt tokens it sends. A tier can only have
        # served the reuse if its pool cannot hold that working set.
        l1_pool_tokens = case.get("l1_pool_tokens")
        l2_pool_tokens = case.get("l2_pool_tokens")
        working_set_tokens = prompt_tokens
        l1_can_hold = (
            working_set_tokens <= l1_pool_tokens
            if l1_pool_tokens and working_set_tokens
            else None
        )
        l2_can_hold = (
            working_set_tokens <= l2_pool_tokens
            if l2_pool_tokens and working_set_tokens
            else None
        )

        # Both sides of a bandwidth come from the same metrics window: the bytes
        # moved and the duration of moving them. A prefetch can serve a later
        # round, so the report keeps these as window totals and derives no
        # per-request share of a latency from them.
        l2_to_gpu_bandwidth = _bandwidth(
            counters, durations, "l2_to_gpu_bytes", "l2_to_gpu_h2d_time"
        )
        device_to_l2_bandwidth = _bandwidth(
            counters, durations, "device_to_l2_bytes", "device_to_l2_backup_time"
        )

        mean_ttft = (
            sum(record["ttft_ms"] for record in group) / len(group) if group else None
        )

        errors = sum(1 for record in group if record.get("error"))
        rows.append(
            {
                "config_id": config_id,
                "cache_tier": config["cache_tier"],
                "cache_tier_description": config["cache_tier_description"],
                "hit_pattern": config["hit_pattern"],
                "case_id": case_id,
                "input_len": case["input_len"],
                "output_len": case["output_len"],
                "num_requests": case["num_requests"],
                "max_concurrency": case["max_concurrency"],
                "repeat_index": repeat_index,
                "round_index": round_index,
                "rounds": config["rounds"],
                "num_samples": len(group),
                "ttft_p50_ms": ttft_p50,
                "ttft_p95_ms": ttft_p95,
                "ttft_p99_ms": ttft_p99,
                "itl_p50_ms": itl_p50,
                "itl_p95_ms": itl_p95,
                "itl_p99_ms": itl_p99,
                "tpot_p50_ms": tpot_p50,
                "tpot_p95_ms": tpot_p95,
                "tpot_p99_ms": tpot_p99,
                "e2e_p50_ms": e2e_p50,
                "e2e_p95_ms": e2e_p95,
                "e2e_p99_ms": e2e_p99,
                "input_tokens_per_s": input_tps,
                "output_tokens_per_s": output_tps,
                "total_tokens_per_s": total_tps,
                "prompt_tokens": prompt_tokens,
                "output_tokens": output_tokens,
                "cache_hit_rate": cache_hit_rate,
                "hit_tokens": hit_tokens,
                "miss_tokens": miss_tokens,
                "hit_source": hit_source,
                "hit_pages": hit_pages,
                "miss_pages": miss_pages,
                "pages_source": pages_source,
                "hit_tier": hit_tier,
                "device_hit_tokens": tier_tokens["device"],
                "host_hit_tokens": tier_tokens["host"],
                "storage_hit_tokens": storage_hit_tokens,
                "storage_backend": case.get("storage_backend"),
                "storage_key_present": saw_storage_key,
                "device_hit_tokens_metric": cached_tokens_per_source["device"],
                "host_hit_tokens_metric": cached_tokens_per_source["host"],
                "storage_hit_tokens_metric": cached_tokens_per_source["storage"],
                "tier_attribution_agrees": tier_attribution_agrees,
                "l1_pool_tokens": l1_pool_tokens,
                "l2_pool_tokens": l2_pool_tokens,
                "working_set_tokens": working_set_tokens,
                "l1_can_hold_working_set": l1_can_hold,
                "l2_can_hold_working_set": l2_can_hold,
                "kv_bytes_per_token": case.get("kv_bytes_per_token_aggregate"),
                "total_kv_bytes_prompt": case.get("total_kv_bytes_prompt"),
                "l2_to_gpu_h2d_time_ms": durations["l2_to_gpu_h2d_time"],
                "device_to_l2_backup_time_ms": durations["device_to_l2_backup_time"],
                "device_eviction_time_ms": durations["device_eviction_time"],
                "l2_to_gpu_bytes": counters["l2_to_gpu_bytes"],
                "device_to_l2_bytes": counters["device_to_l2_bytes"],
                "l2_to_gpu_tokens": counters["l2_to_gpu_tokens"],
                "device_to_l2_tokens": counters["device_to_l2_tokens"],
                "l3_write_tokens": counters["l3_write_tokens"],
                "l3_prefetch_hit_tokens": counters["l3_prefetch_hit_tokens"],
                "l3_prefetch_unfulfilled_tokens": counters[
                    "l3_prefetch_unfulfilled_tokens"
                ],
                "l2_to_gpu_bandwidth_gbps": l2_to_gpu_bandwidth,
                "device_to_l2_bandwidth_gbps": device_to_l2_bandwidth,
                "mean_ttft_ms": mean_ttft,
                "run_wall_ms": wall_ms,
                "errors": errors,
                "timeouts": 0,
                "failed_requests": errors,
            }
        )
    return rows


def check_repeat_stability(rows, threshold=0.05):
    """Check the p50 spread of one setup at one load point across repeats."""
    grouped = {}
    for row in rows:
        key = (row["config_id"], row["case_id"], row["round_index"])
        grouped.setdefault(key, []).append(row)

    warnings = []
    for key, group in sorted(grouped.items()):
        if len(group) < 2:
            continue
        for metric in ("ttft_p50_ms", "e2e_p50_ms", "tpot_p50_ms"):
            values = [row[metric] for row in group if row.get(metric) is not None]
            if len(values) < 2:
                continue
            low, high = min(values), max(values)
            if low <= 0:
                continue
            spread = (high - low) / low
            if spread > threshold:
                warnings.append(
                    {
                        "config_id": key[0],
                        "case_id": key[1],
                        "round_index": key[2],
                        "metric": metric,
                        "min": low,
                        "max": high,
                        "relative_spread": spread,
                        "repeats": len(values),
                        "values": values,
                    }
                )
    return warnings


def kernel_summary(records):
    rows = []
    for record in records:
        if record.get("kind") != "kernel":
            continue
        case = record["case"]
        row = {
            "case_id": case["label"],
            "mode": case["mode"],
            "seq_len": case["seq_len"],
            "batch_size": case["batch_size"],
            "page_size": case["page_size"],
            "num_layers": case["num_layers"],
            "num_kv_heads_per_rank": case["num_kv_heads"],
            "num_qo_heads_per_rank": case["num_qo_heads"],
            "head_dim": case["head_dim"],
            "kv_bytes_written": case["kv_bytes_written"],
            "kv_bytes_read_valid": case["kv_bytes_read_valid"],
            "kv_bytes_read_moved": case["kv_bytes_read_moved"],
            "padding_tokens_read": case["padding_tokens_read"],
            "kv_cache_bytes": case["kv_cache_bytes"],
            "warmup": record["warmup"],
            "timed": record["timed"],
            "device_name": record["device_name"],
            "scatter_passed": record["correctness"]["scatter"].get("passed")
            if isinstance(record["correctness"]["scatter"], dict)
            else None,
            "attention_passed": record["correctness"]["attention"].get("passed")
            if isinstance(record["correctness"]["attention"], dict)
            else None,
        }
        for phase in (
            "metadata",
            "metadata_cpu",
            "metadata_h2d",
            "kv_write",
            "kv_gather",
            "attention_plan",
            "attention",
            "total_step",
        ):
            stats = record["phases"][phase]
            row[f"{phase}_p50_ms"] = stats["p50"]
            row[f"{phase}_p95_ms"] = stats["p95"]
            row[f"{phase}_p99_ms"] = stats["p99"]
        row["per_layer_total_p50_ms"] = record["per_layer_p50_ms"]["total_step"]
        row.update(record["derived"])
        rows.append(row)
    return rows


def write_csv(path, rows, columns=None):
    if not rows:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
        return
    fieldnames = columns or list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat = {
                key: (json.dumps(value) if isinstance(value, (dict, list)) else value)
                for key, value in row.items()
            }
            writer.writerow(flat)


def build_markdown_summary(
    e2e_rows, kernel_rows, manifest, warnings, unavailable_notes
):
    lines = []
    lines.append("# Attention -> KV cache access benchmark results")
    lines.append("")
    lines.append(f"Generated at: {manifest.get('generated_at')}")
    gpu = manifest.get("gpu", {})
    lines.append(
        f"GPU measured: {', '.join(sorted(set(gpu.get('gpu_names') or ['unknown'])))} "
        f"x {gpu.get('gpu_count')}"
    )
    model = manifest.get("model", {})
    lines.append(
        f"Model: {model.get('path')} dtype={model.get('dtype')} "
        f"tp={model.get('tp_size')} page_size={model.get('page_size')}"
    )
    kv = model.get("kv_config", {})
    if kv:
        lines.append(
            f"KV shape: {kv.get('num_layers')} layers x "
            f"{kv.get('num_key_value_heads')} KV heads "
            f"x head_dim {kv.get('head_dim')}; "
            f"{model.get('kv_bytes_per_token_aggregate')} bytes per token "
            f"aggregate, {model.get('kv_bytes_per_token_per_rank')} per rank"
        )
    lines.append("")

    lines.append("## 1. Attention kernel and KV cache access cost")
    lines.append("")
    lines.append(
        "| case | metadata p50 | metadata H2D p50 | kv_write p50 | kv_gather p50 | "
        "attention_plan p50 | attention p50 | attention step p50 | kv_gather GB/s | "
        "attention TFLOP/s | arithmetic intensity FLOP/byte | scatter | "
        "attention check |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for row in kernel_rows:
        lines.append(
            f"| {row['case_id']} | {row['metadata_p50_ms']:.3f} | "
            f"{row['metadata_h2d_p50_ms']:.3f} | "
            f"{row['kv_write_p50_ms']:.3f} | {row['kv_gather_p50_ms']:.3f} | "
            f"{row['attention_plan_p50_ms']:.3f} | "
            f"{row['attention_p50_ms']:.3f} | {row['total_step_p50_ms']:.3f} | "
            f"{row['kv_gather_effective_gbps']:.1f} | "
            f"{row['attention_tflops']:.1f} | "
            f"{row['attention_arithmetic_intensity']:.1f} | "
            f"{row['scatter_passed']} | {row['attention_passed']} |"
        )
    lines.append("")
    lines.append(
        "The attention step is metadata + kv_write + attention_plan + attention, "
        "which is what one forward pass runs. metadata H2D is the host to device "
        "copy of the page table and index tensors inside the metadata window, "
        "reported as a breakdown of it rather than added to it. attention_plan is "
        "the backend compiling its schedule and attention is the per-layer kernel "
        "time. kv_gather is a separate read-only probe that copies every layer's "
        "KV through the page table; it is compared against the step, never added "
        "to it, so it does not appear in the step total or the per-token figures. "
        "When the arithmetic intensity is above this machine's balance point "
        "attention is compute-bound, so its effective GB/s is not the KV read "
        "bandwidth; read the kv_gather GB/s instead."
    )
    lines.append("")

    lines.append("## 2. End-to-end benefit of Mooncake KV transfer and HiCache reuse")
    lines.append("")
    lines.append(
        "| config | case | round | repeat | TTFT p50 | ITL p50 | TPOT p50 | E2E p50 | "
        "hit rate | served by | device tok | host tok | L3 tok | working set | "
        "L1 pool | L2 pool |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for row in e2e_rows:
        lines.append(
            f"| {row['config_id']} | {row['case_id']} | {row['round_index']} | "
            f"{row['repeat_index']} | "
            f"{_fmt(row['ttft_p50_ms'])} | "
            f"{_fmt(row['itl_p50_ms'])} | {_fmt(row['tpot_p50_ms'])} | "
            f"{_fmt(row['e2e_p50_ms'])} | {_fmt(row['cache_hit_rate'])} | "
            f"{row['hit_tier']} | "
            f"{_fmt_int(row['device_hit_tokens'])} | "
            f"{_fmt_int(row['host_hit_tokens'])} | "
            f"{_fmt_int(row['storage_hit_tokens'])} | "
            f"{_fmt_int(row['working_set_tokens'])} | "
            f"{_fmt_int(row['l1_pool_tokens'])} | "
            f"{_fmt_int(row['l2_pool_tokens'])} |"
        )
    lines.append("")
    lines.extend(_reuse_path_lines(e2e_rows))
    lines.append("")
    lines.append(
        "| config | case | round | L2 to GPU ms | L2 to GPU GB/s | device to L2 ms | "
        "device to L2 GB/s | device eviction ms | L3 written tok | "
        "L3 prefetch hit tok |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for row in e2e_rows:
        lines.append(
            f"| {row['config_id']} | {row['case_id']} | {row['round_index']} | "
            f"{_fmt_segment(row['l2_to_gpu_h2d_time_ms'])} | "
            f"{_fmt(row['l2_to_gpu_bandwidth_gbps'])} | "
            f"{_fmt_segment(row['device_to_l2_backup_time_ms'])} | "
            f"{_fmt(row['device_to_l2_bandwidth_gbps'])} | "
            f"{_fmt_segment(row['device_eviction_time_ms'])} | "
            f"{_fmt_int(row['l3_write_tokens'])} | "
            f"{_fmt_int(row['l3_prefetch_hit_tokens'])} |"
        )
    lines.append("")
    lines.append(
        "Each bandwidth divides the bytes of a window by the duration of that same "
        "window, so numerator and denominator cover the same transfers. Both are "
        "window totals: a load-back prefetch can serve a later round, so a share of "
        "one round's latency is not derived from them. SGLang times the two local "
        "cache movements and the device eviction; for the storage backend it counts "
        "the tokens written and the tokens a prefetch served, with no duration, so "
        "the L3 columns carry token counts and no L3 bandwidth is derived."
    )
    lines.append("")

    if warnings:
        lines.append("## 3. Setups whose p50 varies by more than 5% across repeats")
        lines.append("")
        for warning in warnings:
            lines.append(
                f"- {warning['config_id']} / {warning['case_id']} / round "
                f"{warning['round_index']}, {warning['metric']}: "
                f"min {warning['min']:.3f}, max {warning['max']:.3f}, "
                f"relative spread {warning['relative_spread'] * 100:.1f}%"
            )
        lines.append("")

    lines.append("## 4. Staged metrics that could not be obtained")
    lines.append("")
    for note in unavailable_notes:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def _fmt(value):
    if value is None or value == "unavailable":
        return "unavailable"
    if isinstance(value, (int, float)):
        return f"{value:.3f}"
    return str(value)


def _fmt_int(value):
    if value is None or value == "unavailable":
        return "unavailable"
    if isinstance(value, (int, float)):
        return f"{int(value)}"
    return str(value)


def _reuse_path_lines(rows):
    """State which tier served the reuse, and whether that supports a tier claim.

    The per-request cached_tokens_details is what makes a tier comparison
    meaningful: without it, a row can only say that some cache served the
    request, not which one.
    """
    lines = ["Which tier served the reuse, summed over every row of that tier:", ""]
    by_tier = {}
    for row in rows:
        by_tier.setdefault(row["cache_tier"], []).append(row)

    for tier, group in sorted(by_tier.items()):
        reused = [row for row in group if (row["hit_tokens"] or 0) > 0]
        if not reused:
            lines.append(
                f"- `{tier}`: no request was served from cache, so this tier "
                f"reports cold misses only."
            )
            continue
        device = sum(row["device_hit_tokens"] or 0 for row in group)
        host = sum(row["host_hit_tokens"] or 0 for row in group)
        storage = sum(row["storage_hit_tokens"] or 0 for row in group)
        parts = []
        if device:
            parts.append(f"{device} tokens from the GPU pool (L1)")
        if host:
            parts.append(f"{host} tokens from host DRAM (L2)")
        if storage:
            parts.append(f"{storage} tokens from the storage backend (L3)")
        served = "; ".join(parts) if parts else "no tier reported cached tokens"
        line = f"- `{tier}`: {served}."
        beyond_l1 = [row for row in reused if row["l1_can_hold_working_set"] is False]
        beyond_l2 = [row for row in reused if row["l2_can_hold_working_set"] is False]
        if not beyond_l1:
            line += (
                " Every row with reuse had a working set that fits in the L1 pool, so no "
                "row needed a tier below L1 to serve it."
            )
        elif not beyond_l2:
            line += (
                " Every row whose working set exceeded the L1 pool still fit in the L2 "
                "pool, so no row needed the storage backend to serve it."
            )
        else:
            line += (
                " Some rows had a working set larger than both pools, where only the "
                "storage backend could have served the reuse."
            )
        if tier == "mooncake" and not storage:
            line += (
                " The storage backend served no token in this run, so these rows "
                "measure what having the L3 tier configured costs, not an L3 "
                "benefit. Exercising L3 needs a working set larger than the L2 pool, "
                "which means a smaller host pool (--hicache-size or "
                "--hicache-ratio) or longer inputs (--input-lens)."
            )
        lines.append(line)
    return lines


def _fmt_segment(value):
    if value == "unavailable" or value is None:
        return "unavailable"
    if isinstance(value, dict):
        return f"{value['value']:.3f}"
    return str(value)


def collect_unavailable_notes(e2e_rows):
    """List every unavailable measurement and what it would take to get it."""
    notes = []
    concept_help = {
        "l3_fetch_time": (
            "L3 fetch time: SGLang 0.5.20 counts the tokens a storage prefetch "
            "served (sglang:storage_prefetch_hit_tokens_total) but has no histogram "
            "for the transfer itself. A duration needs timing around the storage "
            "backend's batch read, for example MooncakeStore.batch_get_into"
        ),
        "l3_write_time": (
            "L3 write-back time: SGLang counts the tokens written to the storage "
            "backend (sglang:backuped_tokens_total) without a duration. Timing on "
            "the write_backup_storage path would be needed. "
            "sglang:hicache_backup_duration_seconds is not this measurement: it "
            "times the GPU to host DRAM copy"
        ),
        "prefix_query_time": (
            "Prefix query time: SGLang exposes no latency histogram for the HiCache "
            "radix match; a CUDA event around the scheduler's prefix matching path "
            "would be needed"
        ),
        "l3_attribution": (
            "L3 attribution: no request in this run was served from the storage "
            "backend, so the tier below L2 was never exercised. The working set has "
            "to exceed both the L1 and L2 pools"
        ),
    }
    seen = set()
    for row in e2e_rows:
        if row.get("l2_to_gpu_h2d_time_ms") == "unavailable":
            if "l2_to_gpu_h2d_time" not in seen:
                seen.add("l2_to_gpu_h2d_time")
                notes.append(
                    "Host-to-device copy time: sglang:load_back_duration_seconds is "
                    "absent from this server's metrics, so the L2 to GPU transfer was "
                    "never timed. It is present when the hierarchical cache is on"
                )
        if row.get("device_to_l2_backup_time_ms") == "unavailable":
            if "device_to_l2_backup_time" not in seen:
                seen.add("device_to_l2_backup_time")
                notes.append(
                    "GPU-to-host copy time: sglang:hicache_backup_duration_seconds is "
                    "absent from this server's metrics, which happens when the "
                    "hierarchical cache is off"
                )
    for row in e2e_rows:
        if row.get("storage_backend") and not row.get("storage_hit_tokens"):
            if "l3_attribution" not in seen:
                seen.add("l3_attribution")
                notes.append(concept_help["l3_attribution"])
            break
    for concept in ("l3_fetch_time", "l3_write_time", "prefix_query_time"):
        notes.append(concept_help[concept])
    return notes
