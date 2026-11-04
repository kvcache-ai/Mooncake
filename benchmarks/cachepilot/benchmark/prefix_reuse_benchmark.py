"""Simulate long-context Prefix Cache reuse and TTFT reduction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from benchmark.metrics import (
    ensure_dir,
    parse_float_list,
    parse_int_list,
    setup_logger,
    write_csv,
)

PROJECT_ROOT = _ROOT

CSV_FIELDS = [
    "prefix_tokens",
    "cache_hit_ratio",
    "prefix_kv_mb",
    "no_cache_ttft_ms",
    "prefix_cache_ttft_ms",
    "store_get_latency_ms",
    "ttft_reduction_ms",
    "ttft_reduction_pct",
    "num_requests",
    "simulated_prefill_ms_per_token",
]


def compute_row(
    prefix_tokens: int,
    hit_ratio: float,
    *,
    prefill_ms_per_token: float,
    decode_first_token_ms: float,
    store_get_base_ms: float,
    store_get_ms_per_mb: float,
    kv_bytes_per_token: int,
    num_requests: int,
) -> dict:
    prefix_kv_mb = prefix_tokens * kv_bytes_per_token / 1024.0 / 1024.0
    no_cache_ttft = prefix_tokens * prefill_ms_per_token + decode_first_token_ms
    cache_get_latency = store_get_base_ms + prefix_kv_mb * store_get_ms_per_mb
    prefix_cache_ttft = (
        hit_ratio * (cache_get_latency + decode_first_token_ms)
        + (1.0 - hit_ratio) * no_cache_ttft
    )
    ttft_reduction_ms = no_cache_ttft - prefix_cache_ttft
    ttft_reduction_pct = (
        (ttft_reduction_ms / no_cache_ttft * 100.0) if no_cache_ttft > 0 else 0.0
    )
    return {
        "prefix_tokens": prefix_tokens,
        "cache_hit_ratio": hit_ratio,
        "prefix_kv_mb": round(prefix_kv_mb, 6),
        "no_cache_ttft_ms": round(no_cache_ttft, 6),
        "prefix_cache_ttft_ms": round(prefix_cache_ttft, 6),
        "store_get_latency_ms": round(cache_get_latency, 6),
        "ttft_reduction_ms": round(ttft_reduction_ms, 6),
        "ttft_reduction_pct": round(ttft_reduction_pct, 6),
        "num_requests": num_requests,
        "simulated_prefill_ms_per_token": prefill_ms_per_token,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate Prefix Cache reuse impact on TTFT."
    )
    parser.add_argument(
        "--prefix-tokens",
        default="512,1024,2048,4096,8192",
        help="Comma-separated prefix token lengths",
    )
    parser.add_argument(
        "--cache-hit-ratios",
        default="0,0.25,0.5,0.75,0.9",
        help="Comma-separated cache hit ratios",
    )
    parser.add_argument("--num-requests", type=int, default=100)
    parser.add_argument("--prefill-ms-per-token", type=float, default=0.08)
    parser.add_argument("--decode-first-token-ms", type=float, default=20.0)
    parser.add_argument("--store-get-base-ms", type=float, default=2.0)
    parser.add_argument("--store-get-ms-per-mb", type=float, default=0.15)
    parser.add_argument("--kv-bytes-per-token", type=int, default=524288)
    parser.add_argument(
        "--output-csv",
        default="results/csv/prefix_reuse.csv",
    )
    parser.add_argument(
        "--log-file",
        default="results/logs/prefix_reuse.log",
    )
    args = parser.parse_args()

    log_file = Path(args.log_file)
    if not log_file.is_absolute():
        log_file = PROJECT_ROOT / log_file
    ensure_dir(log_file.parent)
    logger = setup_logger(log_file, name="prefix_reuse")

    prefix_tokens = parse_int_list(args.prefix_tokens)
    hit_ratios = parse_float_list(args.cache_hit_ratios)
    if not prefix_tokens or not hit_ratios:
        logger.error("prefix-tokens and cache-hit-ratios must be non-empty")
        return 1

    rows = []
    for pt in prefix_tokens:
        for hr in hit_ratios:
            row = compute_row(
                pt,
                hr,
                prefill_ms_per_token=args.prefill_ms_per_token,
                decode_first_token_ms=args.decode_first_token_ms,
                store_get_base_ms=args.store_get_base_ms,
                store_get_ms_per_mb=args.store_get_ms_per_mb,
                kv_bytes_per_token=args.kv_bytes_per_token,
                num_requests=args.num_requests,
            )
            rows.append(row)
            logger.info(
                "prefix=%s hit=%.2f no_cache=%.2fms cache=%.2fms reduction=%.2f%%",
                pt,
                hr,
                row["no_cache_ttft_ms"],
                row["prefix_cache_ttft_ms"],
                row["ttft_reduction_pct"],
            )

    output_csv = Path(args.output_csv)
    if not output_csv.is_absolute():
        output_csv = PROJECT_ROOT / output_csv
    write_csv(output_csv, rows, CSV_FIELDS, append=False)
    logger.info("Wrote %d rows to %s", len(rows), output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
