"""Parse Mooncake official store_kv_bench.py stdout/stderr output."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Allow running as script: python benchmark/parse_store_output.py
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from benchmark.metrics import ensure_dir, setup_logger, write_csv

# Metric name -> list of regex patterns (first match wins)
_PATTERNS: Dict[str, list] = {
    "req_s": [
        re.compile(r"req/s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"req_s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*req/s", re.I),
    ],
    "kv_s": [
        re.compile(r"kv/s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"kv_s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*kv/s", re.I),
    ],
    "mib_s": [
        re.compile(r"MiB/s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"mib/s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"mib_s\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*MiB/s", re.I),
    ],
    "lat_mean": [
        re.compile(r"lat_mean\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"latency[_\s]?mean\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"mean\s*lat(?:ency)?\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
    "lat_p50": [
        re.compile(r"lat_p50\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"p50\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"lat(?:ency)?[_\s]?p50\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
    "lat_p95": [
        re.compile(r"lat_p95\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"p95\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"lat(?:ency)?[_\s]?p95\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
    "lat_p99": [
        re.compile(r"lat_p99\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"p99\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
        re.compile(r"lat(?:ency)?[_\s]?p99\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
    "misses": [
        re.compile(r"misses?\s*[:=]\s*([0-9]+)", re.I),
        re.compile(r"cache[_\s]?miss(?:es)?\s*[:=]\s*([0-9]+)", re.I),
    ],
    "verify_failures": [
        re.compile(r"verify_failures?\s*[:=]\s*([0-9]+)", re.I),
        re.compile(r"verify[_\s]?fail(?:ures)?\s*[:=]\s*([0-9]+)", re.I),
    ],
    "errors": [
        re.compile(r"errors?\s*[:=]\s*([0-9]+)", re.I),
        re.compile(r"error_count\s*[:=]\s*([0-9]+)", re.I),
    ],
}

_INT_FIELDS = {"misses", "verify_failures", "errors"}


def _match_first(text: str, patterns: list) -> Optional[str]:
    for pat in patterns:
        m = pat.search(text)
        if m:
            return m.group(1)
    return None


def parse_store_output(text: str) -> Dict[str, Any]:
    """Parse store_kv_bench output text into a metrics dict.

    Missing fields are set to None. Never raises on missing metrics.
    """
    result: Dict[str, Any] = {k: None for k in _PATTERNS}
    if not text:
        return result

    for key, patterns in _PATTERNS.items():
        raw = _match_first(text, patterns)
        if raw is None:
            continue
        try:
            if key in _INT_FIELDS:
                result[key] = int(float(raw))
            else:
                result[key] = float(raw)
        except (TypeError, ValueError):
            result[key] = None
    return result


CSV_FIELDS = [
    "req_s",
    "kv_s",
    "mib_s",
    "lat_mean",
    "lat_p50",
    "lat_p95",
    "lat_p99",
    "misses",
    "verify_failures",
    "errors",
    "log_file",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse Mooncake store_kv_bench.py log output into CSV."
    )
    parser.add_argument("--log-file", required=True, help="Path to store benchmark log")
    parser.add_argument(
        "--output-csv",
        required=True,
        help="Path to write parsed metrics CSV",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logger = setup_logger(level=args.log_level, name="parse_store_output")
    log_path = Path(args.log_file)
    if not log_path.exists():
        logger.error("Log file not found: %s", log_path)
        return 1

    text = log_path.read_text(encoding="utf-8", errors="replace")
    metrics = parse_store_output(text)
    metrics["log_file"] = str(log_path)
    logger.info("Parsed metrics: %s", metrics)

    out = Path(args.output_csv)
    ensure_dir(out.parent)
    write_csv(out, [metrics], CSV_FIELDS, append=False)
    logger.info("Wrote %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
