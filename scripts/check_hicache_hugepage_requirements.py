#!/usr/bin/env python3
"""Estimate HugeTLB capacity for Mooncake HiCache launches."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys


_UNIT_MAP = {
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}


@dataclass(frozen=True)
class BudgetSummary:
    baseline_per_rank_bytes: int
    baseline_total_bytes: int
    clean_arena_per_rank_bytes: int
    clean_arena_total_bytes: int
    available_bytes: int | None
    status: str
    exit_code: int


def parse_size(value: str) -> int:
    normalized = value.strip().lower().replace("_", "")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([a-z]*)", normalized)
    if match is None:
        raise ValueError(f"invalid size: {value!r}")

    number = float(match.group(1))
    unit = match.group(2) or "b"
    if unit not in _UNIT_MAP:
        raise ValueError(f"unsupported size unit in {value!r}")

    return int(number * _UNIT_MAP[unit])


def format_size(num_bytes: int) -> str:
    for unit, scale in (("TiB", 1024**4), ("GiB", 1024**3), ("MiB", 1024**2)):
        if num_bytes >= scale:
            return f"{num_bytes / scale:.2f} {unit}"
    if num_bytes >= 1024:
        return f"{num_bytes / 1024:.2f} KiB"
    return f"{num_bytes} B"


def read_available_hugetlb(page_size_bytes: int) -> int | None:
    if page_size_bytes % 1024 != 0:
        return None

    page_size_kib = page_size_bytes // 1024
    path = Path(f"/sys/kernel/mm/hugepages/hugepages-{page_size_kib}kB/nr_hugepages")
    try:
        nr_hugepages = int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, PermissionError, ValueError, OSError):
        return None
    return nr_hugepages * page_size_bytes


def evaluate_budget(
    *,
    tp_size: int,
    hicache_size_bytes: int,
    global_segment_size_bytes: int,
    arena_pool_size_bytes: int,
    available_bytes: int | None,
) -> BudgetSummary:
    if tp_size <= 0:
        raise ValueError("tp_size must be positive")

    baseline_per_rank = hicache_size_bytes + global_segment_size_bytes
    baseline_total = baseline_per_rank * tp_size
    clean_arena_per_rank = baseline_per_rank + arena_pool_size_bytes
    clean_arena_total = clean_arena_per_rank * tp_size

    if available_bytes is None:
        status = "planning_only"
        exit_code = 0
    elif available_bytes < baseline_total:
        status = "insufficient_for_baseline"
        exit_code = 2
    elif available_bytes < clean_arena_total:
        status = "baseline_fits_arena_may_fallback"
        exit_code = 1
    else:
        status = "clean_arena_budget_available"
        exit_code = 0

    return BudgetSummary(
        baseline_per_rank_bytes=baseline_per_rank,
        baseline_total_bytes=baseline_total,
        clean_arena_per_rank_bytes=clean_arena_per_rank,
        clean_arena_total_bytes=clean_arena_total,
        available_bytes=available_bytes,
        status=status,
        exit_code=exit_code,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Estimate HugeTLB capacity needed for a Mooncake HiCache launch. "
            "The tool reports both a baseline floor and a cleaner arena-backed "
            "target derived from benchmark bring-up experience."
        )
    )
    parser.add_argument("--tp-size", type=int, required=True)
    parser.add_argument("--hicache-size", required=True)
    parser.add_argument("--global-segment-size", required=True)
    parser.add_argument("--arena-pool-size", default="0")
    parser.add_argument("--hugepage-size", default="2mb")
    parser.add_argument(
        "--available-hugetlb",
        help=(
            "Override detected HugeTLB capacity with an explicit size such as "
            "'96gb'. If omitted, the tool tries /sys/kernel/mm/hugepages."
        ),
    )
    return parser


def print_summary(args: argparse.Namespace, summary: BudgetSummary) -> None:
    hicache_size = parse_size(args.hicache_size)
    global_segment_size = parse_size(args.global_segment_size)
    arena_pool_size = parse_size(args.arena_pool_size)
    hugepage_size = parse_size(args.hugepage_size)

    print("Mooncake HiCache HugeTLB sizing summary")
    print("")
    print(f"tp-size:                {args.tp_size}")
    print(f"hicache-size:           {args.hicache_size} ({format_size(hicache_size)})")
    print(
        "global-segment-size:    "
        f"{args.global_segment_size} ({format_size(global_segment_size)})"
    )
    print(
        f"arena-pool-size:        {args.arena_pool_size} "
        f"({format_size(arena_pool_size)})"
    )
    print(
        f"hugepage-size:          {args.hugepage_size} "
        f"({format_size(hugepage_size)})"
    )
    if summary.available_bytes is None:
        print("available HugeTLB:      unavailable (planning-only mode)")
    else:
        print("available HugeTLB:      " f"{format_size(summary.available_bytes)}")
    print("")
    print("baseline floor / rank:  " f"{format_size(summary.baseline_per_rank_bytes)}")
    print("baseline floor / total: " f"{format_size(summary.baseline_total_bytes)}")
    print(
        "clean arena / rank:     " f"{format_size(summary.clean_arena_per_rank_bytes)}"
    )
    print("clean arena / total:    " f"{format_size(summary.clean_arena_total_bytes)}")
    print("")

    if summary.status == "planning_only":
        print("Status: planning only")
        print(
            "Meaning: baseline and clean-arena targets were computed, but "
            "host HugeTLB capacity was not detected."
        )
    elif summary.status == "insufficient_for_baseline":
        print("Status: insufficient for baseline")
        print(
            "Meaning: the current HugeTLB pool is below the baseline floor. "
            "Expect startup or allocation failures."
        )
    elif summary.status == "baseline_fits_arena_may_fallback":
        print("Status: baseline fits, arena may fall back")
        print(
            "Meaning: baseline should fit, but the cleaner fully arena-backed "
            "target is above the current HugeTLB pool. Arena allocations may "
            "partially or fully fall back to regular pages."
        )
    else:
        print("Status: clean arena budget available")
        print(
            "Meaning: the current HugeTLB pool meets the cleaner target for "
            "baseline plus arena-backed bring-up."
        )

    print("")
    print(
        "Note: the baseline floor is a conservative bring-up estimate "
        "(hicache + global segment per rank). The clean-arena target adds the "
        "arena pool per rank to show when arena-backed runs are less likely to "
        "spill onto the regular-page fallback path."
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        available_bytes = (
            parse_size(args.available_hugetlb)
            if args.available_hugetlb
            else read_available_hugetlb(parse_size(args.hugepage_size))
        )
        summary = evaluate_budget(
            tp_size=args.tp_size,
            hicache_size_bytes=parse_size(args.hicache_size),
            global_segment_size_bytes=parse_size(args.global_segment_size),
            arena_pool_size_bytes=parse_size(args.arena_pool_size),
            available_bytes=available_bytes,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 64

    print_summary(args, summary)
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
