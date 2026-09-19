#!/usr/bin/env python3
"""Build a reproducible real-image split for patch-aware Qwen3 scheduling.

The source rows and images remain unchanged. Selection uses patch counts from
an independently generated real-processor manifest, then emits large-first
bursts so smaller work can become ready behind expensive FIFO work. This makes
real reorder decisions observable without synthetic images or model mocks.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _select_rows(
    source_rows: list[dict[str, Any]],
    patch_rows: list[dict[str, Any]],
    *,
    small_max: int,
    large_min: int,
    small_count: int,
    large_count: int,
    burst_size: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if min(small_count, large_count) < 1:
        raise ValueError("small/large counts must be positive")
    if burst_size < 2:
        raise ValueError("burst size must be at least two")
    if small_max >= large_min:
        raise ValueError("small_max must be lower than large_min")

    source_by_id = {str(row.get("sample_id") or ""): row for row in source_rows}
    eligible_small = sorted(
        (
            row
            for row in patch_rows
            if int(row.get("patches", 0) or 0) <= small_max
            and str(row.get("sample_id") or "") in source_by_id
        ),
        key=lambda row: (int(row["patches"]), int(row.get("index", 0))),
    )
    eligible_large = sorted(
        (
            row
            for row in patch_rows
            if int(row.get("patches", 0) or 0) >= large_min
            and str(row.get("sample_id") or "") in source_by_id
        ),
        key=lambda row: (-int(row["patches"]), int(row.get("index", 0))),
    )
    if len(eligible_small) < small_count or len(eligible_large) < large_count:
        raise ValueError(
            "insufficient patch-mix candidates: "
            f"small={len(eligible_small)}/{small_count} "
            f"large={len(eligible_large)}/{large_count}"
        )
    small = eligible_small[:small_count]
    large = eligible_large[:large_count]
    selected_ids = [str(row["sample_id"]) for row in small + large]
    if len(set(selected_ids)) != len(selected_ids):
        raise ValueError("small and large selections overlap")

    large_per_burst = max(1, burst_size // 2)
    small_per_burst = max(1, burst_size - large_per_burst)
    ordered_patch_rows: list[dict[str, Any]] = []
    while large or small:
        ordered_patch_rows.extend(large[:large_per_burst])
        del large[:large_per_burst]
        ordered_patch_rows.extend(small[:small_per_burst])
        del small[:small_per_burst]

    output_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    for sequence, patch_row in enumerate(ordered_patch_rows):
        sample_id = str(patch_row["sample_id"])
        row = dict(source_by_id[sample_id])
        extensions = dict(row.get("extensions") or {})
        patches = int(patch_row["patches"])
        bucket = "small" if patches <= small_max else "large"
        extensions.update(
            {
                "perf310_patch_mix_sequence": sequence,
                "perf310_visual_patches": patches,
                "perf310_patch_bucket": bucket,
            }
        )
        row["extensions"] = extensions
        output_rows.append(row)
        manifest_rows.append(
            {
                "sequence": sequence,
                "sample_id": sample_id,
                "patches": patches,
                "patch_bucket": bucket,
                "original_size": list(patch_row.get("original_size") or []),
                "image_grid_thw": list(patch_row.get("image_grid_thw") or []),
            }
        )
    return output_rows, manifest_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-split", required=True)
    parser.add_argument("--patch-manifest", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output", required=True)
    parser.add_argument("--small-max-patches", type=int, default=384)
    parser.add_argument("--large-min-patches", type=int, default=14904)
    parser.add_argument("--small-count", type=int, default=8)
    parser.add_argument("--large-count", type=int, default=8)
    parser.add_argument("--burst-size", type=int, default=8)
    args = parser.parse_args()

    source = Path(args.source_split).resolve()
    patch_manifest = Path(args.patch_manifest).resolve()
    output = Path(args.output).resolve()
    metadata_output = Path(args.metadata_output).resolve()
    source_rows = _load_jsonl(source)
    patch_payload = json.loads(patch_manifest.read_text(encoding="utf-8"))
    patch_rows = list(patch_payload.get("rows") or [])
    output_rows, manifest_rows = _select_rows(
        source_rows,
        patch_rows,
        small_max=args.small_max_patches,
        large_min=args.large_min_patches,
        small_count=args.small_count,
        large_count=args.large_count,
        burst_size=args.burst_size,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            for row in output_rows
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema_version": "perf310-qwen3-real-patch-mix-split-v1",
        "source_split": str(source),
        "patch_manifest": str(patch_manifest),
        "output_split": str(output),
        "mock": False,
        "real_images": True,
        "selection": {
            "small_max_patches": args.small_max_patches,
            "large_min_patches": args.large_min_patches,
            "small_count": args.small_count,
            "large_count": args.large_count,
            "burst_size": args.burst_size,
            "large_first_within_burst": True,
        },
        "rows": manifest_rows,
    }
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
