#!/usr/bin/env python3
"""Issue one real, worker-targeted multimodal request for epoch-fence tests.

This utility reuses the production benchmark's dataset loading, stable UUID,
compact-reference, and streaming measurement paths.  It neither instantiates a
model nor substitutes any transport component; the configured OpenAI endpoint
must be a live EPD proxy.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.benchmark_openai_epd_serving import (  # noqa: E402
    _assign_stable_mm_uuids,
    _compact_mm_uuid_entry,
    _target_decode_worker_entry,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _execute_dataset_request,
    _load_dataset_requests,
)


def _snapshot(url: str) -> dict[str, Any]:
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    finally:
        session.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:39572/v1/chat/completions",
    )
    parser.add_argument(
        "--metrics-url",
        default="http://127.0.0.1:39572/metrics",
    )
    parser.add_argument(
        "--model",
        default="/data01/LWX/Qwen3-VL-8B-Instruct",
    )
    parser.add_argument(
        "--dataset-root",
        default="/data/songbinbin/Proj/Proj_LWX/mooncake_test_dataset",
    )
    parser.add_argument("--chat-split", default="dev-small")
    parser.add_argument("--family", default="W0")
    parser.add_argument("--worker", required=True)
    parser.add_argument("--mode", choices=("full", "compact"), required=True)
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--image-max-pixels", type=int, default=1_003_520)
    parser.add_argument(
        "--stream-metrics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use SSE timing metrics; disable for lifecycle tests that only need a closed response.",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    entries, skipped = _load_dataset_requests(
        dataset_root=args.dataset_root,
        chat_split=args.chat_split,
        max_requests=1,
        families=[args.family],
        model=args.model,
        max_input_len=args.max_input_len,
        request_max_tokens=args.max_tokens,
        skip_oversized=True,
        image_max_pixels=args.image_max_pixels,
    )
    if len(entries) != 1:
        raise RuntimeError(
            f"expected one admissible real request, got {len(entries)}; "
            f"skipped={skipped[:2]}"
        )
    assigned = _assign_stable_mm_uuids(entries)
    if assigned < 1:
        raise RuntimeError("selected real request has no multimodal item")

    entry = entries[0]
    compact_items = 0
    if args.mode == "compact":
        entry, compact_items = _compact_mm_uuid_entry(entry)
        if compact_items < 1:
            raise RuntimeError("compact request did not replace any media payload")
    entry = _target_decode_worker_entry(entry, args.worker)

    metrics_before = _snapshot(args.metrics_url)
    started_at = time.time()
    result = _execute_dataset_request(
        proxy_url=args.endpoint,
        entry=entry,
        index=0,
        request_timeout=args.request_timeout,
        stream_metrics=bool(args.stream_metrics),
    )
    metrics_after = _snapshot(args.metrics_url)
    artifact = {
        "schema_version": 1,
        "started_at_unix_s": started_at,
        "endpoint": args.endpoint,
        "metrics_url": args.metrics_url,
        "model": args.model,
        "dataset_root": str(Path(args.dataset_root).resolve()),
        "chat_split": args.chat_split,
        "family": args.family,
        "worker": args.worker,
        "mode": args.mode,
        "assigned_mm_uuids": assigned,
        "compact_items": compact_items,
        "stream_metrics": bool(args.stream_metrics),
        "request_result": result,
        "decode_mm_hash_cache_before": metrics_before.get(
            "decode_mm_hash_cache", {}
        ),
        "decode_mm_hash_cache_after": metrics_after.get(
            "decode_mm_hash_cache", {}
        ),
        "worker_health_after": metrics_after.get("worker_health", {}),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(artifact, ensure_ascii=False, allow_nan=False))
    status_code = result.get("status_code")
    return 0 if isinstance(status_code, int) else 2


if __name__ == "__main__":
    raise SystemExit(main())
