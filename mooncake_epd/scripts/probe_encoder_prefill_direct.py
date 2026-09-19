#!/usr/bin/env python3
"""Exercise one real Encoder→Prefill direct FeatureBundle transaction.

This probe deliberately stops at the E→P data-plane boundary so Prefill or
Decode KV-transfer lifecycle faults cannot be mistaken for an Encoder direct
engine failure. It uses the production dataset loader, the live model-serving
Encoder, the embedded Prefill allocator, and the real Mooncake backend.
"""

from __future__ import annotations

import argparse
import hashlib
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
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _load_dataset_requests,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--encoder-url", default="http://127.0.0.1:8330")
    parser.add_argument("--prefill-url", default="http://127.0.0.1:8100")
    parser.add_argument("--model", default="/data01/LWX/Qwen3-VL-8B-Instruct")
    parser.add_argument(
        "--dataset-root",
        default="/data/songbinbin/Proj/Proj_LWX/mooncake_test_dataset",
    )
    parser.add_argument("--chat-split", default="dev-small")
    parser.add_argument("--family", default="W0")
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--image-max-pixels", type=int, default=1_003_520)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _timed_post(
    session: requests.Session,
    url: str,
    payload: dict[str, Any],
    timeout: float,
) -> tuple[requests.Response, float]:
    started = time.perf_counter()
    response = session.post(url, json=payload, timeout=max(1.0, timeout))
    return response, (time.perf_counter() - started) * 1000.0


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
            f"expected one admissible real request, got {len(entries)}; skipped={skipped[:2]}"
        )
    assigned = _assign_stable_mm_uuids(entries)
    if assigned < 1:
        raise RuntimeError("selected real request has no multimodal item")

    request_payload = dict(entries[0]["request"])
    metadata = dict(request_payload.get("metadata") or {})
    metadata["mooncake_epd_target_worker_id"] = "prefill-0"
    request_payload["metadata"] = metadata
    artifact: dict[str, Any] = {
        "schema_version": 1,
        "started_at_unix_s": time.time(),
        "model": args.model,
        "dataset_root": str(Path(args.dataset_root).resolve()),
        "chat_split": args.chat_split,
        "family": args.family,
        "assigned_mm_uuids": assigned,
        "request_payload_bytes": len(
            json.dumps(request_payload, ensure_ascii=False).encode("utf-8")
        ),
        "transport_fallback_used": False,
    }

    session = requests.Session()
    session.trust_env = False
    allocated_feature_ids: list[str] = []
    try:
        described, describe_ms = _timed_post(
            session,
            f"{args.encoder_url.rstrip('/')}/describe",
            request_payload,
            args.request_timeout,
        )
        artifact["describe"] = {
            "status_code": described.status_code,
            "elapsed_ms": describe_ms,
            "response_head": described.text[:1000],
        }
        described.raise_for_status()
        described_payload = described.json()
        descriptors = described_payload.get("descriptors")
        ticket = str(described_payload.get("ticket") or "")
        if not ticket or not isinstance(descriptors, list) or not descriptors:
            raise RuntimeError("Encoder describe returned no descriptors/ticket")

        allocated, allocate_ms = _timed_post(
            session,
            f"{args.prefill_url.rstrip('/')}/allocate",
            {
                "descriptors": descriptors,
                "target_worker_id": "prefill-0",
                "zero_fill": False,
            },
            args.request_timeout,
        )
        artifact["allocate"] = {
            "status_code": allocated.status_code,
            "elapsed_ms": allocate_ms,
            "response_head": allocated.text[:1000],
        }
        allocated.raise_for_status()
        targets = allocated.json().get("targets")
        if not isinstance(targets, list) or len(targets) != len(descriptors):
            raise RuntimeError("Prefill allocation returned invalid targets")
        allocated_feature_ids = [
            str(target.get("feature_id") or "")
            for target in targets
            if isinstance(target, dict)
        ]
        remote_incarnations = sorted(
            {
                str(target.get("remote_incarnation") or "")
                for target in targets
                if isinstance(target, dict) and target.get("remote_incarnation")
            }
        )
        artifact["prefill_target"] = {
            "count": len(targets),
            "remote_sessions": sorted(
                {
                    str(target.get("remote_session") or "")
                    for target in targets
                    if isinstance(target, dict)
                }
            ),
            "remote_incarnation_fingerprints": [
                hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
                for token in remote_incarnations
            ],
        }

        published, publish_ms = _timed_post(
            session,
            f"{args.encoder_url.rstrip('/')}/publish_direct",
            {
                "ticket": ticket,
                "metadata": metadata,
                "mooncake_epd_direct_feature_targets": targets,
            },
            args.request_timeout,
        )
        artifact["publish"] = {
            "status_code": published.status_code,
            "elapsed_ms": publish_ms,
            "response_head": published.text[:1000],
        }
        published.raise_for_status()
        handles = published.json().get("handles")
        if not isinstance(handles, list) or not handles:
            raise RuntimeError("Encoder publish returned no handles")
        handle_metadata = [dict(handle.get("metadata") or {}) for handle in handles]
        if any(
            not str(handle.get("uri") or "").startswith("epd-direct://")
            for handle in handles
        ):
            raise RuntimeError("Encoder publish returned a non-direct handle")
        artifact["result"] = {
            "status_code": 200,
            "handle_count": len(handles),
            "backend": sorted({str(item.get("backend") or "") for item in handle_metadata}),
            "direct_backend": sorted(
                {str(item.get("direct_backend") or "") for item in handle_metadata}
            ),
            "direct_engine_generations": sorted(
                {int(item.get("direct_engine_generation", 0)) for item in handle_metadata}
            ),
            "direct_engine_recovery": sorted(
                {str(item.get("direct_engine_recovery") or "none") for item in handle_metadata}
            ),
            "direct_engine_recovered": any(
                bool(item.get("direct_engine_recovered")) for item in handle_metadata
            ),
            "direct_bytes": sum(int(item.get("direct_bytes", 0)) for item in handle_metadata),
            "total_elapsed_ms": describe_ms + allocate_ms + publish_ms,
        }
        return_code = 0
    except Exception as exc:
        artifact["error"] = f"{type(exc).__name__}: {exc}"
        artifact.setdefault("result", {})["status_code"] = None
        return_code = 2
    finally:
        if allocated_feature_ids:
            try:
                released, release_ms = _timed_post(
                    session,
                    f"{args.prefill_url.rstrip('/')}/release",
                    {"feature_ids": allocated_feature_ids},
                    min(args.request_timeout, 30.0),
                )
                artifact["release"] = {
                    "status_code": released.status_code,
                    "elapsed_ms": release_ms,
                }
            except Exception as exc:
                artifact["release"] = {"error": f"{type(exc).__name__}: {exc}"}
        session.close()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(artifact, ensure_ascii=False, allow_nan=False))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
