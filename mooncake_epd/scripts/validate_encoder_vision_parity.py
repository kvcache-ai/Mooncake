#!/usr/bin/env python3
"""Collect exact real-model FeatureBundle checksums from an Encoder service."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.run_vllm_serving_e2e import _load_dataset_requests  # noqa: E402


def _gpu_processes() -> list[str]:
    return subprocess.check_output(
        [
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid,pid,used_memory,process_name",
            "--format=csv,noheader,nounits",
        ],
        text=True,
    ).splitlines()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8330")
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--requests", type=int, default=3)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    entries, skipped = _load_dataset_requests(
        dataset_root=args.dataset_root,
        chat_split="dev-small",
        max_requests=args.requests,
        families=["W0"],
        model=args.model,
        max_input_len=4096,
        request_max_tokens=32,
        skip_oversized=True,
        image_max_pixels=1003520,
    )
    if len(entries) != args.requests:
        raise RuntimeError(f"loaded {len(entries)} requests, expected {args.requests}")

    endpoint = args.endpoint.rstrip("/")
    session = requests.Session()
    session.trust_env = False
    initial_health = session.get(endpoint + "/health", timeout=30).json()
    records: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        payload = copy.deepcopy(entry["request"])
        metadata = dict(payload.get("metadata") or {})
        metadata["workflow_id"] = f"vision-parity-{index}"
        payload["metadata"] = metadata
        started = time.perf_counter()
        response = session.post(endpoint + "/describe", json=payload, timeout=300)
        latency_ms = (time.perf_counter() - started) * 1000.0
        response.raise_for_status()
        body = dict(response.json())
        ticket = str(body["ticket"])
        descriptors = list(body.get("descriptors") or [])
        if not descriptors:
            raise RuntimeError("/describe returned no descriptors")
        for descriptor in descriptors:
            tensor_specs = [descriptor["last_hidden"]]
            tensor_specs.extend(
                dict(item.get("spec") or {})
                for item in descriptor.get("intermediates") or []
            )
            if descriptor.get("grid_thw") is not None:
                tensor_specs.append(descriptor["grid_thw"])
            if not all(str(spec.get("checksum") or "") for spec in tensor_specs):
                raise RuntimeError("Encoder parity validation requires checksum-enabled descriptors")
        cleanup = session.post(
            endpoint + "/discard_direct",
            json={"ticket": ticket},
            timeout=300,
        )
        cleanup.raise_for_status()
        records.append(
            {
                "sequence": index,
                "sample_id": entry["sample"].get("sample_id"),
                "latency_ms": latency_ms,
                "server_encode_ms": body.get("encode_time_ms"),
                "descriptors": descriptors,
                "released_bytes": cleanup.json().get("released_bytes"),
            }
        )

    final_health = session.get(endpoint + "/health", timeout=30).json()
    pending = dict(final_health.get("pending_direct_bundles") or {})
    if int(pending.get("tickets", 0) or 0) != 0:
        raise RuntimeError(f"parity collector leaked direct tickets: {pending}")
    output = {
        "schema_version": "qwen3-vision-parity-v1",
        "mock": False,
        "real_model": True,
        "real_dataset": True,
        "model": args.model,
        "dataset_root": str(Path(args.dataset_root).resolve()),
        "initial_health": initial_health,
        "final_health": final_health,
        "gpu_processes": _gpu_processes(),
        "records": records,
        "dataset_skipped": skipped,
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"encoder_runtime": final_health.get("encoder_runtime"), "records": len(records)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
