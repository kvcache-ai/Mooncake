#!/usr/bin/env python3
"""Stress a real Qwen3-VL processor under shared-instance thread concurrency.

This is a correctness benchmark, not a mock and not an Encoder throughput
claim.  It compares every concurrent processor output byte-for-byte with a
sequential baseline from the same real model and dataset, records the exact
Transformers/tokenizers/Pillow versions, and proves calls actually overlapped.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import sys
import threading
import time
from typing import Any, Mapping

import numpy as np
from PIL import Image, __version__ as pillow_version
import torch
import tokenizers
import transformers
from transformers import AutoProcessor

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.epd_encoder_service import (  # noqa: E402
    EncoderServiceConfig,
    _decode_image_payload,
    _image_url_from_item,
    _iter_mm_image_items,
    _load_url_bytes_sync,
    _processor_inputs,
    _prompt_for_processor,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _load_dataset_requests,
)


def _digest(value: Any) -> str:
    digest = hashlib.sha256()

    def update(item: Any) -> None:
        if isinstance(item, torch.Tensor):
            tensor = item.detach().contiguous().cpu()
            digest.update(b"tensor\0")
            digest.update(str(tensor.dtype).encode())
            digest.update(str(tuple(int(dim) for dim in tensor.shape)).encode())
            digest.update(tensor.view(torch.uint8).numpy().tobytes())
            return
        if isinstance(item, np.ndarray):
            array = np.ascontiguousarray(item)
            digest.update(b"ndarray\0")
            digest.update(str(array.dtype).encode())
            digest.update(str(tuple(int(dim) for dim in array.shape)).encode())
            digest.update(array.tobytes())
            return
        if isinstance(item, Mapping):
            digest.update(b"mapping\0")
            for key in sorted(item, key=str):
                update(str(key))
                update(item[key])
            return
        if isinstance(item, (list, tuple)):
            digest.update(f"sequence:{len(item)}\0".encode())
            for child in item:
                update(child)
            return
        digest.update(json.dumps(item, sort_keys=True, default=str).encode())
        digest.update(b"\0")

    update(value)
    return digest.hexdigest()


def _load_images_and_prompts(args) -> list[tuple[Image.Image, str, str]]:
    entries, skipped = _load_dataset_requests(
        dataset_root=args.dataset_root,
        chat_split=args.dataset_chat_split,
        max_requests=args.samples,
        families=[args.dataset_family],
        model=args.model,
        max_input_len=args.max_input_len,
        request_max_tokens=1,
        skip_oversized=True,
        image_max_pixels=args.image_max_pixels,
    )
    if len(entries) < args.samples:
        raise RuntimeError(
            f"dataset yielded {len(entries)} samples, need {args.samples}; skipped={skipped}"
        )
    config = EncoderServiceConfig(
        model=args.model,
        local_image_root=args.dataset_root,
        max_image_bytes=args.max_image_bytes,
    )
    loaded = []
    for entry in entries[: args.samples]:
        request = dict(entry["request"])
        items = list(_iter_mm_image_items(request))
        if len(items) != 1:
            raise RuntimeError(
                "processor concurrency benchmark requires exactly one image per sample"
            )
        url = _image_url_from_item(items[0])
        if not url:
            raise RuntimeError("dataset image item has no URL")
        payload, _content_type = _load_url_bytes_sync(
            config,
            url,
            max_bytes=args.max_image_bytes,
        )
        loaded.append(
            (
                _decode_image_payload(payload),
                _prompt_for_processor(request),
                str(entry["sample"].get("sample_id") or len(loaded)),
            )
        )
    return loaded


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-chat-split", default="dev-small")
    parser.add_argument("--dataset-family", default="W0")
    parser.add_argument("--samples", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--image-max-pixels", type=int, default=1003520)
    parser.add_argument("--max-image-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if min(args.samples, args.repeats, args.workers) < 1:
        parser.error("samples/repeats/workers must be positive")

    processor = AutoProcessor.from_pretrained(args.model, local_files_only=True)
    inputs = _load_images_and_prompts(args)
    # Warm lazy tokenizer/image-processor state before either measured arm.
    for image, prompt, _sample_id in inputs:
        _processor_inputs(processor, image, prompt)

    baseline: dict[int, str] = {}
    sequential_started = time.perf_counter()
    for repeat in range(args.repeats):
        for index, (image, prompt, _sample_id) in enumerate(inputs):
            output = _processor_inputs(processor, image, prompt)
            current = _digest(output)
            previous = baseline.setdefault(index, current)
            if previous != current:
                raise RuntimeError(
                    f"sequential processor output changed sample={index} repeat={repeat}"
                )
    sequential_s = time.perf_counter() - sequential_started

    barrier = threading.Barrier(args.workers)
    active = 0
    peak_active = 0
    active_lock = threading.Lock()

    def run_one(sequence: int) -> dict[str, Any]:
        nonlocal active, peak_active
        sample_index = sequence % len(inputs)
        image, prompt, sample_id = inputs[sample_index]
        barrier.wait(timeout=60.0)
        with active_lock:
            active += 1
            peak_active = max(peak_active, active)
        started = time.perf_counter()
        try:
            output = _processor_inputs(processor, image, prompt)
            output_digest = _digest(output)
            return {
                "sequence": sequence,
                "sample_index": sample_index,
                "sample_id": sample_id,
                "digest": output_digest,
                "expected_digest": baseline[sample_index],
                "exact": output_digest == baseline[sample_index],
                "latency_ms": (time.perf_counter() - started) * 1000.0,
            }
        finally:
            with active_lock:
                active -= 1

    total = args.repeats * args.workers
    concurrent_started = time.perf_counter()
    records = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(run_one, sequence) for sequence in range(total)]
        for future in as_completed(futures):
            try:
                records.append(future.result())
            except Exception as exc:
                failures.append(f"{type(exc).__name__}: {exc}")
    concurrent_s = time.perf_counter() - concurrent_started
    records.sort(key=lambda item: int(item["sequence"]))
    mismatches = [record for record in records if not record["exact"]]
    passed = not failures and not mismatches and peak_active >= 2
    payload = {
        "schema_version": "qwen3-real-processor-shared-concurrency-v1",
        "model": str(Path(args.model).resolve()),
        "dataset_root": str(Path(args.dataset_root).resolve()),
        "dataset_chat_split": args.dataset_chat_split,
        "dataset_family": args.dataset_family,
        "mock": False,
        "real_processor": True,
        "versions": {
            "transformers": transformers.__version__,
            "tokenizers": tokenizers.__version__,
            "pillow": pillow_version,
            "torch": torch.__version__,
            "processor_class": f"{type(processor).__module__}.{type(processor).__name__}",
        },
        "config": {
            "samples": args.samples,
            "repeats": args.repeats,
            "workers": args.workers,
            "concurrent_calls": total,
        },
        "sequential": {
            "calls": args.samples * args.repeats,
            "elapsed_s": sequential_s,
            "throughput_calls_s": args.samples * args.repeats / sequential_s,
            "baseline_digests": baseline,
        },
        "concurrent": {
            "calls": total,
            "elapsed_s": concurrent_s,
            "throughput_calls_s": total / concurrent_s,
            "peak_active": peak_active,
            "failures": failures,
            "mismatches": len(mismatches),
            "records": records,
        },
        "validation": {
            "passed": passed,
            "requirements": {
                "no_exceptions": not failures,
                "exact_output_parity": not mismatches,
                "actual_overlap": peak_active >= 2,
            },
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"sequential": payload["sequential"], "concurrent": {
        key: value for key, value in payload["concurrent"].items() if key != "records"
    }, "validation": payload["validation"]}, indent=2, sort_keys=True))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
