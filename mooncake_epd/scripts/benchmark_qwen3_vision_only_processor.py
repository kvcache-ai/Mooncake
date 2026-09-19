#!/usr/bin/env python3
"""Benchmark real Qwen3 E-stage vision-only preprocessing against control.

The E stage consumes only ``pixel_values`` and ``image_grid_thw``. This
benchmark compares the existing full multimodal chat-template processor path
with the stage-specialized image processor on real dataset images. It uses an
alternating paired order, byte-exact tensor digests, and no mocks.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import sys
import time
from typing import Any, Callable, Dict

from PIL import __version__ as pillow_version
import tokenizers
import torch
import transformers
from transformers import AutoProcessor

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.benchmark_qwen3_processor_concurrency import (  # noqa: E402
    _digest,
    _load_images_and_prompts,
)
from mooncake_epd.scripts.epd_encoder_service import (  # noqa: E402
    _processor_inputs,
    _processor_vision_inputs,
)


def _vision_payload(output: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
    return {
        "pixel_values": output["pixel_values"],
        "image_grid_thw": output["image_grid_thw"],
    }


def _percent(treatment: float, control: float) -> float:
    return (float(treatment) / float(control) - 1.0) * 100.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-chat-split", default="dev-small")
    parser.add_argument("--dataset-family", default="W0")
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--image-max-pixels", type=int, default=16_777_216)
    parser.add_argument("--max-image-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if min(args.samples, args.repeats) < 1:
        parser.error("samples/repeats must be positive")

    processor = AutoProcessor.from_pretrained(
        args.model,
        local_files_only=True,
    )
    inputs = _load_images_and_prompts(args)

    def control() -> list[Dict[str, torch.Tensor]]:
        return [
            _processor_inputs(processor, image, prompt)
            for image, prompt, _sample_id in inputs
        ]

    def treatment() -> list[Dict[str, torch.Tensor]]:
        return [
            _processor_vision_inputs(processor, image)
            for image, _prompt, _sample_id in inputs
        ]

    methods: Dict[str, Callable[[], list[Dict[str, torch.Tensor]]]] = {
        "control_full_chat_template": control,
        "treatment_vision_only": treatment,
    }
    # Warm lazy tokenizer and image-processor state before measurement.
    for method in methods.values():
        method()

    records = []
    timings: Dict[str, list[float]] = {name: [] for name in methods}
    orders = []
    for repeat in range(args.repeats):
        order = list(methods)
        if repeat % 2:
            order.reverse()
        orders.append(order)
        for name in order:
            started = time.perf_counter()
            methods[name]()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            timings[name].append(elapsed_ms)
            records.append(
                {
                    "repeat": repeat + 1,
                    "order": order,
                    "method": name,
                    "elapsed_ms": elapsed_ms,
                    "throughput_images_s": args.samples / (elapsed_ms / 1000.0),
                }
            )

    control_outputs = control()
    treatment_outputs = treatment()
    parity = []
    for index, ((_, _, sample_id), control_output, treatment_output) in enumerate(
        zip(inputs, control_outputs, treatment_outputs)
    ):
        control_payload = _vision_payload(control_output)
        treatment_payload = _vision_payload(treatment_output)
        control_digest = _digest(control_payload)
        treatment_digest = _digest(treatment_payload)
        parity.append(
            {
                "index": index,
                "sample_id": sample_id,
                "control_digest": control_digest,
                "treatment_digest": treatment_digest,
                "exact": control_digest == treatment_digest,
                "pixel_values_shape": list(control_payload["pixel_values"].shape),
                "image_grid_thw": control_payload["image_grid_thw"].tolist(),
            }
        )

    control_name = "control_full_chat_template"
    treatment_name = "treatment_vision_only"
    medians = {name: statistics.median(values) for name, values in timings.items()}
    mismatches = [row for row in parity if not row["exact"]]
    payload: Dict[str, Any] = {
        "schema_version": "qwen3-real-vision-only-processor-balanced-v1",
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
            "processor_class": (
                f"{type(processor).__module__}.{type(processor).__name__}"
            ),
            "image_processor_class": (
                f"{type(processor.image_processor).__module__}."
                f"{type(processor.image_processor).__name__}"
            ),
        },
        "config": {
            "samples": args.samples,
            "repeats": args.repeats,
            "orders": orders,
            "image_max_pixels": args.image_max_pixels,
        },
        "records": records,
        "timings_ms": timings,
        "median_ms": medians,
        "treatment_elapsed_pct": _percent(
            medians[treatment_name],
            medians[control_name],
        ),
        "treatment_speedup": (
            medians[control_name] / medians[treatment_name]
        ),
        "parity": parity,
        "validation": {
            "passed": not mismatches,
            "mismatches": len(mismatches),
            "exact_vision_tensor_parity": not mismatches,
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "median_ms": medians,
                "treatment_elapsed_pct": payload["treatment_elapsed_pct"],
                "treatment_speedup": payload["treatment_speedup"],
                "validation": payload["validation"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    for image, _prompt, _sample_id in inputs:
        image.close()
    return 0 if not mismatches else 2


if __name__ == "__main__":
    raise SystemExit(main())
