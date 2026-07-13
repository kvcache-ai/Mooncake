"""RFC §8 workload and dev-small evaluation helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from mooncake_epd.tests.dataset import WorkflowExample, build_dataset, image_hash, make_image


DEFAULT_MIX = {
    "W0": 0.25,
    "W1": 0.20,
    "W2": 0.25,
    "W3": 0.15,
    "W4": 0.10,
    "FAULT": 0.05,
}


def _base_item(example: WorkflowExample, workload: str, *, step_count: int) -> Dict:
    img = make_image(example.image_name)
    return {
        "workload": workload,
        "scenario": example.scenario,
        "image_name": example.image_name,
        "image_hash": image_hash(img),
        "step_count": step_count,
        "tool_steps": sum(1 for output in example.tool_outputs if output),
        "shared_image_reuse": int(step_count > 1),
    }


def build_dev_small_workloads(
    dataset: Optional[List[WorkflowExample]] = None,
    *,
    mixed_size: int = 20,
) -> Dict[str, List[Dict]]:
    """Build RFC W0-W5 dev-small workload manifests from the synthetic dataset."""

    dataset = dataset or build_dataset()
    w0: List[Dict] = []
    w1: List[Dict] = []
    w2: List[Dict] = []
    w3: List[Dict] = []
    w4: List[Dict] = []

    for example in dataset:
        first_step = _base_item(example, "W0", step_count=1)
        first_step["prompt"] = example.steps[0]
        w0.append(first_step)

        if example.scenario == "multi_turn":
            item = _base_item(example, "W1", step_count=len(example.steps))
            item["turns"] = len(example.steps)
            w1.append(item)
        elif example.scenario == "tool_use_vqa":
            item = _base_item(example, "W2", step_count=len(example.steps))
            item["tool_wait_buckets_ms"] = [50, 200, 1000, 5000]
            w2.append(item)
        elif example.scenario == "tree_of_thought":
            item = _base_item(example, "W3", step_count=len(example.steps))
            item["branch_widths"] = [2, 4, 8, 16, 32]
            w3.append(item)
        elif example.scenario == "a2a_handoff":
            item = _base_item(example, "W4", step_count=len(example.steps))
            item["handoff_steps"] = max(0, len(example.steps) - 1)
            w4.append(item)

    pools = {"W0": w0, "W1": w1, "W2": w2, "W3": w3, "W4": w4}
    w5: List[Dict] = []
    for workload, ratio in DEFAULT_MIX.items():
        if workload == "FAULT":
            continue
        count = max(1, int(round(mixed_size * ratio)))
        pool = pools[workload]
        for idx in range(count):
            source = pool[idx % len(pool)]
            item = dict(source)
            item["workload"] = "W5"
            item["mixed_from"] = workload
            w5.append(item)
    fault_count = max(1, mixed_size - len(w5))
    for idx in range(fault_count):
        source = w2[idx % len(w2)] if w2 else w0[idx % len(w0)]
        item = dict(source)
        item["workload"] = "W5"
        item["mixed_from"] = "FAULT"
        item["fault_injection"] = ["offload_abort", "handoff_rollback", "overload"]
        w5.append(item)

    return {"W0": w0, "W1": w1, "W2": w2, "W3": w3, "W4": w4, "W5": w5}


def summarize_workloads(workloads: Dict[str, List[Dict]]) -> List[Dict]:
    rows: List[Dict] = []
    for workload in ("W0", "W1", "W2", "W3", "W4", "W5"):
        items = workloads.get(workload, [])
        rows.append(
            {
                "workload": workload,
                "samples": len(items),
                "avg_steps": (
                    sum(int(item.get("step_count", 0)) for item in items) / len(items)
                    if items else 0.0
                ),
                "image_reuse_rate": (
                    sum(int(item.get("shared_image_reuse", 0)) for item in items) / len(items)
                    if items else 0.0
                ),
                "scenarios": sorted({str(item.get("scenario", "unknown")) for item in items}),
            }
        )
    return rows


def export_dev_small_manifest(
    output_path: str | Path,
    dataset: Optional[List[WorkflowExample]] = None,
    *,
    mixed_size: int = 20,
) -> Path:
    workloads = build_dev_small_workloads(dataset=dataset, mixed_size=mixed_size)
    payload = {
        "mix": dict(DEFAULT_MIX),
        "summary": summarize_workloads(workloads),
        "workloads": workloads,
    }
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
