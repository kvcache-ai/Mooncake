"""RFC §8 evaluation helpers for the real ``mooncake_test_dataset``.

This module is intentionally offline by default: it validates and summarizes the
real dataset artifacts without fabricating model metrics. Real serving probes are
owned by ``scripts/run_vllm_serving_e2e.py`` and can consume the same chat split.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

RFC_WORKLOADS = ("W0", "W1", "W2", "W3", "W4", "W5")


def _load_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_jsonl(path: Path, *, limit: int | None = None) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if limit is not None and idx >= limit:
                break
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "avg": float(statistics.fmean(values)),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _workload_family(item: dict[str, Any]) -> str:
    family = item.get("workload_family")
    if not family:
        family = (item.get("extensions") or {}).get("workload_family")
    return str(family or "unknown")


def _image_records_from_messages(messages: list[dict[str, Any]]) -> list[str]:
    images: list[str] = []
    for msg in messages or []:
        content = msg.get("content") or []
        if isinstance(content, str):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "image" and part.get("image"):
                images.append(str(part["image"]))
            elif part.get("type") == "image_url":
                image_url = part.get("image_url")
                if isinstance(image_url, dict) and image_url.get("url"):
                    images.append(str(image_url["url"]))
    return images


def _image_path_exists(dataset_root: Path, path_or_url: str) -> bool:
    if path_or_url.startswith(("http://", "https://", "data:")):
        return True
    return (dataset_root / path_or_url).exists()


def _summarize_workflows(dataset_root: Path, split_path: Path, *, limit: int | None) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    by_family: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    by_priority: Counter[str] = Counter()
    by_task_type: Counter[str] = Counter()
    step_counts: list[float] = []
    token_lens: list[float] = []
    image_counts: list[float] = []
    reuse_groups: Counter[str] = Counter()
    missing_images: list[str] = []
    total_images = 0
    workloads: dict[str, list[dict[str, Any]]] = {key: [] for key in RFC_WORKLOADS}

    total = 0
    for wf in _iter_jsonl(split_path, limit=limit):
        total += 1
        family = _workload_family(wf)
        by_family[family] += 1
        by_source[str(wf.get("source_dataset", "unknown"))] += 1
        by_priority[str(wf.get("priority_class", "unknown"))] += 1
        by_task_type[str(wf.get("task_type", "unknown"))] += 1
        steps = list(wf.get("steps") or [])
        images = list(wf.get("images") or [])
        step_counts.append(float(len(steps)))
        image_counts.append(float(len(images)))
        token_lens.extend(float(step.get("input_token_len", 0) or 0) for step in steps)
        for img in images:
            if not isinstance(img, dict):
                continue
            total_images += 1
            path_or_url = str(img.get("path_or_url", ""))
            reuse_group = str(img.get("reuse_group") or img.get("image_id") or path_or_url)
            if reuse_group:
                reuse_groups[reuse_group] += 1
            if path_or_url and not _image_path_exists(dataset_root, path_or_url):
                missing_images.append(path_or_url)
        if family in workloads:
            workloads[family].append(
                {
                    "workflow_id": wf.get("workflow_id"),
                    "workload": family,
                    "scenario": wf.get("task_type", family),
                    "source_dataset": wf.get("source_dataset"),
                    "priority_class": wf.get("priority_class"),
                    "step_count": len(steps),
                    "image_count": len(images),
                    "shared_image_reuse": int(len(steps) > 1 or any(reuse_groups[g] > 1 for g in reuse_groups)),
                    "prompt_tokens": sum(int(step.get("input_token_len", 0) or 0) for step in steps),
                }
            )

    summary = {
        "path": str(split_path),
        "total_workflows": total,
        "by_family": dict(sorted(by_family.items())),
        "by_source": dict(sorted(by_source.items())),
        "by_priority_class": dict(sorted(by_priority.items())),
        "by_task_type": dict(sorted(by_task_type.items())),
        "step_count": _stats(step_counts),
        "input_token_len": _stats(token_lens),
        "image_count": _stats(image_counts),
        "total_images": total_images,
        "missing_images": missing_images[:50],
        "missing_image_count": len(missing_images),
        "image_path_valid_rate": ((total_images - len(missing_images)) / total_images) if total_images else 1.0,
        "reuse_groups": {
            "unique": len(reuse_groups),
            "reused": sum(1 for count in reuse_groups.values() if count > 1),
            "max_reuse": max(reuse_groups.values(), default=0),
        },
    }
    return summary, workloads


def _summarize_chat(dataset_root: Path, chat_path: Path, *, limit: int | None) -> dict[str, Any]:
    by_family: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    by_priority: Counter[str] = Counter()
    prompt_lens: list[float] = []
    images_per_sample: list[float] = []
    missing_images: list[str] = []
    total_images = 0
    total = 0
    multimodal = 0
    for item in _iter_jsonl(chat_path, limit=limit):
        total += 1
        family = _workload_family(item)
        by_family[family] += 1
        by_source[str(item.get("source_dataset", "unknown"))] += 1
        by_priority[str(item.get("priority_class", "unknown"))] += 1
        prompt_lens.append(float(item.get("prompt_token_len", 0) or 0))
        image_paths = [str((img or {}).get("path_or_url", "")) for img in item.get("images") or [] if isinstance(img, dict)]
        image_paths.extend(_image_records_from_messages(list(item.get("messages") or [])))
        image_paths = [p for p in image_paths if p]
        if image_paths:
            multimodal += 1
        images_per_sample.append(float(len(image_paths)))
        for path_or_url in image_paths:
            total_images += 1
            if not _image_path_exists(dataset_root, path_or_url):
                missing_images.append(path_or_url)
    return {
        "path": str(chat_path),
        "total_examples": total,
        "multimodal_examples": multimodal,
        "by_family": dict(sorted(by_family.items())),
        "by_source": dict(sorted(by_source.items())),
        "by_priority_class": dict(sorted(by_priority.items())),
        "prompt_token_len": _stats(prompt_lens),
        "images_per_sample": _stats(images_per_sample),
        "total_image_references": total_images,
        "missing_images": missing_images[:50],
        "missing_image_count": len(missing_images),
        "image_path_valid_rate": ((total_images - len(missing_images)) / total_images) if total_images else 1.0,
    }


def _summarize_schedules(schedule_dir: Path, *, limit_files: int | None) -> dict[str, Any]:
    by_name: dict[str, Any] = {}
    aggregate_family: Counter[str] = Counter()
    aggregate_fault: Counter[str] = Counter()
    arrival_spans: list[float] = []
    paths = sorted(schedule_dir.glob("*.jsonl"))
    if limit_files is not None:
        paths = paths[:limit_files]
    for path in paths:
        family: Counter[str] = Counter()
        fault: Counter[str] = Counter()
        arrivals: list[float] = []
        count = 0
        load_ratio = None
        mode = None
        split = None
        for event in _iter_jsonl(path):
            count += 1
            fam = str(event.get("workload_family", "unknown"))
            fp = str(event.get("fault_profile", "none"))
            family[fam] += 1
            fault[fp] += 1
            aggregate_family[fam] += 1
            aggregate_fault[fp] += 1
            if event.get("arrival_ms") is not None:
                arrivals.append(float(event["arrival_ms"]))
            load_ratio = event.get("normalized_load_ratio", load_ratio)
            mode = event.get("arrival_mode", mode)
            split = event.get("split", split)
        span = (max(arrivals) - min(arrivals)) if arrivals else 0.0
        arrival_spans.append(span)
        by_name[path.stem] = {
            "path": str(path),
            "split": split,
            "arrival_mode": mode,
            "normalized_load_ratio": load_ratio,
            "event_count": count,
            "family_mix": dict(sorted(family.items())),
            "fault_mix": dict(sorted(fault.items())),
            "arrival_span_ms": span,
        }
    return {
        "schedule_count": len(paths),
        "by_schedule": by_name,
        "aggregate_family_mix": dict(sorted(aggregate_family.items())),
        "aggregate_fault_mix": dict(sorted(aggregate_fault.items())),
        "arrival_span_ms": _stats(arrival_spans),
    }


def _summarize_workload_rows(workloads: dict[str, list[dict[str, Any]]], schedules: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for workload in RFC_WORKLOADS:
        items = workloads.get(workload, [])
        if workload == "W5" and not items:
            samples = int(sum((s.get("event_count", 0) or 0) for s in (schedules.get("by_schedule") or {}).values()))
            scenarios = ["mixed_online_schedule"] if samples else []
            rows.append({"workload": workload, "samples": samples, "avg_steps": 0.0, "image_reuse_rate": 0.0, "scenarios": scenarios})
            continue
        rows.append(
            {
                "workload": workload,
                "samples": len(items),
                "avg_steps": (sum(float(i.get("step_count", 0) or 0) for i in items) / len(items)) if items else 0.0,
                "image_reuse_rate": (sum(int(i.get("shared_image_reuse", 0) or 0) for i in items) / len(items)) if items else 0.0,
                "scenarios": sorted({str(i.get("scenario", "unknown")) for i in items}),
                "sources": sorted({str(i.get("source_dataset", "unknown")) for i in items}),
            }
        )
    return rows


def build_mooncake_dataset_eval(
    dataset_root: str | Path,
    *,
    split: str = "dev-small",
    chat_split: str = "dev-small",
    workflow_limit: int | None = None,
    chat_limit: int | None = None,
    schedule_limit: int | None = None,
    include_workloads: bool = True,
) -> dict[str, Any]:
    root = Path(dataset_root).resolve()
    split_path = root / "splits" / f"{split}.jsonl"
    chat_path = root / "chat_splits" / f"{chat_split}.jsonl"
    schedule_dir = root / "schedules"
    if not root.exists():
        raise FileNotFoundError(f"dataset root not found: {root}")
    if not split_path.exists():
        raise FileNotFoundError(f"workflow split not found: {split_path}")
    if not chat_path.exists():
        raise FileNotFoundError(f"chat split not found: {chat_path}")

    metadata = {
        "summary": _load_json(root / "metadata" / "summary.json", default={}),
        "split_counts": _load_json(root / "metadata" / "split_counts.json", default={}),
        "chat_split_counts": _load_json(root / "metadata" / "chat_split_counts.json", default={}),
        "schedule_counts": _load_json(root / "metadata" / "schedule_counts.json", default={}),
        "source_manifest": _load_json(root / "metadata" / "source_manifest.json", default={}),
    }
    workflow_summary, workloads = _summarize_workflows(root, split_path, limit=workflow_limit)
    chat_summary = _summarize_chat(root, chat_path, limit=chat_limit)
    schedules = _summarize_schedules(schedule_dir, limit_files=schedule_limit) if schedule_dir.exists() else {"schedule_count": 0}
    workload_rows = _summarize_workload_rows(workloads, schedules)
    source_manifest = metadata.get("source_manifest") or {}
    source_status_counts = Counter(
        str((item or {}).get("status", "unknown"))
        for item in source_manifest.values()
        if isinstance(item, dict)
    )
    downloaded_sources = int(source_status_counts.get("downloaded", 0))
    omitted_gated_sources = int(source_status_counts.get("omitted_gated", 0))
    total_sources = len(source_manifest)
    unresolved_sources = [
        key
        for key, item in source_manifest.items()
        if isinstance(item, dict) and item.get("status") not in {"downloaded", "omitted_gated"}
    ]
    pass_checks = {
        "workflow_split_present": split_path.exists(),
        "chat_split_present": chat_path.exists(),
        "workflow_image_paths_valid": workflow_summary["missing_image_count"] == 0,
        "chat_image_paths_valid": chat_summary["missing_image_count"] == 0,
        "w0_w4_covered": all(workflow_summary["by_family"].get(w, 0) > 0 for w in ("W0", "W1", "W2", "W3", "W4")),
        "w5_schedules_present": int(schedules.get("schedule_count", 0) or 0) > 0,
        "source_manifest_resolved": not unresolved_sources and total_sources > 0,
    }
    manifest_workloads = workloads if include_workloads else {key: [] for key in RFC_WORKLOADS}
    payload = {
        "generated_at_unix": time.time(),
        "dataset_root": str(root),
        "dataset_name": (metadata.get("summary") or {}).get("dataset_name", root.name),
        "version": (metadata.get("summary") or {}).get("version"),
        "split": split,
        "chat_split": chat_split,
        "metadata_counts": {
            "split": (metadata.get("split_counts") or {}).get(split, {}),
            "chat_split": (metadata.get("chat_split_counts") or {}).get(chat_split, {}),
        },
        "workflow_summary": workflow_summary,
        "chat_summary": chat_summary,
        "schedule_summary": schedules,
        "source_manifest_summary": {
            "total_sources": total_sources,
            "downloaded_sources": downloaded_sources,
            "omitted_gated_sources": omitted_gated_sources,
            "status_counts": dict(sorted(source_status_counts.items())),
            "unresolved_sources": unresolved_sources,
            "downloaded_ratio": (downloaded_sources / total_sources) if total_sources else 0.0,
            "resolved_ratio": ((downloaded_sources + omitted_gated_sources) / total_sources) if total_sources else 0.0,
        },
        "pass_checks": pass_checks,
        "pass": all(pass_checks.values()),
        "workload_matrix_table": workload_rows,
        "manifest": {
            "mix": {row["workload"]: row["samples"] for row in workload_rows},
            "summary": workload_rows,
            "workloads": manifest_workloads,
            "dataset_eval": {},
        },
    }
    payload["manifest"]["dataset_eval"] = {
        key: value for key, value in payload.items() if key not in {"manifest"}
    }
    return payload


def write_mooncake_dataset_eval_artifacts(
    *,
    dataset_root: str | Path,
    eval_output_path: str | Path,
    manifest_output_path: str | Path | None = None,
    split: str = "dev-small",
    chat_split: str = "dev-small",
    workflow_limit: int | None = None,
    chat_limit: int | None = None,
    schedule_limit: int | None = None,
    include_workloads: bool = True,
) -> dict[str, Any]:
    payload = build_mooncake_dataset_eval(
        dataset_root,
        split=split,
        chat_split=chat_split,
        workflow_limit=workflow_limit,
        chat_limit=chat_limit,
        schedule_limit=schedule_limit,
        include_workloads=include_workloads,
    )
    eval_path = Path(eval_output_path)
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if manifest_output_path is not None:
        manifest_path = Path(manifest_output_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(payload["manifest"], ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build RFC §8 manifest/eval artifacts from mooncake_test_dataset.")
    ap.add_argument("--dataset-root", required=True)
    ap.add_argument("--split", default="dev-small")
    ap.add_argument("--chat-split", default="dev-small")
    ap.add_argument("--workflow-limit", type=int, default=None)
    ap.add_argument("--chat-limit", type=int, default=None)
    ap.add_argument("--schedule-limit", type=int, default=None)
    ap.add_argument("--eval-output", required=True)
    ap.add_argument("--manifest-output", default=None)
    ap.add_argument("--no-workloads", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    payload = write_mooncake_dataset_eval_artifacts(
        dataset_root=args.dataset_root,
        eval_output_path=args.eval_output,
        manifest_output_path=args.manifest_output,
        split=args.split,
        chat_split=args.chat_split,
        workflow_limit=args.workflow_limit,
        chat_limit=args.chat_limit,
        schedule_limit=args.schedule_limit,
        include_workloads=not args.no_workloads,
    )
    print(json.dumps({
        "eval_output": args.eval_output,
        "manifest_output": args.manifest_output,
        "pass": payload.get("pass"),
        "pass_checks": payload.get("pass_checks"),
        "workflow_total": payload.get("workflow_summary", {}).get("total_workflows"),
        "chat_total": payload.get("chat_summary", {}).get("total_examples"),
        "workloads": {row["workload"]: row["samples"] for row in payload.get("workload_matrix_table", [])},
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
