#!/usr/bin/env python3
"""Benchmark Agent KV state cloning with real PagedKVManager/MooncakeKVStateStore.

This runner is intentionally dependency-light but not mock-based: it allocates
real torch KV pages, compares deep-copy branch creation with descriptor/refcount
cloning, and records CoW/materialization behavior.  CUDA measurements bracket
each timed region with device synchronization so asynchronous kernel launch
overhead is never reported as deep-copy throughput.  It is suitable for CI CPU
sanity and for GPU runs by passing ``--device cuda:N``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.agent import AgentStateCloner  # noqa: E402
from mooncake_epd.core.state import MooncakeKVStateStore, PagedKVManager  # noqa: E402
from mooncake_epd.scripts.benchmark_artifact_io import (  # noqa: E402
    write_raw_benchmark_artifacts,
)


def _stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
    ordered = sorted(values)
    idx95 = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return {
        "count": len(values),
        "avg": float(statistics.mean(values)),
        "p50": float(statistics.median(values)),
        "p95": float(ordered[idx95]),
        "max": float(max(values)),
    }


def _synchronize_for_timing(device: torch.device) -> None:
    """Drain CUDA work at a timing boundary without affecting CPU runners."""

    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _make_pm(args: argparse.Namespace) -> PagedKVManager:
    return PagedKVManager(
        page_size=args.page_size,
        num_layers=args.num_layers,
        num_kv_heads=args.num_kv_heads,
        head_dim=args.head_dim,
        dtype=getattr(torch, args.dtype),
        device=torch.device(args.device),
        node_id=args.node_id,
    )


def run(args: argparse.Namespace) -> Dict[str, Any]:
    device = torch.device(args.device)
    pm = _make_pm(args)
    refs = pm.allocate_pages(args.pages, filled=args.page_size)
    for i, ref in enumerate(refs):
        key = torch.full(
            (args.num_layers, args.num_kv_heads, args.page_size, args.head_dim),
            float(i + 1),
            dtype=getattr(torch, args.dtype),
            device=torch.device(args.device),
        )
        value = key + 100
        pm.write_page_slots(ref, key, value)
    _synchronize_for_timing(device)

    # Deep-copy baseline: clone every physical page tensor once per branch and
    # retain every branch until the baseline completes.  A real Agent fork
    # keeps all speculative branches live; freeing each one before the next
    # iteration would let the allocator reuse a single branch's memory and
    # understate the cost of an equivalent fork.
    deep_copy_ms: List[float] = []
    deep_copy_bytes = 0
    deep_copy_branches = []
    for _ in range(args.branches):
        _synchronize_for_timing(device)
        start = time.perf_counter()
        copies = []
        for ref in refs:
            key, value = pm.get_page(ref)
            k = key.clone()
            v = value.clone()
            copies.append((k, v))
            deep_copy_bytes += k.nelement() * k.element_size() + v.nelement() * v.element_size()
        # Keep copies alive until timing ends so allocation/copy is measured.
        assert len(copies) == len(refs)
        _synchronize_for_timing(device)
        deep_copy_ms.append((time.perf_counter() - start) * 1000.0)
        deep_copy_branches.append(copies)
    # The deep-copy branches were retained during their own measurements.  Do
    # not keep their private buffers alive while timing descriptor-only clones.
    del deep_copy_branches
    del copies

    store = MooncakeKVStateStore(pm, node_id=args.node_id)
    cloner = AgentStateCloner(kv_state_store=store)
    cloner.register_kv_refs("agent-parent", refs, workflow_id=args.workflow_id)
    zero_copy_ms: List[float] = []
    branches = []
    for i in range(args.branches):
        # A descriptor/refcount clone issues no CUDA work.  Drain prior work
        # before starting, but do not add a redundant post-sync to its CPU-only
        # measurement (that would benchmark CUDA driver latency, not clone
        # latency).
        _synchronize_for_timing(device)
        start = time.perf_counter()
        branches.append(cloner.clone_kv_state("agent-parent", f"branch-{i}"))
        zero_copy_ms.append((time.perf_counter() - start) * 1000.0)

    refcounts_after_clone = [pm.refcount(ref.global_block_id) for ref in refs]
    first_branch_refs = cloner.get_branch_kv_refs(branches[0].branch_id, for_write=False)
    old_first_ref = first_branch_refs[0]
    _synchronize_for_timing(device)
    cow_copy_start = time.perf_counter()
    cow_ref = pm.cow_page(old_first_ref)
    _synchronize_for_timing(device)
    cow_copy_ms = (time.perf_counter() - cow_copy_start) * 1000.0
    # Commit the branch version so MooncakeKVStateStore does not release the
    # consumed shared ref twice when the branch is later released.
    _synchronize_for_timing(device)
    version_commit_start = time.perf_counter()
    store.commit_state_version(
        branches[0].branch_id,
        [cow_ref] + first_branch_refs[1:],
        state_id=branches[0].branch_id,
        consumed_old_block_ids=[old_first_ref.global_block_id],
        # ``cow_page`` creates the only new page and this benchmark commits it
        # before any write.  The remaining shared pages are immutable, so
        # avoid turning a one-page CoW into a full-state GPU checksum sweep.
        immutable_reused_block_ids=[
            ref.global_block_id for ref in first_branch_refs[1:]
        ],
    )
    _synchronize_for_timing(device)
    version_commit_ms = (time.perf_counter() - version_commit_start) * 1000.0
    cow_total_ms = cow_copy_ms + version_commit_ms
    cow_bytes = 0
    key, value = pm.get_page(cow_ref)
    cow_bytes += key.nelement() * key.element_size() + value.nelement() * value.element_size()

    for branch in branches:
        cloner.release_branch(branch.branch_id)
    cloner.release_branch("agent-parent")

    return {
        "device": args.device,
        "dtype": args.dtype,
        "timing_method": (
            "cuda_synchronized_wall_clock" if device.type == "cuda" else "wall_clock"
        ),
        "pages": args.pages,
        "branches": args.branches,
        "page_size": args.page_size,
        "num_layers": args.num_layers,
        "num_kv_heads": args.num_kv_heads,
        "head_dim": args.head_dim,
        "deep_copy_ms": _stats(deep_copy_ms),
        "zero_copy_clone_ms": _stats(zero_copy_ms),
        "speedup_avg": (statistics.mean(deep_copy_ms) / max(statistics.mean(zero_copy_ms), 1e-9)),
        "deep_copy_bytes": int(deep_copy_bytes),
        "deep_copy_branches_retained": True,
        "zero_copy_clone_bytes": 0,
        "refcounts_after_clone": refcounts_after_clone,
        # Keep ``cow_ms`` for existing artifact readers; the split fields make
        # it explicit that a version commit may intentionally revalidate
        # persisted KV manifests beyond the physical CoW copy itself.
        "cow_ms": cow_total_ms,
        "cow_copy_ms": cow_copy_ms,
        "version_commit_ms": version_commit_ms,
        "version_commit_checksum_scope": "changed_pages_only",
        "version_commit_immutable_reused_pages": max(0, len(first_branch_refs) - 1),
        "version_commit_revalidates_kv_pages": True,
        "cow_bytes_upper_bound": int(cow_bytes),
        "store_stats_after_release": store.stats(),
        "page_manager_stats_after_release": pm.stats(),
    }


def write_agent_clone_raw_artifacts(
    *,
    summary: Dict[str, Any],
    args: argparse.Namespace,
    output: Path,
) -> Dict[str, str]:
    """Persist replayable benchmark provenance without serializing KV payloads.

    The clone benchmark intentionally never writes model KV tensors to disk.
    The raw bundle records only the layout/configuration, measured summary and
    CUDA runtime identity required to reproduce the allocation and timing
    conditions.
    """

    device = torch.device(args.device)
    runtime: Dict[str, Any] = {
        "torch_version": torch.__version__,
        "torch_cuda_version": torch.version.cuda,
        "device": str(device),
        "cuda_available": bool(torch.cuda.is_available()),
    }
    if device.type == "cuda" and torch.cuda.is_available():
        runtime["cuda_device_name"] = torch.cuda.get_device_name(device)
        runtime["cuda_device_capability"] = list(torch.cuda.get_device_capability(device))
    request = {
        "family": "agent_state_clone",
        "workflow_id": str(args.workflow_id),
        "config": {
            "device": str(args.device),
            "dtype": str(args.dtype),
            "node_id": str(args.node_id),
            "branches": int(args.branches),
            "pages": int(args.pages),
            "page_size": int(args.page_size),
            "num_layers": int(args.num_layers),
            "num_kv_heads": int(args.num_kv_heads),
            "head_dim": int(args.head_dim),
        },
    }
    response = {
        "status": "ok",
        "speedup_avg": float(summary.get("speedup_avg", 0.0) or 0.0),
        "zero_copy_clone_bytes": int(summary.get("zero_copy_clone_bytes", -1) or -1),
        "store_stats_after_release": dict(summary.get("store_stats_after_release") or {}),
        "page_manager_stats_after_release": dict(
            summary.get("page_manager_stats_after_release") or {}
        ),
    }
    return write_raw_benchmark_artifacts(
        workdir=output.parent / f"{output.stem}_evidence",
        request_rows=[request],
        response_rows=[response],
        service_logs={},
        metrics=summary,
        runtime=runtime,
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Benchmark real Agent State Cloning over MooncakeKVStateStore")
    ap.add_argument("--output", default="artifacts/agent_state_clone_benchmark.json")
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--dtype", default="float32", choices=["float32", "float16", "bfloat16"])
    ap.add_argument("--node-id", default="prefill-a")
    ap.add_argument("--workflow-id", default="wf-agent-clone-bench")
    ap.add_argument("--branches", type=int, default=4)
    ap.add_argument("--pages", type=int, default=16)
    ap.add_argument("--page-size", type=int, default=16)
    ap.add_argument("--num-layers", type=int, default=8)
    ap.add_argument("--num-kv-heads", type=int, default=4)
    ap.add_argument("--head-dim", type=int, default=64)
    ap.add_argument(
        "--record-raw-artifacts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Persist config/runtime/summary evidence without writing KV tensors (default: true).",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    if bool(args.record_raw_artifacts):
        summary["raw_artifacts"] = write_agent_clone_raw_artifacts(
            summary=summary,
            args=args,
            output=out,
        )
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
