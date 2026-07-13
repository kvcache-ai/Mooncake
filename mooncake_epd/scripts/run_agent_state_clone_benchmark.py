#!/usr/bin/env python3
"""Benchmark Agent KV state cloning with real PagedKVManager/MooncakeKVStateStore.

This runner is intentionally dependency-light but not mock-based: it allocates
real torch KV pages, compares deep-copy branch creation with descriptor/refcount
cloning, and records CoW/materialization behavior.  It is suitable for CI CPU
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

    # Deep-copy baseline: clone every physical page tensor once per branch.
    deep_copy_ms: List[float] = []
    deep_copy_bytes = 0
    for _ in range(args.branches):
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
        deep_copy_ms.append((time.perf_counter() - start) * 1000.0)

    store = MooncakeKVStateStore(pm, node_id=args.node_id)
    cloner = AgentStateCloner(kv_state_store=store)
    cloner.register_kv_refs("agent-parent", refs, workflow_id=args.workflow_id)
    zero_copy_ms: List[float] = []
    branches = []
    for i in range(args.branches):
        start = time.perf_counter()
        branches.append(cloner.clone_kv_state("agent-parent", f"branch-{i}"))
        zero_copy_ms.append((time.perf_counter() - start) * 1000.0)

    refcounts_after_clone = [pm.refcount(ref.global_block_id) for ref in refs]
    cow_start = time.perf_counter()
    first_branch_refs = cloner.get_branch_kv_refs(branches[0].branch_id, for_write=False)
    old_first_ref = first_branch_refs[0]
    cow_ref = pm.cow_page(old_first_ref)
    # Commit the branch version so MooncakeKVStateStore does not release the
    # consumed shared ref twice when the branch is later released.
    store.commit_state_version(
        branches[0].branch_id,
        [cow_ref] + first_branch_refs[1:],
        state_id=branches[0].branch_id,
        consumed_old_block_ids=[old_first_ref.global_block_id],
    )
    cow_ms = (time.perf_counter() - cow_start) * 1000.0
    cow_bytes = 0
    key, value = pm.get_page(cow_ref)
    cow_bytes += key.nelement() * key.element_size() + value.nelement() * value.element_size()

    for branch in branches:
        cloner.release_branch(branch.branch_id)
    cloner.release_branch("agent-parent")

    return {
        "device": args.device,
        "dtype": args.dtype,
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
        "zero_copy_clone_bytes": 0,
        "refcounts_after_clone": refcounts_after_clone,
        "cow_ms": cow_ms,
        "cow_bytes_upper_bound": int(cow_bytes),
        "store_stats_after_release": store.stats(),
        "page_manager_stats_after_release": pm.stats(),
    }


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
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
