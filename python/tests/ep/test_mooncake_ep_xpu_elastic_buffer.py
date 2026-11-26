"""Correctness test for ElasticBuffer.create_elastic on Intel XPU.

    MOONCAKE_EP_TEST_DEVICES=1,1,1 \
        python python/tests/ep/test_mooncake_ep_xpu_elastic_buffer.py

Three processes (stable slots 0-2) share a TCPStore, and no ProcessGroup.
They first build a fixed-world buffer of all three, then a capacity-aware
buffer of capacity 3 whose membership goes [0, 1] -> [0, 1, 2] (slot 2
joins) -> [0, 2] (slot 1 leaves, slot 2 becomes logical rank 1) ->
[0, 1, 2] (slot 1 rejoins with a new buffer, slot 2 is rank 2 again).
Every stage runs dispatch/combine rounds, checked exactly: each rank derives
every member's routing and inputs from a shared seed, so it knows which rows
it must receive and what each combined token must be.

MOONCAKE_EP_TEST_DEVICES (e.g. "0,1,2") picks the XPU of each slot; all
slots must be able to map each other's memory over Level Zero IPC.
"""

import argparse
import os
import sys
import time
from datetime import timedelta

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mooncake_ep_xpu import _device_for  # noqa: E402

CAPACITY = 3


def _barrier(store, name, n):
    if store.add(f"test/{name}", 1) == n:
        store.set(f"test/{name}/done", "1")
    store.wait([f"test/{name}/done"])


def _routing(seed, src, num_ranks, args):
    """Logical rank `src`'s top-k picks, the same on every rank."""
    g = torch.Generator().manual_seed(seed * 4099 + src)
    n = int(torch.randint(0, args.max_tokens + 1, (1,), generator=g))
    scores = torch.rand((n, num_ranks * args.experts_per_rank), generator=g)
    idx = torch.topk(scores, args.num_topk, dim=-1).indices
    idx[torch.rand((n,), generator=g) < 0.2, args.num_topk - 1] = -1
    return idx


def _tokens(src, n, salt, hidden):
    """Small integers, exact in bf16 and after summing a few copies."""
    t = torch.arange(n).view(-1, 1)
    h = torch.arange(hidden).view(1, -1)
    return ((t * 7 + h * 13 + src * 31 + salt) % 64 - 32).float()


def run_round(buf, seed, args, tag, combine=True):
    rank, num_ranks = buf.rank_idx, buf.num_ranks
    E, T, device = args.experts_per_rank, args.max_tokens, "xpu"
    routes = [_routing(seed, src, num_ranks, args) for src in range(num_ranks)]
    idx = routes[rank]
    x = _tokens(rank, idx.shape[0], seed, args.hidden)
    recv, _, recv_w, handle, _ = buf.dispatch(
        x.to(device, torch.bfloat16),
        topk_idx=idx.to(device),
        topk_weights=torch.ones(idx.shape, dtype=torch.float32, device=device),
        num_experts=num_ranks * E,
        num_max_tokens_per_rank=T,
        expert_alignment=1,
        do_cpu_sync=True,
    )
    torch.xpu.synchronize()

    # The rows to receive: each member's tokens routed here at least once.
    expected = []
    for src, route in enumerate(routes):
        hits = ((route >= 0) & (route // E == rank)).any(dim=1)
        expected += [src * T + t for t in hits.nonzero().flatten().tolist()]
    actual = int(handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
    src_global = handle.recv_src_metadata[:actual, 0].long().cpu()
    assert sorted(src_global.tolist()) == sorted(expected), (
        f"{tag}: rank {rank} received rows {sorted(src_global.tolist())}, "
        f"expected {sorted(expected)}"
    )
    want = torch.cat(
        [
            _tokens(int(s) // T, routes[int(s) // T].shape[0], seed, args.hidden)[
                int(s) % T
            ].view(1, -1)
            for s in src_global
        ]
        or [torch.empty(0, args.hidden)]
    )
    assert torch.equal(
        recv[:actual].float().cpu(), want
    ), f"{tag}: rank {rank} dispatch payload mismatch"

    if not combine:
        return
    combined, _, _ = buf.combine(
        recv[:actual].contiguous(), handle, topk_weights=recv_w[:actual].contiguous()
    )
    torch.xpu.synchronize()
    # Each token comes back once from every rank it was sent to.
    valid = idx >= 0
    factor = torch.tensor(
        [len(set((idx[t][valid[t]] // E).tolist())) for t in range(idx.shape[0])],
        dtype=torch.float32,
    ).view(-1, 1)
    assert torch.equal(combined.float().cpu(), x * factor), (
        f"{tag}: rank {rank} combine mismatch, max diff "
        f"{(combined.float().cpu() - x * factor).abs().max().item()}"
    )


def run_stage(buf, args, stage, tag):
    for r in range(args.rounds):
        run_round(buf, stage * 100 + r, args, f"{tag} round {r}")
    # An odd number of calls leaves each buffer on its second half, with
    # flags set in the first: a membership change must clear them.
    run_round(buf, stage * 100 + 99, args, f"{tag} dispatch", combine=False)


def worker(slot, args):
    from mooncake.mooncake_elastic_buffer import ElasticBuffer

    torch.xpu.set_device(_device_for(slot))
    store = dist.TCPStore(
        "127.0.0.1",
        args.port,
        CAPACITY,
        is_master=slot == 0,
        timeout=timedelta(seconds=args.timeout),
    )
    common = dict(
        num_max_tokens_per_rank=args.max_tokens,
        hidden=args.hidden,
        num_topk=args.num_topk,
        store_timeout_secs=args.timeout,
    )

    # Fixed world: all three slots, no capacity.
    buf = ElasticBuffer.create_elastic(
        slot, CAPACITY, store, store_namespace="fixed", **common
    )
    assert (buf.rank_idx, buf.num_ranks) == (slot, CAPACITY)
    run_stage(buf, args, 0, "fixed")
    _barrier(store, "fixed-done", CAPACITY)
    buf.destroy()

    planned = dict(capacity_ranks=CAPACITY, **common)
    # [0, 1]; slot 2 waits to join.
    if slot in (0, 1):
        buf = ElasticBuffer.create_elastic(
            slot, 2, store, active_members=[0, 1], store_namespace="p0", **planned
        )
        run_stage(buf, args, 1, "[0, 1]")
        buf.connect_ranks([2], active_members=[0, 1, 2], store_namespace="p1")
    else:
        buf = ElasticBuffer.create_elastic(
            slot,
            3,
            store,
            active_members=[0, 1, 2],
            previous_members=[0, 1],
            store_namespace="p1",
            **planned,
        )
    assert buf.rank_idx == slot
    run_stage(buf, args, 2, "[0, 1, 2]")

    # Slot 1 leaves; slot 2 becomes logical rank 1.
    old_peers = [s for s in range(CAPACITY) if s != slot]
    buf.disconnect_ranks(
        [1] if slot != 1 else old_peers,
        active_members=[0, 2],
        store_namespace="p2",
    )
    if slot != 1:
        assert buf.rank_idx == [0, 2].index(slot)
        run_stage(buf, args, 3, "[0, 2]")
        # Slot 1 rejoins with a new buffer; slot 2 is logical rank 2 again.
        buf.connect_ranks([1], active_members=[0, 1, 2], store_namespace="p3")
    else:
        assert buf.runtime is None
        buf = ElasticBuffer.create_elastic(
            slot,
            3,
            store,
            active_members=[0, 1, 2],
            previous_members=[0, 2],
            store_namespace="p3",
            **planned,
        )
    assert buf.rank_idx == slot
    run_stage(buf, args, 4, "[0, 1, 2] again")

    _barrier(store, "planned-done", CAPACITY)
    buf.destroy()
    _barrier(store, "exit", CAPACITY)
    if slot == 0:
        print("PASSED", flush=True)
        # Keep the Store server up until every client has disconnected.
        time.sleep(1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hidden", type=int, default=2048)
    p.add_argument("--max-tokens", type=int, default=64)
    p.add_argument("--num-topk", type=int, default=4)
    p.add_argument("--experts-per-rank", type=int, default=4)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--port", type=int, default=29611)
    args = p.parse_args()
    mp.spawn(worker, args=(args,), nprocs=CAPACITY, join=True)


if __name__ == "__main__":
    main()
