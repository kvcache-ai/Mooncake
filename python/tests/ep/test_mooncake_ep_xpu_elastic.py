"""Correctness test for Mooncake EP elastic mode (Buffer.create_elastic) on
Intel XPU.

    MOONCAKE_EP_TEST_DEVICES=1,1,1 \
        python python/tests/ep/test_mooncake_ep_xpu_elastic.py --num-ranks 3

Every rank owns a buffer of --capacity rank slots (by default one more than
--num-ranks, so a slot stays empty) and connects to its peers over Level Zero
IPC through a TCPStore, with no ProcessGroup in the buffer. Dispatch/combine
rounds are checked exactly after each lifecycle step: all ranks, one rank
masked while it stays alive, unmasked again, the last rank replaced by a new
buffer (as a restarted process would be), mask and clean_mask_buffer, a
zeroed buffer, and a scale-down and staged scale-up of the last rank in the
order vLLM's elastic EP uses (set_tcp_store_group, connect_ranks with
activate=False, then update_mask_buffer).

MOONCAKE_EP_TEST_DEVICES (e.g. "0,1,2") picks the XPU of each rank; all ranks
must be able to map each other's memory over Level Zero IPC.
"""

import argparse
import os
import random
import sys
from datetime import timedelta

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mooncake_ep_xpu import (  # noqa: E402
    _device_for,
    _expert_scale,
    _fp8_reference,
)


def _route(n, num_experts, num_topk, allowed, device):
    """Top-k picks among the experts of `allowed` ranks, some slots -1."""
    scores = torch.rand((n, num_experts), device=device)
    scores[:, ~allowed] = -1.0
    idx = torch.topk(scores, num_topk, dim=-1).indices
    if n:
        idx[torch.rand((n, num_topk), device=device) < 0.1] = -1
    return idx


def _gather(t, group, size):
    out = [torch.empty_like(t) for _ in range(size)]
    dist.all_gather(out, t.contiguous(), group=group)
    return out


def run_round(buf, rank, members, group, args, use_fp8, hook_mode, tag):
    """One dispatch/combine among `members`, the active ranks, checked exactly
    against every member's inputs. Ranks outside `members` must not call it."""
    device = torch.device("xpu", torch.xpu.current_device())
    E, hidden, T = args.experts_per_rank, args.hidden, args.max_tokens
    bound = max(members) + 1
    num_experts = bound * E
    allowed = torch.zeros(num_experts, dtype=torch.bool, device=device)
    for r in members:
        allowed[r * E : (r + 1) * E] = True

    n = random.randint(0, T)
    x = torch.randn((n, hidden), device=device).to(torch.bfloat16)
    idx = _route(n, num_experts, args.num_topk, allowed, device)
    w = torch.rand((n, args.num_topk), device=device)

    recv, recv_count, handle, event, hook = buf.dispatch(
        x,
        idx,
        T,
        num_experts,
        use_fp8=use_fp8,
        async_finish=not hook_mode,
        return_recv_hook=hook_mode,
    )
    hook() if hook_mode else event.current_stream_wait()
    recv_x, recv_scales = recv if use_fp8 else (recv, None)
    assert recv_count.numel() == E
    assert handle[1].shape == (E, bound), f"{tag}: layout {handle[1].shape}"

    # Every member's inputs, padded to T rows (bf16 sent as fp32 over gloo).
    pad_x = torch.zeros((T, hidden), dtype=torch.bfloat16)
    pad_x[:n] = x.cpu()
    pad_idx = torch.full((T, args.num_topk), -1, dtype=torch.int64)
    pad_idx[:n] = idx.cpu()
    size = len(members)
    all_x = [t.to(torch.bfloat16) for t in _gather(pad_x.float(), group, size)]
    all_idx = _gather(pad_idx, group, size)
    all_n = [int(t) for t in _gather(torch.tensor([n]), group, size)]
    by_rank = {r: (all_x[i], all_idx[i][: all_n[i]], all_n[i]) for i, r in enumerate(members)}

    src_info, layout = handle[0].cpu(), handle[1].cpu()
    counts, got_x = recv_count.cpu(), recv_x.cpu()
    got_scales = recv_scales.cpu() if use_fp8 else None
    for le in range(E):
        expert = rank * E + le
        want_total = sum(int((ri == expert).sum()) for _, ri, _ in by_rank.values())
        assert int(counts[le]) == want_total, (
            f"{tag}: expert {expert} received {int(counts[le])}, want {want_total}"
        )
        for r in range(bound):
            cnt = int(layout[le, r]) & 0xFFFFFFFF
            begin = int(layout[le, r]) >> 32
            if r not in by_rank:
                assert cnt == 0, f"{tag}: {cnt} tokens from inactive rank {r}"
                continue
            rx, ri, rn = by_rank[r]
            assert cnt == int((ri == expert).any(-1).sum()), (
                f"{tag}: expert {expert} from rank {r}: {cnt} tokens"
            )
            if cnt == 0:
                continue
            tok = src_info[le, begin : begin + cnt].long()
            assert bool(((tok >= 0) & (tok < rn)).all()), f"{tag}: bad src_info"
            assert bool((ri[tok] == expert).any(-1).all()), f"{tag}: wrong token"
            if use_fp8:
                codes, scale_inv, _ = _fp8_reference(rx[tok])
                assert torch.equal(
                    got_x[le, begin : begin + cnt].view(torch.uint8),
                    codes.view(torch.uint8),
                ), f"{tag}: FP8 payload from rank {r}"
                assert torch.equal(
                    got_scales[le, begin : begin + cnt], scale_inv
                ), f"{tag}: FP8 scales from rank {r}"
            else:
                assert torch.equal(got_x[le, begin : begin + cnt], rx[tok]), (
                    f"{tag}: payload from rank {r}"
                )

    if use_fp8:
        expert_in = (
            recv_x.float().view(E, -1, hidden // 128, 128) * recv_scales.unsqueeze(-1)
        ).view(E, -1, hidden)
    else:
        expert_in = recv_x.float()
    local = torch.arange(E, device=device) + rank * E
    expert_out = (expert_in * _expert_scale(local).view(-1, 1, 1)).to(torch.bfloat16)
    combined, event, hook = buf.combine(
        expert_out,
        idx,
        w,
        handle,
        async_finish=not hook_mode,
        return_recv_hook=hook_mode,
    )
    hook() if hook_mode else event.current_stream_wait()

    src = _fp8_reference(x)[2] if use_fp8 else x.float()
    ref = torch.zeros((n, hidden), device=device)
    for k in range(args.num_topk):
        e = idx[:, k]
        y = (src * _expert_scale(e.clamp(min=0)).view(-1, 1)).to(torch.bfloat16).float()
        ref += torch.where((e >= 0).view(-1, 1), y * w[:, k : k + 1], 0.0)
    torch.testing.assert_close(combined.float(), ref, rtol=2e-2, atol=2e-2, msg=tag)


def run_rounds(buf, rank, members, group, args, tag):
    for i in range(args.rounds):
        run_round(
            buf, rank, members, group, args, bool(i % 2), bool((i // 2) % 2),
            f"{tag} round {i}",
        )


def mask_of(buf):
    out = torch.empty(buf.group_size, dtype=torch.int32, device=buf._active_ranks.device)
    buf.query_mask_buffer(out)
    return out.tolist()


def worker(rank, world, args, port):
    try:
        _worker(rank, world, args, port)
    except BaseException:
        # A peer still inside a receive kernel would block mp.spawn's join.
        import traceback

        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)


def _worker(rank, world, args, port):
    torch.xpu.set_device(_device_for(rank))
    store = dist.TCPStore(
        "127.0.0.1", port, world, rank == 0, timeout=timedelta(seconds=300)
    )
    dist.init_process_group(
        "gloo", store=dist.PrefixStore("pg", store), rank=rank, world_size=world
    )
    everyone = list(range(world))
    random.seed(1234 + rank)
    torch.manual_seed(1234 + rank)

    from mooncake.mooncake_ep_buffer import Buffer

    cap = args.capacity or world + 1
    assert cap >= world
    E = args.experts_per_rank
    size = Buffer.get_rdma_size_hint(args.max_tokens, args.hidden, cap, cap * E)

    def make():
        b = Buffer.create_elastic(
            rank=rank,
            tcp_store_group=store,
            explicitly_destroy=True,
            timeout_ms=args.timeout_ms,
        )
        b.update_memory_buffers(cap, E, size)
        return b

    def say(msg):
        dist.barrier()
        if rank == 0:
            print(msg, flush=True)

    # 1. Connect everyone; the slots past `world` stay empty and masked.
    buf = make()
    assert buf.runtime._get_active_rank_bound() == rank + 1
    buf.connect_ranks([r for r in everyone if r != rank])
    assert buf.runtime._get_active_rank_bound() == world
    assert mask_of(buf) == [0] * world + [1] * (cap - world), mask_of(buf)
    try:
        buf.connect_ranks([(rank + 1) % world])
        raise AssertionError("connecting a connected slot must fail")
    except ValueError as e:
        assert "already connected" in str(e), e
    try:
        buf.barrier()
        raise AssertionError("barrier() must be unsupported on XPU")
    except NotImplementedError:
        pass
    run_rounds(buf, rank, everyone, None, args, "all ranks")
    say(f"all {world} ranks (capacity {cap}): {args.rounds} rounds ok")

    # 2. Mask rank m while it stays alive, run without it, then unmask it.
    # Its peers must not leave flags in its buffer meanwhile, or its first
    # round back would read them as already sent.
    m = 1 if world > 2 else world - 1
    rest = [r for r in everyone if r != m]
    rest_group = dist.new_group(rest)
    if rank != m:
        buf.update_mask_buffer(m, True)
        assert mask_of(buf)[m] == 1
        assert buf.runtime._get_active_rank_bound() == max(rest) + 1
        run_rounds(buf, rank, rest, rest_group, args, f"rank {m} masked")
    dist.barrier()
    if rank != m:
        buf.update_mask_buffer(m, False)
        assert mask_of(buf)[m] == 0
    run_rounds(buf, rank, everyone, None, args, f"rank {m} unmasked")
    say(f"rank {m} masked, then unmasked: ok")

    # 3. Replace the last rank with a new buffer, as a restarted process.
    last = world - 1
    if rank == last:
        dist.barrier()  # peers have disconnected the old buffer
        buf.destroy()
        buf = make()
        buf.connect_ranks([r for r in everyone if r != rank])
    else:
        buf.disconnect_ranks([last])
        assert mask_of(buf)[last] == 1
        assert buf.runtime._get_active_rank_bound() == last
        dist.barrier()
        buf.connect_ranks([last])
    assert buf.runtime._get_active_rank_bound() == world
    assert mask_of(buf) == [0] * world + [1] * (cap - world)
    run_rounds(buf, rank, everyone, None, args, f"rank {last} replaced")
    say(f"rank {last} replaced: ok")

    # 4. clean_mask_buffer reactivates every connected rank.
    if rank != last:
        buf.update_mask_buffer(last, True)
    buf.clean_mask_buffer()
    assert mask_of(buf) == [0] * world + [1] * (cap - world)
    run_rounds(buf, rank, everyone, None, args, "after clean_mask_buffer")
    say("clean_mask_buffer: ok")

    # 5. Zero the whole buffer between rounds, as vLLM's clean_buffers does.
    torch.xpu.synchronize()
    dist.barrier()
    view = buf.get_local_buffer_tensor(dtype=torch.int8, use_rdma_buffer=True)
    assert view.numel() == size and view.device.type == "xpu"
    base = buf.runtime._get_local_buffer_descriptor()[0]
    assert view.data_ptr() % (1 << 64) == base, (hex(view.data_ptr()), hex(base))
    assert bool(view.ne(0).any()), "the rounds left no data in the buffer view"
    try:
        buf.get_local_buffer_tensor(dtype=torch.int8, use_rdma_buffer=False)
        raise AssertionError("use_rdma_buffer=False must be rejected")
    except ValueError:
        pass
    view.zero_()
    torch.xpu.synchronize()
    dist.barrier()
    buf.clean_mask_buffer()
    run_rounds(buf, rank, everyone, None, args, "after zeroing")
    say("zeroed buffer: ok")

    # 6. Scale down by the last rank, then scale back up as vLLM's elastic
    # EP does: peers switch to a fresh TCPStore, connect the new rank staged
    # (activate=False), keep running without it, and only then unmask it.
    kept = everyone[:-1]
    kept_group = dist.new_group(kept)
    torch.xpu.synchronize()
    dist.barrier()
    if rank == last:
        dist.barrier()  # peers have disconnected this buffer
        buf.destroy()
    else:
        buf.disconnect_ranks([last])
        assert buf.runtime._get_active_rank_bound() == last
        dist.barrier()
        run_rounds(buf, rank, kept, kept_group, args, f"scaled down to {last}")
    say(f"scaled down to {last} ranks: ok")

    scale_store = dist.PrefixStore("scale_up", store)
    if rank == last:
        buf = Buffer.create_elastic(
            rank=rank,
            tcp_store_group=scale_store,
            explicitly_destroy=True,
            timeout_ms=args.timeout_ms,
        )
        buf.update_memory_buffers(cap, E, size)
        buf.connect_ranks([r for r in everyone if r != rank])
    else:
        buf.set_tcp_store_group(None)
        try:
            buf.connect_ranks([last], activate=False)
            raise AssertionError("connect_ranks without a TCPStore must fail")
        except RuntimeError as e:
            assert "TCPStore" in str(e), e
        buf.set_tcp_store_group(scale_store)
        buf.connect_ranks([last], activate=False)
        assert mask_of(buf)[last] == 1
        assert buf.runtime._get_active_rank_bound() == last
        try:
            buf.connect_ranks([last], activate=False)
            raise AssertionError("connecting a staged slot must fail")
        except ValueError as e:
            assert "already connected" in str(e), e
        run_rounds(buf, rank, kept, kept_group, args, f"rank {last} staged")
    dist.barrier()
    if rank != last:
        buf.update_mask_buffer(last, False)
    assert buf.runtime._get_active_rank_bound() == world
    assert mask_of(buf) == [0] * world + [1] * (cap - world)
    run_rounds(buf, rank, everyone, None, args, f"rank {last} scaled up")
    say(f"staged scale-up back to {world} ranks: ok")

    torch.xpu.synchronize()
    dist.barrier()
    buf.destroy()
    dist.barrier()
    if rank == 0:
        print(f"PASSED: elastic EP on {world} ranks, capacity {cap}", flush=True)
    dist.destroy_process_group()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-ranks", type=int, default=3)
    p.add_argument("--capacity", type=int, default=0, help="default: num-ranks + 1")
    p.add_argument("--experts-per-rank", type=int, default=4)
    p.add_argument("--num-topk", type=int, default=4)
    p.add_argument("--max-tokens", type=int, default=64)
    p.add_argument("--hidden", type=int, default=2048)
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--timeout-ms", type=int, default=30000)
    p.add_argument("--port", type=int, default=0)
    args = p.parse_args()
    port = args.port or random.randint(20000, 40000)
    mp.spawn(worker, args=(args.num_ranks, args, port), nprocs=args.num_ranks)


if __name__ == "__main__":
    sys.exit(main())
