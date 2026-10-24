"""Correctness test for the native Mooncake EP path on Intel XPU.

Every round draws fresh random tokens and routing, so a receiver that reads a
stale line from a previous round fails the exact comparison below.

    python python/tests/ep/test_mooncake_ep_xpu.py --num-ranks 4

MOONCAKE_EP_TEST_DEVICES (e.g. "0,1,2,3") picks the XPU of each rank.
"""

import argparse
import os
import random
import sys

import torch
import torch.distributed as dist
import torch.multiprocessing as mp


_CPU_GROUP = None


def _device_for(local_rank: int) -> int:
    devices = os.environ.get("MOONCAKE_EP_TEST_DEVICES")
    if devices:
        return int(devices.split(",")[local_rank])
    return local_rank


def _route(num_tokens, num_experts, num_topk, num_masked, device):
    scores = torch.rand((num_tokens, num_experts), device=device)
    topk_idx = torch.topk(scores, num_topk, dim=-1).indices
    for _ in range(min(num_masked, num_tokens * num_topk)):
        topk_idx[random.randrange(num_tokens), random.randrange(num_topk)] = -1
    return topk_idx


def _gather(t, group, world):
    out = [torch.empty_like(t) for _ in range(world)]
    dist.all_gather(out, t.contiguous(), group=group)
    return out


def _fp8_reference(x: torch.Tensor):
    """Per-128-channel E4M3 cast with the CUDA kernel's arithmetic.

    Returns (codes, scale_inv, dequantized), computed on the CPU: the scale is
    a true division, 448 / amax, as in the kernel. (`448.0 / t` in torch is
    t.reciprocal() * 448, which rounds differently.)
    """
    xf = x.float().cpu().view(x.size(0), x.size(1) // 128, 128)
    amax = xf.abs().amax(dim=-1, keepdim=True).clamp(min=1e-4)
    scale = torch.full_like(amax, 448.0) / amax
    codes = (xf * scale).to(torch.float8_e4m3fn)
    scale_inv = amax * (1.0 / 448.0)
    deq = (codes.float() * scale_inv).view(x.shape)
    return codes.view(x.shape), scale_inv.squeeze(-1), deq.to(x.device)


def _expert_scale(expert: torch.Tensor) -> torch.Tensor:
    # A distinct factor per expert, so a token combined from the wrong expert
    # (or the wrong slot) produces the wrong value.
    return 1.0 + (expert.float() + 1.0) / 64.0


def check_round(
    buffer, group, rank, world, args, num_tokens, use_fp8, hook_mode, with_weights
):
    """with_weights passes the top-k weights to dispatch, so combine reduces on
    the expert side; combine then gets zero weights, which it must not read."""
    device = torch.device("xpu", torch.xpu.current_device())
    hidden, num_experts, num_topk = args.hidden, args.num_experts, args.num_topk
    max_tokens = args.max_tokens
    num_local = num_experts // world

    x = torch.randn((num_tokens, hidden), device=device).to(torch.bfloat16)
    topk_idx = _route(num_tokens, num_experts, num_topk, args.num_masked, device)
    topk_weights = torch.rand((num_tokens, num_topk), device=device)
    active = torch.ones((world,), dtype=torch.int32, device=device)

    recv, recv_count, handle, event, hook = buffer.dispatch(
        x,
        topk_idx,
        active,
        max_tokens,
        num_experts,
        args.timeout_us,
        use_fp8=use_fp8,
        async_finish=not hook_mode,
        return_recv_hook=hook_mode,
        topk_weights=topk_weights if with_weights else None,
    )
    hook() if hook_mode else event.current_stream_wait()
    recv_x, recv_scales = recv if use_fp8 else (recv, None)

    # All ranks' inputs, padded to max_tokens so all_gather sees one shape.
    pad_x = torch.zeros((max_tokens, hidden), dtype=x.dtype, device=device)
    pad_x[:num_tokens] = x
    pad_idx = torch.full((max_tokens, num_topk), -1, dtype=torch.int64, device=device)
    pad_idx[:num_tokens] = topk_idx
    counts = torch.tensor([num_tokens], device=device)
    all_x = _gather(pad_x, group, world)
    all_idx = _gather(pad_idx, group, world)
    all_counts = [int(c) for c in _gather(counts, group, world)]

    # Verify on CPU copies: one transfer per tensor instead of a sync per check.
    src_info, layout_range = handle[0].cpu(), handle[1].cpu()
    counts_cpu = recv_count.cpu()
    got_x = recv_x.cpu()
    got_scales = recv_scales.cpu() if use_fp8 else None
    all_x = [t.cpu() for t in all_x]
    all_idx = [t[:c].cpu() for t, c in zip(all_idx, all_counts)]
    for le in range(num_local):
        expert = rank * num_local + le
        n = int(counts_cpu[le])
        expected = sum(int((all_idx[r] == expert).sum()) for r in range(world))
        assert n == expected, f"expert {expert}: received {n}, expected {expected}"
        for r in range(world):
            cnt = int(layout_range[le, r]) & 0xFFFFFFFF
            begin = int(layout_range[le, r]) >> 32
            want = int((all_idx[r] == expert).sum())
            assert cnt == want, f"expert {expert} from rank {r}: {cnt} != {want}"
            if cnt == 0:
                continue
            tok = src_info[le, begin : begin + cnt].long()
            assert bool((tok >= 0).all() and (tok < all_counts[r]).all())
            assert bool(
                (all_idx[r][tok] == expert).any(dim=-1).all()
            ), "wrong source token"
            # Each source token appears once per matching top-k slot.
            assert int((all_idx[r][tok] == expert).sum()) >= cnt
            if use_fp8:
                codes, scale_inv, _ = _fp8_reference(all_x[r][tok])
                got = got_x[le, begin : begin + cnt]
                assert torch.equal(
                    got.view(torch.uint8), codes.view(torch.uint8)
                ), f"expert {expert}: FP8 payload from rank {r} differs"
                assert torch.equal(
                    got_scales[le, begin : begin + cnt], scale_inv
                ), f"expert {expert}: FP8 scales from rank {r} differ"
            else:
                assert torch.equal(
                    got_x[le, begin : begin + cnt], all_x[r][tok]
                ), f"expert {expert}: payload from rank {r} differs"

    # Expert e's "GEMM" scales its tokens by a distinct factor.
    if use_fp8:
        dq = recv_x.float().view(
            num_local, -1, hidden // 128, 128
        ) * recv_scales.unsqueeze(-1)
        expert_in = dq.view(num_local, -1, hidden)
    else:
        expert_in = recv_x.float()
    local_experts = torch.arange(num_local, device=device) + rank * num_local
    expert_out = (expert_in * _expert_scale(local_experts).view(-1, 1, 1)).to(
        torch.bfloat16
    )

    out = torch.empty((num_tokens, hidden), dtype=torch.bfloat16, device=device)
    combined, event, hook = buffer.combine(
        expert_out,
        topk_idx,
        torch.zeros_like(topk_weights) if with_weights else topk_weights,
        active,
        args.timeout_us,
        handle,
        async_finish=not hook_mode,
        return_recv_hook=hook_mode,
        out=out,
    )
    hook() if hook_mode else event.current_stream_wait()
    assert combined.data_ptr() == out.data_ptr()

    # Reference: what each expert returned for this token, weighted.
    src = _fp8_reference(x)[2] if use_fp8 else x.float()
    ref = torch.zeros((num_tokens, hidden), device=device)
    for k in range(num_topk):
        e = topk_idx[:, k]
        valid = e >= 0
        y = (src * _expert_scale(e.clamp(min=0)).view(-1, 1)).to(torch.bfloat16).float()
        ref += torch.where(valid.view(-1, 1), y * topk_weights[:, k : k + 1], 0.0)
    try:
        torch.testing.assert_close(combined.float(), ref, rtol=2e-2, atol=2e-2)
    except AssertionError:
        _diagnose_combine(
            combined, ref, src, topk_idx, topk_weights, active, num_local, world, rank
        )
        raise


def _diagnose_combine(
    combined, ref, src, topk_idx, topk_weights, active, num_local, world, rank
):
    bad = ~torch.isclose(combined.float(), ref, rtol=2e-2, atol=2e-2)
    rows = bad.any(-1).nonzero().flatten().tolist()
    print(
        f"[rank {rank}] combine mismatch: active={active.tolist()} bad_rows={len(rows)}/{combined.size(0)} "
        f"first={rows[:8]}",
        flush=True,
    )
    for r in range(world):
        # Reference without rank r's experts: matches if r was dropped.
        part = torch.zeros_like(ref)
        for k in range(topk_idx.size(1)):
            e = topk_idx[:, k]
            keep = (e >= 0) & (e // num_local != r)
            y = (
                (src * _expert_scale(e.clamp(min=0)).view(-1, 1))
                .to(torch.bfloat16)
                .float()
            )
            part += torch.where(keep.view(-1, 1), y * topk_weights[:, k : k + 1], 0.0)
        ok = torch.isclose(combined.float(), part, rtol=2e-2, atol=2e-2).all(-1)
        print(
            f"[rank {rank}]   rows matching 'rank {r} dropped': {int(ok[rows].sum()) if rows else 0}",
            flush=True,
        )
    if rows:
        t = rows[0]
        print(
            f"[rank {rank}]   row {t}: topk={topk_idx[t].tolist()} combined[:4]={combined[t,:4].tolist()} "
            f"ref[:4]={ref[t,:4].tolist()}",
            flush=True,
        )


def check_two_batch_overlap(buffer, group, rank, world, args):
    """Two micro-batches' results held at once, as TBO/DBO interleave them.

    As with the CUDA kernels, each call's receive hook must run before the
    next call is issued: a call cleans the other buffer half, which is where
    the previous call's peers wrote. Combining in reverse order checks that
    each combine reduces with its own dispatch's top-k snapshot.
    """
    device = torch.device("xpu", torch.xpu.current_device())
    hidden, num_experts, num_topk = args.hidden, args.num_experts, args.num_topk
    active = torch.ones((world,), dtype=torch.int32, device=device)
    batches = []
    # One micro-batch reduces on the expert side, the other does not.
    for with_weights in (True, False):
        n = random.randint(1, args.max_tokens)
        x = torch.randn((n, hidden), device=device).to(torch.bfloat16)
        idx = _route(n, num_experts, num_topk, 0, device)
        w = torch.rand((n, num_topk), device=device)
        recv, _, handle, _, hook = buffer.dispatch(
            x,
            idx,
            active,
            args.max_tokens,
            num_experts,
            args.timeout_us,
            use_fp8=False,
            return_recv_hook=True,
            topk_weights=w if with_weights else None,
        )
        hook()
        batches.append((x, idx, w, recv, handle, with_weights))
    for x, idx, w, recv, handle, with_weights in reversed(batches):
        # Pass a different top-k tensor: combine must use the dispatch's.
        combined, _, hook = buffer.combine(
            recv,
            torch.zeros_like(idx),
            torch.zeros_like(w) if with_weights else w,
            active,
            args.timeout_us,
            handle,
            return_recv_hook=True,
        )
        hook()
        # Identity experts: combine must return x weighted by the valid slots.
        ref = x.float() * torch.where(idx >= 0, w, 0.0).sum(-1, keepdim=True)
        torch.testing.assert_close(combined.float(), ref, rtol=2e-2, atol=2e-2)


def check_duplicate_experts(buffer, group, rank, world, args, with_weights):
    """Every token names expert 0 in all its top-k slots, so more than T picks
    target one expert. The excess is dropped (as no CUDA buffer has room for
    it), never written past the expert's region, and the buffer stays usable."""
    device = torch.device("xpu", torch.xpu.current_device())
    n, hidden, num_topk = args.max_tokens, args.hidden, args.num_topk
    x = torch.randn((n, hidden), device=device).to(torch.bfloat16)
    idx = torch.zeros((n, num_topk), dtype=torch.int64, device=device)
    w = torch.rand((n, num_topk), device=device)
    active = torch.ones((world,), dtype=torch.int32, device=device)
    recv, cnt, handle, _, _ = buffer.dispatch(
        x,
        idx,
        active,
        args.max_tokens,
        args.num_experts,
        args.timeout_us,
        use_fp8=False,
        topk_weights=w if with_weights else None,
    )
    combined, _, _ = buffer.combine(recv, idx, w, active, args.timeout_us, handle)
    torch.xpu.synchronize()
    want = world * args.max_tokens if rank == 0 else 0
    assert int(cnt[0]) == want, f"expert 0 received {int(cnt[0])}, want {want}"
    assert bool(torch.isfinite(combined.float()).all())


def _identity_round(buffer, world, args, n, timeout_us=None):
    """Inputs for a round whose experts return their input unchanged."""
    device = torch.device("xpu", torch.xpu.current_device())
    x = torch.randn((n, args.hidden), device=device).to(torch.bfloat16)
    idx = _route(n, args.num_experts, args.num_topk, 0, device)
    w = torch.rand((n, args.num_topk), device=device)
    active = torch.ones((world,), dtype=torch.int32, device=device)
    return x, idx, w, active, timeout_us or args.timeout_us


def _dispatch(buffer, args, rnd, with_weights=False):
    x, idx, w, active, timeout_us = rnd
    recv, _, handle, _, _ = buffer.dispatch(
        x,
        idx,
        active,
        args.max_tokens,
        args.num_experts,
        timeout_us,
        use_fp8=False,
        topk_weights=w if with_weights else None,
    )
    return recv, handle


def _check_identity_combine(rnd, combined, what):
    x, _, w, active, _ = rnd
    torch.xpu.synchronize()
    assert bool(active.all()), f"{what}: active_ranks={active.tolist()}"
    ref = x.float() * w.sum(-1, keepdim=True)
    torch.testing.assert_close(
        combined.float(), ref, rtol=2e-2, atol=2e-2, msg=lambda m: f"{what}: {m}"
    )


def check_reused_src_info(buffer, group, rank, world, args):
    """The caching allocator can give a dispatch the src_info block of an
    earlier, finished one. That dispatch must still get a top-k snapshot of
    its own, which a second dispatch held at the same time must not take."""
    n = args.max_tokens
    # Go once around the ring of snapshots, each dispatch with a src_info of
    # its own (kept alive, so the allocator cannot hand it out again).
    handles = []
    for i in range(8):
        rnd = _identity_round(buffer, world, args, n)
        recv, handle = _dispatch(buffer, args, rnd)
        combined, _, _ = buffer.combine(recv, rnd[1], rnd[2], rnd[3], rnd[4], handle)
        _check_identity_combine(rnd, combined, f"ring round {i}")
        handles.append(handle)
    first = handles[0][0]
    assert len({h[0].data_ptr() for h in handles}) == len(handles)
    real_empty = torch.empty

    def empty_reusing_first(*size, **kw):
        shape = size[0] if len(size) == 1 and not isinstance(size[0], int) else size
        if kw.get("dtype") == torch.int32 and tuple(shape) == tuple(first.shape):
            return first
        return real_empty(*size, **kw)

    a = _identity_round(buffer, world, args, n)
    torch.empty = empty_reusing_first
    try:
        recv_a, handle_a = _dispatch(buffer, args, a, with_weights=True)
    finally:
        torch.empty = real_empty
    assert handle_a[0].data_ptr() == first.data_ptr(), "src_info was not reused"
    b = _identity_round(buffer, world, args, n)
    recv_b, handle_b = _dispatch(buffer, args, b)
    out_a, _, _ = buffer.combine(recv_a, a[1], a[2], a[3], a[4], handle_a)
    out_b, _, _ = buffer.combine(recv_b, b[1], b[2], b[3], b[4], handle_b)
    _check_identity_combine(a, out_a, "dispatch with a reused src_info")
    _check_identity_combine(b, out_b, "dispatch held alongside it")


def check_unknown_handle(buffer, group, rank, world, args):
    """Combine finds its dispatch by the handle's src_info tensor; a handle
    with a copy of it must be rejected, not reduced with another's routing."""
    rnd = _identity_round(buffer, world, args, args.max_tokens)
    recv, handle = _dispatch(buffer, args, rnd)
    # One element into a fresh block: no allocation starts there, so unlike a
    # clone the copy cannot land on an earlier dispatch's freed src_info.
    storage = torch.empty(
        handle[0].numel() + 1, dtype=handle[0].dtype, device=handle[0].device
    )
    copy = storage[1:].view_as(handle[0]).copy_(handle[0])
    copied = (copy,) + tuple(handle[1:])
    try:
        buffer.combine(recv, rnd[1], rnd[2], rnd[3], rnd[4], copied)
    except RuntimeError as e:
        assert "src_info" in str(e), e
    else:
        raise AssertionError("combine accepted a handle from no dispatch")
    combined, _, _ = buffer.combine(recv, rnd[1], rnd[2], rnd[3], rnd[4], handle)
    _check_identity_combine(rnd, combined, "combine after a rejected handle")


def check_cross_stream(buffer, group, rank, world, args):
    """Rounds issued back to back on two streams, with nothing ordering the
    streams, must run in issue order as on one stream (CUDA runs them all on
    its comm stream)."""
    streams = [torch.xpu.Stream(), torch.xpu.Stream()]
    rounds = [
        _identity_round(buffer, world, args, args.max_tokens, 5_000_000)
        for _ in range(6)
    ]
    for s in streams:
        s.wait_stream(torch.xpu.current_stream())
    outs = []
    for i, rnd in enumerate(rounds):
        with torch.xpu.stream(streams[i % 2]):
            recv, handle = _dispatch(buffer, args, rnd, with_weights=bool(i % 2))
            out, _, _ = buffer.combine(recv, rnd[1], rnd[2], rnd[3], rnd[4], handle)
            outs.append(out)
    for s in streams:
        torch.xpu.current_stream().wait_stream(s)
    for i, (rnd, out) in enumerate(zip(rounds, outs)):
        _check_identity_combine(rnd, out, f"cross-stream round {i}")


class _NoPeers:
    """A runtime that could not map its peers."""

    def __init__(self, runtime):
        self._runtime = runtime

    def use_fast_path(self):
        return False

    def __getattr__(self, name):
        return getattr(self._runtime, name)


def check_fast_path_agreement(buffer, group, rank, world, args):
    """If one rank cannot reach its peers, every rank must fall back: a rank
    in the native kernels would wait forever on one in the fallback."""
    runtime = buffer.runtime
    if rank == world - 1:
        buffer.runtime = _NoPeers(runtime)
    try:
        buffer.connect()
    finally:
        buffer.runtime = runtime
    flags = [None] * world
    dist.all_gather_object(flags, buffer._use_fallback, group=_CPU_GROUP)
    assert all(flags), f"_use_fallback per rank: {flags}"
    buffer.connect()
    assert not buffer._use_fallback, "the fast path did not come back"


def check_dead_rank(buffer, group, rank, world, args):
    """The last rank skips a dispatch/combine. The others must time out on it,
    drop it from active_ranks, and finish from the remaining ranks, as the
    CUDA path does. Leaves the buffer unusable, so it runs last."""
    device = torch.device("xpu", torch.xpu.current_device())
    hidden, num_experts, num_topk = args.hidden, args.num_experts, args.num_topk
    num_local = num_experts // world
    dead = world - 1
    dist.barrier(_CPU_GROUP)
    if rank == dead:
        return
    n = args.max_tokens // 2
    x = torch.randn((n, hidden), device=device).to(torch.bfloat16)
    idx = _route(n, num_experts, num_topk, 0, device)
    w = torch.rand((n, num_topk), device=device)
    active = torch.ones((world,), dtype=torch.int32, device=device)
    timeout_us = 2_000_000
    recv, cnt, handle, _, _ = buffer.dispatch(
        x,
        idx,
        active,
        args.max_tokens,
        num_experts,
        timeout_us,
        use_fp8=False,
        topk_weights=w if args.dead_rank_weights else None,
    )
    combined, _, _ = buffer.combine(recv, idx, w, active, timeout_us, handle)
    torch.xpu.synchronize()
    assert active.tolist() == [1] * dead + [0], f"active_ranks={active.tolist()}"
    # Nothing came from the dead rank.
    assert int(handle[1][:, dead].cpu().bitwise_and(0xFFFFFFFF).sum()) == 0
    # Identity experts: the dead rank's experts contribute nothing.
    live = (idx >= 0) & (idx // num_local != dead)
    ref = x.float() * torch.where(live, w, 0.0).sum(-1, keepdim=True)
    torch.testing.assert_close(combined.float(), ref, rtol=2e-2, atol=2e-2)


def worker(local_rank, world, args, port):
    try:
        _worker(local_rank, world, args, port)
    except BaseException:
        # A peer still inside a receive kernel would block mp.spawn's join, so
        # report the failure here rather than after it.
        import traceback

        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)


def _worker(local_rank, world, args, port):
    torch.xpu.set_device(_device_for(local_rank))
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ["MASTER_PORT"] = str(port)
    dist.init_process_group("xccl", rank=local_rank, world_size=world)
    group = dist.new_group(list(range(world)))
    global _CPU_GROUP
    _CPU_GROUP = dist.new_group(list(range(world)), backend="gloo")
    random.seed(1234 + local_rank)
    torch.manual_seed(1234 + local_rank)

    from mooncake.mooncake_ep_buffer import Buffer

    size = Buffer.get_ep_buffer_size_hint(
        args.max_tokens, args.hidden, world, args.num_experts
    )
    buffer = Buffer(group, size)
    assert not buffer._use_fallback, "native XPU fast path was not selected"

    token_counts = [0, 1, args.max_tokens] + [
        random.randint(1, args.max_tokens) for _ in range(args.rounds)
    ]
    for i, n in enumerate(token_counts):
        # All ranks must agree on the dtype/hook mode; vary per round.
        use_fp8 = bool(i % 2)
        hook_mode = bool((i // 2) % 2)
        with_weights = bool((i // 4) % 2)
        check_round(
            buffer, group, local_rank, world, args, n, use_fp8, hook_mode, with_weights
        )
        if local_rank == 0:
            print(
                f"round {i}: tokens={n} fp8={use_fp8} hook={hook_mode} "
                f"weights_at_dispatch={with_weights} ok",
                flush=True,
            )
    # A rank with no tokens passes an empty weights tensor; it must still take
    # part in the pre-reduced combine like its peers. A few tokens elsewhere
    # suffice and keep the CPU reference check fast at 8 ranks.
    for empty_rank, use_fp8, hook_mode in ((0, False, False), (world - 1, True, True)):
        n = 0 if local_rank == empty_rank else max(1, args.max_tokens // 8)
        check_round(buffer, group, local_rank, world, args, n, use_fp8, hook_mode, True)
    for _ in range(4):
        check_two_batch_overlap(buffer, group, local_rank, world, args)
    check_duplicate_experts(buffer, group, local_rank, world, args, False)
    check_duplicate_experts(buffer, group, local_rank, world, args, True)
    check_reused_src_info(buffer, group, local_rank, world, args)
    check_unknown_handle(buffer, group, local_rank, world, args)
    check_cross_stream(buffer, group, local_rank, world, args)
    check_fast_path_agreement(buffer, group, local_rank, world, args)
    check_round(
        buffer, group, local_rank, world, args, args.max_tokens, False, False, True
    )
    if world > 2:
        check_dead_rank(buffer, group, local_rank, world, args)
    # Free the buffer only once no peer can still be reading from it.
    torch.xpu.synchronize()
    dist.barrier(_CPU_GROUP)
    del buffer
    torch.xpu.synchronize()
    dist.barrier(_CPU_GROUP)
    if local_rank == 0:
        print(
            f"PASSED: {len(token_counts)} rounds + 2 empty-rank rounds"
            " + 4 two-batch rounds + duplicate experts"
            " + reused src_info + rejected handle + cross-stream + fallback agreement"
            f"{' + dead-rank timeout' if world > 2 else ''} on {world} ranks "
            f"(hidden={args.hidden}, experts={args.num_experts}, topk={args.num_topk})",
            flush=True,
        )
    dist.destroy_process_group()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-ranks", type=int, default=2)
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--hidden", type=int, default=7168)
    p.add_argument("--num-experts", type=int, default=64)
    p.add_argument("--num-topk", type=int, default=8)
    p.add_argument("--num-masked", type=int, default=10)
    p.add_argument("--rounds", type=int, default=12)
    p.add_argument("--timeout-us", type=int, default=30_000_000)
    p.add_argument(
        "--dead-rank-weights",
        action="store_true",
        help="give the dead-rank check's dispatch its top-k weights",
    )
    args = p.parse_args()
    port = 29500 + random.randint(0, 1000)
    mp.spawn(worker, args=(args.num_ranks, args, port), nprocs=args.num_ranks)


if __name__ == "__main__":
    sys.exit(main())
