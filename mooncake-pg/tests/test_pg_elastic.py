import os
import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from mooncake import pg
from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
    configure_mooncake_device_filter,
    get_mooncake_backend,
    require_test_device,
    wait_until,
)


BROKEN_RANK = 1


def _extension_worker(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
) -> None:
    """Worker for testing extension mode - new ranks join existing group."""
    initial_world_size = ctx.world_size - 1
    extension_rank = ctx.world_size - 1

    join_ranks = [extension_rank]

    if ctx.proc_rank < initial_world_size:
        # Original ranks
        device = ctx.init_group(
            world_size=initial_world_size,
            max_group_size=ctx.world_size,
        )
        backend = ctx.get_backend()

        # First collective
        tensor = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        baseline = int(tensor.cpu().item())

        # Signal ready and extend
        if ctx.proc_rank == 0:
            extend_event.set()

        # Two-phase extension protocol:
        #   1) joiner publishes metadata + establishes transport readiness
        #   2) healthy ranks recover/activate it via recover_ranks()
        wait_until(
            lambda: all(pg.get_peer_state(backend, join_ranks)),
            timeout_s=10.0,
            poll_interval_s=0.01,
            description=f"rank {ctx.proc_rank} waiting for joiner ready",
        )
        resp = pg.recover_ranks(backend, join_ranks)
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {ctx.proc_rank}: recover_ranks should apply, got {resp.status}"

        # Final collective
        final_tensor = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(final_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({
            "role": "original",
            "rank": ctx.proc_rank,
            "baseline": baseline,
        })
    else:
        # Extension rank
        if not extend_event.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for extend_event")

        device = ctx.init_group(
            rank=extension_rank,
            world_size=ctx.world_size,
            max_group_size=ctx.world_size,
        )

        backend = ctx.get_backend()

        # Before join_group, the joining backend executes collectives with an
        # effective {self} mask and does not involve the existing ranks.
        local_tensor = torch.tensor(
            [extension_rank + 1], dtype=torch.int32, device=device
        )
        dist.all_reduce(local_tensor, op=dist.ReduceOp.SUM)
        local_value = int(local_tensor.cpu().item())
        if local_value != extension_rank + 1:
            raise AssertionError(
                f"extension rank expected local-only sum "
                f"{extension_rank + 1}, got {local_value}"
            )

        pg.join_group(backend)

        # Final collective
        final_tensor = torch.tensor([extension_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(final_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({
            "role": "extension",
            "rank": extension_rank,
        })


def _extension_p2p_worker(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
    direction: str,
) -> None:
    """Worker for testing P2P after max_group_size/recover_ranks scale-up."""
    initial_world_size = ctx.world_size - 1
    extension_rank = ctx.world_size - 1
    primary_peer = initial_world_size - 1
    join_ranks = [extension_rank]

    if initial_world_size < 1:
        raise AssertionError("elastic P2P test expects at least one primary rank")

    if ctx.proc_rank < initial_world_size:
        device = ctx.init_group(
            world_size=initial_world_size,
            max_group_size=ctx.world_size,
        )
        backend = ctx.get_backend()

        if ctx.proc_rank == 0:
            extend_event.set()

        wait_until(
            lambda: all(pg.get_peer_state(backend, join_ranks)),
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for joiner ready",
        )
        resp = pg.recover_ranks(backend, join_ranks)
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {ctx.proc_rank}: recover_ranks should apply, got {resp.status}"
    else:
        if not extend_event.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for extend_event")

        device = ctx.init_group(
            rank=extension_rank,
            world_size=ctx.world_size,
            max_group_size=ctx.world_size,
        )
        backend = ctx.get_backend()
        pg.join_group(backend)

    # Prove elastic collectives are functional before isolating P2P.
    collective = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(collective, op=dist.ReduceOp.SUM)
    expected_sum = ctx.world_size * (ctx.world_size + 1) // 2
    if int(collective.cpu().item()) != expected_sum:
        raise AssertionError(
            f"rank {ctx.proc_rank}: post-recovery all_reduce expected "
            f"{expected_sum}, got {int(collective.cpu().item())}"
        )

    dist.barrier()

    if direction == "joiner_to_primary":
        src_rank = extension_rank
        dst_rank = primary_peer
    elif direction == "primary_to_joiner":
        src_rank = primary_peer
        dst_rank = extension_rank
    else:
        raise AssertionError(f"unknown P2P direction: {direction}")

    numel = 1024
    if ctx.proc_rank == src_rank:
        send_tensor = torch.full(
            (numel,), src_rank, dtype=torch.int32, device=device
        )
        works = dist.batch_isend_irecv(
            [dist.P2POp(op=dist.isend, tensor=send_tensor, peer=dst_rank)]
        )
        for work in works:
            work.wait()
        ctx.synchronize()
        value = "sent"
    elif ctx.proc_rank == dst_rank:
        recv_tensor = torch.empty((numel,), dtype=torch.int32, device=device)
        works = dist.batch_isend_irecv(
            [dist.P2POp(op=dist.irecv, tensor=recv_tensor, peer=src_rank)]
        )
        for work in works:
            work.wait()
        ctx.synchronize()
        expected = torch.full_like(recv_tensor, src_rank)
        if not torch.equal(recv_tensor.cpu(), expected.cpu()):
            raise AssertionError(
                f"rank {ctx.proc_rank}: received unexpected P2P payload "
                f"from rank {src_rank}"
            )
        value = "received"
    else:
        value = "idle"

    dist.barrier()
    ctx.record_result(
        {
            "direction": direction,
            "role": (
                "src"
                if ctx.proc_rank == src_rank
                else "dst"
                if ctx.proc_rank == dst_rank
                else "idle"
            ),
            "value": value,
        }
    )


def _extension_worker_with_subgroups(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
) -> None:
    """Multi-subgroup elastic extension test using split-ranks pattern.

    Layout (world_size=4, primary=[0,1], joiners=[2,3]):
      group_a: primary ranks=[0],   extended ranks=[0,2],     max_group_size=2
      group_b: primary ranks=[1],   extended ranks=[1,3],     max_group_size=2
      group_c: primary ranks=[0,1], extended ranks=[0,1,2,3], max_group_size=4
    """
    configure_mooncake_device_filter(ctx.device_filters)
    device = require_test_device(ctx.proc_rank, ctx.device_type)

    assert ctx.world_size == 4, "this test assumes world_size=4"
    initial_world_size = 2
    join_ranks = [2, 3]
    is_joiner = ctx.proc_rank >= initial_world_size

    if not is_joiner:
        # Primary ranks: init WORLD with world_size=2, max_group_size=4
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": initial_world_size,
            "pg_options": pg.MooncakeBackendOptions(ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        world_backend = get_mooncake_backend(device_type=ctx.device_type)

        # Subgroups with split-ranks pattern
        group_a = dist.new_group(
            ranks=[0],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(2),
        )
        group_b = dist.new_group(
            ranks=[1],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(2),
        )
        group_c = dist.new_group(
            ranks=[0, 1],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(4),
        )
        a_backend = get_mooncake_backend(group_a, device_type=ctx.device_type) if ctx.proc_rank == 0 else None
        b_backend = get_mooncake_backend(group_b, device_type=ctx.device_type) if ctx.proc_rank == 1 else None
        c_backend = get_mooncake_backend(group_c, device_type=ctx.device_type)

        # Pre-activation: WORLD primary ranks sum to 1+2=3
        t = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.SUM)
        if int(t.cpu().item()) != 3:
            raise AssertionError(f"WORLD pre: expected 3, got {int(t.cpu().item())}")

        # group_c primaries (ranks [0,1]): sum to 1+2=3
        tc = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tc, op=dist.ReduceOp.SUM, group=group_c)
        if int(tc.cpu().item()) != 3:
            raise AssertionError(f"group_c pre: expected 3, got {int(tc.cpu().item())}")

        if ctx.proc_rank == 0:
            extend_event.set()

        # WORLD: wait for joiners then activate
        wait_until(
            lambda: all(pg.get_peer_state(world_backend, join_ranks)),
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for joiners",
        )
        resp = pg.recover_ranks(world_backend, join_ranks)
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {ctx.proc_rank}: recover_ranks(world) should apply, got {resp.status}"

        # group_a: rank 0 waits for joiner (local rank 1 = global rank 2)
        if ctx.proc_rank == 0:
            wait_until(
                lambda: pg.get_peer_state(a_backend, [1])[0],
                timeout_s=30.0,
                poll_interval_s=0.05,
                description="rank 0 waiting for group_a joiner",
            )
            resp = pg.recover_ranks(a_backend, [1])
            assert resp.status == pg.ViewUpdateStatus.Applied, \
                f"rank 0: recover_ranks(group_a) should apply, got {resp.status}"

        # group_b: rank 1 waits for joiner (local rank 1 = global rank 3)
        if ctx.proc_rank == 1:
            wait_until(
                lambda: pg.get_peer_state(b_backend, [1])[0],
                timeout_s=30.0,
                poll_interval_s=0.05,
                description="rank 1 waiting for group_b joiner",
            )
            resp = pg.recover_ranks(b_backend, [1])
            assert resp.status == pg.ViewUpdateStatus.Applied, \
                f"rank 1: recover_ranks(group_b) should apply, got {resp.status}"

        # group_c: both primaries wait for both joiners (local ranks 2,3)
        wait_until(
            lambda: all(pg.get_peer_state(c_backend, [2, 3])),
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for group_c joiners",
        )
        resp = pg.recover_ranks(c_backend, [2, 3])
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {ctx.proc_rank}: recover_ranks(group_c) should apply, got {resp.status}"

        # Post-activation: WORLD all 4 ranks → 1+2+3+4=10
        t = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.SUM)
        if int(t.cpu().item()) != 10:
            raise AssertionError(f"WORLD post: expected 10, got {int(t.cpu().item())}")

        # group_a: ranks [0,2], values [1,3], sum=4
        if ctx.proc_rank == 0:
            ta = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
            dist.all_reduce(ta, op=dist.ReduceOp.SUM, group=group_a)
            if int(ta.cpu().item()) != 4:
                raise AssertionError(f"group_a post: expected 4, got {int(ta.cpu().item())}")

        # group_b: ranks [1,3], values [2,4], sum=6
        if ctx.proc_rank == 1:
            tb = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
            dist.all_reduce(tb, op=dist.ReduceOp.SUM, group=group_b)
            if int(tb.cpu().item()) != 6:
                raise AssertionError(f"group_b post: expected 6, got {int(tb.cpu().item())}")

        # group_c: all 4 ranks, sum=10
        tc = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tc, op=dist.ReduceOp.SUM, group=group_c)
        if int(tc.cpu().item()) != 10:
            raise AssertionError(f"group_c post: expected 10, got {int(tc.cpu().item())}")

        ctx.record_result({"role": "extension_subgroups", "rank": ctx.proc_rank})
    else:
        # Joiners: wait for extend_event, then init WORLD with world_size=4
        if not extend_event.wait(timeout=60.0):
            raise TimeoutError("timed out waiting for extend_event")

        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": ctx.world_size,
            "pg_options": pg.MooncakeBackendOptions(ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        world_backend = get_mooncake_backend(device_type=ctx.device_type)

        # Subgroups: full eventual membership; matching call order with primaries.
        group_a = dist.new_group(
            ranks=[0, 2],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(2),
        )
        group_b = dist.new_group(
            ranks=[1, 3],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(2),
        )
        group_c = dist.new_group(
            ranks=[0, 1, 2, 3],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(4),
        )
        a_backend = get_mooncake_backend(group_a, device_type=ctx.device_type) if ctx.proc_rank == 2 else None
        b_backend = get_mooncake_backend(group_b, device_type=ctx.device_type) if ctx.proc_rank == 3 else None
        c_backend = get_mooncake_backend(group_c, device_type=ctx.device_type)

        # Before any join_group call, WORLD collectives are local-only on each
        # joining rank and do not involve the primary ranks.
        t = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.SUM)
        local_value = int(t.cpu().item())
        if local_value != ctx.proc_rank + 1:
            raise AssertionError(
                f"WORLD local-only: expected {ctx.proc_rank + 1}, "
                f"got {local_value}"
            )

        # Join groups in same order primaries created them
        pg.join_group(world_backend)
        if ctx.proc_rank == 2:
            pg.join_group(a_backend)
        if ctx.proc_rank == 3:
            pg.join_group(b_backend)
        pg.join_group(c_backend)

        # Post-activation: WORLD all 4 ranks → 1+2+3+4=10
        t = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.SUM)
        if int(t.cpu().item()) != 10:
            raise AssertionError(f"WORLD post: expected 10, got {int(t.cpu().item())}")

        # group_a: ranks [0,2], sum=4
        if ctx.proc_rank == 2:
            ta = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
            dist.all_reduce(ta, op=dist.ReduceOp.SUM, group=group_a)
            if int(ta.cpu().item()) != 4:
                raise AssertionError(f"group_a post: expected 4, got {int(ta.cpu().item())}")

        # group_b: ranks [1,3], sum=6
        if ctx.proc_rank == 3:
            tb = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
            dist.all_reduce(tb, op=dist.ReduceOp.SUM, group=group_b)
            if int(tb.cpu().item()) != 6:
                raise AssertionError(f"group_b post: expected 6, got {int(tb.cpu().item())}")

        # group_c: all 4 ranks, sum=10
        tc = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tc, op=dist.ReduceOp.SUM, group=group_c)
        if int(tc.cpu().item()) != 10:
            raise AssertionError(f"group_c post: expected 10, got {int(tc.cpu().item())}")

        ctx.record_result({"role": "extension_subgroups", "rank": ctx.proc_rank})


def _run_allgather_reduce_scatter(
    device: str,
    active_world_size: int,
    rank: int,
) -> None:
    """Run _allgather_base and _reduce_scatter_base and assert correctness.

    active_world_size: number of currently active ranks (buffer size).
    rank: this rank's logical rank (0-indexed).
    Each rank contributes value (rank + 1).
    """

    # --- _allgather_base ---
    # input: scalar (rank+1); output: flat buffer of active_world_size elements
    input_t = torch.tensor([rank + 1], dtype=torch.int32, device=device)
    output_t = torch.zeros(active_world_size, dtype=torch.int32, device=device)
    dist.all_gather_into_tensor(output_t, input_t)

    for j in range(active_world_size):
        expected = j + 1
        got = int(output_t[j].item())
        if got != expected:
            raise AssertionError(
                f"allgather slot {j}: expected {expected}, got {got} "
                f"(rank={rank}, active_world_size={active_world_size})"
            )

    # --- _reduce_scatter_base ---
    # input: flat buffer [1, 2, ..., active_world_size]; output: scalar chunk
    # With SUM and equal chunks of size 1, each rank receives its own slot's sum
    # across all ranks. Since all ranks send the same input buffer [1..N],
    # rank j receives sum of input[j] from all ranks = (j+1) * active_world_size.
    input_rs = torch.arange(1, active_world_size + 1, dtype=torch.int32, device=device)
    output_rs = torch.zeros(1, dtype=torch.int32, device=device)
    dist.reduce_scatter_tensor(output_rs, input_rs)
    expected_rs = (rank + 1) * active_world_size
    got_rs = int(output_rs[0].item())
    if got_rs != expected_rs:
        raise AssertionError(
            f"reduce_scatter slot {rank}: expected {expected_rs}, got {got_rs} "
            f"(rank={rank}, active_world_size={active_world_size})"
        )


def _allgather_reduce_scatter_extension_worker(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
) -> None:
    """Test _allgather_base and _reduce_scatter_base across elastic extension.

    Layout: world_size=4, initial=3, extension_rank=3, max_group_size=4.
    Pre-activation: 3 active ranks, max_group_size=4 → exercises the overflow path.
    Post-activation: 4 active ranks → exercises correctness after extension.
    """
    configure_mooncake_device_filter(ctx.device_filters)
    device = require_test_device(ctx.proc_rank, ctx.device_type)

    assert ctx.world_size == 4
    initial_world_size = 3
    extension_rank = 3
    join_ranks = [extension_rank]
    is_joiner = ctx.proc_rank == extension_rank

    if not is_joiner:
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": initial_world_size,
            "pg_options": pg.MooncakeBackendOptions(ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        backend = get_mooncake_backend(device_type=ctx.device_type)

        # Pre-activation: 3 active ranks, max_group_size=4.
        # This is the overflow path: buggy code would iterate 4 times into a
        # buffer sized for 3.
        _run_allgather_reduce_scatter(device, initial_world_size, ctx.proc_rank)

        if ctx.proc_rank == 0:
            extend_event.set()

        wait_until(
            lambda: all(pg.get_peer_state(backend, join_ranks)),
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for joiner",
        )
        resp = pg.recover_ranks(backend, join_ranks)
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {ctx.proc_rank}: recover_ranks should apply, got {resp.status}"

        # Post-activation: all 4 ranks active.
        _run_allgather_reduce_scatter(device, ctx.world_size, ctx.proc_rank)

        ctx.record_result({"role": "primary", "rank": ctx.proc_rank})
    else:
        if not extend_event.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for extend_event")

        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": extension_rank,
            "world_size": ctx.world_size,
            "pg_options": pg.MooncakeBackendOptions(ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        backend = get_mooncake_backend(device_type=ctx.device_type)

        pg.join_group(backend)

        # Post-activation: all 4 ranks active.
        _run_allgather_reduce_scatter(device, ctx.world_size, extension_rank)

        ctx.record_result({"role": "joiner", "rank": extension_rank})


def _allgather_reduce_scatter_recovery_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
    start_recovery: mp.Event,
) -> None:
    """Test _allgather_base and _reduce_scatter_base across rank recovery.

    Layout: world_size=4, broken_rank=3, replacement takes rank 3.
    Pre-failure: 4 active ranks.
    Post-failure (3 survivors): 3 active ranks, max_group_size=4 → overflow path.
    Post-recovery: 4 active ranks again.
    """
    broken_rank = ctx.world_size - 1
    logical_rank = ctx.rank if ctx.proc_rank < ctx.world_size else broken_rank

    if ctx.proc_rank < ctx.world_size:
        device = ctx.init_group(rank=logical_rank)
        backend = ctx.get_backend()

        # Pre-failure: all 4 ranks active.
        _run_allgather_reduce_scatter(device, ctx.world_size, logical_rank)

        if logical_rank == broken_rank:
            ctx.record_result({"role": "broken"})
            broken_exited.set()
            os._exit(0)

        # Survivors: 3 active ranks, max_group_size=4 → overflow path.
        broken_exited.wait()

        _run_allgather_reduce_scatter(device, ctx.world_size - 1, logical_rank)

        if logical_rank == 0:
            start_recovery.set()

        wait_until(
            lambda: pg.get_peer_state(backend, [broken_rank])[0],
            timeout_s=30.0,
            poll_interval_s=2.0,
            description=f"rank {logical_rank} waiting for replacement",
        )
        resp = pg.recover_ranks(backend, [broken_rank])
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {logical_rank}: recover_ranks should apply, got {resp.status}"

        # Post-recovery: all 4 ranks active again.
        _run_allgather_reduce_scatter(device, ctx.world_size, logical_rank)

        ctx.record_result({"role": "survivor"})
    else:
        start_recovery.wait()
        device = ctx.init_group(rank=logical_rank, )
        backend = ctx.get_backend()
        pg.join_group(backend)

        # Post-recovery: all 4 ranks active.
        _run_allgather_reduce_scatter(device, ctx.world_size, logical_rank)

        ctx.record_result({"role": "replacement"})


def _fault_detection_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
) -> None:
    """Worker for testing fault detection - survivors can continue without broken rank."""
    device = ctx.init_group()
    backend = ctx.get_backend()

    # Step 1: All ranks participate in first collective
    tensor = torch.tensor([ctx.rank], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

    if ctx.rank == BROKEN_RANK:
        # Step 2: Broken rank exits after first collective
        ctx.record_result({"role": "broken"})
        broken_exited.set()
        os._exit(0)

    # Step 3: Survivors wait for broken rank to exit
    broken_exited.wait()

    # Step 4: Survivors run collective without broken rank
    # This should not hang - verifies fault detection works
    tensor = torch.tensor([ctx.rank], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

    ctx.record_result({"role": "survivor"})


def _replacement_recovery_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
    start_recovery: mp.Event,
) -> None:
    """Worker for testing replacement recovery."""
    logical_rank = ctx.rank if ctx.proc_rank < ctx.world_size else BROKEN_RANK

    if ctx.proc_rank < ctx.world_size:
        # Original rank (0, 1, 2, or 3)
        device = ctx.init_group(rank=logical_rank)

        # Round 1: all healthy
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        if logical_rank == BROKEN_RANK:
            # Broken rank exits
            ctx.record_result({"role": "broken"})
            broken_exited.set()
            os._exit(0)

        # Survivor ranks
        broken_exited.wait()
        backend = ctx.get_backend()

        # Round 2: run collective with dead rank
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        # Signal that we're ready for replacement
        if logical_rank == 0:
            start_recovery.set()

        # Wait for replacement to be connected (metadata published)
        wait_until(
            lambda: pg.get_peer_state(backend, [BROKEN_RANK])[0],
            timeout_s=30.0,
            poll_interval_s=2.0,
            description=f"rank {logical_rank} waiting for replacement to connect",
        )

        # All ranks call recover_ranks to include replacement
        resp = pg.recover_ranks(backend, [BROKEN_RANK])
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {logical_rank}: recover_ranks should apply, got {resp.status}"

        # Final collective with all 4 ranks
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({"role": "survivor"})
    else:
        # Replacement process (proc_rank = world_size)
        # Wait for signal to start
        start_recovery.wait()

        # Replacement initializes with is_extension
        device = ctx.init_group(rank=logical_rank, )
        backend = ctx.get_backend()

        # join_group completes the connection and switches to global mode
        pg.join_group(backend)

        # Final collective with all 4 ranks
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({"role": "replacement"})


def _manual_deactivate_recovery_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
    start_recovery: mp.Event,
) -> None:
    """Manual deactivation and recovery with auto_deactivate disabled:
    Round 1 (all healthy): failedRanks = all 0s, activeRanks = all 1s.
    Round 2 (rank died, not yet deactivated): activeRanks unchanged.
    Round 3: survivors sync, manually deactivate, and run as a reduced group.
    Round 4: a replacement joins, survivors recover it, and the full group
      runs again.
    """
    logical_rank = ctx.rank if ctx.proc_rank < ctx.world_size else BROKEN_RANK

    if ctx.proc_rank < ctx.world_size:
        device = ctx.init_group(
            rank=logical_rank,
            auto_deactivate_on_failure=False,
            auto_sync_on_failure=False,
        )
        backend = ctx.get_backend()

        # Round 1: all healthy
        expected_all = ctx.world_size * (ctx.world_size + 1) // 2
        tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        assert int(tensor.cpu().item()) == expected_all
        assert pg.get_local_success(work), \
            f"rank {ctx.rank}: round 1 should succeed locally"

        failed_ranks_hint = pg.get_failed_ranks_hint(work)
        assert failed_ranks_hint.tolist() == [0] * ctx.world_size

        active_ranks = pg.get_active_ranks(backend)
        assert active_ranks.cpu().tolist() == [1] * ctx.world_size

        if logical_rank == BROKEN_RANK:
            ctx.record_result({"role": "broken"})
            broken_exited.set()
            os._exit(0)

        broken_exited.wait()

        # Round 2: rank died, auto_deactivate=False ==> activeRanks unchanged.
        # local_success=False because the dead rank is still in the group.
        expected_reduced = expected_all - (BROKEN_RANK + 1)
        tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        assert int(tensor.cpu().item()) == expected_reduced
        assert not pg.get_local_success(work), \
            f"rank {ctx.rank}: round 2 should detect broken rank"

        failed_ranks_hint = pg.get_failed_ranks_hint(work)
        expected_failed_ranks_hint = [0] * ctx.world_size
        expected_failed_ranks_hint[BROKEN_RANK] = 1
        assert failed_ranks_hint.tolist() == expected_failed_ranks_hint

        active_ranks = pg.get_active_ranks(backend)
        assert active_ranks.cpu().tolist() == [1] * ctx.world_size

        # sync_after_failure waits for the shared reconciliation decision even
        # when membership is managed manually.  Its response applies the
        # authoritative group view before returning, and the local failed-link
        # observation must already be visible through get_peer_state().
        sync_resp = pg.sync_after_failure(backend)
        assert sync_resp.status in (
            pg.SyncAfterFailureStatus.Reconciled,
            pg.SyncAfterFailureStatus.NoPending,
        ), f"rank {logical_rank}: sync_after_failure failed: {sync_resp.reject_reason}"
        assert not pg.get_peer_state(backend, [BROKEN_RANK])[0], \
            f"rank {logical_rank}: failed rank remained locally activatable after sync"

        # Survivors deactivate the dead rank before issuing new collectives.
        resp = pg.deactivate_ranks(backend, [BROKEN_RANK])
        assert resp.status in (
            pg.ViewUpdateStatus.Applied,
            pg.ViewUpdateStatus.AppliedWithDroppedRanks,
        ), f"rank {ctx.rank}: deactivate_ranks should apply, got {resp.status}"
        # If ranks were dropped, they should only be the BROKEN_RANK
        assert resp.status != pg.ViewUpdateStatus.AppliedWithDroppedRanks or set(
            resp.dropped_ranks
        ) == {
            BROKEN_RANK
        }, f"rank {ctx.rank}: unexpected dropped ranks {resp.dropped_ranks}"

        # Round 3: after deactivate, collective with reduced group succeeds.
        expected_reduced = expected_all - (BROKEN_RANK + 1)
        tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        assert int(tensor.cpu().item()) == expected_reduced
        assert pg.get_local_success(work), \
            f"rank {ctx.rank}: round 3 should succeed after manual deactivate"

        if logical_rank == 0:
            start_recovery.set()

        wait_until(
            lambda: pg.get_peer_state(backend, [BROKEN_RANK])[0],
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {logical_rank} waiting for replacement",
        )

        resp = pg.recover_ranks(backend, [BROKEN_RANK])
        assert resp.status == pg.ViewUpdateStatus.Applied, \
            f"rank {logical_rank}: recover_ranks should apply, got {resp.status}"

        tensor = torch.tensor(
            [logical_rank + 1], dtype=torch.int32, device=device
        )
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        assert int(tensor.cpu().item()) == expected_all

        ctx.record_result({"role": "survivor"})

    else:
        if not start_recovery.wait(timeout=120.0):
            raise TimeoutError("timed out waiting to start manual recovery")
        device = ctx.init_group(
            rank=logical_rank,
            auto_deactivate_on_failure=False,
            auto_sync_on_failure=False,
        )
        backend = ctx.get_backend()
        pg.join_group(backend)

        tensor = torch.tensor(
            [logical_rank + 1], dtype=torch.int32, device=device
        )
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        ctx.record_result({"role": "replacement"})


class _ElasticMixin:
    world_size = 4
    spawn_timeout_s = 30.0

    def test_failed_rank(self) -> None:
        """Test that survivors can continue collective after a rank fails."""
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _fault_detection_worker,
            broken_exited,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        # All survivors should complete
        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        self.assertEqual(len(survivor_rows), self.world_size - 1)

        # Broken rank should have exited (may not have result)
        broken_rows = [r for r in rows if r.get("role") == "broken"]
        self.assertGreaterEqual(len(broken_rows), 1)

    def test_recovery(self) -> None:
        """Test that replacement can join and restore full collective."""
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _replacement_recovery_worker,
            broken_exited,
            start_recovery,
            nprocs=self.world_size + 1,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        # Verify all participants completed
        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        replacement_rows = [r for r in rows if r.get("role") == "replacement"]
        broken_rows = [r for r in rows if r.get("role") == "broken"]

        self.assertEqual(len(survivor_rows), self.world_size - 1)
        self.assertEqual(len(replacement_rows), 1)
        self.assertGreaterEqual(len(broken_rows), 1)

    def test_extension(self) -> None:
        """Test extension mode allows new ranks to join existing group."""
        spawn_ctx = mp.get_context("spawn")
        extend_event = spawn_ctx.Event()

        # Spawn world_size processes: (world_size - 1) original + 1 extension
        rows = self.spawn_backend_and_collect(
            _extension_worker,
            extend_event,
            nprocs=self.world_size,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        # Verify all participants completed
        original_rows = [r for r in rows if r.get("role") == "original"]
        extension_rows = [r for r in rows if r.get("role") == "extension"]

        self.assertEqual(len(original_rows), self.world_size - 1)
        self.assertEqual(len(extension_rows), 1)

        # Verify baseline sum: 1+2+...+(world_size-1) = world_size*(world_size-1)/2
        expected_baseline = (self.world_size - 1) * self.world_size // 2
        for row in original_rows:
            self.assertEqual(row.get("baseline"), expected_baseline)

    def test_extension_p2p_joiner_to_primary(self) -> None:
        """Test P2P from an extension rank to an original rank after recovery."""
        spawn_ctx = mp.get_context("spawn")
        extend_event = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _extension_p2p_worker,
            extend_event,
            "joiner_to_primary",
            nprocs=self.world_size,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)
        self.assertEqual(len([r for r in rows if r.get("role") == "src"]), 1)
        self.assertEqual(len([r for r in rows if r.get("role") == "dst"]), 1)

    def test_extension_p2p_primary_to_joiner(self) -> None:
        """Test P2P from an original rank to an extension rank after recovery."""
        spawn_ctx = mp.get_context("spawn")
        extend_event = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _extension_p2p_worker,
            extend_event,
            "primary_to_joiner",
            nprocs=self.world_size,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)
        self.assertEqual(len([r for r in rows if r.get("role") == "src"]), 1)
        self.assertEqual(len([r for r in rows if r.get("role") == "dst"]), 1)

    def test_extension_with_subgroups(self) -> None:
        """Test extension with multiple disjoint subgroups using split-ranks pattern."""
        spawn_ctx = mp.get_context("spawn")
        extend_event = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _extension_worker_with_subgroups,
            extend_event,
            nprocs=self.world_size,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        result_rows = [r for r in rows if r.get("role") == "extension_subgroups"]
        self.assertEqual(len(result_rows), self.world_size)

    def test_allgather_reduce_scatter_extension(self) -> None:
        """Test _allgather_base/_reduce_scatter_base correctness across elastic extension.

        Exercises the overflow path: pre-activation uses max_group_size=4 with only
        3 active ranks, so the buggy code would access slot 3 of a size-3 buffer.
        """
        spawn_ctx = mp.get_context("spawn")
        extend_event = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _allgather_reduce_scatter_extension_worker,
            extend_event,
            nprocs=self.world_size,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        primary_rows = [r for r in rows if r.get("role") == "primary"]
        joiner_rows = [r for r in rows if r.get("role") == "joiner"]
        self.assertEqual(len(primary_rows), self.world_size - 1)
        self.assertEqual(len(joiner_rows), 1)

    def test_allgather_reduce_scatter_recovery(self) -> None:
        """Test _allgather_base/_reduce_scatter_base correctness across rank recovery.

        Exercises the overflow path: post-failure survivors run with 3 active ranks
        and max_group_size=4, so the buggy code would access slot 3 of a size-3 buffer.
        """
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _allgather_reduce_scatter_recovery_worker,
            broken_exited,
            start_recovery,
            nprocs=self.world_size + 1,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        replacement_rows = [r for r in rows if r.get("role") == "replacement"]
        broken_rows = [r for r in rows if r.get("role") == "broken"]
        self.assertEqual(len(survivor_rows), self.world_size - 1)
        self.assertEqual(len(replacement_rows), 1)
        self.assertGreaterEqual(len(broken_rows), 1)

    def test_manual_evict_recovery(self) -> None:
        """Test manual evict and recovery with auto_deactivate disabled.

        Round 1: all healthy, failedRanks = all 0s.
        Round 2: rank dies; sync reconciles but leaves membership unchanged.
        Round 3: manually deactivate and run with the reduced group.
        Round 4: activate a replacement and run with the full group.
        """
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _manual_deactivate_recovery_worker,
            broken_exited,
            start_recovery,
            nprocs=self.world_size + 1,
            timeout_s=60.0,
        )

        self.assert_all_ok(rows)

        # All survivors should complete
        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        self.assertEqual(len(survivor_rows), self.world_size - 1)

        replacement_rows = [r for r in rows if r.get("role") == "replacement"]
        self.assertEqual(len(replacement_rows), 1)

        # Broken rank should have exited (may not have result)
        broken_rows = [r for r in rows if r.get("role") == "broken"]
        self.assertGreaterEqual(len(broken_rows), 1)


class TestMooncakePGElasticCPU(
    _ElasticMixin, MooncakePGCPUBackendTestCase
):
    pass


class TestMooncakePGElasticCUDA(
    _ElasticMixin, MooncakePGCUDABackendTestCase
):
    @classmethod
    def configure_for_cuda_device_count(cls, device_count: int) -> None:
        if device_count < 2:
            return
        cls.world_size = min(device_count, 4)


if __name__ == "__main__":
    unittest.main()
