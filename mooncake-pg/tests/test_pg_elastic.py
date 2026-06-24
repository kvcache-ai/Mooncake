import os
import time
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


def _dynamic_world_size_worker(
    ctx: MooncakePGWorkerContext,
) -> None:
    """Worker for testing that dist.get_world_size() reflects dynamic size after extend."""
    initial_world_size = ctx.world_size
    ctx.init_group()
    backend = ctx.get_backend()

    initial_ws = dist.get_world_size()
    assert initial_ws == initial_world_size, (
        f"rank {ctx.rank}: initial world_size={initial_ws}, expected {initial_world_size}"
    )

    pg.extend_group_size_to(backend, initial_world_size + 1)

    new_ws = dist.get_world_size()
    assert new_ws == initial_world_size + 1, (
        f"rank {ctx.rank}: after extend world_size={new_ws}, expected {initial_world_size + 1}"
    )

    ctx.record_result({"initial_ws": initial_ws, "new_ws": new_ws})


def _extension_worker(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
    init_done_event: mp.Event,
) -> None:
    """Worker for testing extension mode - new ranks join existing group."""
    initial_world_size = ctx.world_size - 1
    extension_rank = ctx.world_size - 1

    join_ranks = [extension_rank]

    if ctx.proc_rank < initial_world_size:
        # Original ranks
        device = ctx.init_group(
            world_size=initial_world_size,
            max_world_size=ctx.world_size,
        )
        backend = ctx.get_backend()

        # group_size should equal initial_world_size immediately after init
        # (max_world_size only pre-allocates capacity, does not change visible size)
        actual_ws = dist.get_world_size()
        assert actual_ws == initial_world_size, (
            f"rank {ctx.proc_rank}: initial world_size={actual_ws}, "
            f"expected initial_world_size={initial_world_size}"
        )

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
        # Note: get_peer_state() is collective among *healthy ranks*.
        wait_until(
            lambda: all(pg.get_peer_state(backend, join_ranks)),
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for joiner ready",
        )
        pg.recover_ranks(backend, join_ranks)

        # After recover_ranks, world_size should now reflect the expanded group
        actual_ws_after = dist.get_world_size()
        assert actual_ws_after == ctx.world_size, (
            f"rank {ctx.proc_rank}: world_size after recover={actual_ws_after}, "
            f"expected max_world_size={ctx.world_size}"
        )

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
            is_extension=True,
            max_world_size=ctx.world_size,
        )

        backend = ctx.get_backend()

        # group_size for extension rank equals world_size passed at init.
        # Note: this is world_size (= max_world_size for joiners), not 1,
        # because the joiner's activeSize is initialized to world_size.
        # The local-only behavior is ensured by activeRanks masking, not
        # by a smaller activeSize.
        actual_ws = dist.get_world_size()
        assert actual_ws == ctx.world_size, (
            f"extension rank: initial world_size={actual_ws}, "
            f"expected {ctx.world_size}"
        )

        # In extension mode, joiner starts in local-only collectives.
        local_tensor = torch.tensor([extension_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(local_tensor, op=dist.ReduceOp.SUM)
        if int(local_tensor.cpu().item()) != extension_rank + 1:
            raise AssertionError(
                f"extension rank expected local-only sum {extension_rank + 1}, got {int(local_tensor.cpu().item())}"
            )

        # join_group publishes metadata and then blocks until recover_ranks()
        # publishes the extension state.
        pg.join_group(backend)

        # After joinGroup, world_size should reflect the full group
        actual_ws_after = dist.get_world_size()
        assert actual_ws_after == ctx.world_size, (
            f"extension rank: world_size after joinGroup={actual_ws_after}, "
            f"expected {ctx.world_size}"
        )

        # Final collective
        final_tensor = torch.tensor([extension_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(final_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({
            "role": "extension",
            "rank": extension_rank,
        })


def _extension_worker_with_subgroups(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
) -> None:
    """Multi-subgroup elastic extension test using split-ranks pattern.

    Layout (world_size=4, primary=[0,1], joiners=[2,3]):
      group_a: primary ranks=[0],   joiner ranks=[0,2],     max_world_size=2
      group_b: primary ranks=[1],   joiner ranks=[1,3],     max_world_size=2
      group_c: primary ranks=[0,1], joiner ranks=[0,1,2,3], max_world_size=4

    Primary ranks must initialize WORLD with world_size=initial_world_size and
    create subgroups using only their current membership; joiners wait for the
    extend signal, then init WORLD with the full world_size and create subgroups
    using the full eventual membership. PyTorch's new_group uses a monotonic
    call counter for the store prefix, so primary and joiner side land on the
    same prefix as long as the call order matches. backendIndex_ is also
    process-local and increments only when the rank is an actual member of the
    new group, so it stays aligned across primaries and joiners that all call
    new_group in the same order.
    """
    configure_mooncake_device_filter(ctx.device_filters)
    device = require_test_device(ctx.proc_rank, ctx.device_type)

    assert ctx.world_size == 4, "this test assumes world_size=4"
    initial_world_size = 2
    join_ranks = [2, 3]
    is_joiner = ctx.proc_rank >= initial_world_size

    a_active = torch.tensor([1, 0], dtype=torch.int32, device=device)
    b_active = torch.tensor([1, 0], dtype=torch.int32, device=device)
    c_active = torch.tensor([1, 1, 0, 0], dtype=torch.int32, device=device)

    if not is_joiner:
        # Primary ranks: init WORLD with world_size=2, max_world_size=4
        world_active = torch.tensor([1, 1, 0, 0], dtype=torch.int32, device=device)
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": initial_world_size,
            "pg_options": pg.MooncakeBackendOptions(world_active, False, ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        world_backend = get_mooncake_backend(device_type=ctx.device_type)

        # Subgroups with split-ranks pattern. All ranks in WORLD must call
        # new_group in the same order even for groups they are not members of.
        group_a = dist.new_group(
            ranks=[0],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(a_active, False, 2),
        )
        group_b = dist.new_group(
            ranks=[1],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(b_active, False, 2),
        )
        group_c = dist.new_group(
            ranks=[0, 1],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(c_active, False, 4),
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

        # WORLD: wait for joiners then recover
        wait_until(
            lambda: all(pg.get_peer_state(world_backend, join_ranks)),
            timeout_s=60.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for WORLD joiners",
        )
        pg.recover_ranks(world_backend, join_ranks)

        # group_a: rank 0 waits for joiner (local rank 1 = global rank 2)
        if ctx.proc_rank == 0:
            wait_until(
                lambda: pg.get_peer_state(a_backend, [1])[0],
                timeout_s=60.0,
                poll_interval_s=0.05,
                description="rank 0 waiting for group_a joiner",
            )
            pg.recover_ranks(a_backend, [1])

        # group_b: rank 1 waits for joiner (local rank 1 = global rank 3)
        if ctx.proc_rank == 1:
            wait_until(
                lambda: pg.get_peer_state(b_backend, [1])[0],
                timeout_s=60.0,
                poll_interval_s=0.05,
                description="rank 1 waiting for group_b joiner",
            )
            pg.recover_ranks(b_backend, [1])

        # group_c: both primaries wait for both joiners (local ranks 2,3)
        wait_until(
            lambda: all(pg.get_peer_state(c_backend, [2, 3])),
            timeout_s=60.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.proc_rank} waiting for group_c joiners",
        )
        pg.recover_ranks(c_backend, [2, 3])

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

        world_active = torch.tensor([1, 1, 0, 0], dtype=torch.int32, device=device)
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": ctx.world_size,
            "pg_options": pg.MooncakeBackendOptions(world_active, True, ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        world_backend = get_mooncake_backend(device_type=ctx.device_type)

        # Subgroups: full eventual membership; matching call order with primaries.
        group_a = dist.new_group(
            ranks=[0, 2],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(a_active, True, 2),
        )
        group_b = dist.new_group(
            ranks=[1, 3],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(b_active, True, 2),
        )
        group_c = dist.new_group(
            ranks=[0, 1, 2, 3],
            backend=ctx.backend_name,
            pg_options=pg.MooncakeBackendOptions(c_active, True, 4),
        )
        a_backend = get_mooncake_backend(group_a, device_type=ctx.device_type) if ctx.proc_rank == 2 else None
        b_backend = get_mooncake_backend(group_b, device_type=ctx.device_type) if ctx.proc_rank == 3 else None
        c_backend = get_mooncake_backend(group_c, device_type=ctx.device_type)

        # Joiners are local-only until join_group is called
        t = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(t, op=dist.ReduceOp.SUM)
        if int(t.cpu().item()) != ctx.proc_rank + 1:
            raise AssertionError(
                f"WORLD local-only: expected {ctx.proc_rank + 1}, got {int(t.cpu().item())}"
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

    Layout: world_size=4, initial=3, extension_rank=3, max_world_size=4.
    Pre-activation: 3 active ranks, max_world_size=4 → exercises the overflow path.
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
        active = torch.tensor([1, 1, 1, 0], dtype=torch.int32, device=device)
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": ctx.proc_rank,
            "world_size": initial_world_size,
            "pg_options": pg.MooncakeBackendOptions(active, False, ctx.world_size),
        }
        if ctx.device_type == "cuda":
            dist_kwargs["device_id"] = device
        dist.init_process_group(**dist_kwargs)
        backend = get_mooncake_backend(device_type=ctx.device_type)

        # Pre-activation: 3 active ranks, max_world_size=4.
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
        pg.recover_ranks(backend, join_ranks)

        # Post-activation: all 4 ranks active.
        _run_allgather_reduce_scatter(device, ctx.world_size, ctx.proc_rank)

        ctx.record_result({"role": "primary", "rank": ctx.proc_rank})
    else:
        if not extend_event.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for extend_event")

        active = torch.tensor([1, 1, 1, 0], dtype=torch.int32, device=device)
        dist_kwargs = {
            "backend": ctx.backend_name,
            "rank": extension_rank,
            "world_size": ctx.world_size,
            "pg_options": pg.MooncakeBackendOptions(active, True, ctx.world_size),
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
    replacement_ready: mp.Event,
    start_recovery: mp.Event,
) -> None:
    """Test _allgather_base and _reduce_scatter_base across rank recovery.

    Layout: world_size=4, broken_rank=3, replacement takes rank 3.
    Pre-failure: 4 active ranks.
    Post-failure (3 survivors): 3 active ranks, max_world_size=4 → overflow path.
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

        # Survivors: 3 active ranks, max_world_size=4 → overflow path.
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
        replacement_ready.wait()
        pg.recover_ranks(backend, [broken_rank])

        # Post-recovery: all 4 ranks active again.
        _run_allgather_reduce_scatter(device, ctx.world_size, logical_rank)

        ctx.record_result({"role": "survivor"})
    else:
        start_recovery.wait()
        device = ctx.init_group(rank=logical_rank, is_extension=True)
        backend = ctx.get_backend()
        replacement_ready.set()
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

    # Step 1: All ranks participate in first collective
    tensor = torch.tensor([ctx.rank], dtype=torch.int32, device=device)
    work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
    work.wait()

    # Verify failedRanks = all 0s when all healthy
    failed_ranks = pg.get_failed_ranks(work)
    assert (
        failed_ranks.cpu().tolist() == [0] * ctx.world_size
    ), f"rank {ctx.rank}: pre-failure failed_ranks={failed_ranks.cpu().tolist()}"

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
    work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
    work.wait()

    # Verify failedRanks shows broken rank as 1
    failed_ranks = pg.get_failed_ranks(work)
    expected_failed_ranks = [0] * ctx.world_size
    expected_failed_ranks[BROKEN_RANK] = 1
    assert failed_ranks.cpu().tolist() == expected_failed_ranks, (
        f"rank {ctx.rank}: post-failure failed_ranks={failed_ranks.cpu().tolist()}, "
        f"expected {expected_failed_ranks}"
    )

    # Verify activeRanks also deactivates broken rank (auto_deactivate=True default)
    active_ranks = pg.get_active_ranks(dist.group.WORLD)
    expected_active_ranks = [1] * ctx.world_size
    expected_active_ranks[BROKEN_RANK] = 0
    assert active_ranks.cpu().tolist() == expected_active_ranks, (
        f"rank {ctx.rank}: post-failure active_ranks={active_ranks.cpu().tolist()}, "
        f"expected {expected_active_ranks}"
    )

    ctx.record_result({"role": "survivor"})


def _replacement_recovery_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
    replacement_ready: mp.Event,
    start_recovery: mp.Event,
) -> None:
    """Worker for testing replacement recovery."""
    logical_rank = ctx.rank if ctx.proc_rank < ctx.world_size else BROKEN_RANK

    if ctx.proc_rank < ctx.world_size:
        # Original rank (0, 1, 2, or 3)
        device = ctx.init_group(rank=logical_rank)

        # First collective with all ranks
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        failed_ranks = pg.get_failed_ranks(work)
        assert failed_ranks.cpu().tolist() == [0] * ctx.world_size

        if logical_rank == BROKEN_RANK:
            # Broken rank exits
            ctx.record_result({"role": "broken"})
            broken_exited.set()
            os._exit(0)

        # Survivor ranks
        broken_exited.wait()
        backend = ctx.get_backend()

        # Run collective with broken rank
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        failed_ranks = pg.get_failed_ranks(work)
        expected_failed_ranks = [0] * ctx.world_size
        expected_failed_ranks[BROKEN_RANK] = 1
        assert failed_ranks.cpu().tolist() == expected_failed_ranks

        # Signal that we're ready for replacement
        if logical_rank == 0:
            start_recovery.set()

        # Wait for replacement to be connected (metadata published)
        # Use longer poll interval to avoid overloading the connection poller
        wait_until(
            lambda: pg.get_peer_state(backend, [BROKEN_RANK])[0],
            timeout_s=30.0,
            poll_interval_s=2.0,
            description=f"rank {logical_rank} waiting for replacement to connect",
        )

        # Wait for replacement to be ready for join_group
        replacement_ready.wait()

        # All ranks call recover_ranks to include replacement
        pg.recover_ranks(backend, [BROKEN_RANK])

        # Final collective with all 4 ranks
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        failed_ranks = pg.get_failed_ranks(work)
        assert failed_ranks.cpu().tolist() == [0] * ctx.world_size

        ctx.record_result({"role": "survivor"})
    else:
        # Replacement process (proc_rank = world_size)
        # Wait for signal to start
        start_recovery.wait()

        # Replacement initializes with is_extension (local-only mode)
        device = ctx.init_group(rank=logical_rank, is_extension=True)
        backend = ctx.get_backend()

        # Signal that we're initialized and ready for join_group
        replacement_ready.set()

        # join_group completes the connection and switches to global mode
        pg.join_group(backend)

        # Final collective with all 4 ranks
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({"role": "replacement"})


def _manual_deactivate_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
) -> None:
    """Multi-round test with auto_deactivate_on_failure=False:
    Round 1 (all healthy): failedRanks = all 0s, activeRanks = all 1s.
    Round 2 (rank died, not yet deactivated): activeRanks unchanged.
    --- Survivors deactivate the dead rank. ---
    Round 3 (after deactivate): failedRanks = all 0s for reduced group,
      activeRanks reflects the deactivation.
    """
    device = ctx.init_group(auto_deactivate_on_failure=False)
    backend = ctx.get_backend()

    # Round 1: all healthy
    expected_all = ctx.world_size * (ctx.world_size + 1) // 2
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
    work.wait()
    assert int(tensor.cpu().item()) == expected_all

    failed_ranks = pg.get_failed_ranks(work)
    assert failed_ranks.cpu().tolist() == [0] * ctx.world_size

    active_ranks = pg.get_active_ranks(backend)
    assert active_ranks.cpu().tolist() == [1] * ctx.world_size

    if ctx.rank == BROKEN_RANK:
        ctx.record_result({"role": "broken"})
        broken_exited.set()
        os._exit(0)

    broken_exited.wait()

    # Round 2: rank died, auto_deactivate=False ==> activeRanks unchanged
    # Verify activeRanks still has the dead rank.
    expected_reduced = expected_all - (BROKEN_RANK + 1)
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
    work.wait()
    assert int(tensor.cpu().item()) == expected_reduced

    failed_ranks = pg.get_failed_ranks(work)
    expected_failed_ranks = [0] * ctx.world_size
    expected_failed_ranks[BROKEN_RANK] = 1
    assert failed_ranks.cpu().tolist() == expected_failed_ranks

    active_ranks = pg.get_active_ranks(backend)
    assert active_ranks.cpu().tolist() == [1] * ctx.world_size

    # Survivors deactivate the dead rank before issuing new collectives.
    pg.deactivate_rank(backend, [BROKEN_RANK], disconnect=True)

    active_ranks = pg.get_active_ranks(backend)
    expected_active_ranks = [1] * ctx.world_size
    expected_active_ranks[BROKEN_RANK] = 0
    assert active_ranks.cpu().tolist() == expected_active_ranks

    # Round 3: after deactivate, collective with reduced group
    expected_reduced = expected_all - (BROKEN_RANK + 1)
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
    work.wait()
    assert int(tensor.cpu().item()) == expected_reduced

    # Round 3: deactivated rank doesn't participate, thus no failures.
    failed_ranks = pg.get_failed_ranks(work)
    assert failed_ranks.cpu().tolist() == [0] * ctx.world_size

    ctx.record_result({"role": "survivor"})


class _ElasticMixin:
    world_size = 4
    spawn_timeout_s = 30.0

    def test_dynamic_world_size(self) -> None:
        """Test that dist.get_world_size() returns updated value after extend_group_size_to."""
        rows = self.spawn_backend_and_collect(
            _dynamic_world_size_worker,
            timeout_s=30.0,
        )

        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["new_ws"], self.world_size + 1)

    def test_failed_rank(self) -> None:
        """Test that survivors can continue collective after a rank fails."""
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _fault_detection_worker,
            broken_exited,
            timeout_s=30.0,
        )

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
        replacement_ready = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _replacement_recovery_worker,
            broken_exited,
            replacement_ready,
            start_recovery,
            nprocs=self.world_size + 1,
            timeout_s=30.0,
        )

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
        init_done_event = spawn_ctx.Event()

        # Spawn world_size processes: (world_size - 1) original + 1 extension
        rows = self.spawn_backend_and_collect(
            _extension_worker,
            extend_event,
            init_done_event,
            nprocs=self.world_size,
            timeout_s=30.0,
        )

        # Verify all participants completed
        original_rows = [r for r in rows if r.get("role") == "original"]
        extension_rows = [r for r in rows if r.get("role") == "extension"]

        # Original: world_size - 1 ranks, Extension: 1 rank
        self.assertEqual(len(original_rows), self.world_size - 1)
        self.assertEqual(len(extension_rows), 1)

        # Verify baseline sum: 1+2+...+(world_size-1) = world_size*(world_size-1)/2
        expected_baseline = (self.world_size - 1) * self.world_size // 2
        for row in original_rows:
            self.assertEqual(row.get("baseline"), expected_baseline)

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

        result_rows = [r for r in rows if r.get("role") == "extension_subgroups"]
        self.assertEqual(len(result_rows), self.world_size)

    def test_allgather_reduce_scatter_extension(self) -> None:
        """Test _allgather_base/_reduce_scatter_base correctness across elastic extension.

        Exercises the overflow path: pre-activation uses max_world_size=4 with only
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

        primary_rows = [r for r in rows if r.get("role") == "primary"]
        joiner_rows = [r for r in rows if r.get("role") == "joiner"]
        self.assertEqual(len(primary_rows), self.world_size - 1)
        self.assertEqual(len(joiner_rows), 1)

    def test_allgather_reduce_scatter_recovery(self) -> None:
        """Test _allgather_base/_reduce_scatter_base correctness across rank recovery.

        Exercises the overflow path: post-failure survivors run with 3 active ranks
        and max_world_size=4, so the buggy code would access slot 3 of a size-3 buffer.
        """
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()
        replacement_ready = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _allgather_reduce_scatter_recovery_worker,
            broken_exited,
            replacement_ready,
            start_recovery,
            nprocs=self.world_size + 1,
            timeout_s=60.0,
        )

        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        replacement_rows = [r for r in rows if r.get("role") == "replacement"]
        broken_rows = [r for r in rows if r.get("role") == "broken"]
        self.assertEqual(len(survivor_rows), self.world_size - 1)
        self.assertEqual(len(replacement_rows), 1)
        self.assertGreaterEqual(len(broken_rows), 1)

    def test_manual_evict(self) -> None:
        """Test multi-round manual evict with auto_deactivate_on_failure=False.

        Round 1: all healthy, failedRanks = all 0s.
        Round 2: rank died, activeRanks unchanged, manually deactivate.
        Round 3: after deactivate, collective succeeds with reduced group.
        """
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _manual_deactivate_worker,
            broken_exited,
            timeout_s=30.0,
        )

        # All survivors should complete
        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        self.assertEqual(len(survivor_rows), self.world_size - 1)

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
