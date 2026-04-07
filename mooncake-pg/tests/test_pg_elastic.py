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
    wait_until,
)


BROKEN_RANK = 1


def _extension_worker(
    ctx: MooncakePGWorkerContext,
    extend_event: mp.Event,
    init_done_event: mp.Event,
) -> None:
    """Worker for testing extension mode - new ranks join existing group."""
    initial_world_size = ctx.world_size - 1
    extension_rank = ctx.world_size - 1

    if ctx.proc_rank < initial_world_size:
        # Original ranks
        device = ctx.init_group(world_size=initial_world_size)
        backend = ctx.get_backend()

        # First collective
        tensor = torch.tensor([ctx.proc_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        baseline = int(tensor.cpu().item())

        # Signal ready and extend
        if ctx.proc_rank == 0:
            extend_event.set()
        pg.extend_group_size_to(backend, ctx.world_size)

        # Wait for extension rank to complete init before collective
        if not init_done_event.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for extension init")

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
        )

        # Signal init complete before collective
        init_done_event.set()

        # Final collective
        final_tensor = torch.tensor([extension_rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(final_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({
            "role": "extension",
            "rank": extension_rank,
        })


def _fault_detection_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited: mp.Event,
) -> None:
    """Worker for testing fault detection - survivors can continue without broken rank."""
    device = ctx.init_group()

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
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        if logical_rank == BROKEN_RANK:
            # Broken rank exits
            ctx.record_result({"role": "broken"})
            broken_exited.set()
            os._exit(0)

        # Survivor ranks
        broken_exited.wait()
        backend = ctx.get_backend()

        # Run collective without broken rank
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

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
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

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
