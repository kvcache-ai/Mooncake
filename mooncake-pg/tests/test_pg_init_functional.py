import unittest

import torch
import torch.distributed as dist

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
)


def _basic_init_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test basic init works and rank/world_size are correct."""
    device = ctx.init_group()
    # Verify rank and world_size are accessible
    assert dist.get_rank() == ctx.rank, f"rank mismatch: {dist.get_rank()} != {ctx.rank}"
    assert dist.get_world_size() == ctx.world_size, f"world_size mismatch"
    # Simple collective to verify group works
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    ctx.record_result({"sum": int(tensor.cpu().item())})


def _null_init_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test init without pg_options (using defaults)."""
    device = ctx.init_group(use_pg_options=False)
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    ctx.record_result({"sum": int(tensor.cpu().item())})


def _subgroup_create_destroy_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test subgroup creation and destruction."""
    device = ctx.init_group()
    world_size = ctx.world_size
    rank = ctx.rank

    # Create two subgroups: even ranks and odd ranks
    # Note: new_group is collective, all ranks must call it
    even_ranks = list(range(0, world_size, 2))
    odd_ranks = list(range(1, world_size, 2))

    even_group = None
    odd_group = None
    subgroup_sum = None

    try:
        # All ranks collectively create both groups
        even_group = dist.new_group(ranks=even_ranks, backend=ctx.backend_name)
        odd_group = dist.new_group(ranks=odd_ranks, backend=ctx.backend_name)

        # Each rank uses its respective group
        if rank in even_ranks and even_group is not None:
            tensor = torch.tensor([rank], dtype=torch.int32, device=device)
            dist.all_reduce(tensor, group=even_group, op=dist.ReduceOp.SUM)
            subgroup_sum = int(tensor.cpu().item())
        elif rank in odd_ranks and odd_group is not None:
            tensor = torch.tensor([rank], dtype=torch.int32, device=device)
            dist.all_reduce(tensor, group=odd_group, op=dist.ReduceOp.SUM)
            subgroup_sum = int(tensor.cpu().item())

        # Verify world group still works after subgroup operations
        world_tensor = torch.tensor([1], dtype=torch.int32, device=device)
        dist.all_reduce(world_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result({
            "subgroup_sum": subgroup_sum,
            "world_sum": int(world_tensor.cpu().item()),
        })
    finally:
        for g in (even_group, odd_group):
            if g is not None:
                try:
                    dist.destroy_process_group(g)
                except Exception:
                    pass


def _destroy_and_reinit_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test destroy and re-init process group."""
    # First init
    device = ctx.init_group()
    tensor1 = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor1, op=dist.ReduceOp.SUM)
    sum1 = int(tensor1.cpu().item())

    # Destroy WORLD group
    try:
        dist.destroy_process_group(dist.group.WORLD)
    except Exception:
        pass

    # Re-init
    device = ctx.init_group()
    tensor2 = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor2, op=dist.ReduceOp.SUM)
    sum2 = int(tensor2.cpu().item())

    ctx.record_result({"sum1": sum1, "sum2": sum2})


class _InitFunctionalMixin:
    def test_basic_init(self) -> None:
        """Test basic init works and rank/world_size are correct."""
        rows = self.spawn_backend_and_collect(_basic_init_worker)
        self.assert_all_ok(rows)
        expected_sum = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum"], expected_sum)

    def test_null_init(self) -> None:
        """Test init without pg_options (using defaults)."""
        rows = self.spawn_backend_and_collect(_null_init_worker)
        self.assert_all_ok(rows)
        expected_sum = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum"], expected_sum)

    def test_subgroup_create_destroy(self) -> None:
        """Test subgroup creation and destruction."""
        if self.world_size < 4:
            self.skipTest("subgroup test requires at least 4 ranks")

        rows = self.spawn_backend_and_collect(_subgroup_create_destroy_worker)
        self.assert_all_ok(rows)

        # Verify world sum is correct
        expected_world = self.world_size
        for row in rows:
            self.assertEqual(row["world_sum"], expected_world)

        # Verify subgroup sums
        even_ranks = list(range(0, self.world_size, 2))
        odd_ranks = list(range(1, self.world_size, 2))
        expected_even = sum(even_ranks)
        expected_odd = sum(odd_ranks)

        for row in rows:
            if row["rank"] in even_ranks:
                self.assertEqual(row["subgroup_sum"], expected_even)
            else:
                self.assertEqual(row["subgroup_sum"], expected_odd)

    def test_destroy_and_reinit(self) -> None:
        """Test destroy and re-init process group."""
        rows = self.spawn_backend_and_collect(_destroy_and_reinit_worker)
        self.assert_all_ok(rows)

        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum1"], expected)
            self.assertEqual(row["sum2"], expected)


class TestMooncakePGInitFunctionalCPU(_InitFunctionalMixin, MooncakePGCPUBackendTestCase):
    world_size = 4


class TestMooncakePGInitFunctionalCUDA(_InitFunctionalMixin, MooncakePGCUDABackendTestCase):
    world_size = 4

    @classmethod
    def configure_for_cuda_device_count(cls, device_count: int) -> None:
        if device_count < 2:
            return
        if device_count >= 4:
            cls.world_size = 4
            return
        cls.world_size = 2


if __name__ == "__main__":
    unittest.main()
