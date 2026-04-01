import os
import time
import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from mooncake import pg


os.environ["MASTER_ADDR"] = "127.0.0.1"
os.environ["MASTER_PORT"] = "19000"

broken_rank = 1


def _elastic_worker(rank, num_processes, signals):
    """Worker for testing elastic world size extension."""
    assert num_processes % 2 == 0
    if rank < num_processes // 2:
        # Ensure correct operation before extension
        world_size = num_processes // 2
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=world_size,
        )
        tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == sum(range(1, world_size + 1))
        if rank == 0:
            signals["extend"] = 1

        backend = dist.group.WORLD._get_backend(torch.device("cpu"))
        # Extend world
        # Note: `extend_group_size_to` is non-blocking. Blocking will only
        # occur at the first communication if some peers have not yet connected.
        # This allows overlapping other operations between the group expansion
        # and the first communication call.
        pg.extend_group_size_to(backend, num_processes)
    else:
        while "extend" not in signals:
            time.sleep(1)
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )

    # Ensure correct operation after extension
    tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    assert tensor.item() == sum(range(1, num_processes + 1)), (
        f"Rank {rank} expected {sum(range(1, num_processes + 1))}, "
        f"get {tensor.item()}"
    )


def _deferred_recovery_worker(rank, num_processes, signals):
    """Worker for testing deferred rank recovery join."""
    if rank < num_processes:
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )
        if rank == broken_rank:
            return  # Simulate broken rank

        expected_without_broken = sum(range(0, num_processes)) - broken_rank
        tensor = torch.tensor([rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == expected_without_broken

        time.sleep(5)
        signals["recover"] = 1
        backend = dist.group.WORLD._get_backend(torch.device("cpu"))
        while True:
            (peer_state,) = pg.get_peer_state(backend, [broken_rank])
            if peer_state:
                break

        # Healthy ranks keep making progress before the recovered rank joins.
        tensor = torch.tensor([rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == expected_without_broken

        while "join_ready" not in signals:
            time.sleep(0.1)

        pg.recover_ranks(backend, [broken_rank])

        tensor = torch.tensor([rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == sum(range(0, num_processes)), (
            f"Rank {rank} expected {sum(range(0, num_processes))}, "
            f"get {tensor.item()}"
        )
    else:
        while "recover" not in signals:
            time.sleep(0.1)

        dist.init_process_group(
            backend="mooncake-cpu",
            rank=broken_rank,
            world_size=num_processes,
            pg_options=pg.MooncakeBackendOptions(
                torch.ones((num_processes,), dtype=torch.int32),
                True,
            ),
        )

        backend = dist.group.WORLD._get_backend(torch.device("cpu"))

        # Deferred join starts in a local-only mode so collectives stay self-contained.
        tensor = torch.tensor([broken_rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == broken_rank

        signals["join_ready"] = 1
        pg.join_group(backend)

        tensor = torch.tensor([broken_rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == sum(range(0, num_processes)), (
            f"Rank {rank} expected {sum(range(0, num_processes))}, "
            f"get {tensor.item()}"
        )


class TestMooncakeBackend(unittest.TestCase):
    def test_elastic_extension(self):
        num_processes = 4
        mp_manager = mp.Manager()
        signals = mp_manager.dict()
        mp.spawn(_elastic_worker, args=(num_processes, signals), nprocs=num_processes)

    def test_rank_recovery(self):
        num_processes = 4
        mp_manager = mp.Manager()
        signals = mp_manager.dict()
        mp.spawn(
            _recovery_worker,
            args=(num_processes, signals),
            nprocs=num_processes + 1,
        )

    def test_rank_recovery_deferred_join(self):
        num_processes = 4
        mp_manager = mp.Manager()
        signals = mp_manager.dict()
        mp.spawn(
            _deferred_recovery_worker,
            args=(num_processes, signals),
            nprocs=num_processes + 1,
        )


if __name__ == "__main__":
    unittest.main()
