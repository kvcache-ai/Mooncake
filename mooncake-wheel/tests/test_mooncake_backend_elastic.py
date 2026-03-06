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
        while True:
            num_synced_ranks = pg.get_num_synced_ranks(backend)
            if num_synced_ranks == num_processes:
                break
            # Simulate ongoing operations
            tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
            dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
            assert tensor.item() == sum(range(1, world_size + 1))

        # Extend world
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


def _recovery_worker(rank, num_processes, signals):
    """Worker for testing rank recovery."""
    if rank < num_processes:
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )
        if rank == broken_rank:
            return  # Simulate broken rank

        logical_rank = dist.get_rank()
        tensor = torch.tensor([logical_rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == sum(range(0, num_processes)) - broken_rank

        time.sleep(5)
        signals["recover"] = 1
        backend = dist.group.WORLD._get_backend(torch.device("cpu"))
        while True:
            (peer_state,) = pg.get_peer_state(backend, [broken_rank])
            if peer_state:
                break
        pg.recover_ranks(backend, [broken_rank])
    else:
        while "recover" not in signals:
            time.sleep(1)
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=broken_rank,
            world_size=num_processes,
            pg_options=pg.MooncakeBackendOptions(
                torch.ones((num_processes,), dtype=torch.int32),
                True,
            ),
        )

    # From here on, all active ranks (including the recovered one) are part
    # of the same process group.
    logical_rank = dist.get_rank()
    world_size = dist.get_world_size()

    # First, validate correctness within a subgroup that includes the
    # recovered rank to mimic real-world setups with multiple groups.
    subgroup_ranks = list(range(max(world_size // 2, 1)))
    if logical_rank in subgroup_ranks:
        subgroup = dist.new_group(ranks=subgroup_ranks)

        # Ensure correct collective behavior within the subgroup.
        sub_tensor = torch.tensor([logical_rank], dtype=torch.int32, device="cpu")
        dist.all_reduce(sub_tensor, op=dist.ReduceOp.SUM, group=subgroup)
        expected_sub_sum = sum(subgroup_ranks)
        assert sub_tensor.item() == expected_sub_sum, (
            f"Rank {rank} in subgroup expected {expected_sub_sum}, "
            f"got {sub_tensor.item()}"
        )

        # Test point-to-point send/recv restricted to the subgroup.
        root_rank = subgroup_ranks[0]
        if logical_rank == root_rank:
            for dst in subgroup_ranks[1:]:
                send_tensor = torch.tensor([dst], dtype=torch.int32, device="cpu")
                dist.send(send_tensor, dst=dst)

            for src in subgroup_ranks[1:]:
                recv_tensor = torch.empty(1, dtype=torch.int32, device="cpu")
                dist.recv(recv_tensor, src=src)
                assert recv_tensor.item() == src + 100
        else:
            recv_tensor = torch.empty(1, dtype=torch.int32, device="cpu")
            dist.recv(recv_tensor, src=root_rank)
            assert recv_tensor.item() == logical_rank

            response = torch.tensor(
                [logical_rank + 100], dtype=torch.int32, device="cpu"
            )
            dist.send(response, dst=root_rank)

    # Ensure correct operation after recovery on the full group.
    tensor = torch.tensor([logical_rank], dtype=torch.int32, device="cpu")
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    assert tensor.item() == sum(range(0, num_processes)), (
        f"Rank {rank} expected {sum(range(0, num_processes))}, "
        f"get {tensor.item()}"
    )

    # Test point-to-point send/recv correctness after recovery on the full group.
    if logical_rank == 0:
        # Rank 0 sends to every other rank.
        for dst in range(1, world_size):
            send_tensor = torch.tensor([dst], dtype=torch.int32, device="cpu")
            dist.send(send_tensor, dst=dst)

        # Rank 0 receives replies from every other rank.
        for src in range(1, world_size):
            recv_tensor = torch.empty(1, dtype=torch.int32, device="cpu")
            dist.recv(recv_tensor, src=src)
            assert recv_tensor.item() == src + 100
    else:
        # Non-zero ranks receive from rank 0 and send a response back.
        recv_tensor = torch.empty(1, dtype=torch.int32, device="cpu")
        dist.recv(recv_tensor, src=0)
        assert recv_tensor.item() == logical_rank

        response = torch.tensor(
            [logical_rank + 100], dtype=torch.int32, device="cpu"
        )
        dist.send(response, dst=0)


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


if __name__ == "__main__":
    unittest.main()
