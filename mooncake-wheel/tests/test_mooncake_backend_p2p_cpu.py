import os
import time
import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from mooncake import ep  # noqa: F401 - ensures ep module is imported and patches batch_isend_irecv


def _worker_ring(rank: int, world_size: int, results):
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29500")

    dist.init_process_group(
        backend="mooncake-cpu",
        rank=rank,
        world_size=world_size,
        pg_options=ep.MooncakeBackendOptions(
            torch.zeros((world_size,), dtype=torch.int32, device="cpu")
        ),
    )

    send_tensor = torch.tensor([rank], dtype=torch.int64, device="cpu")
    recv_tensor = torch.empty_like(send_tensor)

    dst = (rank + 1) % world_size
    src = (rank - 1 + world_size) % world_size

    p2p_ops = [
        dist.P2POp(op=dist.isend, tensor=send_tensor, peer=dst),
        dist.P2POp(op=dist.irecv, tensor=recv_tensor, peer=src),
    ]

    works = dist.batch_isend_irecv(p2p_ops)
    for w in works:
        w.wait()

    results[rank] = recv_tensor.item()

    while len(results) < world_size:
        time.sleep(0.1)

    dist.destroy_process_group()


def _worker_ordering(rank: int, results):
    # Only two ranks participate in this test.
    world_size = 2
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29501")

    dist.init_process_group(
        backend="mooncake-cpu",
        rank=rank,
        world_size=world_size,
        pg_options=ep.MooncakeBackendOptions(
            torch.zeros((world_size,), dtype=torch.int32, device="cpu")
        ),
    )

    num_msgs = 4

    if rank == 0:
        send_tensors = [
            torch.tensor([i], dtype=torch.int64, device="cpu")
            for i in range(num_msgs)
        ]
        ops = [
            dist.P2POp(op=dist.isend, tensor=t, peer=1) for t in send_tensors
        ]
        works = dist.batch_isend_irecv(ops)
        for w in works:
            w.wait()
        results[rank] = "ok"
    else:
        recv_tensors = [
            torch.empty(1, dtype=torch.int64, device="cpu")
            for _ in range(num_msgs)
        ]
        ops = [
            dist.P2POp(op=dist.irecv, tensor=t, peer=0) for t in recv_tensors
        ]
        works = dist.batch_isend_irecv(ops)
        for w in works:
            w.wait()
        results[rank] = [t.item() for t in recv_tensors]

    while len(results) < world_size:
        time.sleep(0.1)

    dist.destroy_process_group()


class TestMooncakeBackendP2PCPU(unittest.TestCase):
    def test_ring_send_recv(self):
        world_size = 4
        mp_manager = mp.Manager()
        results = mp_manager.dict()
        mp.spawn(
            _worker_ring,
            args=(world_size, results),
            nprocs=world_size,
            join=True,
        )

        for rank in range(world_size):
            expected = (rank - 1 + world_size) % world_size
            self.assertEqual(results[rank], expected)

    def test_ordering_between_two_ranks(self):
        world_size = 2
        mp_manager = mp.Manager()
        results = mp_manager.dict()
        mp.spawn(
            _worker_ordering,
            args=(results,),
            nprocs=world_size,
            join=True,
        )

        # Rank 1 should receive messages [0, 1, 2, 3] in order.
        self.assertEqual(results[1], list(range(4)))


if __name__ == "__main__":
    unittest.main()


