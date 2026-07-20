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
)


def _ring_send_recv_worker(
    ctx: MooncakePGWorkerContext,
) -> None:
    device = ctx.init_group()
    send_tensor = torch.tensor([ctx.rank], dtype=torch.int64, device=device)
    recv_tensor = torch.empty_like(send_tensor)
    dst = (ctx.rank + 1) % ctx.world_size
    src = (ctx.rank - 1 + ctx.world_size) % ctx.world_size
    ops = [
        dist.P2POp(op=dist.isend, tensor=send_tensor, peer=dst),
        dist.P2POp(op=dist.irecv, tensor=recv_tensor, peer=src),
    ]
    works = dist.batch_isend_irecv(ops)
    for work in works:
        work.wait()
    ctx.synchronize()
    ctx.record_result({"value": int(recv_tensor.cpu().item())})


def _direct_and_batch_send_recv_worker(
    ctx: MooncakePGWorkerContext,
) -> None:
    if ctx.world_size != 2:
        raise AssertionError("direct send/recv smoke expects world_size=2")
    device = ctx.init_group()
    peer = 1 - ctx.rank
    if ctx.rank == 0:
        direct = torch.tensor([12], dtype=torch.int32, device=device)
        dist.send(direct, dst=peer)
        batch = torch.tensor([1200], dtype=torch.int32, device=device)
        requests = dist.batch_isend_irecv([dist.P2POp(dist.isend, batch, peer=peer)])
        for request in requests:
            request.wait()
        value = {"direct": int(direct.cpu().item()), "batch": int(batch.cpu().item())}
    else:
        direct = torch.empty(1, dtype=torch.int32, device=device)
        dist.recv(direct, src=peer)
        batch = torch.empty(1, dtype=torch.int32, device=device)
        requests = dist.batch_isend_irecv([dist.P2POp(dist.irecv, batch, peer=peer)])
        for request in requests:
            request.wait()
        value = {"direct": int(direct.cpu().item()), "batch": int(batch.cpu().item())}
    ctx.synchronize()
    ctx.record_result(value)


def _ordering_worker(
    ctx: MooncakePGWorkerContext,
) -> None:
    device = ctx.init_group()
    if ctx.rank >= 2:
        ctx.record_result({"value": "skip"})
        return

    num_msgs = 4
    if ctx.rank == 0:
        send_tensors = [torch.tensor([i], dtype=torch.int64, device=device) for i in range(num_msgs)]
        ops = [dist.P2POp(op=dist.isend, tensor=t, peer=1) for t in send_tensors]
        works = dist.batch_isend_irecv(ops)
        for work in works:
            work.wait()
        value = "ok"
    else:
        recv_tensors = [torch.empty(1, dtype=torch.int64, device=device) for _ in range(num_msgs)]
        ops = [dist.P2POp(op=dist.irecv, tensor=t, peer=0) for t in recv_tensors]
        works = dist.batch_isend_irecv(ops)
        for work in works:
            work.wait()
        value = [int(t.cpu().item()) for t in recv_tensors]
    ctx.synchronize()
    ctx.record_result({"value": value})


def _multiple_senders_worker(
    ctx: MooncakePGWorkerContext,
) -> None:
    device = ctx.init_group()
    if ctx.rank == 0:
        send_tensor = torch.tensor([100], dtype=torch.int64, device=device)
        recv_tensor = torch.empty(1, dtype=torch.int64, device=device)
        ops = [
            dist.P2POp(op=dist.isend, tensor=send_tensor, peer=1),
            dist.P2POp(op=dist.irecv, tensor=recv_tensor, peer=1),
        ]
        works = dist.batch_isend_irecv(ops)
        for work in works:
            work.wait()
        value = int(recv_tensor.cpu().item())
    elif ctx.rank == 1:
        recv_from_0 = torch.empty(1, dtype=torch.int64, device=device)
        recv_from_2 = torch.empty(1, dtype=torch.int64, device=device)
        send_tensor = torch.tensor([101], dtype=torch.int64, device=device)
        ops = [
            dist.P2POp(op=dist.irecv, tensor=recv_from_0, peer=0),
            dist.P2POp(op=dist.irecv, tensor=recv_from_2, peer=2),
            dist.P2POp(op=dist.isend, tensor=send_tensor, peer=0),
        ]
        works = dist.batch_isend_irecv(ops)
        for work in works:
            work.wait()
        value = [int(recv_from_0.cpu().item()), int(recv_from_2.cpu().item())]
    elif ctx.rank == 2:
        send_tensor = torch.tensor([200], dtype=torch.int64, device=device)
        works = dist.batch_isend_irecv([dist.P2POp(op=dist.isend, tensor=send_tensor, peer=1)])
        for work in works:
            work.wait()
        value = "ok"
    else:
        value = "skip"
    ctx.synchronize()
    ctx.record_result({"value": value})


BROKEN_RANK = 1


def _p2p_fault_detection_worker(
    ctx: MooncakePGWorkerContext,
    broken_exited,
) -> None:
    """P2P fault tolerance: 2 rounds of all-to-all.
    Round 1: all healthy -> failedRanks = 0s.
    Round 2: rank 1 exited -> peer's failedRanks[1] = 1
    """

    def p2p_all_to_all(ctx, device):
        """All-to-all P2P send/recv. Returns work handles."""
        ops = []
        for p in range(ctx.world_size):
            if p == ctx.rank:
                continue
            s = torch.tensor([ctx.rank], dtype=torch.int64, device=device)
            r = torch.empty_like(s)
            ops.append(dist.P2POp(op=dist.isend, tensor=s, peer=p))
            ops.append(dist.P2POp(op=dist.irecv, tensor=r, peer=p))
        return dist.batch_isend_irecv(ops)

    device = ctx.init_group()

    # Round 1: all healthy
    works = p2p_all_to_all(ctx, device)
    for w in works:
        w.wait()
    ctx.synchronize()
    for w in works:
        failed_ranks = pg.get_failed_ranks(w)
        assert (
            failed_ranks.cpu().tolist() == [0] * ctx.world_size
        ), f"rank {ctx.rank} round 1: failed_ranks={failed_ranks.cpu().tolist()}"

    if ctx.rank == BROKEN_RANK:
        ctx.record_result({"role": "broken"})
        broken_exited.set()
        os._exit(0)

    broken_exited.wait()

    # Round 2: BROKEN_RANK is dead
    works = p2p_all_to_all(ctx, device)
    for w in works:
        w.wait()

    normal_failed_ranks = [0] * ctx.world_size
    broken_peer_failed_ranks = [0] * ctx.world_size
    broken_peer_failed_ranks[BROKEN_RANK] = 1
    peers = [p for p in range(ctx.world_size) if p != ctx.rank]
    for w, peer in zip(works, [p for p in peers for _ in range(2)]):
        failed_ranks = pg.get_failed_ranks(w)
        expected = (
            broken_peer_failed_ranks if peer == BROKEN_RANK else normal_failed_ranks
        )
        assert failed_ranks.cpu().tolist() == expected

    ctx.record_result({"role": "survivor"})


class _P2PMixin:
    world_size = 4

    def test_ring_send_recv(self) -> None:
        rows = self.spawn_backend_and_collect(_ring_send_recv_worker)
        self.assert_all_ok(rows)
        for row in rows:
            expected = (row["rank"] - 1 + self.world_size) % self.world_size
            self.assertEqual(row["value"], expected)

    def test_direct_and_batch_send_recv(self) -> None:
        rows = self.spawn_backend_and_collect(
            _direct_and_batch_send_recv_worker,
            world_size=2,
            nprocs=2,
        )
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["direct"], 12)
            self.assertEqual(row["batch"], 1200)

    def test_ordering_between_two_ranks(self) -> None:
        rows = self.spawn_backend_and_collect(_ordering_worker)
        self.assert_all_ok(rows)
        rank1 = next(row for row in rows if row["rank"] == 1)
        self.assertEqual(rank1["value"], list(range(4)))

    def test_p2p_fault_detection(self) -> None:
        """Test P2P fault detection: failedRanks and activeRanks on P2P failure."""
        spawn_ctx = mp.get_context("spawn")
        broken_exited = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _p2p_fault_detection_worker,
            broken_exited,
            world_size=3,
            nprocs=3,
            timeout_s=60.0,
        )

        survivor_rows = [r for r in rows if r.get("role") == "survivor"]
        broken_rows = [r for r in rows if r.get("role") == "broken"]
        self.assertEqual(len(survivor_rows), 2)
        self.assertGreaterEqual(len(broken_rows), 1)

    def test_multiple_senders_to_same_receiver(self) -> None:
        if self.world_size < 3:
            self.skipTest("multiple-sender P2P coverage requires at least 3 ranks")
        rows = self.spawn_backend_and_collect(_multiple_senders_worker)
        self.assert_all_ok(rows)
        rank0 = next(row for row in rows if row["rank"] == 0)
        rank1 = next(row for row in rows if row["rank"] == 1)
        rank2 = next(row for row in rows if row["rank"] == 2)
        self.assertEqual(rank0["value"], 101)
        self.assertIn(100, rank1["value"])
        self.assertIn(200, rank1["value"])
        self.assertEqual(len(rank1["value"]), 2)
        self.assertEqual(rank2["value"], "ok")


class TestMooncakePGP2PCPU(_P2PMixin, MooncakePGCPUBackendTestCase):
    pass


class TestMooncakePGP2PCUDA(_P2PMixin, MooncakePGCUDABackendTestCase):

    @classmethod
    def configure_for_cuda_device_count(cls, device_count: int) -> None:
        if device_count < 2:
            return
        cls.world_size = min(device_count, 4)


if __name__ == "__main__":
    unittest.main()
