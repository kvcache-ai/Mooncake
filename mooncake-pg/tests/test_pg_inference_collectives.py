import unittest

import torch
import torch.distributed as dist

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGWorkerContext,
)
from pg_test_topology import (
    build_dp_tp_topology,
    build_many_group_smoke_topology,
    build_tp_pp_topology,
    create_named_groups,
    destroy_named_groups,
    find_local_named_group,
)


def _local_group_by_prefix(
    groups: dict[str, object], prefix: str
) -> tuple[str | None, object | None]:
    for name, group in groups.items():
        if name.startswith(prefix):
            return name, group
    return None, None


def _tp_allreduce_many_groups_worker(ctx: MooncakePGWorkerContext) -> None:
    groups: dict[str, object] = {}
    try:
        topology = build_dp_tp_topology()
        assert topology.world_size == ctx.world_size
        device = ctx.init_group()
        groups = create_named_groups(topology, backend=ctx.backend_name)
        tp_spec, tp_group = find_local_named_group(topology, groups, ctx.rank, "tp")
        if tp_spec is None or tp_group is None:
            raise AssertionError(f"rank {ctx.rank} has no TP group")
        group_ranks = list(tp_spec.ranks)
        values = []
        for iteration in range(3):
            tensor = torch.tensor([ctx.rank + iteration + 1], dtype=torch.int32, device=device)
            dist.all_reduce(tensor, group=tp_group, op=dist.ReduceOp.SUM)
            values.append(int(tensor.item()))
        ctx.record_result({"values": values, "group_ranks": group_ranks})
    finally:
        destroy_named_groups(groups)


def _tp_allgather_many_groups_worker(ctx: MooncakePGWorkerContext) -> None:
    groups: dict[str, object] = {}
    try:
        topology = build_dp_tp_topology()
        assert topology.world_size == ctx.world_size
        device = ctx.init_group()
        groups = create_named_groups(topology, backend=ctx.backend_name)
        tp_spec, tp_group = find_local_named_group(topology, groups, ctx.rank, "tp")
        if tp_spec is None or tp_group is None:
            raise AssertionError(f"rank {ctx.rank} has no TP group")
        group_ranks = list(tp_spec.ranks)
        values = []
        for iteration in range(3):
            local = torch.tensor([ctx.rank * 10 + iteration], dtype=torch.int32, device=device)
            gathered = torch.empty(len(group_ranks), dtype=torch.int32, device=device)
            dist.all_gather_into_tensor(gathered, local, group=tp_group)
            values.append(gathered.tolist())
        ctx.record_result({"values": values, "group_ranks": group_ranks})
    finally:
        destroy_named_groups(groups)


def _pp_send_recv_smoke_worker(ctx: MooncakePGWorkerContext) -> None:
    groups: dict[str, object] = {}
    try:
        topology = build_tp_pp_topology()
        assert topology.world_size == ctx.world_size
        device = ctx.init_group()
        groups = create_named_groups(topology, backend=ctx.backend_name)

        lane_spec, lane_group = find_local_named_group(topology, groups, ctx.rank, "pp")
        if lane_group is None or lane_spec is None:
            raise AssertionError(f"rank {ctx.rank} has no PP lane group")

        if len(lane_spec.ranks) != 2:
            raise AssertionError(f"PP lane {lane_spec.name} is not 2-rank: {lane_spec.ranks}")
        local_index = lane_spec.ranks.index(ctx.rank)
        peer_group_rank = 1 - local_index
        src, dst = lane_spec.ranks
        direct_value = None
        batch_value = None
        if local_index == 0:
            direct = torch.tensor([src * 100 + dst], dtype=torch.int32, device=device)
            dist.send(direct, group=lane_group, group_dst=peer_group_rank)
            batch = torch.tensor([src * 1000 + dst], dtype=torch.int32, device=device)
            requests = dist.batch_isend_irecv(
                [dist.P2POp(dist.isend, batch, group=lane_group, group_peer=peer_group_rank)]
            )
            for request in requests:
                request.wait()
            direct_value = int(direct.item())
            batch_value = int(batch.item())
        else:
            direct = torch.empty(1, dtype=torch.int32, device=device)
            dist.recv(direct, group=lane_group, group_src=peer_group_rank)
            batch = torch.empty(1, dtype=torch.int32, device=device)
            requests = dist.batch_isend_irecv(
                [dist.P2POp(dist.irecv, batch, group=lane_group, group_peer=peer_group_rank)]
            )
            for request in requests:
                request.wait()
            direct_value = int(direct.item())
            batch_value = int(batch.item())

        ctx.record_result({"direct": direct_value, "batch": batch_value})
    finally:
        destroy_named_groups(groups)


def _overlapping_group_collective_worker(ctx: MooncakePGWorkerContext) -> None:
    groups: dict[str, object] = {}
    try:
        topology = build_many_group_smoke_topology()
        assert topology.world_size == ctx.world_size
        device = ctx.init_group()
        groups = create_named_groups(topology, backend=ctx.backend_name)

        _, tp_group = _local_group_by_prefix(groups, "tp")
        dp_name, dp_group = _local_group_by_prefix(groups, "dp")
        ep_name, _ = _local_group_by_prefix(groups, "ep")

        tp_tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tp_tensor, group=tp_group, op=dist.ReduceOp.SUM)

        dp_root = int(dp_name.removeprefix("dp"))
        dp_tensor = torch.tensor(
            [dp_root * 100 + 5 if ctx.rank == dp_root else -1], dtype=torch.int32, device=device
        )
        dist.broadcast(dp_tensor, src=dp_root, group=dp_group)

        ctx.record_result({
            "tp_value": int(tp_tensor.item()),
            "dp_value": int(dp_tensor.item()),
            "ep_group": list(next(spec.ranks for spec in topology.ordered_groups if spec.name == ep_name)),
        })
    finally:
        destroy_named_groups(groups)


class _InferenceCollectivesMixin:
    world_size = 8

    def test_tp_allreduce_many_groups(self) -> None:
        rows = self.spawn_backend_and_collect(_tp_allreduce_many_groups_worker)
        self.assert_all_ok(rows)

        for row in rows:
            ranks = row["group_ranks"]
            expected = [sum(peer + iteration + 1 for peer in ranks) for iteration in range(3)]
            self.assertEqual(row["values"], expected)

    def test_tp_allgather_many_groups(self) -> None:
        rows = self.spawn_backend_and_collect(_tp_allgather_many_groups_worker)
        self.assert_all_ok(rows)

        for row in rows:
            ranks = row["group_ranks"]
            expected = [[peer * 10 + iteration for peer in ranks] for iteration in range(3)]
            self.assertEqual(row["values"], expected)

    def test_overlapping_group_collective_traffic(self) -> None:
        rows = self.spawn_backend_and_collect(_overlapping_group_collective_worker)
        self.assert_all_ok(rows)


class _InferenceP2PSmokeMixin:
    world_size = 4

    def test_pp_send_recv_smoke(self) -> None:
        rows = self.spawn_backend_and_collect(_pp_send_recv_smoke_worker)
        self.assert_all_ok(rows)

        expected_direct = {0: 2, 1: 103, 2: 2, 3: 103}
        expected_batch = {0: 2, 1: 1003, 2: 2, 3: 1003}
        for row in rows:
            self.assertEqual(row["direct"], expected_direct[row["rank"]])
            self.assertEqual(row["batch"], expected_batch[row["rank"]])


class TestMooncakePGInferenceCollectivesCPU(
    _InferenceCollectivesMixin, MooncakePGCPUBackendTestCase
):
    pass


class TestMooncakePGInferenceP2PSmokeCPU(
    _InferenceP2PSmokeMixin, MooncakePGCPUBackendTestCase
):
    pass


if __name__ == "__main__":
    unittest.main()
