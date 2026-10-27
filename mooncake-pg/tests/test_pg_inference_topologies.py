import unittest

import torch
import torch.distributed as dist

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGWorkerContext,
)
from pg_test_topology import (
    build_dp_tp_ep_topology,
    build_dp_tp_topology,
    build_prefill_decode_topology,
    build_tp_only_topology,
    build_tp_pp_topology,
    create_named_groups,
    destroy_named_groups,
    find_local_named_group,
)

TOPOLOGY_BUILDERS = {
    "tp_only": build_tp_only_topology,
    "dp_tp": build_dp_tp_topology,
    "tp_pp": build_tp_pp_topology,
    "dp_tp_ep": build_dp_tp_ep_topology,
    "prefill_decode": build_prefill_decode_topology,
}


def _run_lane_send_recv(
    topology,
    groups: dict[str, object],
    rank: int,
    device: torch.device,
) -> dict[str, int | None]:
    lane_spec, lane_group = find_local_named_group(
        topology, groups, rank, ("pp", "lane")
    )
    if lane_group is None or lane_spec is None:
        return {"lane_sent": None, "lane_recv": None}

    if len(lane_spec.ranks) != 2:
        raise AssertionError(f"lane {lane_spec.name} is not 2-rank: {lane_spec.ranks}")
    local_index = lane_spec.ranks.index(rank)
    peer_group_rank = 1 - local_index
    src, dst = lane_spec.ranks
    if local_index == 0:
        value = src * 100 + dst
        tensor = torch.tensor([value], dtype=torch.int32, device=device)
        dist.send(tensor, group=lane_group, group_dst=peer_group_rank)
        return {"lane_sent": value, "lane_recv": None}

    tensor = torch.empty(1, dtype=torch.int32, device=device)
    dist.recv(tensor, group=lane_group, group_src=peer_group_rank)
    return {"lane_sent": None, "lane_recv": int(tensor.item())}


def _topology_worker(
    ctx: MooncakePGWorkerContext,
    topology_name: str,
) -> None:
    groups: dict[str, object] = {}
    try:
        topology = TOPOLOGY_BUILDERS[topology_name]()
        if topology.world_size != ctx.world_size:
            raise AssertionError(
                f"{topology_name} expects world_size={topology.world_size}, got {ctx.world_size}"
            )

        device = ctx.init_group()
        groups = create_named_groups(topology, backend=ctx.backend_name)
        membership = topology.membership_for_rank(ctx.rank)
        payload: dict[str, object] = {
            "membership": membership,
        }

        tp_spec, tp_group = find_local_named_group(topology, groups, ctx.rank, "tp")
        if tp_group is not None:
            tp_tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
            dist.all_reduce(tp_tensor, group=tp_group, op=dist.ReduceOp.SUM)
            payload["tp_group"] = list(tp_spec.ranks)
            payload["tp_value"] = int(tp_tensor.item())

        dp_spec, dp_group = find_local_named_group(topology, groups, ctx.rank, "dp")
        if dp_group is not None:
            root_rank = min(dp_spec.ranks)
            dp_tensor = torch.tensor(
                [root_rank * 100 + 7 if ctx.rank == root_rank else -1],
                dtype=torch.int32,
                device=device,
            )
            dist.broadcast(dp_tensor, src=root_rank, group=dp_group)
            payload["dp_group"] = list(dp_spec.ranks)
            payload["dp_value"] = int(dp_tensor.item())

        ep_spec, ep_group = find_local_named_group(topology, groups, ctx.rank, "ep")
        if ep_group is not None:
            payload["ep_group"] = list(ep_spec.ranks)

        service_spec, service_group = find_local_named_group(
            topology, groups, ctx.rank, "service"
        )
        if service_group is not None:
            service_tensor = torch.tensor(
                [ctx.rank + 1], dtype=torch.int32, device=device
            )
            dist.all_reduce(
                service_tensor, group=service_group, op=dist.ReduceOp.SUM
            )
            payload["service_group"] = list(service_spec.ranks)
            payload["service_value"] = int(service_tensor.item())

        payload.update(_run_lane_send_recv(topology, groups, ctx.rank, device))
        ctx.record_result(payload)
    finally:
        destroy_named_groups(groups)


class _InferenceTopologiesMixin:
    world_size = 4

    def test_tp_only_topology(self) -> None:
        rows = self.spawn_backend_and_collect(_topology_worker, "tp_only")
        self.assert_all_ok(rows)

        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["tp_value"], expected)

    def test_tp_pp_topology(self) -> None:
        rows = self.spawn_backend_and_collect(_topology_worker, "tp_pp")
        self.assert_all_ok(rows)

        expected_tp = {
            0: 3,
            1: 3,
            2: 7,
            3: 7,
        }
        expected_lane_recv = {2: 2, 3: 103}
        for row in rows:
            self.assertEqual(row["tp_value"], expected_tp[row["rank"]])
            if row["rank"] in expected_lane_recv:
                self.assertEqual(row["lane_recv"], expected_lane_recv[row["rank"]])

    def test_prefill_decode_topology(self) -> None:
        rows = self.spawn_backend_and_collect(_topology_worker, "prefill_decode")
        self.assert_all_ok(rows)

        expected_service = {0: 3, 1: 3, 2: 7, 3: 7}
        expected_lane_recv = {2: 2, 3: 103}
        for row in rows:
            self.assertEqual(row["service_value"], expected_service[row["rank"]])
            if row["rank"] in expected_lane_recv:
                self.assertEqual(row["lane_recv"], expected_lane_recv[row["rank"]])


class _InferenceTopologiesLargeMixin:
    world_size = 8

    def test_dp_tp_topology(self) -> None:
        rows = self.spawn_backend_and_collect(_topology_worker, "dp_tp")
        self.assert_all_ok(rows)

        expected_tp = {0: 10, 1: 10, 2: 10, 3: 10, 4: 26, 5: 26, 6: 26, 7: 26}
        expected_dp = {
            0: 7,
            4: 7,
            1: 107,
            5: 107,
            2: 207,
            6: 207,
            3: 307,
            7: 307,
        }
        for row in rows:
            self.assertEqual(row["tp_value"], expected_tp[row["rank"]])
            self.assertEqual(row["dp_value"], expected_dp[row["rank"]])

    def test_dp_tp_ep_topology(self) -> None:
        rows = self.spawn_backend_and_collect(_topology_worker, "dp_tp_ep")
        self.assert_all_ok(rows)

        expected_tp = {0: 4, 2: 4, 1: 6, 3: 6, 4: 12, 6: 12, 5: 14, 7: 14}
        expected_dp = {
            0: 7,
            4: 7,
            1: 107,
            5: 107,
            2: 207,
            6: 207,
            3: 307,
            7: 307,
        }
        for row in rows:
            self.assertEqual(row["tp_value"], expected_tp[row["rank"]])
            self.assertEqual(row["dp_value"], expected_dp[row["rank"]])


class TestMooncakePGInferenceTopologiesCPU(
    _InferenceTopologiesMixin, MooncakePGCPUBackendTestCase
):
    pass


class TestMooncakePGInferenceTopologiesLargeCPU(
    _InferenceTopologiesLargeMixin, MooncakePGCPUBackendTestCase
):
    pass


if __name__ == "__main__":
    unittest.main()
