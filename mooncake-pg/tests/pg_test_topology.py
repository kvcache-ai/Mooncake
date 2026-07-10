from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import torch.distributed as dist


@dataclass(frozen=True)
class GroupSpec:
    name: str
    family: str
    ranks: tuple[int, ...]


@dataclass(frozen=True)
class TopologySpec:
    world_size: int
    ordered_groups: tuple[GroupSpec, ...]
    p2p_lanes: tuple[tuple[int, int], ...] = ()

    def membership_for_rank(self, rank: int) -> dict[str, list[list[int]]]:
        membership: dict[str, list[list[int]]] = {}
        for group in self.ordered_groups:
            if rank in group.ranks:
                membership.setdefault(group.family, []).append(list(group.ranks))
        return membership


def find_local_group_spec(
    topology: TopologySpec,
    rank: int,
    families: str | Iterable[str],
) -> GroupSpec | None:
    if isinstance(families, str):
        families = (families,)
    family_set = set(families)
    for group_spec in topology.ordered_groups:
        if group_spec.family in family_set and rank in group_spec.ranks:
            return group_spec
    return None


def find_local_named_group(
    topology: TopologySpec,
    groups: dict[str, object],
    rank: int,
    families: str | Iterable[str],
) -> tuple[GroupSpec | None, object | None]:
    group_spec = find_local_group_spec(topology, rank, families)
    if group_spec is None:
        return None, None
    return group_spec, groups.get(group_spec.name)

def create_named_groups(
    topology: TopologySpec,
    *,
    backend: str = "mooncake-cpu",
) -> dict[str, object]:
    groups: dict[str, object] = {}
    rank = dist.get_rank()
    for group_spec in topology.ordered_groups:
        group = dist.new_group(ranks=list(group_spec.ranks), backend=backend)
        if rank in group_spec.ranks:
            groups[group_spec.name] = group
    return groups


def destroy_named_groups(groups: dict[str, object]) -> None:
    for name in reversed(list(groups.keys())):
        try:
            dist.destroy_process_group(groups[name])
        except Exception:
            pass


def build_tp_only_topology() -> TopologySpec:
    return TopologySpec(
        world_size=4,
        ordered_groups=(
            GroupSpec("tp0", "tp", (0, 1, 2, 3)),
        ),
    )


def build_tp_only_topology_2() -> TopologySpec:
    return TopologySpec(
        world_size=2,
        ordered_groups=(
            GroupSpec("tp0", "tp", (0, 1)),
        ),
    )


def build_dp_tp_topology() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp0", "tp", (0, 1, 2, 3)),
        GroupSpec("tp1", "tp", (4, 5, 6, 7)),
        GroupSpec("dp0", "dp", (0, 4)),
        GroupSpec("dp1", "dp", (1, 5)),
        GroupSpec("dp2", "dp", (2, 6)),
        GroupSpec("dp3", "dp", (3, 7)),
    )
    return TopologySpec(world_size=8, ordered_groups=ordered_groups)


def build_dp_tp_topology_4() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp0", "tp", (0, 1)),
        GroupSpec("tp1", "tp", (2, 3)),
        GroupSpec("dp0", "dp", (0, 2)),
        GroupSpec("dp1", "dp", (1, 3)),
    )
    return TopologySpec(world_size=4, ordered_groups=ordered_groups)


def build_tp_pp_topology() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp_stage0", "tp", (0, 1)),
        GroupSpec("tp_stage1", "tp", (2, 3)),
        GroupSpec("pp_lane0", "pp", (0, 2)),
        GroupSpec("pp_lane1", "pp", (1, 3)),
    )
    return TopologySpec(
        world_size=4,
        ordered_groups=ordered_groups,
        p2p_lanes=((0, 2), (1, 3)),
    )


def build_tp_pp_topology_2() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp_stage0", "tp", (0, 1)),
        GroupSpec("pp_lane0", "pp", (0, 1)),
    )
    return TopologySpec(
        world_size=2,
        ordered_groups=ordered_groups,
        p2p_lanes=((0, 1),),
    )


def build_dp_tp_ep_topology() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp_dp0_ep0", "tp", (0, 2)),
        GroupSpec("tp_dp0_ep1", "tp", (1, 3)),
        GroupSpec("tp_dp1_ep0", "tp", (4, 6)),
        GroupSpec("tp_dp1_ep1", "tp", (5, 7)),
        GroupSpec("dp_tp0_ep0", "dp", (0, 4)),
        GroupSpec("dp_tp0_ep1", "dp", (1, 5)),
        GroupSpec("dp_tp1_ep0", "dp", (2, 6)),
        GroupSpec("dp_tp1_ep1", "dp", (3, 7)),
        GroupSpec("ep_dp0_tp0", "ep", (0, 1)),
        GroupSpec("ep_dp0_tp1", "ep", (2, 3)),
        GroupSpec("ep_dp1_tp0", "ep", (4, 5)),
        GroupSpec("ep_dp1_tp1", "ep", (6, 7)),
    )
    return TopologySpec(world_size=8, ordered_groups=ordered_groups)


def build_dp_tp_ep_topology_4() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp_dp0_ep0", "tp", (0, 2)),
        GroupSpec("tp_dp0_ep1", "tp", (1, 3)),
        GroupSpec("dp_tp0_ep0", "dp", (0, 1)),
        GroupSpec("dp_tp1_ep0", "dp", (2, 3)),
        GroupSpec("ep_dp0_tp0", "ep", (0, 1)),
        GroupSpec("ep_dp1_tp0", "ep", (2, 3)),
    )
    return TopologySpec(world_size=4, ordered_groups=ordered_groups)


def build_prefill_decode_topology() -> TopologySpec:
    ordered_groups = (
        GroupSpec("prefill", "service", (0, 1)),
        GroupSpec("decode", "service", (2, 3)),
        GroupSpec("lane0", "lane", (0, 2)),
        GroupSpec("lane1", "lane", (1, 3)),
    )
    return TopologySpec(
        world_size=4,
        ordered_groups=ordered_groups,
        p2p_lanes=((0, 2), (1, 3)),
    )


def build_prefill_decode_topology_2() -> TopologySpec:
    ordered_groups = (
        GroupSpec("prefill", "service", (0,)),
        GroupSpec("decode", "service", (1,)),
        GroupSpec("lane0", "lane", (0, 1)),
    )
    return TopologySpec(
        world_size=2,
        ordered_groups=ordered_groups,
        p2p_lanes=((0, 1),),
    )


def build_many_group_smoke_topology() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp0", "tp", (0, 1, 2, 3)),
        GroupSpec("tp1", "tp", (4, 5, 6, 7)),
        GroupSpec("dp0", "dp", (0, 4)),
        GroupSpec("dp1", "dp", (1, 5)),
        GroupSpec("dp2", "dp", (2, 6)),
        GroupSpec("dp3", "dp", (3, 7)),
        GroupSpec("ep0", "ep", (0, 1)),
        GroupSpec("ep1", "ep", (2, 3)),
        GroupSpec("ep2", "ep", (4, 5)),
        GroupSpec("ep3", "ep", (6, 7)),
        GroupSpec("lane0", "pp", (0, 6)),
        GroupSpec("lane1", "pp", (1, 7)),
        GroupSpec("lane2", "pp", (2, 4)),
        GroupSpec("lane3", "pp", (3, 5)),
    )
    return TopologySpec(
        world_size=8,
        ordered_groups=ordered_groups,
        p2p_lanes=((0, 6), (1, 7), (2, 4), (3, 5)),
    )


def build_many_group_smoke_topology_4() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp0", "tp", (0, 1)),
        GroupSpec("tp1", "tp", (2, 3)),
        GroupSpec("dp0", "dp", (0, 2)),
        GroupSpec("dp1", "dp", (1, 3)),
        GroupSpec("ep0", "ep", (0, 3)),
        GroupSpec("ep1", "ep", (1, 2)),
    )
    return TopologySpec(world_size=4, ordered_groups=ordered_groups)


def build_many_group_smoke_topology_2() -> TopologySpec:
    ordered_groups = (
        GroupSpec("tp0", "tp", (0, 1)),
        GroupSpec("dp0", "dp", (0, 1)),
    )
    return TopologySpec(
        world_size=2,
        ordered_groups=ordered_groups,
    )
