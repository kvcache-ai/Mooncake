from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from itertools import product
from math import prod

from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
    plan_placement_transfer,
    plan_placement_transfer_to_local_target,
)


@dataclass(frozen=True)
class RuntimeInputs(Sequence[WeightPlacementPart]):
    """One complete placement plus its per-participant runtime bindings."""

    placement: WeightPlacementManifest
    bindings: tuple[WeightRuntimeBindingManifest, ...]

    def __len__(self) -> int:
        return len(self.nonempty_parts)

    def __iter__(self) -> Iterator[WeightPlacementPart]:
        return iter(self.nonempty_parts)

    def __getitem__(self, index):
        return self.nonempty_parts[index]

    @property
    def nonempty_parts(self) -> tuple[WeightPlacementPart, ...]:
        return tuple(part for part in self.placement.parts if part.fragments)

    @property
    def fragments(self) -> tuple[PlacementFragment, ...]:
        return self.placement.fragments

    @property
    def binding(self) -> WeightRuntimeBindingManifest:
        if len(self.bindings) != 1:
            raise ValueError("runtime inputs do not identify one binding")
        return self.bindings[0]


def _canonical_strides_bytes(shape: tuple[int, ...], itemsize: int) -> tuple[int, ...]:
    strides = []
    running = itemsize
    for extent in reversed(shape):
        strides.append(running)
        running *= extent
    return tuple(reversed(strides))


def topology_for_participants(
    participants: Sequence[tuple[str, ParallelRank]],
) -> ParallelTopology:
    """Build a complete topology while keeping fixture-only participants minimal."""

    if not participants:
        raise ValueError("test topology requires at least one participant")
    participant_by_rank: dict[ParallelRank, str] = {}
    for participant_id, rank in participants:
        participant_by_rank.setdefault(rank, participant_id)

    sizes = {
        axis: max(getattr(rank, axis) for rank in participant_by_rank) + 1
        for axis in ("tp", "pp", "ep", "dp")
    }
    observed = {
        axis: {getattr(rank, axis) for rank in participant_by_rank} for axis in sizes
    }
    missing = {
        axis: sorted(set(range(size)) - observed[axis]) for axis, size in sizes.items()
    }
    placeholder_count = max((len(values) for values in missing.values()), default=0)
    defaults = {axis: min(observed[axis]) for axis in sizes}
    for index in range(placeholder_count):
        coordinates = {
            axis: values[index] if index < len(values) else defaults[axis]
            for axis, values in missing.items()
        }
        rank = ParallelRank(**coordinates)
        participant_by_rank.setdefault(
            rank,
            f"fixture-empty-d{rank.dp}-t{rank.tp}-p{rank.pp}-e{rank.ep}",
        )

    return ParallelTopology(
        tp_size=sizes["tp"],
        pp_size=sizes["pp"],
        ep_size=sizes["ep"],
        dp_size=sizes["dp"],
        participants=tuple(
            TopologyParticipant(participant_id, rank)
            for rank, participant_id in sorted(
                participant_by_rank.items(),
                key=lambda item: (
                    item[0].dp,
                    item[0].pp,
                    item[0].ep,
                    item[0].tp,
                ),
            )
        ),
    )


def global_placement_from_fragments(
    *,
    resource_id: str,
    revision: str,
    placement_set_id: str,
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
    participant_ids: dict[ParallelRank, str] | None = None,
    weight_generation: int = 1,
) -> WeightPlacementManifest:
    fragment_items = tuple(fragments)
    participant_ids = participant_ids or {
        rank: (f"{placement_set_id}-d{rank.dp}-t{rank.tp}-p{rank.pp}-e{rank.ep}")
        for rank in {fragment.rank for fragment in fragment_items}
    }
    topology = topology_for_participants(
        tuple(
            (participant_id, rank) for rank, participant_id in participant_ids.items()
        )
    )
    return WeightPlacementManifest.from_fragments(
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        topology=topology,
        tensors=tuple(tensors),
        fragments=fragment_items,
    )


def rebuild_placement(
    placement: WeightPlacementManifest,
    *,
    tensors: Sequence[TensorDescriptor] | None = None,
    fragments: Sequence[PlacementFragment] | None = None,
) -> WeightPlacementManifest:
    """Re-run the global collection barrier with selected logical content."""

    return WeightPlacementManifest.from_fragments(
        resource_id=placement.resource_id,
        revision=placement.revision,
        weight_generation=placement.weight_generation,
        placement_set_id=placement.placement_set_id,
        topology=placement.topology,
        tensors=tuple(tensors if tensors is not None else placement.tensors),
        fragments=tuple(fragments if fragments is not None else placement.fragments),
    )


def runtime_inputs_from_groups(
    *,
    resource_id: str,
    revision: str,
    placement_set_id: str,
    tensors: Sequence[TensorDescriptor],
    groups: Sequence[
        tuple[
            str,
            Sequence[PlacementFragment],
            Sequence[RuntimeBindingFragment],
        ]
    ],
    weight_generation: int = 1,
) -> RuntimeInputs:
    """Aggregate participant parts, then bind each non-empty participant."""

    grouped: dict[
        ParallelRank,
        tuple[str, list[PlacementFragment], list[RuntimeBindingFragment]],
    ] = {}
    for participant_id, placement_fragments, binding_fragments in groups:
        placement_items = tuple(placement_fragments)
        if not placement_items:
            raise ValueError("runtime fixture participant must contain fragments")
        ranks = {fragment.rank for fragment in placement_items}
        if len(ranks) != 1:
            raise ValueError("runtime fixture participant spans parallel ranks")
        rank = next(iter(ranks))
        if rank not in grouped:
            grouped[rank] = (participant_id, [], [])
        _, grouped_placements, grouped_bindings = grouped[rank]
        grouped_placements.extend(placement_items)
        grouped_bindings.extend(binding_fragments)

    participant_ids = {
        rank: participant_id for rank, (participant_id, _, _) in grouped.items()
    }
    placement = global_placement_from_fragments(
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        tensors=tensors,
        fragments=tuple(
            fragment
            for _, placement_fragments, _ in grouped.values()
            for fragment in placement_fragments
        ),
        participant_ids=participant_ids,
    )
    bindings = tuple(
        WeightRuntimeBindingManifest(
            resource_id=resource_id,
            revision=revision,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id=participant_id,
            participant_id=participant_id,
            generation=1,
            lease_id=f"{participant_id}-lease",
            fragments=tuple(binding_fragments),
        )
        for participant_id, _, binding_fragments in grouped.values()
    )
    return RuntimeInputs(placement, bindings)


def descriptor(
    *,
    global_shape: tuple[int, ...] = (8, 4),
    shard_dim: int = 0,
    fingerprint: str = "sglang:qwen3.5:bf16:v1",
) -> TensorDescriptor:
    return TensorDescriptor(
        tensor_id="layers.2.experts.3.w1",
        global_shape=global_shape,
        dtype="bfloat16",
        itemsize=2,
        shard_dims=(shard_dim,),
        layer_id=2,
        expert_id=3,
        layout_fingerprint=fingerprint,
        parallel_axes=(
            ReplicatedAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            OwnershipAxis(kind="ep"),
            SplitAxis(kind="tp", dim=shard_dim),
        ),
    )


def tp_manifests(
    *,
    tp: int,
    dp: int = 1,
    pp_rank: int,
    ep_rank: int,
    address_base: int,
    worker_prefix: str,
    tensor: TensorDescriptor | None = None,
) -> RuntimeInputs:
    tensor = tensor or descriptor()
    (dim,) = tensor.shard_dims
    assert tensor.global_shape[dim] % tp == 0
    extent = tensor.global_shape[dim] // tp
    groups = []
    for dp_rank, tp_rank in product(range(dp), range(tp)):
        local_shape = list(tensor.global_shape)
        local_shape[dim] = extent
        global_offset = [0] * len(tensor.global_shape)
        global_offset[dim] = tp_rank * extent
        worker_id = f"{worker_prefix}-d{dp_rank}-t{tp_rank}"
        placement_fragment_id = f"{worker_id}-placement"
        nbytes = prod(local_shape) * tensor.itemsize
        placement_fragment = PlacementFragment(
            placement_fragment_id=placement_fragment_id,
            tensor_id=tensor.tensor_id,
            global_offset=tuple(global_offset),
            local_shape=tuple(local_shape),
            nbytes=nbytes,
            rank=ParallelRank(
                dp=dp_rank,
                tp=tp_rank,
                pp=pp_rank,
                ep=ep_rank,
            ),
        )
        groups.append(
            (
                worker_id,
                (placement_fragment,),
                (
                    RuntimeBindingFragment(
                        placement_fragment_id=placement_fragment_id,
                        fragment_id=f"{worker_id}-fragment",
                        address=address_base + (dp_rank * tp + tp_rank) * 0x1000,
                        nbytes=nbytes,
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        device="cuda:0",
                        itemsize=tensor.itemsize,
                        local_shape=tuple(local_shape),
                        strides_bytes=_canonical_strides_bytes(
                            tuple(local_shape), tensor.itemsize
                        ),
                        storage_address=(
                            address_base + (dp_rank * tp + tp_rank) * 0x1000
                        ),
                        storage_nbytes=nbytes,
                        storage_offset_bytes=0,
                    ),
                ),
            )
        )
    return runtime_inputs_from_groups(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id=worker_prefix,
        tensors=(tensor,),
        groups=groups,
    )


def plan_transfer(source: RuntimeInputs, target: RuntimeInputs):
    return plan_placement_transfer(source.placement, target.placement)


def plan_transfer_to_local_target(
    source: RuntimeInputs,
    target: RuntimeInputs,
    *,
    target_index: int = 0,
):
    target_binding = target.bindings[target_index]
    return plan_placement_transfer_to_local_target(
        source.placement,
        target.placement,
        target_binding.participant_id,
    )


def combine_runtime_inputs(*inputs: RuntimeInputs) -> RuntimeInputs:
    if not inputs:
        raise ValueError("runtime input collection must not be empty")
    first = inputs[0].placement
    groups = []
    tensors: dict[str, TensorDescriptor] = {}
    for item in inputs:
        placement = item.placement
        if (
            placement.resource_id != first.resource_id
            or placement.revision != first.revision
            or placement.weight_generation != first.weight_generation
        ):
            raise ValueError("runtime input identities differ")
        tensors.update({tensor.tensor_id: tensor for tensor in placement.tensors})
        parts = {part.participant_id: part for part in placement.parts}
        groups.extend(
            (
                binding.participant_id,
                parts[binding.participant_id].fragments,
                binding.fragments,
            )
            for binding in item.bindings
        )
    return runtime_inputs_from_groups(
        resource_id=first.resource_id,
        revision=first.revision,
        weight_generation=first.weight_generation,
        placement_set_id=f"combined-{first.placement_set_id}",
        tensors=tuple(tensors.values()),
        groups=groups,
    )


def replace_placement_fragment(
    inputs: RuntimeInputs,
    index: int,
    fragment: PlacementFragment,
) -> RuntimeInputs:
    current_fragment = inputs.fragments[index]
    parts = {part.participant_id: part for part in inputs.placement.parts}
    groups = []
    for binding in inputs.bindings:
        placement_fragments = tuple(
            fragment if item == current_fragment else item
            for item in parts[binding.participant_id].fragments
        )
        groups.append((binding.participant_id, placement_fragments, binding.fragments))
    return runtime_inputs_from_groups(
        resource_id=inputs.placement.resource_id,
        revision=inputs.placement.revision,
        weight_generation=inputs.placement.weight_generation,
        placement_set_id=inputs.placement.placement_set_id,
        tensors=inputs.placement.tensors,
        groups=groups,
    )


def operation_for_target(plan, tp_rank: int, dp_rank: int = 0):
    return [
        operation
        for operation in plan.operations
        if operation.target.rank.tp == tp_rank and operation.target.rank.dp == dp_rank
    ]


def distribute_tp_shards_across_ep_ranks(manifests: RuntimeInputs) -> RuntimeInputs:
    parts = {part.participant_id: part for part in manifests.placement.parts}
    groups = []
    for binding in manifests.bindings:
        fragments = tuple(
            replace(
                fragment,
                rank=replace(fragment.rank, ep=fragment.rank.tp),
            )
            for fragment in parts[binding.participant_id].fragments
        )
        groups.append((binding.participant_id, fragments, binding.fragments))
    return runtime_inputs_from_groups(
        resource_id=manifests.placement.resource_id,
        revision=manifests.placement.revision,
        weight_generation=manifests.placement.weight_generation,
        placement_set_id=manifests.placement.placement_set_id,
        tensors=manifests.placement.tensors,
        groups=groups,
    )


class CountingFragment:
    def __init__(self, fragment: PlacementFragment, accesses: list[int]) -> None:
        self._fragment = fragment
        self._accesses = accesses

    @property
    def tensor_id(self) -> str:
        self._accesses[0] += 1
        return self._fragment.tensor_id

    def __getattr__(self, name: str):
        return getattr(self._fragment, name)
