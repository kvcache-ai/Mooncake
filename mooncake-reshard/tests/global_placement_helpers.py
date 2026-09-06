from __future__ import annotations

from collections.abc import Callable, Sequence

from mooncake.reshard.weight import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)


def participant_id(prefix: str, rank: ParallelRank) -> str:
    return f"{prefix}-d{rank.dp}-p{rank.pp}-e{rank.ep}-t{rank.tp}"


def contiguous_strides_bytes(
    local_shape: Sequence[int], itemsize: int
) -> tuple[int, ...]:
    stride = itemsize
    result = []
    for extent in reversed(local_shape):
        result.append(stride)
        stride *= extent
    return tuple(reversed(result))


def runtime_fragment(
    *,
    placement: PlacementFragment,
    tensor: TensorDescriptor,
    fragment_id: str,
    address: int,
    worker_id: str,
    endpoint: str,
    device: str = "cuda:0",
    storage_address: int | None = None,
    storage_nbytes: int | None = None,
    owner=None,
) -> RuntimeBindingFragment:
    storage_address = address if storage_address is None else storage_address
    storage_nbytes = placement.nbytes if storage_nbytes is None else storage_nbytes
    return RuntimeBindingFragment(
        placement_fragment_id=placement.placement_fragment_id,
        fragment_id=fragment_id,
        address=address,
        nbytes=placement.nbytes,
        worker_id=worker_id,
        endpoint=endpoint,
        device=device,
        itemsize=tensor.itemsize,
        local_shape=placement.local_shape,
        strides_bytes=contiguous_strides_bytes(placement.local_shape, tensor.itemsize),
        storage_address=storage_address,
        storage_nbytes=storage_nbytes,
        storage_offset_bytes=address - storage_address,
        owner=owner,
    )


def global_placement(
    *,
    resource_id: str,
    revision: str,
    placement_set_id: str,
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
    ranks: Sequence[ParallelRank] | None = None,
    weight_generation: int = 1,
) -> WeightPlacementManifest:
    rank_items = tuple(
        sorted(
            set(ranks or (fragment.rank for fragment in fragments)),
            key=lambda rank: (rank.dp, rank.pp, rank.ep, rank.tp),
        )
    )
    if not rank_items:
        raise ValueError("test placement ranks must not be empty")
    topology = ParallelTopology(
        tp_size=max(rank.tp for rank in rank_items) + 1,
        pp_size=max(rank.pp for rank in rank_items) + 1,
        ep_size=max(rank.ep for rank in rank_items) + 1,
        dp_size=max(rank.dp for rank in rank_items) + 1,
        participants=tuple(
            TopologyParticipant(
                participant_id(placement_set_id, rank),
                rank,
            )
            for rank in rank_items
        ),
    )
    return WeightPlacementManifest.from_fragments(
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        topology=topology,
        tensors=tuple(tensors),
        fragments=tuple(fragments),
    )


def runtime_bindings(
    placement: WeightPlacementManifest,
    *,
    instance_prefix: str,
    address_for_fragment: Callable[[int, PlacementFragment], int] | None = None,
    generation: int = 1,
) -> tuple[WeightRuntimeBindingManifest, ...]:
    tensors = {tensor.tensor_id: tensor for tensor in placement.tensors}
    fragment_index = {
        fragment.placement_fragment_id: index
        for index, fragment in enumerate(placement.fragments)
    }
    address_for_fragment = address_for_fragment or (
        lambda index, fragment: 0x100000 + index * 0x10000
    )
    return tuple(
        WeightRuntimeBindingManifest(
            resource_id=placement.resource_id,
            revision=placement.revision,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            participant_id=part.participant_id,
            instance_id=f"{instance_prefix}-{part.participant_id}",
            generation=generation,
            lease_id=f"lease-{instance_prefix}-{part.participant_id}",
            fragments=tuple(
                runtime_fragment(
                    placement=fragment,
                    tensor=tensors[fragment.tensor_id],
                    fragment_id=(
                        f"runtime-{instance_prefix}-{fragment.placement_fragment_id}"
                    ),
                    address=address_for_fragment(
                        fragment_index[fragment.placement_fragment_id], fragment
                    ),
                    worker_id=part.participant_id,
                    endpoint=f"{part.participant_id}:12345",
                )
                for fragment in part.fragments
            ),
        )
        for part in placement.parts
    )
