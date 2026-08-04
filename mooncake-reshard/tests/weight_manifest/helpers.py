from __future__ import annotations

from dataclasses import asdict

from mooncake.reshard.weight import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorParallelAxis,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
)


MODEL_ID = "model"
REVISION = "revision"
WEIGHT_GENERATION = 7
PLACEMENT_SET_ID = "placement-set-7"
PARTICIPANT_ID = "worker-0"


def parallel_topology(
    *,
    participants: tuple[TopologyParticipant, ...] | None = None,
    **overrides,
) -> ParallelTopology:
    participants = participants or (
        TopologyParticipant(PARTICIPANT_ID, ParallelRank()),
    )
    values = {
        "tp_size": max(item.rank.tp for item in participants) + 1,
        "pp_size": max(item.rank.pp for item in participants) + 1,
        "ep_size": max(item.rank.ep for item in participants) + 1,
        "dp_size": max(item.rank.dp for item in participants) + 1,
        "participants": participants,
    }
    values.update(overrides)
    return ParallelTopology(**values)


def descriptor(**overrides) -> TensorDescriptor:
    values = {
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (4, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "test:qwen:bf16:v1",
        "parallel_axes": (TensorParallelAxis(kind="tp", split_dim=0),),
    }
    values.update(overrides)
    return TensorDescriptor(**values)


def placement_fragment(**overrides) -> PlacementFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "nbytes": 32,
        "rank": ParallelRank(),
    }
    values.update(overrides)
    return PlacementFragment(**values)


def placement_part(
    *,
    topology: ParallelTopology | None = None,
    participant_id: str = PARTICIPANT_ID,
    rank: ParallelRank | None = None,
    tensors: tuple[TensorDescriptor, ...] = (),
    fragments: tuple[PlacementFragment, ...] = (),
    **overrides,
) -> WeightPlacementPart:
    topology = topology or parallel_topology()
    rank = rank or topology.participant(participant_id).rank
    values = {
        "resource_id": MODEL_ID,
        "revision": REVISION,
        "weight_generation": WEIGHT_GENERATION,
        "placement_set_id": PLACEMENT_SET_ID,
        "topology_id": topology.topology_id,
        "participant_id": participant_id,
        "rank": rank,
        "tensors": tensors,
        "fragments": fragments,
    }
    values.update(overrides)
    return WeightPlacementPart(**values)


def placement_manifest(**overrides) -> WeightPlacementManifest:
    values = {
        "resource_id": MODEL_ID,
        "revision": REVISION,
        "weight_generation": WEIGHT_GENERATION,
        "placement_set_id": PLACEMENT_SET_ID,
        "placement_id": None,
    }
    for field in tuple(values):
        if field in overrides:
            values[field] = overrides.pop(field)

    tensors = overrides.pop("tensors", (descriptor(),))
    fragments = overrides.pop("fragments", (placement_fragment(),))
    topology = overrides.pop("topology", None)
    parts = overrides.pop("parts", None)
    if overrides:
        values.update(overrides)

    fragments_are_valid = isinstance(fragments, tuple) and all(
        isinstance(item, PlacementFragment) for item in fragments
    )
    tensors_are_valid = isinstance(tensors, tuple) and all(
        isinstance(item, TensorDescriptor) for item in tensors
    )
    if topology is None:
        ranks = (
            tuple(sorted({item.rank for item in fragments}, key=_rank_key))
            if fragments_are_valid and fragments
            else (ParallelRank(),)
        )
        topology = parallel_topology(
            participants=tuple(
                TopologyParticipant(f"worker-{index}", rank)
                for index, rank in enumerate(ranks)
            )
        )

    if parts is None and fragments_are_valid and tensors_are_valid:
        tensor_by_id = {item.tensor_id: item for item in tensors}
        generated_parts = []
        for index, participant in enumerate(topology.participants):
            local_fragments = tuple(
                item for item in fragments if item.rank == participant.rank
            )
            local_tensor_ids = {item.tensor_id for item in local_fragments}
            local_tensors = tuple(
                tensor_by_id[tensor_id]
                for tensor_id in sorted(local_tensor_ids)
                if tensor_id in tensor_by_id
            )
            if not fragments and index == 0:
                local_tensors = tensors
            generated_parts.append(
                placement_part(
                    topology=topology,
                    participant_id=participant.participant_id,
                    rank=participant.rank,
                    tensors=local_tensors,
                    fragments=local_fragments,
                    resource_id=values["resource_id"],
                    revision=values["revision"],
                    weight_generation=values["weight_generation"],
                    placement_set_id=values["placement_set_id"],
                )
            )
        parts = tuple(generated_parts)
    elif parts is None:
        participant = topology.participants[0]
        parts = (
            placement_part(
                topology=topology,
                participant_id=participant.participant_id,
                rank=participant.rank,
                tensors=tensors,
                fragments=fragments,
                resource_id=values["resource_id"],
                revision=values["revision"],
                weight_generation=values["weight_generation"],
                placement_set_id=values["placement_set_id"],
            ),
        )

    return WeightPlacementManifest(topology=topology, parts=parts, **values)


def _rank_key(rank: ParallelRank) -> tuple[int, int, int, int]:
    return (rank.dp, rank.tp, rank.pp, rank.ep)


def binding_fragment(**overrides) -> RuntimeBindingFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "itemsize": 2,
        "local_shape": (4, 4),
        "strides_bytes": (8, 2),
        "storage_address": 0x1000,
        "storage_nbytes": 32,
        "storage_offset_bytes": 0,
    }
    if "address" in overrides and "storage_address" not in overrides:
        values["storage_address"] = overrides["address"]
    if "nbytes" in overrides and "storage_nbytes" not in overrides:
        values["storage_nbytes"] = overrides["nbytes"]
    values.update(overrides)
    return RuntimeBindingFragment(**values)


def binding_manifest(
    *,
    placement: WeightPlacementManifest | None = None,
    **overrides,
) -> WeightRuntimeBindingManifest:
    logical = placement or placement_manifest()
    participant_id = overrides.pop(
        "participant_id",
        logical.parts[0].participant_id,
    )
    placement_part = next(
        (item for item in logical.parts if item.participant_id == participant_id),
        None,
    )
    tensor_by_id = {tensor.tensor_id: tensor for tensor in logical.tensors}
    default_fragments = (
        tuple(
            binding_fragment(
                placement_fragment_id=fragment.placement_fragment_id,
                fragment_id=f"runtime-{index}",
                address=0x1000 + index * 0x100,
                nbytes=fragment.nbytes,
                itemsize=tensor_by_id[fragment.tensor_id].itemsize,
                local_shape=fragment.local_shape,
                strides_bytes=_contiguous_strides_bytes(
                    fragment.local_shape,
                    tensor_by_id[fragment.tensor_id].itemsize,
                ),
            )
            for index, fragment in enumerate(placement_part.fragments)
        )
        if placement_part is not None
        else ()
    )
    values = {
        "resource_id": logical.resource_id,
        "revision": logical.revision,
        "placement_id": logical.placement_id,
        "placement_digest": logical.digest,
        "participant_id": participant_id,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": default_fragments,
    }
    values.update(overrides)
    return WeightRuntimeBindingManifest(**values)


def _contiguous_strides_bytes(shape: tuple[int, ...], itemsize: int) -> tuple[int, ...]:
    result = []
    running = itemsize
    for extent in reversed(shape):
        result.append(running)
        running *= extent
    return tuple(reversed(result))


def runtime_inventory_tensor(**overrides) -> dict:
    values = {
        "fragment_id": "runtime-0",
        "placement_fragment_id": "placement-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (4, 4),
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "test:qwen:bf16:v1",
        "parallel_axes": ({"kind": "tp", "split_dim": 0},),
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "rank": {"dp": 0, "tp": 0, "pp": 0, "ep": 0},
        "lease_generation": 7,
        "aliases": (),
        "is_contiguous": True,
        "stride": (4, 1),
        "storage_offset": 0,
        "byte_offset": 0,
    }
    values.update(overrides)
    return values


def placement_part_inventory(
    *,
    topology: ParallelTopology | None = None,
    participant_id: str = PARTICIPANT_ID,
    tensors: tuple[dict, ...] | None = None,
    **overrides,
) -> dict:
    topology = topology or parallel_topology()
    participant = topology.participant(participant_id)
    values = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "weight_generation": WEIGHT_GENERATION,
        "placement_set_id": PLACEMENT_SET_ID,
        "topology_id": topology.topology_id,
        "participant_id": participant_id,
        "rank": asdict(participant.rank),
        "tensors": tensors if tensors is not None else (runtime_inventory_tensor(),),
    }
    values.update(overrides)
    return values
