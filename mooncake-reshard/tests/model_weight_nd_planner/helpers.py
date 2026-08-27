from __future__ import annotations

from itertools import product
from math import prod

from mooncake.reshard._compat import _strict_zip
from mooncake.reshard.weight.manifest import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
)

from model_weight_planner.helpers import (
    RuntimeInputs,
    _canonical_strides_bytes,
    plan_transfer as plan_transfer,
    runtime_inputs_from_groups,
)


MODEL_ID = "qwen-family-moe"
REVISION = "step-42"


def tensor_descriptor(
    tensor_id: str,
    *,
    global_shape: tuple[int, ...],
    shard_dims: tuple[int, ...],
    layer_id: int | None = 0,
    parallel_axes: tuple[SplitAxis | ReplicatedAxis | OwnershipAxis, ...] | None = None,
) -> TensorDescriptor:
    if parallel_axes is None:
        if not shard_dims:
            parallel_axes = (
                ReplicatedAxis(kind="dp"),
                OwnershipAxis(kind="pp"),
            )
        elif len(shard_dims) == 1:
            parallel_axes = (
                ReplicatedAxis(kind="dp"),
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="tp", dim=shard_dims[0]),
            )
        else:
            parallel_axes = (
                ReplicatedAxis(kind="dp"),
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="ep", dim=shard_dims[0]),
                SplitAxis(kind="tp", dim=shard_dims[1]),
            )
    return TensorDescriptor(
        tensor_id=tensor_id,
        global_shape=global_shape,
        dtype="bfloat16",
        itemsize=2,
        layer_id=layer_id,
        expert_id=None,
        layout_fingerprint="framework:logical-contiguous:v2",
        shard_dims=shard_dims,
        parallel_axes=parallel_axes,
    )


def build_manifests(
    side: str,
    placements: list[
        tuple[
            TensorDescriptor,
            ParallelRank,
            tuple[int, ...],
            tuple[int, ...],
        ]
    ],
    *,
    address_base: int,
) -> RuntimeInputs:
    grouped: dict[
        ParallelRank,
        list[tuple[TensorDescriptor, tuple[int, ...], tuple[int, ...]]],
    ] = {}
    for tensor, rank, offset, shape in placements:
        grouped.setdefault(rank, []).append((tensor, offset, shape))

    groups = []
    address = address_base
    for rank in sorted(grouped, key=lambda item: (item.dp, item.pp, item.ep, item.tp)):
        worker_id = f"{side}-d{rank.dp}-p{rank.pp}-e{rank.ep}-t{rank.tp}"
        placement_fragments = []
        binding_fragments = []
        tensors: dict[str, TensorDescriptor] = {}
        for tensor, offset, shape in sorted(
            grouped[rank], key=lambda item: item[0].tensor_id
        ):
            nbytes = prod(shape) * tensor.itemsize
            placement_fragment_id = f"{worker_id}-{tensor.tensor_id}-placement"
            placement_fragments.append(
                PlacementFragment(
                    placement_fragment_id=placement_fragment_id,
                    tensor_id=tensor.tensor_id,
                    global_offset=offset,
                    local_shape=shape,
                    nbytes=nbytes,
                    rank=rank,
                )
            )
            binding_fragments.append(
                RuntimeBindingFragment(
                    placement_fragment_id=placement_fragment_id,
                    fragment_id=f"{worker_id}-{tensor.tensor_id}",
                    address=address,
                    nbytes=nbytes,
                    worker_id=worker_id,
                    endpoint=f"{worker_id}:12345",
                    device="cuda:0",
                    itemsize=tensor.itemsize,
                    local_shape=shape,
                    strides_bytes=_canonical_strides_bytes(shape, tensor.itemsize),
                    storage_address=address,
                    storage_nbytes=nbytes,
                    storage_offset_bytes=0,
                )
            )
            address += nbytes + 4096
            tensors[tensor.tensor_id] = tensor
        groups.append((worker_id, tuple(placement_fragments), tuple(binding_fragments)))
    return runtime_inputs_from_groups(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id=side,
        tensors=tuple(
            sorted(
                {tensor.tensor_id: tensor for tensor, _, _, _ in placements}.values(),
                key=lambda item: item.tensor_id,
            )
        ),
        groups=groups,
    )


def ep_tp_placements(
    tensors: tuple[TensorDescriptor, ...],
    *,
    dp: int,
    pp_owner: dict[str, int],
    ep: int,
    tp: int,
    tp_dim: int,
) -> list[tuple[TensorDescriptor, ParallelRank, tuple[int, ...], tuple[int, ...]]]:
    placements = []
    for tensor in tensors:
        assert tensor.global_shape[0] % ep == 0
        assert tensor.global_shape[tp_dim] % tp == 0
        expert_extent = tensor.global_shape[0] // ep
        tp_extent = tensor.global_shape[tp_dim] // tp
        for dp_rank, ep_rank, tp_rank in product(range(dp), range(ep), range(tp)):
            shape = list(tensor.global_shape)
            offset = [0] * len(shape)
            shape[0] = expert_extent
            offset[0] = ep_rank * expert_extent
            shape[tp_dim] = tp_extent
            offset[tp_dim] = tp_rank * tp_extent
            placements.append(
                (
                    tensor,
                    ParallelRank(
                        dp=dp_rank,
                        pp=pp_owner[tensor.tensor_id],
                        ep=ep_rank,
                        tp=tp_rank,
                    ),
                    tuple(offset),
                    tuple(shape),
                )
            )
    return placements


def fragment_payload(descriptor: TensorDescriptor, fragment) -> bytearray:
    global_strides = []
    running = 1
    for extent in reversed(descriptor.global_shape):
        global_strides.append(running)
        running *= extent
    global_strides.reverse()
    payload = bytearray()
    for local_coordinate in product(
        *(range(extent) for extent in fragment.local_shape)
    ):
        global_coordinate = tuple(
            begin + local
            for begin, local in _strict_zip(fragment.global_offset, local_coordinate)
        )
        value = 1 + sum(
            coordinate * stride
            for coordinate, stride in _strict_zip(global_coordinate, global_strides)
        )
        mask = (1 << (descriptor.itemsize * 8)) - 1
        payload.extend((value & mask).to_bytes(descriptor.itemsize, "little"))
    return payload


def assert_plan_copies_logical_contents(
    plan,
    source_manifests: RuntimeInputs,
    target_manifests: RuntimeInputs,
) -> None:
    descriptors = {
        tensor.tensor_id: tensor for tensor in source_manifests.placement.tensors
    }
    source_payloads = {}
    source_parts = {
        part.participant_id: part for part in source_manifests.placement.parts
    }
    for binding in source_manifests.bindings:
        part = source_parts[binding.participant_id]
        placement_by_id = {
            fragment.placement_fragment_id: fragment for fragment in part.fragments
        }
        for runtime_fragment in binding.fragments:
            placement_fragment = placement_by_id[runtime_fragment.placement_fragment_id]
            source_payloads[placement_fragment.placement_fragment_id] = fragment_payload(
                descriptors[placement_fragment.tensor_id], placement_fragment
            )

    target_payloads = {}
    target_placements_by_fragment_id = {}
    target_parts = {
        part.participant_id: part for part in target_manifests.placement.parts
    }
    for binding in target_manifests.bindings:
        part = target_parts[binding.participant_id]
        placement_by_id = {
            fragment.placement_fragment_id: fragment for fragment in part.fragments
        }
        for runtime_fragment in binding.fragments:
            placement_fragment = placement_by_id[runtime_fragment.placement_fragment_id]
            target_payloads[placement_fragment.placement_fragment_id] = bytearray(
                runtime_fragment.nbytes
            )
            target_placements_by_fragment_id[placement_fragment.placement_fragment_id] = (
                placement_fragment
            )

    for operation in plan.operations:
        source = source_payloads[operation.source.placement_fragment_id]
        target = target_payloads[operation.target.placement_fragment_id]
        for source_offset, target_offset, nbytes in operation.iter_segments(
            max_segments=operation.segment_count
        ):
            target[target_offset : target_offset + nbytes] = source[
                source_offset : source_offset + nbytes
            ]

    for fragment_id, placement_fragment in target_placements_by_fragment_id.items():
        assert target_payloads[fragment_id] == fragment_payload(
            descriptors[placement_fragment.tensor_id], placement_fragment
        )


def pp_placements(
    tensors: tuple[TensorDescriptor, ...],
    owners: dict[str, int],
) -> list[tuple[TensorDescriptor, ParallelRank, tuple[int, ...], tuple[int, ...]]]:
    return [
        (
            tensor,
            ParallelRank(pp=owners[tensor.tensor_id]),
            (0,),
            tensor.global_shape,
        )
        for tensor in tensors
    ]
