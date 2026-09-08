from __future__ import annotations

import ctypes
from dataclasses import dataclass, replace
from itertools import product
from math import prod

from mooncake.reshard._compat import _strict_zip
from mooncake.reshard.weight.manifest import (
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
)
from mooncake.reshard.weight.planner import (
    BoundWeightFragment,
    bind_logical_transfer_plan,
    plan_placement_transfer,
)
from mooncake.reshard.weight.lifetime import (
    AcquiredWeightBinding,
    weight_allocation_fence,
)
from mooncake.reshard.weight.store import WeightStore
from mooncake.reshard.lifetime import TerminalTransferState


@dataclass(frozen=True)
class RuntimeParticipant:
    participant_id: str
    rank: ParallelRank
    placement_fragments: tuple[PlacementFragment, ...]
    binding_fragments: tuple[RuntimeBindingFragment, ...]
    instance_id: str
    generation: int = 1
    lease_id: str | None = None


@dataclass(frozen=True)
class RuntimeInputs:
    placement: WeightPlacementManifest
    bindings: tuple[WeightRuntimeBindingManifest, ...]

    @property
    def binding(self) -> WeightRuntimeBindingManifest:
        if len(self.bindings) != 1:
            raise ValueError("runtime input does not contain exactly one binding")
        return self.bindings[0]

    def __len__(self) -> int:
        return len(self.bindings)

    def __getitem__(self, index: int) -> RuntimeInputs:
        return RuntimeInputs(self.placement, (self.bindings[index],))


class _TestAllocationToken:
    def __init__(self, fence) -> None:
        self._fence = fence
        self.released_states = []

    @property
    def fence(self):
        return self._fence

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        self.released_states.append(terminal_state)


class _TestAllocationGuard:
    def __init__(self, binding: WeightRuntimeBindingManifest) -> None:
        self.binding = binding
        self.tokens: list[_TestAllocationToken] = []

    def acquire(
        self,
        *,
        transfer_id: str,
        expected_binding: WeightRuntimeBindingManifest,
        required_fragment_ids: tuple[str, ...],
    ) -> AcquiredWeightBinding:
        assert transfer_id
        assert expected_binding == self.binding
        token = _TestAllocationToken(
            weight_allocation_fence(
                self.binding,
                required_fragment_ids,
                token_id=(
                    f"{self.binding.instance_id}-{self.binding.participant_id}-"
                    f"{','.join(sorted(required_fragment_ids))}"
                ),
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.binding, token=token)


def allocation_guards_for_bindings(
    bindings: tuple[WeightRuntimeBindingManifest, ...],
) -> dict[tuple[str, str], _TestAllocationGuard]:
    return {
        (binding.instance_id, binding.participant_id): _TestAllocationGuard(binding)
        for binding in bindings
    }


class GuardedWeightStore(WeightStore):
    """Test-only caller adapter that explicitly supplies framework pins."""

    def upload(self, plan, source_placement, source_binding, **kwargs):
        kwargs.setdefault(
            "source_allocation_guards",
            allocation_guards_for_bindings((source_binding,)),
        )
        return super().upload(plan, source_placement, source_binding, **kwargs)

    def load(self, plan, target_placement, target_binding, **kwargs):
        kwargs.setdefault(
            "target_allocation_guards",
            allocation_guards_for_bindings((target_binding,)),
        )
        return super().load(plan, target_placement, target_binding, **kwargs)


def with_empty_participant(
    inputs: RuntimeInputs,
    *,
    participant_id: str,
    rank: ParallelRank,
) -> RuntimeInputs:
    """Add a topology participant that owns no tensors and needs no binding."""

    topology = ParallelTopology(
        tp_size=max(inputs.placement.topology.tp_size, rank.tp + 1),
        pp_size=max(inputs.placement.topology.pp_size, rank.pp + 1),
        ep_size=max(inputs.placement.topology.ep_size, rank.ep + 1),
        dp_size=max(inputs.placement.topology.dp_size, rank.dp + 1),
        participants=(
            *inputs.placement.topology.participants,
            TopologyParticipant(participant_id=participant_id, rank=rank),
        ),
    )
    placement = WeightPlacementManifest(
        resource_id=inputs.placement.resource_id,
        revision=inputs.placement.revision,
        weight_generation=inputs.placement.weight_generation,
        placement_set_id=inputs.placement.placement_set_id,
        topology=topology,
        parts=(
            *(
                WeightPlacementPart(
                    resource_id=part.resource_id,
                    revision=part.revision,
                    weight_generation=part.weight_generation,
                    placement_set_id=part.placement_set_id,
                    topology_id=topology.topology_id,
                    participant_id=part.participant_id,
                    rank=part.rank,
                    tensors=part.tensors,
                    fragments=part.fragments,
                )
                for part in inputs.placement.parts
            ),
            WeightPlacementPart(
                resource_id=inputs.placement.resource_id,
                revision=inputs.placement.revision,
                weight_generation=inputs.placement.weight_generation,
                placement_set_id=inputs.placement.placement_set_id,
                topology_id=topology.topology_id,
                participant_id=participant_id,
                rank=rank,
                tensors=(),
                fragments=(),
            ),
        ),
    )
    bindings = tuple(
        replace(
            binding,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
        )
        for binding in inputs.bindings
    )
    return RuntimeInputs(placement, bindings)


def make_runtime_inputs(
    *,
    resource_id: str,
    revision: str,
    tensors: tuple[TensorDescriptor, ...],
    participants: tuple[RuntimeParticipant, ...],
    weight_generation: int = 1,
    placement_set_id: str,
) -> RuntimeInputs:
    topology = ParallelTopology(
        tp_size=max(participant.rank.tp for participant in participants) + 1,
        pp_size=max(participant.rank.pp for participant in participants) + 1,
        ep_size=max(participant.rank.ep for participant in participants) + 1,
        dp_size=max(participant.rank.dp for participant in participants) + 1,
        participants=tuple(
            TopologyParticipant(
                participant_id=participant.participant_id,
                rank=participant.rank,
            )
            for participant in participants
        ),
    )
    placement = WeightPlacementManifest.from_fragments(
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        topology=topology,
        tensors=tensors,
        fragments=tuple(
            fragment
            for participant in participants
            for fragment in participant.placement_fragments
        ),
    )
    bindings = tuple(
        WeightRuntimeBindingManifest(
            resource_id=placement.resource_id,
            revision=placement.revision,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id=participant.instance_id,
            participant_id=participant.participant_id,
            generation=participant.generation,
            lease_id=participant.lease_id or f"{participant.instance_id}-lease",
            fragments=participant.binding_fragments,
        )
        for participant in participants
    )
    return RuntimeInputs(placement, bindings)


def bound_fragments(inputs: RuntimeInputs) -> tuple[BoundWeightFragment, ...]:
    result = []
    for binding in inputs.bindings:
        part = next(
            item
            for item in inputs.placement.parts
            if item.participant_id == binding.participant_id
        )
        runtime_by_placement_id = {
            fragment.placement_fragment_id: fragment for fragment in binding.fragments
        }
        result.extend(
            BoundWeightFragment(
                placement=fragment,
                binding=runtime_by_placement_id[fragment.placement_fragment_id],
                instance_id=binding.instance_id,
                runtime_lease_id=binding.lease_id,
                lease_generation=binding.generation,
                owner=runtime_by_placement_id[fragment.placement_fragment_id].owner,
            )
            for fragment in part.fragments
        )
    return tuple(result)


def plan_transfer(sources: RuntimeInputs, targets: RuntimeInputs):
    logical = plan_placement_transfer(sources.placement, targets.placement)
    source_participants = {
        executor.participant_id for executor in logical.source_executors
    }
    target_participants = {
        executor.participant_id for executor in logical.target_executors
    }
    return bind_logical_transfer_plan(
        logical,
        source_bindings=tuple(
            binding
            for binding in sources.bindings
            if binding.participant_id in source_participants
        ),
        target_bindings=tuple(
            binding
            for binding in targets.bindings
            if binding.participant_id in target_participants
        ),
    )


@dataclass
class FakeReplicateConfig:
    group_ids: list[str]
    data_type: str
    with_hard_pin: bool


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.group_ids: dict[str, str] = {}
        self.configs: dict[str, tuple[str, bool]] = {}
        self.calls: list[str] = []
        self.put_batches: list[tuple[str, ...]] = []
        self.registered: set[int] = set()
        self.register_calls = 0
        self.register_args: list[tuple[int, int]] = []
        self.unregister_calls = 0
        self.unregister_addresses: list[int] = []
        self.fail_key: str | None = None
        self.processing_keys: set[str] = set()
        self.range_get_calls = 0
        self.range_sizes: list[int] = []
        self.range_batch_sizes: list[int] = []
        self.exist_batch_sizes: list[int] = []
        self.register_result = 0
        self.removed_keys: list[str] = []
        self.remove_forces: list[bool] = []
        self.unregister_results: dict[int, int] = {}
        self.unregister_exceptions: dict[int, Exception] = {}
        self.fail_after_write_key: str | None = None
        self.manifest_race_value: bytes | None = None
        self.manifest_race_key: str | None = None
        self.after_batch_is_exist = None

    def register_buffer(self, address: int, nbytes: int) -> int:
        self.calls.append("register_buffer")
        self.register_calls += 1
        self.register_args.append((address, nbytes))
        if self.register_result != 0:
            return self.register_result
        self.registered.add(address)
        return 0

    def unregister_buffer(self, address: int) -> int:
        self.calls.append("unregister_buffer")
        self.unregister_calls += 1
        self.unregister_addresses.append(address)
        if address in self.unregister_exceptions:
            raise self.unregister_exceptions[address]
        result = self.unregister_results.get(address, 0)
        if result == 0:
            self.registered.remove(address)
        return result

    def batch_put_from(
        self,
        keys: list[str],
        addresses: list[int],
        sizes: list[int],
        config: FakeReplicateConfig,
    ) -> list[int]:
        self.calls.append("batch_put_from")
        self.put_batches.append(tuple(keys))
        results = []
        for key, address, size, group_id in zip(
            keys, addresses, sizes, config.group_ids
        ):
            if key == self.fail_key:
                results.append(-1)
                continue
            if key in self.processing_keys:
                results.append(0)
                continue
            self.objects[key] = ctypes.string_at(address, size)
            self.group_ids[key] = group_id
            self.configs[key] = (config.data_type, config.with_hard_pin)
            results.append(0)
        return results

    def put(self, key: str, value, config: FakeReplicateConfig) -> int:
        self.calls.append("put")
        if self.manifest_race_value is not None and key == self.manifest_race_key:
            self.objects[key] = self.manifest_race_value
            self.group_ids[key] = config.group_ids[0]
            self.configs[key] = (config.data_type, config.with_hard_pin)
            self.manifest_race_value = None
            return 0
        if key == self.fail_key:
            return -1
        if key in self.processing_keys:
            return 0
        if key in self.objects:
            return 0
        self.objects[key] = bytes(value)
        self.group_ids[key] = config.group_ids[0]
        self.configs[key] = (config.data_type, config.with_hard_pin)
        if key == self.fail_after_write_key:
            return -1
        return 0

    def get(self, key: str) -> bytes:
        self.calls.append("get")
        return self.objects[key]

    def is_exist(self, key: str) -> int:
        self.calls.append("is_exist")
        return int(key in self.objects)

    def batch_is_exist(self, keys: list[str]) -> list[int]:
        self.calls.append("batch_is_exist")
        self.exist_batch_sizes.append(len(keys))
        results = [self.is_exist(key) for key in keys]
        callback = self.after_batch_is_exist
        self.after_batch_is_exist = None
        if callback is not None:
            callback()
        return results

    def remove(self, key: str, force: bool = False) -> int:
        self.calls.append("remove")
        self.removed_keys.append(key)
        self.remove_forces.append(force)
        self.objects.pop(key, None)
        self.processing_keys.discard(key)
        self.group_ids.pop(key, None)
        return 0

    def get_into_ranges(
        self,
        addresses: list[int],
        all_keys: list[list[str]],
        all_dst_offsets: list[list[list[int]]],
        all_src_offsets: list[list[list[int]]],
        all_sizes: list[list[list[int]]],
    ) -> list[list[list[int]]]:
        self.calls.append("get_into_ranges")
        self.range_get_calls += 1
        self.range_batch_sizes.append(
            sum(len(sizes) for buffer in all_sizes for sizes in buffer)
        )
        results = []
        for address, keys, dst_offsets, src_offsets, sizes in zip(
            addresses,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        ):
            buffer_results = []
            for key, dst_group, src_group, size_group in zip(
                keys, dst_offsets, src_offsets, sizes
            ):
                object_data = self.objects[key]
                range_results = []
                for dst, src, size in zip(dst_group, src_group, size_group):
                    self.range_sizes.append(size)
                    ctypes.memmove(address + dst, object_data[src : src + size], size)
                    range_results.append(size)
                buffer_results.append(range_results)
            results.append(buffer_results)
        return results


def tensor_descriptor() -> TensorDescriptor:
    return TensorDescriptor(
        tensor_id="layers.2.experts.3.w1",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layer_id=2,
        expert_id=3,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
        parallel_axes=(
            ReplicatedAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            OwnershipAxis(kind="ep"),
            SplitAxis(kind="tp", dim=0),
        ),
    )


def source_manifests(
    dp: int = 2,
    tp: int = 2,
    *,
    weight_generation: int = 1,
) -> RuntimeInputs:
    tensor = tensor_descriptor()
    extent = tensor.global_shape[0] // tp
    participants = []
    for dp_rank in range(dp):
        for tp_rank in range(tp):
            start = tp_rank * extent
            owner = (ctypes.c_ubyte * extent)(*range(start, start + extent))
            worker_id = f"source-d{dp_rank}-t{tp_rank}"
            placement_fragment_id = f"{worker_id}-placement"
            rank = ParallelRank(dp=dp_rank, tp=tp_rank)
            participants.append(
                RuntimeParticipant(
                    participant_id=worker_id,
                    rank=rank,
                    instance_id=worker_id,
                    placement_fragments=(
                        PlacementFragment(
                            placement_fragment_id=placement_fragment_id,
                            tensor_id=tensor.tensor_id,
                            global_offset=(start,),
                            local_shape=(extent,),
                            nbytes=extent,
                            rank=rank,
                        ),
                    ),
                    binding_fragments=(
                        RuntimeBindingFragment(
                            placement_fragment_id=placement_fragment_id,
                            fragment_id=f"{worker_id}-fragment",
                            address=ctypes.addressof(owner),
                            nbytes=extent,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=1,
                            local_shape=(extent,),
                            strides_bytes=(1,),
                            storage_address=ctypes.addressof(owner),
                            storage_nbytes=extent,
                            storage_offset_bytes=0,
                            owner=owner,
                        ),
                    ),
                )
            )
    return make_runtime_inputs(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        weight_generation=weight_generation,
        placement_set_id=f"source-dp{dp}-tp{tp}",
        tensors=(tensor,),
        participants=tuple(participants),
    )


def target_manifests(
    dp: int = 3,
    tp: int = 4,
    *,
    weight_generation: int = 1,
) -> RuntimeInputs:
    tensor = tensor_descriptor()
    extent = tensor.global_shape[0] // tp
    participants = []
    for dp_rank in range(dp):
        for tp_rank in range(tp):
            owner = (ctypes.c_ubyte * extent)(*[255] * extent)
            worker_id = f"target-d{dp_rank}-t{tp_rank}"
            placement_fragment_id = f"{worker_id}-placement"
            rank = ParallelRank(dp=dp_rank, tp=tp_rank)
            participants.append(
                RuntimeParticipant(
                    participant_id=worker_id,
                    rank=rank,
                    instance_id=worker_id,
                    placement_fragments=(
                        PlacementFragment(
                            placement_fragment_id=placement_fragment_id,
                            tensor_id=tensor.tensor_id,
                            global_offset=(tp_rank * extent,),
                            local_shape=(extent,),
                            nbytes=extent,
                            rank=rank,
                        ),
                    ),
                    binding_fragments=(
                        RuntimeBindingFragment(
                            placement_fragment_id=placement_fragment_id,
                            fragment_id=f"{worker_id}-fragment",
                            address=ctypes.addressof(owner),
                            nbytes=extent,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=1,
                            local_shape=(extent,),
                            strides_bytes=(1,),
                            storage_address=ctypes.addressof(owner),
                            storage_nbytes=extent,
                            storage_offset_bytes=0,
                            owner=owner,
                        ),
                    ),
                )
            )
    return make_runtime_inputs(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        weight_generation=weight_generation,
        placement_set_id=f"target-dp{dp}-tp{tp}",
        tensors=(tensor,),
        participants=tuple(participants),
    )


def multi_dim_store_manifests(
    prefix: str,
    *,
    source: bool,
    target_dim: int = 2,
) -> RuntimeInputs:
    tensor = TensorDescriptor(
        tensor_id="layers.0.experts.w1",
        global_shape=(4, 6, 8),
        dtype="uint8",
        itemsize=1,
        layer_id=0,
        expert_id=None,
        layout_fingerprint="framework:logical-contiguous:v2",
        shard_dims=(0,) if source else (target_dim,),
        parallel_axes=(
            SplitAxis(
                kind="ep" if source else "tp",
                dim=0 if source else target_dim,
            ),
        ),
    )
    participants = []
    rank_count = 4 if source else 2
    for rank in range(rank_count):
        if source:
            offset = (rank, 0, 0)
            shape = (1, 6, 8)
            parallel_rank = ParallelRank(ep=rank)
        else:
            shape_list = list(tensor.global_shape)
            shape_list[target_dim] //= rank_count
            offset_list = [0, 0, 0]
            offset_list[target_dim] = rank * shape_list[target_dim]
            offset = tuple(offset_list)
            shape = tuple(shape_list)
            parallel_rank = ParallelRank(tp=rank)
        values = []
        for coordinate in product(*(range(extent) for extent in shape)):
            global_coordinate = tuple(
                begin + local for begin, local in _strict_zip(offset, coordinate)
            )
            values.append(
                global_coordinate[0] * 48
                + global_coordinate[1] * 8
                + global_coordinate[2]
                if source
                else 255
            )
        owner = (ctypes.c_ubyte * prod(shape))(*values)
        worker_id = f"{prefix}-{rank}"
        placement_fragment_id = f"{worker_id}-placement"
        participants.append(
            RuntimeParticipant(
                participant_id=worker_id,
                rank=parallel_rank,
                instance_id=worker_id,
                placement_fragments=(
                    PlacementFragment(
                        placement_fragment_id=placement_fragment_id,
                        tensor_id=tensor.tensor_id,
                        global_offset=offset,
                        local_shape=shape,
                        nbytes=prod(shape),
                        rank=parallel_rank,
                    ),
                ),
                binding_fragments=(
                    RuntimeBindingFragment(
                        placement_fragment_id=placement_fragment_id,
                        fragment_id=f"{worker_id}-fragment",
                        address=ctypes.addressof(owner),
                        nbytes=prod(shape),
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        device="cuda:0",
                        itemsize=1,
                        local_shape=shape,
                        strides_bytes=(shape[1] * shape[2], shape[2], 1),
                        storage_address=ctypes.addressof(owner),
                        storage_nbytes=prod(shape),
                        storage_offset_bytes=0,
                        owner=owner,
                    ),
                ),
            )
        )
    return make_runtime_inputs(
        resource_id="qwen-family-moe",
        revision="step-42",
        placement_set_id=f"{prefix}-multi-dim-{target_dim}",
        tensors=(tensor,),
        participants=tuple(participants),
    )


def expected_multi_dim_fragment(fragment: PlacementFragment) -> bytes:
    values = []
    for coordinate in product(*(range(extent) for extent in fragment.local_shape)):
        global_coordinate = tuple(
            begin + local
            for begin, local in _strict_zip(fragment.global_offset, coordinate)
        )
        values.append(
            global_coordinate[0] * 48 + global_coordinate[1] * 8 + global_coordinate[2]
        )
    return bytes(values)


def make_weight_store(
    store: InMemoryStore | None = None,
    *,
    max_range_bytes: int = 64 * 1024 * 1024,
    max_ranges_per_request: int = 1024,
    max_region_segments: int = 1_000_000,
):
    current = store or InMemoryStore()
    return current, GuardedWeightStore(
        current,
        config_factory=lambda group_ids, record_type: FakeReplicateConfig(
            list(group_ids),
            data_type=("WEIGHT" if record_type == "payload" else "METADATA"),
            with_hard_pin=True,
        ),
        max_range_bytes=max_range_bytes,
        max_ranges_per_request=max_ranges_per_request,
        max_region_segments=max_region_segments,
    )


def rebuild_runtime_inputs(
    inputs: RuntimeInputs,
    *,
    tensors: tuple[TensorDescriptor, ...] | None = None,
    placement_fragments: dict[str, tuple[PlacementFragment, ...]] | None = None,
    binding_fragments: dict[str, tuple[RuntimeBindingFragment, ...]] | None = None,
) -> RuntimeInputs:
    placement_updates = placement_fragments or {}
    binding_updates = binding_fragments or {}
    participants = []
    for binding in inputs.bindings:
        part = next(
            item
            for item in inputs.placement.parts
            if item.participant_id == binding.participant_id
        )
        current_placements = placement_updates.get(
            binding.participant_id, part.fragments
        )
        participants.append(
            RuntimeParticipant(
                participant_id=binding.participant_id,
                rank=(current_placements[0].rank if current_placements else part.rank),
                placement_fragments=current_placements,
                binding_fragments=binding_updates.get(
                    binding.participant_id, binding.fragments
                ),
                instance_id=binding.instance_id,
                generation=binding.generation,
                lease_id=binding.lease_id,
            )
        )
    return make_runtime_inputs(
        resource_id=inputs.placement.resource_id,
        revision=inputs.placement.revision,
        weight_generation=inputs.placement.weight_generation,
        placement_set_id=inputs.placement.placement_set_id,
        tensors=tensors if tensors is not None else inputs.placement.tensors,
        participants=tuple(participants),
    )


def coalesce_runtime_inputs(
    inputs: RuntimeInputs,
    *,
    instance_id: str,
    worker_id: str | None = None,
) -> RuntimeInputs:
    if not inputs.bindings:
        raise ValueError("runtime inputs must not be empty")
    runtime_worker = worker_id or instance_id
    placement_fragments = tuple(
        replace(fragment, rank=ParallelRank())
        for fragment in inputs.placement.fragments
    )
    binding_fragments = tuple(
        replace(fragment, worker_id=runtime_worker)
        for binding in inputs.bindings
        for fragment in binding.fragments
    )
    generations = {binding.generation for binding in inputs.bindings}
    if len(generations) != 1:
        raise ValueError("runtime inputs have different generations")
    return make_runtime_inputs(
        resource_id=inputs.placement.resource_id,
        revision=inputs.placement.revision,
        weight_generation=inputs.placement.weight_generation,
        placement_set_id=f"{inputs.placement.placement_set_id}-coalesced",
        tensors=inputs.placement.tensors,
        participants=(
            RuntimeParticipant(
                participant_id=instance_id,
                rank=ParallelRank(),
                placement_fragments=placement_fragments,
                binding_fragments=binding_fragments,
                instance_id=instance_id,
                generation=generations.pop(),
                lease_id=f"{instance_id}-lease",
            ),
        ),
    )


def upload_all(weight_store: WeightStore, plan, manifests: RuntimeInputs):
    receipts = []
    try:
        for binding in manifests.bindings:
            receipts.extend(weight_store.upload(plan, manifests.placement, binding))
    except Exception:
        weight_store.abort_upload(plan, receipts)
        raise
    return receipts


def load_all(weight_store: WeightStore, plan, manifests: RuntimeInputs) -> None:
    for binding in manifests.bindings:
        weight_store.load(plan, manifests.placement, binding)
