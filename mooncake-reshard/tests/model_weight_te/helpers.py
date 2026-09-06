from __future__ import annotations

from dataclasses import replace
from math import prod
from typing import Sequence

from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    WeightRuntimeBindingManifest,
)
from mooncake.reshard.weight.te import MemoryRegistrationLease
from mooncake.reshard.weight._te.lifetime import (
    AcquiredWeightBinding,
    weight_allocation_fence,
)
from mooncake.reshard.transfer_engine.lifetime import TerminalTransferState

from global_placement_helpers import global_placement
from model_weight_planner.helpers import (
    RuntimeInputs,
    plan_transfer as plan_transfer,
    plan_transfer_to_local_target as plan_transfer_to_local_target,
)


class FakeTransferEngine:
    def __init__(self) -> None:
        self.calls = []
        self.fail_endpoint: str | None = None
        self.read_result: int | None = None
        self.register_calls: list[tuple[int, int]] = []
        self.unregister_calls: list[int] = []

    def register_memory(self, address: int, nbytes: int) -> int:
        self.register_calls.append((address, nbytes))
        return 0

    def unregister_memory(self, address: int) -> int:
        self.unregister_calls.append(address)
        return 0

    def batch_transfer_sync_write(
        self,
        endpoint: str,
        source_addresses: list[int],
        target_addresses: list[int],
        sizes: list[int],
    ) -> int:
        self.calls.append((endpoint, source_addresses, target_addresses, sizes))
        return -5 if endpoint == self.fail_endpoint else 0

    def batch_transfer_sync_read(
        self,
        endpoint: str,
        target_addresses: list[int],
        source_addresses: list[int],
        sizes: list[int],
    ) -> int:
        self.calls.append((endpoint, target_addresses, source_addresses, sizes))
        if self.read_result is not None:
            return self.read_result
        return -5 if endpoint == self.fail_endpoint else 0


class FakeAllocationLifetimeToken:
    def __init__(self, fence) -> None:
        self._fence = fence
        self.released_states: list[TerminalTransferState] = []

    @property
    def fence(self):
        return self._fence

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        self.released_states.append(terminal_state)


class FakeWeightAllocationGuard:
    def __init__(self, binding: WeightRuntimeBindingManifest) -> None:
        self.binding = binding
        self.acquisitions: list[tuple[str, tuple[str, ...]]] = []
        self.tokens: list[FakeAllocationLifetimeToken] = []

    def acquire(
        self,
        *,
        transfer_id: str,
        expected_binding: WeightRuntimeBindingManifest,
        required_fragment_ids: tuple[str, ...],
    ) -> AcquiredWeightBinding:
        assert expected_binding == self.binding
        self.acquisitions.append((transfer_id, tuple(required_fragment_ids)))
        token = FakeAllocationLifetimeToken(
            weight_allocation_fence(
                self.binding,
                required_fragment_ids,
                token_id=(
                    f"{self.binding.instance_id}-{self.binding.participant_id}-"
                    f"{len(self.tokens)}"
                ),
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.binding, token=token)


class FakeBatchTransferTicket:
    def __init__(
        self,
        statuses: list[str],
        *,
        on_drain=None,
    ) -> None:
        self._statuses = list(statuses)
        self._on_drain = on_drain
        self.drain_calls: list[int] = []

    @property
    def status(self):
        return FakeCompletionStatus(self._statuses[0])

    @property
    def drained(self) -> bool:
        return self._statuses[0] != "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms: int):
        self.drain_calls.append(timeout_ms)
        if self._on_drain is not None:
            self._on_drain()
        if len(self._statuses) > 1:
            self._statuses.pop(0)
        return FakeCompletionStatus(self._statuses[0])


class FakeCompletionStatus:
    def __init__(self, name: str) -> None:
        self.name = name


def contiguous_strides_bytes(
    shape: tuple[int, ...],
    itemsize: int,
) -> tuple[int, ...]:
    stride = itemsize
    result = []
    for extent in reversed(shape):
        result.append(stride)
        stride *= extent
    return tuple(reversed(result))


def runtime_fragment(
    *,
    placement_fragment_id: str,
    fragment_id: str,
    address: int,
    nbytes: int,
    worker_id: str,
    endpoint: str,
    local_shape: tuple[int, ...],
    itemsize: int,
    storage_address: int | None = None,
    storage_nbytes: int | None = None,
) -> RuntimeBindingFragment:
    storage_address = address if storage_address is None else storage_address
    storage_nbytes = nbytes if storage_nbytes is None else storage_nbytes
    return RuntimeBindingFragment(
        placement_fragment_id=placement_fragment_id,
        fragment_id=fragment_id,
        address=address,
        nbytes=nbytes,
        worker_id=worker_id,
        endpoint=endpoint,
        device="cuda:0",
        itemsize=itemsize,
        local_shape=local_shape,
        strides_bytes=contiguous_strides_bytes(local_shape, itemsize),
        storage_address=storage_address,
        storage_nbytes=storage_nbytes,
        storage_offset_bytes=address - storage_address,
    )


def manifests(
    tp: int,
    prefix: str,
    address_base: int,
    *,
    tensor: TensorDescriptor | None = None,
) -> RuntimeInputs:
    tensor = tensor or TensorDescriptor(
        tensor_id="layers.0.mlp.gate_up",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
        parallel_axes=(
            ReplicatedAxis("dp"),
            SplitAxis("tp", dim=0),
        ),
    )
    (dim,) = tensor.shard_dims
    extent = tensor.global_shape[dim] // tp
    fragments = []
    participant_inputs = []
    ranks = []
    nbytes_by_fragment = {}
    bindings = []
    for tp_rank in range(tp):
        worker_id = f"{prefix}-t{tp_rank}"
        rank = ParallelRank(tp=tp_rank)
        local_shape = list(tensor.global_shape)
        local_shape[dim] = extent
        global_offset = [0] * len(tensor.global_shape)
        global_offset[dim] = tp_rank * extent
        placement_fragment_id = f"{worker_id}-placement"
        nbytes = prod(local_shape) * tensor.itemsize
        fragments.append(
            PlacementFragment(
                placement_fragment_id=placement_fragment_id,
                tensor_id=tensor.tensor_id,
                global_offset=tuple(global_offset),
                local_shape=tuple(local_shape),
                nbytes=nbytes,
                rank=rank,
            )
        )
        ranks.append(rank)
        participant_inputs.append((tp_rank, worker_id, rank, placement_fragment_id))
        nbytes_by_fragment[placement_fragment_id] = nbytes

    placement = global_placement(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id=prefix,
        tensors=(tensor,),
        fragments=fragments,
        ranks=ranks,
    )
    participant_by_rank = {
        participant.rank: participant.participant_id
        for participant in placement.topology.participants
    }
    for tp_rank, worker_id, rank, placement_fragment_id in participant_inputs:
        nbytes = nbytes_by_fragment[placement_fragment_id]
        bindings.append(
            WeightRuntimeBindingManifest(
                resource_id=placement.resource_id,
                revision=placement.revision,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
                participant_id=participant_by_rank[rank],
                instance_id=worker_id,
                generation=1,
                lease_id=f"{worker_id}-runtime-lease",
                fragments=(
                    runtime_fragment(
                        placement_fragment_id=placement_fragment_id,
                        fragment_id=f"{worker_id}-fragment",
                        address=address_base + tp_rank * 0x1000,
                        nbytes=nbytes,
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        local_shape=tuple(local_shape),
                        itemsize=tensor.itemsize,
                    ),
                ),
            )
        )
    return RuntimeInputs(placement, tuple(bindings))


def registration_leases(inputs: RuntimeInputs) -> tuple[MemoryRegistrationLease, ...]:
    return tuple(
        MemoryRegistrationLease.from_fragment(
            fragment,
            lease_generation=binding.generation,
            runtime_lease_id=binding.lease_id,
        )
        for binding in inputs.bindings
        for fragment in binding.fragments
    )


def allocation_guards(
    inputs: RuntimeInputs,
) -> dict[tuple[str, str], FakeWeightAllocationGuard]:
    return allocation_guards_for_bindings(inputs.bindings)


def allocation_guards_for_bindings(
    bindings: Sequence[WeightRuntimeBindingManifest],
) -> dict[tuple[str, str], FakeWeightAllocationGuard]:
    return {
        (binding.instance_id, binding.participant_id): FakeWeightAllocationGuard(
            binding
        )
        for binding in bindings
    }


def participant_inputs(inputs: RuntimeInputs, index: int) -> RuntimeInputs:
    return RuntimeInputs(inputs.placement, (inputs.bindings[index],))


def with_revision(inputs: RuntimeInputs, revision: str) -> RuntimeInputs:
    placement = global_placement(
        resource_id=inputs.placement.resource_id,
        revision=revision,
        weight_generation=inputs.placement.weight_generation,
        placement_set_id=inputs.placement.placement_set_id,
        tensors=inputs.placement.tensors,
        fragments=inputs.placement.fragments,
        ranks=tuple(
            participant.rank for participant in inputs.placement.topology.participants
        ),
    )
    return RuntimeInputs(
        placement,
        tuple(
            replace(
                binding,
                revision=revision,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
            )
            for binding in inputs.bindings
        ),
    )


def execute_sink(
    sink,
    plan,
    source: RuntimeInputs,
    targets: RuntimeInputs,
    *,
    source_binding_index: int = 0,
    **kwargs,
):
    kwargs.setdefault("source_allocation_guards", allocation_guards(source))
    kwargs.setdefault("target_allocation_guards", allocation_guards(targets))
    return sink.execute(
        plan,
        source.placement,
        source.bindings[source_binding_index],
        targets.placement,
        targets.bindings,
        **kwargs,
    )


def execute_reader(
    reader,
    plan,
    sources: RuntimeInputs,
    target: RuntimeInputs,
    *,
    target_binding_index: int = 0,
    **kwargs,
):
    kwargs.setdefault("source_allocation_guards", allocation_guards(sources))
    kwargs.setdefault("target_allocation_guards", allocation_guards(target))
    return reader.execute(
        plan,
        sources.placement,
        sources.bindings,
        target.placement,
        target.bindings[target_binding_index],
        **kwargs,
    )


def nd_te_manifests(
    prefix: str,
    address_base: int,
    *,
    source: bool,
) -> RuntimeInputs:
    tensor = TensorDescriptor(
        tensor_id="layers.0.experts.w1",
        global_shape=(4, 6, 8),
        dtype="uint8",
        itemsize=1,
        layer_id=0,
        expert_id=None,
        layout_fingerprint="framework:logical-contiguous:v2",
        shard_dims=(0,) if source else (2,),
        parallel_axes=(SplitAxis("ep" if source else "tp", dim=0 if source else 2),),
    )
    fragments = []
    participant_inputs = []
    ranks = []
    bindings = []
    for rank in range(2):
        worker_id = f"{prefix}-{rank}"
        offset = (rank * 2, 0, 0) if source else (0, 0, rank * 4)
        shape = (2, 6, 8) if source else (4, 6, 4)
        parallel_rank = ParallelRank(ep=rank) if source else ParallelRank(tp=rank)
        placement_fragment_id = f"{worker_id}-placement"
        fragments.append(
            PlacementFragment(
                placement_fragment_id=placement_fragment_id,
                tensor_id=tensor.tensor_id,
                global_offset=offset,
                local_shape=shape,
                nbytes=prod(shape),
                rank=parallel_rank,
            )
        )
        ranks.append(parallel_rank)
        participant_inputs.append(
            (rank, worker_id, parallel_rank, placement_fragment_id, prod(shape))
        )

    placement = global_placement(
        resource_id="qwen-family-moe",
        revision="step-42",
        placement_set_id=prefix,
        tensors=(tensor,),
        fragments=fragments,
        ranks=ranks,
    )
    participant_by_rank = {
        participant.rank: participant.participant_id
        for participant in placement.topology.participants
    }
    for (
        rank,
        worker_id,
        parallel_rank,
        placement_fragment_id,
        nbytes,
    ) in participant_inputs:
        bindings.append(
            WeightRuntimeBindingManifest(
                resource_id=placement.resource_id,
                revision=placement.revision,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
                participant_id=participant_by_rank[parallel_rank],
                instance_id=worker_id,
                generation=1,
                lease_id=f"{worker_id}-runtime-lease",
                fragments=(
                    runtime_fragment(
                        placement_fragment_id=placement_fragment_id,
                        fragment_id=f"{worker_id}-fragment",
                        address=address_base + rank * 0x1000,
                        nbytes=nbytes,
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        local_shape=shape,
                        itemsize=tensor.itemsize,
                    ),
                ),
            )
        )
    return RuntimeInputs(placement, tuple(bindings))
