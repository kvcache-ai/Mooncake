from __future__ import annotations

from dataclasses import replace
from inspect import signature

import pytest

from mooncake.reshard.weight import te
from mooncake.reshard.weight._te.completion import (
    TransferCompletionUnknownError,
    TransferEngineError,
)
from mooncake.reshard.weight._te.reader import (
    DirectReadReceipt,
    MooncakeTransferEngineReader,
)
from mooncake.reshard.weight._te.execution import validate_manifest_pair
from mooncake.reshard.weight._te.registration import (
    MemoryRegistrationLease,
    same_runtime_snapshot,
    validate_registration,
)
from mooncake.reshard.weight._te.sink import (
    DirectTransferReceipt,
    MooncakeTransferEngineSink,
)
from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    SplitAxis,
    WeightRuntimeBindingManifest,
)

from global_placement_helpers import global_placement


def manifest_pair():
    tensor = TensorDescriptor(
        tensor_id="layers.0.weight",
        global_shape=(4,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layout_fingerprint="test:contiguous:v1",
        parallel_axes=(SplitAxis("tp", dim=0),),
    )
    placement = global_placement(
        resource_id="qwen",
        revision="step-1",
        placement_set_id="te-module-layout",
        tensors=(tensor,),
        fragments=(
            PlacementFragment(
                placement_fragment_id="placement-0",
                tensor_id=tensor.tensor_id,
                global_offset=(0,),
                local_shape=(4,),
                nbytes=4,
                rank=ParallelRank(),
            ),
        ),
    )
    participant = placement.topology.participants[0]
    binding = WeightRuntimeBindingManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        participant_id=participant.participant_id,
        instance_id="worker-0",
        generation=7,
        lease_id="runtime-lease-7",
        fragments=(
            RuntimeBindingFragment(
                placement_fragment_id="placement-0",
                fragment_id="runtime-placement-0",
                address=0x1100,
                nbytes=4,
                worker_id=participant.participant_id,
                endpoint=f"{participant.participant_id}:12345",
                device="cuda:0",
                itemsize=1,
                local_shape=(4,),
                strides_bytes=(1,),
                storage_address=0x1000,
                storage_nbytes=0x1000,
                storage_offset_bytes=0x100,
            ),
        ),
    )
    return placement, binding


def test_te_responsibility_modules_preserve_public_identity() -> None:
    assert te.TransferEngineError is TransferEngineError
    assert te.TransferCompletionUnknownError is TransferCompletionUnknownError
    assert te.MemoryRegistrationLease is MemoryRegistrationLease
    assert te.DirectTransferReceipt is DirectTransferReceipt
    assert te.DirectReadReceipt is DirectReadReceipt
    assert te.MooncakeTransferEngineSink is MooncakeTransferEngineSink
    assert te.MooncakeTransferEngineReader is MooncakeTransferEngineReader


def test_te_execution_contract_uses_explicit_placement_and_binding() -> None:
    sink_parameters = tuple(signature(MooncakeTransferEngineSink.execute).parameters)
    assert sink_parameters[:6] == (
        "self",
        "plan",
        "source_placement",
        "source_binding",
        "target_placement",
        "target_bindings",
    )

    reader_parameters = tuple(
        signature(MooncakeTransferEngineReader.execute).parameters
    )
    assert reader_parameters[:6] == (
        "self",
        "plan",
        "source_placement",
        "source_bindings",
        "target_placement",
        "target_binding",
    )


def test_te_registration_keeps_generation_and_lease_fences() -> None:
    _, binding = manifest_pair()
    fragment = binding.fragments[0]
    registration = MemoryRegistrationLease.from_fragment(
        fragment,
        lease_generation=binding.generation,
        runtime_lease_id=binding.lease_id,
    )
    assert registration.device == fragment.device
    assert registration.address == fragment.address
    assert registration.nbytes == fragment.nbytes
    assert registration.itemsize == fragment.itemsize
    assert registration.local_shape == fragment.local_shape
    assert registration.strides_bytes == fragment.strides_bytes
    assert registration.storage_address == fragment.storage_address
    assert registration.storage_nbytes == fragment.storage_nbytes
    assert registration.storage_offset_bytes == fragment.storage_offset_bytes

    validate_registration(
        fragment,
        {fragment.fragment_id: registration},
        "source",
        lease_generation=binding.generation,
        runtime_lease_id=binding.lease_id,
    )
    with pytest.raises(TransferEngineError, match="registration lease mismatch"):
        validate_registration(
            fragment,
            {fragment.fragment_id: replace(registration, lease_generation=8)},
            "source",
            lease_generation=binding.generation,
            runtime_lease_id=binding.lease_id,
        )
    with pytest.raises(TransferEngineError, match="registration lease mismatch"):
        validate_registration(
            fragment,
            {fragment.fragment_id: replace(registration, device="cuda:1")},
            "source",
            lease_generation=binding.generation,
            runtime_lease_id=binding.lease_id,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"itemsize": 2},
        {"local_shape": (2,)},
        {"strides_bytes": (2,)},
        {"storage_address": 0xF00, "storage_offset_bytes": 0x200},
        {"storage_nbytes": 0x2000},
        {"storage_address": 0xE00, "storage_offset_bytes": 0x300},
    ],
)
def test_te_registration_rejects_runtime_storage_evidence_mismatch(
    changes: dict[str, object],
) -> None:
    _, binding = manifest_pair()
    fragment = binding.fragments[0]
    registration = MemoryRegistrationLease.from_fragment(
        fragment,
        lease_generation=binding.generation,
        runtime_lease_id=binding.lease_id,
    )

    with pytest.raises(TransferEngineError, match="registration lease mismatch"):
        validate_registration(
            fragment,
            {fragment.fragment_id: replace(registration, **changes)},
            "source",
            lease_generation=binding.generation,
            runtime_lease_id=binding.lease_id,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"itemsize": 2},
        {"local_shape": (2,)},
        {"strides_bytes": (2,)},
        {"storage_address": 0xF00, "storage_offset_bytes": 0x200},
        {"storage_nbytes": 0x2000},
    ],
)
def test_te_snapshot_identity_includes_runtime_storage_evidence(
    changes: dict[str, object],
) -> None:
    _, binding = manifest_pair()
    fragment = binding.fragments[0]

    assert not same_runtime_snapshot(fragment, replace(fragment, **changes))


def test_te_snapshot_identity_excludes_runtime_owner() -> None:
    _, binding = manifest_pair()
    fragment = binding.fragments[0]

    assert same_runtime_snapshot(fragment, replace(fragment, owner=object()))


def test_te_rejects_binding_for_different_placement_digest() -> None:
    placement, binding = manifest_pair()

    with pytest.raises(TransferEngineError, match="placement digest"):
        validate_manifest_pair(
            type("Plan", (), {"resource_id": "qwen", "revision": "step-1"})(),
            placement,
            replace(binding, placement_digest="0" * 64),
            "source",
        )
