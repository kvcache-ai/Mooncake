from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.transfer_engine import MooncakeTransferEngineExecutor
from mooncake.reshard.weight._te.execution import validate_manifest_pair
from mooncake.reshard.weight._te.registration import (
    MemoryRegistrationLease,
    registered_sources,
    same_runtime_snapshot,
    validate_registration,
)
from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    WeightRuntimeBindingManifest,
)
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    TransferEngineError,
)

from global_placement_helpers import global_placement

from .helpers import (
    FakeTransferEngine,
    RuntimeInputs,
    execute_reader,
    execute_sink,
    manifests,
    participant_inputs,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
    runtime_fragment,
)


@pytest.mark.parametrize(
    ("executor_kind", "side"),
    [
        ("sink", "source"),
        ("sink", "target"),
        ("reader", "source"),
        ("reader", "target"),
    ],
)
@pytest.mark.parametrize("mismatch", ["registration", "manifest"])
def test_te_pre_registered_paths_fence_runtime_lease_id_before_native_calls(
    executor_kind: str,
    side: str,
    mismatch: str,
) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=2, prefix="target", address_base=0x40000)
    plan = (
        plan_transfer(sources, targets)
        if executor_kind == "sink"
        else plan_transfer_to_local_target(sources, participant_inputs(targets, 0))
    )
    current_sources = sources
    current_targets = targets
    if mismatch == "manifest":
        if side == "source":
            current_sources = RuntimeInputs(
                sources.placement,
                (
                    replace(
                        sources.bindings[0],
                        lease_id="rotated-source-runtime-lease",
                    ),
                    *sources.bindings[1:],
                ),
            )
        else:
            current_targets = RuntimeInputs(
                targets.placement,
                (
                    replace(
                        targets.bindings[0],
                        lease_id="rotated-target-runtime-lease",
                    ),
                    *targets.bindings[1:],
                ),
            )

    source_registrations = list(registration_leases(current_sources))
    target_registrations = list(registration_leases(current_targets))
    if mismatch == "registration":
        registrations = (
            source_registrations if side == "source" else target_registrations
        )
        registrations[0] = replace(
            registrations[0],
            runtime_lease_id=f"stale-{side}-runtime-lease",
        )

    engine = FakeTransferEngine()
    expected_error = (
        f"{side} registration lease mismatch"
        if mismatch == "registration"
        else f"{side} executor snapshot mismatch"
    )
    with pytest.raises(TransferEngineError, match=expected_error):
        try:
            if executor_kind == "sink":
                execute_sink(
                    MooncakeTransferEngineSink(engine),
                    plan,
                    current_sources,
                    current_targets,
                    target_registrations=tuple(target_registrations),
                    source_pre_registered=True,
                    source_registrations=tuple(source_registrations),
                )
            else:
                execute_reader(
                    MooncakeTransferEngineReader(engine),
                    plan,
                    current_sources,
                    participant_inputs(current_targets, 0),
                    source_registrations=tuple(source_registrations),
                    target_pre_registered=True,
                    target_registrations=tuple(target_registrations),
                )
        finally:
            assert engine.calls == []
            assert engine.register_calls == []
            assert engine.unregister_calls == []


def test_te_registration_deduplicates_views_of_one_storage_allocation() -> None:
    engine = FakeTransferEngine()
    pending_owner = MooncakeTransferEngineExecutor(engine)
    fragments = tuple(
        runtime_fragment(
            placement_fragment_id=f"placement-{index}",
            fragment_id=f"fragment-{index}",
            address=0x1000 + offset,
            nbytes=0x100,
            worker_id="worker",
            endpoint="worker:12345",
            local_shape=(0x100,),
            itemsize=1,
            storage_address=0x1000,
            storage_nbytes=0x1000,
        )
        for index, offset in enumerate((0x100, 0x300))
    )

    with registered_sources(
        engine,
        pending_owner,
        fragments,
        pre_registered=False,
        registrations=None,
        lease_generation=1,
        runtime_lease_id="runtime-lease",
        resources=(),
    ):
        assert engine.register_calls == [(0x1000, 0x1000)]
        assert engine.unregister_calls == []

    assert engine.unregister_calls == [0x1000]


@pytest.mark.parametrize("executor_kind", ["sink", "reader"])
@pytest.mark.parametrize("side", ["source", "target"])
def test_te_execution_rejects_storage_snapshot_mismatch_before_native_calls(
    executor_kind: str,
    side: str,
) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=2, prefix="target", address_base=0x40000)
    plan = (
        plan_transfer(sources, targets)
        if executor_kind == "sink"
        else plan_transfer_to_local_target(sources, participant_inputs(targets, 0))
    )
    current = sources if side == "source" else targets
    fragment = current.bindings[0].fragments[0]
    changed_fragment = replace(
        fragment,
        storage_address=fragment.address - 1,
        storage_nbytes=fragment.nbytes + 1,
        storage_offset_bytes=1,
    )
    changed = RuntimeInputs(
        current.placement,
        (
            replace(current.bindings[0], fragments=(changed_fragment,)),
            *current.bindings[1:],
        ),
    )
    current_sources = changed if side == "source" else sources
    current_targets = changed if side == "target" else targets
    engine = FakeTransferEngine()

    with pytest.raises(TransferEngineError, match=f"{side} executor snapshot mismatch"):
        if executor_kind == "sink":
            execute_sink(
                MooncakeTransferEngineSink(engine),
                plan,
                current_sources,
                current_targets,
                target_registrations=registration_leases(current_targets),
            )
        else:
            execute_reader(
                MooncakeTransferEngineReader(engine),
                plan,
                current_sources,
                participant_inputs(current_targets, 0),
                source_registrations=registration_leases(current_sources),
                target_pre_registered=True,
                target_registrations=registration_leases(
                    participant_inputs(current_targets, 0)
                ),
            )

    assert engine.calls == []
    assert engine.register_calls == []
    assert engine.unregister_calls == []


def _registration_manifest_pair():
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
        placement_set_id="te-registration-contract",
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


def test_te_registration_keeps_generation_and_lease_fences() -> None:
    _, binding = _registration_manifest_pair()
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
    _, binding = _registration_manifest_pair()
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
    _, binding = _registration_manifest_pair()
    fragment = binding.fragments[0]

    assert not same_runtime_snapshot(fragment, replace(fragment, **changes))


def test_te_snapshot_identity_excludes_runtime_owner() -> None:
    _, binding = _registration_manifest_pair()
    fragment = binding.fragments[0]

    assert same_runtime_snapshot(fragment, replace(fragment, owner=object()))


def test_te_rejects_binding_for_different_placement_digest() -> None:
    placement, binding = _registration_manifest_pair()

    with pytest.raises(TransferEngineError, match="placement digest"):
        validate_manifest_pair(
            type("Plan", (), {"resource_id": "qwen", "revision": "step-1"})(),
            placement,
            replace(binding, placement_digest="0" * 64),
            "source",
        )
