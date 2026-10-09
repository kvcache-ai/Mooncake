from __future__ import annotations

from dataclasses import replace
from inspect import signature
from types import SimpleNamespace

import pytest

from mooncake.reshard.weight import store
from mooncake.reshard.weight._store import (
    PayloadStoreOperations as ExportedPayloadStoreOperations,
)
from mooncake.reshard.weight._store import (
    WeightUploadService as ExportedWeightUploadService,
)
from mooncake.reshard.weight._store import (
    WeightUploadTransaction as ExportedWeightUploadTransaction,
)
from mooncake.reshard.weight._store.entrypoint import begin_weight_snapshot
from mooncake.reshard.weight._store.store import WeightStore, WeightStoreError
from mooncake.reshard.weight._store.snapshot import (
    WeightSnapshotAdapter,
    WeightSnapshotDescriptor,
)
from mooncake.reshard.weight._store.writer import WeightStoreWriter
from mooncake.reshard.weight._store.backend import default_config_factory
from mooncake.reshard.weight._store.contracts import (
    UploadOperation,
    UploadReceipt,
    WeightLoadPlan,
    WeightUploadPlan,
)
from mooncake.reshard.weight._store.payload import PayloadStoreOperations
from mooncake.reshard.weight._store.transaction import WeightUploadTransaction
from mooncake.reshard.weight._store.upload import WeightUploadService
from mooncake.reshard.weight.manifest import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    SplitAxis,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)


def manifest_pair():
    tensor = TensorDescriptor(
        tensor_id="layers.0.weight",
        global_shape=(4,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layout_fingerprint="test:contiguous:v1",
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )
    rank = ParallelRank()
    placement = WeightPlacementManifest.from_fragments(
        resource_id="qwen",
        revision="step-1",
        weight_generation=3,
        placement_set_id="module-layout-test",
        topology=ParallelTopology(
            tp_size=1,
            pp_size=1,
            ep_size=1,
            dp_size=1,
            participants=(TopologyParticipant(participant_id="worker-0", rank=rank),),
        ),
        tensors=(tensor,),
        fragments=(
            PlacementFragment(
                placement_fragment_id="placement-0",
                tensor_id=tensor.tensor_id,
                global_offset=(0,),
                local_shape=(4,),
                nbytes=4,
                rank=rank,
            ),
        ),
    )
    binding = WeightRuntimeBindingManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        instance_id="worker-0",
        participant_id="worker-0",
        generation=7,
        lease_id="lease-7",
        fragments=(
            RuntimeBindingFragment(
                placement_fragment_id="placement-0",
                fragment_id="runtime-0",
                address=0x1000,
                nbytes=4,
                worker_id="worker-0",
                endpoint="worker-0:12345",
                device="cuda:0",
                itemsize=1,
                local_shape=(4,),
                strides_bytes=(1,),
                storage_address=0x1000,
                storage_nbytes=4,
                storage_offset_bytes=0,
            ),
        ),
    )
    return placement, binding


def test_store_responsibility_modules_preserve_public_identity() -> None:
    assert store.WeightStore is WeightStore
    assert store.WeightStoreError is WeightStoreError
    assert store.UploadOperation is UploadOperation
    assert store.UploadReceipt is UploadReceipt
    assert store.WeightLoadPlan is WeightLoadPlan
    assert store.WeightUploadPlan is WeightUploadPlan


def test_store_internal_modules_match_their_responsibilities() -> None:
    assert WeightStore.__module__.endswith("._store.store")
    assert WeightSnapshotDescriptor.__module__.endswith("._store.snapshot")
    assert WeightSnapshotAdapter.__module__.endswith("._store.snapshot")
    assert WeightStoreWriter.__module__.endswith("._store.writer")
    assert begin_weight_snapshot.__module__.endswith("._store.entrypoint")


def test_store_internal_services_have_one_definition() -> None:
    assert ExportedPayloadStoreOperations is PayloadStoreOperations
    assert ExportedWeightUploadTransaction is WeightUploadTransaction
    assert ExportedWeightUploadService is WeightUploadService


def test_store_contract_uses_explicit_placement_and_binding() -> None:
    for name in (
        "plan_upload",
        "upload",
        "commit_upload",
        "abort_upload",
        "finalize_upload_transaction",
        "plan_load",
        "load",
    ):
        assert callable(getattr(WeightStore, name))
    for legacy_name in (
        "prepare_upload",
        "commit",
        "finalize_upload_session",
    ):
        assert not hasattr(WeightStore, legacy_name)

    plan_parameters = tuple(signature(WeightStore.plan_upload).parameters)
    assert plan_parameters[:4] == (
        "self",
        "source_placement",
        "source_bindings",
        "namespace",
    )

    upload_parameters = tuple(signature(WeightStore.upload).parameters)
    assert upload_parameters[:7] == (
        "self",
        "plan",
        "source_placement",
        "source_binding",
        "source_worker_id",
        "source_allocation_guards",
        "registration_lease",
    )

    plan_load_parameters = tuple(signature(WeightStore.plan_load).parameters)
    assert plan_load_parameters[:4] == (
        "self",
        "manifest",
        "target_placement",
        "target_bindings",
    )

    load_parameters = tuple(signature(WeightStore.load).parameters)
    assert load_parameters[:7] == (
        "self",
        "plan",
        "target_placement",
        "target_binding",
        "target_worker_id",
        "target_allocation_guards",
        "registration_lease",
    )


def test_store_rejects_binding_for_different_placement_digest() -> None:
    placement, binding = manifest_pair()

    with pytest.raises(WeightStoreError, match="placement digest"):
        WeightStore(object()).plan_upload(
            placement,
            (replace(binding, placement_digest="0" * 64),),
        )


def test_native_store_requires_group_semantics_binding(monkeypatch) -> None:
    class ReplicateConfigWithoutGroups:
        __slots__ = ("with_hard_pin", "data_type")

        def __init__(self) -> None:
            self.with_hard_pin = False
            self.data_type = None

    class ObjectDataType:
        WEIGHT = "weight"
        METADATA = "metadata"

    native_store_module = SimpleNamespace(
        ReplicateConfig=ReplicateConfigWithoutGroups,
        ObjectDataType=ObjectDataType,
    )

    monkeypatch.setattr(
        "mooncake.reshard.weight._store.backend.import_module",
        lambda _: native_store_module,
    )

    with pytest.raises(WeightStoreError, match="ReplicateConfig.group_ids"):
        default_config_factory(("weight-group",), "payload")
