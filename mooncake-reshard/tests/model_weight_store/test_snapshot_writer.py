from __future__ import annotations

import pytest

from mooncake.reshard.weight.store import (
    WeightSnapshotDescriptor,
    WeightStoreError,
)
from mooncake.reshard.weight.storage_manifest import StoredWeightManifest

from .helpers import (
    RuntimeInputs,
    allocation_guards_for_bindings,
    make_weight_store,
    source_manifests,
)


class _SnapshotAdapter:
    def __init__(self, source: RuntimeInputs) -> None:
        self.source = source

    def export_source(self, snapshot: WeightSnapshotDescriptor) -> RuntimeInputs:
        assert snapshot.resource_id == self.source.placement.resource_id
        assert snapshot.revision == self.source.placement.revision
        assert snapshot.weight_generation == self.source.placement.weight_generation
        return self.source

    def resolve_fragment_ids(
        self,
        *,
        tensor_id: str,
        tensor: object,
        source: RuntimeInputs,
    ) -> tuple[str, ...]:
        assert tensor_id == source.placement.tensors[0].tensor_id
        assert isinstance(tensor, str)
        return (tensor,)

    def source_allocation_guards(self, binding):
        return allocation_guards_for_bindings((binding,))


class _MismatchedTensorAdapter(_SnapshotAdapter):
    def resolve_fragment_ids(
        self,
        *,
        tensor_id: str,
        tensor: object,
        source: RuntimeInputs,
    ) -> tuple[str, ...]:
        return (source.bindings[0].fragments[0].placement_fragment_id,)


def _snapshot_descriptor(source: RuntimeInputs) -> WeightSnapshotDescriptor:
    return WeightSnapshotDescriptor(
        resource_id=source.placement.resource_id,
        revision=source.placement.revision,
        weight_generation=source.placement.weight_generation,
        namespace="production",
    )


def test_snapshot_writer_commits_one_stored_weight_manifest() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2, weight_generation=7)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    )

    for binding in source.bindings:
        fragment = binding.fragments[0]
        writer.write_tensor(
            source.placement.tensors[0].tensor_id,
            fragment.placement_fragment_id,
        )

    manifest = writer.commit()

    assert isinstance(manifest, StoredWeightManifest)
    assert manifest.namespace == "production"
    assert manifest.weight_generation == 7
    persisted = store.objects[manifest.manifest_key].decode("utf-8")
    assert "address" not in persisted
    assert "lease_id" not in persisted
    assert "instance_id" not in persisted


def test_snapshot_writer_retries_manifest_publish_after_commit_decision() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    )

    for binding in source.bindings:
        fragment = binding.fragments[0]
        writer.write_tensor(
            source.placement.tensors[0].tensor_id,
            fragment.placement_fragment_id,
        )

    store.fail_key = writer.plan.manifest.manifest_key
    with pytest.raises(WeightStoreError, match="manifest put failed"):
        writer.commit()

    assert writer.plan.control_key in store.objects
    assert writer.plan.manifest.manifest_key not in store.objects
    assert all(
        operation.target.object_key in store.objects
        for operation in writer.plan.operations
    )

    store.fail_key = None
    assert writer.commit() == writer.plan.manifest


def test_snapshot_writer_context_preserves_manifest_publish_retry() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    )

    store.fail_key = writer.plan.manifest.manifest_key
    with pytest.raises(WeightStoreError, match="manifest put failed"):
        with writer:
            for binding in source.bindings:
                fragment = binding.fragments[0]
                writer.write_tensor(
                    source.placement.tensors[0].tensor_id,
                    fragment.placement_fragment_id,
                )
            writer.commit()

    assert writer.plan.control_key in store.objects
    assert writer.plan.manifest.manifest_key not in store.objects
    with pytest.raises(WeightStoreError, match="retry commit instead"):
        writer.abort()

    store.fail_key = None
    assert writer.commit() == writer.plan.manifest


def test_snapshot_writer_rejects_commit_with_missing_fragment() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    )
    fragment = source.bindings[0].fragments[0]
    writer.write_tensor(
        source.placement.tensors[0].tensor_id,
        fragment.placement_fragment_id,
    )

    with pytest.raises(WeightStoreError, match="missing required fragments"):
        writer.commit()

    assert writer.plan.manifest.manifest_key not in store.objects
    assert not any(
        key.startswith(f"{writer.plan.manifest.group_id}/payload/")
        for key in store.objects
    )
    assert store.objects[writer.plan.control_key]


def test_snapshot_writer_rejects_fragment_for_another_tensor() -> None:
    _, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _MismatchedTensorAdapter(source),
    )

    with pytest.raises(WeightStoreError, match="another tensor"):
        writer.write_tensor("wrong-tensor", object())


def test_snapshot_writer_context_commits() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=2, weight_generation=7)

    with weight_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    ) as writer:
        for binding in source.bindings:
            fragment = binding.fragments[0]
            writer.write_tensor(
                source.placement.tensors[0].tensor_id,
                fragment.placement_fragment_id,
            )

    assert writer.plan.manifest.manifest_key in store.objects


def test_native_store_exposes_explicit_snapshot_writer_only() -> None:
    native_store = pytest.importorskip("mooncake.store")
    source = source_manifests(dp=1, tp=2, weight_generation=7)
    raw_store = native_store.MooncakeDistributedStore()
    writer = raw_store.begin_weight_snapshot(
        _snapshot_descriptor(source),
        _SnapshotAdapter(source),
    )

    assert hasattr(writer, "write_tensor")
    for name in (
        "get_tensor_with_parallelism",
        "batch_get_tensor_with_parallelism",
        "get_tensor_with_parallelism_into",
        "batch_get_tensor_with_parallelism_into",
        "put_tensor_with_parallelism",
        "batch_put_tensor_with_parallelism",
        "put_tensor_with_parallelism_from",
        "batch_put_tensor_with_parallelism_from",
        "upsert_tensor_with_parallelism",
        "upsert_tensor_with_parallelism_from",
        "batch_upsert_tensor_with_parallelism",
        "batch_upsert_tensor_with_parallelism_from",
    ):
        assert not hasattr(raw_store, name)
    for name in ("ParallelAxis", "TensorParallelism", "ReadTarget"):
        assert not hasattr(native_store, name)
