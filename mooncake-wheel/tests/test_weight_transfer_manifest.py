from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from mooncake.weight_transfer.manifest import (
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    StoredFragment,
    TensorDescriptor,
    WeightManifest,
)


def tensor_descriptor(**overrides) -> TensorDescriptor:
    values = {
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (8, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "sglang:qwen3.5:bf16:v1",
    }
    values.update(overrides)
    return TensorDescriptor(**values)


def runtime_fragment(**overrides) -> RuntimeFragment:
    values = {
        "fragment_id": "runtime-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "source-0",
        "endpoint": "source-0:12345",
        "rank": ParallelRank(dp=0, tp=0, pp=1, ep=1),
        "lease_generation": 7,
    }
    values.update(overrides)
    return RuntimeFragment(**values)


def stored_fragment(**overrides) -> StoredFragment:
    values = {
        "fragment_id": "stored-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_offset": (0, 0),
        "local_shape": (8, 4),
        "object_key": "weights/default/qwen/rev/payload/0",
        "object_offset": 0,
        "nbytes": 64,
    }
    values.update(overrides)
    return StoredFragment(**values)


def test_weight_manifest_round_trip_is_stable_and_has_no_runtime_address() -> None:
    manifest = WeightManifest(
        namespace="default",
        model_id="qwen3.5-0.8b",
        revision="step-42",
        group_id="weights/default/qwen3.5-0.8b/step-42",
        manifest_key="weights/default/qwen3.5-0.8b/step-42/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(
            stored_fragment(
                object_key=("weights/default/qwen3.5-0.8b/step-42/payload/0")
            ),
        ),
        created_at="2026-07-17T00:00:00Z",
    )

    encoded = manifest.to_json()

    assert WeightManifest.from_json(encoded) == manifest
    assert encoded == manifest.to_json()
    assert "4096" not in encoded
    assert "address" not in json.loads(encoded)["fragments"][0]


def test_runtime_manifest_keeps_addresses_ephemeral() -> None:
    manifest = RuntimeManifest(
        model_id="qwen3.5-0.8b",
        revision="step-42",
        instance_id="source",
        tensors=(tensor_descriptor(),),
        fragments=(runtime_fragment(),),
    )

    assert manifest.fragments[0].address == 0x1000
    assert not hasattr(manifest, "to_json")


def test_runtime_manifest_imports_framework_inventory_without_framework_dependency() -> (
    None
):
    rank = SimpleNamespace(dp=1, tp=2, pp=3, ep=4)
    tensor = SimpleNamespace(
        fragment_id="sglang-fragment",
        tensor_id="layers.2.experts.3.w1",
        global_shape=(8, 4),
        global_offset=(4, 0),
        local_shape=(4, 4),
        dtype="bfloat16",
        itemsize=2,
        partition_dim=0,
        layer_id=2,
        expert_id=3,
        layout_fingerprint="sglang:qwen3.5:moe-w13:v1",
        address=0x9000,
        nbytes=32,
        worker_id="sglang-worker",
        endpoint="sglang-worker:12345",
        rank=rank,
        lease_generation=9,
    )
    inventory = SimpleNamespace(
        model_id="qwen3.5-0.8b",
        revision="step-42",
        instance_id="sglang-instance",
        generation=9,
        tensors=(tensor,),
        format_version=1,
    )

    manifest = RuntimeManifest.from_runtime_inventory(
        inventory,
        owner_resolver=lambda record: ("owner", record.fragment_id),
    )

    assert manifest.tensors[0].tensor_id == tensor.tensor_id
    assert manifest.fragments[0].address == 0x9000
    assert manifest.fragments[0].rank == ParallelRank(dp=1, tp=2, pp=3, ep=4)
    assert manifest.fragments[0].owner == ("owner", "sglang-fragment")


@pytest.mark.parametrize(
    "fragment",
    [
        stored_fragment(tensor_id="missing"),
        stored_fragment(global_offset=(6, 0), local_shape=(4, 4)),
        stored_fragment(nbytes=31),
    ],
)
def test_weight_manifest_rejects_invalid_fragment(fragment: StoredFragment) -> None:
    with pytest.raises(ValueError):
        WeightManifest(
            namespace="default",
            model_id="qwen",
            revision="rev",
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(fragment,),
            created_at="2026-07-17T00:00:00Z",
        )


def test_weight_manifest_rejects_missing_tensor_coverage() -> None:
    with pytest.raises(ValueError, match="not fully covered"):
        WeightManifest(
            namespace="default",
            model_id="qwen",
            revision="rev",
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(stored_fragment(local_shape=(4, 4), nbytes=32),),
            created_at="2026-07-17T00:00:00Z",
        )


def test_weight_manifest_rejects_duplicate_fragment_geometry() -> None:
    with pytest.raises(ValueError, match="duplicate fragment geometry"):
        WeightManifest(
            namespace="default",
            model_id="qwen",
            revision="rev",
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(
                stored_fragment(),
                stored_fragment(
                    fragment_id="stored-1",
                    object_key="weights/default/qwen/rev/payload/1",
                ),
            ),
            created_at="2026-07-17T00:00:00Z",
        )


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"format_version": 2}, "unsupported runtime inventory format_version"),
        ({"generation": 8}, "lease generation mismatch"),
    ],
)
def test_runtime_manifest_rejects_incompatible_framework_inventory(
    overrides: dict, message: str
) -> None:
    tensor = SimpleNamespace(
        fragment_id="sglang-fragment",
        tensor_id="layers.2.experts.3.w1",
        global_shape=(8, 4),
        global_offset=(0, 0),
        local_shape=(4, 4),
        dtype="bfloat16",
        itemsize=2,
        partition_dim=0,
        layer_id=2,
        expert_id=3,
        layout_fingerprint="sglang:qwen3.5:moe-w13:v1",
        address=0x9000,
        nbytes=32,
        worker_id="sglang-worker",
        endpoint="sglang-worker:12345",
        rank=SimpleNamespace(dp=0, tp=0, pp=0, ep=0),
        lease_generation=9,
    )
    values = dict(
        model_id="qwen3.5-0.8b",
        revision="step-42",
        instance_id="sglang-instance",
        generation=9,
        tensors=(tensor,),
        format_version=1,
    )
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        RuntimeManifest.from_runtime_inventory(SimpleNamespace(**values))


def test_runtime_manifest_rejects_duplicate_fragment_ids() -> None:
    fragment = runtime_fragment()

    with pytest.raises(ValueError, match="duplicate fragment_id"):
        RuntimeManifest(
            model_id="qwen",
            revision="rev",
            instance_id="source",
            tensors=(tensor_descriptor(),),
            fragments=(fragment, fragment),
        )


def test_manifest_types_cannot_cross_the_runtime_storage_boundary() -> None:
    with pytest.raises(ValueError, match="RuntimeManifest fragments"):
        RuntimeManifest(
            model_id="qwen",
            revision="rev",
            instance_id="source",
            tensors=(tensor_descriptor(),),
            fragments=(stored_fragment(),),
        )

    with pytest.raises(ValueError, match="WeightManifest fragments"):
        WeightManifest(
            namespace="default",
            model_id="qwen",
            revision="rev",
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(runtime_fragment(),),
            created_at="2026-07-17T00:00:00Z",
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ParallelRank(dp=True),
        lambda: tensor_descriptor(global_shape=(8.0, 4)),
        lambda: tensor_descriptor(itemsize=2.0),
        lambda: tensor_descriptor(partition_dim=0.0),
        lambda: runtime_fragment(global_offset=(0.0, 0)),
        lambda: runtime_fragment(address=4096.0),
        lambda: runtime_fragment(lease_generation=False),
        lambda: stored_fragment(object_offset=0.0),
        lambda: stored_fragment(nbytes=64.0),
    ],
)
def test_manifest_schema_rejects_non_integer_numeric_fields(factory) -> None:
    with pytest.raises(ValueError, match="integer"):
        factory()


def test_weight_manifest_json_rejects_non_finite_and_non_mapping_values() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        WeightManifest.from_json('{"format_version":NaN}')

    with pytest.raises(ValueError, match="JSON object"):
        WeightManifest.from_json("[]")


def test_weight_manifest_json_rejects_float_geometry() -> None:
    manifest = WeightManifest(
        namespace="default",
        model_id="qwen",
        revision="rev",
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(stored_fragment(),),
        created_at="2026-07-17T00:00:00Z",
    )
    raw = json.loads(manifest.to_json())
    raw["tensors"][0]["global_shape"][0] = 8.0

    with pytest.raises(ValueError, match="integer"):
        WeightManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    "manifest_key, object_key",
    [
        (
            "weights/default/other/rev/manifest",
            "weights/default/qwen/rev/payload/0",
        ),
        (
            "weights/default/qwen/rev/manifest",
            "weights/default/other/rev/payload/0",
        ),
    ],
)
def test_weight_manifest_binds_manifest_and_payload_keys_to_group(
    manifest_key: str, object_key: str
) -> None:
    with pytest.raises(ValueError, match="group"):
        WeightManifest(
            namespace="default",
            model_id="qwen",
            revision="rev",
            group_id="weights/default/qwen/rev",
            manifest_key=manifest_key,
            tensors=(tensor_descriptor(),),
            fragments=(stored_fragment(object_key=object_key),),
            created_at="2026-07-17T00:00:00Z",
        )


@pytest.mark.parametrize("mutation", ["missing-version", "unknown-field"])
def test_weight_manifest_json_requires_exact_top_level_schema(mutation: str) -> None:
    manifest = WeightManifest(
        namespace="default",
        model_id="qwen",
        revision="rev",
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(stored_fragment(),),
        created_at="2026-07-17T00:00:00Z",
    )
    raw = json.loads(manifest.to_json())
    if mutation == "missing-version":
        del raw["format_version"]
    else:
        raw["future_semantics"] = "required"

    with pytest.raises(ValueError, match="schema"):
        WeightManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    "overrides",
    [
        {"global_shape": ()},
        {"partition_dim": 2},
        {"itemsize": 0},
        {"tensor_id": ""},
    ],
)
def test_tensor_descriptor_rejects_invalid_schema(overrides: dict) -> None:
    with pytest.raises(ValueError):
        tensor_descriptor(**overrides)
