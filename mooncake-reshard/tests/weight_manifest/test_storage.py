from __future__ import annotations

import json

import pytest

from mooncake.reshard.weight.manifest import (
    ParallelRank,
    RuntimeBindingFragment,
    TensorDescriptor,
    OwnershipAxis,
    SplitAxis,
)
from mooncake.reshard.weight.storage_manifest import (
    StoredFragmentSnapshot,
    StoredWeightManifest,
)


def tensor_descriptor(**overrides) -> TensorDescriptor:
    values = {
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (8, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "shard_dims": (0,),
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "sglang:qwen3.5:bf16:v1",
        "parallel_axes": (SplitAxis(kind="tp", dim=0),),
    }
    values.update(overrides)
    return TensorDescriptor(**values)


def runtime_binding_fragment(**overrides) -> RuntimeBindingFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "source-0",
        "endpoint": "source-0:12345",
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


def stored_fragment(**overrides) -> StoredFragmentSnapshot:
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
    return StoredFragmentSnapshot(**values)


def test_weight_manifest_round_trip_is_stable_and_has_no_runtime_address() -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        weight_generation=42,
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

    assert StoredWeightManifest.from_json(encoded) == manifest
    assert encoded == manifest.to_json()
    assert "4096" not in encoded
    assert "address" not in json.loads(encoded)["fragments"][0]


def test_weight_manifest_round_trip_preserves_shard_dims() -> None:
    tensor = tensor_descriptor(
        tensor_id="layers.2.experts.w1",
        global_shape=(2, 8, 4),
        shard_dims=(0, 1),
        expert_id=None,
        parallel_axes=(
            SplitAxis(kind="ep", dim=0),
            SplitAxis(kind="tp", dim=1),
        ),
    )
    group_id = "weights/default/qwen/rev"
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id=group_id,
        manifest_key=f"{group_id}/manifest",
        tensors=(tensor,),
        fragments=tuple(
            StoredFragmentSnapshot(
                fragment_id=f"stored-e{expert}-o{out_shard}",
                tensor_id=tensor.tensor_id,
                global_offset=(expert, out_shard * 4, 0),
                local_shape=(1, 4, 4),
                object_key=f"{group_id}/payload/e{expert}-o{out_shard}",
                object_offset=0,
                nbytes=32,
            )
            for expert in range(2)
            for out_shard in range(2)
        ),
        created_at="2026-07-22T00:00:00Z",
    )

    encoded = manifest.to_json()
    decoded = StoredWeightManifest.from_json(encoded)

    assert decoded == manifest
    assert json.loads(encoded)["tensors"][0]["shard_dims"] == [0, 1]


def test_weight_manifest_round_trip_preserves_single_and_multi_axis_shards() -> None:
    single_axis = tensor_descriptor(tensor_id="layers.0.attn.qkv", expert_id=None)
    multi_axis_tensor = tensor_descriptor(
        tensor_id="layers.2.experts.w1",
        global_shape=(2, 8, 4),
        shard_dims=(0, 1),
        expert_id=None,
        parallel_axes=(
            SplitAxis(kind="ep", dim=0),
            SplitAxis(kind="tp", dim=1),
        ),
    )
    group_id = "weights/default/qwen/rev"
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id=group_id,
        manifest_key=f"{group_id}/manifest",
        tensors=(single_axis, multi_axis_tensor),
        fragments=(
            StoredFragmentSnapshot(
                fragment_id="single-axis",
                tensor_id=single_axis.tensor_id,
                global_offset=(0, 0),
                local_shape=single_axis.global_shape,
                object_key=f"{group_id}/payload/single-axis",
                object_offset=0,
                nbytes=64,
            ),
            *(
                StoredFragmentSnapshot(
                    fragment_id=f"multi-e{expert}-o{out_shard}",
                    tensor_id=multi_axis_tensor.tensor_id,
                    global_offset=(expert, out_shard * 4, 0),
                    local_shape=(1, 4, 4),
                    object_key=(f"{group_id}/payload/multi-e{expert}-o{out_shard}"),
                    object_offset=0,
                    nbytes=32,
                )
                for expert in range(2)
                for out_shard in range(2)
            ),
        ),
        created_at="2026-07-22T00:00:00Z",
    )

    encoded = manifest.to_json()
    decoded = StoredWeightManifest.from_json(encoded)

    assert decoded == manifest
    raw_single_axis = next(
        tensor
        for tensor in json.loads(encoded)["tensors"]
        if tensor["tensor_id"] == single_axis.tensor_id
    )
    assert raw_single_axis["shard_dims"] == [0]


def test_weight_manifest_round_trip_preserves_parallel_axes() -> None:
    tensor = tensor_descriptor(
        parallel_axes=(
            OwnershipAxis(kind="pp"),
            SplitAxis(kind="tp", dim=0),
        ),
    )
    group_id = "weights/default/qwen/rev"
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id=group_id,
        manifest_key=f"{group_id}/manifest",
        tensors=(tensor,),
        fragments=(stored_fragment(object_key=f"{group_id}/payload/0"),),
        created_at="2026-07-22T00:00:00Z",
    )

    encoded = manifest.to_json()

    assert StoredWeightManifest.from_json(encoded) == manifest
    assert json.loads(encoded)["tensors"][0]["parallel_axes"] == [
        {"kind": "pp", "semantics": "ownership"},
        {"kind": "tp", "semantics": "split", "dim": 0},
    ]


def test_weight_manifest_json_emits_single_axis_shard_dims_field() -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(stored_fragment(),),
        created_at="2026-07-17T00:00:00Z",
    )

    raw = json.loads(manifest.to_json())

    assert raw["tensors"][0]["shard_dims"] == [0]


@pytest.mark.parametrize(
    "fragment",
    [
        stored_fragment(tensor_id="missing"),
        stored_fragment(global_offset=(6, 0), local_shape=(4, 4)),
        stored_fragment(nbytes=31),
    ],
)
def test_weight_manifest_rejects_invalid_fragment(
    fragment: StoredFragmentSnapshot,
) -> None:
    with pytest.raises(ValueError):
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(fragment,),
            created_at="2026-07-17T00:00:00Z",
        )


def test_weight_manifest_rejects_missing_tensor_coverage() -> None:
    with pytest.raises(ValueError, match="not fully covered"):
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(stored_fragment(local_shape=(4, 4), nbytes=32),),
            created_at="2026-07-17T00:00:00Z",
        )


def test_weight_manifest_rejects_duplicate_fragment_geometry() -> None:
    with pytest.raises(ValueError, match="duplicate fragment geometry"):
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
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


@pytest.mark.parametrize("second_offset", [0, 16])
def test_weight_manifest_rejects_overlapping_object_ranges(
    second_offset: int,
) -> None:
    with pytest.raises(ValueError, match="object ranges overlap"):
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(
                stored_fragment(local_shape=(4, 4), nbytes=32),
                stored_fragment(
                    fragment_id="stored-1",
                    global_offset=(4, 0),
                    local_shape=(4, 4),
                    object_offset=second_offset,
                    nbytes=32,
                ),
            ),
            created_at="2026-07-17T00:00:00Z",
        )


def test_weight_manifest_allows_adjacent_object_ranges() -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(
            stored_fragment(local_shape=(4, 4), nbytes=32),
            stored_fragment(
                fragment_id="stored-1",
                global_offset=(4, 0),
                local_shape=(4, 4),
                object_offset=32,
                nbytes=32,
            ),
        ),
        created_at="2026-07-17T00:00:00Z",
    )

    assert tuple(fragment.object_offset for fragment in manifest.fragments) == (0, 32)


def test_weight_manifest_allows_same_offset_for_different_objects() -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(
            stored_fragment(local_shape=(4, 4), nbytes=32),
            stored_fragment(
                fragment_id="stored-1",
                global_offset=(4, 0),
                local_shape=(4, 4),
                object_key="weights/default/qwen/rev/payload/1",
                nbytes=32,
            ),
        ),
        created_at="2026-07-17T00:00:00Z",
    )

    assert tuple(fragment.object_offset for fragment in manifest.fragments) == (0, 0)


def test_manifest_types_cannot_cross_the_runtime_storage_boundary() -> None:
    with pytest.raises(ValueError, match="StoredWeightManifest fragments"):
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
            group_id="weights/default/qwen/rev",
            manifest_key="weights/default/qwen/rev/manifest",
            tensors=(tensor_descriptor(),),
            fragments=(runtime_binding_fragment(),),
            created_at="2026-07-17T00:00:00Z",
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ParallelRank(dp=True),
        lambda: tensor_descriptor(global_shape=(8.0, 4)),
        lambda: tensor_descriptor(itemsize=2.0),
        lambda: tensor_descriptor(shard_dims=(0.0,)),
        lambda: runtime_binding_fragment(address=4096.0),
        lambda: runtime_binding_fragment(nbytes=False),
        lambda: stored_fragment(object_offset=0.0),
        lambda: stored_fragment(nbytes=64.0),
    ],
)
def test_manifest_schema_rejects_non_integer_numeric_fields(factory) -> None:
    with pytest.raises(ValueError, match="integer"):
        factory()


def test_weight_manifest_json_rejects_non_finite_and_non_mapping_values() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        StoredWeightManifest.from_json('{"model_id":NaN}')

    with pytest.raises(ValueError, match="JSON object"):
        StoredWeightManifest.from_json("[]")


def test_weight_manifest_json_rejects_float_geometry() -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(stored_fragment(),),
        created_at="2026-07-17T00:00:00Z",
    )
    raw = json.loads(manifest.to_json())
    raw["tensors"][0]["global_shape"][0] = 8.0

    with pytest.raises(ValueError, match="integer"):
        StoredWeightManifest.from_json(json.dumps(raw))


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
        StoredWeightManifest(
            namespace="default",
            resource_id="qwen",
            revision="rev",
            weight_generation=7,
            group_id="weights/default/qwen/rev",
            manifest_key=manifest_key,
            tensors=(tensor_descriptor(),),
            fragments=(stored_fragment(object_key=object_key),),
            created_at="2026-07-17T00:00:00Z",
        )


@pytest.mark.parametrize("mutation", ["missing-field", "unknown-field"])
def test_weight_manifest_json_requires_exact_top_level_schema(mutation: str) -> None:
    manifest = StoredWeightManifest(
        namespace="default",
        resource_id="qwen",
        revision="rev",
        weight_generation=7,
        group_id="weights/default/qwen/rev",
        manifest_key="weights/default/qwen/rev/manifest",
        tensors=(tensor_descriptor(),),
        fragments=(stored_fragment(),),
        created_at="2026-07-17T00:00:00Z",
    )
    raw = json.loads(manifest.to_json())
    if mutation == "missing-field":
        del raw["revision"]
    else:
        raw["future_semantics"] = "required"

    with pytest.raises(ValueError, match="schema"):
        StoredWeightManifest.from_json(json.dumps(raw))
