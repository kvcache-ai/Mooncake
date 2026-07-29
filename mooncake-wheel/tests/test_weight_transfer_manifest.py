from __future__ import annotations

import inspect
import json
import typing
from dataclasses import replace
from types import SimpleNamespace

import pytest

import mooncake.weight_transfer as weight_transfer
from mooncake.weight_transfer import (
    ParallelRank,
    PlacementFragment,
    PlacementManifest,
    RuntimeBindingFragment,
    RuntimeBindingManifest,
    RuntimeFragment,
    RuntimeManifest,
    TensorDescriptor,
    bind_runtime_manifest,
    placement_manifest_from_runtime_manifest,
    runtime_binding_from_runtime_manifest,
)


MODEL_ID = "model"
REVISION = "revision"


def test_public_api_is_minimal_and_explicit() -> None:
    assert weight_transfer.__all__ == [
        "ParallelRank",
        "PlacementFragment",
        "PlacementManifest",
        "RuntimeBindingFragment",
        "RuntimeBindingManifest",
        "RuntimeFragment",
        "RuntimeManifest",
        "TensorDescriptor",
        "bind_runtime_manifest",
        "placement_manifest_from_runtime_manifest",
        "runtime_binding_from_runtime_manifest",
    ]


def test_public_type_hints_resolve() -> None:
    for name in weight_transfer.__all__:
        value = getattr(weight_transfer, name)
        targets = (value, value.__init__) if inspect.isclass(value) else (value,)
        for target in targets:
            typing.get_type_hints(target)


def descriptor(**overrides) -> TensorDescriptor:
    values = {
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (8, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "test:qwen:bf16:v1",
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
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "rank": ParallelRank(dp=0, tp=0, pp=1, ep=1),
        "lease_generation": 7,
    }
    values.update(overrides)
    return RuntimeFragment(**values)


def placement_fragment(**overrides) -> PlacementFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "nbytes": 32,
        "rank": ParallelRank(dp=0, tp=0, pp=1, ep=1),
    }
    values.update(overrides)
    return PlacementFragment(**values)


def placement_manifest(**overrides) -> PlacementManifest:
    values = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": None,
        "tensors": (descriptor(),),
        "fragments": (placement_fragment(),),
    }
    values.update(overrides)
    return PlacementManifest(**values)


def binding_fragment(**overrides) -> RuntimeBindingFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
    }
    values.update(overrides)
    return RuntimeBindingFragment(**values)


def binding_manifest(
    *,
    placement: PlacementManifest | None = None,
    **overrides,
) -> RuntimeBindingManifest:
    logical = placement or placement_manifest()
    values = {
        "model_id": logical.model_id,
        "revision": logical.revision,
        "placement_id": logical.placement_id,
        "placement_digest": logical.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (binding_fragment(),),
    }
    values.update(overrides)
    return RuntimeBindingManifest(**values)


def runtime_inventory_tensor(**overrides) -> dict:
    values = {
        "fragment_id": "runtime-0",
        "placement_fragment_id": "placement-0",
        "tensor_id": "layers.2.experts.3.w1",
        "global_shape": (8, 4),
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
        "layer_id": 2,
        "expert_id": 3,
        "layout_fingerprint": "test:qwen:bf16:v1",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "rank": {"dp": 0, "tp": 0, "pp": 1, "ep": 1},
        "lease_generation": 7,
        "aliases": (),
        "is_contiguous": True,
        "stride": (4, 1),
        "storage_offset": 0,
        "byte_offset": 0,
    }
    values.update(overrides)
    return values


def runtime_inventory(**overrides) -> dict:
    values = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "instance_id": "instance",
        "placement_id": None,
        "generation": 7,
        "lease_id": "lease-7",
        "tensors": (runtime_inventory_tensor(),),
    }
    values.update(overrides)
    return values


def test_partition_dim_and_single_axis_shard_dims_are_normalized() -> None:
    single_axis = descriptor()
    nd_descriptor = descriptor(
        partition_dim=None,
        shard_dims=(0, 1),
        global_shape=(8, 16, 32),
    )

    assert single_axis.effective_shard_dims == (0,)
    assert nd_descriptor.effective_shard_dims == (0, 1)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"global_shape": ()}, "global_shape"),
        ({"global_shape": (8.0, 4)}, "integer"),
        ({"itemsize": True}, "integer"),
        ({"partition_dim": 2}, "out of range"),
        ({"partition_dim": 0, "shard_dims": (1,)}, "conflicts"),
        ({"partition_dim": None, "shard_dims": (0, 0)}, "duplicates"),
        ({"partition_dim": None, "shard_dims": (1, 0)}, "sorted"),
        ({"partition_dim": None, "shard_dims": (2,)}, "out-of-range"),
        ({"partition_dim": None, "shard_dims": (True,)}, "integer"),
        ({"layout_fingerprint": ""}, "layout_fingerprint"),
    ],
)
def test_tensor_descriptor_rejects_invalid_schema(
    overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        descriptor(**overrides)


def test_tensor_descriptor_requires_explicit_layout_fingerprint() -> None:
    values = {
        "tensor_id": "weight",
        "global_shape": (4, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "partition_dim": 0,
    }

    with pytest.raises(TypeError, match="layout_fingerprint"):
        TensorDescriptor(**values)


def test_runtime_inventory_is_framework_neutral_and_retains_owner() -> None:
    inventory = runtime_inventory(
        tensors=(runtime_inventory_tensor(device="cuda:0"),),
    )
    owner = object()

    manifest = RuntimeManifest.from_runtime_inventory(
        inventory,
        owner_resolver=lambda record: owner if record["fragment_id"] else None,
    )

    assert manifest.model_id == MODEL_ID
    assert manifest.revision == REVISION
    assert manifest.instance_id == "instance"
    assert manifest.placement_id is None
    assert manifest.generation == 7
    assert manifest.lease_id == "lease-7"
    assert manifest.fragments[0].owner is owner
    assert manifest.fragments[0].device == "cuda:0"
    assert manifest.fragments[0].rank == ParallelRank(dp=0, tp=0, pp=1, ep=1)
    assert not hasattr(manifest, "to_json")


def test_runtime_inventory_accepts_object_records_and_optional_semantics() -> None:
    tensor = runtime_inventory_tensor()
    tensor.pop("layer_id")
    tensor.pop("expert_id")
    tensor.pop("aliases")
    inventory = SimpleNamespace(
        **{
            **runtime_inventory(),
            "tensors": (SimpleNamespace(**tensor),),
        }
    )

    manifest = RuntimeManifest.from_runtime_inventory(inventory)

    assert manifest.tensors[0].layer_id is None
    assert manifest.tensors[0].expert_id is None
    assert manifest.fragments[0].aliases == ()


def test_runtime_inventory_imports_multi_axis_logical_box() -> None:
    tensor = runtime_inventory_tensor(
        tensor_id="layers.2.experts.w1",
        global_shape=(8, 16, 32),
        global_offset=(3, 8, 0),
        local_shape=(1, 8, 32),
        partition_dim=None,
        shard_dims=(0, 1),
        expert_id=None,
        nbytes=512,
        stride=(256, 32, 1),
    )

    manifest = RuntimeManifest.from_runtime_inventory(
        runtime_inventory(tensors=(tensor,))
    )

    assert manifest.tensors[0].effective_shard_dims == (0, 1)
    assert manifest.fragments[0].global_offset == (3, 8, 0)
    assert manifest.fragments[0].local_shape == (1, 8, 32)


def test_runtime_inventory_normalizes_equivalent_single_axis_descriptors() -> None:
    first = runtime_inventory_tensor(
        fragment_id="runtime-0",
        placement_fragment_id="placement-0",
        global_offset=(0, 0),
        address=0x1000,
        worker_id="worker-0",
        endpoint="worker-0:12345",
        rank={"dp": 0, "tp": 0, "pp": 1, "ep": 1},
    )
    second = runtime_inventory_tensor(
        fragment_id="runtime-1",
        placement_fragment_id="placement-1",
        shard_dims=(0,),
        global_offset=(4, 0),
        address=0x2000,
        worker_id="worker-1",
        endpoint="worker-1:12345",
        rank={"dp": 0, "tp": 1, "pp": 1, "ep": 1},
    )

    manifest = RuntimeManifest.from_runtime_inventory(
        runtime_inventory(tensors=(first, second))
    )

    assert manifest.tensors[0].partition_dim == 0
    assert manifest.tensors[0].shard_dims == (0,)
    assert len(manifest.fragments) == 2


@pytest.mark.parametrize(
    "tensor_overrides, message",
    [
        ({"is_contiguous": False}, "contiguous"),
        ({"is_contiguous": 1}, "contiguous"),
        ({"stride": (1, 4)}, "canonical stride"),
        ({"stride": (4, True)}, "stride"),
        ({"storage_offset": -1}, "storage_offset"),
        ({"storage_offset": 0.0}, "storage_offset"),
        ({"byte_offset": 1}, "item-aligned"),
        ({"byte_offset": 0.0}, "byte_offset"),
        ({"nbytes": 31}, "byte size mismatch"),
    ],
)
def test_runtime_inventory_rejects_unsafe_physical_views(
    tensor_overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        RuntimeManifest.from_runtime_inventory(
            runtime_inventory(tensors=(runtime_inventory_tensor(**tensor_overrides),))
        )


def test_runtime_inventory_requires_explicit_view_address_semantics() -> None:
    inventory = runtime_inventory(
        tensors=(
            runtime_inventory_tensor(
                storage_offset=7,
                byte_offset=14,
            ),
        )
    )

    with pytest.raises(ValueError, match="address_semantics"):
        RuntimeManifest.from_runtime_inventory(inventory)

    manifest = RuntimeManifest.from_runtime_inventory(
        inventory,
        address_semantics="view",
    )

    assert manifest.fragments[0].address == 0x1000


def test_runtime_inventory_rejects_unknown_address_semantics() -> None:
    with pytest.raises(ValueError, match="address_semantics"):
        RuntimeManifest.from_runtime_inventory(
            runtime_inventory(),
            address_semantics="storage",
        )


def test_runtime_inventory_accepts_arbitrary_singleton_stride() -> None:
    tensor = runtime_inventory_tensor(
        global_shape=(8, 1, 4),
        global_offset=(0, 0, 0),
        local_shape=(4, 1, 4),
        stride=(4, 99, 1),
        nbytes=32,
    )

    manifest = RuntimeManifest.from_runtime_inventory(
        runtime_inventory(tensors=(tensor,))
    )

    assert manifest.fragments[0].local_shape == (4, 1, 4)


def test_runtime_inventory_requires_snapshot_generation_match() -> None:
    with pytest.raises(ValueError, match="lease generation mismatch"):
        RuntimeManifest.from_runtime_inventory(
            runtime_inventory(
                generation=8,
                tensors=(runtime_inventory_tensor(lease_generation=7),),
            )
        )


def test_direct_runtime_manifest_requires_one_generation() -> None:
    with pytest.raises(ValueError, match="inconsistent lease generations"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=(descriptor(),),
            fragments=(
                runtime_fragment(fragment_id="runtime-0", lease_generation=7),
                runtime_fragment(
                    fragment_id="runtime-1",
                    global_offset=(4, 0),
                    address=0x2000,
                    lease_generation=8,
                ),
            ),
        )


def test_runtime_manifest_derives_generation_for_direct_construction() -> None:
    manifest = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        tensors=(descriptor(),),
        fragments=(runtime_fragment(),),
    )

    assert manifest.generation == 7


def test_empty_runtime_manifest_retains_explicit_generation() -> None:
    manifest = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        tensors=(),
        fragments=(),
        generation=9,
        lease_id="lease-9",
    )

    assert runtime_binding_from_runtime_manifest(manifest).generation == 9


@pytest.mark.parametrize("right_address", [0x1000, 0x1010])
def test_runtime_manifest_rejects_overlapping_independent_ranges(
    right_address: int,
) -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=tensors,
            fragments=(
                runtime_fragment(fragment_id="runtime-a", tensor_id="a.weight"),
                runtime_fragment(
                    fragment_id="runtime-b",
                    tensor_id="b.weight",
                    address=right_address,
                ),
            ),
        )


def test_runtime_manifest_rejects_nested_overlap() -> None:
    tensors = (
        descriptor(
            tensor_id="large.weight",
            global_shape=(16,),
            dtype="uint8",
            itemsize=1,
            partition_dim=None,
        ),
        descriptor(
            tensor_id="small.weight",
            global_shape=(4,),
            dtype="uint8",
            itemsize=1,
            partition_dim=None,
        ),
        descriptor(
            tensor_id="nested.weight",
            global_shape=(4,),
            dtype="uint8",
            itemsize=1,
            partition_dim=None,
        ),
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=tensors,
            fragments=(
                runtime_fragment(
                    fragment_id="large",
                    tensor_id="large.weight",
                    global_offset=(0,),
                    local_shape=(16,),
                    nbytes=16,
                ),
                runtime_fragment(
                    fragment_id="small",
                    tensor_id="small.weight",
                    global_offset=(0,),
                    local_shape=(4,),
                    address=0x1000,
                    nbytes=4,
                    aliases=("large.weight", "small.weight"),
                ),
                runtime_fragment(
                    fragment_id="nested",
                    tensor_id="nested.weight",
                    global_offset=(0,),
                    local_shape=(4,),
                    address=0x1008,
                    nbytes=4,
                ),
            ),
        )


def test_runtime_manifest_treats_endpoint_as_routing_not_address_space() -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=tensors,
            fragments=(
                runtime_fragment(fragment_id="a", tensor_id="a.weight"),
                runtime_fragment(
                    fragment_id="b",
                    tensor_id="b.weight",
                    endpoint="worker-0:54321",
                ),
            ),
        )


def test_runtime_manifest_allows_same_address_on_different_workers() -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )

    manifest = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        tensors=tensors,
        fragments=(
            runtime_fragment(fragment_id="a", tensor_id="a.weight"),
            runtime_fragment(
                fragment_id="b",
                tensor_id="b.weight",
                worker_id="worker-1",
                endpoint="worker-1:12345",
            ),
        ),
    )

    assert len(manifest.fragments) == 2


def test_runtime_manifest_treats_device_as_address_space() -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )

    manifest = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        tensors=tensors,
        fragments=(
            runtime_fragment(fragment_id="a", tensor_id="a.weight"),
            runtime_fragment(
                fragment_id="b",
                tensor_id="b.weight",
                device="cuda:1",
            ),
        ),
    )

    assert len(manifest.fragments) == 2


def test_runtime_manifest_rejects_duplicate_logical_box_for_same_rank() -> None:
    with pytest.raises(ValueError, match="duplicate logical fragment"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            generation=7,
            tensors=(descriptor(),),
            fragments=(
                runtime_fragment(fragment_id="runtime-0", address=0x1000),
                runtime_fragment(fragment_id="runtime-1", address=0x2000),
            ),
        )


def test_runtime_manifest_allows_only_exact_compatible_declared_aliases() -> None:
    aliases = ("lm_head.weight", "model.embed_tokens.weight")
    tensors = (
        descriptor(tensor_id="embed.weight"),
        descriptor(tensor_id="head.weight"),
    )
    fragments = tuple(
        runtime_fragment(
            fragment_id=f"runtime-{tensor.tensor_id}",
            tensor_id=tensor.tensor_id,
            aliases=aliases,
        )
        for tensor in tensors
    )

    manifest = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        tensors=tensors,
        fragments=fragments,
    )

    assert len(manifest.fragments) == 2

    with pytest.raises(ValueError, match="address ranges overlap"):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=(
                tensors[0],
                replace(tensors[1], layout_fingerprint="different"),
            ),
            fragments=fragments,
        )


@pytest.mark.parametrize(
    "fragment",
    [
        runtime_fragment(tensor_id="missing"),
        runtime_fragment(global_offset=(6, 0)),
        runtime_fragment(nbytes=31),
    ],
)
def test_runtime_manifest_rejects_invalid_fragment(fragment: RuntimeFragment) -> None:
    with pytest.raises(ValueError):
        RuntimeManifest(
            model_id=MODEL_ID,
            revision=REVISION,
            instance_id="instance",
            tensors=(descriptor(),),
            fragments=(fragment,),
        )


def test_runtime_fragment_requires_device() -> None:
    with pytest.raises(ValueError, match="device"):
        runtime_fragment(device="")


def test_placement_round_trip_is_stable_and_address_free() -> None:
    placement = placement_manifest()

    encoded = placement.to_json()
    decoded = PlacementManifest.from_json(encoded)

    assert decoded == placement
    assert decoded.digest == placement.digest
    assert placement.placement_id == (
        "sha256:c4f3bc2feed99a64ff156fd57cb5cf626ae8b38c1551c0fe2225e1921e14d73a"
    )
    assert placement.digest == (
        "7320a5090dc88556c57e5d9e8cb316d05b3bf99b00f7a0c0da04198ca995e915"
    )
    assert encoded == placement.to_json()
    for forbidden in (
        "address",
        "endpoint",
        "worker_id",
        "instance_id",
        "generation",
        "lease_id",
        "owner",
    ):
        assert forbidden not in encoded


def test_placement_digest_is_independent_of_inventory_order() -> None:
    tensors = (
        descriptor(tensor_id="b.weight"),
        descriptor(tensor_id="a.weight"),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="b",
            tensor_id="b.weight",
            rank=ParallelRank(tp=1),
        ),
        placement_fragment(
            placement_fragment_id="a",
            tensor_id="a.weight",
            rank=ParallelRank(tp=0),
        ),
    )

    first = placement_manifest(tensors=tensors, fragments=fragments)
    second = placement_manifest(
        tensors=tuple(reversed(tensors)),
        fragments=tuple(reversed(fragments)),
    )

    assert first == second
    assert first.to_json() == second.to_json()
    assert first.digest == second.digest


def test_placement_normalizes_partition_dim_and_shard_dims() -> None:
    partitioned = placement_manifest(
        tensors=(descriptor(shard_dims=None),),
    )
    nd_placement = placement_manifest(
        tensors=(descriptor(shard_dims=(0,)),),
    )

    assert partitioned == nd_placement
    assert partitioned.tensors[0].shard_dims == (0,)
    assert partitioned.digest == nd_placement.digest


@pytest.mark.parametrize("mutation", ["missing", "unknown", "nan"])
def test_placement_json_requires_strict_schema(mutation: str) -> None:
    raw = json.loads(placement_manifest().to_json())
    if mutation == "missing":
        del raw["revision"]
    elif mutation == "unknown":
        raw["future_semantics"] = "required"
    else:
        raw["model_id"] = float("nan")

    with pytest.raises(ValueError):
        PlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("path", "mutation"),
    [
        (("tensors", 0), ("pop", "dtype")),
        (("tensors", 0), ("set", "future_semantics")),
        (("fragments", 0), ("pop", "nbytes")),
        (("fragments", 0), ("set", "future_semantics")),
        (("fragments", 0, "rank"), ("pop", "tp")),
        (("fragments", 0, "rank"), ("set", "future_semantics")),
    ],
)
def test_placement_json_requires_strict_nested_schema(
    path: tuple, mutation: tuple[str, str]
) -> None:
    raw = json.loads(placement_manifest().to_json())
    target = raw
    for component in path:
        target = target[component]
    operation, field = mutation
    if operation == "pop":
        target.pop(field)
    else:
        target[field] = "unsupported"

    with pytest.raises(ValueError, match="schema"):
        PlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize("value", ["not-json", "[]", '"placement"'])
def test_placement_json_rejects_invalid_document(value: str) -> None:
    with pytest.raises(ValueError):
        PlacementManifest.from_json(value)


def test_placement_json_rejects_duplicate_object_keys() -> None:
    encoded = placement_manifest().to_json()
    duplicated = encoded.replace(
        '"model_id":"model"',
        '"model_id":"model","model_id":"other"',
        1,
    )

    with pytest.raises(ValueError, match="duplicate JSON field"):
        PlacementManifest.from_json(duplicated)


@pytest.mark.parametrize("aliases", ["alias", {"alias": 1}, ["alias", "alias"]])
def test_placement_json_rejects_invalid_aliases(aliases) -> None:
    raw = json.loads(placement_manifest().to_json())
    raw["fragments"][0]["aliases"] = aliases

    with pytest.raises(ValueError, match="aliases"):
        PlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("tensors",), {}),
        (("fragments",), 1),
        (("tensors", 0, "global_shape"), 8),
        (("tensors", 0, "shard_dims"), "0"),
        (("fragments", 0, "global_offset"), 0),
        (("fragments", 0, "local_shape"), None),
        (("fragments", 0, "rank"), []),
    ],
)
def test_placement_json_rejects_wrong_container_types(
    path: tuple, value: object
) -> None:
    raw = json.loads(placement_manifest().to_json())
    target = raw
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value

    with pytest.raises(ValueError):
        PlacementManifest.from_json(json.dumps(raw))


def test_placement_inventory_is_framework_neutral() -> None:
    tensor = runtime_inventory_tensor()
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "tensors": (
            {
                key: value
                for key, value in tensor.items()
                if key
                in {
                    "placement_fragment_id",
                    "tensor_id",
                    "global_shape",
                    "global_offset",
                    "local_shape",
                    "dtype",
                    "itemsize",
                    "partition_dim",
                    "layer_id",
                    "expert_id",
                    "layout_fingerprint",
                    "nbytes",
                    "rank",
                    "aliases",
                }
            },
        ),
    }

    placement = PlacementManifest.from_runtime_inventory(inventory)

    assert placement.fragments[0].rank == ParallelRank(dp=0, tp=0, pp=1, ep=1)
    assert placement.fragments[0].global_offset == (0, 0)


def test_placement_inventory_normalizes_equivalent_single_axis_descriptors() -> None:
    fields = {
        "placement_fragment_id",
        "tensor_id",
        "global_shape",
        "global_offset",
        "local_shape",
        "dtype",
        "itemsize",
        "partition_dim",
        "layer_id",
        "expert_id",
        "layout_fingerprint",
        "shard_dims",
        "nbytes",
        "rank",
        "aliases",
    }
    first = runtime_inventory_tensor(
        placement_fragment_id="placement-0",
        global_offset=(0, 0),
        rank={"dp": 0, "tp": 0, "pp": 1, "ep": 1},
    )
    second = runtime_inventory_tensor(
        placement_fragment_id="placement-1",
        shard_dims=(0,),
        global_offset=(4, 0),
        rank={"dp": 0, "tp": 1, "pp": 1, "ep": 1},
    )
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "tensors": tuple(
            {key: value for key, value in tensor.items() if key in fields}
            for tensor in (first, second)
        ),
    }

    placement = PlacementManifest.from_runtime_inventory(inventory)

    assert placement.tensors[0].partition_dim == 0
    assert placement.tensors[0].shard_dims == (0,)
    assert len(placement.fragments) == 2


def test_placement_id_must_match_canonical_logical_content() -> None:
    with pytest.raises(ValueError, match="canonical logical content"):
        placement_manifest(placement_id="opaque-placement-id")
