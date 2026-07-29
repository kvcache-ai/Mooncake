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


def test_runtime_binding_inventory_retains_owner() -> None:
    placement = placement_manifest()
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (
            {
                "placement_fragment_id": "placement-0",
                "fragment_id": "runtime-0",
                "address": 0x1000,
                "nbytes": 32,
                "worker_id": "worker-0",
                "endpoint": "worker-0:12345",
                "device": "cuda:0",
                "is_contiguous": True,
            },
        ),
    }
    owner = object()

    binding = RuntimeBindingManifest.from_runtime_inventory(
        inventory,
        owner_resolver=lambda record: owner if record["fragment_id"] else None,
    )

    assert binding.fragments[0].owner is owner
    assert binding.fragments[0].device == "cuda:0"


def test_runtime_binding_inventory_requires_contiguous_proof() -> None:
    placement = placement_manifest()
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (
            {
                "placement_fragment_id": "placement-0",
                "fragment_id": "runtime-0",
                "address": 0x1000,
                "nbytes": 32,
                "worker_id": "worker-0",
                "endpoint": "worker-0:12345",
                "device": "cuda:0",
            },
        ),
    }

    with pytest.raises(ValueError, match="is_contiguous"):
        RuntimeBindingManifest.from_runtime_inventory(inventory)


def test_runtime_binding_inventory_rejects_fragment_generation_mismatch() -> None:
    placement = placement_manifest()
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (
            {
                "placement_fragment_id": "placement-0",
                "fragment_id": "runtime-0",
                "address": 0x1000,
                "nbytes": 32,
                "worker_id": "worker-0",
                "endpoint": "worker-0:12345",
                "device": "cuda:0",
                "is_contiguous": True,
                "lease_generation": 6,
            },
        ),
    }

    with pytest.raises(ValueError, match="lease generation"):
        RuntimeBindingManifest.from_runtime_inventory(inventory)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("is_contiguous", False, "contiguous"),
        ("storage_offset", -1, "storage_offset"),
        ("device", "", "device"),
    ],
)
def test_runtime_binding_inventory_rejects_unsafe_views(
    field: str, value: object, message: str
) -> None:
    placement = placement_manifest()
    fragment = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "is_contiguous": True,
        field: value,
    }
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (fragment,),
    }

    with pytest.raises(ValueError, match=message):
        RuntimeBindingManifest.from_runtime_inventory(inventory)


@pytest.mark.parametrize("offset_field", ["storage_offset", "byte_offset"])
def test_runtime_binding_inventory_requires_explicit_view_address_semantics(
    offset_field: str,
) -> None:
    placement = placement_manifest()
    fragment = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 32,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "is_contiguous": True,
        offset_field: 7,
    }
    inventory = {
        "model_id": MODEL_ID,
        "revision": REVISION,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "instance_id": "instance",
        "generation": 7,
        "lease_id": "lease-7",
        "fragments": (fragment,),
    }

    with pytest.raises(ValueError, match="address_semantics"):
        RuntimeBindingManifest.from_runtime_inventory(inventory)

    binding = RuntimeBindingManifest.from_runtime_inventory(
        inventory,
        address_semantics="view",
    )

    assert binding.fragments[0].address == 0x1000


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"placement_digest": ""}, "placement_digest"),
        ({"placement_digest": "g" * 64}, "SHA-256"),
        ({"placement_digest": "a" * 63}, "SHA-256"),
    ],
)
def test_runtime_binding_requires_content_attestation(
    overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        binding_manifest(**overrides)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"model_id": "other"}, "model_id"),
        ({"revision": "other"}, "revision"),
        ({"placement_id": "other"}, "placement_id"),
    ],
)
def test_binding_rejects_identity_mismatch(overrides: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        bind_runtime_manifest(placement_manifest(), binding_manifest(**overrides))


def test_binding_requires_exact_fragment_set_and_size() -> None:
    placement = placement_manifest()

    with pytest.raises(ValueError, match="missing placement fragment"):
        bind_runtime_manifest(
            placement,
            binding_manifest(placement=placement, fragments=()),
        )
    with pytest.raises(ValueError, match="unknown placement fragment"):
        bind_runtime_manifest(
            placement,
            binding_manifest(
                placement=placement,
                fragments=(binding_fragment(placement_fragment_id="unknown"),),
            ),
        )
    with pytest.raises(ValueError, match="byte size"):
        bind_runtime_manifest(
            placement,
            binding_manifest(
                placement=placement,
                fragments=(binding_fragment(nbytes=64),),
            ),
        )


def test_binding_rejects_duplicate_fragment_ids() -> None:
    fragment = binding_fragment()

    with pytest.raises(ValueError, match="duplicate placement fragment"):
        binding_manifest(fragments=(fragment, replace(fragment, fragment_id="other")))
    with pytest.raises(ValueError, match="duplicate runtime fragment_id"):
        binding_manifest(
            fragments=(
                fragment,
                replace(
                    fragment,
                    placement_fragment_id="placement-other",
                ),
            )
        )


def test_binding_allows_one_rank_to_span_runtime_locations() -> None:
    placement = placement_manifest(
        fragments=(
            placement_fragment(placement_fragment_id="left"),
            placement_fragment(
                placement_fragment_id="right",
                global_offset=(4, 0),
            ),
        )
    )
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(placement_fragment_id="left"),
            binding_fragment(
                placement_fragment_id="right",
                fragment_id="runtime-right",
                address=0x2000,
                worker_id="worker-1",
                endpoint="worker-1:12345",
            ),
        ),
    )

    runtime = bind_runtime_manifest(placement, binding)

    assert {fragment.worker_id for fragment in runtime.fragments} == {
        "worker-0",
        "worker-1",
    }


def test_binding_rejects_overlapping_runtime_ranges() -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(
                placement_fragment_id="a",
                tensor_id="a.weight",
                rank=ParallelRank(tp=0),
            ),
            placement_fragment(
                placement_fragment_id="b",
                tensor_id="b.weight",
                rank=ParallelRank(tp=1),
            ),
        ),
    )
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(placement_fragment_id="a", fragment_id="runtime-a"),
            binding_fragment(
                placement_fragment_id="b",
                fragment_id="runtime-b",
                endpoint="worker-0:54321",
            ),
        ),
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        bind_runtime_manifest(placement, binding)


def test_binding_preserves_logical_and_physical_halves() -> None:
    placement = placement_manifest()
    binding = binding_manifest(placement=placement)

    runtime = bind_runtime_manifest(placement, binding)

    assert runtime.model_id == placement.model_id
    assert runtime.revision == placement.revision
    assert runtime.placement_id == placement.placement_id
    assert runtime.instance_id == binding.instance_id
    assert runtime.generation == binding.generation
    assert runtime.lease_id == binding.lease_id
    assert runtime.fragments[0].global_offset == placement.fragments[0].global_offset
    assert runtime.fragments[0].address == binding.fragments[0].address
    assert runtime.fragments[0].placement_fragment_id == "placement-0"


def test_binding_order_does_not_change_runtime_manifest() -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="a",
            tensor_id="a.weight",
            rank=ParallelRank(tp=0),
        ),
        placement_fragment(
            placement_fragment_id="b",
            tensor_id="b.weight",
            rank=ParallelRank(tp=1),
        ),
    )
    placement = placement_manifest(tensors=tensors, fragments=fragments)
    bindings = (
        binding_fragment(
            placement_fragment_id="a",
            fragment_id="runtime-a",
            address=0x1000,
        ),
        binding_fragment(
            placement_fragment_id="b",
            fragment_id="runtime-b",
            address=0x2000,
        ),
    )

    first = bind_runtime_manifest(
        placement,
        binding_manifest(placement=placement, fragments=bindings),
    )
    second = bind_runtime_manifest(
        placement,
        binding_manifest(
            placement=placement,
            fragments=tuple(reversed(bindings)),
        ),
    )

    assert first == second


def test_empty_placement_binds_to_generation_scoped_empty_runtime() -> None:
    placement = placement_manifest(tensors=(), fragments=())
    binding = binding_manifest(
        placement=placement,
        fragments=(),
        generation=11,
        lease_id="lease-11",
    )

    runtime = bind_runtime_manifest(placement, binding)

    assert runtime.fragments == ()
    assert runtime.generation == 11
    assert runtime.lease_id == "lease-11"


def test_runtime_projection_round_trip_supports_rebinding() -> None:
    owner = object()
    runtime = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        generation=7,
        lease_id="lease-7",
        tensors=(descriptor(),),
        fragments=(runtime_fragment(owner=owner),),
    )

    placement = placement_manifest_from_runtime_manifest(runtime)
    binding = runtime_binding_from_runtime_manifest(runtime)
    rebound = bind_runtime_manifest(
        placement,
        replace(
            binding,
            instance_id="instance-2",
            generation=8,
            lease_id="lease-8",
            fragments=(
                replace(
                    binding.fragments[0],
                    address=0x2000,
                    worker_id="worker-2",
                    endpoint="worker-2:12345",
                ),
            ),
        ),
    )

    assert binding.fragments[0].owner is owner
    assert rebound.fragments[0].owner is owner
    assert rebound.fragments[0].address == 0x2000
    assert rebound.generation == 8
    assert placement.digest == placement_manifest_from_runtime_manifest(rebound).digest


@pytest.mark.parametrize(
    "mutate",
    [
        lambda placement: replace(
            placement,
            fragments=(replace(placement.fragments[0], global_offset=(4, 0)),),
        ),
        lambda placement: replace(
            placement,
            fragments=(replace(placement.fragments[0], rank=ParallelRank(tp=1)),),
        ),
        lambda placement: replace(
            placement,
            fragments=(
                replace(
                    placement.fragments[0],
                    aliases=("alias-a", "alias-b"),
                ),
            ),
        ),
        lambda placement: replace(
            placement,
            tensors=(replace(placement.tensors[0], dtype="float16"),),
        ),
        lambda placement: replace(
            placement,
            tensors=(
                replace(
                    placement.tensors[0],
                    layout_fingerprint="test:qwen:packed:v2",
                ),
            ),
        ),
        lambda placement: replace(
            placement,
            tensors=(
                replace(
                    placement.tensors[0],
                    partition_dim=None,
                    shard_dims=(0, 1),
                ),
            ),
            fragments=(
                replace(
                    placement.fragments[0],
                    local_shape=(8, 2),
                ),
            ),
        ),
    ],
)
def test_placement_identity_attests_exact_logical_content(mutate) -> None:
    placement = placement_manifest()

    with pytest.raises(ValueError, match="canonical logical content"):
        mutate(placement)


def test_projection_identity_is_stable_across_runtime_restarts() -> None:
    first = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance-a",
        generation=7,
        lease_id="lease-7",
        tensors=(descriptor(),),
        fragments=(runtime_fragment(fragment_id="runtime-a"),),
    )
    second = replace(
        first,
        instance_id="instance-b",
        generation=8,
        lease_id="lease-8",
        fragments=(
            replace(
                first.fragments[0],
                fragment_id="runtime-b",
                address=0x2000,
                worker_id="worker-b",
                endpoint="worker-b:12345",
                lease_generation=8,
            ),
        ),
    )

    first_placement = placement_manifest_from_runtime_manifest(first)
    second_placement = placement_manifest_from_runtime_manifest(second)

    assert first_placement.placement_id == second_placement.placement_id
    assert first_placement.digest == second_placement.digest
    assert (
        first_placement.fragments[0].placement_fragment_id
        == second_placement.fragments[0].placement_fragment_id
    )


def test_projection_identity_normalizes_single_axis_shard_representations() -> None:
    partitioned = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        generation=7,
        lease_id="lease-7",
        tensors=(descriptor(shard_dims=None),),
        fragments=(runtime_fragment(),),
    )
    nd_runtime = replace(
        partitioned,
        tensors=(descriptor(shard_dims=(0,)),),
    )

    partitioned_placement = placement_manifest_from_runtime_manifest(partitioned)
    nd_placement = placement_manifest_from_runtime_manifest(nd_runtime)

    assert partitioned_placement.placement_id == nd_placement.placement_id
    assert partitioned_placement.digest == nd_placement.digest


def test_runtime_projection_requires_lease_and_known_generation() -> None:
    without_lease = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        generation=7,
        tensors=(descriptor(),),
        fragments=(runtime_fragment(),),
    )
    without_generation = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        lease_id="lease",
        tensors=(),
        fragments=(),
    )

    with pytest.raises(ValueError, match="lease_id"):
        runtime_binding_from_runtime_manifest(without_lease)
    with pytest.raises(ValueError, match="generation"):
        runtime_binding_from_runtime_manifest(without_generation)


@pytest.mark.parametrize(
    "project",
    [
        placement_manifest_from_runtime_manifest,
        runtime_binding_from_runtime_manifest,
    ],
)
def test_runtime_projection_rejects_explicit_empty_placement_id(project) -> None:
    runtime = RuntimeManifest(
        model_id=MODEL_ID,
        revision=REVISION,
        instance_id="instance",
        generation=7,
        lease_id="lease-7",
        tensors=(descriptor(),),
        fragments=(runtime_fragment(),),
    )

    with pytest.raises(ValueError, match="placement_id"):
        project(runtime, placement_id="")


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ParallelRank(dp=True),
        lambda: runtime_fragment(address=4096.0),
        lambda: runtime_fragment(lease_generation=False),
        lambda: binding_fragment(nbytes=32.0),
        lambda: binding_manifest(generation=True),
    ],
)
def test_contract_rejects_bool_and_float_integer_fields(factory) -> None:
    with pytest.raises(ValueError, match="integer"):
        factory()


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: placement_manifest(tensors=(object(),)), "tensors"),
        (lambda: placement_manifest(fragments=(object(),)), "fragments"),
        (lambda: binding_manifest(fragments=(object(),)), "fragments"),
        (
            lambda: RuntimeManifest(
                model_id=MODEL_ID,
                revision=REVISION,
                instance_id="instance",
                tensors=(object(),),
                fragments=(),
            ),
            "tensors",
        ),
    ],
)
def test_manifest_collections_reject_wrong_element_types(factory, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: placement_manifest(tensors=None), "tensors"),
        (lambda: placement_manifest(fragments=None), "fragments"),
        (lambda: binding_manifest(fragments=None), "fragments"),
        (
            lambda: RuntimeManifest(
                model_id=MODEL_ID,
                revision=REVISION,
                instance_id="instance",
                tensors=None,
                fragments=(),
            ),
            "tensors",
        ),
        (
            lambda: RuntimeManifest(
                model_id=MODEL_ID,
                revision=REVISION,
                instance_id="instance",
                tensors=(),
                fragments=None,
            ),
            "fragments",
        ),
    ],
)
def test_manifest_collections_reject_wrong_container_types(
    factory, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: runtime_fragment(address=2**64),
        lambda: runtime_fragment(address=2**64 - 16, nbytes=32),
        lambda: runtime_fragment(nbytes=2**64),
        lambda: binding_fragment(address=2**64),
        lambda: binding_fragment(address=2**64 - 16, nbytes=32),
        lambda: binding_manifest(generation=2**64),
    ],
)
def test_physical_contract_rejects_values_outside_u64_abi(factory) -> None:
    with pytest.raises(ValueError, match="64-bit"):
        factory()


def test_physical_contract_rejects_unrepresentable_exclusive_end() -> None:
    with pytest.raises(ValueError, match="64-bit"):
        runtime_fragment(address=2**64 - 4, nbytes=4)


def test_inventory_missing_required_field_is_a_contract_error() -> None:
    inventory = runtime_inventory()
    del inventory["model_id"]

    with pytest.raises(ValueError, match="missing required field: model_id"):
        RuntimeManifest.from_runtime_inventory(inventory)
