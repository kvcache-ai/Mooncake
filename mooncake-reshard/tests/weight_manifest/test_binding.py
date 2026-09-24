from __future__ import annotations

from dataclasses import replace
from math import prod
from types import SimpleNamespace

import pytest

from mooncake.reshard.weight import (
    SplitAxis,
    validate_runtime_binding,
)

from .helpers import (
    binding_fragment,
    binding_manifest,
    descriptor,
    placement_fragment,
    placement_manifest,
)


def test_runtime_binding_fragment_retains_owner() -> None:
    owner = object()

    fragment = binding_fragment(owner=owner)

    assert fragment.owner is owner
    assert fragment.device == "cuda:0"


def test_runtime_binding_rejects_duck_typed_manifests() -> None:
    placement = placement_manifest()
    binding = binding_manifest(placement=placement)
    placement_fields = dict(vars(placement))
    placement_fields.update(
        resource_kind=placement.resource_kind,
        digest=placement.digest,
    )
    duck_placement = SimpleNamespace(**placement_fields)
    duck_binding = SimpleNamespace(
        **vars(binding),
        resource_kind=binding.resource_kind,
    )

    with pytest.raises(ValueError, match="WeightPlacementManifest"):
        validate_runtime_binding(duck_placement, binding)
    with pytest.raises(ValueError, match="WeightRuntimeBindingManifest"):
        validate_runtime_binding(placement, duck_binding)


def test_runtime_binding_rejects_noncanonical_stride() -> None:
    placement = placement_manifest()
    binding = binding_manifest(
        placement=placement,
        fragments=(binding_fragment(strides_bytes=(2, 8)),),
    )

    with pytest.raises(ValueError, match="stride"):
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize(
    ("shape", "strides_bytes", "canonical_strides_bytes"),
    [
        ((1, 4), (200, 2), (8, 2)),
        ((4, 1), (2, 200), (2, 2)),
        ((2, 1, 4), (8, 0, 2), (8, 8, 2)),
        ((1,), (0,), (2,)),
    ],
)
def test_binding_normalizes_stride_on_singleton_dimensions(
    shape: tuple[int, ...],
    strides_bytes: tuple[int, ...],
    canonical_strides_bytes: tuple[int, ...],
) -> None:
    nbytes = prod(shape) * 2
    tensor = descriptor(
        global_shape=shape,
        shard_dims=(),
        parallel_axes=(),
        expert_id=None,
    )
    placement = placement_manifest(
        tensors=(tensor,),
        fragments=(
            placement_fragment(
                global_offset=(0,) * len(shape),
                local_shape=shape,
                nbytes=nbytes,
            ),
        ),
    )
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(
                nbytes=nbytes,
                local_shape=shape,
                strides_bytes=strides_bytes,
            ),
        ),
    )

    assert binding.fragments[0].strides_bytes == canonical_strides_bytes
    assert validate_runtime_binding(placement, binding) is None


def test_runtime_binding_does_not_trust_runtime_itemsize() -> None:
    placement = placement_manifest()
    binding = binding_manifest(
        placement=placement,
        fragments=(binding_fragment(itemsize=1),),
    )

    with pytest.raises(ValueError, match="itemsize"):
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"storage_offset_bytes": -1}, "storage_offset_bytes"),
        ({"device": ""}, "device"),
        (
            {
                "address": 0x1000,
                "storage_address": 0x1000,
                "storage_nbytes": 64,
                "storage_offset_bytes": 8,
            },
            "storage_address plus",
        ),
        (
            {
                "address": 0x1010,
                "storage_address": 0x1000,
                "storage_nbytes": 40,
                "storage_offset_bytes": 16,
            },
            "storage allocation",
        ),
    ],
)
def test_runtime_binding_fragment_rejects_unsafe_views(
    overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        binding_fragment(**overrides)


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
        ({"resource_id": "other"}, "resource_id"),
        ({"revision": "other"}, "revision"),
        ({"placement_id": "other"}, "placement_id"),
    ],
)
def test_binding_rejects_identity_mismatch(overrides: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_runtime_binding(placement_manifest(), binding_manifest(**overrides))


def test_binding_requires_exact_fragment_set_and_size() -> None:
    placement = placement_manifest()

    with pytest.raises(ValueError, match="missing placement fragment"):
        validate_runtime_binding(
            placement,
            binding_manifest(placement=placement, fragments=()),
        )
    with pytest.raises(ValueError, match="unknown placement fragment"):
        validate_runtime_binding(
            placement,
            binding_manifest(
                placement=placement,
                fragments=(binding_fragment(placement_fragment_id="unknown"),),
            ),
        )
    with pytest.raises(ValueError, match="byte size"):
        validate_runtime_binding(
            placement,
            binding_manifest(
                placement=placement,
                fragments=(binding_fragment(nbytes=64),),
            ),
        )

    with pytest.raises(ValueError, match="local_shape"):
        validate_runtime_binding(
            placement,
            binding_manifest(
                placement=placement,
                fragments=(
                    binding_fragment(
                        local_shape=(2, 8),
                        strides_bytes=(16, 2),
                    ),
                ),
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
        tensors=(descriptor(global_shape=(8, 4)),),
        fragments=(
            placement_fragment(placement_fragment_id="left"),
            placement_fragment(
                placement_fragment_id="right",
                global_offset=(4, 0),
            ),
        ),
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

    assert validate_runtime_binding(placement, binding) is None


@pytest.mark.parametrize("right_address", [0x1000, 0x1010])
def test_binding_rejects_overlapping_runtime_ranges(right_address: int) -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(
                placement_fragment_id="a",
                tensor_id="a.weight",
            ),
            placement_fragment(
                placement_fragment_id="b",
                tensor_id="b.weight",
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
                address=right_address,
                endpoint="worker-0:54321",
            ),
        ),
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        validate_runtime_binding(placement, binding)


def test_binding_rejects_partially_overlapping_storage_allocations() -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(placement_fragment_id="a", tensor_id="a.weight"),
            placement_fragment(placement_fragment_id="b", tensor_id="b.weight"),
        ),
    )
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(
                placement_fragment_id="a",
                fragment_id="runtime-a",
                address=0x1100,
                storage_address=0x1000,
                storage_nbytes=0x1000,
                storage_offset_bytes=0x100,
            ),
            binding_fragment(
                placement_fragment_id="b",
                fragment_id="runtime-b",
                address=0x2100,
                storage_address=0x1800,
                storage_nbytes=0x1000,
                storage_offset_bytes=0x900,
            ),
        ),
    )

    with pytest.raises(ValueError, match="storage allocation ranges overlap"):
        validate_runtime_binding(placement, binding)


def test_binding_allows_disjoint_views_of_same_storage_allocation() -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(placement_fragment_id="a", tensor_id="a.weight"),
            placement_fragment(placement_fragment_id="b", tensor_id="b.weight"),
        ),
    )
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(
                placement_fragment_id="a",
                fragment_id="runtime-a",
                address=0x1000,
                storage_address=0x1000,
                storage_nbytes=0x100,
                storage_offset_bytes=0,
            ),
            binding_fragment(
                placement_fragment_id="b",
                fragment_id="runtime-b",
                address=0x1080,
                storage_address=0x1000,
                storage_nbytes=0x100,
                storage_offset_bytes=0x80,
            ),
        ),
    )

    assert validate_runtime_binding(placement, binding) is None


def test_binding_treats_endpoint_as_routing_not_address_space() -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(placement_fragment_id="a", tensor_id="a.weight"),
            placement_fragment(placement_fragment_id="b", tensor_id="b.weight"),
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
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize(
    ("field", "value"),
    [("worker_id", "worker-1"), ("device", "cuda:1")],
)
def test_binding_separates_worker_and_device_address_spaces(
    field: str,
    value: str,
) -> None:
    placement = placement_manifest(
        tensors=(
            descriptor(tensor_id="a.weight"),
            descriptor(tensor_id="b.weight"),
        ),
        fragments=(
            placement_fragment(placement_fragment_id="a", tensor_id="a.weight"),
            placement_fragment(placement_fragment_id="b", tensor_id="b.weight"),
        ),
    )
    second_overrides = {
        "placement_fragment_id": "b",
        "fragment_id": "runtime-b",
        field: value,
    }
    if field == "worker_id":
        second_overrides["endpoint"] = "worker-1:12345"
    binding = binding_manifest(
        placement=placement,
        fragments=(
            binding_fragment(placement_fragment_id="a", fragment_id="runtime-a"),
            binding_fragment(**second_overrides),
        ),
    )

    assert validate_runtime_binding(placement, binding) is None


def test_binding_allows_only_exact_compatible_declared_aliases() -> None:
    aliases = ("embed.weight", "head.weight")
    tensors = (
        descriptor(tensor_id="embed.weight", expert_id=None),
        descriptor(tensor_id="head.weight", expert_id=None),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="embed",
            tensor_id="embed.weight",
            aliases=aliases,
        ),
        placement_fragment(
            placement_fragment_id="head",
            tensor_id="head.weight",
            aliases=aliases,
        ),
    )
    placement = placement_manifest(tensors=tensors, fragments=fragments)
    binding_fragments = (
        binding_fragment(
            placement_fragment_id="embed",
            fragment_id="runtime-embed",
        ),
        binding_fragment(
            placement_fragment_id="head",
            fragment_id="runtime-head",
        ),
    )

    assert (
        validate_runtime_binding(
            placement,
            binding_manifest(placement=placement, fragments=binding_fragments),
        )
        is None
    )

    incompatible = placement_manifest(
        tensors=(
            tensors[0],
            replace(tensors[1], layout_fingerprint="different"),
        ),
        fragments=fragments,
    )
    with pytest.raises(ValueError, match="address ranges overlap"):
        validate_runtime_binding(
            incompatible,
            binding_manifest(
                placement=incompatible,
                fragments=binding_fragments,
            ),
        )

    incompatible_axes = placement_manifest(
        tensors=(
            tensors[0],
            replace(
                tensors[1],
                parallel_axes=(SplitAxis(kind="ep", dim=0),),
            ),
        ),
        fragments=fragments,
    )
    with pytest.raises(ValueError, match="address ranges overlap"):
        validate_runtime_binding(
            incompatible_axes,
            binding_manifest(
                placement=incompatible_axes,
                fragments=binding_fragments,
            ),
        )


def test_binding_rejects_alias_group_without_tensor_members() -> None:
    aliases = ("unrelated-x", "unrelated-y")

    with pytest.raises(ValueError, match="alias"):
        placement_manifest(
            tensors=(
                descriptor(tensor_id="a.weight"),
                descriptor(tensor_id="b.weight"),
            ),
            fragments=(
                placement_fragment(
                    placement_fragment_id="a",
                    tensor_id="a.weight",
                    aliases=aliases,
                ),
                placement_fragment(
                    placement_fragment_id="b",
                    tensor_id="b.weight",
                    aliases=aliases,
                ),
            ),
        )


def test_binding_validation_preserves_logical_and_physical_halves() -> None:
    placement = placement_manifest()
    binding = binding_manifest(placement=placement)

    assert validate_runtime_binding(placement, binding) is None
    assert placement.fragments[0].global_offset == (0, 0)
    assert binding.fragments[0].address == 0x1000


def test_binding_validation_is_order_independent() -> None:
    tensors = (
        descriptor(tensor_id="a.weight"),
        descriptor(tensor_id="b.weight"),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="a",
            tensor_id="a.weight",
        ),
        placement_fragment(
            placement_fragment_id="b",
            tensor_id="b.weight",
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

    first = validate_runtime_binding(
        placement,
        binding_manifest(placement=placement, fragments=bindings),
    )
    second = validate_runtime_binding(
        placement,
        binding_manifest(
            placement=placement,
            fragments=tuple(reversed(bindings)),
        ),
    )

    assert first == second


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"tensors": (), "fragments": ()}, "must contain tensors"),
    ],
)
def test_global_placement_must_not_be_empty(
    overrides: dict,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        placement_manifest(**overrides)
