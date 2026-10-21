from __future__ import annotations

import inspect
import typing

import pytest

import mooncake.reshard.weight.manifest as model_weight
from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    SplitAxis,
    TensorDescriptor,
)

from .helpers import descriptor


def test_public_api_is_minimal_and_explicit() -> None:
    assert model_weight.__all__ == [
        "ParallelRank",
        "ParallelTopology",
        "PlacementFragment",
        "TopologyParticipant",
        "WeightPlacementManifest",
        "WeightPlacementPart",
        "RuntimeBindingFragment",
        "WeightRuntimeBindingManifest",
        "SplitAxis",
        "ReplicatedAxis",
        "OwnershipAxis",
        "TensorDescriptor",
        "validate_runtime_binding",
        "validate_runtime_bindings",
    ]


def test_public_type_hints_resolve() -> None:
    for name in model_weight.__all__:
        value = getattr(model_weight, name)
        targets = (value, value.__init__) if inspect.isclass(value) else (value,)
        for target in targets:
            typing.get_type_hints(target)


def test_tensor_descriptor_uses_canonical_shard_dims() -> None:
    single_axis = descriptor()
    multidim = descriptor(
        shard_dims=(0, 1),
        global_shape=(8, 16, 32),
        expert_id=None,
        parallel_axes=(
            SplitAxis(kind="ep", dim=0),
            SplitAxis(kind="tp", dim=1),
        ),
    )

    assert single_axis.shard_dims == (0,)
    assert multidim.shard_dims == (0, 1)


def test_tensor_descriptor_carries_explicit_parallel_axis_semantics() -> None:
    value = descriptor(
        global_shape=(8, 16, 32),
        shard_dims=(0, 2),
        expert_id=None,
        parallel_axes=(
            SplitAxis(kind="tp", dim=2),
            SplitAxis(kind="ep", dim=0),
            OwnershipAxis(kind="pp"),
            ReplicatedAxis(kind="dp"),
        ),
    )

    assert tuple(axis.kind for axis in value.parallel_axes) == (
        "dp",
        "pp",
        "ep",
        "tp",
    )
    assert tuple(
        axis.dim for axis in value.parallel_axes if isinstance(axis, SplitAxis)
    ) == (0, 2)


@pytest.mark.parametrize(
    ("parallel_axes", "shard_dims", "message"),
    [
        ((SplitAxis(kind="tp", dim=1),), (0,), "shard_dims"),
        (
            (SplitAxis(kind="tp", dim=0), ReplicatedAxis(kind="tp")),
            (0,),
            "duplicate",
        ),
        (
            (SplitAxis(kind="tp", dim=0), SplitAxis(kind="ep", dim=0)),
            (0,),
            "share a dimension",
        ),
    ],
)
def test_tensor_descriptor_rejects_ambiguous_parallel_axis_semantics(
    parallel_axes: tuple[object, ...],
    shard_dims: tuple[int, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        descriptor(
            global_shape=(8, 8),
            shard_dims=shard_dims,
            parallel_axes=parallel_axes,
        )


def test_individually_allocated_expert_rejects_ep_split_semantics() -> None:
    with pytest.raises(ValueError, match="individually allocated expert"):
        descriptor(
            expert_id=3,
            parallel_axes=(SplitAxis(kind="ep", dim=0),),
        )

    assert descriptor(
        expert_id=3,
        shard_dims=(),
        parallel_axes=(OwnershipAxis(kind="ep"),),
    )


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: SplitAxis(kind="unknown", dim=0), "kind"),
        (lambda: SplitAxis(kind="tp", dim=-1), "at least"),
        (lambda: SplitAxis(kind="tp", dim=True), "integer"),
        (lambda: SplitAxis(kind="pp", dim=0), "split semantics"),
        (lambda: SplitAxis(kind="dp", dim=0), "split semantics"),
        (lambda: SplitAxis(kind="ep", dim=1), "leading"),
        (lambda: ReplicatedAxis(kind="unknown"), "kind"),
        (lambda: OwnershipAxis(kind="unknown"), "kind"),
    ],
)
def test_explicit_parallel_axis_rejects_invalid_schema(factory, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"global_shape": ()}, "global_shape"),
        ({"global_shape": (8.0, 4)}, "integer"),
        ({"itemsize": True}, "integer"),
        ({"shard_dims": (0, 0)}, "duplicates"),
        ({"shard_dims": (1, 0)}, "sorted"),
        ({"shard_dims": (2,)}, "out-of-range"),
        ({"shard_dims": (True,)}, "integer"),
        ({"layout_fingerprint": ""}, "layout_fingerprint"),
        ({"parallel_axes": (object(),)}, "explicit axis"),
    ],
)
def test_tensor_descriptor_rejects_invalid_schema(
    overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        descriptor(**overrides)


@pytest.mark.parametrize(
    "shape",
    [
        {8: None, 4: None},
        {8, 4},
        frozenset((8, 4)),
        (extent for extent in (8, 4)),
    ],
)
def test_tensor_descriptor_rejects_unordered_or_one_shot_shape(shape) -> None:
    with pytest.raises(ValueError, match="global_shape must contain integers"):
        descriptor(global_shape=shape)


def test_tensor_descriptor_accepts_ordered_shape_sequence() -> None:
    assert descriptor(global_shape=[8, 4]).global_shape == (8, 4)


def test_tensor_descriptor_requires_explicit_layout_fingerprint() -> None:
    values = {
        "tensor_id": "weight",
        "global_shape": (4, 4),
        "dtype": "bfloat16",
        "itemsize": 2,
        "shard_dims": (0,),
        "parallel_axes": (SplitAxis(kind="tp", dim=0),),
    }

    with pytest.raises(TypeError, match="layout_fingerprint"):
        TensorDescriptor(**values)


def test_placement_fragment_derives_stable_id_from_logical_content() -> None:
    values = {
        "tensor_id": "weight",
        "global_offset": (0, 0),
        "local_shape": (4, 4),
        "nbytes": 32,
        "rank": ParallelRank(tp=1),
    }

    first = PlacementFragment(**values)
    second = PlacementFragment(**values)

    assert first.placement_fragment_id == second.placement_fragment_id
    assert first.placement_fragment_id.startswith("sha256:")
