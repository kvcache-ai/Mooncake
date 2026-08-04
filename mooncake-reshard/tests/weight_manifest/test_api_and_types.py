from __future__ import annotations

import inspect
import typing
import pytest

import mooncake.reshard.weight.manifest as model_weight

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    TensorParallelAxis,
    TensorDescriptor,
)

from .helpers import (
    descriptor,
)


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
        "TensorParallelAxis",
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


def test_partition_dim_and_single_axis_shard_dims_are_normalized() -> None:
    single_axis = descriptor()
    multidim_descriptor = descriptor(
        partition_dim=None,
        shard_dims=(0, 1),
        global_shape=(8, 16, 32),
        expert_id=None,
        parallel_axes=(
            TensorParallelAxis(kind="ep", split_dim=0),
            TensorParallelAxis(kind="tp", split_dim=1),
        ),
    )

    assert single_axis.effective_shard_dims == (0,)
    assert multidim_descriptor.effective_shard_dims == (0, 1)


def test_tensor_descriptor_carries_parallel_axis_semantics() -> None:
    value = descriptor(
        global_shape=(8, 16, 32),
        partition_dim=None,
        shard_dims=(0, 2),
        expert_id=None,
        parallel_axes=(
            TensorParallelAxis(kind="tp", split_dim=2),
            TensorParallelAxis(kind="ep", split_dim=0),
            TensorParallelAxis(kind="pp"),
        ),
    )

    assert tuple(axis.kind for axis in value.parallel_axes) == ("pp", "ep", "tp")
    assert tuple(
        axis.split_dim for axis in value.parallel_axes if axis.split_dim is not None
    ) == (0, 2)


@pytest.mark.parametrize(
    ("parallel_axes", "shard_dims", "message"),
    [
        ((TensorParallelAxis(kind="tp", split_dim=1),), (0,), "shard_dims"),
        (
            (
                TensorParallelAxis(kind="tp", split_dim=0),
                TensorParallelAxis(kind="tp", split_dim=1),
            ),
            (0, 1),
            "duplicate",
        ),
        (
            (
                TensorParallelAxis(kind="tp", split_dim=0),
                TensorParallelAxis(kind="ep", split_dim=0),
            ),
            (0,),
            "split_dim",
        ),
    ],
)
def test_tensor_descriptor_rejects_ambiguous_parallel_axis_semantics(
    parallel_axes: tuple[TensorParallelAxis, ...],
    shard_dims: tuple[int, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        descriptor(
            global_shape=(8, 8),
            partition_dim=None,
            shard_dims=shard_dims,
            parallel_axes=parallel_axes,
        )


def test_individually_allocated_expert_rejects_ep_split_semantics() -> None:
    with pytest.raises(ValueError, match="individually allocated expert"):
        descriptor(
            expert_id=3,
            parallel_axes=(TensorParallelAxis(kind="ep", split_dim=0),),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"kind": "unknown"}, "kind"),
        ({"kind": "tp", "split_dim": -1}, "split_dim"),
        ({"kind": "tp", "split_dim": True}, "integer"),
        ({"kind": "pp", "split_dim": 0}, "ownership-only"),
        ({"kind": "dp", "split_dim": 0}, "ownership-only"),
        ({"kind": "ep", "split_dim": 1}, "leading"),
    ],
)
def test_tensor_parallel_axis_rejects_invalid_schema(
    overrides: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        TensorParallelAxis(**overrides)


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
        "partition_dim": 0,
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
