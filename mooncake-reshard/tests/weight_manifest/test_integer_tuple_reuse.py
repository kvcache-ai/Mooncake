"""Integer geometry normalization preserves public constructor contracts."""

import pytest

from mooncake.reshard.contracts import RuntimeBindingFragment
from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    SplitAxis,
    TensorDescriptor,
)


class TupleSubclass(tuple):
    pass


class IntSubclass(int):
    pass


def placement(shape, offset=(0,)):
    return PlacementFragment("tensor", offset, shape, 8, ParallelRank())


def runtime(shape):
    return RuntimeBindingFragment(
        "placement",
        "fragment",
        16,
        8,
        "worker",
        "endpoint",
        "cpu",
        4,
        shape,
        (4,),
        16,
        8,
        0,
    )


@pytest.mark.parametrize("constructor", [placement, runtime])
def test_exact_integer_shape_tuple_is_reused(constructor):
    shape = tuple([2])
    assert constructor(shape).local_shape is shape


def test_descriptor_and_placement_reuse_valid_integer_tuples():
    shape = tuple([2, 3])
    dims = tuple([0])
    value = TensorDescriptor(
        "tensor", shape, "float32", 4, dims, "layout", (SplitAxis("tp", 0),)
    )
    assert value.global_shape is shape
    assert value.shard_dims is dims
    offset = tuple([0, 1])
    assert placement(shape, offset).global_offset is offset


@pytest.mark.parametrize("constructor", [placement, runtime])
def test_lists_are_copied_before_later_mutation(constructor):
    shape = [2]
    value = constructor(shape)
    shape[0] = 3
    assert value.local_shape == (2,)
    assert type(value.local_shape) is tuple


def test_tuple_subclass_keeps_distinct_public_acceptance_rules():
    shape = TupleSubclass((2,))
    assert type(placement(shape).local_shape) is tuple
    with pytest.raises(ValueError, match="runtime local_shape must be a tuple or list"):
        runtime(shape)


@pytest.mark.parametrize(
    "constructor,prefix",
    [
        (placement, "local_shape"),
        (runtime, "runtime local_shape[0]"),
    ],
)
@pytest.mark.parametrize(
    "item,suffix",
    [
        (True, "must be an integer"),
        (IntSubclass(2), "must be an integer"),
        (2.0, "must be an integer"),
        ("2", "must be an integer"),
        (0, "must be at least 1"),
        (-1, "must be at least 1"),
    ],
)
def test_invalid_tuple_members_keep_error_messages(constructor, prefix, item, suffix):
    with pytest.raises(ValueError) as error:
        constructor((item,))
    assert str(error.value) == f"{prefix} {suffix}"


@pytest.mark.parametrize("constructor", [placement, runtime])
def test_generators_remain_rejected(constructor):
    with pytest.raises(ValueError):
        constructor(item for item in (2,))


def test_empty_and_large_integer_geometry_keep_existing_boundaries():
    assert placement(()).local_shape == ()
    assert placement((1 << 64,)).local_shape == (1 << 64,)
    assert runtime((1 << 64,)).local_shape == (1 << 64,)
    with pytest.raises(ValueError, match="runtime stride rank differs"):
        runtime(())
    with pytest.raises(ValueError, match="global_shape must not be empty"):
        TensorDescriptor("tensor", (), "float32", 4, (), "layout", ())
    with pytest.raises(ValueError, match="global_offset must be at least 0"):
        placement((2,), (-1,))
    with pytest.raises(
        ValueError, match="nbytes must fit in an unsigned 64-bit integer"
    ):
        PlacementFragment("tensor", (0,), (2,), 1 << 64, ParallelRank())


def test_runtime_singleton_strides_are_still_canonicalized():
    value = RuntimeBindingFragment(
        "placement",
        "fragment",
        16,
        8,
        "worker",
        "endpoint",
        "cpu",
        4,
        (1, 2),
        (0, 4),
        16,
        8,
        0,
    )
    assert value.strides_bytes == (8, 4)


def test_later_invalid_members_keep_their_original_error_index():
    with pytest.raises(ValueError) as error:
        runtime((2, True))
    assert str(error.value) == "runtime local_shape[1] must be an integer"
