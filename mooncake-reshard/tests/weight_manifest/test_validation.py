from __future__ import annotations

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    TensorParallelAxis,
    TopologyParticipant,
)

from .helpers import (
    binding_fragment,
    binding_manifest,
    descriptor,
    parallel_topology,
    placement_fragment,
    placement_manifest,
    placement_part,
)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ParallelRank(dp=True),
        lambda: binding_fragment(address=4096.0),
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
        lambda: binding_fragment(address=2**64),
        lambda: binding_fragment(address=2**64 - 16, nbytes=32),
        lambda: binding_fragment(nbytes=2**64),
        lambda: binding_manifest(generation=2**64),
    ],
)
def test_physical_contract_rejects_values_outside_u64_abi(factory) -> None:
    with pytest.raises(ValueError, match="64-bit"):
        factory()


def test_physical_contract_rejects_unrepresentable_exclusive_end() -> None:
    with pytest.raises(ValueError, match="64-bit"):
        binding_fragment(address=2**64 - 4, nbytes=4)


def _n_dim_descriptor(**overrides):
    values = {
        "global_shape": (8, 8),
        "partition_dim": None,
        "shard_dims": (0, 1),
        "expert_id": None,
        "parallel_axes": (
            TensorParallelAxis(kind="ep", split_dim=0),
            TensorParallelAxis(kind="tp", split_dim=1),
        ),
    }
    values.update(overrides)
    return descriptor(**values)


def test_logical_validation_rejects_same_owner_n_dim_overlap() -> None:
    tensor = _n_dim_descriptor()

    with pytest.raises(ValueError, match="logical fragment boxes overlap"):
        placement_part(
            tensors=(tensor,),
            fragments=(
                placement_fragment(
                    placement_fragment_id="left",
                    local_shape=(6, 8),
                    nbytes=96,
                ),
                placement_fragment(
                    placement_fragment_id="right",
                    global_offset=(4, 0),
                    local_shape=(4, 8),
                    nbytes=64,
                ),
            ),
        )


def test_logical_validation_accepts_adjacent_n_dim_boxes() -> None:
    tensor = _n_dim_descriptor()
    fragments = tuple(
        placement_fragment(
            placement_fragment_id=f"box-{row}-{column}",
            global_offset=(row, column),
            local_shape=(4, 4),
            nbytes=32,
        )
        for row in (0, 4)
        for column in (0, 4)
    )

    placement = placement_manifest(tensors=(tensor,), fragments=fragments)

    assert len(placement.fragments) == 4


def test_complete_placement_validation_is_python_39_compatible() -> None:
    placement = placement_manifest()

    assert placement.fragments


@pytest.mark.parametrize(
    ("tensor", "fragment", "message"),
    [
        (
            _n_dim_descriptor(),
            placement_fragment(
                global_offset=(7, 0),
                local_shape=(2, 8),
                nbytes=32,
            ),
            "out of bounds",
        ),
        (
            descriptor(),
            placement_fragment(
                local_shape=(4, 2),
                nbytes=16,
            ),
            "non-shard axis",
        ),
        (
            descriptor(),
            placement_fragment(nbytes=31),
            "byte size mismatch",
        ),
    ],
)
def test_logical_validation_rejects_invalid_fragment_geometry(
    tensor, fragment, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        placement_part(tensors=(tensor,), fragments=(fragment,))


def test_logical_validation_allows_cross_rank_overlap() -> None:
    tensor = descriptor(global_shape=(8, 4))
    topology = parallel_topology(
        participants=(
            TopologyParticipant("worker-0", ParallelRank(tp=0)),
            TopologyParticipant("worker-1", ParallelRank(tp=1)),
        ),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="rank-0",
            local_shape=(8, 4),
            nbytes=64,
            rank=ParallelRank(tp=0),
        ),
        placement_fragment(
            placement_fragment_id="rank-1",
            local_shape=(8, 4),
            nbytes=64,
            rank=ParallelRank(tp=1),
        ),
    )

    placement = placement_manifest(
        topology=topology,
        tensors=(tensor,),
        fragments=fragments,
    )

    assert len(placement.fragments) == 2


def test_partial_part_is_allowed_but_global_placement_is_rejected() -> None:
    topology = parallel_topology()
    tensor = descriptor(global_shape=(8, 4))
    part = placement_part(
        topology=topology,
        tensors=(tensor,),
        fragments=(
            placement_fragment(
                local_shape=(4, 4),
                nbytes=32,
            ),
        ),
    )

    assert part.fragments[0].local_shape == (4, 4)
    with pytest.raises(ValueError, match="not fully covered"):
        placement_manifest(topology=topology, parts=(part,))
