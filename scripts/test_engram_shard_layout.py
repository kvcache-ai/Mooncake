"""CPU checks for placement independent of calculation ranks."""

import pytest

from mooncake._engram_shard import ShardLayout


def test_unequal_head_tables_have_disjoint_global_addresses():
    tables = {
        "h0": {"num_rows": 17, "row_bytes": 264},
        "h1": {"num_rows": 29, "row_bytes": 264},
        "h2": {"num_rows": 53, "row_bytes": 264},
    }
    layout = ShardLayout(tables, {"h0": 0, "h1": 0, "h2": 1}, 2)
    addresses = []
    for name, spec in tables.items():
        first = layout.owners[name] * layout.slot_rows + layout.starts[name]
        addresses += list(range(first, first + spec["num_rows"]))
    assert len(set(addresses)) == sum(t["num_rows"] for t in tables.values())
    assert layout.local_names(0) == ["h0", "h1"]
    assert layout.local_names(1) == ["h2"]
    assert layout.stride == 264
    assert (
        ShardLayout(dict(reversed(list(tables.items()))), layout.owners, 2).identity
        == layout.identity
    )


@pytest.mark.parametrize(
    "owners,shards", [({}, 2), ({"a": 2}, 2), ({"a": -1}, 2), ({"a": 0}, 0)]
)
def test_reject_incomplete_or_invalid_placement(owners, shards):
    with pytest.raises(ValueError):
        ShardLayout({"a": {"num_rows": 17, "row_bytes": 264}}, owners, shards)


def test_reject_address_overflow_before_allocating_storage():
    with pytest.raises(ValueError, match="int32"):
        ShardLayout({"a": {"num_rows": 2**30, "row_bytes": 264}}, {"a": 0}, 2)


def test_byte_width_padding_does_not_change_public_width():
    layout = ShardLayout({"a": {"num_rows": 7, "row_bytes": 17}}, {"a": 0}, 1)
    assert layout.width == 17 and layout.stride == 24
