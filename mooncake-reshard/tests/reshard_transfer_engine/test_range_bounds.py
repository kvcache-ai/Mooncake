from dataclasses import replace

import pytest

from mooncake.reshard.transfer_engine import TransferBatchRange


def _range():
    return TransferBatchRange(4096, 32, 8192, 64, (0, 24), (16, 56), (8, 8))


def test_range_accepts_exact_capacity_boundary():
    assert _range().operation_count == 2


@pytest.mark.parametrize("side", ("source", "target"))
@pytest.mark.parametrize("offset", (-1, True, 1.5, 1 << 64))
def test_range_rejects_invalid_offsets_on_either_side(side, offset):
    with pytest.raises(ValueError, match=f"{side} offset is invalid"):
        replace(_range(), **{side + "_offsets": (0, offset)})


@pytest.mark.parametrize("side,offset", (("source", 25), ("target", 57)))
def test_range_rejects_last_segment_crossing_capacity(side, offset):
    with pytest.raises(ValueError, match=f"exceeds {side} allocation bounds"):
        replace(_range(), **{side + "_offsets": (0, offset)})


@pytest.mark.parametrize("size", (0, -1, True, 1.5, 1 << 64))
def test_range_rejects_invalid_sizes_before_address_arithmetic(size):
    with pytest.raises(ValueError, match="size is invalid"):
        replace(_range(), sizes=(8, size))


def test_range_rejects_unaligned_segment_sequences():
    with pytest.raises(ValueError, match="aligned"):
        replace(_range(), target_offsets=(0,))
