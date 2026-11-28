"""Unit tests for mooncake.shared_segment argument parsing and multi-owner API."""

from __future__ import annotations

import ctypes
import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest


def test_shared_segment_binary_blob_round_trip():
    engine = pytest.importorskip("mooncake.engine")
    segment_type = engine.SharedSegment
    size = 4096
    owner, owner_blob = segment_type.create("binary-round-trip", size, 2, 0)
    peer, peer_blob = segment_type.create("binary-round-trip", size, 2, 1)
    blobs = [owner_blob, peer_blob]

    assert all(isinstance(blob, bytes) and b"\0" in blob for blob in blobs)
    assert not owner.ready()
    assert not peer.ready()
    owner.complete(blobs)
    peer.complete(blobs)

    assert owner.ready()
    assert peer.ready()
    payload = b"mooncake\0\xff"
    ctypes.memmove(owner.base_addr(), payload, len(payload))
    assert ctypes.string_at(peer.base_addr(), len(payload)) == payload


def _load_shared_segment_module():
    """Load mooncake-integration/shared_segment.py with a stub mooncake.engine."""
    repo_root = Path(__file__).resolve().parents[3]
    src_path = repo_root / "mooncake-integration" / "shared_segment.py"

    fake_pkg = types.ModuleType("mooncake_test_pkg")
    fake_pkg.__path__ = [str(src_path.parent)]
    fake_engine = types.ModuleType("mooncake_test_pkg.engine")

    class _StubCppSegment:
        supported_result = True
        last_create_args = None

        def __init__(self, base_addr=40 << 40, size=4096, owners=(0,), device_addr=0):
            self._base = base_addr
            self._size = size
            self._owners = list(owners)
            self._device = device_addr
            self.completed_blobs = None

        @classmethod
        def supported(cls, mmap=True, host_register=False):
            return cls.supported_result

        @classmethod
        def create(
            cls,
            name,
            size,
            world_size,
            rank_id,
            owner_rank=0,
            device_id=0,
            mmap=True,
            host_register=False,
            owner_ranks=None,
        ):
            cls.last_create_args = {
                "name": name,
                "size": size,
                "world_size": world_size,
                "rank_id": rank_id,
                "owner_rank": owner_rank,
                "device_id": device_id,
                "mmap": mmap,
                "host_register": host_register,
                "owner_ranks": owner_ranks,
            }
            owners = owner_ranks if owner_ranks is not None else [owner_rank]
            return cls(size=size, owners=owners), b"stub-blob"

        def complete(self, blobs):
            self.completed_blobs = list(blobs)

        def base_addr(self):
            return self._base

        def device_addr(self):
            return self._device

        def size(self):
            return self._size

        def owner_ranks(self):
            return list(self._owners)

    fake_engine.SharedSegment = _StubCppSegment
    sys.modules["mooncake_test_pkg"] = fake_pkg
    sys.modules["mooncake_test_pkg.engine"] = fake_engine

    spec = importlib.util.spec_from_file_location(
        "mooncake_test_pkg.shared_segment", src_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["mooncake_test_pkg.shared_segment"] = module
    spec.loader.exec_module(module)
    return module, _StubCppSegment


def _sample_blocks():
    torch = pytest.importorskip("torch")
    return {"k": {"count": 2, "shape": (4, 4), "dtype": torch.float32}}


def test_comm_group_and_tp_group_kwargs_compat():
    blocks = _sample_blocks()
    mod, stub = _load_shared_segment_module()
    group_a = object()
    group_b = object()

    with mock.patch.object(
        mod, "_raise_if_any_rank_failed"
    ) as mock_check, mock.patch.object(
        mod, "_all_gather_blob", return_value=[b"a", b"b"]
    ) as mock_gather:
        # New comm_group parameter works.
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, comm_group=group_a
        )
        assert mock_gather.call_args[0][2] is group_a
        assert mock_check.call_args[0][3] is group_a

        # Legacy tp_group keyword accepted via **kwargs.
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, tp_group=group_a
        )
        assert mock_gather.call_args[0][2] is group_a

        # Passing both with the same object is allowed.
        mod.create_shared_segment(
            "seg",
            blocks,
            world_size=2,
            rank_id=0,
            comm_group=group_a,
            tp_group=group_a,
        )
        assert mock_gather.call_args[0][2] is group_a

    # Conflicting comm_group and tp_group raises SharedSegmentError.
    with pytest.raises(mod.SharedSegmentError, match="deprecated alias"):
        mod.create_shared_segment(
            "seg",
            blocks,
            world_size=2,
            rank_id=0,
            comm_group=group_a,
            tp_group=group_b,
        )

    # Extra positional args or unknown kwargs raise TypeError.
    with pytest.raises(TypeError, match="positional"):
        mod.create_shared_segment("seg", blocks, 1, 0, 0, 0, None, True, False, "extra")
    with pytest.raises(TypeError, match="unexpected keyword"):
        mod.create_shared_segment("seg", blocks, world_size=1, rank_id=0, bogus_kw=123)
    assert stub.last_create_args is not None


def test_owner_ranks_and_legacy_owner_rank_compat():
    blocks = _sample_blocks()
    mod, stub = _load_shared_segment_module()

    # Default owner is (0,).
    seg = mod.create_shared_segment("seg", blocks, world_size=1, rank_id=0)
    assert seg.owner_ranks() == (0,)
    assert stub.last_create_args["owner_ranks"] == [0]
    assert stub.last_create_args["owner_rank"] == 0

    # Multi-owner list via owner_ranks.
    with mock.patch.object(mod, "_raise_if_any_rank_failed"), mock.patch.object(
        mod, "_all_gather_blob", return_value=[b"0", b"1", b"2", b"3"]
    ):
        seg = mod.create_shared_segment(
            "seg",
            blocks,
            world_size=4,
            rank_id=1,
            owner_ranks=[0, 2],
            comm_group=object(),
        )
        assert seg.owner_ranks() == (0, 2)
        assert stub.last_create_args["owner_ranks"] == [0, 2]
        assert stub.last_create_args["owner_rank"] == 0

        # Legacy owner_rank keyword works with both int and sequence.
        seg = mod.create_shared_segment(
            "seg",
            blocks,
            world_size=4,
            rank_id=1,
            owner_rank=3,
            comm_group=object(),
        )
        assert seg.owner_ranks() == (3,)
        assert stub.last_create_args["owner_ranks"] == [3]

        # Positional arg 4 accepts int or sequence.
        seg = mod.create_shared_segment("seg", blocks, 4, 1, [2, 0], 0, object())
        assert seg.owner_ranks() == (2, 0)
        assert stub.last_create_args["owner_ranks"] == [2, 0]


def test_owner_ranks_validation_errors():
    blocks = _sample_blocks()
    mod, _ = _load_shared_segment_module()

    with pytest.raises(mod.SharedSegmentError, match="not be empty"):
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, owner_ranks=[]
        )

    with pytest.raises(mod.SharedSegmentError, match="duplicate"):
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, owner_ranks=[0, 0]
        )

    with pytest.raises(mod.SharedSegmentError, match=r"\[0, 2\)"):
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, owner_ranks=[0, 2]
        )

    with pytest.raises(mod.SharedSegmentError, match="deprecated alias"):
        mod.create_shared_segment(
            "seg",
            blocks,
            world_size=2,
            rank_id=0,
            owner_ranks=[0],
            owner_rank=1,
        )

    with pytest.raises(mod.SharedSegmentError, match="int or a sequence"):
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, owner_ranks=True
        )

    with pytest.raises(mod.SharedSegmentError, match="int or a sequence"):
        mod.create_shared_segment(
            "seg", blocks, world_size=2, rank_id=0, owner_ranks="01"
        )
