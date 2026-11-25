import ctypes

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
