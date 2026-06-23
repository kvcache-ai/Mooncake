from __future__ import annotations

import ctypes
import gc

import pytest

from mooncake.buffer_pool import BufferPool

pytestmark = pytest.mark.skipif(BufferPool is None, reason="native BufferPool is unavailable")


class _LocalLease:
    def __init__(self, store: _FakeStore, size: int) -> None:
        self._store = store
        self._buffer = ctypes.create_string_buffer(size)
        self.ptr = ctypes.addressof(self._buffer)
        self.size = size
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._store.release_local(self.ptr)


class _FakeStore:
    def __init__(self, local_capacity: int) -> None:
        self.local_capacity = local_capacity
        self.local_used = 0
        self.local_leases: dict[int, _LocalLease] = {}
        self.overflow_regions: dict[int, int] = {}
        self.objects: dict[str, bytes] = {}

    def local_buffer_pool_capacity(self) -> int:
        return self.local_capacity

    def local_buffer_pool_try_acquire(self, size: int):
        if self.local_used + size > self.local_capacity:
            return None
        lease = _LocalLease(self, size)
        self.local_used += size
        self.local_leases[lease.ptr] = lease
        return lease

    def release_local(self, ptr: int) -> None:
        lease = self.local_leases.pop(ptr)
        self.local_used -= lease.size

    def register_buffer(self, ptr: int, size: int) -> int:
        self.overflow_regions[int(ptr)] = int(size)
        return 0

    def unregister_buffer(self, ptr: int, size: int) -> int:
        ptr = int(ptr)
        if self.overflow_regions.get(ptr) != int(size):
            return -1
        del self.overflow_regions[ptr]
        return 0

    def put_from(self, key: str, ptr: int, size: int) -> int:
        self.objects[key] = ctypes.string_at(int(ptr), int(size))
        return 0

    def get_into(self, key: str, ptr: int, size: int) -> int:
        data = self.objects[key]
        if len(data) > size:
            return -1
        ctypes.memmove(int(ptr), data, len(data))
        return len(data)


def _write_payload(lease, payload: bytes) -> None:
    view = lease.buffer
    try:
        view[: len(payload)] = payload
    finally:
        del view
        gc.collect()


def _read_payload(lease, size: int) -> bytes:
    view = lease.buffer
    try:
        return bytes(view[:size])
    finally:
        del view
        gc.collect()


def test_buffer_pool_put_from_get_into_with_local_buffer() -> None:
    store = _FakeStore(local_capacity=256)
    pool = BufferPool(store, max_bytes=256, alignment=64)
    payload = b"buffer-pool-local-payload"

    writer = pool.acquire(64)
    _write_payload(writer, payload)
    assert store.put_from("local-key", writer.ptr, len(payload)) == 0
    writer.release()

    reader = pool.acquire(64)
    assert store.get_into("local-key", reader.ptr, reader.size) == len(payload)
    assert _read_payload(reader, len(payload)) == payload
    reader.release()

    pool.close()
    assert store.local_used == 0
    assert store.overflow_regions == {}


def test_buffer_pool_put_from_get_into_with_overflow_buffer() -> None:
    store = _FakeStore(local_capacity=32)
    pool = BufferPool(store, alignment=64)
    payload = b"payload larger than the local scratch capacity"

    writer = pool.acquire(len(payload))
    _write_payload(writer, payload)
    assert store.put_from("overflow-key", writer.ptr, len(payload)) == 0
    assert writer.ptr in store.overflow_regions
    writer.release()
    assert store.overflow_regions == {}

    reader = pool.acquire(len(payload))
    assert store.get_into("overflow-key", reader.ptr, reader.size) == len(payload)
    assert _read_payload(reader, len(payload)) == payload
    assert reader.ptr in store.overflow_regions
    reader.release()

    pool.close()
    assert store.local_used == 0
    assert store.overflow_regions == {}
