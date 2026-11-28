"""Opt-in native Dummy-client BufferPool integration tests."""

import ctypes
import gc
import os
import uuid

import pytest

from mooncake.buffer_pool import BufferPool
from mooncake.store import MooncakeDistributedStore


@pytest.fixture
def store():
    endpoint = os.environ.get("MOONCAKE_TEST_DUMMY_ENDPOINT")
    if not endpoint:
        pytest.skip("set MOONCAKE_TEST_DUMMY_ENDPOINT for the native Dummy test")
    client = MooncakeDistributedStore()
    assert client.setup_dummy(0, 8 << 20, endpoint) == 0
    yield client
    client.close()


def test_shm_pool_does_not_consume_transfer_staging(store) -> None:
    pool = BufferPool(store, max_bytes=32 << 20, block_on_exhaustion=False)
    lease = pool.acquire(24 << 20)
    view = lease.buffer
    view[:16] = b"0123456789abcdef"

    source = ctypes.create_string_buffer(8 << 20)
    source.value = b"full-sync"
    ptr = ctypes.addressof(source)
    assert store.register_buffer(ptr, 8 << 20) == 0
    key = "cpu-shm-pool-full-" + uuid.uuid4().hex
    try:
        assert store.batch_put_from([key], [ptr], [8 << 20]) == [0]
        assert store.get(key)[:9] == b"full-sync"
    finally:
        store.remove(key, True)
        assert store.unregister_buffer(ptr) == 0
        del view
        lease.release()
        pool.close()


def test_torch_numpy_views_retain_native_lease(store) -> None:
    torch = pytest.importorskip("torch")
    pool = BufferPool(store, max_regions=1, block_on_exhaustion=False)
    lease = pool.acquire(8192)
    tensor = torch.frombuffer(lease.buffer, dtype=torch.int16)
    assert tensor.data_ptr() == lease.ptr
    array = tensor.numpy()
    with pytest.raises(RuntimeError, match="exported views"):
        lease.release()
    del tensor, lease
    gc.collect()
    array[0] = -32768
    with pytest.raises(RuntimeError, match="exhausted"):
        pool.acquire(1)
    del array
    gc.collect()
    replacement = pool.acquire(1)
    replacement.release()
    pool.close()


def test_shm_subrange_put_snapshots_before_reuse(store) -> None:
    pool = BufferPool(store)
    lease = pool.acquire(8192)
    view = lease.buffer
    view[17:25] = b"raw-bits"
    key = "cpu-shm-pool-view-" + uuid.uuid4().hex
    try:
        assert store.batch_put_from([key], [lease.ptr + 17], [8]) == [0]
        view[17:25] = b"new-bits"
        assert store.get(key) == b"raw-bits"
    finally:
        store.remove(key, True)
        del view
        lease.release()
        pool.close()


def test_shm_pool_reuses_exact_capacity(store) -> None:
    pool = BufferPool(store, max_bytes=8 << 20, block_on_exhaustion=False)
    lease = pool.acquire(8 << 20)
    with pytest.raises(RuntimeError, match="exhausted"):
        pool.acquire(1)
    lease.release()
    replacement = pool.acquire(8 << 20)
    replacement.release()
    pool.close()
