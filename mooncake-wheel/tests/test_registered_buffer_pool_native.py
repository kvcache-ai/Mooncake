from __future__ import annotations

import gc
import threading
import time

import pytest

from mooncake.store import MooncakeDistributedStore, RegisteredBufferPool


def create_store() -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    rc = store.setup(
        "localhost",
        "P2PHANDSHAKE",
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        "tcp",
        "",
        "127.0.0.1:50051",
    )
    assert rc == 0
    return store


def test_registered_buffer_pool_reuses_released_buffer() -> None:
    store = create_store()
    pool = RegisteredBufferPool(store, 1024 * 1024, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1234)
    ptr = lease.ptr
    lease.buffer[:4] = b"abcd"
    assert bytes(lease.buffer[:4]) == b"abcd"
    lease.release()

    reused = pool.acquire(2048)
    assert reused.ptr == ptr
    reused.release()
    pool.close()


def test_registered_buffer_pool_prewarm_and_close() -> None:
    store = create_store()
    pool = RegisteredBufferPool(
        store,
        1024 * 1024,
        min_size_class=4096,
        alignment=4096,
        prewarm_size=1024,
        prewarm_count=2,
    )
    lease1 = pool.acquire(1024)
    lease2 = pool.acquire(1024)
    lease1.release()
    lease2.release()
    pool.close()


@pytest.mark.parametrize("size", [0, 1, 128 * 1024 + 1])
def test_registered_buffer_pool_supports_arbitrary_sizes(size: int) -> None:
    store = create_store()
    pool = RegisteredBufferPool(
        store, 1024 * 1024, max_size_class=128 * 1024, alignment=4096
    )

    lease = pool.acquire(size)
    assert lease.size == size
    assert len(lease.buffer) == size
    lease.release()
    pool.close()


def test_registered_buffer_pool_nonblocking_exhaustion() -> None:
    store = create_store()
    pool = RegisteredBufferPool(
        store,
        4096,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        block_on_exhaustion=False,
    )

    lease = pool.acquire(1)
    with pytest.raises(RuntimeError, match="exhausted"):
        pool.acquire(1)
    lease.release()
    pool.close()


def test_registered_buffer_pool_timeout_allows_late_release() -> None:
    store = create_store()
    pool = RegisteredBufferPool(
        store,
        4096,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
    )
    lease = pool.acquire(1)
    acquired = []

    def acquire_after_release() -> None:
        acquired.append(pool.acquire(1, timeout=1.0))

    thread = threading.Thread(target=acquire_after_release)
    thread.start()
    time.sleep(0.1)
    lease.release()
    thread.join(timeout=5)

    assert not thread.is_alive()
    acquired[0].release()
    pool.close()


def test_registered_buffer_pool_blocking_acquire_releases_gil() -> None:
    store = create_store()
    pool = RegisteredBufferPool(
        store,
        4096,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
    )
    lease = pool.acquire(1)
    acquired = []

    def acquire_after_release() -> None:
        acquired.append(pool.acquire(1))

    thread = threading.Thread(target=acquire_after_release)
    thread.start()
    time.sleep(0.1)
    lease.release()
    thread.join(timeout=5)

    assert not thread.is_alive()
    acquired[0].release()
    pool.close()


def test_registered_buffer_pool_memoryview_keeps_lease_alive() -> None:
    store = create_store()
    pool = RegisteredBufferPool(store, 4096, min_size_class=4096, alignment=4096)

    lease = pool.acquire(4)
    view = lease.buffer
    view[:] = b"test"

    assert bytes(view) == b"test"
    with pytest.raises(RuntimeError, match="exported views"):
        lease.release()
    del view
    gc.collect()
    lease.release()
    pool.close()


def test_registered_buffer_pool_releases_from_destructor() -> None:
    store = create_store()
    pool = RegisteredBufferPool(store, 4096, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1)
    del lease
    gc.collect()

    pool.close()


def test_registered_buffer_pool_rejects_close_with_active_lease() -> None:
    store = create_store()
    pool = RegisteredBufferPool(store, 1024 * 1024, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1)
    with pytest.raises(RuntimeError, match="active leases"):
        pool.close()
    lease.release()
    pool.close()


def test_registered_buffer_pool_rejects_huge_size_overflow() -> None:
    store = create_store()
    pool = RegisteredBufferPool(store, 1024 * 1024, min_size_class=4096, alignment=4096)

    with pytest.raises(RuntimeError, match="overflow|capacity"):
        pool.acquire((1 << 64) - 1)
    pool.close()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
