import ctypes
import threading
import time

import pytest

from mooncake.remote_tensor_batch import (
    BatchReadPlanner,
    RangeReadPlanner,
    RegisteredBufferPool,
    RegisteredBufferPoolConfig,
)


class FakeStore:
    def __init__(self):
        self.registered = set()
        self.register_count = 0
        self.unregister_count = 0
        self.objects = {}
        self.batch_get_into_count = 0
        self.get_into_ranges_count = 0
        self.failed_register_ptrs = set()
        self.block_register = False
        self.register_started = threading.Event()
        self.release_register = threading.Event()
        self.block_unregister = False
        self.unregister_started = threading.Event()
        self.release_unregister = threading.Event()

    def register_buffer(self, ptr, size):
        if self.block_register:
            self.register_started.set()
            assert self.release_register.wait(timeout=1.0)
        if ptr in self.failed_register_ptrs:
            return -1
        self.registered.add(ptr)
        self.register_count += 1
        return 0

    def unregister_buffer(self, ptr):
        if self.block_unregister:
            self.unregister_started.set()
            assert self.release_unregister.wait(timeout=1.0)
        assert ptr in self.registered
        self.registered.remove(ptr)
        self.unregister_count += 1
        return 0

    def batch_get_into(self, keys, ptrs, sizes):
        self.batch_get_into_count += 1
        results = []
        for key, ptr, size in zip(keys, ptrs, sizes):
            data = self.objects[key]
            ctypes.memmove(ptr, data, min(len(data), size))
            results.append(len(data) if len(data) <= size else -1)
        return results

    def get_into_ranges(self, buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes):
        self.get_into_ranges_count += 1
        results = []
        for ptr, keys, key_dst_offsets, key_src_offsets, key_sizes in zip(
            buffers,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        ):
            buffer_results = []
            for key, dst_offsets, src_offsets, sizes in zip(keys, key_dst_offsets, key_src_offsets, key_sizes):
                data = self.objects[key]
                key_results = []
                for dst_offset, src_offset, size in zip(dst_offsets, src_offsets, sizes):
                    chunk = data[src_offset : src_offset + size]
                    ctypes.memmove(ptr + dst_offset, chunk, min(len(chunk), size))
                    key_results.append(len(chunk) if len(chunk) == size else -1)
                buffer_results.append(key_results)
            results.append(buffer_results)
        return results


def test_batch_read_planner_executes_whole_key_reads():
    store = FakeStore()
    store.objects["a"] = b"abc"
    store.objects["b"] = b"defg"
    first = bytearray(3)
    second = bytearray(4)
    planner = BatchReadPlanner(store)
    planner.add("a", ctypes.addressof(ctypes.c_char.from_buffer(first)), len(first))
    planner.add("b", ctypes.addressof(ctypes.c_char.from_buffer(second)), len(second))

    planner.execute()

    assert bytes(first) == b"abc"
    assert bytes(second) == b"defg"
    assert store.batch_get_into_count == 1


def test_batch_read_planner_checks_result_sizes():
    store = FakeStore()
    store.objects["a"] = b"abc"
    buffer = bytearray(2)
    planner = BatchReadPlanner(store)
    planner.add("a", ctypes.addressof(ctypes.c_char.from_buffer(buffer)), len(buffer))

    with pytest.raises(RuntimeError, match="batch_get_into failed"):
        planner.execute()


def test_range_read_planner_executes_whole_key_reads_into_offsets():
    store = FakeStore()
    store.objects["a"] = b"abc"
    store.objects["b"] = b"defg"
    buffer = bytearray(7)
    planner = RangeReadPlanner(store, ctypes.addressof(ctypes.c_char.from_buffer(buffer)))
    planner.add_whole_key("a", 0, 3)
    planner.add_whole_key("b", 3, 4)

    planner.execute()

    assert bytes(buffer) == b"abcdefg"
    assert store.get_into_ranges_count == 1


def test_registered_buffer_pool_registers_external_buffer_best_effort():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=4096, min_size_class=1024, alignment=4096)
    buffer = bytearray(16)
    ptr = ctypes.addressof(ctypes.c_char.from_buffer(buffer))

    lease = pool.try_register_external(ptr, len(buffer))
    assert lease is not None
    assert ptr in store.registered
    lease.release()
    assert ptr not in store.registered

    store.failed_register_ptrs.add(ptr)
    assert pool.try_register_external(ptr, len(buffer)) is None
    pool.close()


def test_registered_buffer_pool_groups_transfer_sizes_by_scratch_policy():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=8192, min_size_class=1024, max_size_class=2048, alignment=1024)

    assert pool.iter_transfer_groups([512, 1024, 1024, 3000, 512]) == [[0, 1], [2], [3], [4]]
    pool.close()


def test_registered_buffer_pool_reuses_released_region():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=1024 * 1024, min_size_class=1024, alignment=4096)

    with pool.buffer(1000) as first:
        first_ptr = first.ptr
        assert first.size == 1000

    with pool.buffer(900) as second:
        assert second.ptr == first_ptr

    stats = pool.stats()
    assert stats.acquire_count == 2
    assert stats.allocate_count == 1
    assert stats.reuse_count == 1
    assert stats.release_count == 2
    assert store.register_count == 1

    pool.close()
    assert store.unregister_count == 1
    assert not store.registered


def test_registered_buffer_pool_enforces_capacity_without_blocking():
    store = FakeStore()
    pool = RegisteredBufferPool(
        store,
        max_bytes=4096,
        min_size_class=4096,
        alignment=4096,
        block_on_exhaustion=False,
    )

    lease = pool.acquire(1024)
    try:
        with pytest.raises(RuntimeError, match="exhausted"):
            pool.acquire(1024)
    finally:
        lease.release()
        pool.close()


def test_registered_buffer_pool_rejects_double_release():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=4096, min_size_class=1024, alignment=4096)
    lease = pool.acquire(512)
    lease.release()

    with pytest.raises(RuntimeError, match="released twice"):
        lease.release()

    pool.close()


def test_registered_buffer_pool_prewarm_registers_and_reuses():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=8192, min_size_class=1024, alignment=4096)
    pool.prewarm(1000, 2)

    assert store.register_count == 2
    with pool.buffer(1000) as first, pool.buffer(1000) as second:
        assert first.ptr != second.ptr

    stats = pool.stats()
    assert stats.allocate_count == 0
    assert stats.reuse_count == 2
    pool.close()
    assert store.unregister_count == 2


def test_registered_buffer_pool_handles_oversize_requests_without_transfer_branching():
    store = FakeStore()
    pool = RegisteredBufferPool(
        store,
        max_bytes=8192,
        min_size_class=1024,
        max_size_class=2048,
        alignment=1024,
    )

    with pool.buffer(3000) as lease:
        assert lease.size == 3000

    stats = pool.stats()
    assert stats.oversize_allocate_count == 1
    assert stats.free_regions == 0
    assert store.register_count == 1
    assert store.unregister_count == 1
    pool.close()


def test_registered_buffer_pool_config_prewarms_size_class():
    store = FakeStore()
    pool = RegisteredBufferPool.from_config(
        store,
        RegisteredBufferPoolConfig(
            max_bytes=8192,
            min_size_class=1024,
            alignment=4096,
            prewarm_size=1000,
            prewarm_count=2,
        ),
    )

    stats = pool.stats()
    assert stats.free_regions == 2
    assert stats.size_classes == {1024: 2}
    assert store.register_count == 2
    pool.close()
    assert store.unregister_count == 2


def test_registered_buffer_pool_requires_prewarm_size_with_count():
    store = FakeStore()
    with pytest.raises(ValueError, match="prewarm_size"):
        RegisteredBufferPool(store, max_bytes=8192, prewarm_count=1)


def test_registered_buffer_pool_ensure_prewarmed_only_adds_missing_regions():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=8192, min_size_class=1024, alignment=4096)
    pool.ensure_prewarmed(1000, 2)
    pool.ensure_prewarmed(1000, 3)

    stats = pool.stats()
    assert stats.free_regions == 3
    assert store.register_count == 3
    pool.close()
    assert store.unregister_count == 3


def test_registered_buffer_pool_release_is_not_blocked_by_slow_registration():
    store = FakeStore()
    pool = RegisteredBufferPool(
        store,
        max_bytes=8192,
        min_size_class=4096,
        alignment=4096,
        block_on_exhaustion=False,
    )
    first = pool.acquire(1024)
    store.block_register = True
    registration_error = []
    second_leases = []

    def acquire_second():
        try:
            second_leases.append(pool.acquire(1024))
        except BaseException as exc:
            registration_error.append(exc)

    thread = threading.Thread(target=acquire_second)
    thread.start()
    assert store.register_started.wait(timeout=1.0)

    release_start = time.monotonic()
    first.release()
    release_s = time.monotonic() - release_start

    store.release_register.set()
    thread.join(timeout=1.0)
    for lease in second_leases:
        lease.release()
    pool.close()

    assert release_s < 0.5
    assert not registration_error


def test_registered_buffer_pool_close_waits_for_successful_reservation():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=8192, min_size_class=4096, alignment=4096)
    store.block_register = True
    leases = []

    def acquire_buffer():
        leases.append(pool.acquire(1024))

    acquire_thread = threading.Thread(target=acquire_buffer)
    acquire_thread.start()
    assert store.register_started.wait(timeout=1.0)

    close_error = []

    def close_pool():
        try:
            pool.close()
        except BaseException as exc:
            close_error.append(exc)

    close_thread = threading.Thread(target=close_pool)
    close_thread.start()
    store.release_register.set()
    acquire_thread.join(timeout=1.0)
    close_thread.join(timeout=1.0)

    assert not close_thread.is_alive()
    assert close_error and "active leases" in str(close_error[0])
    for lease in leases:
        lease.release()
    pool.close()


def test_registered_buffer_pool_external_release_is_thread_safe():
    store = FakeStore()
    pool = RegisteredBufferPool(store, max_bytes=4096, min_size_class=1024, alignment=4096)
    buffer = bytearray(16)
    ptr = ctypes.addressof(ctypes.c_char.from_buffer(buffer))
    lease = pool.try_register_external(ptr, len(buffer))
    assert lease is not None
    store.block_unregister = True
    errors = []

    def release_lease():
        try:
            lease.release()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=release_lease)
    second = threading.Thread(target=release_lease)
    first.start()
    assert store.unregister_started.wait(timeout=1.0)
    second.start()
    store.release_unregister.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)
    pool.close()

    assert store.unregister_count == 1
    assert len(errors) == 1
    assert "released twice" in str(errors[0])
