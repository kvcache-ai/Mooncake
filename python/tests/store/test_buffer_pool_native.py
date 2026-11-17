from __future__ import annotations

import ctypes
import gc
import weakref
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from mooncake.buffer_pool import BufferPool
from mooncake.store import MooncakeDistributedStore


def create_store() -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    rc = store.setup(
        "localhost",
        "P2PHANDSHAKE",
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        "tcp",
        "",
        os.getenv("MOONCAKE_ALIGNMENT_MASTER", "127.0.0.1:50051"),
    )
    assert rc == 0
    return store


def test_buffer_pool_acquires_released_buffer() -> None:
    store = create_store()
    pool = BufferPool(store, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1234)
    lease.buffer[:4] = b"abcd"
    assert bytes(lease.buffer[:4]) == b"abcd"
    lease.release()

    next_lease = pool.acquire(2048)
    next_lease.release()
    pool.close()


def test_buffer_pool_uses_store_local_buffer_by_default() -> None:
    store = create_store()
    pool = BufferPool(store)

    lease = pool.acquire(1024)
    assert lease.size == 1024
    lease.release()
    pool.close()


def test_buffer_pool_overflows_when_local_buffer_is_full() -> None:
    store = create_store()
    pool = BufferPool(store)

    local_lease = pool.acquire(4 * 1024 * 1024)
    overflow_lease = pool.acquire(1024, block=False)
    assert overflow_lease.size == 1024

    overflow_lease.release()
    local_lease.release()
    pool.close()


def test_buffer_pool_prewarm_and_close() -> None:
    store = create_store()
    pool = BufferPool(
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


@pytest.mark.parametrize("alignment", [64, 4096, 65536])
def test_buffer_pool_uses_local_buffer_alignment(alignment: int) -> None:
    store = create_store()
    pool = BufferPool(store, min_size_class=4096, alignment=alignment)

    leases = [pool.acquire(size) for size in (1, 67, 1024)]
    for lease in leases:
        assert lease.ptr % alignment == 0
        lease.release()
    lease = pool.acquire(73)
    assert lease.ptr % alignment == 0
    lease.release()
    pool.close()


def test_buffer_pool_rejects_invalid_alignment() -> None:
    store = create_store()
    with pytest.raises(RuntimeError, match="alignment"):
        BufferPool(store, 1024 * 1024, alignment=12345)


@pytest.mark.parametrize("size", [0, 1, 128 * 1024 + 1])
def test_buffer_pool_supports_arbitrary_sizes(size: int) -> None:
    store = create_store()
    pool = BufferPool(store, 1024 * 1024, max_size_class=128 * 1024, alignment=4096)

    lease = pool.acquire(size)
    assert lease.size == size
    assert len(lease.buffer) == size
    lease.release()
    pool.close()


def test_buffer_pool_nonblocking_exhaustion() -> None:
    store = create_store()
    pool = BufferPool(
        store,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        max_regions=1,
        block_on_exhaustion=False,
    )

    lease = pool.acquire(1)
    with pytest.raises(RuntimeError, match="exhausted"):
        pool.acquire(1)
    lease.release()
    pool.close()


def test_buffer_pool_timeout_allows_late_release() -> None:
    store = create_store()
    pool = BufferPool(
        store,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        max_regions=1,
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


def test_buffer_pool_blocking_acquire_releases_gil() -> None:
    store = create_store()
    pool = BufferPool(
        store,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        max_regions=1,
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


def test_buffer_pool_memoryview_keeps_lease_alive() -> None:
    store = create_store()
    pool = BufferPool(store, 4096, min_size_class=4096, alignment=4096)

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


def test_buffer_pool_releases_from_destructor() -> None:
    store = create_store()
    pool = BufferPool(store, 4096, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1)
    del lease
    gc.collect()

    pool.close()


def test_buffer_pool_rejects_close_with_active_lease() -> None:
    store = create_store()
    pool = BufferPool(store, 1024 * 1024, min_size_class=4096, alignment=4096)

    lease = pool.acquire(1)
    with pytest.raises(RuntimeError, match="active leases"):
        pool.close()
    lease.release()
    pool.close()


def test_buffer_pool_rejects_huge_size_overflow() -> None:
    store = create_store()
    pool = BufferPool(store, 1024 * 1024, min_size_class=4096, alignment=4096)

    with pytest.raises(RuntimeError, match="overflow|capacity"):
        pool.acquire((1 << 64) - 1)
    pool.close()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))


def test_default_pool_does_not_add_alignment_padding_or_overflow_registrations():
    store = create_store()
    observed = _ObservedStore(store)
    pool = BufferPool(observed)
    leases = []
    try:
        for _ in range(4):
            leases.append(pool.acquire(1024 * 1024))
        assert observed.registrations == 0
        leases.append(pool.acquire(1024))
        assert observed.registrations == 1
    finally:
        for lease in leases:
            lease.release()
        pool.close()


class _ExternalOwner:
    def __init__(self, size=4096):
        self.buffer = ctypes.create_string_buffer(size)

    @property
    def ptr(self):
        return ctypes.addressof(self.buffer)


@pytest.fixture
def external_pool():
    store = create_store()
    pool = BufferPool(store)
    yield store, pool
    pool.close()
    assert store.close() == 0


@pytest.fixture
def external_region(external_pool):
    store, pool = external_pool
    owner = _ExternalOwner()
    with pool.register_external_region(owner.ptr, 4096, owner) as lease:
        yield store, pool, owner, lease


def test_external_pool_region_is_opaque_and_keeps_original_objects(external_region):
    from mooncake.store import BufferLease

    store, pool, owner, lease = external_region
    assert isinstance(lease, BufferLease)
    assert lease.owner is owner
    assert lease.store is store
    assert lease.ptr == owner.ptr
    assert lease.size == 4096
    with pytest.raises(TypeError, match="opaque"):
        _ = lease.buffer
    with pytest.raises(TypeError):
        memoryview(lease)
    for attr, value in (("owner", None), ("store", None), ("ptr", 0), ("size", 0)):
        with pytest.raises(AttributeError):
            setattr(lease, attr, value)
    with lease.borrow(8, 16) as view:
        assert view.ptr == owner.ptr + 8
        assert view.size == 16
        assert view.owner is owner
        assert view.store is store
        with pytest.raises(TypeError):
            memoryview(view)
        with pytest.raises(RuntimeError, match="exported views"):
            lease.release()
    with pytest.raises(RuntimeError, match="closed"):
        _ = view.ptr


@pytest.mark.parametrize(
    "offset,size", [(4097, None), (4096, 1), (0, 4097), (-1, 1), (0, -1)]
)
def test_external_pool_borrow_rejects_invalid_ranges(external_region, offset, size):
    store, pool, owner, lease = external_region
    with pytest.raises(
        (ValueError, TypeError, OverflowError, RuntimeError),
        match="borrow .*exceeds lease|Unable to cast|incompatible function arguments",
    ):
        lease.borrow(offset, size)
    with lease.borrow(4096, 0) as empty:
        assert empty.size == 0
        assert empty.ptr == owner.ptr + 4096


def test_external_pool_borrow_pins_owner_and_parent_until_release():
    store = create_store()
    pool = BufferPool(store)
    owner = _ExternalOwner()
    weak_owner = weakref.ref(owner)
    lease = pool.register_external_region(owner.ptr, 4096, owner)
    borrowed = lease.borrow()
    del owner, lease, pool
    gc.collect()
    assert weak_owner() is not None
    assert borrowed.owner is weak_owner()
    assert borrowed.store is store
    borrowed.release()
    gc.collect()
    assert weak_owner() is None
    assert store.close() == 0


def test_external_pool_does_not_consume_cpu_region_budget_or_retry_live_leases():
    store = create_store()
    pool = BufferPool(store, max_regions=1, block_on_exhaustion=False)
    owner = _ExternalOwner()
    external = pool.register_external_region(owner.ptr, 4096, owner)
    cpu = pool.acquire(1024)
    try:
        with pytest.raises(RuntimeError, match="exhausted"):
            pool.acquire(1024)
        pool.retry_pending_external_registrations()
        assert external.ptr == owner.ptr
        assert cpu.size == 1024
    finally:
        external.release()
        cpu.release()
        pool.close()
        assert store.close() == 0


def test_external_pool_storage_anchor_survives_tensor_rebinding(external_pool):
    torch = pytest.importorskip("torch")
    _, pool = external_pool
    source = torch.arange(1024, dtype=torch.int32)
    anchor = source.untyped_storage()
    pointer = source.data_ptr()
    weak_anchor = weakref.ref(anchor)
    lease = pool.register_external_region(pointer, anchor.nbytes(), anchor)
    try:
        source.set_(torch.empty(1, dtype=torch.int32))
        del anchor
        gc.collect()
        assert weak_anchor() is lease.owner
        assert lease.ptr == pointer
        assert ctypes.c_int32.from_address(lease.ptr + 128 * 4).value == 128
    finally:
        lease.release()
        gc.collect()
        assert weak_anchor() is None


@pytest.mark.parametrize(
    "ptr,size,owner",
    [(0, 4096, object()), (1, 0, object()), ((1 << 64) - 1, 2, object()), (1, 1, None)],
)
def test_external_pool_rejects_invalid_registration_without_poisoning_pool(
    external_pool, ptr, size, owner
):
    _, pool = external_pool
    with pytest.raises((ValueError, TypeError, OverflowError)):
        pool.register_external_region(ptr, size, owner)
    pool.retry_pending_external_registrations()
    with pool.acquire(64) as cpu:
        assert cpu.size == 64


def test_external_pool_reregisters_same_address_after_release(external_pool):
    _, pool = external_pool
    owner = _ExternalOwner()
    for _ in range(3):
        with pool.register_external_region(owner.ptr, 4096, owner) as lease:
            assert lease.owner is owner
            with lease.borrow() as view:
                assert view.ptr == owner.ptr
        assert lease.owner is None


def test_external_pool_uncertain_registration_pins_owner_without_blind_unregister():
    import mooncake

    # Isolate the deliberate process-lifetime owner pin. The child uses the
    # same package/native search path; no installed wheel may replace it.
    bootstrap = (
        "import sys; sys.path.insert(0, "
        + repr(os.path.dirname(mooncake.__path__[0]))
        + "); import mooncake; mooncake.__path__[:] = "
        + repr(list(mooncake.__path__))
        + "; sys.path.insert(0, "
        + repr(os.path.dirname(__file__))
        + ");\n"
    )
    code = textwrap.dedent(
        """
        import ctypes, gc, os, weakref
        import pytest
        from test_buffer_pool_native import BufferPool, create_store, _ObservedStore
        store = create_store()
        arena = BufferPool(store)
        local = arena.acquire(4096)
        class Owner:
            pass
        owner = Owner()
        owner.local = local
        weak = weakref.ref(owner)
        wrapper = _ObservedStore(store)
        pool = BufferPool(wrapper)
        healthy_owner = ctypes.create_string_buffer(4096)
        healthy = pool.register_external_region(ctypes.addressof(healthy_owner), 4096, healthy_owner)
        # This range belongs to the existing CPU arena, not this pool.
        with pytest.raises(RuntimeError):
            pool.register_external_region(local.ptr, 4096, owner)
        del owner
        gc.collect()
        assert weak() is not None
        for cleanup in (pool.retry_pending_external_registrations, pool.close):
            with pytest.raises(RuntimeError, match="uncertain"):
                cleanup()
        assert wrapper.unregistrations == 0
        with healthy.borrow() as view:
            assert view.ptr == ctypes.addressof(healthy_owner)
        local.buffer[:4] = b"live"
        assert bytes(local.buffer[:4]) == b"live"
        healthy.release()
        assert wrapper.unregistrations == 1
        del pool
        gc.collect()
        assert weak() is not None
        print("EXPECTED_UNCERTAIN_OWNER_RETAINED")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", bootstrap + code],
        env=dict(os.environ, CUDA_VISIBLE_DEVICES=""),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "EXPECTED_UNCERTAIN_OWNER_RETAINED" in result.stdout


class _ObservedStore:
    def __init__(self, store):
        self.store = store
        self.registrations = 0
        self.unregistrations = 0
        self.before_register = lambda: None
        self.before_unregister = lambda: None

    def _get_pyclient_capsule(self):
        return self.store._get_pyclient_capsule()

    def register_buffer(self, ptr, size):
        self.registrations += 1
        self.before_register()
        return self.store.register_buffer(ptr, size)

    def unregister_buffer(self, ptr):
        self.unregistrations += 1
        self.before_unregister()
        return self.store.unregister_buffer(ptr)


@pytest.fixture
def observed_pool():
    store = create_store()
    observed = _ObservedStore(store)
    pool = BufferPool(observed)
    yield store, observed, pool
    observed.before_unregister = lambda: None
    pool.retry_pending_external_registrations()
    pool.close()
    assert store.close() == 0


def test_external_pool_reuses_one_registration_for_distinct_payloads(observed_pool):
    store, observed, pool = observed_pool
    owner = _ExternalOwner()
    lease = pool.register_external_region(owner.ptr, 4096, owner)
    try:
        for value in (b"first", b"second", b"third"):
            with lease.borrow(128, len(value)) as view:
                ctypes.memmove(view.ptr, value, len(value))
                key = f"pool-reuse-{owner.ptr}"
                assert store.put_from(key, view.ptr, view.size) == 0
                assert store.get(key) == value
                assert store.remove(key, force=True) == 0
        assert observed.registrations == 1
        assert observed.unregistrations == 0
        with pytest.raises(RuntimeError, match="active leases"):
            pool.close()
        with pytest.raises(ValueError, match="overlaps"):
            pool.register_external_region(owner.ptr + 1, 128, owner)
    finally:
        lease.release()
    assert observed.unregistrations == 1


@pytest.mark.parametrize("orphan", [False, True])
def test_external_pool_unregister_failure_retains_owner_and_can_retry(
    observed_pool, orphan
):
    _, observed, pool = observed_pool
    owner = _ExternalOwner()
    weak_owner = weakref.ref(owner)
    lease = pool.register_external_region(owner.ptr, 4096, owner)
    del owner

    def fail():
        raise RuntimeError("injected unregister failure")

    observed.before_unregister = fail
    with pytest.raises(RuntimeError, match="injected"):
        lease.release()
    assert weak_owner() is lease.owner
    with pytest.raises(RuntimeError, match="cleanup is pending"):
        lease.borrow()
    if orphan:
        del lease
        gc.collect()
        assert weak_owner() is not None
    observed.before_unregister = lambda: None
    if orphan:
        pool.retry_pending_external_registrations()
    else:
        lease.release()
    gc.collect()
    assert weak_owner() is None


def test_external_pool_release_blocks_borrows_during_unregister(observed_pool):
    _, observed, pool = observed_pool
    entered, finish = threading.Event(), threading.Event()
    errors = []
    owner = _ExternalOwner()
    lease = pool.register_external_region(owner.ptr, 4096, owner)

    def wait():
        entered.set()
        assert finish.wait(5)

    def release():
        try:
            lease.release()
        except BaseException as error:
            errors.append(error)

    observed.before_unregister = wait
    thread = threading.Thread(target=release)
    thread.start()
    try:
        assert entered.wait(5)
        for operation in (lease.borrow, lease.release, lambda: lease.ptr):
            with pytest.raises(RuntimeError, match="progress"):
                operation()
        with pytest.raises(RuntimeError, match="active leases"):
            pool.close()
    finally:
        finish.set()
        thread.join(10)
    assert not thread.is_alive()
    assert errors == []
    assert observed.unregistrations == 1


def test_external_pool_close_waits_for_registration_and_rolls_it_back(observed_pool):
    _, observed, pool = observed_pool
    entered, finish = threading.Event(), threading.Event()
    outcome = []
    owner = _ExternalOwner()

    def wait():
        entered.set()
        assert finish.wait(5)

    def register():
        try:
            outcome.append(pool.register_external_region(owner.ptr, 4096, owner))
        except RuntimeError as error:
            outcome.append(str(error))

    observed.before_register = wait
    thread = threading.Thread(target=register)
    thread.start()
    assert entered.wait(5)
    closer = threading.Thread(target=pool.close)
    closer.start()
    try:
        # Close must remain blocked until the submitted registration completes.
        closer.join(0.05)
        assert closer.is_alive()
    finally:
        finish.set()
        thread.join(10)
        closer.join(10)
    assert not thread.is_alive() and not closer.is_alive()
    assert len(outcome) == 1 and isinstance(outcome[0], str)
    assert observed.registrations == observed.unregistrations == 1
