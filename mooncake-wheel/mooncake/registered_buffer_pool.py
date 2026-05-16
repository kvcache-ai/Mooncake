from __future__ import annotations

import ctypes
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Optional, Sequence


try:
    from mooncake.store import RegisteredBufferPool as CppRegisteredBufferPool
except ImportError:  # pragma: no cover - extension may be unavailable in pure Python tests.
    CppRegisteredBufferPool = None


@dataclass(frozen=True)
class RegisteredBufferPoolConfig:
    """Configuration for a reusable Mooncake registered scratch buffer pool."""

    max_bytes: int
    min_size_class: int = 64 * 1024
    max_size_class: Optional[int] = None
    alignment: int = 8 * 1024 * 1024
    block_on_exhaustion: bool = True
    default_timeout: Optional[float] = None
    max_regions: Optional[int] = None
    prewarm_size: Optional[int] = None
    prewarm_count: int = 0


@dataclass(frozen=True)
class RegisteredBufferPoolStats:
    """Point-in-time counters for a registered scratch buffer pool."""

    size_classes: dict[int, int]
    free_regions: int
    in_use_regions: int
    total_regions: int
    total_bytes: int
    free_bytes: int
    in_use_bytes: int
    acquire_count: int
    reuse_count: int
    allocate_count: int
    oversize_allocate_count: int
    wait_count: int
    release_count: int
    register_s: float
    unregister_s: float


@dataclass
class RegisteredBufferLease:
    """Context-managed lease for a registered scratch buffer."""

    pool: "RegisteredBufferPool"
    region: "WritableBufferRegion"
    requested_size: int
    closed: bool = False

    @property
    def buffer(self) -> Any:
        return self.region.buffer

    @property
    def ptr(self) -> int:
        return self.region.ptr

    @property
    def size(self) -> int:
        return self.requested_size

    def view_region(self) -> "WritableBufferRegion":
        return self.region.view_region(self.requested_size)

    def release(self) -> None:
        if self.closed:
            raise RuntimeError("registered buffer lease released twice")
        self.pool.release(self)
        self.closed = True

    def __enter__(self) -> "RegisteredBufferLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


@dataclass
class ExternalRegisteredBufferLease:
    """Context-managed registration for caller-owned destination memory."""

    pool: "RegisteredBufferPool"
    ptr: int
    size: int
    closed: bool = False

    def release(self) -> None:
        self.pool.unregister_external(self)

    def __enter__(self) -> "ExternalRegisteredBufferLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


class RegisteredBufferPool:
    """Reusable pool of Mooncake-registered scratch buffers.

    Uses the C++ store pool when available; otherwise falls back to the pure Python implementation for tests and
    unrecompiled local extensions.
    """

    def __init__(
        self,
        store: Any,
        max_bytes: int,
        *,
        min_size_class: int = 64 * 1024,
        max_size_class: Optional[int] = None,
        alignment: int = 8 * 1024 * 1024,
        block_on_exhaustion: bool = True,
        default_timeout: Optional[float] = None,
        max_regions: Optional[int] = None,
        prewarm_size: Optional[int] = None,
        prewarm_count: int = 0,
    ) -> None:
        self._cpp_pool = None
        if CppRegisteredBufferPool is not None and store.__class__.__module__ == "mooncake.store":
            self._cpp_pool = CppRegisteredBufferPool(
                store,
                max_bytes,
                min_size_class,
                max_size_class,
                alignment,
                block_on_exhaustion,
                default_timeout,
                max_regions,
            )
            if prewarm_count:
                if prewarm_size is None:
                    raise ValueError("prewarm_size is required when prewarm_count is positive")
                self._cpp_pool.prewarm(prewarm_size, prewarm_count)
            return
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if min_size_class <= 0 or alignment <= 0:
            raise ValueError("min_size_class and alignment must be positive")
        self.store = store
        self.max_bytes = int(max_bytes)
        self.min_size_class = int(min_size_class)
        self.max_size_class = min(int(max_size_class or max_bytes), self.max_bytes)
        self.alignment = int(alignment)
        self.block_on_exhaustion = block_on_exhaustion
        self.default_timeout = default_timeout
        self.max_regions = max_regions
        self._free: dict[int, Deque[WritableBufferRegion]] = defaultdict(deque)
        self._in_use: set[int] = set()
        self._external_in_use: set[int] = set()
        self._regions: dict[int, WritableBufferRegion] = {}
        self._region_sizes: dict[int, int] = {}
        self._closed = False
        self._closing = False
        self._total_bytes = 0
        self._reserved_bytes = 0
        self._acquire_count = 0
        self._reuse_count = 0
        self._allocate_count = 0
        self._oversize_allocate_count = 0
        self._wait_count = 0
        self._release_count = 0
        self._register_s = 0.0
        self._unregister_s = 0.0
        self._condition = threading.Condition(threading.Lock())
        if prewarm_count:
            if prewarm_size is None:
                raise ValueError("prewarm_size is required when prewarm_count is positive")
            self.prewarm(prewarm_size, prewarm_count)

    @classmethod
    def from_config(cls, store: Any, config: RegisteredBufferPoolConfig) -> "RegisteredBufferPool":
        """Create and optionally prewarm a pool from configuration."""
        return cls(
            store,
            max_bytes=config.max_bytes,
            min_size_class=config.min_size_class,
            max_size_class=config.max_size_class,
            alignment=config.alignment,
            block_on_exhaustion=config.block_on_exhaustion,
            default_timeout=config.default_timeout,
            max_regions=config.max_regions,
            prewarm_size=config.prewarm_size,
            prewarm_count=config.prewarm_count,
        )

    def acquire(
        self,
        size: int,
        *,
        block: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> RegisteredBufferLease:
        """Acquire a registered buffer lease of at least ``size`` bytes."""
        if self._cpp_pool is not None:
            return self._cpp_pool.acquire(size, block, timeout)
        if size < 0:
            raise ValueError("size must be non-negative")
        should_block = self.block_on_exhaustion if block is None else block
        timeout_s = self.default_timeout if timeout is None else timeout
        end_time = None if timeout_s is None else time.monotonic() + timeout_s
        self._raise_if_impossible(size)
        with self._condition:
            while True:
                self._raise_if_closed()
                lease = self._try_acquire_locked(size)
                if lease is not None:
                    return lease
                if not should_block:
                    raise RuntimeError("registered buffer pool is exhausted")
                remaining = None if end_time is None else end_time - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("timed out waiting for registered buffer")
                self._wait_count += 1
                self._condition.wait(remaining)

    def try_acquire(self, size: int) -> RegisteredBufferLease:
        """Acquire a lease without waiting for capacity."""
        if self._cpp_pool is not None:
            return self._cpp_pool.try_acquire(size)
        return self.acquire(size, block=False)

    def buffer(self, size: int, *, timeout: Optional[float] = None) -> RegisteredBufferLease:
        """Return a context-managed registered buffer lease."""
        if self._cpp_pool is not None:
            return self._cpp_pool.buffer(size, None, timeout)
        return self.acquire(size, timeout=timeout)

    def try_register_external(self, ptr: int, size: int) -> Optional[ExternalRegisteredBufferLease]:
        """Best-effort registration for caller-owned destination memory."""
        if self._cpp_pool is not None:
            return self._cpp_pool.try_register_external(ptr, size)
        if size < 0:
            raise ValueError("size must be non-negative")
        with self._condition:
            self._raise_if_closed()
            if size == 0:
                return ExternalRegisteredBufferLease(self, ptr, size)
            if ptr in self._external_in_use:
                raise RuntimeError("external buffer is already registered through this pool")
        register_start = time.perf_counter()
        ret = self.store.register_buffer(ptr, size)
        register_s = time.perf_counter() - register_start
        with self._condition:
            self._register_s += register_s
            if ret != 0:
                return None
            try:
                self._raise_if_closed()
            except RuntimeError:
                pass
            else:
                self._external_in_use.add(ptr)
                self._condition.notify_all()
                return ExternalRegisteredBufferLease(self, ptr, size)
        unregister_start = time.perf_counter()
        unregister_ret = self.store.unregister_buffer(ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
        if unregister_ret != 0:
            raise RuntimeError(f"unregister_buffer failed with retcode {unregister_ret}")
        raise RuntimeError("registered buffer pool is closed")

    def unregister_external(self, lease: ExternalRegisteredBufferLease) -> None:
        """Unregister caller-owned destination memory previously registered by the pool."""
        with self._condition:
            if lease.closed:
                raise RuntimeError("external registered buffer lease released twice")
            lease.closed = True
            if lease.size == 0:
                return
            if lease.ptr not in self._external_in_use:
                lease.closed = False
                raise RuntimeError("external registered buffer lease does not belong to this pool or is not active")
            self._external_in_use.remove(lease.ptr)
        unregister_start = time.perf_counter()
        ret = self.store.unregister_buffer(lease.ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
            if ret != 0:
                self._external_in_use.add(lease.ptr)
                lease.closed = False
                self._condition.notify_all()
                raise RuntimeError(f"unregister_buffer failed with retcode {ret}")
            self._condition.notify_all()

    def iter_transfer_groups(self, sizes: Sequence[int]) -> list[list[int]]:
        """Group transfer item indexes by the pool's scratch-buffer sizing policy."""
        if self._cpp_pool is not None:
            return self._cpp_pool.iter_transfer_groups(sizes)
        groups: list[list[int]] = []
        current_group: list[int] = []
        current_bytes = 0
        for index, size in enumerate(sizes):
            item_size = int(size)
            if item_size < 0:
                raise ValueError("transfer sizes must be non-negative")
            if item_size == 0:
                continue
            if current_group and current_bytes + item_size > self.max_size_class:
                groups.append(current_group)
                current_group = []
                current_bytes = 0
            current_group.append(index)
            current_bytes += item_size
        if current_group:
            groups.append(current_group)
        return groups

    def release(self, lease: RegisteredBufferLease) -> None:
        """Return a lease to the pool."""
        region = lease.region
        with self._condition:
            if region.ptr not in self._in_use:
                raise RuntimeError("registered buffer lease does not belong to this pool or is not active")
            self._in_use.remove(region.ptr)
            region_size = self._region_sizes[region.ptr]
            should_unregister = self._closed or region_size > self.max_size_class
        if should_unregister:
            try:
                self._unregister_region(region)
            except RuntimeError:
                with self._condition:
                    self._in_use.add(region.ptr)
                    self._condition.notify_all()
                raise
            with self._condition:
                self._release_count += 1
                self._condition.notify_all()
            return
        with self._condition:
            self._free[region_size].append(region)
            self._release_count += 1
            self._condition.notify_all()

    def prewarm(self, size: int, count: int) -> None:
        """Register ``count`` reusable buffers for a size class ahead of the transfer path."""
        if self._cpp_pool is not None:
            self._cpp_pool.prewarm(size, count)
            return
        if count < 0:
            raise ValueError("count must be non-negative")
        size_class = self._size_class(size)
        for _ in range(count):
            self._append_prewarmed_region(size_class)

    def ensure_prewarmed(self, size: int, count: int) -> None:
        """Ensure at least ``count`` free reusable buffers exist for the request size class."""
        if self._cpp_pool is not None:
            self._cpp_pool.ensure_prewarmed(size, count)
            return
        if count < 0:
            raise ValueError("count must be non-negative")
        size_class = self._size_class(size)
        while True:
            with self._condition:
                missing_count = max(0, count - len(self._free[size_class]))
            if missing_count == 0:
                return
            self._append_prewarmed_region(size_class)

    def stats(self) -> RegisteredBufferPoolStats:
        """Return current pool counters."""
        if self._cpp_pool is not None:
            return self._cpp_pool.stats()
        with self._condition:
            free_by_class = {size: len(regions) for size, regions in self._free.items() if regions}
            free_bytes = sum(size * count for size, count in free_by_class.items())
            in_use_bytes = sum(self._region_sizes[ptr] for ptr in self._in_use)
            return RegisteredBufferPoolStats(
                size_classes=free_by_class,
                free_regions=sum(free_by_class.values()),
                in_use_regions=len(self._in_use) + len(self._external_in_use),
                total_regions=len(self._regions),
                total_bytes=self._total_bytes,
                free_bytes=free_bytes,
                in_use_bytes=in_use_bytes,
                acquire_count=self._acquire_count,
                reuse_count=self._reuse_count,
                allocate_count=self._allocate_count,
                oversize_allocate_count=self._oversize_allocate_count,
                wait_count=self._wait_count,
                release_count=self._release_count,
                register_s=self._register_s,
                unregister_s=self._unregister_s,
            )

    def close(self) -> None:
        """Unregister all free regions and mark the pool closed."""
        if self._cpp_pool is not None:
            self._cpp_pool.close()
            return
        with self._condition:
            if self._closed:
                return
            while self._reserved_bytes:
                self._condition.wait()
            self._closing = True
            active_count = len(self._in_use) + len(self._external_in_use)
            if active_count:
                self._closing = False
                raise RuntimeError(f"cannot close registered buffer pool with {active_count} active leases")
            free_regions = []
            for regions in self._free.values():
                while regions:
                    free_regions.append(regions.pop())
        errors = []
        retained = []
        for region in free_regions:
            try:
                self._unregister_region(region)
            except RuntimeError as exc:
                errors.append(exc)
                retained.append(region)
        with self._condition:
            if retained:
                for region in retained:
                    self._free[self._region_sizes[region.ptr]].append(region)
            else:
                self._free.clear()
            if errors:
                self._closing = False
                self._condition.notify_all()
                raise RuntimeError(f"unregister_buffer failed for {len(errors)} pooled regions") from errors[0]
            self._closed = True
            self._closing = False
            self._condition.notify_all()

    def __enter__(self) -> "RegisteredBufferPool":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _try_acquire_locked(self, requested_size: int) -> Optional[RegisteredBufferLease]:
        oversize_class = self._oversize_size_class(requested_size)
        if oversize_class is not None:
            return self._allocate_acquire_unlocked(oversize_class, requested_size, oversize=True)
        size_class = self._size_class(requested_size)
        if self._free[size_class]:
            region = self._free[size_class].pop()
            self._in_use.add(region.ptr)
            self._acquire_count += 1
            self._reuse_count += 1
            return RegisteredBufferLease(self, region, requested_size)
        if self._has_capacity_for(size_class):
            return self._allocate_acquire_unlocked(size_class, requested_size, oversize=False)
        return None

    def _has_capacity_for(self, size_class: int) -> bool:
        if size_class > self.max_bytes:
            return False
        if self._total_bytes + self._reserved_bytes + size_class > self.max_bytes:
            return False
        if self.max_regions is None:
            return True
        reserved_regions = math.ceil(self._reserved_bytes / size_class) if size_class else 0
        return len(self._regions) + reserved_regions < self.max_regions

    def _raise_if_impossible(self, size: int) -> None:
        size_class = self._oversize_size_class_for_empty_pool(size) or self._size_class(size)
        if size_class > self.max_bytes:
            raise ValueError(f"requested buffer size exceeds pool capacity: {size} > {self.max_bytes}")
        if self.max_regions == 0:
            raise ValueError("registered buffer pool max_regions is zero")

    def _oversize_size_class_for_empty_pool(self, size: int) -> Optional[int]:
        if size <= self.max_size_class:
            return None
        return ((int(size) + self.alignment - 1) // self.alignment) * self.alignment

    def _allocate_acquire_unlocked(
        self,
        size_class: int,
        requested_size: int,
        *,
        oversize: bool,
    ) -> RegisteredBufferLease:
        self._reserve_region(size_class)
        self._condition.release()
        try:
            region = self._register_region(size_class)
        except BaseException:
            self._condition.acquire()
            self._reserved_bytes -= size_class
            self._condition.notify_all()
            raise
        self._condition.acquire()
        self._reserved_bytes -= size_class
        self._condition.notify_all()
        try:
            self._raise_if_closed()
        except RuntimeError:
            self._condition.release()
            try:
                self._unregister_region(region)
            finally:
                self._condition.acquire()
            raise
        self._in_use.add(region.ptr)
        self._acquire_count += 1
        if oversize:
            self._oversize_allocate_count += 1
        else:
            self._allocate_count += 1
        return RegisteredBufferLease(self, region, requested_size)

    def _reserve_region(self, size_class: int) -> None:
        self._raise_if_closed()
        if self._closing:
            raise RuntimeError("registered buffer pool is closing")
        if not self._has_capacity_for(size_class):
            raise RuntimeError("registered buffer pool capacity exceeded")
        self._reserved_bytes += size_class

    def _append_prewarmed_region(self, size_class: int) -> None:
        region = self._allocate_registered_region(size_class)
        with self._condition:
            try:
                self._raise_if_closed()
                if self._closing:
                    raise RuntimeError("registered buffer pool is closing")
                self._free[size_class].append(region)
                self._condition.notify_all()
                return
            except BaseException:
                pass
        self._unregister_region(region)
        raise RuntimeError("registered buffer pool is closed")

    def _allocate_registered_region(self, size_class: int) -> "WritableBufferRegion":
        with self._condition:
            self._reserve_region(size_class)
        try:
            region = self._register_region(size_class)
        except BaseException:
            with self._condition:
                self._reserved_bytes -= size_class
                self._condition.notify_all()
            raise
        with self._condition:
            self._reserved_bytes -= size_class
            self._condition.notify_all()
        return region

    def _register_region(self, size: int) -> "WritableBufferRegion":
        buffer = bytearray(size)
        region = writable_buffer_region(buffer, registered=True)
        register_start = time.perf_counter()
        ret = self.store.register_buffer(region.ptr, region.size)
        register_s = time.perf_counter() - register_start
        if ret != 0:
            region.close()
            with self._condition:
                self._register_s += register_s
            raise RuntimeError(f"register_buffer failed with retcode {ret}")
        with self._condition:
            self._register_s += register_s
            self._regions[region.ptr] = region
            self._region_sizes[region.ptr] = region.size
            self._total_bytes += region.size
        return region

    def _unregister_region(self, region: "WritableBufferRegion") -> None:
        unregister_start = time.perf_counter()
        ret = self.store.unregister_buffer(region.ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
        if ret != 0:
            raise RuntimeError(f"unregister_buffer failed with retcode {ret}")
        with self._condition:
            self._regions.pop(region.ptr, None)
            self._total_bytes -= self._region_sizes.pop(region.ptr, region.size)
        region.registered = False
        region.close()

    def _size_class(self, size: int) -> int:
        size = max(int(size), 1)
        capped_size = min(size, self.max_size_class)
        if capped_size <= self.min_size_class:
            return self.min_size_class
        if capped_size <= self.alignment:
            return 1 << (capped_size - 1).bit_length()
        return ((capped_size + self.alignment - 1) // self.alignment) * self.alignment

    def _oversize_size_class(self, size: int) -> Optional[int]:
        if size <= self.max_size_class:
            return None
        rounded_size = ((int(size) + self.alignment - 1) // self.alignment) * self.alignment
        if self._total_bytes + self._reserved_bytes + rounded_size > self.max_bytes:
            return None
        if self.max_regions is not None:
            reserved_regions = 1 if self._reserved_bytes else 0
            if len(self._regions) + reserved_regions >= self.max_regions:
                return None
        return rounded_size

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise RuntimeError("registered buffer pool is closed")


@dataclass
class WritableBufferRegion:
    """Writable buffer pointer that keeps the Python buffer export alive."""

    buffer: Any
    view: memoryview
    c_buffer: Any
    ptr: int
    size: int
    registered: bool = False
    owns_view: bool = True
    closed: bool = False

    def view_region(self, size: int) -> "WritableBufferRegion":
        if self.closed:
            raise ValueError("buffer region is closed")
        if size > self.size:
            raise ValueError(f"buffer too small: need {size} bytes, got {self.size}")
        return WritableBufferRegion(
            buffer=self.buffer,
            view=self.view,
            c_buffer=self.c_buffer,
            ptr=self.ptr,
            size=size,
            registered=self.registered,
            owns_view=False,
        )

    def close(self) -> None:
        if self.closed:
            return
        self.c_buffer = None
        if self.owns_view:
            self.view.release()
        self.closed = True

    def __enter__(self) -> "WritableBufferRegion":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def writable_buffer_region(buffer: Any, registered: bool = False) -> WritableBufferRegion:
    """Return a writable buffer region while keeping the Python buffer export alive."""
    view = memoryview(buffer)
    if view.readonly:
        view.release()
        raise ValueError("buffer must be writable")
    if view.format != "B":
        try:
            contiguous = view.cast("B")
        except BaseException:
            view.release()
            raise
        view.release()
    else:
        contiguous = view
    try:
        c_buffer = (ctypes.c_ubyte * contiguous.nbytes).from_buffer(contiguous)
    except BaseException:
        contiguous.release()
        raise
    return WritableBufferRegion(
        buffer=buffer,
        view=contiguous,
        c_buffer=c_buffer,
        ptr=ctypes.addressof(c_buffer),
        size=contiguous.nbytes,
        registered=registered,
    )
