from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import Future
import ctypes
import gc
import importlib.machinery
import importlib.util
import json
import mmap
import os
import pathlib
import queue
import sys
import threading
from dataclasses import dataclass, field
import warnings

from ._runtime import package_dir, preload_native_libraries

_logger = logging.getLogger("mooncake.store")


def _load_native():
    root = package_dir()
    repo_root = root.parent.parent
    preload_native_libraries(root)
    try:
        from . import _store_rs as native  # type: ignore

        return native
    except Exception:
        target_roots: list[pathlib.Path] = []
        for env_key in ("MOONCAKE_PYTHON_TARGET_DIR", "CARGO_TARGET_DIR"):
            env_value = os.environ.get(env_key)
            if env_value:
                target_roots.append(pathlib.Path(env_value).expanduser())
        target_roots.append(repo_root / "target")
        suffixes = list(importlib.machinery.EXTENSION_SUFFIXES) + [".so"]
        patterns = []
        for suffix in suffixes:
            patterns.extend([f"_store_rs{suffix}", f"lib_store_rs{suffix}"])

        candidates: list[pathlib.Path] = []
        seen: set[pathlib.Path] = set()
        for target_root in target_roots:
            for profile in ("debug", "release"):
                for profile_dir in (
                    target_root / profile,
                    target_root / profile / "deps",
                ):
                    for pattern in patterns:
                        for candidate in profile_dir.glob(pattern):
                            resolved = candidate.resolve()
                            if resolved in seen or not resolved.exists():
                                continue
                            seen.add(resolved)
                            candidates.append(resolved)

        candidates.sort(
            key=lambda candidate: (
                candidate.stat().st_mtime_ns,
                1 if f"{pathlib.Path('target') / 'debug'}" in str(candidate) else 0,
            ),
            reverse=True,
        )

        for candidate in candidates:
            spec = importlib.util.spec_from_file_location(
                "mooncake._store_rs", candidate
            )
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules["mooncake._store_rs"] = module
            spec.loader.exec_module(module)
            return module

        raise ImportError(
            "cannot find native mooncake store module; run `cargo build -p mooncake-store-py` first"
        )


_native = _load_native()

_CACHE_STATUS = "status"
_CACHE_STATUS_LIST = "status_list"
_CACHE_BYTES = "bytes"
_CACHE_BYTES_LIST = "bytes_list"
_CACHE_BOOL = "bool"

# Python compatibility policy: cache data-plane failures degrade to misses or
# failed cache operations. Non-cache control-plane methods stay strict.
_CACHE_COMPAT_POLICY = {
    "register_buffer": _CACHE_STATUS,
    "unregister_buffer": _CACHE_STATUS,
    "put": _CACHE_STATUS,
    "put_from": _CACHE_STATUS,
    "batch_put": _CACHE_STATUS,
    "batch_put_from": _CACHE_STATUS_LIST,
    "batch_put_from_raw": _CACHE_STATUS_LIST,
    "batch_put_from_multi_buffers": _CACHE_STATUS_LIST,
    "batch_put_from_multi_buffers_raw": _CACHE_STATUS_LIST,
    "batch_get": _CACHE_BYTES_LIST,
    "batch_get_buffer": _CACHE_BYTES_LIST,
    "batch_get_into": _CACHE_STATUS_LIST,
    "batch_get_into_raw": _CACHE_STATUS_LIST,
    "batch_get_into_multi_buffers": _CACHE_STATUS_LIST,
    "get": _CACHE_BYTES,
    "get_into": _CACHE_STATUS,
    "is_exist": _CACHE_BOOL,
    "batch_is_exist": _CACHE_STATUS_LIST,
    "get_size": _CACHE_STATUS,
    "remove": _CACHE_STATUS,
    "batch_remove": _CACHE_STATUS_LIST,
}


def _cache_compat_fallback(name: str, count: int | None = None):
    kind = _CACHE_COMPAT_POLICY[name]
    if kind == _CACHE_STATUS:
        return -1
    if kind == _CACHE_BYTES:
        return b""
    if kind == _CACHE_BOOL:
        return False
    if count is None:
        raise ValueError(f"{name} soft-fail fallback requires an item count")
    if kind == _CACHE_STATUS_LIST:
        return [-1] * count
    if kind == _CACHE_BYTES_LIST:
        return [b""] * count
    raise AssertionError(f"unknown cache compatibility fallback {kind!r}")


def _normalize_hugepage_size(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        mapping = {
            "2m": 2 * 1024 * 1024,
            "2mb": 2 * 1024 * 1024,
            "2097152": 2 * 1024 * 1024,
            "1g": 1024 * 1024 * 1024,
            "1gb": 1024 * 1024 * 1024,
            "1073741824": 1024 * 1024 * 1024,
        }
        if normalized in mapping:
            return mapping[normalized]
    raise ValueError(
        f"unsupported hugepage size {value!r}; supported values are 2MB and 1GB"
    )


def _normalize_local_hostname_and_transport_port(
    local_hostname,
    transport_rpc_port,
) -> tuple[str, int | None]:
    hostname = str(local_hostname).strip()
    explicit_port = None if transport_rpc_port is None else int(transport_rpc_port)
    embedded_host = hostname
    embedded_port = None

    if hostname.startswith("["):
        closing = hostname.find("]")
        if closing > 0 and closing + 1 < len(hostname) and hostname[closing + 1] == ":":
            candidate = hostname[closing + 2 :]
            if candidate.isdigit():
                embedded_host = hostname[1:closing]
                embedded_port = int(candidate)
    elif hostname.count(":") == 1:
        candidate_host, candidate_port = hostname.rsplit(":", 1)
        if candidate_host and candidate_port.isdigit():
            embedded_host = candidate_host
            embedded_port = int(candidate_port)

    if (
        embedded_port is not None
        and explicit_port is not None
        and embedded_port != explicit_port
    ):
        raise ValueError(
            "local_hostname embedded transport port conflicts with transport_rpc_port"
        )
    return embedded_host, explicit_port if explicit_port is not None else embedded_port


class _BatchStatusResult(list):
    def status_code(self) -> int:
        for status in self:
            if int(status) != 0:
                return int(status)
        return 0

    def __bool__(self) -> bool:
        return self.status_code() != 0

    def __eq__(self, other):
        if isinstance(other, int):
            return self.status_code() == other
        return super().__eq__(other)

    def __int__(self) -> int:
        return self.status_code()


class MooncakeHostMemAllocator:
    def __init__(
        self,
        use_hugepage: bool | None = None,
        hugepage_size: int | str | None = None,
    ) -> None:
        self._use_hugepage = use_hugepage
        self._hugepage_size = _normalize_hugepage_size(hugepage_size)
        native_allocator = getattr(_native, "MooncakeHostMemAllocator", None)
        self._native_allocator = (
            native_allocator(
                use_hugepage=self._use_hugepage,
                hugepage_size=self._hugepage_size,
            )
            if native_allocator
            else None
        )
        self._allocations: dict[int, mmap.mmap] = {}

    def alloc(self, size: int) -> int:
        if self._native_allocator is not None:
            return int(self._native_allocator.alloc(int(size)))
        if self._use_hugepage or self._hugepage_size is not None:
            raise RuntimeError(
                "hugepage allocation requires the native mooncake store extension"
            )
        requested = int(size)
        if requested <= 0:
            raise ValueError("allocation size must be positive")
        region = mmap.mmap(-1, requested)
        pointer = ctypes.addressof(ctypes.c_char.from_buffer(region))
        self._allocations[pointer] = region
        return pointer

    def free(self, ptr: int) -> int:
        if self._native_allocator is not None:
            return int(self._native_allocator.free(int(ptr)))
        pointer = int(ptr)
        region = self._allocations.pop(pointer, None)
        if region is None:
            return -1
        region.close()
        return 0

    def close(self) -> None:
        if self._native_allocator is not None:
            return
        pointers = list(self._allocations)
        for pointer in pointers:
            self.free(pointer)

    def __del__(self) -> None:
        self.close()


class _NativeStoreWorker:
    def __init__(self) -> None:
        self._queue: queue.Queue[tuple[str, tuple, dict, Future] | None] = queue.Queue()
        self._closed = False
        self._startup = Future()
        self._thread = threading.Thread(
            target=self._run, name="mooncake-store-native", daemon=True
        )
        self._thread.start()
        self._startup.result()

    def call(self, name: str, *args, **kwargs):
        if self._closed:
            raise RuntimeError("store is closed")
        future = Future()
        self._queue.put((name, args, kwargs, future))
        return future.result()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._queue.put(None)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=1)

    def _run(self) -> None:
        try:
            native = _native.MooncakeDistributedStore()
        except BaseException as exc:
            self._startup.set_exception(exc)
            return
        self._startup.set_result(None)
        while True:
            request = self._queue.get()
            if request is None:
                try:
                    native.close()
                except Exception:
                    pass
                native = None
                gc.collect()
                return
            name, args, kwargs, future = request
            try:
                result = getattr(native, name)(*args, **kwargs)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)


@dataclass
class ReplicateConfig:
    replica_num: int = 1
    preferred_segment: str = ""
    preferred_segments: list[str] = field(default_factory=list)
    preferred_storage_owner: str = ""
    preferred_storage_owners: list[str] = field(default_factory=list)
    prefer_local: bool = True
    with_soft_pin: bool = False
    prefer_alloc_in_same_node: bool = False

    def copy(self) -> "ReplicateConfig":
        return type(self)(
            replica_num=self.replica_num,
            preferred_segment=self.preferred_segment,
            preferred_segments=list(self.preferred_segments),
            preferred_storage_owner=self.preferred_storage_owner,
            preferred_storage_owners=list(self.preferred_storage_owners),
            prefer_local=self.prefer_local,
            with_soft_pin=self.with_soft_pin,
            prefer_alloc_in_same_node=self.prefer_alloc_in_same_node,
        )


def init_tracing(filter: str | None = None):
    return _native.init_tracing(filter)


def metrics_text() -> str:
    return _native.metrics_text()


def start_metrics_server(bind_addr: str = "127.0.0.1:0") -> str:
    return _native.start_metrics_server(bind_addr)


def stop_metrics_server() -> None:
    _native.stop_metrics_server()


def metrics_server_address() -> str | None:
    return _native.metrics_server_address()


class MooncakeDistributedStore:
    def __init__(self) -> None:
        self._native = _native.MooncakeDistributedStore()
        self._worker = None
        self._lock = threading.RLock()
        self._registered_buffers: dict[int, int] = {}
        self._tracked_keys: set[tuple[str | None, str | None, str | None, str]] = set()
        self._default_domain: str | None = None
        self._default_object_set: str | None = None

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda *args, **kwargs: self._invoke(name, *args, **kwargs)

    @staticmethod
    def _log_setup_config(
        local_hostname,
        metadata_url,
        transport_metadata_url,
        global_segment_size,
        local_buffer_size,
        protocol,
        setup_kwargs,
    ):
        try:
            _logger.info(
                "mooncake-store-py setup: local_hostname=%s metadata_url=%s "
                "transport_metadata_url=%s global_segment_size=%s "
                "local_buffer_size=%s protocol=%s tenant=%s keyspace=%s "
                "stable_id=%s labels=%s initial_state=%s routed_writes=%s "
                "replica_count=%s route_topk=%s transport_rpc_port=%s "
                "transport_backend=%s domain=%s object_set=%s "
                "local_segment_name=%s expires_at_ms=%s use_hugepage=%s "
                "hugepage_size=%s route_control=%s",
                local_hostname,
                metadata_url,
                transport_metadata_url,
                global_segment_size,
                local_buffer_size,
                protocol,
                setup_kwargs.get("tenant"),
                setup_kwargs.get("keyspace"),
                setup_kwargs.get("stable_id"),
                setup_kwargs.get("labels"),
                setup_kwargs.get("initial_state"),
                setup_kwargs.get("routed_writes"),
                setup_kwargs.get("replica_count"),
                setup_kwargs.get("route_topk"),
                setup_kwargs.get("transport_rpc_port"),
                setup_kwargs.get("transport_backend"),
                setup_kwargs.get("domain"),
                setup_kwargs.get("object_set"),
                setup_kwargs.get("local_segment_name"),
                setup_kwargs.get("expires_at_ms"),
                setup_kwargs.get("use_hugepage"),
                setup_kwargs.get("hugepage_size"),
                setup_kwargs.get("route_control"),
            )
            keyspace = setup_kwargs.get("keyspace")
            if keyspace:
                _logger.warning(
                    "custom keyspace=%s is set — make sure you understand "
                    "its effect on tenant isolation; misconfigured keyspace "
                    "can cause nodes to register in different metadata prefixes",
                    keyspace,
                )
        except Exception:
            _logger.error("failed to log setup config", exc_info=True)

    def _invoke(self, name: str, *args, **kwargs):
        if self._worker is not None:
            return self._worker.call(name, *args, **kwargs)
        return getattr(self._native, name)(*args, **kwargs)

    def _invoke_cache(
        self,
        name: str,
        *args,
        fallback_count: int | None = None,
        **kwargs,
    ):
        try:
            return self._invoke(name, *args, **kwargs)
        except Exception:
            _logger.error(
                "MooncakeDistributedStore.%s raised an exception; "
                "returning compat fallback status code",
                name,
                exc_info=True,
            )
            return _cache_compat_fallback(name, fallback_count)

    def setup(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], Mapping) and not kwargs:
            return self._setup_from_config_dict(dict(args[0]))
        if len(args) > 8:
            raise TypeError("setup accepts at most 8 positional arguments")
        if len(args) == 8:
            args = args[:7]
        args = list(args)
        kwargs.pop("engine", None)
        if "state" in kwargs and "initial_state" not in kwargs:
            kwargs["initial_state"] = kwargs.pop("state")
        kwargs = _apply_setup_env_defaults(kwargs)
        if "hugepage_size" in kwargs:
            kwargs["hugepage_size"] = _normalize_hugepage_size(kwargs["hugepage_size"])
        self._default_domain = (
            kwargs.get("domain") if _has_value(kwargs.get("domain")) else None
        )
        self._default_object_set = (
            kwargs.get("object_set") if _has_value(kwargs.get("object_set")) else None
        )
        if args:
            local_hostname, transport_rpc_port = (
                _normalize_local_hostname_and_transport_port(
                    args[0],
                    kwargs.get("transport_rpc_port"),
                )
            )
            args[0] = local_hostname
            if transport_rpc_port is not None and "transport_rpc_port" not in kwargs:
                kwargs["transport_rpc_port"] = transport_rpc_port
        _warn_setup_policy_fallback(kwargs, source="MooncakeDistributedStore.setup")
        _init_tracing_from_env()
        self._log_setup_config(
            local_hostname=args[0] if len(args) > 0 else None,
            metadata_url=args[6] if len(args) > 6 else None,
            transport_metadata_url=args[1] if len(args) > 1 else None,
            global_segment_size=args[2] if len(args) > 2 else None,
            local_buffer_size=args[3] if len(args) > 3 else None,
            protocol=args[4] if len(args) > 4 else None,
            setup_kwargs=kwargs,
        )
        result = self._invoke("setup", *args, **kwargs)
        _logger.info("mooncake-store-py ready: stable_id=%s", kwargs.get("stable_id"))
        _start_metrics_server_from_env()
        return result

    def setup_dummy(
        self,
        mem_pool_size: int,
        local_buffer_size: int,
        server_address: str,
        *,
        keyspace: str | None = None,
        worker_scope: str | None = None,
    ):
        kwargs = _apply_dummy_setup_env_defaults(
            {
                "keyspace": keyspace,
                "worker_scope": worker_scope,
            }
        )
        return self._invoke(
            "setup_dummy",
            mem_pool_size,
            local_buffer_size,
            server_address,
            keyspace=kwargs.get("keyspace"),
            worker_scope=kwargs.get("worker_scope"),
        )

    def close(self) -> None:
        with self._lock:
            self._registered_buffers.clear()
            self._tracked_keys.clear()
        try:
            self._invoke("close")
        finally:
            if self._worker is not None:
                self._worker.close()
            self._default_domain = None
            self._default_object_set = None

    def register_buffer(self, buffer_ptr: int, size: int):
        result = self._invoke_cache("register_buffer", buffer_ptr, size)
        if result == 0:
            with self._lock:
                self._registered_buffers[int(buffer_ptr)] = int(size)
        return result

    def unregister_buffer(self, buffer_ptr: int, size: int | None = None):
        resolved_size = self._resolve_registered_size(buffer_ptr, size)
        result = self._invoke_cache("unregister_buffer", buffer_ptr, resolved_size)
        if result == 0:
            with self._lock:
                self._registered_buffers.pop(int(buffer_ptr), None)
        return result

    def put(self, key: str, value, *, tenant: str | None = None, config=None):
        result = self._invoke_cache(
            "put",
            key,
            value,
            tenant=tenant,
            **_replication_kwargs(config),
        )
        if result == 0:
            self._track_keys([key], tenant=tenant)
        return result

    def put_from(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        tenant: str | None = None,
        config=None,
    ):
        result = self._invoke_cache(
            "put_from",
            key,
            buffer_ptr,
            size,
            tenant=tenant,
            **_replication_kwargs(config),
        )
        if result == 0:
            self._track_keys([key], tenant=tenant)
        return result

    def batch_put(
        self,
        items=None,
        *,
        keys: Sequence[str] | None = None,
        values: Sequence[bytes] | None = None,
        tenant: str | None = None,
        config=None,
    ):
        normalized = _normalize_key_value_items(items, keys, values)
        result = self._invoke_cache(
            "batch_put",
            normalized,
            tenant=tenant,
            **_replication_kwargs(config),
        )
        if result == 0:
            self._track_keys([key for key, _ in normalized], tenant=tenant)
        return result

    def put_batch(
        self,
        keys: Sequence[str],
        values: Sequence[bytes],
        config=None,
        *,
        tenant: str | None = None,
    ):
        return self.batch_put(keys=keys, values=values, tenant=tenant, config=config)

    def batch_put_from(self, *args, tenant: str | None = None, config=None):
        if len(args) == 1:
            items = _normalize_pointer_items(args[0])
            result = self._invoke_cache(
                "batch_put_from",
                items,
                fallback_count=len(items),
                tenant=tenant,
                **_replication_kwargs(config),
            )
            statuses = _coerce_batch_status_result(result, len(items))
            self._track_successful_keys(
                [key for key, _, _ in items], statuses, tenant=tenant
            )
            return statuses
        if len(args) == 3:
            keys, buffer_ptrs, sizes = _normalize_raw_batch_args(*args)
            result = self._invoke_cache(
                "batch_put_from_raw",
                keys,
                buffer_ptrs,
                sizes,
                fallback_count=len(keys),
                tenant=tenant,
                **_replication_kwargs(config),
            )
            statuses = _coerce_batch_status_result(result, len(keys))
            self._track_successful_keys(keys, statuses, tenant=tenant)
            return statuses
        raise TypeError(
            "batch_put_from expects either items or (keys, buffer_ptrs, sizes)"
        )

    def batch_put_from_multi_buffers(
        self,
        *args,
        tenant: str | None = None,
        config=None,
    ):
        if len(args) == 1:
            items = list(args[0])
            if _items_use_bytes_payloads(items):
                return self._invoke_cache(
                    "batch_put_from_multi_buffers",
                    items,
                    fallback_count=len(items),
                    tenant=tenant,
                    **_replication_kwargs(config),
                )
            keys, all_buffer_ptrs, all_sizes = _normalize_descriptor_items(items)
            return self._invoke_cache(
                "batch_put_from_multi_buffers_raw",
                keys,
                all_buffer_ptrs,
                all_sizes,
                fallback_count=len(keys),
                tenant=tenant,
                **_replication_kwargs(config),
            )
        if len(args) == 3:
            keys, all_buffer_ptrs, all_sizes = _normalize_raw_multi_buffer_args(*args)
            return self._invoke_cache(
                "batch_put_from_multi_buffers_raw",
                keys,
                all_buffer_ptrs,
                all_sizes,
                fallback_count=len(keys),
                tenant=tenant,
                **_replication_kwargs(config),
            )
        raise TypeError(
            "batch_put_from_multi_buffers expects items or (keys, all_buffer_ptrs, all_sizes)"
        )

    def batch_get(self, keys: Sequence[str], *, tenant: str | None = None):
        key_list = list(keys)
        return self._invoke_cache(
            "batch_get", key_list, fallback_count=len(key_list), tenant=tenant
        )

    def get_batch(self, keys: Sequence[str], *, tenant: str | None = None):
        return self.batch_get(keys, tenant=tenant)

    def batch_get_buffer(self, keys: Sequence[str], *, tenant: str | None = None):
        key_list = list(keys)
        return self._invoke_cache(
            "batch_get_buffer",
            key_list,
            fallback_count=len(key_list),
            tenant=tenant,
        )

    def batch_get_into(self, *args, tenant: str | None = None):
        if len(args) == 1:
            items = list(args[0])
            return self._invoke_cache(
                "batch_get_into", items, fallback_count=len(items), tenant=tenant
            )
        if len(args) == 3:
            keys, buffer_ptrs, sizes = _normalize_raw_batch_args(*args)
            return self._invoke_cache(
                "batch_get_into_raw",
                keys,
                buffer_ptrs,
                sizes,
                fallback_count=len(keys),
                tenant=tenant,
            )
        raise TypeError(
            "batch_get_into expects either items or (keys, buffer_ptrs, sizes)"
        )

    def batch_get_into_multi_buffers(
        self,
        *args,
        prefer_alloc_in_same_node: bool = False,
        tenant: str | None = None,
    ):
        if len(args) == 1:
            keys, all_buffer_ptrs, all_sizes = _normalize_descriptor_items(
                list(args[0])
            )
        elif len(args) == 3:
            keys, all_buffer_ptrs, all_sizes = _normalize_raw_multi_buffer_args(*args)
        else:
            raise TypeError(
                "batch_get_into_multi_buffers expects items or (keys, all_buffer_ptrs, all_sizes)"
            )
        return self._invoke_cache(
            "batch_get_into_multi_buffers",
            keys,
            all_buffer_ptrs,
            all_sizes,
            fallback_count=len(keys),
            prefer_alloc_in_same_node=prefer_alloc_in_same_node,
            tenant=tenant,
        )

    def get(self, key: str, *, tenant: str | None = None):
        return self._invoke_cache("get", key, tenant=tenant)

    def get_into(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        tenant: str | None = None,
    ):
        return self._invoke_cache("get_into", key, buffer_ptr, size, tenant=tenant)

    def get_into_ranges(
        self,
        buffer_ptrs: list[int],
        all_keys: list[list[str]],
        all_dst_offsets: list[list[list[int]]],
        all_src_offsets: list[list[list[int]]],
        all_sizes: list[list[list[int]]],
        *,
        buffer_sizes: list[int] | None = None,
        tenant: str | None = None,
    ) -> list[list[list[int]]]:
        return self._invoke(
            "get_into_ranges",
            buffer_ptrs,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
            buffer_sizes=buffer_sizes,
            tenant=tenant,
        )

    # ─── Tensor API ─────────────────────────────────────────────────────────

    def put_tensor(self, key: str, tensor, *, tenant: str | None = None, config=None):
        """Store a tensor with binary metadata header.

        The stored object is [TensorMetadata(304 bytes) | raw_data].
        """
        return self._invoke(
            "put_tensor",
            key,
            tensor,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def put_tensor_from(
        self, key: str, tensor, *, tenant: str | None = None, config=None
    ):
        """Store a tensor via zero-copy from registered memory.

        The tensor MUST reside in a buffer previously passed to
        register_buffer(), with at least TENSOR_METADATA_WIRE_SIZE (304)
        bytes of headroom before tensor.data_ptr().

        For tensors NOT in registered memory, use put_tensor() instead.
        """
        return self._invoke(
            "put_tensor_from",
            key,
            tensor,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def get_tensor(self, key: str, *, tensor=None, tenant: str | None = None):
        """Read a tensor from the store.

        If *tensor* is provided, its storage is used as the destination buffer.
        This path requires that the tensor resides in a buffer previously
        passed to register_buffer(), with at least TENSOR_METADATA_WIRE_SIZE
        (304) bytes of headroom before tensor.data_ptr().

        For tensors NOT in registered memory, omit the *tensor* parameter —
        the fallback path allocates internally and returns a new torch.Tensor.
        """
        import torch

        if tensor is not None:
            buf_ptr = tensor.data_ptr() - TENSOR_METADATA_WIRE_SIZE
            buf_size = (
                TENSOR_METADATA_WIRE_SIZE + tensor.nelement() * tensor.element_size()
            )
            result = self._invoke(
                "get_tensor_into", key, buf_ptr, buf_size, tenant=tenant
            )
            return _tensor_from_read_result(result)

        # Fallback: read raw bytes and reconstruct
        raw = self._invoke("get", key, tenant=tenant)
        return _tensor_from_raw_bytes(raw)

    def get_tensor_into(
        self, key: str, buffer_ptr: int, size: int, *, tenant: str | None = None
    ):
        """Read a tensor into a pre-allocated buffer. Returns a TensorReadResult."""
        return self._invoke("get_tensor_into", key, buffer_ptr, size, tenant=tenant)

    # ─── Parallel Tensor API ─────────────────────────────────────────────

    def put_tensor_with_parallelism(
        self,
        key: str,
        tensor: torch.Tensor,
        *,
        parallelism: TensorParallelism | None = None,
        writer_partition: tuple[int, int, int] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Store a tensor with parallelism metadata.

        Args:
            key: Base key name (e.g. "model.layers.0.weight")
            tensor: A contiguous torch.Tensor.
            parallelism: A TensorParallelism object describing the parallelism layout.
            writer_partition: A (rank, size, split_dim) tuple for writer-partition mode.
            tenant: Optional tenant name.
            config: Optional ReplicateConfig.
        """
        return self._invoke(
            "put_tensor_with_parallelism",
            key,
            tensor,
            parallelism=parallelism,
            writer_partition=writer_partition,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def upsert_tensor_with_parallelism(
        self,
        key: str,
        tensor: torch.Tensor,
        *,
        parallelism: TensorParallelism | None = None,
        writer_partition: tuple[int, int, int] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Upsert a tensor with parallelism: removes existing then puts."""
        return self._invoke(
            "upsert_tensor_with_parallelism",
            key,
            tensor,
            parallelism=parallelism,
            writer_partition=writer_partition,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def get_tensor_with_parallelism(
        self,
        key: str,
        *,
        target: ReadTarget | None = None,
        tensor: torch.Tensor | None = None,
        tenant: str | None = None,
    ):
        """Read a tensor with parallelism-aware routing.

        Args:
            key: Base key name.
            target: A ReadTarget specifying AsStored/Shard/Full mode.
            tensor: Optional pre-allocated torch.Tensor to fill.
            tenant: Optional tenant name.
        """
        result = self._invoke(
            "get_tensor_with_parallelism",
            key,
            target=target,
            tensor=tensor,
            tenant=tenant,
        )
        if hasattr(result, "dtype") and hasattr(result, "shape"):
            if isinstance(result.dtype, int):
                return _tensor_from_read_result(result)
            return result
        if isinstance(result, (bytes, bytearray)):
            return _tensor_from_raw_bytes(result)
        return result

    def get_tensor_with_parallelism_into(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        target: ReadTarget | None = None,
        tenant: str | None = None,
    ):
        """Read a parallelism-aware tensor into a pre-registered buffer.

        Returns a TensorReadResult with data_ptr, shape, dtype info.
        """
        return self._invoke(
            "get_tensor_with_parallelism_into",
            key,
            buffer_ptr,
            size,
            target=target,
            tenant=tenant,
        )

    # ─── _from Tensor API ──────────────────────────────────────────────────

    def put_tensor_with_parallelism_from(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        parallelism: TensorParallelism | None = None,
        writer_partition: tuple[int, int, int] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Put a tensor from a raw buffer [TensorMetadata | data] with parallelism."""
        return self._invoke(
            "put_tensor_with_parallelism_from",
            key,
            buffer_ptr,
            size,
            parallelism=parallelism,
            writer_partition=writer_partition,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def upsert_tensor_with_parallelism_from(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        parallelism: TensorParallelism | None = None,
        writer_partition: tuple[int, int, int] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Upsert a tensor from a raw buffer with parallelism."""
        return self._invoke(
            "upsert_tensor_with_parallelism_from",
            key,
            buffer_ptr,
            size,
            parallelism=parallelism,
            writer_partition=writer_partition,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    # ─── Batch Tensor API ────────────────────────────────────────────────

    def batch_put_tensor_with_parallelism(
        self,
        keys: list[str],
        tensors: list,
        *,
        parallelisms: list[TensorParallelism | None] | None = None,
        writer_partitions: list[tuple[int, int, int] | None] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Batch put tensors with parallelism metadata."""
        return self._invoke(
            "batch_put_tensor_with_parallelism",
            keys,
            tensors,
            parallelisms=parallelisms,
            writer_partitions=writer_partitions,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def batch_upsert_tensor_with_parallelism(
        self,
        keys: list[str],
        tensors: list,
        *,
        parallelisms: list[TensorParallelism | None] | None = None,
        writer_partitions: list[tuple[int, int, int] | None] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Batch upsert tensors with parallelism."""
        return self._invoke(
            "batch_upsert_tensor_with_parallelism",
            keys,
            tensors,
            parallelisms=parallelisms,
            writer_partitions=writer_partitions,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def batch_put_tensor_with_parallelism_from(
        self,
        keys: list[str],
        buffer_ptrs: list[int],
        sizes: list[int],
        *,
        parallelisms: list[TensorParallelism | None] | None = None,
        writer_partitions: list[tuple[int, int, int] | None] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Batch put tensors from raw buffers with parallelism."""
        return self._invoke(
            "batch_put_tensor_with_parallelism_from",
            keys,
            buffer_ptrs,
            sizes,
            parallelisms=parallelisms,
            writer_partitions=writer_partitions,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def batch_upsert_tensor_with_parallelism_from(
        self,
        keys: list[str],
        buffer_ptrs: list[int],
        sizes: list[int],
        *,
        parallelisms: list[TensorParallelism | None] | None = None,
        writer_partitions: list[tuple[int, int, int] | None] | None = None,
        tenant: str | None = None,
        config: ReplicateConfig | None = None,
    ):
        """Batch upsert tensors from raw buffers with parallelism."""
        return self._invoke(
            "batch_upsert_tensor_with_parallelism_from",
            keys,
            buffer_ptrs,
            sizes,
            parallelisms=parallelisms,
            writer_partitions=writer_partitions,
            tenant=tenant,
            **_replication_kwargs_simple(config),
        )

    def batch_get_tensor_with_parallelism(
        self,
        keys: list[str],
        *,
        targets: list[ReadTarget | None] | None = None,
        tensors: list | None = None,
        tenant: str | None = None,
    ):
        """Batch get tensors with parallelism-aware routing."""
        results = self._invoke(
            "batch_get_tensor_with_parallelism",
            keys,
            targets=targets,
            tensors=tensors,
            tenant=tenant,
        )
        if isinstance(results, list):
            return [
                _tensor_from_raw_bytes(r) if isinstance(r, (bytes, bytearray)) else r
                for r in results
            ]
        return results

    def batch_get_tensor_with_parallelism_into(
        self,
        keys: list[str],
        buffer_ptrs: list[int],
        sizes: list[int],
        *,
        targets: list[ReadTarget | None] | None = None,
        tenant: str | None = None,
    ):
        """Batch get tensors into pre-registered buffers with parallelism."""
        return self._invoke(
            "batch_get_tensor_with_parallelism_into",
            keys,
            buffer_ptrs,
            sizes,
            targets=targets,
            tenant=tenant,
        )

    # ─── End Tensor API ───────────────────────────────────────────────────

    def is_exist(self, key: str, *, tenant: str | None = None) -> bool:
        return bool(self._invoke_cache("is_exist", key, tenant=tenant))

    def batch_is_exist(self, keys: Sequence[str], *, tenant: str | None = None):
        key_list = list(keys)
        return self._invoke_cache(
            "batch_is_exist",
            key_list,
            fallback_count=len(key_list),
            tenant=tenant,
        )

    def get_hostname(self) -> str:
        return self._invoke("get_hostname")

    def get_size(self, key: str, *, tenant: str | None = None) -> int:
        return self._invoke_cache("get_size", key, tenant=tenant)

    def remove(self, key: str, *, force: bool = False, tenant: str | None = None):
        result = self._invoke_cache("remove", key, force=force, tenant=tenant)
        if result == 0:
            self._forget_keys([key], tenant=tenant)
        return result

    def batch_remove(
        self,
        keys: Sequence[str],
        *,
        force: bool = False,
        tenant: str | None = None,
    ):
        key_list = list(keys)
        result = self._invoke_cache(
            "batch_remove",
            key_list,
            fallback_count=len(key_list),
            force=force,
            tenant=tenant,
        )
        for key, status in zip(key_list, result):
            if int(status) == 0:
                self._forget_keys([key], tenant=tenant)
        return result

    def remove_all(self, force: bool = False):
        try:
            removed = self._invoke("remove_all", force=force)
            if int(removed) >= 0:
                self._tracked_keys.clear()
            return removed
        except Exception:
            pass
        removed = 0
        for (tenant, domain, object_set), keys in self._group_tracked_keys().items():
            original_domain = self._default_domain
            original_object_set = self._default_object_set
            self._default_domain = domain
            self._default_object_set = object_set
            try:
                statuses = self._invoke_cache(
                    "batch_remove",
                    keys,
                    fallback_count=len(keys),
                    force=force,
                    tenant=tenant,
                )
                for key, status in zip(keys, statuses):
                    if int(status) == 0:
                        self._forget_keys([key], tenant=tenant)
                        removed += 1
            finally:
                self._default_domain = original_domain
                self._default_object_set = original_object_set
        return removed

    def start_metrics_server(self, bind_addr: str = "127.0.0.1:0") -> str:
        return self._invoke("start_metrics_server", bind_addr)

    def stop_metrics_server(self) -> None:
        self._invoke("stop_metrics_server")

    def metrics_server_address(self) -> str | None:
        return self._invoke("metrics_server_address")

    def _resolve_registered_size(self, buffer_ptr: int, size: int | None) -> int:
        if size is not None:
            return int(size)
        with self._lock:
            if int(buffer_ptr) not in self._registered_buffers:
                raise ValueError(
                    "buffer size is required for unknown pointers; call register_buffer first or pass size explicitly"
                )
            return self._registered_buffers[int(buffer_ptr)]

    def _track_keys(self, keys: Iterable[str], *, tenant: str | None = None) -> None:
        with self._lock:
            for key in keys:
                self._tracked_keys.add(
                    (tenant, self._default_domain, self._default_object_set, str(key))
                )

    def _forget_keys(self, keys: Iterable[str], *, tenant: str | None = None) -> None:
        with self._lock:
            for key in keys:
                self._tracked_keys.discard(
                    (tenant, self._default_domain, self._default_object_set, str(key))
                )

    def _track_successful_keys(
        self,
        keys: Iterable[str],
        statuses: Iterable[int],
        *,
        tenant: str | None = None,
    ) -> None:
        self._track_keys(
            (key for key, status in zip(keys, statuses) if int(status) == 0),
            tenant=tenant,
        )

    def _group_tracked_keys(
        self,
    ) -> dict[tuple[str | None, str | None, str | None], list[str]]:
        grouped: dict[tuple[str | None, str | None, str | None], list[str]] = {}
        with self._lock:
            for tenant, domain, object_set, key in self._tracked_keys:
                grouped.setdefault((tenant, domain, object_set), []).append(key)
        return grouped

    def _setup_from_config_dict(self, config: Mapping[str, object]):
        config = _apply_setup_env_defaults(dict(config))
        _warn_setup_policy_fallback(
            config, source="MooncakeDistributedStore.setup config"
        )
        if "local_hostname" not in config:
            raise TypeError("setup config requires `local_hostname`")
        # Transfer Engine metadata. Defaults to P2PHANDSHAKE so callers that omit
        # the field still get a working classic_te bootstrap. Pass a `redis://...`
        # value explicitly when using the `tent` backend. Accepts the upstream
        # Mooncake key `metadata_server` as an alias for compatibility with
        # existing JSON configs.
        transport_metadata_url = (
            config.get("transport_metadata_url")
            or config.get("metadata_server")
            or os.environ.get("MC_STORE_RS_TRANSPORT_METADATA_URL")
            or "P2PHANDSHAKE"
        )
        # Store-RS metadata URL. Accepts the upstream Mooncake keys
        # `master_server` / `master_server_addr` / `master_server_address` as
        # aliases for compatibility with existing JSON configs.
        metadata_url = (
            config.get("metadata_url")
            or config.get("master_server")
            or config.get("master_server_addr")
            or config.get("master_server_address")
            or os.environ.get("MC_STORE_RS_METADATA_URL")
        )
        if not _has_value(metadata_url):
            raise TypeError(
                "setup config requires `metadata_url` (Store-RS metadata URL, "
                "e.g. redis://host:port/db or etcd://host:port). "
                "MC_STORE_RS_METADATA_URL env is honored as a fallback."
            )
        transport_rpc_port = _coerce_optional_int(
            config.get("transport_rpc_port", config.get("rpc_server_port"))
        )
        local_hostname, transport_rpc_port = (
            _normalize_local_hostname_and_transport_port(
                config["local_hostname"],
                transport_rpc_port,
            )
        )
        initial_state = (
            _coerce_optional_str(config.get("initial_state", config.get("state")))
            or "active"
        )
        self._default_domain = (
            _coerce_optional_str(config.get("domain"))
            if _has_value(config.get("domain"))
            else None
        )
        self._default_object_set = (
            _coerce_optional_str(config.get("object_set"))
            if _has_value(config.get("object_set"))
            else None
        )
        _init_tracing_from_env()
        setup_kwargs = dict(
            stable_id=_coerce_optional_str(config.get("stable_id")),
            initial_state=initial_state,
            tenant=str(config.get("tenant", "default")),
            domain=self._default_domain,
            object_set=self._default_object_set,
            labels=_coerce_mapping(config.get("labels")),
            routed_writes=_coerce_bool(config.get("routed_writes"), False),
            replica_count=_coerce_int(config.get("replica_count"), 1),
            route_topk=_coerce_int(config.get("route_topk"), 2),
            keyspace=_coerce_optional_str(config.get("keyspace")),
            worker_scope=_coerce_optional_str(config.get("worker_scope")),
            transport_rpc_port=transport_rpc_port,
            transport_backend=_coerce_optional_str(config.get("transport_backend")),
            local_segment_name=_coerce_optional_str(config.get("local_segment_name")),
            expires_at_ms=_coerce_optional_int(config.get("expires_at_ms")),
            use_hugepage=_coerce_optional_bool(config.get("use_hugepage")),
            hugepage_size=_normalize_hugepage_size(config.get("hugepage_size")),
            eviction_high_watermark_percent=_coerce_optional_int(
                config.get("eviction_high_watermark_percent")
            ),
            eviction_low_watermark_percent=_coerce_optional_int(
                config.get("eviction_low_watermark_percent")
            ),
            route_control=str(config.get("route_control", "embedded_wrh")),
        )
        global_segment_size = _coerce_int(
            config.get("global_segment_size"), 16 * 1024 * 1024
        )
        local_buffer_size = _coerce_int(
            config.get("local_buffer_size"), 16 * 1024 * 1024
        )
        protocol = str(config.get("protocol", "tcp"))
        self._log_setup_config(
            local_hostname=local_hostname,
            metadata_url=metadata_url,
            transport_metadata_url=transport_metadata_url,
            global_segment_size=global_segment_size,
            local_buffer_size=local_buffer_size,
            protocol=protocol,
            setup_kwargs=setup_kwargs,
        )
        result = self._invoke(
            "setup",
            local_hostname,
            str(transport_metadata_url),
            global_segment_size,
            local_buffer_size,
            protocol,
            str(config.get("rdma_devices", "")),
            str(metadata_url),
            **setup_kwargs,
        )
        _logger.info("mooncake-store-py ready: stable_id=%s", setup_kwargs["stable_id"])
        _start_metrics_server_from_env()
        return result


def _replication_kwargs(config) -> dict:
    if config is None:
        return {}
    preferred_segments = list(getattr(config, "preferred_segments", []) or [])
    preferred_segment = getattr(config, "preferred_segment", "")
    if (
        preferred_segment not in ("", None)
        and preferred_segment not in preferred_segments
    ):
        preferred_segments.insert(0, preferred_segment)
    preferred_storage_owners = list(
        getattr(config, "preferred_storage_owners", []) or []
    )
    preferred_storage_owner = getattr(config, "preferred_storage_owner", "")
    if (
        preferred_storage_owner not in ("", None)
        and preferred_storage_owner not in preferred_storage_owners
    ):
        preferred_storage_owners.insert(0, preferred_storage_owner)
    return {
        "replica_count": int(getattr(config, "replica_num", 1)),
        "preferred_segments": preferred_segments or None,
        "preferred_storage_owners": preferred_storage_owners or None,
        "prefer_local": bool(getattr(config, "prefer_local", True)),
        "prefer_alloc_in_same_node": bool(
            getattr(config, "prefer_alloc_in_same_node", False)
        ),
        "with_soft_pin": bool(getattr(config, "with_soft_pin", False)),
    }


def _replication_kwargs_simple(config) -> dict:
    """Extract only replica_count from config (for tensor APIs)."""
    if config is None:
        return {}
    replica_count = int(getattr(config, "replica_num", 1))
    if replica_count <= 1:
        return {}
    return {"replica_count": replica_count}


# Size of the TensorMetadata wire-format struct (must match Rust TensorMetadata::WIRE_SIZE).
TENSOR_METADATA_WIRE_SIZE = 304

_DTYPE_MAP = {
    0: "float32",
    1: "float64",
    2: "int8",
    3: "uint8",
    4: "int16",
    5: "uint16",
    6: "int32",
    7: "uint32",
    8: "int64",
    9: "uint64",
    10: "bool",
    11: "float16",
    12: "bfloat16",
    13: "float8_e4m3fn",
    14: "float8_e5m2",
}


def _tensor_from_read_result(result):
    """Convert a TensorReadResult into a torch.Tensor view."""
    import torch
    import ctypes as _ctypes

    dtype_str = _DTYPE_MAP.get(result.dtype)
    if dtype_str is None:
        raise ValueError(f"unsupported dtype code: {result.dtype}")
    torch_dtype = getattr(torch, dtype_str)
    numel = 1
    for d in result.shape:
        numel *= d
    # Create tensor from data pointer
    tensor = torch.frombuffer(
        (_ctypes.c_char * result.data_bytes).from_address(result.data_ptr),
        dtype=torch_dtype,
    ).reshape(result.shape)
    return tensor


def _tensor_from_raw_bytes(raw: bytes):
    """Parse TensorMetadata from raw bytes and return a torch.Tensor."""
    import torch
    import struct

    if len(raw) < TENSOR_METADATA_WIRE_SIZE:
        raise ValueError("raw data too short for tensor metadata")
    magic, version, header_size, dtype_i32, ndim = struct.unpack_from("<IHHII", raw, 0)
    if magic != 0x4D4F4F4E:
        raise ValueError(f"bad tensor magic: {magic:#x}")
    layout_kind, _reserved_flags, data_offset, data_bytes = struct.unpack_from(
        "<IIqq", raw, 16
    )
    # Read local shape from layout (after header, offset 40 + 64 for global shape)
    local_shape_offset = 40 + 64  # after header(40) + global_shape(64)
    shape = []
    for i in range(ndim):
        dim = struct.unpack_from("<q", raw, local_shape_offset + i * 8)[0]
        shape.append(dim)

    dtype_str = _DTYPE_MAP.get(dtype_i32)
    if dtype_str is None:
        raise ValueError(f"unsupported dtype code: {dtype_i32}")
    torch_dtype = getattr(torch, dtype_str)
    # Single copy via bytearray from memoryview slice (avoids double-copy).
    data = bytearray(memoryview(raw)[data_offset : data_offset + data_bytes])
    tensor = torch.frombuffer(data, dtype=torch_dtype).reshape(shape)
    return tensor


def _normalize_key_value_items(
    items,
    keys: Sequence[str] | None,
    values: Sequence[bytes] | None,
):
    if items is not None:
        if keys is not None or values is not None:
            raise TypeError("batch_put accepts either items or keys/values")
        return list(items)
    if keys is None or values is None:
        raise TypeError("batch_put requires items or both keys and values")
    if len(keys) != len(values):
        raise ValueError("keys and values must have the same length")
    return list(zip(keys, values))


def _normalize_raw_batch_args(
    keys: Sequence[str],
    buffer_ptrs: Sequence[int],
    sizes: Sequence[int],
):
    if len(keys) != len(buffer_ptrs) or len(keys) != len(sizes):
        raise ValueError("keys, buffer_ptrs, and sizes must have the same length")
    return list(keys), [int(ptr) for ptr in buffer_ptrs], [int(size) for size in sizes]


def _normalize_pointer_items(items: Iterable[tuple]):
    normalized = []
    for item in items:
        if len(item) != 3:
            raise TypeError("batch put-from items must be (key, buffer_ptr, size)")
        key, buffer_ptr, size = item
        normalized.append((str(key), int(buffer_ptr), int(size)))
    return normalized


def _coerce_batch_status_result(result, count: int) -> _BatchStatusResult:
    if isinstance(result, int):
        return _BatchStatusResult([int(result)] * count)
    return _BatchStatusResult([int(status) for status in result])


def _coerce_bool(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _coerce_optional_bool(value) -> bool | None:
    if value is None:
        return None
    return _coerce_bool(value, False)


def _coerce_int(value, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _coerce_optional_int(value) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_optional_str(value) -> str | None:
    if value in ("", None):
        return None
    return str(value)


def _coerce_mapping(value) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("labels must be a mapping")
    return {str(key): str(item) for key, item in value.items()}


def _warn_setup_policy_fallback(config: Mapping[str, object], *, source: str) -> None:
    policy_keys = []
    if _has_value(config.get("route_topk")):
        policy_keys.append("route_topk")
    if _has_value(config.get("route_control")):
        policy_keys.append("route_control")
    if not policy_keys:
        return
    joined = ", ".join(policy_keys)
    warnings.warn(
        f"{source} provided {joined}; admin-managed tenant policy in metadata is the preferred configuration surface and these values are treated as compatibility fallbacks",
        UserWarning,
        stacklevel=4 if source == "MooncakeDistributedStore.setup config" else 3,
    )


_SETUP_ENV_DEFAULTS = {
    "stable_id": ("MC_STORE_RS_STABLE_ID", _coerce_optional_str),
    "initial_state": ("MC_STORE_RS_INITIAL_STATE", _coerce_optional_str),
    "tenant": ("MC_STORE_RS_TENANT", str),
    "domain": ("MC_STORE_RS_DOMAIN", str),
    "object_set": ("MC_STORE_RS_OBJECT_SET", str),
    "routed_writes": (
        "MC_STORE_RS_ROUTED_WRITES",
        lambda value: _coerce_bool(value, False),
    ),
    "replica_count": ("MC_STORE_RS_REPLICA_COUNT", lambda value: _coerce_int(value, 1)),
    "route_topk": ("MC_STORE_RS_ROUTE_TOPK", lambda value: _coerce_int(value, 2)),
    "keyspace": ("MC_STORE_RS_KEYSPACE", _coerce_optional_str),
    "transport_rpc_port": ("MC_STORE_RS_TRANSPORT_RPC_PORT", _coerce_optional_int),
    "transport_backend": ("MC_STORE_RS_TRANSPORT_BACKEND", _coerce_optional_str),
    "local_segment_name": ("MC_STORE_RS_LOCAL_SEGMENT_NAME", _coerce_optional_str),
    "expires_at_ms": ("MC_STORE_RS_EXPIRES_AT_MS", _coerce_optional_int),
    "eviction_high_watermark_percent": (
        "MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT",
        _coerce_optional_int,
    ),
    "eviction_low_watermark_percent": (
        "MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT",
        _coerce_optional_int,
    ),
    "route_control": ("MC_STORE_RS_ROUTE_CONTROL", _coerce_optional_str),
}

_DUMMY_SETUP_ENV_DEFAULT_KEYS = ("keyspace",)


def _apply_setup_env_defaults(config: Mapping[str, object]) -> dict:
    merged = dict(config)
    for key, (env_name, coerce) in _SETUP_ENV_DEFAULTS.items():
        if _has_value(merged.get(key)):
            continue
        if key == "initial_state" and _has_value(merged.get("state")):
            continue
        if key == "transport_rpc_port" and _has_value(merged.get("rpc_server_port")):
            continue
        env_value = os.environ.get(env_name)
        if not _has_value(env_value):
            continue
        merged[key] = coerce(env_value)

    if not _has_value(merged.get("labels")):
        labels = _labels_from_env()
        if labels:
            merged["labels"] = labels
    return merged


def _apply_dummy_setup_env_defaults(config: Mapping[str, object]) -> dict:
    merged = dict(config)
    for key in _DUMMY_SETUP_ENV_DEFAULT_KEYS:
        if _has_value(merged.get(key)):
            continue
        env_name, coerce = _SETUP_ENV_DEFAULTS[key]
        env_value = os.environ.get(env_name)
        if not _has_value(env_value):
            continue
        merged[key] = coerce(env_value)
    return merged


def _has_value(value) -> bool:
    return value not in (None, "")


def _labels_from_env() -> dict[str, str] | None:
    value = os.environ.get("MC_STORE_RS_LABELS")
    if not _has_value(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("{"):
        decoded = json.loads(text)
        return _coerce_mapping(decoded)

    labels = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        key, separator, label_value = item.partition("=")
        if not separator or not key.strip():
            raise ValueError(
                "MC_STORE_RS_LABELS must be a JSON object or comma-separated key=value pairs"
            )
        labels[key.strip()] = label_value.strip()
    return labels or None


def _start_metrics_server_from_env() -> str | None:
    bind_addr = os.environ.get("MC_STORE_RS_METRICS_ADDR")
    if not _has_value(bind_addr):
        return None
    existing = _native.metrics_server_address()
    if existing is not None:
        return existing
    return _native.start_metrics_server(str(bind_addr).strip())


def _init_tracing_from_env() -> bool:
    if not _env_truthy(os.environ.get("MC_STORE_RS_TRACE")) and not _has_value(
        os.environ.get("MC_STORE_RS_TRACE_FILE")
    ):
        return False
    trace_filter = _coerce_optional_str(os.environ.get("MC_STORE_RS_TRACE_FILTER"))
    _native.init_tracing(trace_filter)
    return True


def _env_truthy(value) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_raw_multi_buffer_args(
    keys: Sequence[str],
    all_buffer_ptrs: Sequence[Sequence[int]],
    all_sizes: Sequence[Sequence[int]],
):
    if len(keys) != len(all_buffer_ptrs) or len(keys) != len(all_sizes):
        raise ValueError(
            "keys, all_buffer_ptrs, and all_sizes must have the same length"
        )
    return (
        list(keys),
        [[int(ptr) for ptr in group] for group in all_buffer_ptrs],
        [[int(size) for size in group] for group in all_sizes],
    )


def _normalize_descriptor_items(items: Iterable[tuple]):
    keys = []
    all_buffer_ptrs = []
    all_sizes = []
    for item in items:
        if len(item) == 2:
            key, descriptors = item
            ptrs = []
            sizes = []
            for descriptor in descriptors:
                if len(descriptor) != 2:
                    raise TypeError(
                        "multi-buffer descriptors must be (buffer_ptr, size) pairs"
                    )
                buffer_ptr, size = descriptor
                ptrs.append(int(buffer_ptr))
                sizes.append(int(size))
        elif len(item) == 3:
            key, ptrs, sizes = item
            ptrs = [int(ptr) for ptr in ptrs]
            sizes = [int(size) for size in sizes]
        else:
            raise TypeError(
                "multi-buffer items must be (key, descriptors) or (key, ptrs, sizes)"
            )
        keys.append(key)
        all_buffer_ptrs.append(ptrs)
        all_sizes.append(sizes)
    return keys, all_buffer_ptrs, all_sizes


def _items_use_bytes_payloads(items: Sequence[tuple]) -> bool:
    if not items:
        return True
    first = items[0]
    if len(first) != 2:
        return False
    _, buffers = first
    if not buffers:
        return True
    sample = buffers[0]
    return isinstance(sample, (bytes, bytearray, memoryview))


# Parallelism types from native module (optional — absent in older wheels)
try:
    ParallelAxis = _native.ParallelAxis
    TensorParallelism = _native.TensorParallelism
    ReadTarget = _native.ReadTarget
    AXIS_DP = _native.AXIS_DP()
    AXIS_TP = _native.AXIS_TP()
    AXIS_EP = _native.AXIS_EP()
    AXIS_PP = _native.AXIS_PP()
    READ_MODE_AS_STORED = _native.READ_MODE_AS_STORED()
    READ_MODE_SHARD = _native.READ_MODE_SHARD()
    READ_MODE_FULL = _native.READ_MODE_FULL()
except AttributeError:
    pass

__all__ = [
    "MooncakeDistributedStore",
    "MooncakeHostMemAllocator",
    "ReplicateConfig",
    "init_tracing",
    "metrics_text",
    "start_metrics_server",
    "stop_metrics_server",
    "metrics_server_address",
]

_PARALLEL_EXPORTS = [
    "ParallelAxis",
    "TensorParallelism",
    "ReadTarget",
    "AXIS_DP",
    "AXIS_TP",
    "AXIS_EP",
    "AXIS_PP",
    "READ_MODE_AS_STORED",
    "READ_MODE_SHARD",
    "READ_MODE_FULL",
]
for _name in _PARALLEL_EXPORTS:
    if _name in globals():
        __all__.append(_name)
