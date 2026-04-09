from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import ctypes
import importlib.machinery
import importlib.util
import mmap
import pathlib
import sys
from dataclasses import dataclass, field


def _load_native():
    package_dir = pathlib.Path(__file__).resolve().parent
    repo_root = package_dir.parent.parent
    _preload_upstream_libraries(package_dir)
    try:
        from . import _store_rs as native  # type: ignore

        return native
    except Exception:
        target_root = repo_root / "target"
        suffixes = list(importlib.machinery.EXTENSION_SUFFIXES) + [".so"]
        patterns = []
        for suffix in suffixes:
            patterns.extend([f"_store_rs{suffix}", f"lib_store_rs{suffix}"])

        for profile in ("debug", "release"):
            for profile_dir in (target_root / profile, target_root / profile / "deps"):
                for pattern in patterns:
                    candidate = profile_dir / pattern
                    if not candidate.exists():
                        continue
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


def _preload_upstream_libraries(package_dir: pathlib.Path) -> None:
    if (package_dir.parent / "mooncake_store_rs.libs").is_dir():
        return
    for library in _native_library_candidates(package_dir):
        if library.exists():
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)


def _native_library_candidates(package_dir: pathlib.Path) -> list[pathlib.Path]:
    roots = [
        package_dir,
        package_dir / "lib",
        package_dir.parent,
        package_dir.parent.parent,
    ]
    relative_paths = [
        pathlib.Path("libtransfer_engine.so"),
        pathlib.Path("libtent_shared.so"),
        pathlib.Path("third_party")
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "src"
        / "libtransfer_engine.so",
        pathlib.Path("third_party")
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "tent"
        / "src"
        / "libtent_shared.so",
    ]
    candidates: list[pathlib.Path] = []
    seen: set[pathlib.Path] = set()
    for root in roots:
        for relative_path in relative_paths:
            candidate = (root / relative_path).resolve()
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


_native = _load_native()


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
        self._registered_buffers: dict[int, int] = {}
        self._tracked_keys: set[tuple[str | None, str]] = set()

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    def setup(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], Mapping) and not kwargs:
            return self._setup_from_config_dict(dict(args[0]))
        if len(args) > 8:
            raise TypeError("setup accepts at most 8 positional arguments")
        if len(args) == 8:
            args = args[:7]
        kwargs.pop("engine", None)
        if "hugepage_size" in kwargs:
            kwargs["hugepage_size"] = _normalize_hugepage_size(kwargs["hugepage_size"])
        return self._native.setup(*args, **kwargs)

    def close(self) -> None:
        self._registered_buffers.clear()
        self._tracked_keys.clear()
        self._native.close()

    def register_buffer(self, buffer_ptr: int, size: int):
        result = self._native.register_buffer(buffer_ptr, size)
        if result == 0:
            self._registered_buffers[int(buffer_ptr)] = int(size)
        return result

    def unregister_buffer(self, buffer_ptr: int, size: int | None = None):
        resolved_size = self._resolve_registered_size(buffer_ptr, size)
        result = self._native.unregister_buffer(buffer_ptr, resolved_size)
        if result == 0:
            self._registered_buffers.pop(int(buffer_ptr), None)
        return result

    def put(self, key: str, value, *, tenant: str | None = None, config=None):
        result = self._native.put(
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
        result = self._native.put_from(
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
        result = self._native.batch_put(
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
            result = self._native.batch_put_from(
                items,
                tenant=tenant,
                **_replication_kwargs(config),
            )
            self._track_keys([key for key, _, _ in items], tenant=tenant)
            return _coerce_batch_status_result(result, len(items))
        if len(args) == 3:
            keys, buffer_ptrs, sizes = _normalize_raw_batch_args(*args)
            result = self._native.batch_put_from_raw(
                keys,
                buffer_ptrs,
                sizes,
                tenant=tenant,
                **_replication_kwargs(config),
            )
            self._track_keys(keys, tenant=tenant)
            return _coerce_batch_status_result(result, len(keys))
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
                return self._native.batch_put_from_multi_buffers(
                    items,
                    tenant=tenant,
                    **_replication_kwargs(config),
                )
            keys, all_buffer_ptrs, all_sizes = _normalize_descriptor_items(items)
            return self._native.batch_put_from_multi_buffers_raw(
                keys,
                all_buffer_ptrs,
                all_sizes,
                tenant=tenant,
                **_replication_kwargs(config),
            )
        if len(args) == 3:
            keys, all_buffer_ptrs, all_sizes = _normalize_raw_multi_buffer_args(*args)
            return self._native.batch_put_from_multi_buffers_raw(
                keys,
                all_buffer_ptrs,
                all_sizes,
                tenant=tenant,
                **_replication_kwargs(config),
            )
        raise TypeError(
            "batch_put_from_multi_buffers expects items or (keys, all_buffer_ptrs, all_sizes)"
        )

    def batch_get(self, keys: Sequence[str], *, tenant: str | None = None):
        return self._native.batch_get(list(keys), tenant=tenant)

    def batch_get_buffer(self, keys: Sequence[str], *, tenant: str | None = None):
        return self._native.batch_get_buffer(list(keys), tenant=tenant)

    def batch_get_into(self, *args, tenant: str | None = None):
        if len(args) == 1:
            return self._native.batch_get_into(args[0], tenant=tenant)
        if len(args) == 3:
            keys, buffer_ptrs, sizes = _normalize_raw_batch_args(*args)
            return self._native.batch_get_into_raw(
                keys, buffer_ptrs, sizes, tenant=tenant
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
            keys, all_buffer_ptrs, all_sizes = _normalize_descriptor_items(list(args[0]))
        elif len(args) == 3:
            keys, all_buffer_ptrs, all_sizes = _normalize_raw_multi_buffer_args(*args)
        else:
            raise TypeError(
                "batch_get_into_multi_buffers expects items or (keys, all_buffer_ptrs, all_sizes)"
            )
        return self._native.batch_get_into_multi_buffers(
            keys,
            all_buffer_ptrs,
            all_sizes,
            prefer_alloc_in_same_node=prefer_alloc_in_same_node,
            tenant=tenant,
        )

    def remove(self, key: str, *, force: bool = False, tenant: str | None = None):
        result = self._native.remove(key, force=force, tenant=tenant)
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
        result = self._native.batch_remove(key_list, force=force, tenant=tenant)
        for key, status in zip(key_list, result):
            if int(status) == 0:
                self._forget_keys([key], tenant=tenant)
        return result

    def remove_all(self, force: bool = False):
        if hasattr(self._native, "remove_all"):
            try:
                removed = self._native.remove_all(force=force)
                if int(removed) >= 0:
                    self._tracked_keys.clear()
                return removed
            except Exception:
                pass
        removed = 0
        for tenant, keys in self._group_tracked_keys().items():
            statuses = self._native.batch_remove(keys, force=force, tenant=tenant)
            for key, status in zip(keys, statuses):
                if int(status) == 0:
                    self._forget_keys([key], tenant=tenant)
                    removed += 1
        return removed

    def start_metrics_server(self, bind_addr: str = "127.0.0.1:0") -> str:
        return self._native.start_metrics_server(bind_addr)

    def stop_metrics_server(self) -> None:
        self._native.stop_metrics_server()

    def metrics_server_address(self) -> str | None:
        return self._native.metrics_server_address()

    def _resolve_registered_size(self, buffer_ptr: int, size: int | None) -> int:
        if size is not None:
            return int(size)
        if int(buffer_ptr) not in self._registered_buffers:
            raise ValueError(
                "buffer size is required for unknown pointers; call register_buffer first or pass size explicitly"
            )
        return self._registered_buffers[int(buffer_ptr)]

    def _track_keys(
        self,
        keys: Iterable[str],
        *,
        tenant: str | None = None,
    ) -> None:
        for key in keys:
            self._tracked_keys.add((tenant, str(key)))

    def _forget_keys(
        self,
        keys: Iterable[str],
        *,
        tenant: str | None = None,
    ) -> None:
        for key in keys:
            self._tracked_keys.discard((tenant, str(key)))

    def _group_tracked_keys(self) -> dict[str | None, list[str]]:
        grouped: dict[str | None, list[str]] = {}
        for tenant, key in self._tracked_keys:
            grouped.setdefault(tenant, []).append(key)
        return grouped

    def _setup_from_config_dict(self, config: Mapping[str, object]):
        if "local_hostname" not in config:
            raise TypeError("setup config requires `local_hostname`")
        metadata_url = config.get("metadata_server", config.get("metadata_url"))
        if metadata_url is None:
            raise TypeError("setup config requires `metadata_server`")
        return self._native.setup(
            str(config["local_hostname"]),
            str(metadata_url),
            _coerce_int(config.get("global_segment_size"), 16 * 1024 * 1024),
            _coerce_int(config.get("local_buffer_size"), 16 * 1024 * 1024),
            str(config.get("protocol", "tcp")),
            str(config.get("rdma_devices", "")),
            str(config.get("master_server_addr", config.get("master_server", ""))),
            stable_id=_coerce_optional_str(config.get("stable_id")),
            tenant=str(config.get("tenant", "default")),
            labels=_coerce_mapping(config.get("labels")),
            routed_writes=_coerce_bool(config.get("routed_writes"), False),
            replica_count=_coerce_int(config.get("replica_count"), 1),
            keyspace=_coerce_optional_str(config.get("keyspace")),
            transport_metadata_url=_coerce_optional_str(
                config.get("transport_metadata_url")
            ),
            local_segment_name=_coerce_optional_str(config.get("local_segment_name")),
            expires_at_ms=_coerce_optional_int(config.get("expires_at_ms")),
            use_hugepage=_coerce_optional_bool(config.get("use_hugepage")),
            hugepage_size=_normalize_hugepage_size(config.get("hugepage_size")),
        )


def _replication_kwargs(config) -> dict:
    if config is None:
        return {}
    preferred_segments = list(getattr(config, "preferred_segments", []) or [])
    preferred_segment = getattr(config, "preferred_segment", "")
    if preferred_segment not in ("", None) and preferred_segment not in preferred_segments:
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
        if result != 0:
            raise RuntimeError(f"batch put-from failed with status {result}")
        return _BatchStatusResult([0] * count)
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
