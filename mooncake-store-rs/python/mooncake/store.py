from __future__ import annotations

import ctypes
import importlib.machinery
import importlib.util
import pathlib
import sys
from dataclasses import dataclass, field
from typing import Iterable, Sequence


def _load_native():
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    _preload_upstream_libraries(repo_root)
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


def _preload_upstream_libraries(repo_root: pathlib.Path) -> None:
    build_root = repo_root / "third_party" / "Mooncake" / "build-rust" / "mooncake-transfer-engine"
    libraries = [
        build_root / "src" / "libtransfer_engine.so",
        build_root / "tent" / "src" / "libtent_shared.so",
    ]
    for library in libraries:
        if library.exists():
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)


_native = _load_native()


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

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    def setup(self, *args, **kwargs):
        return self._native.setup(*args, **kwargs)

    def close(self) -> None:
        self._registered_buffers.clear()
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
        return self._native.put(key, value, tenant=tenant, **_replication_kwargs(config))

    def put_from(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        *,
        tenant: str | None = None,
        config=None,
    ):
        return self._native.put_from(
            key,
            buffer_ptr,
            size,
            tenant=tenant,
            **_replication_kwargs(config),
        )

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
        return self._native.batch_put(
            normalized,
            tenant=tenant,
            **_replication_kwargs(config),
        )

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
            return self._native.batch_put_from(
                args[0],
                tenant=tenant,
                **_replication_kwargs(config),
            )
        if len(args) == 3:
            keys, buffer_ptrs, sizes = _normalize_raw_batch_args(*args)
            return self._native.batch_put_from_raw(
                keys,
                buffer_ptrs,
                sizes,
                tenant=tenant,
                **_replication_kwargs(config),
            )
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
        return self._native.remove(key, force=force, tenant=tenant)

    def batch_remove(
        self,
        keys: Sequence[str],
        *,
        force: bool = False,
        tenant: str | None = None,
    ):
        return self._native.batch_remove(list(keys), force=force, tenant=tenant)

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
    "ReplicateConfig",
    "init_tracing",
    "metrics_text",
    "start_metrics_server",
    "stop_metrics_server",
    "metrics_server_address",
]
