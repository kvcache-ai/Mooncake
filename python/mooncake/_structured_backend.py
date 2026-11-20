"""Backend contracts used by the shared structured-object transport."""

from __future__ import annotations

import ctypes
import importlib
from typing import Any, Sequence


def cpp_tensor_codec_module():
    try:
        return importlib.import_module("mooncake._store")
    except ModuleNotFoundError as exc:
        if exc.name != "mooncake._store":
            raise
        return None


def tensor_codec_helper(name: str) -> Any:
    module = cpp_tensor_codec_module()
    helper = None if module is None else getattr(module, name, None)
    if not callable(helper):
        raise RuntimeError(
            "the C++ Store tensor codec is required for this structured tensor operation"
        )
    return helper


def tensor_metadata_size() -> int:
    module = cpp_tensor_codec_module()
    helper = None if module is None else getattr(module, "_tensor_metadata_size", None)
    return int(helper()) if callable(helper) else 0


def deserialize_tensor_payload(payload: bytes) -> Any:
    return tensor_codec_helper("_deserialize_tensor")(payload)


def slice_tensor_metadata(
    metadata: bytes, shape: Sequence[int], data_bytes: int
) -> bytes:
    patched = bytearray(metadata)
    ctypes.c_uint64.from_buffer(patched, 32).value = int(data_bytes)
    global_shape_offset = 40
    local_shape_offset = global_shape_offset + 8 * 8
    for index, dim in enumerate(shape):
        ctypes.c_int64.from_buffer(
            patched, global_shape_offset + index * 8
        ).value = int(dim)
        ctypes.c_int64.from_buffer(patched, local_shape_offset + index * 8).value = int(
            dim
        )
    return bytes(patched)


class StructuredStoreBackend:
    """Adapt native operations without owning bundle or manifest policy."""

    prefer_registered_put = False
    supports_storage_groups = True

    def supports_native_tensor(self, tensor: Any) -> bool:
        return True

    def validate_write_config(self, config: Any) -> None:
        pass

    def __init__(self, store: Any) -> None:
        self.store = store
        self.get_into_ranges = getattr(store, "get_into_ranges", None)

    def pointer_put(self, config: Any = None):
        put_tensor_from = getattr(self.store, "put_tensor_from", None)
        if config is None and callable(put_tensor_from):
            return put_tensor_from
        return getattr(self.store, "put_from", None)

    def tensor_put(self, config: Any = None):
        return getattr(
            self.store, "put_tensor" if config is None else "pub_tensor", None
        )

    def get_tensor_into(self, key: str, ptr: int, size: int):
        get_tensor_into = getattr(self.store, "get_tensor_into", None)
        if not callable(get_tensor_into):
            raise RuntimeError(
                "structured tensor payload does not support get_tensor_into"
            )
        return get_tensor_into(key, ptr, size)

    def existence_status(self, key: str) -> int | None:
        is_exist = getattr(self.store, "is_exist", None)
        if not callable(is_exist):
            return None
        status = is_exist(key)
        if status not in (0, 1):
            raise RuntimeError(f"is_exist failed for {key}: {status}")
        return int(status)

    def create_buffer_pool(self):
        get_capsule = getattr(self.store, "_get_pyclient_capsule", None)
        if not callable(get_capsule) or get_capsule() is None:
            return None
        from mooncake.buffer_pool import BufferPool

        return BufferPool(self.store)

    tensor_metadata_size = staticmethod(tensor_metadata_size)
    deserialize_tensor = staticmethod(deserialize_tensor_payload)


def structured_store_backend(store: Any) -> StructuredStoreBackend:
    factory = getattr(store, "_structured_object_backend", None)
    return factory() if callable(factory) else StructuredStoreBackend(store)
