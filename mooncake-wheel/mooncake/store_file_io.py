from __future__ import annotations

import io
import logging
import os
import posixpath
from importlib import import_module
from typing import Any


LOGGER = logging.getLogger(__name__)

INVALID_PARAMS = -600
FILE_NOT_FOUND = -1100
PERSISTENT_FAIL = -1503


def _normalize_format(format_name: str, file_name: str) -> str:
    if format_name is None:
        format_name = "auto"

    normalized = format_name.lower().replace("-", "_")
    if normalized == "auto":
        if str(file_name).lower().endswith(".safetensors"):
            return "safetensors"
        return "torch"

    if normalized in {"safetensors", "safe_tensors"}:
        return "safetensors"
    if normalized in {"torch", "pt", "pth", "standard", "standard_file"}:
        return "torch"

    raise ValueError(f"Unsupported file format: {format_name}")


def _normalize_filesystem(filesystem: str | None) -> str:
    if filesystem is None:
        return "auto"

    normalized = filesystem.lower().replace("-", "_")
    if normalized in {"local", "localfs", "posix", "file"}:
        return "file"
    return normalized


def _build_target_url(file_name: os.PathLike[str] | str, filesystem: str | None) -> str:
    target = os.fspath(file_name)
    if not target:
        raise ValueError("file_name must not be empty")

    normalized_fs = _normalize_filesystem(filesystem)
    if normalized_fs in {"auto", "file"} or "://" in target:
        return target

    return f"{normalized_fs}://{target}"


def _open_fs_target(
    file_name: os.PathLike[str] | str,
    filesystem: str | None,
    storage_options: dict[str, Any] | None,
):
    import fsspec

    target_url = _build_target_url(file_name, filesystem)
    options = dict(storage_options or {})
    fs, path = fsspec.core.url_to_fs(target_url, **options)
    return fs, path


def _write_bytes(
    file_name: os.PathLike[str] | str,
    payload: bytes,
    filesystem: str | None,
    storage_options: dict[str, Any] | None,
) -> None:
    fs, path = _open_fs_target(file_name, filesystem, storage_options)
    parent_dir = posixpath.dirname(path)
    if parent_dir:
        try:
            fs.makedirs(parent_dir, exist_ok=True)
        except (AttributeError, NotImplementedError, OSError):
            pass

    with fs.open(path, "wb") as handle:
        handle.write(payload)


def _read_bytes(
    file_name: os.PathLike[str] | str,
    filesystem: str | None,
    storage_options: dict[str, Any] | None,
) -> bytes:
    fs, path = _open_fs_target(file_name, filesystem, storage_options)
    with fs.open(path, "rb") as handle:
        return handle.read()


def _serialize_tensor(tensor: Any, format_name: str, tensor_name: str) -> bytes:
    if format_name == "safetensors":
        from safetensors.torch import save as safetensors_save

        return safetensors_save({tensor_name: tensor})

    import torch

    buffer = io.BytesIO()
    torch.save(tensor, buffer)
    return buffer.getvalue()


def _pick_tensor_entry(
    loaded_tensors: dict[str, Any],
    preferred_name: str | None,
    fallback_name: str | None,
) -> Any:
    if not loaded_tensors:
        raise ValueError("No tensors found in the file")

    if preferred_name and preferred_name in loaded_tensors:
        return loaded_tensors[preferred_name]

    if fallback_name and fallback_name in loaded_tensors:
        return loaded_tensors[fallback_name]

    first_name = next(iter(loaded_tensors))
    if preferred_name or fallback_name:
        LOGGER.warning(
            "Tensor entry %s was not found in file; using %s instead",
            preferred_name or fallback_name,
            first_name,
        )
    return loaded_tensors[first_name]


def _deserialize_tensor(
    payload: bytes,
    format_name: str,
    tensor_name: str | None,
    store_key: str | None,
    map_location: Any,
) -> Any:
    if format_name == "safetensors":
        from safetensors.torch import load as safetensors_load

        loaded_tensors = safetensors_load(payload)
        return _pick_tensor_entry(loaded_tensors, tensor_name, store_key)

    import torch

    return torch.load(io.BytesIO(payload), map_location=map_location)


def _save_tensor_to_file(
    self,
    key: str,
    file_name: os.PathLike[str] | str | None = None,
    format: str = "auto",
    filesystem: str = "auto",
    storage_options: dict[str, Any] | None = None,
    tensor_name: str | None = None,
) -> int:
    resolved_file_name = key if file_name is None else file_name
    resolved_tensor_name = key if tensor_name is None else tensor_name

    try:
        tensor = self.get_tensor(key)
        if tensor is None:
            LOGGER.error("Failed to fetch tensor for key: %s", key)
            return FILE_NOT_FOUND

        format_name = _normalize_format(format, os.fspath(resolved_file_name))
        payload = _serialize_tensor(tensor, format_name, resolved_tensor_name)
        _write_bytes(resolved_file_name, payload, filesystem, storage_options)
        return 0
    except FileNotFoundError:
        LOGGER.exception("File path not found while saving tensor for key %s", key)
        return FILE_NOT_FOUND
    except (TypeError, ValueError):
        LOGGER.exception("Invalid parameters while saving tensor for key %s", key)
        return INVALID_PARAMS
    except Exception:
        LOGGER.exception("Failed to persist tensor for key %s", key)
        return PERSISTENT_FAIL


def _load_tensor_from_file(
    self,
    key: str | None = None,
    file_name: os.PathLike[str] | str | None = None,
    format: str = "auto",
    filesystem: str = "auto",
    storage_options: dict[str, Any] | None = None,
    tensor_name: str | None = None,
    map_location: Any = None,
):
    if file_name is None:
        LOGGER.error("file_name must be provided when loading a tensor")
        return None

    target_store_key = os.fspath(file_name) if key is None else key

    try:
        payload = _read_bytes(file_name, filesystem, storage_options)
        format_name = _normalize_format(format, os.fspath(file_name))
        tensor = _deserialize_tensor(
            payload, format_name, tensor_name, target_store_key, map_location
        )
        rc = self.put_tensor(target_store_key, tensor)
        if rc != 0:
            LOGGER.error(
                "Failed to store tensor for key %s, rc=%s", target_store_key, rc
            )
            return None
        return tensor
    except FileNotFoundError:
        LOGGER.exception("Tensor file %s was not found", file_name)
        return None
    except Exception:
        LOGGER.exception("Failed to load tensor from file %s", file_name)
        return None


def _save_tensor_to_safetensor(
    self,
    key: str,
    file_name: os.PathLike[str] | str | None = None,
    filesystem: str = "auto",
    storage_options: dict[str, Any] | None = None,
    tensor_name: str | None = None,
) -> int:
    return self.save_tensor_to_file(
        key,
        file_name=file_name,
        format="safetensors",
        filesystem=filesystem,
        storage_options=storage_options,
        tensor_name=tensor_name,
    )


def _load_tensor_from_safetensor(
    self,
    key: str | None = None,
    file_name: os.PathLike[str] | str | None = None,
    filesystem: str = "auto",
    storage_options: dict[str, Any] | None = None,
    tensor_name: str | None = None,
):
    return self.load_tensor_from_file(
        key=key,
        file_name=file_name,
        format="safetensors",
        filesystem=filesystem,
        storage_options=storage_options,
        tensor_name=tensor_name,
    )


def patch_store_file_io_support() -> None:
    try:
        store_module = import_module("mooncake.store")
    except ModuleNotFoundError:
        return

    store_cls = getattr(store_module, "MooncakeDistributedStore", None)
    if store_cls is None:
        return

    if getattr(store_cls, "_mooncake_file_io_patched", False):
        return

    store_cls.save_tensor_to_file = _save_tensor_to_file
    store_cls.load_tensor_from_file = _load_tensor_from_file
    store_cls.save_kv_cache_to_file = _save_tensor_to_file
    store_cls.load_kv_cache_from_file = _load_tensor_from_file
    store_cls.save_tensor_to_safetensor = _save_tensor_to_safetensor
    store_cls.load_tensor_from_safetensor = _load_tensor_from_safetensor
    store_cls._mooncake_file_io_patched = True
