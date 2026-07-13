from __future__ import annotations

import logging
import os
import posixpath
from importlib import import_module
from typing import Any


LOGGER = logging.getLogger(__name__)

INVALID_PARAMS = -600
FILE_NOT_FOUND = -1100
PERSISTENT_FAIL = -1503

_LOCAL_FILESYSTEM_SCHEMES = {"file", ""}
_OBJECT_STORE_SCHEMES = {
    "s3",
    "gs",
    "gcs",
    "abfs",
    "adl",
    "oss",
    "http",
    "https",
    "ftp",
}


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


def _filesystem_scheme(
    file_name: os.PathLike[str] | str, filesystem: str | None
) -> str:
    target_url = _build_target_url(file_name, filesystem)
    if "://" not in target_url:
        return "file"
    return target_url.split("://", 1)[0].lower()


def _is_remote_target(
    file_name: os.PathLike[str] | str, filesystem: str | None
) -> bool:
    return _filesystem_scheme(file_name, filesystem) not in _LOCAL_FILESYSTEM_SCHEMES


def _supports_directory_creation(fs, scheme: str) -> bool:
    if scheme in _LOCAL_FILESYSTEM_SCHEMES:
        return True

    protocol = getattr(fs, "protocol", scheme)
    if isinstance(protocol, (tuple, list)):
        protocol = protocol[0] if protocol else scheme
    return str(protocol).lower() in _LOCAL_FILESYSTEM_SCHEMES


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


def _ensure_parent_dir(
    fs,
    path: str,
    *,
    file_name: os.PathLike[str] | str,
    filesystem: str | None,
) -> None:
    """Create parent directories for local filesystem backends only."""
    scheme = _filesystem_scheme(file_name, filesystem)
    if not _supports_directory_creation(fs, scheme):
        return

    parent_dir = posixpath.dirname(path)
    if not parent_dir:
        return

    try:
        fs.makedirs(parent_dir, exist_ok=True)
    except (AttributeError, NotImplementedError):
        pass
    except OSError as exc:
        LOGGER.warning("Failed to create directory %s: %s", parent_dir, exc)


def _write_bytes(
    file_name: os.PathLike[str] | str,
    payload: bytes,
    filesystem: str | None,
    storage_options: dict[str, Any] | None,
) -> None:
    fs, path = _open_fs_target(file_name, filesystem, storage_options)
    _ensure_parent_dir(fs, path, file_name=file_name, filesystem=filesystem)

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

    if len(loaded_tensors) == 1:
        only_name = next(iter(loaded_tensors))
        if preferred_name or fallback_name:
            LOGGER.warning(
                "Tensor entry %s was not found in file; using sole entry %s",
                preferred_name or fallback_name,
                only_name,
            )
        return loaded_tensors[only_name]

    requested = preferred_name or fallback_name or "<unspecified>"
    available = ", ".join(sorted(loaded_tensors))
    raise ValueError(
        f"Tensor entry {requested!r} was not found in safetensors file; "
        f"available entries: {available}"
    )


def _coerce_loaded_tensor(loaded: Any) -> Any:
    try:
        import torch
    except ImportError as exc:
        raise ValueError(
            "torch package is required for the 'torch' format. "
            "Install with: pip install torch"
        ) from exc

    if isinstance(loaded, torch.Tensor):
        return loaded

    raise ValueError(
        "torch checkpoint must contain a single tensor; got "
        f"{type(loaded).__name__}. Use format='safetensors' for remote "
        "artifacts or save a raw tensor checkpoint."
    )


def _torch_load_from_handle(handle, map_location: Any, weights_only: bool) -> Any:
    import inspect

    try:
        import torch
    except ImportError as exc:
        raise ValueError(
            "torch package is required for the 'torch' format. "
            "Install with: pip install torch"
        ) from exc

    load_params = inspect.signature(torch.load).parameters
    if weights_only and "weights_only" not in load_params:
        raise ValueError(
            "Installed torch does not support torch.load(weights_only=...); "
            "upgrade to torch >= 2.0 or use format='safetensors'."
        )

    try:
        loaded = torch.load(
            handle,
            map_location=map_location,
            weights_only=weights_only,
        )
    except TypeError as exc:
        if weights_only:
            raise ValueError(
                "torch.load rejected weights_only=True; use format='safetensors' "
                "for untrusted artifacts."
            ) from exc
        loaded = torch.load(handle, map_location=map_location)

    return _coerce_loaded_tensor(loaded)


def _apply_map_location(tensor, map_location):
    """Move a tensor (or dict of tensors) to the device given by *map_location*."""
    if map_location is None:
        return tensor
    if isinstance(tensor, dict):
        return {k: v.to(map_location) for k, v in tensor.items()}
    if hasattr(tensor, "to"):
        return tensor.to(map_location)
    return tensor


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
        fs, path = _open_fs_target(resolved_file_name, filesystem, storage_options)
        _ensure_parent_dir(
            fs,
            path,
            file_name=resolved_file_name,
            filesystem=filesystem,
        )

        if format_name == "torch":
            try:
                import torch
            except ImportError:
                raise ValueError(
                    "torch package is required for the 'torch' format. "
                    "Install with: pip install torch"
                )
            # Stream directly to the fsspec file handle -- avoids
            # materialising the entire serialised tensor as an in-memory
            # bytes object.
            with fs.open(path, "wb") as handle:
                torch.save(tensor, handle)
        else:
            # safetensors.torch.save() returns bytes; write them straight
            # to the open handle to avoid an extra copy through _write_bytes.
            try:
                from safetensors.torch import save as safetensors_save
            except ImportError:
                raise ValueError(
                    "safetensors package is required for the 'safetensors' "
                    "format. Install with: pip install safetensors"
                )
            payload = safetensors_save({resolved_tensor_name: tensor})
            with fs.open(path, "wb") as handle:
                handle.write(payload)

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
    weights_only: bool = True,
    allow_unsafe_remote_torch_load: bool = False,
):
    if file_name is None:
        LOGGER.error("file_name must be provided when loading a tensor")
        return None

    target_store_key = os.fspath(file_name) if key is None else key

    try:
        format_name = _normalize_format(format, os.fspath(file_name))
        if (
            format_name == "torch"
            and _is_remote_target(file_name, filesystem)
            and not allow_unsafe_remote_torch_load
        ):
            scheme = _filesystem_scheme(file_name, filesystem)
            raise ValueError(
                f"Refusing to torch.load from remote scheme {scheme!r}. "
                "Use format='safetensors' for remote artifacts or pass "
                "allow_unsafe_remote_torch_load=True only for trusted sources."
            )

        fs, path = _open_fs_target(file_name, filesystem, storage_options)

        if format_name == "torch":
            with fs.open(path, "rb") as handle:
                tensor = _torch_load_from_handle(
                    handle,
                    map_location=map_location,
                    weights_only=weights_only,
                )
        else:
            try:
                from safetensors.torch import load as safetensors_load
            except ImportError:
                raise ValueError(
                    "safetensors package is required for the 'safetensors' "
                    "format. Install with: pip install safetensors"
                )
            with fs.open(path, "rb") as handle:
                payload = handle.read()
            loaded_tensors = safetensors_load(payload)
            tensor = _pick_tensor_entry(
                loaded_tensors, tensor_name, target_store_key
            )
            tensor = _apply_map_location(tensor, map_location)

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
    except ValueError:
        LOGGER.exception("Invalid tensor file request for %s", file_name)
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
    except (ImportError, OSError):
        # ImportError / OSError covers cases where the native extension
        # exists but fails to load (e.g. missing shared libraries).
        # Returning here keeps the file-IO patch best-effort without
        # breaking the package import.
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
