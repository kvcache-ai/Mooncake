"""Mooncake Store adapter for immutable exact hidden-state cache objects.

The adapter uses registered-buffer ``put_from``/``get_into`` operations only;
there is no Python bytes payload path in the data plane.  A fixed-size manifest
is published after its tensor payload so a remote reader never needs an
unbounded or ambiguous metadata fetch before allocating its destination.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import re
import time
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

import torch

from .hidden_cache_key import HiddenStateCacheKeyV2


class HiddenStateStoreError(RuntimeError):
    """Raised when a Store-backed hidden-state object is unsafe to use."""


_SAFE_KEY_SEGMENT = re.compile(r"[^A-Za-z0-9_.-]+")
_MANIFEST_SCHEMA_V2 = "mooncake-hidden-state-store-v2"


def _safe_segment(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        raise HiddenStateStoreError("Store key component must not be empty")
    normalized = _SAFE_KEY_SEGMENT.sub("_", text).strip("._")
    if not normalized:
        raise HiddenStateStoreError(f"invalid Store key component: {value!r}")
    return normalized[:160]


def _tensor_nbytes(tensor: torch.Tensor) -> int:
    return int(tensor.numel() * tensor.element_size())


def _tensor_checksum(tensor: torch.Tensor) -> str:
    """Checksum an already CPU-resident Store payload, never a lookup input."""

    if tensor.device.type != "cpu":
        raise HiddenStateStoreError("checksum requires a CPU Store payload")
    view = tensor.detach().contiguous().view(torch.uint8)
    return hashlib.sha256(view.numpy().tobytes()).hexdigest()


def _dtype_from_text(value: str) -> torch.dtype:
    name = str(value).strip()
    if name.startswith("torch."):
        name = name[len("torch.") :]
    dtype = getattr(torch, name, None)
    if not isinstance(dtype, torch.dtype):
        raise HiddenStateStoreError(f"unsupported hidden-state dtype: {value}")
    return dtype


@dataclass(frozen=True)
class HiddenStateStoreManifestV2:
    key_digest: str
    payload_key: str
    shape: tuple[int, ...]
    dtype: str
    nbytes: int
    payload_checksum: str
    lease_deadline_unix: float
    created_at_unix: float
    schema: str = _MANIFEST_SCHEMA_V2
    manifest_checksum: str = ""

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "key_digest": self.key_digest,
            "payload_key": self.payload_key,
            "shape": list(self.shape),
            "dtype": self.dtype,
            "nbytes": int(self.nbytes),
            "payload_checksum": self.payload_checksum,
            "lease_deadline_unix": float(self.lease_deadline_unix),
            "created_at_unix": float(self.created_at_unix),
        }

    def checksum(self) -> str:
        raw = json.dumps(
            self.canonical_payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def as_payload(self) -> dict[str, Any]:
        return {**self.canonical_payload(), "manifest_checksum": self.manifest_checksum}

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "HiddenStateStoreManifestV2":
        values = dict(payload)
        expected = {
            "schema",
            "key_digest",
            "payload_key",
            "shape",
            "dtype",
            "nbytes",
            "payload_checksum",
            "lease_deadline_unix",
            "created_at_unix",
            "manifest_checksum",
        }
        unknown = sorted(set(values) - expected)
        missing = sorted(expected - set(values))
        if unknown or missing:
            raise HiddenStateStoreError(
                "invalid hidden-state manifest fields"
                + (f" unknown={unknown}" if unknown else "")
                + (f" missing={missing}" if missing else "")
            )
        manifest = cls(
            schema=str(values["schema"]),
            key_digest=str(values["key_digest"]),
            payload_key=str(values["payload_key"]),
            shape=tuple(int(value) for value in values["shape"]),
            dtype=str(values["dtype"]),
            nbytes=int(values["nbytes"]),
            payload_checksum=str(values["payload_checksum"]),
            lease_deadline_unix=float(values["lease_deadline_unix"]),
            created_at_unix=float(values["created_at_unix"]),
            manifest_checksum=str(values["manifest_checksum"]),
        )
        manifest.validate()
        return manifest

    def validate(self) -> None:
        if self.schema != _MANIFEST_SCHEMA_V2:
            raise HiddenStateStoreError(f"unsupported hidden-state manifest schema: {self.schema}")
        if not self.key_digest or not self.payload_key:
            raise HiddenStateStoreError("hidden-state manifest is missing key identity")
        if not self.shape or any(int(value) <= 0 for value in self.shape):
            raise HiddenStateStoreError("hidden-state manifest shape must be positive")
        dtype = _dtype_from_text(self.dtype)
        expected_nbytes = int(torch.empty(self.shape, dtype=dtype).numel() * torch.empty((), dtype=dtype).element_size())
        if int(self.nbytes) != expected_nbytes:
            raise HiddenStateStoreError(
                f"hidden-state manifest nbytes mismatch: expected={expected_nbytes} got={self.nbytes}"
            )
        if len(self.payload_checksum) != 64:
            raise HiddenStateStoreError("hidden-state payload checksum must be sha256 hex")
        if self.manifest_checksum != self.checksum():
            raise HiddenStateStoreError("hidden-state manifest checksum mismatch")


class MooncakeHiddenStateStore:
    """Store-backed exact L2 cache for immutable hidden-state tensors."""

    def __init__(
        self,
        store: Any,
        *,
        key_prefix: str = "epd/hidden/v2",
        manifest_bytes: int = 16 * 1024,
        require_registered_buffers: bool = True,
    ) -> None:
        self.store = store
        self.key_prefix = "/".join(
            _safe_segment(part) for part in str(key_prefix).split("/") if part
        )
        if not self.key_prefix:
            raise ValueError("key_prefix is required")
        self.manifest_bytes = int(manifest_bytes)
        if self.manifest_bytes < 1024:
            raise ValueError("manifest_bytes must be at least 1024")
        self.require_registered_buffers = bool(require_registered_buffers)
        self._metrics: Dict[str, int] = {
            "puts": 0,
            "put_bytes": 0,
            "gets": 0,
            "hits": 0,
            "misses": 0,
            "get_bytes": 0,
            "expired": 0,
            "checksum_failures": 0,
            "invalid_manifests": 0,
            "gc_objects": 0,
            "errors": 0,
            "gpu_staging_bytes": 0,
        }

    def stats(self) -> Dict[str, int]:
        return dict(self._metrics)

    def put(
        self,
        key: HiddenStateCacheKeyV2,
        tensor: torch.Tensor,
        *,
        lease_seconds: float,
    ) -> HiddenStateStoreManifestV2:
        """Publish a tensor payload then its fixed-size manifest.

        Key construction never reads ``tensor``.  CUDA tensors are explicitly
        staged once for Store publication; the checksum is computed over that
        CPU staging buffer, not via a separate hot-path D2H hash.
        """

        if not isinstance(key, HiddenStateCacheKeyV2):
            raise TypeError("key must be HiddenStateCacheKeyV2")
        if str(tensor.dtype) != key.dtype:
            raise HiddenStateStoreError(
                f"cache-key dtype does not match tensor: key={key.dtype} tensor={tensor.dtype}"
            )
        if lease_seconds <= 0.0:
            raise ValueError("lease_seconds must be positive")
        storage = tensor.detach().to(device="cpu", copy=True).contiguous()
        if tensor.device.type != "cpu":
            self._metrics["gpu_staging_bytes"] += _tensor_nbytes(storage)
        payload_key, manifest_key = self._keys(key)
        now = time.time()
        manifest = HiddenStateStoreManifestV2(
            key_digest=key.digest,
            payload_key=payload_key,
            shape=tuple(int(value) for value in storage.shape),
            dtype=str(storage.dtype),
            nbytes=_tensor_nbytes(storage),
            payload_checksum=_tensor_checksum(storage),
            lease_deadline_unix=now + float(lease_seconds),
            created_at_unix=now,
        )
        manifest = HiddenStateStoreManifestV2(
            **{**asdict(manifest), "manifest_checksum": manifest.checksum()}
        )
        manifest_tensor = self._manifest_tensor(manifest)
        written: list[str] = []
        try:
            with ExitStack() as registrations:
                payload_ptr, payload_size = registrations.enter_context(
                    self._registered_tensor(storage)
                )
                self._put_from(payload_key, payload_ptr, payload_size)
                written.append(payload_key)
                manifest_ptr, manifest_size = registrations.enter_context(
                    self._registered_tensor(manifest_tensor)
                )
                self._put_from(manifest_key, manifest_ptr, manifest_size)
                written.append(manifest_key)
        except Exception:
            self._metrics["errors"] += 1
            self._remove_keys_best_effort(written)
            raise
        self._metrics["puts"] += 1
        self._metrics["put_bytes"] += int(manifest.nbytes)
        return manifest

    def get(
        self,
        key: HiddenStateCacheKeyV2,
        *,
        target_device: torch.device | str,
        target_dtype: Optional[torch.dtype] = None,
    ) -> Optional[torch.Tensor]:
        """Read and validate one exact L2 cache object, returning a fresh tensor."""

        if not isinstance(key, HiddenStateCacheKeyV2):
            raise TypeError("key must be HiddenStateCacheKeyV2")
        self._metrics["gets"] += 1
        payload_key, manifest_key = self._keys(key)
        try:
            manifest = self._read_manifest(manifest_key)
        except KeyError:
            self._metrics["misses"] += 1
            return None
        except Exception:
            self._metrics["invalid_manifests"] += 1
            self._metrics["errors"] += 1
            self._metrics["misses"] += 1
            self._remove_keys_best_effort([manifest_key, payload_key])
            return None
        if manifest.key_digest != key.digest or manifest.payload_key != payload_key:
            self._metrics["invalid_manifests"] += 1
            self._metrics["misses"] += 1
            self._remove_keys_best_effort([manifest_key, payload_key])
            return None
        if manifest.lease_deadline_unix <= time.time():
            self._metrics["expired"] += 1
            self._metrics["misses"] += 1
            self._remove_keys_best_effort([manifest_key, payload_key])
            return None
        try:
            dtype = _dtype_from_text(manifest.dtype)
            storage = torch.empty(manifest.shape, dtype=dtype, device="cpu")
            with self._registered_tensor(storage) as (pointer, size):
                if int(size) != int(manifest.nbytes):
                    raise HiddenStateStoreError("destination allocation does not match manifest nbytes")
                self._get_into(payload_key, pointer, size)
            if _tensor_checksum(storage) != manifest.payload_checksum:
                self._metrics["checksum_failures"] += 1
                self._metrics["misses"] += 1
                self._remove_keys_best_effort([manifest_key, payload_key])
                return None
        except Exception:
            self._metrics["errors"] += 1
            self._metrics["misses"] += 1
            return None
        self._metrics["hits"] += 1
        self._metrics["get_bytes"] += int(manifest.nbytes)
        output = storage.to(device=target_device, dtype=target_dtype, non_blocking=True)
        # ``storage`` is per-read, but clone the identity-preserving CPU case
        # as well so a caller never retains a mutable view into adapter memory.
        return output.clone() if output.data_ptr() == storage.data_ptr() else output

    def remove(self, key: HiddenStateCacheKeyV2) -> int:
        payload_key, manifest_key = self._keys(key)
        self._remove_keys([manifest_key, payload_key])
        self._metrics["gc_objects"] += 2
        return 2

    # ------------------------------------------------------------------
    # Store wire helpers
    # ------------------------------------------------------------------
    def _keys(self, key: HiddenStateCacheKeyV2) -> tuple[str, str]:
        root = f"{self.key_prefix}/{key.digest}"
        return f"{root}/payload", f"{root}/manifest"

    def _manifest_tensor(self, manifest: HiddenStateStoreManifestV2) -> torch.Tensor:
        raw = json.dumps(
            manifest.as_payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        if len(raw) >= self.manifest_bytes:
            raise HiddenStateStoreError(
                f"hidden-state manifest exceeds fixed record size: {len(raw)} >= {self.manifest_bytes}"
            )
        tensor = torch.zeros((self.manifest_bytes,), dtype=torch.uint8, device="cpu")
        ctypes.memmove(int(tensor.data_ptr()), raw, len(raw))
        return tensor

    def _read_manifest(self, manifest_key: str) -> HiddenStateStoreManifestV2:
        buffer = torch.empty((self.manifest_bytes,), dtype=torch.uint8, device="cpu")
        with self._registered_tensor(buffer) as (pointer, size):
            self._get_into(manifest_key, pointer, size)
        raw = ctypes.string_at(int(buffer.data_ptr()), self.manifest_bytes).split(b"\0", 1)[0]
        if not raw:
            raise HiddenStateStoreError("empty hidden-state manifest")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HiddenStateStoreError("invalid hidden-state manifest JSON") from exc
        if not isinstance(payload, Mapping):
            raise HiddenStateStoreError("hidden-state manifest must be a mapping")
        return HiddenStateStoreManifestV2.from_payload(payload)

    @contextmanager
    def _registered_tensor(self, tensor: torch.Tensor) -> Iterator[tuple[int, int]]:
        if tensor.device.type != "cpu" or not tensor.is_contiguous():
            raise HiddenStateStoreError("Store transfer requires contiguous CPU tensors")
        pointer = int(tensor.data_ptr())
        size = _tensor_nbytes(tensor)
        if pointer <= 0 or size <= 0:
            raise HiddenStateStoreError("invalid tensor pointer or byte size")
        register = getattr(self.store, "register_buffer", None)
        unregister = getattr(self.store, "unregister_buffer", None)
        registered_here = False
        if callable(register):
            status = register(pointer, size)
            if status not in (None, 0, -600):
                raise HiddenStateStoreError(f"register_buffer failed: {status}")
            registered_here = status in (None, 0)
        elif self.require_registered_buffers:
            raise HiddenStateStoreError("Store client lacks register_buffer")
        try:
            yield pointer, size
        finally:
            if registered_here and callable(unregister):
                status = unregister(pointer)
                if status not in (None, 0):
                    raise HiddenStateStoreError(f"unregister_buffer failed: {status}")

    def _put_from(self, key: str, pointer: int, size: int) -> None:
        batch = getattr(self.store, "batch_put_from", None)
        if callable(batch):
            statuses = list(batch([str(key)], [int(pointer)], [int(size)]))
            if len(statuses) != 1 or statuses[0] not in (None, 0):
                raise HiddenStateStoreError(f"batch_put_from failed for {key}: {statuses}")
            return
        put = getattr(self.store, "put_from", None)
        if not callable(put):
            raise HiddenStateStoreError("Store client lacks put_from/batch_put_from")
        status = put(str(key), int(pointer), int(size))
        if status not in (None, 0):
            raise HiddenStateStoreError(f"put_from failed for {key}: {status}")

    def _get_into(self, key: str, pointer: int, size: int) -> None:
        batch = getattr(self.store, "batch_get_into", None)
        if callable(batch):
            read_sizes = list(batch([str(key)], [int(pointer)], [int(size)]))
            if len(read_sizes) != 1 or int(read_sizes[0]) != int(size):
                raise HiddenStateStoreError(
                    f"batch_get_into failed for {key}: expected={size} got={read_sizes}"
                )
            return
        get = getattr(self.store, "get_into", None)
        if not callable(get):
            raise HiddenStateStoreError("Store client lacks get_into/batch_get_into")
        read = get(str(key), int(pointer), int(size))
        if int(read) != int(size):
            raise HiddenStateStoreError(
                f"get_into failed for {key}: expected={size} got={read}"
            )

    def _remove_keys(self, keys: Sequence[str]) -> None:
        pending = list(dict.fromkeys(str(key) for key in keys if str(key)))
        if not pending:
            return
        batch_remove = getattr(self.store, "batch_remove", None)
        if callable(batch_remove):
            try:
                try:
                    statuses = list(batch_remove(pending, True))
                except TypeError:
                    statuses = list(batch_remove(pending))
                if len(statuses) != len(pending):
                    raise HiddenStateStoreError("batch_remove returned an invalid result count")
                pending = [
                    key
                    for key, status in zip(pending, statuses)
                    if status not in (None, 0, -704)
                ]
                if not pending:
                    return
            except Exception:
                # Fall through to individual remove.  A raised batch call has
                # no trustworthy per-key completion information.
                pass
        remove = getattr(self.store, "remove", None)
        if not callable(remove):
            raise HiddenStateStoreError("Store client lacks remove for hidden-state cleanup")
        failures = []
        for key in pending:
            try:
                try:
                    status = remove(key, True)
                except TypeError:
                    status = remove(key)
            except KeyError:
                continue
            except Exception as exc:
                failures.append((key, exc))
                continue
            if status not in (None, 0, -704):
                failures.append((key, status))
        if failures:
            raise HiddenStateStoreError(f"failed to remove hidden-state objects: {failures[:3]}")

    def _remove_keys_best_effort(self, keys: Sequence[str]) -> None:
        try:
            self._remove_keys(keys)
        except Exception:
            pass
