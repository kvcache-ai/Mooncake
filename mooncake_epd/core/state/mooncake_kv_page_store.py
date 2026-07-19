"""Store-backed immutable KV pages for Agent manifest cloning.

The class intentionally accepts an injected Mooncake Store client rather than
constructing one from environment.  That keeps import-time dependencies out of
the control plane and makes the data path testable against a fault-injecting
client.  Production clients use registered-buffer ``put_from`` /
``batch_put_from`` and ``get_into`` / ``batch_get_into``; ordinary ``get`` or
Python serialization is never used by this adapter.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Iterator, List, Sequence

import torch

from .kv_manifest import KVManifestError, KVStateManifest, manifest_checksum
from .page_manager import BlockRef, PagedKVManager


class KVPageStoreError(RuntimeError):
    """Raised when Store-backed KV export/import cannot be completed safely."""


_SAFE_KEY_SEGMENT = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe_segment(value: object) -> str:
    raw = str(value).strip()
    if not raw:
        raise KVPageStoreError("Store key component must not be empty")
    normalized = _SAFE_KEY_SEGMENT.sub("_", raw).strip("._")
    if not normalized:
        raise KVPageStoreError(f"invalid Store key component: {value!r}")
    return normalized[:160]


def _tensor_nbytes(tensor: torch.Tensor) -> int:
    return int(tensor.numel() * tensor.element_size())


def _pair_checksum(key: torch.Tensor, value: torch.Tensor) -> str:
    digest = hashlib.sha256()
    for tensor in (key, value):
        # Preserve the physical KV representation.  In particular, calling
        # ``numpy`` directly on torch.bfloat16 raises, while an uint8 view is
        # supported and does not lossy-cast the cache values before Store
        # integrity verification.
        cpu_bytes = tensor.detach().contiguous().to(device="cpu").view(torch.uint8)
        digest.update(cpu_bytes.numpy().tobytes())
    return digest.hexdigest()


class MooncakeKVPageStore:
    """Export immutable KV pages and import them into target page managers.

    A successful export returns replacement manifests that contain Store keys.
    ``clone`` is metadata-only: child manifests reuse these keys and thus have
    ``clone_payload_bytes == 0``.  Import is explicitly a materialized copy and
    records concrete byte counts instead of calling it cross-node zero-copy.
    """

    def __init__(
        self,
        store: Any,
        *,
        key_prefix: str = "epd/kv/v2",
        require_registered_buffers: bool = True,
    ) -> None:
        self.store = store
        self.key_prefix = "/".join(_safe_segment(part) for part in str(key_prefix).split("/") if part)
        if not self.key_prefix:
            raise ValueError("key_prefix is required")
        self.require_registered_buffers = bool(require_registered_buffers)
        self._metrics: Dict[str, int] = {
            "export_calls": 0,
            "export_pages": 0,
            "export_bytes": 0,
            "import_calls": 0,
            "import_pages": 0,
            "import_bytes": 0,
            "gc_calls": 0,
            "gc_objects": 0,
            "failures": 0,
        }

    def stats(self) -> Dict[str, int]:
        return dict(self._metrics)

    def export_pages(
        self,
        page_manager: PagedKVManager,
        refs: Sequence[BlockRef],
        manifests: Sequence[KVStateManifest],
    ) -> List[KVStateManifest]:
        """Export complete immutable pages and bind Store keys into manifests.

        Pages are exported at full fixed page size so the target can write into
        a preallocated vLLM-compatible block without a shape conversion.  The
        logical valid token range remains in the manifest's ``token_*`` fields.
        """

        if len(refs) != len(manifests):
            raise KVPageStoreError("refs and manifests must have the same length")
        bindings: List[tuple[KVStateManifest, torch.Tensor, torch.Tensor, str, str]] = []
        for ref, manifest in zip(refs, manifests):
            if manifest.block_id != str(ref.global_block_id):
                raise KVPageStoreError(
                    f"manifest block mismatch: {manifest.block_id} != {ref.global_block_id}"
                )
            key, value = page_manager.get_page(ref)
            if not key.is_contiguous() or not value.is_contiguous():
                raise KVPageStoreError("KV page tensors must be contiguous for zero-copy Store export")
            key_store_key, value_store_key = self._page_keys(manifest)
            bindings.append((manifest, key, value, key_store_key, value_store_key))

        written: List[str] = []
        try:
            with ExitStack() as registrations:
                keys: List[str] = []
                pointers: List[int] = []
                sizes: List[int] = []
                for _manifest, key, value, key_name, value_name in bindings:
                    for name, tensor in ((key_name, key), (value_name, value)):
                        pointer, size = registrations.enter_context(self._registered_tensor(tensor))
                        keys.append(name)
                        pointers.append(pointer)
                        sizes.append(size)
                written.extend(keys)
                self._batch_put(keys, pointers, sizes)
            exported: List[KVStateManifest] = []
            for manifest, key, value, key_name, value_name in bindings:
                payload = {
                    **manifest.canonical_payload(),
                    "key_store_key": key_name,
                    "value_store_key": value_name,
                    "key_nbytes": _tensor_nbytes(key),
                    "value_nbytes": _tensor_nbytes(value),
                    "data_checksum": _pair_checksum(key, value),
                    "checksum": "",
                }
                payload["checksum"] = manifest_checksum(payload)
                exported.append(KVStateManifest(**payload))  # type: ignore[arg-type]
        except Exception:
            self._metrics["failures"] += 1
            self._cleanup(written)
            raise
        self._metrics["export_calls"] += 1
        self._metrics["export_pages"] += len(exported)
        self._metrics["export_bytes"] += sum(
            int(item.key_nbytes or 0) + int(item.value_nbytes or 0) for item in exported
        )
        return exported

    def clone_manifests(
        self,
        manifests: Sequence[KVStateManifest],
        *,
        state_id: str,
        version_id: str,
        snapshot_epoch: int,
        owner_node_id: str,
        target_node_id: str | None,
        lease_deadline: float,
    ) -> List[KVStateManifest]:
        """Create child descriptors without copying any KV page payload."""

        cloned: List[KVStateManifest] = []
        for index, manifest in enumerate(manifests):
            manifest.validate(allow_expired=False)
            payload = {
                **manifest.canonical_payload(),
                "state_id": str(state_id),
                "version_id": str(version_id),
                "snapshot_epoch": int(snapshot_epoch),
                "owner_node_id": str(owner_node_id),
                "target_node_id": target_node_id,
                "lease_id": f"{state_id}:lease:{index}",
                "lease_deadline": float(lease_deadline),
                "checksum": "",
            }
            payload["checksum"] = manifest_checksum(payload)
            cloned.append(KVStateManifest(**payload))  # type: ignore[arg-type]
        return cloned

    def materialize_pages(
        self,
        page_manager: PagedKVManager,
        manifests: Sequence[KVStateManifest],
    ) -> List[BlockRef]:
        """Allocate target-local pages, load their payloads, then verify hashes."""

        refs: List[BlockRef] = []
        try:
            for manifest in manifests:
                self._validate_target_layout(page_manager, manifest)
                if not manifest.has_store_payload:
                    raise KVPageStoreError(
                        f"manifest {manifest.transfer_key} has no Store page objects"
                    )
                ref = page_manager.allocate_page(filled=manifest.token_end - manifest.token_start)
                refs.append(ref)
                key, value = page_manager.get_page(ref)
                with ExitStack() as registrations:
                    key_ptr, key_size = registrations.enter_context(self._registered_tensor(key))
                    value_ptr, value_size = registrations.enter_context(self._registered_tensor(value))
                    self._batch_get(
                        [str(manifest.key_store_key), str(manifest.value_store_key)],
                        [key_ptr, value_ptr],
                        [key_size, value_size],
                    )
                actual = _pair_checksum(key, value)
                if actual != manifest.data_checksum:
                    raise KVPageStoreError(
                        f"payload checksum mismatch for {manifest.transfer_key}: "
                        f"expected={manifest.data_checksum} actual={actual}"
                    )
        except Exception:
            self._metrics["failures"] += 1
            if refs:
                page_manager.release_refs(refs)
            raise
        self._metrics["import_calls"] += 1
        self._metrics["import_pages"] += len(refs)
        self._metrics["import_bytes"] += sum(
            int(manifest.key_nbytes or 0) + int(manifest.value_nbytes or 0)
            for manifest in manifests
        )
        return refs

    def remove_page_objects(self, keys: Sequence[str]) -> int:
        """Remove catalog-owned immutable page objects after refcounted GC.

        This is intentionally narrower than a generic Store delete API: the
        Agent catalog supplies only key/value object names for pages it owns.
        The existing rollback helper validates per-key outcomes and fails
        closed if a Store may still contain an object.
        """

        unique = list(dict.fromkeys(str(key) for key in keys if str(key)))
        if not unique:
            return 0
        self._cleanup(unique)
        self._metrics["gc_calls"] += 1
        self._metrics["gc_objects"] += len(unique)
        return len(unique)

    def _page_keys(self, manifest: KVStateManifest) -> tuple[str, str]:
        root = "/".join(
            (
                self.key_prefix,
                _safe_segment(manifest.model_revision),
                _safe_segment(manifest.workflow_id),
                _safe_segment(manifest.state_id),
                _safe_segment(manifest.version_id),
                _safe_segment(manifest.block_id),
            )
        )
        return f"{root}/key", f"{root}/value"

    @contextmanager
    def _registered_tensor(self, tensor: torch.Tensor) -> Iterator[tuple[int, int]]:
        if not tensor.is_contiguous():
            raise KVPageStoreError("Store transfer requires contiguous tensors")
        pointer = int(tensor.data_ptr())
        size = _tensor_nbytes(tensor)
        if pointer <= 0 or size <= 0:
            raise KVPageStoreError("invalid tensor pointer or byte size")
        register = getattr(self.store, "register_buffer", None)
        unregister = getattr(self.store, "unregister_buffer", None)
        registered_here = False
        if callable(register):
            status = register(pointer, size)
            if status not in (None, 0, -600):
                raise KVPageStoreError(f"register_buffer failed: {status}")
            registered_here = status in (None, 0)
        elif self.require_registered_buffers:
            raise KVPageStoreError("Store client lacks register_buffer required for zero-copy transfer")
        try:
            yield pointer, size
        finally:
            if registered_here and callable(unregister):
                status = unregister(pointer)
                if status not in (None, 0):
                    raise KVPageStoreError(f"unregister_buffer failed: {status}")

    def _batch_put(self, keys: Sequence[str], pointers: Sequence[int], sizes: Sequence[int]) -> None:
        batch = getattr(self.store, "batch_put_from", None)
        if callable(batch):
            statuses = list(batch(list(keys), list(pointers), list(sizes)))
            if len(statuses) != len(keys):
                raise KVPageStoreError("batch_put_from returned an invalid result count")
            for key, status in zip(keys, statuses):
                if status not in (None, 0):
                    raise KVPageStoreError(f"batch_put_from failed for {key}: {status}")
            return
        put = getattr(self.store, "put_from", None)
        if not callable(put):
            raise KVPageStoreError("Store client lacks put_from/batch_put_from")
        for key, pointer, size in zip(keys, pointers, sizes):
            status = put(key, pointer, size)
            if status not in (None, 0):
                raise KVPageStoreError(f"put_from failed for {key}: {status}")

    def _batch_get(self, keys: Sequence[str], pointers: Sequence[int], sizes: Sequence[int]) -> None:
        batch = getattr(self.store, "batch_get_into", None)
        if callable(batch):
            read_sizes = list(batch(list(keys), list(pointers), list(sizes)))
            if len(read_sizes) != len(keys):
                raise KVPageStoreError("batch_get_into returned an invalid result count")
            for key, expected, actual in zip(keys, sizes, read_sizes):
                if int(actual) != int(expected):
                    raise KVPageStoreError(
                        f"batch_get_into failed for {key}: expected {expected}, got {actual}"
                    )
            return
        get = getattr(self.store, "get_into", None)
        if not callable(get):
            raise KVPageStoreError("Store client lacks get_into/batch_get_into")
        for key, pointer, size in zip(keys, pointers, sizes):
            actual = get(key, pointer, size)
            if int(actual) != int(size):
                raise KVPageStoreError(f"get_into failed for {key}: expected {size}, got {actual}")

    def _cleanup(self, keys: Sequence[str]) -> None:
        """Best-effort rollback that still exposes incomplete Store cleanup.

        Export rollback cannot safely report success when ``batch_remove``
        returns per-key failures.  Retry only failed keys through ``remove``;
        if any object may remain, raise so callers record the export failure
        and operators can reconcile the deterministic key prefix.
        """

        unique = list(dict.fromkeys(keys))
        if not unique:
            return
        pending = list(unique)
        batch_remove = getattr(self.store, "batch_remove", None)
        if callable(batch_remove):
            try:
                try:
                    statuses = list(batch_remove(pending, True))
                except TypeError:
                    statuses = list(batch_remove(pending))
                if len(statuses) != len(pending):
                    raise KVPageStoreError(
                        "batch_remove returned an invalid result count during export rollback"
                    )
                pending = [
                    key
                    for key, status in zip(pending, statuses)
                    if status not in (None, 0, -704)
                ]
                if not pending:
                    return
            except Exception as exc:
                # Preserve all keys for the per-key fallback because a raised
                # batch call provides no reliable per-key completion result.
                pending = list(unique)
                batch_error = exc
            else:
                batch_error = None
        else:
            batch_error = None
        remove = getattr(self.store, "remove", None)
        if not callable(remove):
            detail = f"; batch error: {batch_error}" if batch_error is not None else ""
            raise KVPageStoreError(
                f"unable to clean up {len(pending)} Store page objects: remove is unavailable{detail}"
            )
        failures: List[tuple[str, object]] = []
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
            detail = f"; batch error: {batch_error}" if batch_error is not None else ""
            raise KVPageStoreError(
                f"failed to clean up {len(failures)} Store page objects: {failures[:3]}{detail}"
            )

    @staticmethod
    def _validate_target_layout(page_manager: PagedKVManager, manifest: KVStateManifest) -> None:
        try:
            manifest.validate(allow_expired=False)
        except KVManifestError as exc:
            raise KVPageStoreError(str(exc)) from exc
        expected = {
            "dtype": str(page_manager.dtype),
            "page_size": int(page_manager.page_size),
            "num_kv_heads": int(page_manager.num_kv_heads),
            "head_dim": int(page_manager.head_dim),
        }
        for name, value in expected.items():
            received = getattr(manifest, name)
            if received is not None and received != value:
                raise KVPageStoreError(
                    f"KV layout mismatch for {name}: target={value!r} manifest={received!r}"
                )
