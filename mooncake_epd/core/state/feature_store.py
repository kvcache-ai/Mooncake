"""Feature bundle store: content-addressed cache for ViT/DeepStack outputs."""

from __future__ import annotations

import hashlib
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional

import torch


@dataclass(frozen=True)
class TensorSpec:
    """Portable description of a tensor carried across E→P.

    The descriptor is deliberately metadata-only: it is safe to pass through
    control-plane messages and lets the receiver validate a Mooncake handle
    before consuming hidden states in the model hot path.
    """

    shape: tuple[int, ...]
    dtype: str
    device_type: str
    nbytes: int
    checksum: Optional[str] = None

    @classmethod
    def from_tensor(cls, tensor: torch.Tensor, *, checksum: bool = False) -> "TensorSpec":
        digest = None
        if checksum:
            digest = _tensor_checksum(tensor)
        return cls(
            shape=tuple(int(dim) for dim in tensor.shape),
            dtype=str(tensor.dtype).replace("torch.", ""),
            device_type=str(tensor.device.type),
            nbytes=int(tensor.nelement() * tensor.element_size()),
            checksum=digest,
        )

    def to_dict(self) -> Dict:
        return {
            "shape": list(self.shape),
            "dtype": self.dtype,
            "device_type": self.device_type,
            "nbytes": int(self.nbytes),
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, payload: Dict) -> "TensorSpec":
        return cls(
            shape=tuple(int(dim) for dim in payload.get("shape", ())),
            dtype=str(payload.get("dtype") or "unknown"),
            device_type=str(payload.get("device_type") or "unknown"),
            nbytes=int(payload.get("nbytes", 0) or 0),
            checksum=(None if payload.get("checksum") is None else str(payload.get("checksum"))),
        )


@dataclass(frozen=True)
class FeatureBundleDescriptor:
    """Metadata contract for a vision hidden-state FeatureBundle.

    This is the boundary object that should be carried by EPD control messages:
    Encoder owns tensors, TransferEngine/Mooncake owns data movement, and Prefill
    validates this descriptor before injecting image hidden states and skipping
    the vision tower.  It prevents silently reusing hidden states from the wrong
    model, processor, shape, dtype, or image key.
    """

    feature_id: str
    model_fingerprint: str = ""
    processor_fingerprint: str = ""
    last_hidden: TensorSpec = field(default_factory=lambda: TensorSpec((), "unknown", "unknown", 0))
    intermediates: tuple[tuple[int, TensorSpec], ...] = field(default_factory=tuple)
    grid_thw: Optional[TensorSpec] = None
    nbytes: int = 0
    metadata: Dict = field(default_factory=dict)

    @classmethod
    def from_bundle(
        cls,
        bundle: "FeatureBundle",
        *,
        checksum: bool = False,
    ) -> "FeatureBundleDescriptor":
        return cls(
            feature_id=str(bundle.image_hash),
            model_fingerprint=str(bundle.metadata.get("model_fingerprint") or ""),
            processor_fingerprint=str(bundle.metadata.get("processor_fingerprint") or ""),
            last_hidden=TensorSpec.from_tensor(bundle.last_hidden, checksum=checksum),
            intermediates=tuple(
                (int(idx), TensorSpec.from_tensor(tensor, checksum=checksum))
                for idx, tensor in bundle.intermediates
            ),
            grid_thw=(
                TensorSpec.from_tensor(bundle.grid_thw, checksum=checksum)
                if bundle.grid_thw is not None
                else None
            ),
            nbytes=bundle.nbytes(),
            metadata={
                key: value
                for key, value in dict(bundle.metadata).items()
                if _is_descriptor_json_safe(value)
            },
        )

    def to_dict(self) -> Dict:
        return {
            "feature_id": self.feature_id,
            "model_fingerprint": self.model_fingerprint,
            "processor_fingerprint": self.processor_fingerprint,
            "last_hidden": self.last_hidden.to_dict(),
            "intermediates": [
                {"layer": int(layer), "spec": spec.to_dict()}
                for layer, spec in self.intermediates
            ],
            "grid_thw": self.grid_thw.to_dict() if self.grid_thw is not None else None,
            "nbytes": int(self.nbytes),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Dict) -> "FeatureBundleDescriptor":
        grid_payload = payload.get("grid_thw")
        return cls(
            feature_id=str(payload.get("feature_id") or ""),
            model_fingerprint=str(payload.get("model_fingerprint") or ""),
            processor_fingerprint=str(payload.get("processor_fingerprint") or ""),
            last_hidden=TensorSpec.from_dict(dict(payload.get("last_hidden") or {})),
            intermediates=tuple(
                (int(item.get("layer")), TensorSpec.from_dict(dict(item.get("spec") or {})))
                for item in list(payload.get("intermediates") or [])
            ),
            grid_thw=(TensorSpec.from_dict(dict(grid_payload)) if grid_payload is not None else None),
            nbytes=int(payload.get("nbytes", 0) or 0),
            metadata=dict(payload.get("metadata") or {}),
        )

    def validate_bundle(
        self,
        bundle: "FeatureBundle",
        *,
        expected_model_fingerprint: Optional[str] = None,
        expected_processor_fingerprint: Optional[str] = None,
        require_checksum: bool = False,
    ) -> None:
        """Raise ValueError if ``bundle`` does not match this descriptor."""

        if str(bundle.image_hash) != self.feature_id:
            raise ValueError(
                f"feature id mismatch: descriptor={self.feature_id} bundle={bundle.image_hash}"
            )
        model_fp = str(bundle.metadata.get("model_fingerprint") or "")
        processor_fp = str(bundle.metadata.get("processor_fingerprint") or "")
        if expected_model_fingerprint is not None and model_fp != str(expected_model_fingerprint):
            raise ValueError(
                f"model fingerprint mismatch: expected={expected_model_fingerprint} got={model_fp}"
            )
        if self.model_fingerprint and model_fp and self.model_fingerprint != model_fp:
            raise ValueError(
                f"descriptor model fingerprint mismatch: descriptor={self.model_fingerprint} bundle={model_fp}"
            )
        if expected_processor_fingerprint is not None and processor_fp != str(expected_processor_fingerprint):
            raise ValueError(
                f"processor fingerprint mismatch: expected={expected_processor_fingerprint} got={processor_fp}"
            )
        if self.processor_fingerprint and processor_fp and self.processor_fingerprint != processor_fp:
            raise ValueError(
                "descriptor processor fingerprint mismatch: "
                f"descriptor={self.processor_fingerprint} bundle={processor_fp}"
            )
        _validate_tensor_spec("last_hidden", self.last_hidden, bundle.last_hidden, require_checksum=require_checksum)
        if len(self.intermediates) != len(bundle.intermediates):
            raise ValueError(
                f"intermediate count mismatch: descriptor={len(self.intermediates)} bundle={len(bundle.intermediates)}"
            )
        for pos, ((expected_idx, spec), (actual_idx, tensor)) in enumerate(zip(self.intermediates, bundle.intermediates)):
            if int(expected_idx) != int(actual_idx):
                raise ValueError(
                    f"intermediate[{pos}] layer mismatch: descriptor={expected_idx} bundle={actual_idx}"
                )
            _validate_tensor_spec(f"intermediate[{expected_idx}]", spec, tensor, require_checksum=require_checksum)
        if self.grid_thw is None:
            if bundle.grid_thw is not None:
                raise ValueError("descriptor has no grid_thw but bundle provides one")
        elif bundle.grid_thw is None:
            raise ValueError("descriptor requires grid_thw but bundle has none")
        else:
            _validate_tensor_spec("grid_thw", self.grid_thw, bundle.grid_thw, require_checksum=require_checksum)


@dataclass
class FeatureBundle:
    image_hash: str
    last_hidden: torch.Tensor
    intermediates: List[tuple] = field(default_factory=list)
    grid_thw: Optional[torch.Tensor] = None
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.last_hidden, torch.Tensor):
            raise TypeError("FeatureBundle.last_hidden must be a torch.Tensor")
        normalised = []
        for item in self.intermediates:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise TypeError("FeatureBundle.intermediates entries must be (layer_idx, tensor)")
            idx, tensor = item
            if not isinstance(tensor, torch.Tensor):
                raise TypeError("FeatureBundle intermediate payload must be a torch.Tensor")
            normalised.append((int(idx), tensor))
        self.intermediates = normalised
        if self.grid_thw is not None and not isinstance(self.grid_thw, torch.Tensor):
            raise TypeError("FeatureBundle.grid_thw must be a torch.Tensor when provided")
        self.metadata = dict(self.metadata or {})

    def to(self, device: torch.device) -> "FeatureBundle":
        return FeatureBundle(
            image_hash=self.image_hash,
            last_hidden=self.last_hidden.to(device),
            intermediates=[(idx, t.to(device)) for idx, t in self.intermediates],
            grid_thw=self.grid_thw.to(device) if self.grid_thw is not None else None,
            metadata=dict(self.metadata),
        )

    def nbytes(self) -> int:
        n = self.last_hidden.nelement() * self.last_hidden.element_size()
        for _, t in self.intermediates:
            n += t.nelement() * t.element_size()
        if self.grid_thw is not None:
            n += self.grid_thw.nelement() * self.grid_thw.element_size()
        return int(n)

    def descriptor(self, *, checksum: bool = False) -> FeatureBundleDescriptor:
        return FeatureBundleDescriptor.from_bundle(self, checksum=checksum)

    def validate_against(
        self,
        descriptor: FeatureBundleDescriptor,
        *,
        expected_model_fingerprint: Optional[str] = None,
        expected_processor_fingerprint: Optional[str] = None,
        require_checksum: bool = False,
    ) -> None:
        descriptor.validate_bundle(
            self,
            expected_model_fingerprint=expected_model_fingerprint,
            expected_processor_fingerprint=expected_processor_fingerprint,
            require_checksum=require_checksum,
        )


@dataclass
class FeatureRecord:
    image_hash: str
    owner_shard: str = "local"
    physical_node_id: str = "local"
    refcount: int = 0
    lease_count: int = 0
    status: str = "ACTIVE"
    size_bytes: int = 0
    created_at: float = field(default_factory=time.monotonic)
    last_updated_at: float = field(default_factory=time.monotonic)
    ttl_deadline: float = float("inf")
    metadata: Dict = field(default_factory=dict)


def _tensor_checksum(tensor: torch.Tensor) -> str:
    h = hashlib.sha256()
    t = tensor.detach().contiguous().cpu()
    header = f"{tuple(t.shape)}|{t.dtype}|{tuple(t.stride())}".encode()
    h.update(header)
    # Avoid ``ndarray.tobytes()`` here: for 30-100MiB multimodal hidden states
    # it adds another full memory copy on the strict FeatureHandle validation
    # path.  hashlib accepts buffer-protocol objects directly, so the contiguous
    # uint8 NumPy view can be streamed without allocating a duplicate bytes
    # object.
    h.update(memoryview(t.view(torch.uint8).numpy()))
    return h.hexdigest()


def _validate_tensor_spec(
    name: str,
    spec: TensorSpec,
    tensor: torch.Tensor,
    *,
    require_checksum: bool = False,
) -> None:
    shape = tuple(int(dim) for dim in tensor.shape)
    dtype = str(tensor.dtype).replace("torch.", "")
    nbytes = int(tensor.nelement() * tensor.element_size())
    if shape != tuple(spec.shape):
        raise ValueError(f"{name} shape mismatch: descriptor={spec.shape} tensor={shape}")
    if dtype != spec.dtype:
        raise ValueError(f"{name} dtype mismatch: descriptor={spec.dtype} tensor={dtype}")
    if nbytes != int(spec.nbytes):
        raise ValueError(f"{name} nbytes mismatch: descriptor={spec.nbytes} tensor={nbytes}")
    if require_checksum or spec.checksum:
        actual = _tensor_checksum(tensor)
        if spec.checksum != actual:
            raise ValueError(f"{name} checksum mismatch")


def _is_descriptor_json_safe(value) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_descriptor_json_safe(item) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _is_descriptor_json_safe(item)
            for key, item in value.items()
        )
    return False


def hash_pixel_values(pixel_values: torch.Tensor, sample_budget: int = 4096) -> str:
    """Return a collision-resistant content digest for multimodal inputs.

    Earlier revisions sampled large tensors for speed. That is unsafe for an
    MM Store key because two images that differ outside the sample window can
    alias and reuse the wrong FeatureBundle. The optional ``sample_budget`` is
    kept for API compatibility but the digest always covers the full contiguous
    byte representation.
    """

    del sample_budget
    h = hashlib.sha256()
    t = pixel_values.detach().contiguous().cpu()
    header = f"{tuple(t.shape)}|{t.dtype}|{tuple(t.stride())}".encode()
    h.update(header)
    h.update(memoryview(t.view(torch.uint8).numpy()))
    return h.hexdigest()


class FeatureStore:
    """LRU feature cache with hard refcount + lease semantics."""

    def __init__(
        self,
        max_bytes: int = 4 * 1024 ** 3,
        max_entries: int = 1024,
        ttl_seconds: Optional[float] = None,
        *,
        node_id: str = "local",
    ):
        self._max_bytes = max_bytes
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._node_id = str(node_id)
        self._cache: "OrderedDict[str, FeatureBundle]" = OrderedDict()
        self._records: Dict[str, FeatureRecord] = {}
        self._current_bytes = 0
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    # ------------------------------------------------------------------
    # CRUD / lookup
    # ------------------------------------------------------------------
    def get(self, image_hash: str) -> Optional[FeatureBundle]:
        with self._lock:
            bundle = self._cache.get(image_hash)
            record = self._records.get(image_hash)
            if bundle is None or record is None:
                self._misses += 1
                return None
            if self._is_expired(record) and record.refcount <= 0 and record.lease_count <= 0:
                self._remove(image_hash)
                self._misses += 1
                return None
            self._touch(image_hash)
            self._hits += 1
            return bundle

    def put(
        self,
        image_hash: str,
        bundle: FeatureBundle,
        *,
        owner_shard: Optional[str] = None,
        physical_node_id: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        with self._lock:
            existing_record = self._records.get(image_hash)
            existing_bundle = self._cache.pop(image_hash, None)
            if existing_bundle is not None:
                self._current_bytes -= existing_bundle.nbytes()

            bundle_bytes = bundle.nbytes()
            self._ensure_capacity(bundle_bytes)
            self._cache[image_hash] = bundle
            ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
            ttl_deadline = (
                float("inf")
                if ttl is None
                else time.monotonic() + max(0.0, float(ttl))
            )
            record = existing_record or FeatureRecord(
                image_hash=image_hash,
                owner_shard=str(owner_shard or self._node_id),
                physical_node_id=str(physical_node_id or self._node_id),
            )
            record.owner_shard = str(owner_shard or record.owner_shard or self._node_id)
            record.physical_node_id = str(
                physical_node_id or record.physical_node_id or self._node_id
            )
            record.size_bytes = bundle_bytes
            record.status = "ACTIVE"
            record.last_updated_at = time.monotonic()
            record.ttl_deadline = ttl_deadline
            if metadata:
                merged = dict(record.metadata)
                merged.update(metadata)
                record.metadata = merged
            self._records[image_hash] = record
            self._current_bytes += bundle_bytes

    def has(self, image_hash: str) -> bool:
        with self._lock:
            record = self._records.get(image_hash)
            if image_hash not in self._cache or record is None:
                return False
            if self._is_expired(record) and record.refcount <= 0 and record.lease_count <= 0:
                self._remove(image_hash)
                return False
            return True

    def get_record(self, image_hash: str) -> Optional[FeatureRecord]:
        with self._lock:
            record = self._records.get(image_hash)
            return None if record is None else replace(record)

    # ------------------------------------------------------------------
    # Lifetime control
    # ------------------------------------------------------------------
    def incref(self, image_hash: str) -> int:
        with self._lock:
            record = self._require_record(image_hash)
            record.refcount += 1
            self._touch(image_hash)
            return record.refcount

    def release(self, image_hash: str) -> int:
        with self._lock:
            record = self._records.get(image_hash)
            if record is None:
                return 0
            record.refcount = max(0, record.refcount - 1)
            record.last_updated_at = time.monotonic()
            if record.refcount <= 0 and record.lease_count <= 0 and self._is_expired(record):
                self._remove(image_hash)
                return 0
            return record.refcount

    def lease(self, image_hash: str, *, ttl_seconds: Optional[float] = None) -> int:
        with self._lock:
            record = self._require_record(image_hash)
            record.lease_count += 1
            if ttl_seconds is not None:
                record.ttl_deadline = max(
                    record.ttl_deadline,
                    time.monotonic() + max(0.0, float(ttl_seconds)),
                )
            self._touch(image_hash)
            return record.lease_count

    def release_lease(self, image_hash: str) -> int:
        with self._lock:
            record = self._records.get(image_hash)
            if record is None:
                return 0
            record.lease_count = max(0, record.lease_count - 1)
            record.last_updated_at = time.monotonic()
            if record.refcount <= 0 and record.lease_count <= 0 and self._is_expired(record):
                self._remove(image_hash)
                return 0
            return record.lease_count

    def refcount(self, image_hash: str) -> int:
        with self._lock:
            record = self._records.get(image_hash)
            return 0 if record is None else record.refcount

    def lease_count(self, image_hash: str) -> int:
        with self._lock:
            record = self._records.get(image_hash)
            return 0 if record is None else record.lease_count

    def refresh_ttl(self, image_hash: str, *, ttl_seconds: Optional[float] = None) -> None:
        with self._lock:
            record = self._require_record(image_hash)
            ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
            record.ttl_deadline = (
                float("inf")
                if ttl is None
                else time.monotonic() + max(0.0, float(ttl))
            )
            record.last_updated_at = time.monotonic()

    def sweep_expired(self) -> int:
        with self._lock:
            expired = [
                key
                for key, record in self._records.items()
                if self._is_expired(record)
                and record.refcount <= 0
                and record.lease_count <= 0
            ]
            for key in expired:
                self._remove(key)
            return len(expired)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _require_record(self, image_hash: str) -> FeatureRecord:
        record = self._records.get(image_hash)
        if record is None or image_hash not in self._cache:
            raise KeyError(f"feature bundle not found: {image_hash}")
        return record

    def _touch(self, image_hash: str) -> None:
        if image_hash in self._cache:
            self._cache.move_to_end(image_hash)
        record = self._records.get(image_hash)
        if record is not None:
            record.last_updated_at = time.monotonic()

    def _is_expired(self, record: FeatureRecord) -> bool:
        return record.ttl_deadline != float("inf") and time.monotonic() > record.ttl_deadline

    def _ensure_capacity(self, incoming_bytes: int) -> None:
        while (
            (self._current_bytes + incoming_bytes > self._max_bytes)
            or (len(self._cache) >= self._max_entries)
        ) and self._cache:
            oldest_key = next(iter(self._cache))
            oldest = self._records.get(oldest_key)
            if oldest is not None and (oldest.refcount > 0 or oldest.lease_count > 0):
                movable = [
                    key
                    for key in self._cache.keys()
                    if (
                        (rec := self._records.get(key)) is not None
                        and rec.refcount <= 0
                        and rec.lease_count <= 0
                    )
                ]
                if not movable:
                    break
                oldest_key = movable[0]
            self._remove(oldest_key)

    def _remove(self, key: str) -> None:
        record = self._records.get(key)
        if record is not None and (record.refcount > 0 or record.lease_count > 0):
            return
        bundle = self._cache.pop(key, None)
        self._records.pop(key, None)
        if bundle is not None:
            self._current_bytes = max(0, self._current_bytes - bundle.nbytes())
            self._evictions += 1

    # ------------------------------------------------------------------
    # Maintenance / introspection
    # ------------------------------------------------------------------
    def clear(self) -> None:
        with self._lock:
            survivors = OrderedDict()
            survivor_records: Dict[str, FeatureRecord] = {}
            current_bytes = 0
            for key, bundle in self._cache.items():
                record = self._records.get(key)
                if record is None:
                    continue
                if record.refcount > 0 or record.lease_count > 0:
                    survivors[key] = bundle
                    survivor_records[key] = record
                    current_bytes += bundle.nbytes()
            self._cache = survivors
            self._records = survivor_records
            self._current_bytes = current_bytes
            self._hits = 0
            self._misses = 0
            self._evictions = 0

    def stats(self) -> Dict[str, float]:
        with self._lock:
            total = self._hits + self._misses
            expired = sum(1 for record in self._records.values() if self._is_expired(record))
            return {
                "entries": len(self._cache),
                "bytes": self._current_bytes,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total else 0.0,
                "pinned_entries": sum(1 for v in self._records.values() if v.refcount > 0),
                "leased_entries": sum(1 for v in self._records.values() if v.lease_count > 0),
                "total_refcount": sum(v.refcount for v in self._records.values()),
                "total_leases": sum(v.lease_count for v in self._records.values()),
                "expired_entries": expired,
                "evictions": self._evictions,
            }
