"""Canonical, content-bound metadata for Agent KV state pages.

The descriptor is deliberately payload-free.  A clone creates new manifests
and leases but never copies KV bytes; a target execution worker materializes
those immutable page objects only when it needs local writable blocks.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional


class KVManifestError(ValueError):
    """Raised when a KV manifest is malformed or incompatible."""


@dataclass(frozen=True)
class KVStateManifest:
    """One immutable logical KV page with ownership and compatibility fences."""

    model_id: str
    model_revision: str
    workflow_id: str
    state_id: str
    version_id: str
    snapshot_epoch: int
    token_start: int
    token_end: int
    layer_group: int
    layer_start: int
    layer_end: int
    tp_rank: int
    tp_size: int
    owner_node_id: str
    block_id: str
    checksum: str
    lease_id: str
    lease_deadline: float
    source_generation: str
    target_generation: str
    created_at: float = field(default_factory=time.monotonic)
    target_node_id: Optional[str] = None
    dtype: Optional[str] = None
    page_size: Optional[int] = None
    num_kv_heads: Optional[int] = None
    head_dim: Optional[int] = None
    data_checksum: Optional[str] = None
    key_store_key: Optional[str] = None
    value_store_key: Optional[str] = None
    key_nbytes: Optional[int] = None
    value_nbytes: Optional[int] = None

    def __post_init__(self) -> None:
        for name in (
            "model_id",
            "model_revision",
            "workflow_id",
            "state_id",
            "version_id",
            "owner_node_id",
            "block_id",
            "checksum",
            "lease_id",
            "source_generation",
            "target_generation",
        ):
            object.__setattr__(self, name, str(getattr(self, name)))
        for name in ("target_node_id", "dtype", "data_checksum", "key_store_key", "value_store_key"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, str(value))
        self.validate()

    @property
    def transfer_key(self) -> str:
        return ":".join(
            (
                self.workflow_id,
                self.state_id,
                self.version_id,
                str(self.snapshot_epoch),
                str(self.layer_group),
                str(self.tp_rank),
                self.block_id,
                self.source_generation,
                self.target_generation,
            )
        )

    @property
    def has_store_payload(self) -> bool:
        return bool(
            self.key_store_key
            and self.value_store_key
            and int(self.key_nbytes or 0) > 0
            and int(self.value_nbytes or 0) > 0
        )

    def validate(self, *, now: Optional[float] = None, allow_expired: bool = True) -> None:
        required = {
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "workflow_id": self.workflow_id,
            "state_id": self.state_id,
            "version_id": self.version_id,
            "owner_node_id": self.owner_node_id,
            "block_id": self.block_id,
            "checksum": self.checksum,
            "lease_id": self.lease_id,
            "source_generation": self.source_generation,
            "target_generation": self.target_generation,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise KVManifestError("missing required manifest fields: " + ", ".join(missing))
        if int(self.snapshot_epoch) < 0:
            raise KVManifestError("snapshot_epoch must be non-negative")
        if int(self.token_start) < 0 or int(self.token_end) <= int(self.token_start):
            raise KVManifestError("token range must be non-empty and ordered")
        if int(self.layer_group) < 0 or int(self.layer_start) < 0 or int(self.layer_end) <= int(self.layer_start):
            raise KVManifestError("layer range must be non-empty and ordered")
        if int(self.tp_rank) < 0 or int(self.tp_size) <= 0 or int(self.tp_rank) >= int(self.tp_size):
            raise KVManifestError("invalid tensor-parallel rank/size")
        if not allow_expired and float(self.lease_deadline) < (time.monotonic() if now is None else float(now)):
            raise KVManifestError(f"manifest lease expired: {self.lease_id}")
        for name in ("page_size", "num_kv_heads", "head_dim", "key_nbytes", "value_nbytes"):
            value = getattr(self, name)
            if value is not None and int(value) <= 0:
                raise KVManifestError(f"{name} must be positive when present")
        store_fields = (self.key_store_key, self.value_store_key, self.key_nbytes, self.value_nbytes)
        if any(value is not None for value in store_fields) and not self.has_store_payload:
            raise KVManifestError("Store-backed pages require both keys and positive byte counts")

    def canonical_payload(self) -> dict[str, Any]:
        # Every current manifest field is scalar/immutable.  ``asdict``
        # recursively deep-copies all fields, which is unnecessary for a
        # checksum payload and made descriptor-only Agent forks spend more
        # time cloning Python metadata than sharing the KV pages themselves.
        # Iterating the dataclass schema keeps this forward-compatible when a
        # new checksum-bound field is added.
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name != "created_at"
        }

    def manifest_checksum(self) -> str:
        return manifest_checksum(self.canonical_payload())

    def with_checksum(self) -> "KVStateManifest":
        return KVStateManifest(**{**asdict(self), "checksum": self.manifest_checksum()})

    def as_control_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["transfer_key"] = self.transfer_key
        return payload

    @classmethod
    def from_control_payload(cls, payload: Mapping[str, Any]) -> "KVStateManifest":
        if not isinstance(payload, Mapping):
            raise KVManifestError("manifest payload must be a mapping")
        values = dict(payload)
        values.pop("transfer_key", None)
        allowed = set(cls.__dataclass_fields__)  # type: ignore[attr-defined]
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise KVManifestError("unknown manifest fields: " + ", ".join(unknown))
        return cls(**values)


def manifest_checksum(payload: Mapping[str, Any]) -> str:
    """Return a stable digest over identity, layout, lease and payload refs."""

    values = dict(payload)
    values.pop("checksum", None)
    values.pop("created_at", None)
    raw = json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
