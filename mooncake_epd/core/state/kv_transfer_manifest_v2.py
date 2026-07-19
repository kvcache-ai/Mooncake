"""Checksummed P-to-D KV transfer manifests.

The vLLM Mooncake connector still owns the physical pointer exchange and the
actual transfer completion.  This module supplies the metadata fence that was
previously missing from the P-to-D handoff: a manifest binds one transfer to
its source/destination workers, worker generations, KV layout, block groups,
transport declaration, and a wall-clock lease.

``KVTransferManifestV2`` deliberately contains no raw pointers or tensor
payload.  Pointer values are process-local and must never be treated as a
durable identity.  A consumer validates this envelope before asking the
connector to pull remote KV blocks; a stale, tampered, or wrong-target
manifest is rejected before the data plane begins.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


KV_TRANSFER_MANIFEST_SCHEMA_V2 = "mooncake.epd.kv_transfer_manifest.v2"


class KVTransferManifestError(ValueError):
    """Raised when a KV transfer manifest is malformed or incompatible."""


def _json_copy(value: Mapping[str, Any] | None, *, label: str) -> Dict[str, Any]:
    try:
        encoded = json.dumps(
            dict(value or {}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise KVTransferManifestError(f"{label} must be JSON-serializable") from exc
    if not isinstance(decoded, dict):  # Defensive; dict(...) above should guarantee this.
        raise KVTransferManifestError(f"{label} must encode to an object")
    return decoded


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KVTransferManifestError("manifest is not JSON-serializable") from exc


def kv_transfer_manifest_checksum(payload: Mapping[str, Any]) -> str:
    """Return a stable digest over every control-plane field except checksum."""

    values = dict(payload)
    values.pop("checksum", None)
    return hashlib.sha256(_canonical_json(values)).hexdigest()


def _normalise_block_groups(value: Any) -> Tuple[Tuple[int, ...], ...]:
    """Normalise vLLM's flat/nested remote block IDs without accepting pointers."""

    if not isinstance(value, (list, tuple)) or not value:
        raise KVTransferManifestError("remote_block_ids must be a non-empty list")
    raw_groups: Sequence[Any]
    if all(not isinstance(item, (list, tuple)) for item in value):
        raw_groups = (value,)
    else:
        raw_groups = value
    groups: list[Tuple[int, ...]] = []
    for raw_group in raw_groups:
        if not isinstance(raw_group, (list, tuple)):
            raise KVTransferManifestError("remote_block_ids must contain only block-id groups")
        group: list[int] = []
        for raw_id in raw_group:
            try:
                block_id = int(raw_id)
            except (TypeError, ValueError) as exc:
                raise KVTransferManifestError(
                    f"remote block id is not an integer: {raw_id!r}"
                ) from exc
            if block_id < 0:
                raise KVTransferManifestError("remote block IDs must be non-negative")
            group.append(block_id)
        if group:
            groups.append(tuple(group))
    if not groups:
        raise KVTransferManifestError("remote_block_ids must contain at least one block")
    return tuple(groups)


def _normalise_layer_groups(value: Any) -> Tuple[Tuple[int, int], ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise KVTransferManifestError("layer_groups must be a list of [start, end] pairs")
    groups: list[Tuple[int, int]] = []
    for raw_group in value:
        if not isinstance(raw_group, (list, tuple)) or len(raw_group) != 2:
            raise KVTransferManifestError("layer_groups must contain [start, end] pairs")
        try:
            start, end = int(raw_group[0]), int(raw_group[1])
        except (TypeError, ValueError) as exc:
            raise KVTransferManifestError("layer group bounds must be integers") from exc
        if start < 0 or end <= start:
            raise KVTransferManifestError("layer group bounds must be ordered and non-negative")
        groups.append((start, end))
    return tuple(groups)


def _unknown(value: str) -> bool:
    return str(value).strip().lower() in {"", "unknown", "none", "null"}


@dataclass(frozen=True)
class KVTransferLayoutV2:
    """Compatibility fields for a vLLM KV-cache layout.

    Fields can remain ``"unknown"`` in compatibility mode while an older
    worker is being upgraded.  Strict deployments must set every identity
    field and all positive layout dimensions before dispatching a transfer.
    """

    model_id: str = "unknown"
    model_revision: str = "unknown"
    vllm_adapter_version: str = "unknown"
    dtype: str = "unknown"
    block_size: int = 0
    num_layers: int = 0
    tp_size: int = 1
    tp_rank: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("model_id", "model_revision", "vllm_adapter_version", "dtype"):
            object.__setattr__(self, name, str(getattr(self, name) or "unknown"))
        object.__setattr__(self, "extra", _json_copy(self.extra, label="layout.extra"))
        self.validate()

    def validate(self, *, require_compatibility: bool = False) -> None:
        if int(self.block_size) < 0 or int(self.num_layers) < 0:
            raise KVTransferManifestError("block_size and num_layers must be non-negative")
        if int(self.tp_size) <= 0 or int(self.tp_rank) < 0 or int(self.tp_rank) >= int(self.tp_size):
            raise KVTransferManifestError("invalid tensor-parallel rank/size")
        if require_compatibility:
            missing = [
                name
                for name in ("model_id", "model_revision", "vllm_adapter_version", "dtype")
                if _unknown(str(getattr(self, name)))
            ]
            if missing:
                raise KVTransferManifestError(
                    "strict KV layout is missing: " + ", ".join(missing)
                )
            if int(self.block_size) <= 0 or int(self.num_layers) <= 0:
                raise KVTransferManifestError(
                    "strict KV layout requires positive block_size and num_layers"
                )

    def as_control_payload(self) -> Dict[str, Any]:
        return {
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "vllm_adapter_version": self.vllm_adapter_version,
            "dtype": self.dtype,
            "block_size": int(self.block_size),
            "num_layers": int(self.num_layers),
            "tp_size": int(self.tp_size),
            "tp_rank": int(self.tp_rank),
            "extra": dict(self.extra),
        }

    @property
    def checksum(self) -> str:
        return hashlib.sha256(_canonical_json(self.as_control_payload())).hexdigest()

    @classmethod
    def from_control_payload(cls, payload: Mapping[str, Any]) -> "KVTransferLayoutV2":
        if not isinstance(payload, Mapping):
            raise KVTransferManifestError("layout must be an object")
        values = dict(payload)
        allowed = {
            "model_id",
            "model_revision",
            "vllm_adapter_version",
            "dtype",
            "block_size",
            "num_layers",
            "tp_size",
            "tp_rank",
            "extra",
        }
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise KVTransferManifestError("unknown layout fields: " + ", ".join(unknown))
        try:
            return cls(
                model_id=str(values.get("model_id") or "unknown"),
                model_revision=str(values.get("model_revision") or "unknown"),
                vllm_adapter_version=str(values.get("vllm_adapter_version") or "unknown"),
                dtype=str(values.get("dtype") or "unknown"),
                block_size=int(values.get("block_size", 0) or 0),
                num_layers=int(values.get("num_layers", 0) or 0),
                tp_size=int(values.get("tp_size", 1) or 1),
                tp_rank=int(values.get("tp_rank", 0) or 0),
                extra=dict(values.get("extra") or {}),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, KVTransferManifestError):
                raise
            raise KVTransferManifestError(f"invalid KV layout: {exc}") from exc


@dataclass(frozen=True)
class KVTransferManifestV2:
    """Immutable, checksummed metadata required to consume one P-to-D handoff."""

    schema_version: str
    manifest_id: str
    idempotency_key: str
    transfer_id: str
    workflow_id: str
    attempt: int
    source_engine_id: str
    source_generation: str
    destination_engine_id: str
    destination_generation: str
    layout: KVTransferLayoutV2
    layout_checksum: str
    remote_block_ids: Tuple[Tuple[int, ...], ...]
    token_count: int
    layer_groups: Tuple[Tuple[int, int], ...]
    transport: Dict[str, Any]
    lease_id: str
    issued_at_unix_s: float
    expires_at_unix_s: float
    checksum: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "schema_version",
            "manifest_id",
            "idempotency_key",
            "transfer_id",
            "workflow_id",
            "source_engine_id",
            "source_generation",
            "destination_engine_id",
            "destination_generation",
            "layout_checksum",
            "lease_id",
            "checksum",
        ):
            object.__setattr__(self, name, str(getattr(self, name) or ""))
        if not isinstance(self.layout, KVTransferLayoutV2):
            raise KVTransferManifestError("layout must be a KVTransferLayoutV2")
        object.__setattr__(self, "remote_block_ids", _normalise_block_groups(self.remote_block_ids))
        object.__setattr__(self, "layer_groups", _normalise_layer_groups(self.layer_groups))
        object.__setattr__(self, "transport", _json_copy(self.transport, label="transport"))
        object.__setattr__(self, "metadata", _json_copy(self.metadata, label="metadata"))
        self.validate(require_checksum=bool(self.checksum))

    @classmethod
    def create(
        cls,
        *,
        transfer_id: str,
        workflow_id: str,
        source_engine_id: str,
        source_generation: str = "unknown",
        destination_engine_id: str,
        destination_generation: str = "unknown",
        layout: KVTransferLayoutV2 | Mapping[str, Any] | None = None,
        remote_block_ids: Sequence[Any],
        token_count: int = 0,
        layer_groups: Sequence[Sequence[int]] | None = None,
        transport: Mapping[str, Any] | None = None,
        attempt: int = 0,
        lease_seconds: float = 120.0,
        idempotency_key: Optional[str] = None,
        metadata: Mapping[str, Any] | None = None,
        now: Optional[float] = None,
    ) -> "KVTransferManifestV2":
        timestamp = time.time() if now is None else float(now)
        if timestamp <= 0:
            raise KVTransferManifestError("issued_at_unix_s must be positive")
        if isinstance(layout, KVTransferLayoutV2):
            resolved_layout = layout
        else:
            resolved_layout = KVTransferLayoutV2.from_control_payload(dict(layout or {}))
        transfer = str(transfer_id or "").strip()
        workflow = str(workflow_id or "").strip()
        source = str(source_engine_id or "").strip()
        destination = str(destination_engine_id or "").strip()
        if not transfer or not workflow or not source or not destination:
            raise KVTransferManifestError(
                "transfer_id, workflow_id, source_engine_id, and destination_engine_id are required"
            )
        attempt_i = int(attempt)
        if attempt_i < 0:
            raise KVTransferManifestError("attempt must be non-negative")
        duration = max(0.0, float(lease_seconds))
        groups = _normalise_block_groups(remote_block_ids)
        key = idempotency_key or ":".join(
            (
                KV_TRANSFER_MANIFEST_SCHEMA_V2,
                transfer,
                source,
                destination,
                str(attempt_i),
            )
        )
        payload: Dict[str, Any] = {
            "schema_version": KV_TRANSFER_MANIFEST_SCHEMA_V2,
            "manifest_id": uuid.uuid4().hex,
            "idempotency_key": str(key),
            "transfer_id": transfer,
            "workflow_id": workflow,
            "attempt": attempt_i,
            "source_engine_id": source,
            "source_generation": str(source_generation or "unknown"),
            "destination_engine_id": destination,
            "destination_generation": str(destination_generation or "unknown"),
            "layout": resolved_layout,
            "layout_checksum": resolved_layout.checksum,
            "remote_block_ids": groups,
            "token_count": int(token_count or 0),
            "layer_groups": _normalise_layer_groups(layer_groups),
            "transport": dict(transport or {}),
            "lease_id": f"kv-transfer:{transfer}:{uuid.uuid4().hex}",
            "issued_at_unix_s": timestamp,
            "expires_at_unix_s": timestamp + duration,
            "checksum": "",
            "metadata": dict(metadata or {}),
        }
        checksum = kv_transfer_manifest_checksum(cls._payload_from_values(payload))
        return cls(**{**payload, "checksum": checksum})

    @staticmethod
    def _payload_from_values(values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values)
        layout = payload.get("layout")
        if isinstance(layout, KVTransferLayoutV2):
            payload["layout"] = layout.as_control_payload()
        remote_block_ids = payload.get("remote_block_ids")
        if isinstance(remote_block_ids, tuple):
            payload["remote_block_ids"] = [list(group) for group in remote_block_ids]
        layer_groups = payload.get("layer_groups")
        if isinstance(layer_groups, tuple):
            payload["layer_groups"] = [list(group) for group in layer_groups]
        return payload

    def as_control_payload(self) -> Dict[str, Any]:
        return self._payload_from_values(
            {
                "schema_version": self.schema_version,
                "manifest_id": self.manifest_id,
                "idempotency_key": self.idempotency_key,
                "transfer_id": self.transfer_id,
                "workflow_id": self.workflow_id,
                "attempt": int(self.attempt),
                "source_engine_id": self.source_engine_id,
                "source_generation": self.source_generation,
                "destination_engine_id": self.destination_engine_id,
                "destination_generation": self.destination_generation,
                "layout": self.layout,
                "layout_checksum": self.layout_checksum,
                "remote_block_ids": self.remote_block_ids,
                "token_count": int(self.token_count),
                "layer_groups": self.layer_groups,
                "transport": dict(self.transport),
                "lease_id": self.lease_id,
                "issued_at_unix_s": float(self.issued_at_unix_s),
                "expires_at_unix_s": float(self.expires_at_unix_s),
                "checksum": self.checksum,
                "metadata": dict(self.metadata),
            }
        )

    @classmethod
    def from_control_payload(cls, payload: Mapping[str, Any]) -> "KVTransferManifestV2":
        if not isinstance(payload, Mapping):
            raise KVTransferManifestError("KV transfer manifest must be an object")
        values = dict(payload)
        allowed = {
            "schema_version",
            "manifest_id",
            "idempotency_key",
            "transfer_id",
            "workflow_id",
            "attempt",
            "source_engine_id",
            "source_generation",
            "destination_engine_id",
            "destination_generation",
            "layout",
            "layout_checksum",
            "remote_block_ids",
            "token_count",
            "layer_groups",
            "transport",
            "lease_id",
            "issued_at_unix_s",
            "expires_at_unix_s",
            "checksum",
            "metadata",
        }
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise KVTransferManifestError("unknown manifest fields: " + ", ".join(unknown))
        try:
            manifest = cls(
                schema_version=str(values.get("schema_version") or ""),
                manifest_id=str(values.get("manifest_id") or ""),
                idempotency_key=str(values.get("idempotency_key") or ""),
                transfer_id=str(values.get("transfer_id") or ""),
                workflow_id=str(values.get("workflow_id") or ""),
                attempt=int(values.get("attempt", -1)),
                source_engine_id=str(values.get("source_engine_id") or ""),
                source_generation=str(values.get("source_generation") or ""),
                destination_engine_id=str(values.get("destination_engine_id") or ""),
                destination_generation=str(values.get("destination_generation") or ""),
                layout=KVTransferLayoutV2.from_control_payload(dict(values.get("layout") or {})),
                layout_checksum=str(values.get("layout_checksum") or ""),
                remote_block_ids=_normalise_block_groups(values.get("remote_block_ids")),
                token_count=int(values.get("token_count", 0) or 0),
                layer_groups=_normalise_layer_groups(values.get("layer_groups")),
                transport=dict(values.get("transport") or {}),
                lease_id=str(values.get("lease_id") or ""),
                issued_at_unix_s=float(values.get("issued_at_unix_s", 0.0)),
                expires_at_unix_s=float(values.get("expires_at_unix_s", 0.0)),
                checksum=str(values.get("checksum") or ""),
                metadata=dict(values.get("metadata") or {}),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, KVTransferManifestError):
                raise
            raise KVTransferManifestError(f"invalid KV transfer manifest: {exc}") from exc
        manifest.validate(require_checksum=True)
        return manifest

    def manifest_checksum(self) -> str:
        return kv_transfer_manifest_checksum(self.as_control_payload())

    @property
    def expired(self) -> bool:
        return time.time() >= float(self.expires_at_unix_s)

    def validate(
        self,
        *,
        now: Optional[float] = None,
        require_checksum: bool = True,
        require_generation_fences: bool = False,
        require_compatibility: bool = False,
        allow_expired: bool = True,
    ) -> None:
        required = {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "idempotency_key": self.idempotency_key,
            "transfer_id": self.transfer_id,
            "workflow_id": self.workflow_id,
            "source_engine_id": self.source_engine_id,
            "source_generation": self.source_generation,
            "destination_engine_id": self.destination_engine_id,
            "destination_generation": self.destination_generation,
            "layout_checksum": self.layout_checksum,
            "lease_id": self.lease_id,
        }
        missing = [name for name, value in required.items() if not str(value).strip()]
        if missing:
            raise KVTransferManifestError("missing required manifest fields: " + ", ".join(missing))
        if self.schema_version != KV_TRANSFER_MANIFEST_SCHEMA_V2:
            raise KVTransferManifestError(f"unsupported KV transfer schema: {self.schema_version!r}")
        if int(self.attempt) < 0 or int(self.token_count) < 0:
            raise KVTransferManifestError("attempt and token_count must be non-negative")
        _normalise_block_groups(self.remote_block_ids)
        _normalise_layer_groups(self.layer_groups)
        if float(self.issued_at_unix_s) <= 0.0 or float(self.expires_at_unix_s) < float(self.issued_at_unix_s):
            raise KVTransferManifestError("invalid KV transfer manifest lease timestamps")
        if self.layout_checksum != self.layout.checksum:
            raise KVTransferManifestError("KV transfer layout checksum mismatch")
        self.layout.validate(require_compatibility=require_compatibility)
        protocol = str(self.transport.get("protocol") or "").strip()
        backend = str(self.transport.get("backend") or "").strip()
        if not protocol or not backend:
            raise KVTransferManifestError("transport protocol and backend are required")
        if require_generation_fences or require_compatibility:
            missing_generation = [
                name
                for name in ("source_generation", "destination_generation")
                if _unknown(str(getattr(self, name)))
            ]
            if missing_generation:
                raise KVTransferManifestError(
                    "strict KV transfer generations are missing: "
                    + ", ".join(missing_generation)
                )
            if require_compatibility and (_unknown(protocol) or _unknown(backend)):
                raise KVTransferManifestError("strict KV transport protocol and backend are required")
        current = time.time() if now is None else float(now)
        if not allow_expired and current >= float(self.expires_at_unix_s):
            raise KVTransferManifestError(f"KV transfer manifest lease expired: {self.lease_id}")
        if require_checksum:
            if not self.checksum:
                raise KVTransferManifestError("KV transfer manifest checksum is required")
            if self.checksum != self.manifest_checksum():
                raise KVTransferManifestError("KV transfer manifest checksum mismatch")

    def validate_for_consumer(
        self,
        *,
        transfer_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        source_engine_id: Optional[str] = None,
        source_generation: Optional[str] = None,
        destination_engine_id: Optional[str] = None,
        destination_generation: Optional[str] = None,
        require_generation_fences: bool = False,
        require_compatibility: bool = False,
        now: Optional[float] = None,
    ) -> None:
        """Validate integrity/freshness and bind a manifest to this consumer."""

        self.validate(
            now=now,
            require_checksum=True,
            require_generation_fences=require_generation_fences,
            require_compatibility=require_compatibility,
            allow_expired=False,
        )
        expected = {
            "transfer_id": transfer_id,
            "workflow_id": workflow_id,
            "source_engine_id": source_engine_id,
            "source_generation": source_generation,
            "destination_engine_id": destination_engine_id,
            "destination_generation": destination_generation,
        }
        for name, expected_value in expected.items():
            if expected_value is None:
                continue
            actual = str(getattr(self, name))
            if actual != str(expected_value):
                raise KVTransferManifestError(
                    f"KV transfer manifest {name} mismatch: expected={expected_value!r} actual={actual!r}"
                )

    def validate_for_producer(
        self,
        *,
        transfer_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        source_engine_id: Optional[str] = None,
        source_generation: Optional[str] = None,
        require_generation_fences: bool = False,
        require_compatibility: bool = False,
        now: Optional[float] = None,
    ) -> None:
        """Validate integrity/freshness and bind a manifest to its producer.

        Decode validates the target fence before allocating local blocks.  The
        producer must independently validate its *own* engine and generation
        before it dereferences a received pointer descriptor: a stale Decode
        retry can otherwise ask a restarted Prefill worker to export blocks
        associated with an older incarnation.  The producer intentionally does
        not assert the destination generation here because that fence belongs
        to the Decode worker which issued the request.
        """

        self.validate(
            now=now,
            require_checksum=True,
            require_generation_fences=require_generation_fences,
            require_compatibility=require_compatibility,
            allow_expired=False,
        )
        expected = {
            "transfer_id": transfer_id,
            "workflow_id": workflow_id,
            "source_engine_id": source_engine_id,
            "source_generation": source_generation,
        }
        for name, expected_value in expected.items():
            if expected_value is None:
                continue
            actual = str(getattr(self, name))
            if actual != str(expected_value):
                raise KVTransferManifestError(
                    f"KV transfer manifest {name} mismatch: "
                    f"expected={expected_value!r} actual={actual!r}"
                )

    def rebind_destination(
        self,
        *,
        destination_engine_id: str,
        destination_generation: str = "unknown",
        attempt: Optional[int] = None,
        now: Optional[float] = None,
    ) -> "KVTransferManifestV2":
        """Issue a new target-bound envelope without extending the source lease.

        A Decode scheduler may select a non-affinity worker after Prefill has
        finished.  The source lease is intentionally preserved: rerouting does
        not prove that the producer retained its blocks for longer.
        """

        self.validate(now=now, require_checksum=True, allow_expired=False)
        target = str(destination_engine_id or "").strip()
        if not target:
            raise KVTransferManifestError("destination_engine_id is required")
        next_attempt = int(self.attempt) + 1 if attempt is None else int(attempt)
        if next_attempt < int(self.attempt):
            raise KVTransferManifestError("rerouted manifest attempt cannot decrease")
        values = self.as_control_payload()
        values.update(
            {
                "manifest_id": uuid.uuid4().hex,
                "idempotency_key": ":".join(
                    (
                        KV_TRANSFER_MANIFEST_SCHEMA_V2,
                        self.transfer_id,
                        self.source_engine_id,
                        target,
                        str(next_attempt),
                    )
                ),
                "destination_engine_id": target,
                "destination_generation": str(destination_generation or "unknown"),
                "attempt": next_attempt,
                "checksum": "",
            }
        )
        values["checksum"] = kv_transfer_manifest_checksum(values)
        return KVTransferManifestV2.from_control_payload(values)
