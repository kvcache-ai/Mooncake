"""Versioned control-plane state for E-to-P FeatureHandle transfers.

The existing :class:`FeatureHandle` remains the lightweight object consumed by
vLLM.  ``FeatureStateManifestV2`` adds the missing lifecycle and compatibility
fences around that handle: a request/ordinal identity, producer and consumer
generations, a lease, a checksum, and a small fail-closed state machine.

This module intentionally owns metadata only.  It never serializes hidden
states or treats a manifest as proof that a remote pointer is still valid.  The
direct-buffer registry and Mooncake completion path remain responsible for
data-plane lifetime; callers use this ledger to make control-plane transitions
explicit and testable.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .feature_store import FeatureBundleDescriptor


FEATURE_STATE_SCHEMA_V2 = "mooncake.epd.feature_state.v2"


class FeatureManifestError(ValueError):
    """Raised when a FeatureStateV2 payload is malformed or incompatible."""


class FeatureStateConflictError(FeatureManifestError):
    """Raised when an idempotency or compare-and-swap fence is violated."""


class FeatureStateTransitionError(FeatureManifestError):
    """Raised when a lifecycle transition is not legal from the current state."""


class FeatureState(str, Enum):
    DESCRIBED = "DESCRIBED"
    ALLOCATED = "ALLOCATED"
    TRANSFERRING = "TRANSFERRING"
    READY = "READY"
    CONSUMING = "CONSUMING"
    CONSUMED = "CONSUMED"
    ABORTING = "ABORTING"
    RELEASED = "RELEASED"


_TERMINAL_STATES = frozenset({FeatureState.RELEASED})
_CONSUMABLE_STATES = frozenset(
    {FeatureState.READY, FeatureState.CONSUMING, FeatureState.CONSUMED}
)
_ALLOWED_TRANSITIONS = {
    FeatureState.DESCRIBED: frozenset({FeatureState.ALLOCATED, FeatureState.ABORTING}),
    FeatureState.ALLOCATED: frozenset({FeatureState.TRANSFERRING, FeatureState.ABORTING}),
    FeatureState.TRANSFERRING: frozenset({FeatureState.READY, FeatureState.ABORTING}),
    FeatureState.READY: frozenset({FeatureState.CONSUMING, FeatureState.ABORTING}),
    FeatureState.CONSUMING: frozenset({FeatureState.CONSUMED, FeatureState.ABORTING}),
    FeatureState.CONSUMED: frozenset({FeatureState.RELEASED}),
    FeatureState.ABORTING: frozenset({FeatureState.RELEASED}),
    FeatureState.RELEASED: frozenset(),
}


def _json_copy(value: Mapping[str, Any], *, label: str) -> Dict[str, Any]:
    try:
        raw = json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        decoded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise FeatureManifestError(f"{label} must be JSON-serializable") from exc
    if not isinstance(decoded, dict):  # Defensive; dict(value) above should ensure this.
        raise FeatureManifestError(f"{label} must encode to an object")
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
        raise FeatureManifestError("manifest is not JSON-serializable") from exc


def feature_manifest_checksum(payload: Mapping[str, Any]) -> str:
    """Return the stable digest over all control-plane fields except checksum."""

    values = dict(payload)
    values.pop("checksum", None)
    return hashlib.sha256(_canonical_json(values)).hexdigest()


@dataclass(frozen=True)
class FeatureStateManifestV2:
    """Immutable, checksummed lifecycle record for one logical feature use."""

    schema_version: str
    manifest_id: str
    idempotency_key: str
    request_id: str
    workflow_id: str
    feature_id: str
    ordinal: int
    attempt: int
    producer_id: str
    producer_generation: str
    consumer_id: str
    consumer_generation: str
    model_fingerprint: str
    processor_fingerprint: str
    vllm_adapter_version: str
    descriptor: FeatureBundleDescriptor
    transport: Dict[str, Any]
    lease_id: str
    lease_deadline: float
    owner_id: str
    state: FeatureState
    revision: int
    created_at: float
    updated_at: float
    checksum: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_version", str(self.schema_version))
        object.__setattr__(self, "manifest_id", str(self.manifest_id))
        object.__setattr__(self, "idempotency_key", str(self.idempotency_key))
        object.__setattr__(self, "request_id", str(self.request_id))
        object.__setattr__(self, "workflow_id", str(self.workflow_id))
        object.__setattr__(self, "feature_id", str(self.feature_id))
        object.__setattr__(self, "producer_id", str(self.producer_id))
        object.__setattr__(self, "producer_generation", str(self.producer_generation))
        object.__setattr__(self, "consumer_id", str(self.consumer_id))
        object.__setattr__(self, "consumer_generation", str(self.consumer_generation))
        object.__setattr__(self, "model_fingerprint", str(self.model_fingerprint))
        object.__setattr__(self, "processor_fingerprint", str(self.processor_fingerprint))
        object.__setattr__(self, "vllm_adapter_version", str(self.vllm_adapter_version))
        object.__setattr__(self, "lease_id", str(self.lease_id))
        object.__setattr__(self, "owner_id", str(self.owner_id))
        object.__setattr__(self, "state", FeatureState(self.state))
        object.__setattr__(self, "transport", _json_copy(self.transport, label="transport"))
        object.__setattr__(self, "metadata", _json_copy(self.metadata, label="metadata"))
        if not isinstance(self.descriptor, FeatureBundleDescriptor):
            raise FeatureManifestError("descriptor must be a FeatureBundleDescriptor")
        self.validate(require_checksum=bool(self.checksum))

    @classmethod
    def from_descriptor(
        cls,
        descriptor: FeatureBundleDescriptor,
        *,
        request_id: str,
        workflow_id: str,
        ordinal: int,
        attempt: int = 0,
        producer_id: str = "encoder",
        producer_generation: str = "unknown",
        consumer_id: str,
        consumer_generation: str = "unknown",
        vllm_adapter_version: str = "unknown",
        lease_seconds: float = 60.0,
        owner_id: str = "proxy",
        idempotency_key: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        now: Optional[float] = None,
    ) -> "FeatureStateManifestV2":
        if not isinstance(descriptor, FeatureBundleDescriptor):
            raise FeatureManifestError("descriptor must be a FeatureBundleDescriptor")
        timestamp = time.monotonic() if now is None else float(now)
        request = str(request_id or "").strip()
        workflow = str(workflow_id or "").strip()
        if not request:
            raise FeatureManifestError("request_id is required")
        if not workflow:
            raise FeatureManifestError("workflow_id is required")
        feature_id = str(descriptor.feature_id or "").strip()
        if not feature_id:
            raise FeatureManifestError("descriptor.feature_id is required")
        ordinal_i = int(ordinal)
        attempt_i = int(attempt)
        if ordinal_i < 0 or attempt_i < 0:
            raise FeatureManifestError("ordinal and attempt must be non-negative")
        key = idempotency_key or ":".join(
            (FEATURE_STATE_SCHEMA_V2, request, str(consumer_id), feature_id, str(ordinal_i), str(attempt_i))
        )
        deadline = float("inf") if lease_seconds is None else timestamp + max(0.0, float(lease_seconds))
        payload: Dict[str, Any] = {
            "schema_version": FEATURE_STATE_SCHEMA_V2,
            "manifest_id": uuid.uuid4().hex,
            "idempotency_key": str(key),
            "request_id": request,
            "workflow_id": workflow,
            "feature_id": feature_id,
            "ordinal": ordinal_i,
            "attempt": attempt_i,
            "producer_id": str(producer_id or "encoder"),
            "producer_generation": str(producer_generation or "unknown"),
            "consumer_id": str(consumer_id or "unknown"),
            "consumer_generation": str(consumer_generation or "unknown"),
            "model_fingerprint": str(descriptor.model_fingerprint or "unknown"),
            "processor_fingerprint": str(descriptor.processor_fingerprint or "unknown"),
            "vllm_adapter_version": str(vllm_adapter_version or "unknown"),
            "descriptor": descriptor,
            "transport": {},
            "lease_id": f"feature:{request}:{ordinal_i}:{uuid.uuid4().hex}",
            "lease_deadline": deadline,
            "owner_id": str(owner_id or "proxy"),
            "state": FeatureState.DESCRIBED,
            "revision": 0,
            "created_at": timestamp,
            "updated_at": timestamp,
            "checksum": "",
            "metadata": dict(metadata or {}),
        }
        checksum = feature_manifest_checksum(cls._payload_from_values(payload))
        return cls(**{**payload, "checksum": checksum})

    @staticmethod
    def _payload_from_values(values: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(values)
        descriptor = payload.get("descriptor")
        if isinstance(descriptor, FeatureBundleDescriptor):
            payload["descriptor"] = descriptor.to_dict()
        state = payload.get("state")
        if isinstance(state, FeatureState):
            payload["state"] = state.value
        return payload

    def as_control_payload(self) -> Dict[str, Any]:
        return self._payload_from_values(
            {
                "schema_version": self.schema_version,
                "manifest_id": self.manifest_id,
                "idempotency_key": self.idempotency_key,
                "request_id": self.request_id,
                "workflow_id": self.workflow_id,
                "feature_id": self.feature_id,
                "ordinal": int(self.ordinal),
                "attempt": int(self.attempt),
                "producer_id": self.producer_id,
                "producer_generation": self.producer_generation,
                "consumer_id": self.consumer_id,
                "consumer_generation": self.consumer_generation,
                "model_fingerprint": self.model_fingerprint,
                "processor_fingerprint": self.processor_fingerprint,
                "vllm_adapter_version": self.vllm_adapter_version,
                "descriptor": self.descriptor,
                "transport": dict(self.transport),
                "lease_id": self.lease_id,
                "lease_deadline": None if self.lease_deadline == float("inf") else self.lease_deadline,
                "owner_id": self.owner_id,
                "state": self.state,
                "revision": int(self.revision),
                "created_at": float(self.created_at),
                "updated_at": float(self.updated_at),
                "checksum": self.checksum,
                "metadata": dict(self.metadata),
            }
        )

    @classmethod
    def from_control_payload(cls, payload: Mapping[str, Any]) -> "FeatureStateManifestV2":
        if not isinstance(payload, Mapping):
            raise FeatureManifestError("feature state manifest must be an object")
        values = dict(payload)
        allowed = {
            "schema_version",
            "manifest_id",
            "idempotency_key",
            "request_id",
            "workflow_id",
            "feature_id",
            "ordinal",
            "attempt",
            "producer_id",
            "producer_generation",
            "consumer_id",
            "consumer_generation",
            "model_fingerprint",
            "processor_fingerprint",
            "vllm_adapter_version",
            "descriptor",
            "transport",
            "lease_id",
            "lease_deadline",
            "owner_id",
            "state",
            "revision",
            "created_at",
            "updated_at",
            "checksum",
            "metadata",
        }
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise FeatureManifestError("unknown manifest fields: " + ", ".join(unknown))
        try:
            descriptor = FeatureBundleDescriptor.from_dict(dict(values.get("descriptor") or {}))
            manifest = cls(
                schema_version=str(values.get("schema_version") or ""),
                manifest_id=str(values.get("manifest_id") or ""),
                idempotency_key=str(values.get("idempotency_key") or ""),
                request_id=str(values.get("request_id") or ""),
                workflow_id=str(values.get("workflow_id") or ""),
                feature_id=str(values.get("feature_id") or ""),
                ordinal=int(values.get("ordinal", -1)),
                attempt=int(values.get("attempt", -1)),
                producer_id=str(values.get("producer_id") or ""),
                producer_generation=str(values.get("producer_generation") or ""),
                consumer_id=str(values.get("consumer_id") or ""),
                consumer_generation=str(values.get("consumer_generation") or ""),
                model_fingerprint=str(values.get("model_fingerprint") or ""),
                processor_fingerprint=str(values.get("processor_fingerprint") or ""),
                vllm_adapter_version=str(values.get("vllm_adapter_version") or ""),
                descriptor=descriptor,
                transport=dict(values.get("transport") or {}),
                lease_id=str(values.get("lease_id") or ""),
                lease_deadline=(
                    float("inf") if values.get("lease_deadline") is None else float(values.get("lease_deadline"))
                ),
                owner_id=str(values.get("owner_id") or ""),
                state=FeatureState(str(values.get("state") or "")),
                revision=int(values.get("revision", -1)),
                created_at=float(values.get("created_at", 0.0)),
                updated_at=float(values.get("updated_at", 0.0)),
                checksum=str(values.get("checksum") or ""),
                metadata=dict(values.get("metadata") or {}),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, FeatureManifestError):
                raise
            raise FeatureManifestError(f"invalid FeatureStateV2 manifest: {exc}") from exc
        manifest.validate(require_checksum=True)
        return manifest

    def canonical_payload(self) -> Dict[str, Any]:
        payload = self.as_control_payload()
        payload.pop("checksum", None)
        return payload

    def manifest_checksum(self) -> str:
        return feature_manifest_checksum(self.as_control_payload())

    @property
    def expired(self) -> bool:
        return self.lease_deadline != float("inf") and time.monotonic() >= self.lease_deadline

    def validate(
        self,
        *,
        now: Optional[float] = None,
        require_checksum: bool = True,
        require_compatibility: bool = False,
        allow_expired: bool = True,
    ) -> None:
        required = {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "idempotency_key": self.idempotency_key,
            "request_id": self.request_id,
            "workflow_id": self.workflow_id,
            "feature_id": self.feature_id,
            "producer_id": self.producer_id,
            "producer_generation": self.producer_generation,
            "consumer_id": self.consumer_id,
            "consumer_generation": self.consumer_generation,
            "vllm_adapter_version": self.vllm_adapter_version,
            "lease_id": self.lease_id,
            "owner_id": self.owner_id,
        }
        missing = [name for name, value in required.items() if not str(value).strip()]
        if missing:
            raise FeatureManifestError("missing required manifest fields: " + ", ".join(missing))
        if self.schema_version != FEATURE_STATE_SCHEMA_V2:
            raise FeatureManifestError(
                f"unsupported feature state schema: {self.schema_version!r}"
            )
        if int(self.ordinal) < 0 or int(self.attempt) < 0 or int(self.revision) < 0:
            raise FeatureManifestError("ordinal, attempt, and revision must be non-negative")
        if float(self.created_at) <= 0.0 or float(self.updated_at) < float(self.created_at):
            raise FeatureManifestError("invalid manifest timestamps")
        if self.lease_deadline != float("inf") and float(self.lease_deadline) < float(self.created_at):
            raise FeatureManifestError("lease_deadline precedes created_at")
        if self.feature_id != self.descriptor.feature_id:
            raise FeatureManifestError(
                "manifest feature id does not match descriptor: "
                f"manifest={self.feature_id} descriptor={self.descriptor.feature_id}"
            )
        if int(self.descriptor.last_hidden.nbytes) <= 0:
            raise FeatureManifestError("descriptor.last_hidden must have positive nbytes")
        if require_compatibility:
            unknown = {"", "unknown"}
            if self.model_fingerprint in unknown or self.processor_fingerprint in unknown:
                raise FeatureManifestError("model and processor fingerprints are required in strict mode")
            if self.vllm_adapter_version in unknown:
                raise FeatureManifestError("vLLM adapter version is required in strict mode")
        current = time.monotonic() if now is None else float(now)
        if not allow_expired and self.lease_deadline != float("inf") and current >= self.lease_deadline:
            raise FeatureManifestError(f"feature state lease expired: {self.lease_id}")
        if require_checksum:
            if not self.checksum:
                raise FeatureManifestError("manifest checksum is required")
            if self.checksum != self.manifest_checksum():
                raise FeatureManifestError("feature state manifest checksum mismatch")

    def transition(
        self,
        target_state: FeatureState | str,
        *,
        expected_revision: Optional[int] = None,
        transport_patch: Optional[Mapping[str, Any]] = None,
        metadata_patch: Optional[Mapping[str, Any]] = None,
        now: Optional[float] = None,
    ) -> "FeatureStateManifestV2":
        target = FeatureState(target_state)
        if expected_revision is not None and int(expected_revision) != int(self.revision):
            raise FeatureStateConflictError(
                f"manifest revision mismatch: expected={expected_revision} actual={self.revision}"
            )
        if target == self.state:
            return self
        if target not in _ALLOWED_TRANSITIONS[self.state]:
            raise FeatureStateTransitionError(
                f"invalid FeatureStateV2 transition: {self.state.value} -> {target.value}"
            )
        transition_time = time.monotonic() if now is None else float(now)
        transport = dict(self.transport)
        transport.update(dict(transport_patch or {}))
        metadata = dict(self.metadata)
        metadata.update(dict(metadata_patch or {}))
        payload = self.as_control_payload()
        payload.update(
            {
                "state": target.value,
                "revision": int(self.revision) + 1,
                "updated_at": transition_time,
                "transport": transport,
                "metadata": metadata,
                "checksum": "",
            }
        )
        payload["checksum"] = feature_manifest_checksum(payload)
        return FeatureStateManifestV2.from_control_payload(payload)

    def validate_handle(
        self,
        *,
        feature_id: str,
        descriptor: FeatureBundleDescriptor,
        consumer_id: str,
        consumer_generation: Optional[str] = None,
        require_compatibility: bool = False,
        now: Optional[float] = None,
    ) -> None:
        self.validate(
            now=now,
            require_checksum=True,
            require_compatibility=require_compatibility,
            allow_expired=False,
        )
        if self.state not in _CONSUMABLE_STATES:
            raise FeatureManifestError(
                f"feature state is not consumable: state={self.state.value} manifest={self.manifest_id}"
            )
        if str(feature_id) != self.feature_id:
            raise FeatureManifestError(
                f"feature handle id mismatch: manifest={self.feature_id} handle={feature_id}"
            )
        if descriptor.to_dict() != self.descriptor.to_dict():
            raise FeatureManifestError("feature handle descriptor does not match FeatureStateV2 manifest")
        if str(consumer_id) != self.consumer_id:
            raise FeatureManifestError(
                f"feature state consumer mismatch: manifest={self.consumer_id} worker={consumer_id}"
            )
        if consumer_generation is not None and str(consumer_generation) != self.consumer_generation:
            raise FeatureManifestError(
                "feature state consumer generation mismatch: "
                f"manifest={self.consumer_generation} worker={consumer_generation}"
            )

    def identity_payload(self) -> Dict[str, Any]:
        """Fields that must remain stable for one idempotency key."""

        return {
            "schema_version": self.schema_version,
            "idempotency_key": self.idempotency_key,
            "request_id": self.request_id,
            "workflow_id": self.workflow_id,
            "feature_id": self.feature_id,
            "ordinal": int(self.ordinal),
            "attempt": int(self.attempt),
            "producer_id": self.producer_id,
            "producer_generation": self.producer_generation,
            "consumer_id": self.consumer_id,
            "consumer_generation": self.consumer_generation,
            "model_fingerprint": self.model_fingerprint,
            "processor_fingerprint": self.processor_fingerprint,
            "vllm_adapter_version": self.vllm_adapter_version,
            "descriptor": self.descriptor.to_dict(),
            "owner_id": self.owner_id,
        }


@dataclass(frozen=True)
class FeatureStateTransition:
    manifest_id: str
    from_state: Optional[FeatureState]
    to_state: FeatureState
    revision: int
    timestamp: float
    reason: str = ""


class FeatureStateRegistry:
    """Thread-safe in-process ledger for FeatureStateV2 control transitions.

    This deliberately does not claim distributed durability.  It gives the
    proxy and tests compare-and-swap, idempotency, expiry, and audit semantics
    now; PR-4 replaces the in-process catalog with a leased/WAL-backed owner.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._manifests: Dict[str, FeatureStateManifestV2] = {}
        self._idempotency: Dict[str, str] = {}
        self._history: Dict[str, List[FeatureStateTransition]] = {}

    def register(self, manifest: FeatureStateManifestV2) -> FeatureStateManifestV2:
        manifest.validate(require_checksum=True)
        with self._lock:
            existing_id = self._idempotency.get(manifest.idempotency_key)
            if existing_id is not None:
                existing = self._manifests[existing_id]
                if existing.identity_payload() != manifest.identity_payload():
                    raise FeatureStateConflictError(
                        f"idempotency key conflicts with manifest={existing.manifest_id}"
                    )
                return existing
            existing = self._manifests.get(manifest.manifest_id)
            if existing is not None:
                if existing.checksum != manifest.checksum:
                    raise FeatureStateConflictError(
                        f"manifest id already registered with different checksum: {manifest.manifest_id}"
                    )
                return existing
            self._manifests[manifest.manifest_id] = manifest
            self._idempotency[manifest.idempotency_key] = manifest.manifest_id
            self._history[manifest.manifest_id] = [
                FeatureStateTransition(
                    manifest_id=manifest.manifest_id,
                    from_state=None,
                    to_state=manifest.state,
                    revision=manifest.revision,
                    timestamp=manifest.created_at,
                    reason="registered",
                )
            ]
            return manifest

    def get(self, manifest_id: str) -> FeatureStateManifestV2:
        with self._lock:
            try:
                return self._manifests[str(manifest_id)]
            except KeyError as exc:
                raise KeyError(f"unknown FeatureStateV2 manifest: {manifest_id}") from exc

    def transition(
        self,
        manifest_id: str,
        target_state: FeatureState | str,
        *,
        expected_revision: Optional[int] = None,
        transport_patch: Optional[Mapping[str, Any]] = None,
        metadata_patch: Optional[Mapping[str, Any]] = None,
        reason: str = "",
        now: Optional[float] = None,
    ) -> FeatureStateManifestV2:
        with self._lock:
            current = self.get(manifest_id)
            updated = current.transition(
                target_state,
                expected_revision=expected_revision,
                transport_patch=transport_patch,
                metadata_patch=metadata_patch,
                now=now,
            )
            if updated is current:
                return current
            self._manifests[current.manifest_id] = updated
            self._history[current.manifest_id].append(
                FeatureStateTransition(
                    manifest_id=current.manifest_id,
                    from_state=current.state,
                    to_state=updated.state,
                    revision=updated.revision,
                    timestamp=updated.updated_at,
                    reason=str(reason or ""),
                )
            )
            return updated

    def abort_and_release(
        self,
        manifest_id: str,
        *,
        expected_revision: Optional[int] = None,
        reason: str = "",
        now: Optional[float] = None,
    ) -> FeatureStateManifestV2:
        current = self.get(manifest_id)
        if current.state is FeatureState.RELEASED:
            return current
        if current.state is FeatureState.CONSUMED:
            return self.transition(
                manifest_id,
                FeatureState.RELEASED,
                expected_revision=expected_revision,
                reason=reason or "released after consume",
                now=now,
            )
        if current.state is not FeatureState.ABORTING:
            current = self.transition(
                manifest_id,
                FeatureState.ABORTING,
                expected_revision=expected_revision,
                reason=reason or "aborted",
                now=now,
            )
        return self.transition(
            manifest_id,
            FeatureState.RELEASED,
            expected_revision=current.revision,
            reason=reason or "released after abort",
            now=now,
        )

    def sweep_expired(self, *, now: Optional[float] = None) -> List[str]:
        timestamp = time.monotonic() if now is None else float(now)
        with self._lock:
            expired = [
                manifest_id
                for manifest_id, manifest in self._manifests.items()
                if manifest.state not in _TERMINAL_STATES
                and manifest.lease_deadline != float("inf")
                and manifest.lease_deadline <= timestamp
            ]
        for manifest_id in expired:
            self.abort_and_release(manifest_id, reason="lease expired", now=timestamp)
        return expired

    def history(self, manifest_id: str) -> List[FeatureStateTransition]:
        with self._lock:
            if manifest_id not in self._history:
                raise KeyError(f"unknown FeatureStateV2 manifest: {manifest_id}")
            return list(self._history[manifest_id])

    def stats(self) -> Dict[str, int]:
        with self._lock:
            manifests = list(self._manifests.values())
        stats = {state.value.lower(): 0 for state in FeatureState}
        for manifest in manifests:
            stats[manifest.state.value.lower()] += 1
        stats["manifests"] = len(manifests)
        return stats

    def manifests(self) -> Iterable[FeatureStateManifestV2]:
        with self._lock:
            return tuple(self._manifests.values())
