"""Framework-provided semantic identity for one KV-cache snapshot."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import NewType

from ..contracts import ResourceId, ResourceKind
from .types import require_integer, require_nonempty_string, require_sha256

SnapshotId = NewType("SnapshotId", str)


@dataclass(frozen=True, init=False)
class KVCacheSnapshotDescriptor:
    """Describe what KV content represents, independently of its placement."""

    namespace: str
    resource_id: ResourceId
    snapshot_id: SnapshotId
    model_id: str
    model_revision: str
    token_start: int
    token_count: int
    token_fingerprint: str
    semantic_fingerprint: str
    _digest_cache: str | None = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        namespace: str,
        resource_id: ResourceId,
        model_id: str,
        model_revision: str,
        token_start: int,
        token_count: int,
        token_fingerprint: str,
        semantic_fingerprint: str,
        snapshot_id: SnapshotId | None = None,
    ) -> None:
        for value, name in (
            (namespace, "namespace"),
            (resource_id, "resource_id"),
            (model_id, "model_id"),
            (model_revision, "model_revision"),
        ):
            require_nonempty_string(value, name)
        require_integer(token_start, "token_start")
        require_integer(token_count, "token_count", minimum=1)
        require_sha256(token_fingerprint, "token_fingerprint")
        require_sha256(semantic_fingerprint, "semantic_fingerprint")
        content = _snapshot_content(
            namespace=namespace,
            resource_id=resource_id,
            model_id=model_id,
            model_revision=model_revision,
            token_start=token_start,
            token_count=token_count,
            token_fingerprint=token_fingerprint,
            semantic_fingerprint=semantic_fingerprint,
        )
        canonical_id = SnapshotId(f"sha256:{_canonical_digest(content)}")
        if snapshot_id is not None and snapshot_id != canonical_id:
            raise ValueError("snapshot_id does not match canonical semantic content")
        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "resource_id", resource_id)
        object.__setattr__(self, "snapshot_id", canonical_id)
        object.__setattr__(self, "model_id", model_id)
        object.__setattr__(self, "model_revision", model_revision)
        object.__setattr__(self, "token_start", token_start)
        object.__setattr__(self, "token_count", token_count)
        object.__setattr__(self, "token_fingerprint", token_fingerprint)
        object.__setattr__(self, "semantic_fingerprint", semantic_fingerprint)
        object.__setattr__(self, "_digest_cache", None)

    @property
    def resource_kind(self) -> ResourceKind:
        return ResourceKind.KV_CACHE

    @property
    def token_end(self) -> int:
        return self.token_start + self.token_count

    @property
    def digest(self) -> str:
        digest = self._digest_cache
        if digest is None:
            content = _snapshot_content(
                namespace=self.namespace,
                resource_id=self.resource_id,
                model_id=self.model_id,
                model_revision=self.model_revision,
                token_start=self.token_start,
                token_count=self.token_count,
                token_fingerprint=self.token_fingerprint,
                semantic_fingerprint=self.semantic_fingerprint,
            )
            content["snapshot_id"] = self.snapshot_id
            digest = _canonical_digest(content)
            object.__setattr__(self, "_digest_cache", digest)
        return digest


def _snapshot_content(
    *,
    namespace: str,
    resource_id: ResourceId,
    model_id: str,
    model_revision: str,
    token_start: int,
    token_count: int,
    token_fingerprint: str,
    semantic_fingerprint: str,
) -> dict[str, object]:
    return {
        "schema": "kv-cache-snapshot",
        "resource_kind": ResourceKind.KV_CACHE.value,
        "namespace": namespace,
        "resource_id": resource_id,
        "model_id": model_id,
        "model_revision": model_revision,
        "token_start": token_start,
        "token_count": token_count,
        "token_fingerprint": token_fingerprint,
        "semantic_fingerprint": semantic_fingerprint,
    }


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["KVCacheSnapshotDescriptor", "SnapshotId"]
