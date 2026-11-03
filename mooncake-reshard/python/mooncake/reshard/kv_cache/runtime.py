"""Canonical per-participant KV-cache runtime binding contracts."""

from __future__ import annotations

from dataclasses import dataclass

from ..contracts import (
    ParticipantId,
    PlacementId,
    ResourceId,
    ResourceKind,
    RevisionId,
    RuntimeInstanceId,
)
from .snapshot import SnapshotId
from .types import (
    KVCacheComponent,
    KVCacheRuntimeBuffer,
    require_integer,
    require_manifest_items,
    require_nonempty_string,
    require_sha256,
)


@dataclass(frozen=True)
class KVCacheBufferBinding:
    global_layer_id: int
    component: KVCacheComponent
    fragment: KVCacheRuntimeBuffer

    def __post_init__(self) -> None:
        require_integer(self.global_layer_id, "global_layer_id")
        if not isinstance(self.component, KVCacheComponent):
            raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004
        if not isinstance(self.fragment, KVCacheRuntimeBuffer):
            raise ValueError(  # noqa: TRY004
                "fragment must be a KVCacheRuntimeBuffer"
            )


@dataclass(frozen=True)
class KVCacheRuntimeBindingManifest:
    """Operation-scoped physical buffers supplied by their owning runtime."""

    resource_id: ResourceId
    placement_id: PlacementId
    placement_digest: str
    instance_id: RuntimeInstanceId
    revision: RevisionId
    participant_id: ParticipantId
    buffers: tuple[KVCacheBufferBinding, ...]
    snapshot_id: SnapshotId | None = None
    snapshot_digest: str | None = None

    @property
    def resource_kind(self) -> ResourceKind:
        return ResourceKind.KV_CACHE

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "placement_id",
            "instance_id",
            "revision",
            "participant_id",
        ):
            require_nonempty_string(getattr(self, name), name)
        require_sha256(self.placement_digest, "placement_digest")
        if (self.snapshot_id is None) != (self.snapshot_digest is None):
            raise ValueError(
                "snapshot_id and snapshot_digest must be provided together"
            )
        if self.snapshot_id is not None:
            require_nonempty_string(self.snapshot_id, "snapshot_id")
            require_sha256(self.snapshot_digest, "snapshot_digest")
        buffers = tuple(
            sorted(
                require_manifest_items(
                    self.buffers,
                    "KVCacheRuntimeBindingManifest buffers",
                    KVCacheBufferBinding,
                ),
                key=lambda item: (item.global_layer_id, item.component.value),
            )
        )
        keys = [(item.global_layer_id, item.component) for item in buffers]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate KV-cache buffer binding")
        placement_ids = [item.fragment.placement_fragment_id for item in buffers]
        if len(placement_ids) != len(set(placement_ids)):
            raise ValueError("duplicate placement fragment in runtime binding")
        fragment_ids = [item.fragment.fragment_id for item in buffers]
        if len(fragment_ids) != len(set(fragment_ids)):
            raise ValueError("duplicate runtime fragment_id in runtime binding")
        object.__setattr__(self, "buffers", buffers)

    def buffer(
        self, global_layer_id: int, component: KVCacheComponent
    ) -> KVCacheBufferBinding:
        for item in self.buffers:
            if item.global_layer_id == global_layer_id and item.component is component:
                return item
        raise ValueError(
            f"missing KV-cache buffer: layer={global_layer_id} "
            f"component={component.value}"
        )


# RFC name; keep the prototype name as a compatibility alias.
KVCacheRuntimeBinding = KVCacheRuntimeBindingManifest


__all__ = [
    "KVCacheBufferBinding",
    "KVCacheRuntimeBinding",
    "KVCacheRuntimeBindingManifest",
]
