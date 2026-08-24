"""Structural manifest identity and lifecycle contracts."""

from __future__ import annotations

from enum import Enum
from typing import Protocol

from .ids import LeaseId, PlacementId, ResourceId, RuntimeInstanceId


class ResourceKind(str, Enum):
    """Stable discriminator for a reusable runtime resource."""

    MODEL_WEIGHT = "model_weight"
    KV_CACHE = "kv_cache"


class ResourceManifest(Protocol):
    """Structural contract for a typed reusable resource."""

    @property
    def resource_id(self) -> ResourceId:
        """Return the stable canonical resource identity."""
        ...

    @property
    def resource_kind(self) -> ResourceKind:
        """Return the stable discriminator for the concrete resource."""
        ...


class PlacementManifest(ResourceManifest, Protocol):
    """Address-free logical placement shared by reusable resources."""

    @property
    def placement_id(self) -> PlacementId:
        """Return the stable logical placement identity."""
        ...

    @property
    def digest(self) -> str:
        """Return the digest that attests the serialized placement."""
        ...


class RuntimeBindingManifest(ResourceManifest, Protocol):
    """Physical locations and lifetime fences for one placement."""

    @property
    def placement_id(self) -> PlacementId:
        """Return the logical placement this runtime state attests."""
        ...

    @property
    def placement_digest(self) -> str:
        """Return the digest of the attested logical placement."""
        ...

    @property
    def instance_id(self) -> RuntimeInstanceId:
        """Return the runtime instance that owns this binding."""
        ...

    @property
    def generation(self) -> int:
        """Return the runtime generation fence."""
        ...

    @property
    def lease_id(self) -> LeaseId:
        """Return the live lease fence."""
        ...


class StoredResourceManifest(ResourceManifest, Protocol):
    """Structural contract for persistent reusable-resource metadata."""

    namespace: str
    group_id: str
    manifest_key: str
    created_at: str


def validate_resource_binding_identity(
    placement: PlacementManifest,
    binding: RuntimeBindingManifest,
) -> None:
    """Fence a physical binding to the exact typed logical placement."""

    if placement.resource_kind != binding.resource_kind:
        raise ValueError("placement and runtime binding resource_kind differ")
    if placement.resource_id != binding.resource_id:
        raise ValueError("placement and runtime binding resource_id differ")
    if placement.placement_id != binding.placement_id:
        raise ValueError("placement_id and runtime binding placement_id differ")
    if placement.digest != binding.placement_digest:
        raise ValueError("placement digest and runtime binding placement digest differ")
