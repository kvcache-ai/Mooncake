"""Resource-neutral manifest identity and lifecycle contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional


_MAX_U64 = (1 << 64) - 1


class ResourceKind(str, Enum):
    """Stable discriminator for a reusable runtime resource."""

    MODEL_WEIGHT = "model_weight"
    KV_CACHE = "kv_cache"


@dataclass(frozen=True)
class ResourceManifest(ABC):
    """Base contract for a typed reusable resource."""

    resource_id: str

    def __post_init__(self) -> None:
        _require_nonempty_string(self.resource_id, "resource_id")

    @property
    @abstractmethod
    def resource_kind(self) -> ResourceKind:
        """Return the stable discriminator for the concrete resource."""


@dataclass(frozen=True)
class PlacementManifest(ResourceManifest, ABC):
    """Address-free logical placement shared by reusable resources."""

    placement_id: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.placement_id is not None:
            _require_nonempty_string(self.placement_id, "placement_id")

    @property
    @abstractmethod
    def digest(self) -> str:
        """Return the digest that attests the serialized placement."""


@dataclass(frozen=True)
class RuntimeBindingManifest(ResourceManifest, ABC):
    """Physical locations and lifetime fences for one placement."""

    placement_id: str
    placement_digest: str
    instance_id: str
    generation: int
    lease_id: str

    def __post_init__(self) -> None:
        super().__post_init__()
        for name in ("placement_id", "instance_id", "lease_id"):
            _require_nonempty_string(getattr(self, name), name)
        _require_sha256_digest(self.placement_digest, "placement_digest")
        _require_u64(self.generation, "generation")


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


def _require_nonempty_string(value: object, name: str) -> None:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _require_u64(value: object, name: str) -> None:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be at least 0")
    if value > _MAX_U64:
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")


def _require_sha256_digest(value: object, name: str) -> None:
    _require_nonempty_string(value, name)
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
