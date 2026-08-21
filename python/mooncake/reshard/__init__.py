"""Framework-neutral contracts for reusable model runtime resources."""

from .contracts import (
    ResourceKind,
    ResourceManifest,
    PlacementManifest,
    RuntimeBindingManifest,
)

__all__ = [
    "ResourceKind",
    "ResourceManifest",
    "PlacementManifest",
    "RuntimeBindingManifest",
]
