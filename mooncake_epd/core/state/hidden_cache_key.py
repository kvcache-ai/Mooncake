"""Canonical, content-bound keys for exact hidden-state cache reuse.

The key deliberately consumes request/asset identity metadata rather than a
model input tensor.  Hashing a full CUDA tensor on every cache lookup creates a
hidden device-to-host synchronization point and is not an acceptable L2 cache
hot path.  Callers must propagate a stable asset/content id at ingest time.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


class HiddenCacheKeyError(ValueError):
    """Raised when an exact hidden-state cache identity is underspecified."""


def _required(name: str, value: object) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "unknown":
        raise HiddenCacheKeyError(f"{name} is required for exact distributed cache reuse")
    return text


def _json_value(value: Any) -> Any:
    """Reject non-canonical metadata instead of silently stringifying it."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise HiddenCacheKeyError(
        f"relevant cache kwargs must be JSON scalars/mappings/sequences, got {type(value).__name__}"
    )


def canonical_mm_processor_kwargs(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a canonical request-level multimodal preprocessing projection.

    vLLM's ``mm_processor_kwargs`` changes the pixels and grid that reach a
    vision tower (for example, Qwen's ``max_pixels``).  It is therefore part of
    the identity of any precomputed hidden state, not merely request metadata.
    Keep this helper deliberately model-agnostic: model adapters validate and
    apply the supported subset, while the identity includes every JSON-safe
    request field so an unknown future option cannot accidentally alias an old
    feature buffer.
    """

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise HiddenCacheKeyError("mm_processor_kwargs must be a mapping")
    normalized = _json_value(dict(value))
    if not isinstance(normalized, dict):  # pragma: no cover - _json_value contract
        raise HiddenCacheKeyError("mm_processor_kwargs did not normalize to a mapping")
    return normalized


def stable_multimodal_asset_hash(item: Mapping[str, Any]) -> str:
    """Return the legacy stable content identity for one multimodal item.

    The no-preprocessing-config case intentionally keeps the original 16-hex
    identity used by the serving control plane.  This preserves compatibility
    with existing FeatureHandles while allowing the feature identity below to
    extend it when preprocessing is explicitly configured.
    """

    payload = {
        key: item.get(key)
        for key in sorted(item)
        if key not in {"detail"}
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def stable_multimodal_feature_id(
    item: Mapping[str, Any],
    mm_processor_kwargs: Mapping[str, Any] | None = None,
) -> str:
    """Return an exact feature identity for a request multimodal item.

    The image bytes alone are insufficient when the same asset is submitted at
    two resize/grid settings.  Returning the legacy asset hash for an empty
    preprocessing projection avoids needless cache churn for existing callers;
    otherwise a compact derived identity protects Encoder, Prefill, and vLLM
    caches from cross-grid hidden-state reuse.
    """

    asset_hash = stable_multimodal_asset_hash(item)
    kwargs = canonical_mm_processor_kwargs(mm_processor_kwargs)
    if not kwargs:
        return asset_hash
    raw = json.dumps(
        {
            "asset_hash": asset_hash,
            "mm_processor_kwargs": kwargs,
        },
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass(frozen=True)
class HiddenStateCacheKeyV2:
    """All semantics that may change an exact hidden-state result.

    ``asset_hash`` is an ingest-time content hash (or an equivalently strong
    immutable vLLM feature identity), not a path or an opaque request id.  The
    rest of the fields make cache invalidation explicit across model/processor
    upgrades, grid changes and output-schema revisions.
    """

    asset_hash: str
    modality: str
    model_id: str
    model_revision: str
    processor_revision: str
    dtype: str
    output_schema: str
    grid_thw: tuple[int, ...] = ()
    temporal_patch: str = ""
    relevant_kwargs: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = "mooncake-hidden-cache-key-v2"

    def __post_init__(self) -> None:
        for name in (
            "asset_hash",
            "modality",
            "model_id",
            "model_revision",
            "processor_revision",
            "dtype",
            "output_schema",
            "schema_version",
        ):
            object.__setattr__(self, name, _required(name, getattr(self, name)))
        modality = self.modality.lower()
        if modality not in {"image", "audio", "video", "multimodal"}:
            raise HiddenCacheKeyError(f"unsupported hidden-state modality: {self.modality}")
        object.__setattr__(self, "modality", modality)
        grid = tuple(int(value) for value in self.grid_thw)
        if any(value < 0 for value in grid):
            raise HiddenCacheKeyError("grid_thw must contain non-negative values")
        if modality in {"image", "audio", "video"} and not grid:
            raise HiddenCacheKeyError("grid_thw is required for exact modality cache reuse")
        object.__setattr__(self, "grid_thw", grid)
        object.__setattr__(self, "temporal_patch", str(self.temporal_patch or ""))
        object.__setattr__(self, "relevant_kwargs", _json_value(dict(self.relevant_kwargs)))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "asset_hash": self.asset_hash,
            "modality": self.modality,
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "processor_revision": self.processor_revision,
            "dtype": self.dtype,
            "output_schema": self.output_schema,
            "grid_thw": list(self.grid_thw),
            "temporal_patch": self.temporal_patch,
            "relevant_kwargs": _json_value(dict(self.relevant_kwargs)),
        }

    @property
    def digest(self) -> str:
        raw = json.dumps(
            self.canonical_payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @property
    def cache_id(self) -> str:
        return self.digest

    @classmethod
    def from_vllm_stable_identity(
        cls,
        *,
        stable_asset_hash: str,
        modality: str,
        model_id: str,
        model_revision: str,
        processor_revision: str,
        dtype: str,
        output_schema: str,
        grid_thw: Sequence[int],
        temporal_patch: str = "",
        relevant_kwargs: Mapping[str, Any] | None = None,
    ) -> "HiddenStateCacheKeyV2":
        """Build a key without reading or hashing a model input tensor."""

        return cls(
            asset_hash=stable_asset_hash,
            modality=modality,
            model_id=model_id,
            model_revision=model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            output_schema=output_schema,
            grid_thw=tuple(int(value) for value in grid_thw),
            temporal_patch=temporal_patch,
            relevant_kwargs=dict(relevant_kwargs or {}),
        )
