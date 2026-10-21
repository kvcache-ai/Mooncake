"""Shared, address-free KV Store layout and model metadata; no page snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from functools import cached_property

from ..identity import KVCacheModelDomain
from ..types import (
    KVCacheComponent,
    KVCacheDescriptor,
    require_integer,
    require_manifest_items,
    require_nonempty_string,
    require_sha256,
)
from ._utils import KVCacheLogicalShard, validate_logical_coverage
from ._utils import canonical_digest as _canonical_digest


class KVCacheStoreFormat(str, Enum):
    """Axis order within each K or V component of a Part; D is contiguous."""

    PLHD = "PLHD"  # [token, layer, head, dim]
    LPHD = "LPHD"  # [layer, token, head, dim]
    HPLD = "HPLD"  # [head, token, layer, dim]


@dataclass(frozen=True)
class KVCacheStoreShard:
    """Logical coverage of one Part. key_suffix is its stable writer label,
    retained in the manifest wire format; it is never appended to a page key."""

    layer_ids: tuple[int, ...]
    head_start: int
    head_count: int
    key_suffix: str

    def __post_init__(self) -> None:
        require_nonempty_string(self.key_suffix, "key_suffix")
        layers = require_manifest_items(self.layer_ids, "shard layers", int)
        if not layers or len(set(layers)) != len(layers):
            raise ValueError("shard layers must be nonempty and unique")
        for layer in layers:
            require_integer(layer, "global_layer_id")
        require_integer(self.head_start, "head_start")
        require_integer(self.head_count, "head_count", minimum=1)
        require_integer(self.head_start + self.head_count, "head_end")
        object.__setattr__(self, "layer_ids", tuple(sorted(layers)))

    @property
    def logical_shard(self) -> KVCacheLogicalShard:
        """Project one durable logical copy without its Store key suffix."""
        return KVCacheLogicalShard(self.layer_ids, self.head_start, self.head_count)


@dataclass(frozen=True)
class KVCacheStoreFragment:
    """Relative byte mapping for one layer/component and a head interval."""

    global_layer_id: int
    component: KVCacheComponent
    head_start: int
    head_count: int
    part_index: int
    part_offset: int
    token_stride_bytes: int
    head_stride_bytes: int

    def __post_init__(self) -> None:
        for name in ("global_layer_id", "head_start", "part_index", "part_offset"):
            require_integer(getattr(self, name), name)
        for name in ("head_count", "token_stride_bytes", "head_stride_bytes"):
            require_integer(getattr(self, name), name, minimum=1)
        require_integer(self.head_start + self.head_count, "head_end")
        if not isinstance(self.component, KVCacheComponent):
            raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004


@dataclass(frozen=True)
class KVCacheStoreLayout:
    """A fixed-page object template independent of models, pages and workers.

    Persist only shard geometry and axis order. Sizes and strides are derived,
    so a manifest cannot give conflicting descriptions of the same bytes.
    """

    descriptor: KVCacheDescriptor
    object_format: KVCacheStoreFormat
    shards: tuple[KVCacheStoreShard, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.descriptor, KVCacheDescriptor):
            raise ValueError("descriptor must be a KVCacheDescriptor")  # noqa: TRY004
        if not isinstance(self.object_format, KVCacheStoreFormat):
            raise ValueError(  # noqa: TRY004
                "object_format must be a KVCacheStoreFormat"
            )
        shards = require_manifest_items(self.shards, "Store shards", KVCacheStoreShard)
        if len({shard.key_suffix for shard in shards}) != len(shards):
            raise ValueError("Store shard key suffixes must be unique")
        if not shards or len(shards) * 2 > 100_000:
            raise ValueError("Store object count limit exceeded or empty layout")
        if sum(len(shard.layer_ids) * 2 for shard in shards) > 100_000:
            raise ValueError("Store fragment count limit exceeded")
        object.__setattr__(
            self,
            "shards",
            tuple(sorted(shards, key=lambda s: (s.layer_ids, s.head_start))),
        )
        _validate_layout_coverage(self)
        if sum(self.part_sizes) > 1 << 40:
            raise ValueError("Store layout byte limit exceeded")

    @property
    def token_count(self) -> int:
        """Every object contains exactly one full page, never a variable tail."""
        return self.descriptor.page_size

    @property
    def part_sizes(self) -> tuple[int, ...]:
        """Each Part stores all K bytes followed by all V bytes."""
        d = self.descriptor
        return tuple(
            require_integer(
                len(shard.layer_ids)
                * self.token_count
                * shard.head_count
                * (d.key_head_dim + d.value_head_dim)
                * d.itemsize,
                "object size",
                minimum=1,
            )
            for shard in self.shards
        )

    @property
    def fragments(self) -> tuple[KVCacheStoreFragment, ...]:
        """Derive affine offsets without enumerating tokens or individual bytes."""
        result: list[KVCacheStoreFragment] = []
        d = self.descriptor
        for slot, shard in enumerate(self.shards):
            layers, heads, tokens = (
                len(shard.layer_ids),
                shard.head_count,
                self.token_count,
            )
            for component_index, component in enumerate(KVCacheComponent):
                dim = (
                    d.key_head_dim
                    if component is KVCacheComponent.KEY
                    else d.value_head_dim
                )
                head_bytes = dim * d.itemsize
                if self.object_format is KVCacheStoreFormat.PLHD:
                    layer_stride, token_stride, head_stride = (
                        heads * head_bytes,
                        layers * heads * head_bytes,
                        head_bytes,
                    )
                elif self.object_format is KVCacheStoreFormat.LPHD:
                    layer_stride, token_stride, head_stride = (
                        tokens * heads * head_bytes,
                        heads * head_bytes,
                        head_bytes,
                    )
                else:
                    layer_stride, token_stride, head_stride = (
                        head_bytes,
                        layers * head_bytes,
                        tokens * layers * head_bytes,
                    )
                for local_layer, global_layer in enumerate(shard.layer_ids):
                    result.append(
                        KVCacheStoreFragment(
                            global_layer,
                            component,
                            shard.head_start,
                            heads,
                            slot,
                            local_layer * layer_stride
                            + (
                                layers * tokens * heads * d.key_head_dim * d.itemsize
                                if component_index
                                else 0
                            ),
                            token_stride,
                            head_stride,
                        )
                    )
        return tuple(result)

    @cached_property
    def digest(self) -> str:
        return _canonical_digest(asdict(self))

    @property
    def layout_id(self) -> str:
        return f"sha256:{self.digest}"


def _validate_layout_coverage(layout: KVCacheStoreLayout) -> None:
    """Prove each global layer/head occurs once; dense formats then cover bytes."""
    validate_logical_coverage(
        layout.descriptor,
        tuple(shard.logical_shard for shard in layout.shards),
        label="Store layout",
    )


@dataclass(frozen=True)
class KVCacheStoreManifest:
    """One immutable layout record shared by pages stored in that layout.

    Constructing it does not publish a layout or prove any payload exists.
    The page ObjectMetadata binds manifest_key at its first Part write.
    """

    namespace: str
    model_id: str
    model_revision: str
    semantic_fingerprint: str
    layout: KVCacheStoreLayout

    def __post_init__(self) -> None:
        for name in ("namespace", "model_id", "model_revision", "semantic_fingerprint"):
            require_nonempty_string(getattr(self, name), name)
        if not isinstance(self.layout, KVCacheStoreLayout):
            raise ValueError("layout must be a KVCacheStoreLayout")  # noqa: TRY004

    @cached_property
    def model_domain(self) -> str:
        """Share discovery across TP/PP geometry and physical axis orders."""
        return KVCacheModelDomain(
            self.namespace,
            self.model_id,
            self.model_revision,
            self.semantic_fingerprint,
            self.layout.descriptor,
        ).digest

    @property
    def manifest_key(self) -> str:
        return _manifest_key(self.model_domain, self.layout.layout_id)

    @cached_property
    def digest(self) -> str:
        return _canonical_digest(asdict(self))


def _layout_digest(layout_id: str) -> str:
    """Decode a canonical layout ID before using it in object keys."""
    require_nonempty_string(layout_id, "layout_id")
    if not layout_id.startswith("sha256:"):
        raise ValueError("layout_id must start with sha256:")
    return require_sha256(layout_id[7:], "layout digest")


def _manifest_key(model_domain: str, layout_id: str) -> str:
    """Content-addressed manifest location shared by all pages of a layout."""
    require_sha256(model_domain, "model_domain")
    return f"kv/{model_domain}/manifests/{_layout_digest(layout_id)}"


__all__ = [
    "KVCacheStoreFormat",
    "KVCacheStoreFragment",
    "KVCacheStoreLayout",
    "KVCacheStoreManifest",
    "KVCacheStoreShard",
]
