"""Shared helpers for the Store-reshard test suites."""

from __future__ import annotations

import struct

from mooncake.reshard.kv_cache.store_reshard import (
    KVCacheHostLayout,
    ModelSemanticDomain,
    SourceLayoutManifest,
    SourcePartCoverage,
    part_geometry,
)
from mooncake.reshard.kv_cache.types import KVCacheComponent

ALL_HOST_LAYOUTS = (
    KVCacheHostLayout.PLHD,
    KVCacheHostLayout.LPHD,
    KVCacheHostLayout.HPLD,
)


def make_domain(
    *,
    layer_ids: tuple[int, ...] = (0, 1, 2, 3),
    kv_heads: int = 2,
    head_dim: int = 3,
    itemsize: int = 2,
    page_size: int = 4,
    revision: str = "rev-1",
) -> ModelSemanticDomain:
    return ModelSemanticDomain(
        namespace="sglang",
        model_identity="llama-8b",
        revision=revision,
        dtype="float16",
        itemsize=itemsize,
        page_size=page_size,
        global_layer_ids=layer_ids,
        total_kv_heads=kv_heads,
        key_head_dim=head_dim,
        value_head_dim=head_dim,
    )


def make_part(
    part_index: int,
    layer_ids: tuple[int, ...],
    *,
    head_start: int = 0,
    head_count: int = 2,
) -> SourcePartCoverage:
    return SourcePartCoverage(
        part_index=part_index,
        layer_ids=layer_ids,
        head_start=head_start,
        head_count=head_count,
    )


def layer_sharded_manifest(domain: ModelSemanticDomain) -> SourceLayoutManifest:
    """TP-style source: each part owns a layer range and all heads."""

    layer_ids = domain.global_layer_ids
    half = len(layer_ids) // 2
    return SourceLayoutManifest(
        domain=domain,
        layout=KVCacheHostLayout.PLHD,
        parts=(
            make_part(0, layer_ids[:half]),
            make_part(1, layer_ids[half:]),
        ),
    )


def head_sharded_manifest(domain: ModelSemanticDomain) -> SourceLayoutManifest:
    """Head-sharded source: each part owns all layers and half the heads."""

    half = domain.total_kv_heads // 2
    return SourceLayoutManifest(
        domain=domain,
        layout=KVCacheHostLayout.PLHD,
        parts=(
            make_part(0, domain.global_layer_ids, head_start=0, head_count=half),
            make_part(
                1,
                domain.global_layer_ids,
                head_start=half,
                head_count=domain.total_kv_heads - half,
            ),
        ),
    )


def logical_element(layer: int, head: int, dim: int) -> int:
    """Deterministic per-(layer, head, dim) content payload."""

    return (layer * 1000 + head * 100 + dim) % 65536


def fill_part_buffer(manifest: SourceLayoutManifest, part_index: int) -> bytearray:
    """Materialize one part's K and V regions in the part's own layout.

    Content is addressed by *global* (layer, head) coordinates, so a reader
    can verify a plan independently of any part's local indexing.
    """

    domain = manifest.domain
    part = manifest.part(part_index)
    geometry = part_geometry(manifest, part_index, KVCacheComponent.KEY)
    buffer = bytearray(2 * geometry.total_bytes)
    for component in (0, 1):
        for local_layer, layer_id in enumerate(part.layer_ids):
            for local_head in range(part.head_count):
                element_offset = geometry.byte_offset(
                    page=0,
                    layer=local_layer,
                    head=local_head,
                    component=component,
                )
                for dim in range(domain.key_head_dim):
                    offset = element_offset + dim * domain.itemsize
                    value = logical_element(layer_id, part.head_start + local_head, dim)
                    struct.pack_into("<H", buffer, offset, value)
    return buffer


def apply_plan(source_buffers: dict[int, bytearray], plan, target: bytearray) -> None:
    """Copy every planned range from its part buffer into the target."""

    for item in plan.ranges:
        source = source_buffers[item.source_part_index]
        target[item.target_offset : item.target_offset + item.target_length] = source[
            item.source_offset : item.source_offset + item.source_length
        ]
