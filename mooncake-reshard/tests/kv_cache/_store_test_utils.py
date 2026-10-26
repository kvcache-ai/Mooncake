"""Deterministic KV tensors and a minimal byte backend for reshard tests."""

import ctypes
import struct

from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheStoreFormat,
    KVCacheStoreManifest,
    KVCacheStoreRuntimeBinding,
    plan_kv_cache_store_upload,
)
from test_kv_cache_reshard import _placement

FORMATS = tuple(KVCacheStoreFormat)


def _layout(tp=1, layers=((0, 1), (2, 3)), fmt=FORMATS[0]):
    return plan_kv_cache_store_upload(
        _placement("first", layers, tp), object_format=fmt
    ).layout


def _manifest(layout=None):
    return KVCacheStoreManifest(
        "cache", "model", "revision", "semantic", layout or _layout()
    )


def _offset(fmt, token, layer, head, layers, heads, tokens, head_bytes):
    # Independent row-major tensor indexing; do not use production strides.
    if fmt is KVCacheStoreFormat.PLHD:
        return ((token * layers + layer) * heads + head) * head_bytes
    if fmt is KVCacheStoreFormat.LPHD:
        return ((layer * tokens + token) * heads + head) * head_bytes
    return ((head * tokens + token) * layers + layer) * head_bytes


def _reference(layout, layer_ids, head_start, head_count, component):
    d = layout.descriptor
    dim = d.key_head_dim if component is KVCacheComponent.KEY else d.value_head_dim
    result = bytearray(len(layer_ids) * d.page_size * head_count * dim * d.itemsize)
    for t in range(d.page_size):
        for local_layer, layer in enumerate(layer_ids):
            for h in range(head_count):
                off = _offset(
                    layout.object_format,
                    t,
                    local_layer,
                    h,
                    len(layer_ids),
                    head_count,
                    d.page_size,
                    dim * d.itemsize,
                )
                for x in range(dim):
                    value = layer * 1000 + t * 50 + (head_start + h) * 8 + x
                    if component is KVCacheComponent.VALUE:
                        value += 20000
                    struct.pack_into("<H", result, off + 2 * x, value)
    return result


def memory_binding(
    placement,
    participant,
    fmt,
    pages,
    operation,
    *,
    fill=True,
    endpoint="local",
    allocator=None,
    page_salts=None,
):
    part = placement.part(participant)
    d = placement.descriptor
    layout = plan_kv_cache_store_upload(placement, object_format=fmt).layout
    regions, ranges, buffers, expected = [], [], [], []
    for page in range(pages):
        for component in KVCacheComponent:
            data = _reference(
                layout, part.layer_ids, part.head_start, part.head_count, component
            )
            # Keep page contents distinguishable, while preserving width and shape.
            data = bytes(
                (b + (page if page_salts is None else page_salts[page])) % 256
                for b in data
            )
            if allocator is None:
                buffer = ctypes.create_string_buffer(
                    data if fill else bytes(len(data)), len(data)
                )
            else:
                address = allocator(len(data))
                assert address
                buffer = (ctypes.c_char * len(data)).from_address(address)
                ctypes.memmove(address, data if fill else bytes(len(data)), len(data))
            buffers.append(buffer)
            expected.append(data)
            region_id = f"page-{page}-{component.value}"
            regions.append(
                KVCacheRegisteredRegion(
                    region_id, endpoint, ctypes.addressof(buffer), len(data)
                )
            )
            dim = (
                d.key_head_dim
                if component is KVCacheComponent.KEY
                else d.value_head_dim
            )
            width = dim * d.itemsize
            layers, heads, tokens = len(part.layer_ids), part.head_count, d.page_size
            if fmt is KVCacheStoreFormat.PLHD:
                layer_stride, token_stride, head_stride = (
                    heads * width,
                    layers * heads * width,
                    width,
                )
            elif fmt is KVCacheStoreFormat.LPHD:
                layer_stride, token_stride, head_stride = (
                    tokens * heads * width,
                    heads * width,
                    width,
                )
            else:
                layer_stride, token_stride, head_stride = (
                    width,
                    layers * width,
                    tokens * layers * width,
                )
            for local_layer, layer in enumerate(part.layer_ids):
                for h in range(heads) if fmt is KVCacheStoreFormat.HPLD else (0,):
                    ranges.append(
                        KVCacheResolvedRange(
                            layer,
                            component,
                            page * tokens,
                            tokens,
                            part.head_start + h,
                            1 if fmt is KVCacheStoreFormat.HPLD else heads,
                            region_id,
                            local_layer * layer_stride + h * head_stride,
                            token_stride,
                            head_stride,
                        )
                    )
    binding = KVCacheStoreRuntimeBinding(
        operation,
        placement.resource_id,
        placement.placement_id,
        placement.digest,
        "instance",
        placement.revision,
        part.participant_id,
        tuple(regions),
        tuple(ranges),
    )
    return binding, buffers, expected
