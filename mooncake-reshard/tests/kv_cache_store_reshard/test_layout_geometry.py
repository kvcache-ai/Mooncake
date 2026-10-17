"""Byte-geometry derivation tests for the three host layouts."""

from __future__ import annotations

import pytest

from mooncake.reshard.kv_cache.store_reshard import (
    HostLayoutGeometry,
    KVCacheHostLayout,
)

from .helpers import ALL_HOST_LAYOUTS


def make_geometry(layout: KVCacheHostLayout) -> HostLayoutGeometry:
    return HostLayoutGeometry(
        layout=layout,
        page_size=4,
        layer_count=2,
        kv_heads=3,
        head_dim=2,
        itemsize=2,
    )


@pytest.mark.parametrize("layout", ALL_HOST_LAYOUTS)
def test_total_bytes_matches_logical_descriptor(layout: KVCacheHostLayout) -> None:
    geometry = make_geometry(layout)
    assert geometry.total_bytes == 4 * 2 * 3 * 2 * 2


@pytest.mark.parametrize("layout", ALL_HOST_LAYOUTS)
def test_dim_stride_is_always_itemsize(layout: KVCacheHostLayout) -> None:
    geometry = make_geometry(layout)
    assert geometry.stride_bytes("dim") == 2
    for page in range(4):
        for layer in range(2):
            for head in range(3):
                base = geometry.byte_offset(page=page, layer=layer, head=head)
                assert (
                    geometry.byte_offset(page=page, layer=layer, head=head, component=1)
                    == geometry.total_bytes + base
                )


@pytest.mark.parametrize("layout", [KVCacheHostLayout.PLHD, KVCacheHostLayout.LPHD])
def test_head_is_innermost_except_dim(layout: KVCacheHostLayout) -> None:
    # In PLHD and LPHD, head is the last varying axis: adjacent heads are
    # exactly one element apart.
    geometry = make_geometry(layout)
    assert geometry.stride_bytes("head") == 4


def test_hpld_head_is_the_outermost_axis() -> None:
    geometry = make_geometry(KVCacheHostLayout.HPLD)
    # HPLD: (head, page, layer, dim) — head stride spans pages and layers.
    assert geometry.stride_bytes("head") == 4 * 2 * 4
    assert geometry.contiguous_run_bytes(heads=2) is None


@pytest.mark.parametrize("layout", ALL_HOST_LAYOUTS)
def test_head_run_is_contiguous_in_every_layout(layout: KVCacheHostLayout) -> None:
    geometry = make_geometry(layout)
    if layout is KVCacheHostLayout.HPLD:
        # head is outermost in HPLD, so a multi-head run is strided.
        assert geometry.contiguous_run_bytes(heads=3) is None
        return
    assert geometry.contiguous_run_bytes(heads=3) == 3 * 4


def test_plhd_layer_merges_when_heads_are_full() -> None:
    geometry = make_geometry(KVCacheHostLayout.PLHD)
    # PLHD: (page, layer, head, dim). Two layers of all heads are adjacent.
    assert geometry.contiguous_run_bytes(layers=2, heads=3) == 2 * 3 * 4


def test_plhd_full_box_across_pages_is_contiguous() -> None:
    geometry = make_geometry(KVCacheHostLayout.PLHD)
    # A box covering every layer and head of two pages is one run: page
    # stride equals the full page span.
    assert geometry.contiguous_run_bytes(pages=2, layers=2, heads=3) == 2 * 24


def test_plhd_partial_layers_with_multiple_pages_are_strided() -> None:
    geometry = make_geometry(KVCacheHostLayout.PLHD)
    # One layer of two pages skips the other layer between the pages.
    assert geometry.contiguous_run_bytes(pages=2, layers=1, heads=3) is None


def test_lphd_partial_layer_box_is_strided() -> None:
    geometry = make_geometry(KVCacheHostLayout.LPHD)
    # LPHD: (layer, page, head, dim). Two layers at one head are far apart.
    assert geometry.contiguous_run_bytes(layers=2, heads=1) is None
    # One layer's pages are adjacent.
    assert geometry.contiguous_run_bytes(pages=4, heads=3) == 4 * 3 * 4


def test_hpld_one_head_merges_pages_and_layers() -> None:
    geometry = make_geometry(KVCacheHostLayout.HPLD)
    # HPLD: (head, page, layer, dim). Within one head, pages and layers
    # are the two inner axes.
    assert geometry.contiguous_run_bytes(pages=4, layers=2, heads=1) == 4 * 2 * 4
    # Two heads skip the pages and layers between them.
    assert geometry.contiguous_run_bytes(heads=2) is None


def test_offsets_are_layout_specific() -> None:
    plhd = make_geometry(KVCacheHostLayout.PLHD)
    lphd = make_geometry(KVCacheHostLayout.LPHD)
    hpld = make_geometry(KVCacheHostLayout.HPLD)
    # Same logical coordinate, three different byte offsets.
    assert (
        plhd.byte_offset(page=1, layer=1, head=2)
        != lphd.byte_offset(page=1, layer=1, head=2)
        != hpld.byte_offset(page=1, layer=1, head=2)
    )
    # First element of every layout is at zero.
    for geometry in (plhd, lphd, hpld):
        assert geometry.byte_offset(page=0, layer=0, head=0) == 0


def test_out_of_range_coordinates_are_rejected() -> None:
    geometry = make_geometry(KVCacheHostLayout.PLHD)
    with pytest.raises(ValueError):
        geometry.byte_offset(page=4, layer=0, head=0)
    with pytest.raises(ValueError):
        geometry.byte_offset(page=0, layer=2, head=0)
    # An over-extent box is not a run: None, not an exception, so callers
    # can fall back to per-axis boxes.
    assert geometry.contiguous_run_bytes(pages=5) is None
    assert geometry.contiguous_run_bytes(layers=3) is None


def test_digest_is_stable_and_layout_sensitive() -> None:
    assert (
        make_geometry(KVCacheHostLayout.PLHD).digest()
        == make_geometry(KVCacheHostLayout.PLHD).digest()
    )
    assert (
        make_geometry(KVCacheHostLayout.PLHD).digest()
        != make_geometry(KVCacheHostLayout.LPHD).digest()
    )


def test_invalid_geometry_is_rejected() -> None:
    with pytest.raises(ValueError):
        HostLayoutGeometry(
            layout=KVCacheHostLayout.PLHD,
            page_size=4,
            layer_count=2,
            kv_heads=3,
            head_dim=2,
            itemsize=0,
        )
