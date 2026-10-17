"""Target-driven reshard planning tests, including the 3x3 layout matrix.

The byte-exact matrix is the RFC's acceptance criterion: every conversion
among the three host layouts (PLHD, LPHD, HPLD) must move exactly the
required bytes, with no unrelated data read or overwritten, across
different writer topologies and reader subsets.
"""

from __future__ import annotations

import struct

import pytest

from mooncake.reshard.kv_cache.store_reshard import (
    KVCacheHostLayout,
    ReshardPlanningError,
    SourceLayoutManifest,
    TargetRequirement,
    plan_reshard_read,
    target_geometry,
)
from mooncake.reshard.kv_cache.types import KVCacheComponent

from .helpers import (
    ALL_HOST_LAYOUTS,
    apply_plan,
    fill_part_buffer,
    head_sharded_manifest,
    layer_sharded_manifest,
    logical_element,
    make_domain,
    make_part,
)

KEY = KVCacheComponent.KEY
VALUE = KVCacheComponent.VALUE


def make_manifest_with_layout(
    domain, layout: KVCacheHostLayout
) -> SourceLayoutManifest:
    layer_ids = domain.global_layer_ids
    half = len(layer_ids) // 2
    return SourceLayoutManifest(
        domain=domain,
        layout=layout,
        parts=(
            make_part(0, layer_ids[:half]),
            make_part(1, layer_ids[half:]),
        ),
    )


def requirement(
    component: KVCacheComponent = KEY,
    layers: tuple[int, ...] = (0, 1, 2, 3),
    head_start: int = 0,
    head_count: int = 2,
) -> TargetRequirement:
    return TargetRequirement(
        component=component,
        layer_ids=layers,
        head_start=head_start,
        head_count=head_count,
    )


def run_plan(manifest, req, target_layout):
    plan = plan_reshard_read(manifest, req, target_layout)
    sources = {
        part.part_index: fill_part_buffer(manifest, part.part_index)
        for part in manifest.parts
    }
    geometry = target_geometry(manifest, req, target_layout)
    target = bytearray(2 * geometry.total_bytes)
    apply_plan(sources, plan, target)
    return plan, target, geometry


def verify_bytes_exact(manifest, req, target, geometry) -> None:
    """Every required (layer, head, dim) element must carry its content."""

    component_index = 0 if req.component is KEY else 1
    head_dim = (
        manifest.domain.key_head_dim
        if req.component is KEY
        else manifest.domain.value_head_dim
    )
    itemsize = manifest.domain.itemsize
    for local_layer, layer_id in enumerate(req.layer_ids):
        for global_head in range(req.head_start, req.head_start + req.head_count):
            element_offset = geometry.byte_offset(
                page=0,
                layer=local_layer,
                head=global_head - req.head_start,
                component=component_index,
            )
            for dim in range(head_dim):
                packed = struct.unpack_from(
                    "<H", target, element_offset + dim * itemsize
                )[0]
                assert packed == logical_element(layer_id, global_head, dim), (
                    layer_id,
                    global_head,
                    dim,
                )


def verify_no_overlap(plan) -> None:
    """Planned target ranges must be disjoint."""

    ordered = sorted(plan.ranges, key=lambda item: item.target_offset)
    cursor = 0
    for item in ordered:
        assert item.target_offset >= cursor, "target ranges overlap"
        cursor = item.target_offset + item.target_length


@pytest.mark.parametrize("source_layout", ALL_HOST_LAYOUTS)
@pytest.mark.parametrize("target_layout", ALL_HOST_LAYOUTS)
def test_layout_conversion_matrix_is_byte_exact(
    source_layout: KVCacheHostLayout, target_layout: KVCacheHostLayout
) -> None:
    domain = make_domain()
    manifest = make_manifest_with_layout(domain, source_layout)
    req = requirement()
    plan, target, geometry = run_plan(manifest, req, target_layout)
    verify_no_overlap(plan)
    verify_bytes_exact(manifest, req, target, geometry)
    assert plan.planned_element_count == plan.required_element_count


@pytest.mark.parametrize("target_layout", ALL_HOST_LAYOUTS)
def test_value_component_is_byte_exact(target_layout: KVCacheHostLayout) -> None:
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    req = requirement(component=VALUE)
    plan, target, geometry = run_plan(manifest, req, target_layout)
    verify_bytes_exact(manifest, req, target, geometry)


@pytest.mark.parametrize("target_layout", ALL_HOST_LAYOUTS)
def test_uneven_target_subset_is_byte_exact(target_layout: KVCacheHostLayout) -> None:
    # Uneven PP: the reader owns layers 1 and 3 only.
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    req = requirement(layers=(1, 3))
    plan, target, geometry = run_plan(manifest, req, target_layout)
    verify_no_overlap(plan)
    verify_bytes_exact(manifest, req, target, geometry)
    # Untouched target bytes stay zero: nothing outside the requirement was
    # read or overwritten.
    touched = {
        offset
        for item in plan.ranges
        for offset in range(item.target_offset, item.target_offset + item.target_length)
    }
    for offset in range(len(target)):
        if offset not in touched:
            assert target[offset] == 0, offset


@pytest.mark.parametrize("target_layout", ALL_HOST_LAYOUTS)
def test_head_sharded_source_is_byte_exact(target_layout: KVCacheHostLayout) -> None:
    domain = make_domain(kv_heads=4)
    manifest = head_sharded_manifest(domain)
    req = requirement(head_count=4)
    plan, target, geometry = run_plan(manifest, req, target_layout)
    verify_no_overlap(plan)
    verify_bytes_exact(manifest, req, target, geometry)
    # The full-head read pulls from both parts.
    assert {item.source_part_index for item in plan.ranges} == {0, 1}


def test_head_interval_spanning_parts_is_byte_exact() -> None:
    domain = make_domain(kv_heads=4)
    manifest = head_sharded_manifest(domain)
    req = requirement(head_start=1, head_count=2)
    plan, target, geometry = run_plan(manifest, req, KVCacheHostLayout.PLHD)
    verify_no_overlap(plan)
    verify_bytes_exact(manifest, req, target, geometry)


def test_merge_only_when_contiguous_on_both_sides() -> None:
    # Source PLHD can merge two full layers into one run; target LPHD
    # cannot (its layer axis is outermost), so the plan must stay split.
    domain = make_domain()
    manifest = make_manifest_with_layout(domain, KVCacheHostLayout.PLHD)
    req = requirement(layers=(0, 1))
    plan = plan_reshard_read(manifest, req, KVCacheHostLayout.LPHD)
    assert len(plan.ranges) == 2
    for item in plan.ranges:
        assert item.source_length == 2 * domain.key_head_dim * domain.itemsize


def test_same_side_layouts_merge_into_one_run() -> None:
    domain = make_domain()
    manifest = make_manifest_with_layout(domain, KVCacheHostLayout.PLHD)
    req = requirement(layers=(0, 1))
    plan = plan_reshard_read(manifest, req, KVCacheHostLayout.PLHD)
    assert len(plan.ranges) == 1
    item = plan.ranges[0]
    assert item.source_length == 2 * 2 * domain.key_head_dim * domain.itemsize
    assert item.target_length == item.source_length


def test_plan_is_deterministic() -> None:
    domain = make_domain()
    manifest = make_manifest_with_layout(domain, KVCacheHostLayout.HPLD)
    req = requirement()
    first = plan_reshard_read(manifest, req, KVCacheHostLayout.LPHD)
    second = plan_reshard_read(manifest, req, KVCacheHostLayout.LPHD)
    assert first.ranges == second.ranges


def test_unknown_layer_is_rejected() -> None:
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    with pytest.raises(ReshardPlanningError, match="unknown global layer"):
        plan_reshard_read(manifest, requirement(layers=(0, 9)), KVCacheHostLayout.PLHD)


def test_head_interval_out_of_range_is_rejected() -> None:
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    with pytest.raises(ReshardPlanningError, match="exceeds total_kv_heads"):
        plan_reshard_read(
            manifest, requirement(head_start=1, head_count=2), KVCacheHostLayout.PLHD
        )


def test_ranges_move_equal_bytes_on_both_sides() -> None:
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    plan = plan_reshard_read(manifest, requirement(), KVCacheHostLayout.HPLD)
    for item in plan.ranges:
        assert item.source_length == item.target_length


def test_target_local_offsets_are_relative() -> None:
    # Plans must not embed any address: offsets are relative to the start
    # of the respective region, and every offset stays inside its buffer.
    from mooncake.reshard.kv_cache.store_reshard import part_geometry

    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    req = requirement()
    geometry = target_geometry(manifest, req, KVCacheHostLayout.LPHD)
    plan = plan_reshard_read(manifest, req, KVCacheHostLayout.LPHD)
    for item in plan.ranges:
        assert item.target_offset + item.target_length <= 2 * geometry.total_bytes
        part_size = part_geometry(manifest, item.source_part_index, KEY)
        assert item.source_offset + item.source_length <= 2 * part_size.total_bytes
    assert min(item.target_offset for item in plan.ranges) >= 0
