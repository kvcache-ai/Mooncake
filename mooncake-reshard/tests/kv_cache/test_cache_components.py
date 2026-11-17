"""Non-K/V resources use the same placement, preparation and wire contracts."""

import ctypes
import json
from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCacheModelDomain,
    KVCachePlacementManifest,
    KVCacheStoreFormat,
    KVCacheStoreRuntimeBinding,
    KVCacheTransferLimits,
    kv_cache_placement_from_json,
    kv_cache_placement_to_json,
    kv_cache_runtime_transfer_from_json,
    kv_cache_runtime_transfer_to_json,
    plan_kv_cache_store_upload,
)
from mooncake.reshard.kv_cache._store.binding import validate_store_binding
from test_kv_cache_reshard import _placement, _snapshot


def resource(
    prefix,
    tp,
    component,
    heads=4,
    strided=False,
    dtype="float16",
    itemsize=2,
    head_dim=6,
    layer_ids=(1, 3),
):
    placement = _placement(prefix, (layer_ids,), tp, total_kv_heads=heads, page_size=1)
    descriptor = replace(
        placement.descriptor,
        components=(component,)
        if component is not None
        else (KVCacheComponent.KEY, KVCacheComponent.VALUE),
        dtype=dtype,
        itemsize=itemsize,
        key_head_dim=head_dim,
        value_head_dim=1 if component is not None else head_dim,
        layout=KVCacheLayout.NHD_STRIDED if strided else KVCacheLayout.NHD,
    )
    return KVCachePlacementManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_set_id=placement.placement_set_id,
        topology=placement.topology,
        descriptor=descriptor,
        parts=tuple(replace(p, descriptor=descriptor) for p in placement.parts),
    )


@pytest.mark.parametrize(
    "component", [KVCacheComponent.LATENT, KVCacheComponent.CONV, KVCacheComponent.SSM]
)
@pytest.mark.parametrize("source_tp,target_tp", [(1, 2), (2, 1), (4, 2)])
@pytest.mark.parametrize("dtype,itemsize", [("bfloat16", 2), ("float32", 4)])
def test_resource_planning_wire_and_copy(
    component, source_tp, target_tp, dtype, itemsize
):
    from test_runtime_transfer import assert_content, execute_all, make_operation

    heads = 1 if component is KVCacheComponent.LATENT else 4
    source = resource(
        "source", source_tp, component, heads, dtype=dtype, itemsize=itemsize
    )
    target = resource(
        "target", target_tp, component, heads, dtype=dtype, itemsize=itemsize
    )
    assert kv_cache_placement_from_json(kv_cache_placement_to_json(source)) == source
    operation, keepalive = make_operation(source, target)
    operation = kv_cache_runtime_transfer_from_json(
        kv_cache_runtime_transfer_to_json(operation)
    )
    for plan in operation.logical_plans:
        part = target.part(plan.target_participant_id)
        assert {e.component for e in plan.edges} == {component}
        assert sum(e.head_count for e in plan.edges) == 2 * part.head_count
        if component is KVCacheComponent.LATENT:
            assert len(plan.source_participant_ids) == 1
    receipts, _, _ = execute_all(operation)
    assert keepalive and receipts and all(r.success for r in receipts)
    for binding in operation.target_bindings:
        assert_content(target, binding)


def test_legacy_descriptor_wire_is_unchanged():
    placement = _placement("legacy", ((0,),), 1)
    wire = kv_cache_placement_to_json(placement)
    assert '"components"' not in wire
    assert kv_cache_placement_from_json(wire) == placement
    payload = json.loads(
        kv_cache_placement_to_json(resource("state", 1, KVCacheComponent.SSM))
    )
    payload["descriptor"]["components"] = ["ssm", "ssm"]
    with pytest.raises(ValueError, match="unique"):
        kv_cache_placement_from_json(json.dumps(payload))


@pytest.mark.parametrize(
    "component", [None, KVCacheComponent.CONV, KVCacheComponent.SSM]
)
@pytest.mark.parametrize("source_row,target_row", [(1, 0), (0, 1)])
def test_strided_shared_views_copy_selected_rows_across_tp(
    component, source_row, target_row
):
    from test_runtime_transfer import execute_all
    from test_shared_registration import resolve_all, shared_bindings, shared_plan

    snapshot = _snapshot(token_start=13, token_count=2)
    source_data = bytes(i % 251 for i in range(512))
    source_memory = ctypes.create_string_buffer(source_data, 512)
    target_memory = ctypes.create_string_buffer(b"\xa5" * 512, 512)
    placements, bindings = [], []
    for role, tp, allocation, first in (
        ("source", 2, source_memory, source_row),
        ("target", 1, target_memory, target_row),
    ):
        placement, raw = shared_bindings(
            role,
            snapshot,
            ctypes.addressof(allocation),
            component=component,
            tp=tp,
            strided=True,
        )
        placements.append(placement)
        bindings.append(resolve_all(placement, snapshot, raw, first_token_offset=first))
    # Independent byte oracle: merge each source rank's 32B shard into 64B rows.
    expected = bytearray(target_memory.raw)
    for row in range(2):
        for view in range(2 if component is None else 1):
            for rank in range(2):
                src = (source_row + row) * 128 + rank * 64 + view * 32
                dst = (target_row + row) * 128 + view * 64 + rank * 32
                expected[dst : dst + 32] = source_data[src : src + 32]
    plan = shared_plan(*placements, snapshot, *bindings)
    receipts, _, _ = execute_all(plan)
    assert receipts and all(r.success and r.quiesced for r in receipts)
    assert source_memory.raw == source_data
    assert target_memory.raw == bytes(expected)


def test_component_types_are_part_of_semantic_identity():
    domains = [
        KVCacheModelDomain(
            "cache", "model", "revision", "semantic", resource("state", 1, c).descriptor
        ).digest
        for c in (KVCacheComponent.LATENT, KVCacheComponent.CONV, KVCacheComponent.SSM)
    ]
    assert len(set(domains)) == 3


@pytest.mark.parametrize(
    "component", [KVCacheComponent.LATENT, KVCacheComponent.CONV, KVCacheComponent.SSM]
)
def test_store_rejects_r2r_only_state_resources(component):
    from test_runtime_transfer import memory_binding

    placement = resource("state", 1, component)
    with pytest.raises(ValueError, match="Store.*K/V"):
        plan_kv_cache_store_upload(placement, object_format=KVCacheStoreFormat.PLHD)
    snapshot = _snapshot(token_count=1)
    resolved, buffers = memory_binding(
        placement, placement.parts[0].participant_id, snapshot
    )
    binding = KVCacheStoreRuntimeBinding(
        **{
            name: getattr(resolved, name)
            for name in KVCacheStoreRuntimeBinding.__dataclass_fields__
        }
    )
    assert buffers
    with pytest.raises(ValueError, match="Store.*K/V"):
        validate_store_binding(
            placement, binding, "operation-1", 0, 1, KVCacheTransferLimits()
        )


def test_store_rejects_r2r_only_strided_descriptor():
    from _store_test_utils import _layout

    layout = _layout()
    with pytest.raises(ValueError, match="Store.*NHD"):
        replace(
            layout,
            descriptor=replace(layout.descriptor, layout=KVCacheLayout.NHD_STRIDED),
        )
