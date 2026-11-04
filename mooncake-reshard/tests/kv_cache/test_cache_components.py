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
    KVCacheRuntimeTransferPlan,
    KVCacheStoreFormat,
    KVCacheStoreRuntimeBinding,
    KVCacheTransferLimits,
    kv_cache_placement_from_json,
    kv_cache_placement_to_json,
    kv_cache_runtime_transfer_from_json,
    kv_cache_runtime_transfer_to_json,
    plan_kv_cache_store_upload,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    resolve_contiguous_runtime_binding,
    validate_runtime_binding,
)
from mooncake.reshard.kv_cache._store.binding import validate_store_binding
from test_kv_cache_reshard import _binding, _placement, _snapshot


def resource(
    prefix, tp, component, heads=4, strided=False, dtype="float16", itemsize=2
):
    placement = _placement(prefix, ((1, 3),), tp, total_kv_heads=heads, page_size=1)
    descriptor = replace(
        placement.descriptor,
        components=(component,),
        dtype=dtype,
        itemsize=itemsize,
        key_head_dim=6,
        value_head_dim=1,
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
def test_resource_planning_and_wire(component, source_tp, target_tp):
    heads = 1 if component is KVCacheComponent.LATENT else 4
    source = resource("source", source_tp, component, heads)
    target = resource("target", target_tp, component, heads)
    assert kv_cache_placement_from_json(kv_cache_placement_to_json(source)) == source
    for part in target.parts:
        plan = plan_kv_cache_transfer_to_local_target(
            source, target, part.participant_id
        )
        assert {e.component for e in plan.edges} == {component}
        assert sum(e.head_count for e in plan.edges) == 2 * part.head_count
        if component is KVCacheComponent.LATENT:
            assert len(plan.source_participant_ids) == 1
        for writer in plan.source_participant_ids:
            prepared = prepare_kv_cache_transfer(
                plan.for_source(writer),
                _binding(source, writer, base_address=1000000),
                _binding(target, part.participant_id, base_address=100000000),
            )
            assert (
                all(
                    e.nbytes == 6 * heads // max(source_tp, target_tp) * 2
                    for e in prepared.edges
                )
                if heads >= max(source_tp, target_tp)
                else all(e.nbytes == 12 for e in prepared.edges)
            )


def test_strided_state_views_attest_physical_span_and_offset():
    placement = resource("state", 1, KVCacheComponent.CONV, strided=True)
    part = placement.parts[0]
    binding = _binding(
        placement, part.participant_id, base_address=1000000, capacity_tokens=3
    )
    buffers = []
    for item in binding.buffers:
        f = item.fragment
        stride = f.strides_bytes[0] * 3
        span = 2 * stride + f.strides_bytes[0]
        buffers.append(
            replace(
                item,
                fragment=replace(
                    f,
                    address=f.address + 48,
                    storage_offset_bytes=48,
                    storage_nbytes=3 * stride,
                    nbytes=span,
                    strides_bytes=(stride, *f.strides_bytes[1:]),
                ),
            )
        )
    binding = replace(binding, buffers=tuple(buffers))
    validate_runtime_binding(placement, binding)
    broken = replace(
        binding,
        buffers=(
            replace(
                binding.buffers[0],
                fragment=replace(binding.buffers[0].fragment, nbytes=1),
            ),
            *binding.buffers[1:],
        ),
    )
    with pytest.raises(ValueError, match="nbytes"):
        validate_runtime_binding(placement, broken)


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
    "strides", [(47, 12, 2), (145, 12, 2), (144, 14, 2), (144, 12, 4)]
)
def test_strided_state_rejects_overlapping_or_noncontiguous_rows(strides):
    placement = resource("state", 1, KVCacheComponent.SSM, strided=True)
    binding = _binding(
        placement,
        placement.parts[0].participant_id,
        base_address=1000000,
        capacity_tokens=3,
    )
    first = binding.buffers[0]
    binding = replace(
        binding,
        buffers=(
            replace(first, fragment=replace(first.fragment, strides_bytes=strides)),
            *binding.buffers[1:],
        ),
    )
    with pytest.raises(ValueError, match="disjoint contiguous NHD rows"):
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize(
    "component", [KVCacheComponent.LATENT, KVCacheComponent.CONV, KVCacheComponent.SSM]
)
@pytest.mark.parametrize("dtype,itemsize", [("bfloat16", 2), ("float32", 4)])
@pytest.mark.parametrize("source_tp,target_tp", [(1, 2), (2, 1)])
def test_resolved_executor_copies_non_kv_components(
    component, dtype, itemsize, source_tp, target_tp
):
    from test_runtime_transfer import assert_content, execute_all, make_operation

    heads = 1 if component is KVCacheComponent.LATENT else 4
    source = resource(
        "source", source_tp, component, heads, dtype=dtype, itemsize=itemsize
    )
    target = resource(
        "target", target_tp, component, heads, dtype=dtype, itemsize=itemsize
    )
    operation, keepalive = make_operation(source, target)
    operation = kv_cache_runtime_transfer_from_json(
        kv_cache_runtime_transfer_to_json(operation)
    )
    receipts, _, executors = execute_all(operation)
    assert receipts and keepalive and executors
    assert all(receipt.success for receipt in receipts)
    for binding in operation.target_bindings:
        assert_content(target, binding)


@pytest.mark.parametrize("component", [KVCacheComponent.CONV, KVCacheComponent.SSM])
def test_strided_state_adapter_copies_only_selected_rows(component):
    from test_runtime_transfer import execute_all

    snapshot = _snapshot(token_start=13, token_count=2)
    source = resource("source", 2, component, strided=True, dtype="float32", itemsize=4)
    target = resource("target", 1, component, strided=True, dtype="float32", itemsize=4)
    bindings, keepalive, expected_targets = [], [], []
    for placement, first_row, is_source in ((source, 1, True), (target, 2, False)):
        resolved = []
        for part in placement.parts:
            binding = _binding(
                placement,
                part.participant_id,
                base_address=1000000,
                snapshot=snapshot,
                capacity_tokens=5,
            )
            buffers = []
            for item in binding.buffers:
                f = item.fragment
                row_bytes = f.strides_bytes[0]
                stride = row_bytes * 3
                # A middle group in a fused allocation; other groups are sentinels.
                allocation = ctypes.create_string_buffer(
                    b"\xa5" * (5 * stride), 5 * stride
                )
                expected = bytearray(allocation.raw)
                for row in range(snapshot.token_count):
                    for head in range(part.head_count):
                        value = (
                            0x12340000
                            + item.global_layer_id * 100
                            + row * 10
                            + part.head_start
                            + head
                        )
                        payload = value.to_bytes(4, "little") * 6
                        offset = row_bytes + (first_row + row) * stride + head * 24
                        expected[offset : offset + 24] = payload
                if is_source:
                    allocation.raw = bytes(expected)
                else:
                    expected_targets.append((allocation, bytes(expected)))
                address = ctypes.addressof(allocation)
                buffers.append(
                    replace(
                        item,
                        fragment=replace(
                            f,
                            address=address + row_bytes,
                            storage_address=address,
                            storage_offset_bytes=row_bytes,
                            storage_nbytes=len(allocation),
                            nbytes=4 * stride + row_bytes,
                            strides_bytes=(stride, *f.strides_bytes[1:]),
                        ),
                    )
                )
                keepalive.append(allocation)
            resolved.append(
                resolve_contiguous_runtime_binding(
                    placement,
                    snapshot,
                    replace(binding, buffers=tuple(buffers)),
                    operation_id="state-copy",
                    first_token_offset=first_row,
                )
            )
        bindings.append(tuple(resolved))
    plan = KVCacheRuntimeTransferPlan(
        "state-copy",
        tuple(
            plan_kv_cache_transfer_to_local_target(
                source, target, p.participant_id, snapshot=snapshot
            )
            for p in target.parts
        ),
        *bindings,
    )
    receipts, _, executors = execute_all(plan)
    assert keepalive and executors and all(r.success for r in receipts)
    for allocation, expected in expected_targets:
        assert allocation.raw == expected


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
