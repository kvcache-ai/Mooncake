"""Non-K/V resources use the same placement, preparation and wire contracts."""

import json
from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCachePlacementManifest,
    kv_cache_placement_from_json,
    kv_cache_placement_to_json,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    validate_runtime_binding,
)
from test_kv_cache_reshard import _binding, _placement


def resource(prefix, tp, component, heads=4, strided=False):
    placement = _placement(prefix, ((1, 3),), tp, total_kv_heads=heads, page_size=1)
    descriptor = replace(
        placement.descriptor,
        components=(component,),
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
    "component", [KVCacheComponent.LATENT, KVCacheComponent.CONV, KVCacheComponent.SSM]
)
def test_resolved_executor_copies_non_kv_components(component):
    from test_runtime_transfer import assert_content, execute_all, make_operation

    heads = 1 if component is KVCacheComponent.LATENT else 4
    source = resource("source", 2, component, heads)
    target = resource("target", 1, component, heads)
    operation, keepalive = make_operation(source, target)
    receipts, _, executors = execute_all(operation)
    assert receipts and keepalive and executors
    for binding in operation.target_bindings:
        assert_content(target, binding)
