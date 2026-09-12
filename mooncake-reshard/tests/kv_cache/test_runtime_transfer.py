from __future__ import annotations

import ctypes
import json
import struct
from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheCompletion,
    KVCacheComponent,
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheResolvedRuntimeBinding,
    KVCacheRuntimeTransferPlan,
    KVCacheTargetReceipt,
    KVCacheTransferEngineExecutor,
    KVCacheTransferLimits,
    kv_cache_resolved_binding_from_json,
    kv_cache_resolved_binding_to_json,
    kv_cache_runtime_transfer_from_json,
    kv_cache_runtime_transfer_to_json,
    kv_cache_target_receipt_from_json,
    kv_cache_target_receipt_to_json,
    kv_cache_writer_receipt_from_json,
    kv_cache_writer_receipt_to_json,
    plan_kv_cache_transfer_to_local_target,
    resolve_contiguous_runtime_binding,
    validate_resolved_runtime_binding,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot


def content(layer, component, token, head, dim):
    return (
        layer * 7001
        + (11003 if component is KVCacheComponent.VALUE else 0)
        + token * 137
        + head * 31
        + dim
    ) % 65536


def memory_binding(
    placement,
    participant_id,
    snapshot,
    operation_id="operation-1",
    *,
    source=False,
    reverse=False,
    padding=0,
    endpoint=None,
):
    part = placement.part(participant_id)
    endpoint = endpoint or f"{participant_id}:12345"
    regions, ranges, buffers = [], [], []
    for layer in part.layer_ids:
        for component in KVCacheComponent:
            dim = (
                placement.descriptor.key_head_dim
                if component is KVCacheComponent.KEY
                else placement.descriptor.value_head_dim
            )
            head_stride = dim * 2 + padding
            row_stride = part.head_count * head_stride + padding
            buffer = ctypes.create_string_buffer(snapshot.token_count * row_stride)
            ctypes.memset(ctypes.addressof(buffer), 0xA5, len(buffer))
            region = KVCacheRegisteredRegion(
                f"{layer}-{component.value}",
                endpoint,
                ctypes.addressof(buffer),
                len(buffer),
            )
            regions.append(region)
            buffers.append(buffer)
            for token in range(snapshot.token_start, snapshot.token_end):
                slot = token - snapshot.token_start
                if reverse:
                    slot = snapshot.token_count - slot - 1
                ranges.append(
                    KVCacheResolvedRange(
                        layer,
                        component,
                        token,
                        1,
                        part.head_start,
                        part.head_count,
                        region.region_id,
                        slot * row_stride,
                        row_stride,
                        head_stride,
                    )
                )
                if source:
                    for head in range(
                        part.head_start, part.head_start + part.head_count
                    ):
                        for d in range(dim):
                            struct.pack_into(
                                "<H",
                                buffer,
                                slot * row_stride
                                + (head - part.head_start) * head_stride
                                + d * 2,
                                content(layer, component, token, head, d),
                            )
    binding = KVCacheResolvedRuntimeBinding(
        operation_id,
        placement.resource_id,
        placement.placement_id,
        placement.digest,
        f"instance:{participant_id}",
        placement.revision,
        participant_id,
        snapshot.snapshot_id,
        snapshot.digest,
        tuple(regions),
        tuple(ranges),
    )
    return binding, buffers


def assert_content(placement, binding):
    regions = {r.region_id: r for r in binding.regions}
    touched = set()
    for item in binding.ranges:
        dim = (
            placement.descriptor.key_head_dim
            if item.component is KVCacheComponent.KEY
            else placement.descriptor.value_head_dim
        )
        for token in range(item.token_start, item.token_end):
            for head in range(item.head_start, item.head_end):
                address = item.address(regions[item.region_id], token, head)
                for d in range(dim):
                    assert ctypes.c_uint16.from_address(
                        address + d * 2
                    ).value == content(
                        item.global_layer_id, item.component, token, head, d
                    )
                    touched.update((address + d * 2, address + d * 2 + 1))
    for region in binding.regions:
        for address in range(region.address, region.address + region.nbytes):
            if address not in touched:
                assert ctypes.c_ubyte.from_address(address).value == 0xA5


class MemoryEngine:
    def __init__(self, fail_at=None, probe_result=0):
        self.calls = []
        self.fail_at = fail_at
        self.probe_result = probe_result
        self.probes = []

    def register_memory(self, address, nbytes):
        return 0

    def unregister_memory(self, address):
        return 0

    def send_probe(self, endpoint):
        self.probes.append(endpoint)
        return self.probe_result

    def batch_transfer_sync_write(self, endpoint, sources, targets, lengths):
        self.calls.append((endpoint, sources, targets, lengths))
        if len(self.calls) == self.fail_at:
            return -1
        for source, target, length in zip(sources, targets, lengths):
            ctypes.memmove(target, source, length)
        return 0


def make_operation(source, target, *, limits=None, reverse=True, padding=4):
    limits = limits or KVCacheTransferLimits()
    snapshot = _snapshot(token_start=13, token_count=5)
    plans = tuple(
        plan_kv_cache_transfer_to_local_target(
            source, target, p.participant_id, snapshot=snapshot
        )
        for p in target.parts
        if p.layer_ids
    )
    selected = {w for p in plans for w in p.expected_writer_ids}
    sb, tb, keepalive = [], [], []
    for placement, selected_parts, output, is_source in (
        (source, selected, sb, True),
        (target, {p.target_participant_id for p in plans}, tb, False),
    ):
        for part in placement.parts:
            if part.participant_id in selected_parts:
                binding, buffers = memory_binding(
                    placement,
                    part.participant_id,
                    snapshot,
                    source=is_source,
                    reverse=reverse and is_source,
                    padding=padding,
                )
                output.append(binding)
                keepalive.extend(buffers)
    return KVCacheRuntimeTransferPlan(
        "operation-1", plans, tuple(sb), tuple(tb), limits
    ), keepalive


def execute_all(plan, *, engine_factory=MemoryEngine):
    receipts, engines, executors = [], [], []
    for binding in plan.source_bindings:
        engine = engine_factory()
        executor = KVCacheTransferEngineExecutor(
            engine,
            instance_id=binding.instance_id,
            endpoint=binding.regions[0].endpoint,
        )
        for region in binding.regions:
            executor.register_region(region)
        receipts.extend(executor.execute(plan, binding.participant_id, warmup=True))
        engines.append(engine)
        executors.append(executor)
    return receipts, engines, executors


@pytest.mark.parametrize(
    "source_pp,target_pp,source_tp,target_tp,heads,source_dp,target_dp",
    [
        (((0, 1),), ((0, 1),), 1, 2, 4, 1, 1),
        (((0, 1),), ((0, 1),), 2, 1, 4, 1, 1),
        (((0,), (1,)), ((1,), (0,)), 2, 1, 4, 1, 1),
        (((0, 1),), ((0,), (1,)), 1, 2, 4, 2, 3),
        (((0,),), ((0,),), 4, 1, 2, 1, 1),
        (((0,),), ((0,),), 4, 2, 1, 1, 1),
    ],
)
def test_content_topologies_and_completion(
    source_pp, target_pp, source_tp, target_tp, heads, source_dp, target_dp
):
    source = _placement(
        "source", source_pp, source_tp, total_kv_heads=heads, dp_size=source_dp
    )
    target = _placement(
        "target", target_pp, target_tp, total_kv_heads=heads, dp_size=target_dp
    )
    plan, _keepalive = make_operation(
        source,
        target,
        limits=KVCacheTransferLimits(max_batch_operations=3, max_batch_bytes=48),
    )
    restored = kv_cache_runtime_transfer_from_json(
        kv_cache_runtime_transfer_to_json(plan)
    )
    assert restored == plan
    receipts, engines, executors = execute_all(restored)
    barrier = KVCacheCompletion(plan)
    for receipt in reversed(receipts):
        barrier.record_writer(
            kv_cache_writer_receipt_from_json(kv_cache_writer_receipt_to_json(receipt))
        )
        barrier.record_writer(receipt)
    assert barrier.state == "writers_done"
    assert not barrier.can_activate
    for binding in plan.target_bindings:
        assert_content(target, binding)
        receipt = KVCacheTargetReceipt(
            plan.operation_id, plan.digest, binding.participant_id, True
        )
        barrier.record_target(
            kv_cache_target_receipt_from_json(kv_cache_target_receipt_to_json(receipt))
        )
        barrier.record_target(receipt)
    assert barrier.can_activate
    for binding, executor, engine in zip(plan.source_bindings, executors, engines):
        before = len(engine.calls)
        assert executor.execute(plan, binding.participant_id)
        assert len(engine.calls) == before
        assert len(engine.probes) == len(set(engine.probes))
        for _, sources, _, lengths in engine.calls:
            assert len(sources) <= 3
            assert sum(lengths) <= 48


@pytest.fixture
def small_operation():
    return make_operation(
        _placement("source", ((0,),), 2), _placement("target", ((0,),), 1)
    )


@pytest.mark.parametrize(
    "change,match",
    [
        ({"operation_id": "stale"}, "operation_id"),
        ({"snapshot_digest": "0" * 64}, "snapshot_digest"),
        ({"placement_digest": "0" * 64}, "placement_digest"),
        ({"revision": "stale"}, "revision"),
    ],
)
def test_binding_identity(small_operation, change, match):
    plan, _ = small_operation
    with pytest.raises(ValueError, match=match):
        replace(plan, target_bindings=(replace(plan.target_bindings[0], **change),))


@pytest.mark.parametrize(
    "change,match",
    [
        ({"token_start": 12}, "token interval"),
        ({"head_start": 1}, "unowned heads"),
        ({"region_id": "absent"}, "unknown registered"),
        ({"offset_bytes": 1 << 40}, "bounds"),
        ({"offset_bytes": 1}, "aligned"),
        ({"token_stride_bytes": 1}, "strides"),
        ({"head_stride_bytes": 1}, "strides"),
    ],
)
def test_range_geometry(small_operation, change, match):
    plan, _ = small_operation
    b = plan.target_bindings[0]
    with pytest.raises(ValueError, match=match):
        bad = replace(b, ranges=(replace(b.ranges[0], **change), *b.ranges[1:]))
        replace(plan, target_bindings=(bad,))


def test_coverage_overlap_aliasing_and_participants(small_operation):
    plan, _ = small_operation
    b = plan.target_bindings[0]
    for ranges in (b.ranges[1:], b.ranges + (b.ranges[0],)):
        with pytest.raises(ValueError, match="coverage|overlap"):
            replace(plan, target_bindings=(replace(b, ranges=ranges),))
    with pytest.raises(ValueError, match="source binding participants"):
        replace(plan, source_bindings=plan.source_bindings[:1])
    with pytest.raises(ValueError, match="every target"):
        replace(plan, logical_plans=plan.logical_plans * 2)
    with pytest.raises(ValueError, match="complete local-target"):
        replace(
            plan,
            logical_plans=(
                plan.logical_plans[0].for_source(
                    plan.source_bindings[0].participant_id
                ),
            ),
        )
    with pytest.raises(ValueError, match="expansion limit|operation limit"):
        replace(plan, limits=KVCacheTransferLimits(max_operations=1))
    with pytest.raises(ValueError, match="byte limit"):
        replace(plan, limits=KVCacheTransferLimits(max_bytes=1))
    with pytest.raises(ValueError, match="work limit"):
        replace(plan, limits=KVCacheTransferLimits(max_validation_work=1))
    with pytest.raises(ValueError, match="range count"):
        replace(plan, limits=KVCacheTransferLimits(max_ranges=1))


def test_failure_is_terminal_and_retains_registration(small_operation):
    plan, _ = small_operation
    receipts, _, executors = execute_all(
        plan, engine_factory=lambda: MemoryEngine(fail_at=1)
    )
    assert all(not r.success and not r.quiesced for r in receipts)
    barrier = KVCacheCompletion(plan)
    barrier.record_writer(receipts[0])
    assert barrier.state == "failed"
    assert not barrier.can_activate
    with pytest.raises(ValueError, match="terminal"):
        barrier.record_target(
            KVCacheTargetReceipt(
                plan.operation_id,
                plan.digest,
                plan.target_bindings[0].participant_id,
                True,
            )
        )
    with pytest.raises(RuntimeError, match="in flight"):
        executors[0].unregister_region(plan.source_bindings[0].regions[0].region_id)


def test_probe_failure_does_not_submit_and_is_quiescent(small_operation):
    plan, _ = small_operation
    receipts, engines, executors = execute_all(
        plan, engine_factory=lambda: MemoryEngine(probe_result=-1)
    )
    assert all(not r.success and r.quiesced for r in receipts)
    assert not any(e.calls for e in engines)
    executors[0].unregister_region(plan.source_bindings[0].regions[0].region_id)


def test_receipt_validation_and_timeout(small_operation, monkeypatch):
    plan, _ = small_operation
    receipts, _, _ = execute_all(plan)
    barrier = KVCacheCompletion(plan)
    with pytest.raises(ValueError, match="all expected writers"):
        barrier.record_target(
            KVCacheTargetReceipt(
                plan.operation_id,
                plan.digest,
                plan.target_bindings[0].participant_id,
                True,
            )
        )
    for bad in (
        replace(receipts[0], operation_id="stale"),
        replace(receipts[0], transfer_digest="0" * 64),
        replace(receipts[0], writer_id="unknown"),
        replace(receipts[0], completed_bytes=0),
    ):
        with pytest.raises(ValueError):
            barrier.record_writer(bad)
    barrier.record_writer(receipts[0])
    with pytest.raises(ValueError, match="conflicting"):
        barrier.record_writer(replace(receipts[0], success=False, error="failed"))
    from mooncake.reshard.kv_cache import completion

    monkeypatch.setattr(completion.time, "monotonic", lambda: float("inf"))
    assert barrier.state == "failed"
    assert "deadline" in barrier.error


def test_target_failure_blocks_activation(small_operation):
    plan, _ = small_operation
    receipts, _, _ = execute_all(plan)
    barrier = KVCacheCompletion(plan)
    for receipt in receipts:
        barrier.record_writer(receipt)
    barrier.record_target(
        KVCacheTargetReceipt(
            plan.operation_id,
            plan.digest,
            plan.target_bindings[0].participant_id,
            False,
            "content mismatch",
        )
    )
    assert barrier.state == "failed"


def test_strict_wire_and_overflow(small_operation):
    plan, _ = small_operation
    b = plan.source_bindings[0]
    wire = kv_cache_resolved_binding_to_json(b)
    assert kv_cache_resolved_binding_from_json(wire) == b
    for bad in (
        wire.replace('"schema":', '"schema":"duplicate","schema":', 1),
        wire[:-1] + ',"unknown":1}',
        wire.replace('"token_count":1', '"token_count":true', 1),
        wire.replace('"offset_bytes":0', '"offset_bytes":NaN', 1),
    ):
        with pytest.raises(ValueError):
            kv_cache_resolved_binding_from_json(bad)
    payload = json.loads(kv_cache_runtime_transfer_to_json(plan))
    payload["digest"] = "0" * 64
    with pytest.raises(ValueError, match="digest"):
        kv_cache_runtime_transfer_from_json(json.dumps(payload))
    with pytest.raises(ValueError, match="region address end"):
        KVCacheRegisteredRegion("r", "peer", (1 << 64) - 1, 8)
    with pytest.raises(ValueError, match="wire limit"):
        kv_cache_resolved_binding_from_json(" " * (16 * 1024 * 1024 + 1))


def test_contiguous_compatibility_and_missing_snapshot():
    source = _placement("source", ((0,),), 1)
    target = _placement("target", ((0,),), 1)
    snapshot = _snapshot(token_start=37, token_count=8)
    b = _binding(source, "source-p0-t0", base_address=1000000, snapshot=snapshot)
    resolved = resolve_contiguous_runtime_binding(
        source, snapshot, b, operation_id="op", first_token_offset=3
    )
    validate_resolved_runtime_binding(source, snapshot, resolved, operation_id="op")
    assert all(
        r.token_start == 37 and r.offset_bytes == 3 * r.token_stride_bytes
        for r in resolved.ranges
    )
    with pytest.raises(ValueError, match="capacity"):
        resolve_contiguous_runtime_binding(
            source, snapshot, b, operation_id="op", first_token_offset=255
        )
    logical = plan_kv_cache_transfer_to_local_target(
        source, target, target.parts[0].participant_id
    )
    with pytest.raises(ValueError, match="explicit snapshot"):
        KVCacheRuntimeTransferPlan("op", (logical,), (resolved,), (resolved,))


def test_receiver_limits_cannot_be_raised_by_wire(small_operation):
    plan, _ = small_operation
    payload = json.loads(kv_cache_runtime_transfer_to_json(plan))
    payload["limits"]["max_operations"] = (1 << 64) - 1
    with pytest.raises(ValueError, match="receiver policy"):
        kv_cache_runtime_transfer_from_json(json.dumps(payload))
    with pytest.raises(ValueError, match="receiver policy"):
        kv_cache_runtime_transfer_from_json(
            kv_cache_runtime_transfer_to_json(plan),
            limits=KVCacheTransferLimits(max_bytes=1),
        )


def test_local_registration_and_operation_reuse(small_operation):
    plan, _ = small_operation
    b = plan.source_bindings[0]
    engine = MemoryEngine()
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id=b.instance_id, endpoint=b.regions[0].endpoint
    )
    with pytest.raises(ValueError, match="not registered"):
        executor.validate_local_binding(b)
    with pytest.raises(ValueError, match="not registered"):
        executor.execute(plan, b.participant_id)
    assert not engine.calls
    for region in b.regions:
        executor.register_region(region)
        executor.register_region(region)
    executor.validate_local_binding(b)
    executor.execute(plan, b.participant_id)
    with pytest.raises(ValueError, match="different transfer"):
        changed = replace(
            plan,
            target_bindings=(
                replace(plan.target_bindings[0], instance_id="new-instance"),
            ),
        )
        executor.execute(changed, b.participant_id)


@pytest.mark.parametrize("operation", ["register", "unregister"])
@pytest.mark.parametrize("failure", [-7, False, RuntimeError, KeyboardInterrupt])
def test_registration_failure_blocks_further_operations(
    small_operation, monkeypatch, operation, failure
):
    plan, _keepalive = small_operation
    binding = plan.source_bindings[0]
    engine = MemoryEngine()
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id=binding.instance_id, endpoint=binding.regions[0].endpoint
    )
    for region in binding.regions:
        executor.register_region(region)
    # Also exercise the cached-success path after a later registration error.
    assert all(r.success for r in executor.execute(plan, binding.participant_id))
    submissions = len(engine.calls)
    additional = KVCacheRegisteredRegion(
        "additional",
        binding.regions[0].endpoint,
        max(r.address + r.nbytes for r in binding.regions) + 4096,
        64,
    )
    method = "register_memory" if operation == "register" else "unregister_memory"
    original = getattr(engine, method)
    registration_calls = []

    def fail(*args):
        registration_calls.append(args)
        if isinstance(failure, type):
            raise failure("ambiguous registration state")
        return failure

    monkeypatch.setattr(engine, method, fail)
    with pytest.raises(failure if isinstance(failure, type) else RuntimeError):
        if operation == "register":
            executor.register_region(additional)
        else:
            executor.unregister_region(binding.regions[0].region_id)
    fresh = replace(
        plan,
        operation_id="retry",
        source_bindings=tuple(
            replace(b, operation_id="retry") for b in plan.source_bindings
        ),
        target_bindings=tuple(
            replace(b, operation_id="retry") for b in plan.target_bindings
        ),
    )
    for action in (
        lambda: executor.execute(plan, binding.participant_id),
        lambda: executor.execute(fresh, binding.participant_id),
        lambda: executor.register_region(additional),
        lambda: executor.register_region(binding.regions[0]),
        lambda: executor.unregister_region(binding.regions[0].region_id),
        lambda: executor.validate_local_binding(binding),
    ):
        with pytest.raises(RuntimeError, match="recovery"):
            action()
    assert len(registration_calls) == 1
    assert len(engine.calls) == submissions
    # A transient error disappearing does not prove that partial side effects
    # have been recovered; the original executor must stay quarantined.
    monkeypatch.setattr(engine, method, original)
    with pytest.raises(RuntimeError, match="recovery"):
        executor.execute(plan, binding.participant_id)


def test_registration_preflight_errors_leave_executor_usable(small_operation):
    plan, _keepalive = small_operation
    binding = plan.source_bindings[0]
    executor = KVCacheTransferEngineExecutor(
        MemoryEngine(),
        instance_id=binding.instance_id,
        endpoint=binding.regions[0].endpoint,
    )
    for region in binding.regions:
        executor.register_region(region)
    region = binding.regions[0]
    for invalid in (
        replace(region, endpoint="other"),
        replace(region, nbytes=region.nbytes + 1),
        replace(region, region_id="overlap"),
    ):
        with pytest.raises(ValueError):
            executor.register_region(invalid)
    with pytest.raises(KeyError):
        executor.unregister_region("unknown")
    executor.unregister_region(region.region_id)
    executor.register_region(region)
    executor.validate_local_binding(binding)
    assert all(r.success for r in executor.execute(plan, binding.participant_id))


def test_disjoint_bytes_do_not_hide_duplicate_logical_ranges(small_operation):
    plan, _ = small_operation
    b = plan.target_bindings[0]
    ranges = list(b.ranges)
    ranges[1] = replace(ranges[1], token_start=ranges[0].token_start)
    with pytest.raises(ValueError, match="logical coverage"):
        replace(plan, target_bindings=(replace(b, ranges=tuple(ranges)),))


def test_shared_instance_source_target_alias_is_rejected(small_operation):
    plan, _ = small_operation
    source, target = plan.source_bindings[0], plan.target_bindings[0]
    regions = tuple(
        replace(
            r,
            endpoint=source.regions[0].endpoint,
            address=source.regions[0].address if i == 0 else r.address,
        )
        for i, r in enumerate(target.regions)
    )
    with pytest.raises(ValueError, match="overlap"):
        replace(
            plan,
            target_bindings=(
                replace(target, instance_id=source.instance_id, regions=regions),
            ),
        )


def test_partial_batch_failure_does_not_activate(small_operation):
    plan, _ = small_operation
    plan = replace(
        plan, limits=KVCacheTransferLimits(max_batch_operations=1, max_batch_bytes=32)
    )
    receipts, engines, executors = execute_all(
        plan, engine_factory=lambda: MemoryEngine(fail_at=2)
    )
    assert all(len(e.calls) == 2 for e in engines)
    assert all(
        not r.success and not r.quiesced and r.completed_bytes == 16 for r in receipts
    )
    barrier = KVCacheCompletion(plan)
    barrier.record_writer(receipts[0])
    assert not barrier.can_activate
    with pytest.raises(RuntimeError, match="recovery"):
        fresh = replace(
            plan,
            operation_id="retry",
            source_bindings=tuple(
                replace(b, operation_id="retry") for b in plan.source_bindings
            ),
            target_bindings=tuple(
                replace(b, operation_id="retry") for b in plan.target_bindings
            ),
        )
        executors[0].execute(fresh, fresh.source_bindings[0].participant_id)


def test_failure_retry_uses_new_operation_and_transport(small_operation):
    plan, _ = small_operation
    failed, _, _ = execute_all(plan, engine_factory=lambda: MemoryEngine(fail_at=1))
    assert not failed[0].success
    fresh = replace(
        plan,
        operation_id="retry",
        source_bindings=tuple(
            replace(b, operation_id="retry") for b in plan.source_bindings
        ),
        target_bindings=tuple(
            replace(b, operation_id="retry") for b in plan.target_bindings
        ),
    )
    receipts, _, _ = execute_all(fresh)
    barrier = KVCacheCompletion(fresh)
    for receipt in receipts:
        barrier.record_writer(receipt)
    for b in fresh.target_bindings:
        assert_content(fresh.logical_plans[0].target_placement, b)
        barrier.record_target(
            KVCacheTargetReceipt(
                fresh.operation_id, fresh.digest, b.participant_id, True
            )
        )
    assert barrier.can_activate


def test_invalid_native_return_is_not_success(small_operation):
    plan, _ = small_operation

    class InvalidEngine(MemoryEngine):
        def batch_transfer_sync_write(self, *args):
            return False  # bool compares equal to 0, but is not the native ABI

    receipts, _, _ = execute_all(plan, engine_factory=InvalidEngine)
    assert all(not r.success and not r.quiesced for r in receipts)


def test_adjacent_registered_regions_must_not_be_coalesced():
    source = _placement("source", ((0,),), 1)
    target = _placement("target", ((0,),), 1)
    snapshot = _snapshot(token_start=0, token_count=5)
    bindings, _allocations = [], []
    for placement in (source, target):
        part = placement.parts[0]
        allocation = ctypes.create_string_buffer(640)
        _allocations.append(allocation)
        regions = tuple(
            KVCacheRegisteredRegion(
                component.value,
                f"{part.participant_id}:12345",
                ctypes.addressof(allocation) + index * 320,
                320,
            )
            for index, component in enumerate(KVCacheComponent)
        )
        ranges = tuple(
            KVCacheResolvedRange(0, component, 0, 5, 0, 4, component.value, 0, 64, 16)
            for component in KVCacheComponent
        )
        bindings.append(
            KVCacheResolvedRuntimeBinding(
                "adjacent",
                placement.resource_id,
                placement.placement_id,
                placement.digest,
                f"instance:{part.participant_id}",
                placement.revision,
                part.participant_id,
                snapshot.snapshot_id,
                snapshot.digest,
                regions,
                ranges,
            )
        )
    logical = plan_kv_cache_transfer_to_local_target(
        source, target, target.parts[0].participant_id, snapshot=snapshot
    )
    plan = KVCacheRuntimeTransferPlan(
        "adjacent", (logical,), (bindings[0],), (bindings[1],)
    )
    assert len(plan.writes) == 2
    assert [w.nbytes for w in plan.writes] == [320, 320]
