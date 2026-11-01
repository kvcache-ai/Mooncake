from __future__ import annotations

from dataclasses import replace
from itertools import product

import pytest

from mooncake.reshard.weight._te.batching import iter_transfer_batches
from mooncake.reshard.weight.manifest import SplitAxis, TensorDescriptor
from mooncake.reshard.weight.te import MooncakeTransferEngineReader, TransferEngineError

from model_weight_planner.test_segment_iteration_perf import make_operation
from model_weight_planner.helpers import bound_fragment
from .helpers import (
    FakeTransferEngine,
    RuntimeInputs,
    execute_reader,
    manifests,
    multi_dim_te_manifests,
    participant_inputs,
    plan_transfer_to_local_target,
    registration_leases,
)


def old_segments(operation):
    for indices in product(*(range(count) for count in operation.outer_loop_counts)):
        yield (
            operation.source_base_offset
            + sum(
                index * stride
                for index, stride in zip(indices, operation.source_strides)
            ),
            operation.target_base_offset
            + sum(
                index * stride
                for index, stride in zip(indices, operation.target_strides)
            ),
            operation.inner_bytes,
        )


def batches(operations, limit=8192, budget=1_000_000):
    return iter_transfer_batches(
        "source:12345",
        ((op, op.source, op.target) for op in operations),
        max_batch_operations=limit,
        max_region_segments=budget,
    )


def test_mixed_dimensions_partial_batch_and_storage_offsets():
    operations = [make_operation((3,)), make_operation((8201,)), make_operation((2, 3))]
    operations = [
        replace(
            op,
            source=replace(
                op.source,
                binding=replace(
                    op.source.binding,
                    storage_address=op.source.address - 17,
                    storage_offset_bytes=17,
                    storage_nbytes=op.source.nbytes + 17,
                ),
            ),
            target=replace(
                op.target,
                binding=replace(
                    op.target.binding,
                    storage_address=op.target.address - 31,
                    storage_offset_bytes=31,
                    storage_nbytes=op.target.nbytes + 31,
                ),
            ),
        )
        for op in operations
    ]
    result = tuple(batches(operations))
    assert [batch.operation_count for batch in result] == [8192, 18]
    expected = [
        (op.source.address + so, op.target.address + to, size)
        for op in operations
        for so, to, size in old_segments(op)
    ]
    actual = [
        item
        for batch in result
        for item in zip(batch.source_addresses, batch.target_addresses, batch.sizes)
    ]
    assert actual == expected
    for batch in result:
        for transfer_range in batch.ranges:
            assert (
                transfer_range.source_base_address
                == operations[0].source.storage_address
            )
            assert (
                transfer_range.target_base_address
                == operations[0].target.storage_address
            )


@pytest.mark.parametrize(
    "counts,limit", [((3, 8201), 8192), ((3, 2, 5), 7), ((3, 1), 2)]
)
def test_nd_prefix_and_batch_boundaries(counts, limit):
    operation = make_operation(counts)
    actual = [
        item
        for batch in batches([operation], limit=limit)
        for item in zip(batch.source_addresses, batch.target_addresses, batch.sizes)
    ]
    assert actual == [
        (operation.source.address + so, operation.target.address + to, size)
        for so, to, size in old_segments(operation)
    ]


def test_zero_stride_custom_operation_falls_back():
    operation = make_operation((1,))

    class ZeroStrideOperation:
        outer_loop_counts = (1,)
        source_strides = (0,)
        target_strides = (0,)

        def iter_segments(self, *, max_segments):
            yield 4, 2, 4

    (batch,) = tuple(
        iter_transfer_batches(
            "source:12345",
            ((ZeroStrideOperation(), operation.source, operation.target),),
            max_batch_operations=8192,
            max_region_segments=1,
        )
    )
    assert batch.source_addresses == (operation.source.address + 4,)
    assert batch.target_addresses == (operation.target.address + 2,)


def test_discontinuous_expert_prefix_strides():
    def fragment(name, shape, offset, address):
        from math import prod

        return bound_fragment(
            fragment_id=name,
            tensor_id="tensor",
            global_offset=offset,
            local_shape=shape,
            address=address,
            nbytes=prod(shape) * 2,
            worker_id=name,
            endpoint=f"{name}:12345",
        )

    operation = type(make_operation((2, 3)))(
        tensor_id="tensor",
        source=fragment("source", (4, 9, 8), (0, 0, 0), 0x10000),
        target=fragment("target", (3, 7, 6), (1, 1, 1), 0x20000),
        overlap_offset=(1, 2, 2),
        overlap_shape=(2, 3, 2),
        source_base_offset=180,
        target_base_offset=14,
        inner_bytes=4,
        outer_loop_counts=(2, 3),
        source_strides=(144, 16),
        target_strides=(84, 12),
    )
    actual = [
        item
        for batch in batches([operation], limit=4)
        for item in zip(batch.source_addresses, batch.target_addresses, batch.sizes)
    ]
    assert actual == [
        (operation.source.address + so, operation.target.address + to, size)
        for so, to, size in old_segments(operation)
    ]


@pytest.mark.parametrize("multidim", [False, True])
def test_public_reader_submits_exact_old_algorithm(multidim):
    if multidim:
        sources = multi_dim_te_manifests("source", 0x100000, source=True)
        targets = multi_dim_te_manifests("target", 0x800000, source=False)
    else:
        tensor = TensorDescriptor(
            tensor_id="tensor",
            global_shape=(8201, 8),
            dtype="uint8",
            itemsize=1,
            shard_dims=(1,),
            layout_fingerprint="test",
            parallel_axes=(SplitAxis("tp", dim=1),),
        )
        sources = manifests(2, "source", 0x100000, tensor=tensor)
        targets = manifests(1, "target", 0x800000, tensor=tensor)

    def shifted(inputs):
        return RuntimeInputs(
            inputs.placement,
            tuple(
                replace(
                    binding,
                    fragments=tuple(
                        replace(
                            fragment,
                            storage_address=fragment.address - 19,
                            storage_offset_bytes=19,
                            storage_nbytes=fragment.nbytes + 19,
                        )
                        for fragment in binding.fragments
                    ),
                )
                for binding in inputs.bindings
            ),
        )

    sources, targets = shifted(sources), shifted(targets)
    target = participant_inputs(targets, 0)
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    execute_reader(
        MooncakeTransferEngineReader(
            engine, max_batch_operations=7 if multidim else 8192
        ),
        plan,
        sources,
        target,
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
    )
    expected = []
    for endpoint in sorted({op.source.endpoint for op in plan.operations}):
        ordered = sorted(
            (op for op in plan.operations if op.source.endpoint == endpoint),
            key=lambda op: (
                op.target.address + op.target_offset,
                op.source.address + op.source_offset,
            ),
        )
        expected.extend(
            (endpoint, op.target.address + to, op.source.address + so, size)
            for op in ordered
            for so, to, size in old_segments(op)
        )
    actual = [
        (endpoint, to, so, size)
        for endpoint, targets_, sources_, sizes in engine.calls
        for to, so, size in zip(targets_, sources_, sizes)
    ]
    assert actual == expected


def test_budget_rejects_before_first_batch():
    with pytest.raises(ValueError, match="exceeds max_segments"):
        next(batches([make_operation((9,))], budget=8))


def test_public_reader_budget_rejects_before_native_calls():
    tensor = TensorDescriptor(
        tensor_id="tensor",
        global_shape=(9, 8),
        dtype="uint8",
        itemsize=1,
        shard_dims=(1,),
        layout_fingerprint="test",
        parallel_axes=(SplitAxis("tp", dim=1),),
    )
    sources = manifests(2, "source", 0x100000, tensor=tensor)
    target = participant_inputs(manifests(1, "target", 0x800000, tensor=tensor), 0)
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    with pytest.raises(TransferEngineError, match="max_region_segments"):
        execute_reader(
            MooncakeTransferEngineReader(engine, max_region_segments=8),
            plan,
            sources,
            target,
            source_registrations=registration_leases(sources),
        )
    assert engine.calls == []
    assert engine.register_calls == []


@pytest.mark.parametrize("count,limit", [(1, 1), (8192, 8192), (16384, 8192)])
def test_exact_batch_boundaries(count, limit):
    result = tuple(batches([make_operation((count,))], limit=limit))
    assert [batch.operation_count for batch in result] == [limit] * (count // limit)


def test_subclass_iterator_override_is_preserved():
    original = make_operation((9,))

    class Custom(type(original)):
        def iter_segments(self, *, max_segments):
            yield 0, 0, 2

    operation = Custom(**vars(original))
    (batch,) = tuple(batches([operation]))
    assert batch.sizes == (2,)
