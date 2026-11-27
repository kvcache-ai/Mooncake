from __future__ import annotations

import ctypes

import pytest

from mooncake.reshard.transfer_engine import MooncakeTransferEngineExecutor
from mooncake.reshard.weight.inplace import InplaceWeightResharder
from mooncake.reshard.weight.plan_cache import WeightTransferPlanCache
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    TransferCompletionUnknownError,
)

from .helpers import FakeTransferEngine
from .test_inplace import CopyEngine, execute, runtime


class RecordingExecutor(MooncakeTransferEngineExecutor):
    def __init__(self, engine):
        super().__init__(engine)
        self.batch_count = 0

    def _execute_batch(self, batch, direction, submission):
        self.batch_count += 1
        return super()._execute_batch(batch, direction, submission)


def test_public_inplace_reads_bytes_through_injected_executor():
    engine = CopyEngine()
    executor = RecordingExecutor(engine)
    reader = MooncakeTransferEngineReader(engine, transfer_executor=executor)
    source_buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    target_buffer = ctypes.create_string_buffer(8)
    receipts = execute(
        InplaceWeightResharder(reader, plan_cache=WeightTransferPlanCache()),
        runtime(source_buffer, lease_id="source"),
        runtime(target_buffer, lease_id="target"),
    )
    assert reader.transfer_executor is executor
    assert executor.batch_count == 1
    assert target_buffer.raw == b"abcdefgh"
    assert sum(receipt.nbytes for receipt in receipts) == 8


@pytest.mark.parametrize("executor", [False, True, 1, object()])
def test_reader_rejects_invalid_executor(executor):
    with pytest.raises(ValueError, match="MooncakeTransferEngineExecutor"):
        MooncakeTransferEngineReader(FakeTransferEngine(), transfer_executor=executor)


def test_reader_rejects_executor_for_another_engine():
    executor = MooncakeTransferEngineExecutor(FakeTransferEngine())
    with pytest.raises(ValueError, match="same engine"):
        MooncakeTransferEngineReader(FakeTransferEngine(), transfer_executor=executor)


@pytest.mark.parametrize(
    "options",
    [{"max_completion_drain_attempts": 0}, {"completion_drain_timeout_ms": 0}],
)
def test_reader_rejects_executor_with_different_drain_settings(options):
    engine = FakeTransferEngine()
    executor = MooncakeTransferEngineExecutor(engine, **options)
    with pytest.raises(ValueError, match="drain settings"):
        MooncakeTransferEngineReader(engine, transfer_executor=executor)


def test_reader_preserves_matching_custom_drain_settings():
    engine = FakeTransferEngine()
    options = dict(max_completion_drain_attempts=0, completion_drain_timeout_ms=0)
    executor = MooncakeTransferEngineExecutor(engine, **options)
    reader = MooncakeTransferEngineReader(engine, transfer_executor=executor, **options)
    assert reader.transfer_executor is executor


def test_reader_rejects_executor_with_pending_native_transfer():
    engine = FakeTransferEngine()
    reader = MooncakeTransferEngineReader(engine)
    source_buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    target_buffer = ctypes.create_string_buffer(8)
    engine.read_result = 5
    with pytest.raises(TransferCompletionUnknownError):
        execute(
            InplaceWeightResharder(reader, plan_cache=WeightTransferPlanCache()),
            runtime(source_buffer, lease_id="source"),
            runtime(target_buffer, lease_id="target"),
        )
    executor = reader.transfer_executor
    pending = executor.pending_transfer_ids()
    assert pending
    with pytest.raises(ValueError, match="pending transfers"):
        MooncakeTransferEngineReader(engine, transfer_executor=executor)
    assert executor.pending_transfer_ids() == pending
