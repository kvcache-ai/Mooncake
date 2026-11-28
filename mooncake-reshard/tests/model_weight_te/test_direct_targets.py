from __future__ import annotations

import ctypes
from dataclasses import replace
from itertools import islice
import tracemalloc

import pytest

from mooncake.reshard.contracts import RuntimeBindingFragment
from mooncake.reshard.transfer_engine import (
    RegisteredTransferEngine,
    TransferCompletionUnknownError,
)
from mooncake.reshard.weight.direct_targets import (
    RegisteredDenseTargets,
    dense_fragment_spans,
)
from mooncake.reshard.weight.tiled_coo import TiledCOODenseFragment
from reshard_transfer_engine.test_registered import CopyEngine


def binding(owner, *, name="view", shape=(2, 3), strides=(5, 1), offset=1):
    return RuntimeBindingFragment(
        name,
        name + "-runtime",
        ctypes.addressof(owner) + offset,
        shape[0] * shape[1],
        "worker",
        "local",
        "cpu",
        1,
        shape,
        strides,
        ctypes.addressof(owner),
        ctypes.sizeof(owner),
        offset,
        owner,
    )


def fragment(*, name="view", rows=2, columns=3, width=3, source=0):
    return TiledCOODenseFragment(
        source, rows * columns, rows, columns, name, 0, 0, 0, width
    )


@pytest.mark.parametrize("strides", [(5, 1), (1, 3), (3, 1)])
def test_copy_strided_offset_alias_and_reuse(strides):
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    source = ctypes.create_string_buffer(b"abcdef")
    owner = ctypes.create_string_buffer(16)
    view = binding(owner, strides=strides)
    alias = replace(view, placement_fragment_id="alias", fragment_id="alias-runtime")
    metrics = targets.begin([view, alias])
    assert metrics["registered_owner_count"] == 1
    assert len(engine.registrations) == 1
    assert targets.read_dense(
        "remote", ctypes.addressof(source), 6, [fragment()], 1
    ) == (None, 6)
    for row in range(2):
        for col in range(3):
            assert (
                owner.raw[1 + row * strides[0] + col * strides[1]]
                == source.raw[row * 3 + col]
            )
    targets.end(retain_registration=True)
    assert targets.begin([view])["registration_reused_owner_count"] == 1
    targets.end()
    transport.close()


def test_preflight_all_fragments_before_first_batch():
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    owner = ctypes.create_string_buffer(20000)
    targets.begin([binding(owner, shape=(9000, 1), strides=(2, 1), offset=0)])
    good = fragment(rows=9000, columns=1, width=1)
    with pytest.raises(ValueError):
        targets.read_dense(
            "remote", 1024, 9000, [good, replace(good, source_byte_offset=9000)], 1
        )
    assert engine.calls == []
    assert not targets.poisoned
    targets.end()
    transport.close()


def test_second_batch_failure_poison_stays_after_transport_drain():
    engine = CopyEngine()
    engine.fail_call = 2
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    source = ctypes.create_string_buffer(9000)
    owner = ctypes.create_string_buffer(18000)
    targets.begin([binding(owner, shape=(9000, 1), strides=(2, 1), offset=0)])
    with pytest.raises(Exception):
        targets.read_dense(
            "remote",
            ctypes.addressof(source),
            9000,
            [fragment(rows=9000, columns=1, width=1)],
            1,
        )
    assert len(engine.calls) == 2
    assert max(len(call[3]) for call in engine.calls) <= 8192
    assert targets.poisoned
    assert transport.drain_pending() == ()
    with pytest.raises(RuntimeError):
        targets.end()
    with pytest.raises(RuntimeError):
        targets.release()
    transport.close()


def test_framework_layout_validation_and_owner_replacement():
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    owner = ctypes.create_string_buffer(16)
    view = binding(owner)
    targets.begin([view])

    def stale(_binding):
        raise ValueError("framework layout changed")

    with pytest.raises(ValueError, match="layout changed"):
        targets.read_dense("remote", 1024, 6, [fragment()], 1, validate_target=stale)
    assert engine.calls == []
    targets.end(retain_registration=True)
    replacement = object()
    changed = replace(view, owner=replacement)
    targets.begin([changed])
    assert engine.unregistrations == [view.storage_address]
    assert len(engine.registrations) == 2
    targets.end()
    transport.close()


def test_second_batch_unknown_keeps_generation_poisoned_after_terminal_drain():
    engine = CopyEngine()
    engine.unknown_call = 2
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    source = ctypes.create_string_buffer(9000)
    owner = ctypes.create_string_buffer(18000)
    targets.begin([binding(owner, shape=(9000, 1), strides=(2, 1), offset=0)])
    with pytest.raises(TransferCompletionUnknownError):
        targets.read_dense(
            "remote",
            ctypes.addressof(source),
            9000,
            [fragment(rows=9000, columns=1, width=1)],
            1,
        )
    assert targets.poisoned and transport.poisoned
    with pytest.raises(RuntimeError):
        transport.close()
    engine.ticket.status = "FAILED_DRAINED"
    assert transport.drain_pending() == ()
    assert not transport.poisoned and targets.poisoned
    with pytest.raises(RuntimeError):
        targets.begin([])
    transport.close()


@pytest.mark.parametrize(
    "change",
    [
        {"row_count": 0},
        {"source_byte_offset": -1},
        {"source_byte_count": 5},
        {"target_column_offset": 1},
        {"source_column_offset": 1},
        {"target_fragment_id": "missing"},
        {"row_count": True},
    ],
)
def test_invalid_dense_geometry_submits_nothing(change):
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    targets = RegisteredDenseTargets(transport)
    owner = ctypes.create_string_buffer(16)
    targets.begin([binding(owner)])
    with pytest.raises(ValueError):
        targets.read_dense("remote", 1024, 6, [replace(fragment(), **change)], 1)
    assert engine.calls == []
    targets.end()
    transport.close()


def test_million_row_lowering_does_not_allocate_row_fragments():
    owner = ctypes.create_string_buffer(2_000_000)
    view = binding(owner, shape=(1_000_000, 1), strides=(2, 1), offset=0)
    dense = fragment(rows=1_000_000, columns=1, width=1)
    tracemalloc.start()
    try:
        spans = dense_fragment_spans(dense, view)
        assert sum(size for _, _, size in islice(spans, 8192)) == 8192
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 256 * 1024
