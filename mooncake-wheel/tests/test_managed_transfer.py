"""Opt-in native CPU tests for managed single-range transfers."""

import ctypes
import gc
import os
import time
import weakref

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("MOONCAKE_TEST_MANAGED_TRANSFER") != "1",
    reason="set MOONCAKE_TEST_MANAGED_TRANSFER=1 with a rebuilt native engine",
)
_SIZE = 131076


def _wait(ticket):
    deadline = time.monotonic() + 10
    while True:
        done, error = ticket.poll()
        if done:
            assert not error, error
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("native transfer did not drain")
        time.sleep(0.001)


@pytest.fixture
def pair():
    from mooncake.engine import TransferEngine

    engines = [TransferEngine(), TransferEngine()]
    source = ctypes.create_string_buffer(bytes(i % 251 for i in range(_SIZE)), _SIZE)
    target = ctypes.create_string_buffer(_SIZE)
    for engine, buffer in zip(engines, (source, target)):
        assert engine.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", "") == 0
        assert engine.register_memory(ctypes.addressof(buffer), _SIZE) == 0
    try:
        yield engines, source, target
    finally:
        for engine, buffer in zip(engines, (source, target)):
            assert engine.unregister_memory(ctypes.addressof(buffer)) == 0


@pytest.mark.parametrize("read", [False, True])
def test_read_write_and_repeated_poll(pair, read):
    engines, source, target = pair
    local, remote = (engines[1], engines[0]) if read else engines
    local_buffer, remote_buffer = (target, source) if read else (source, target)
    local.refresh_remote(remote.get_endpoint())
    ticket = local.submit_transfer(
        remote.get_endpoint(),
        ctypes.addressof(local_buffer),
        ctypes.addressof(remote_buffer),
        _SIZE,
        local.TransferOpcode.Read if read else local.TransferOpcode.Write,
    )
    _wait(ticket)
    assert target.raw == source.raw
    assert ticket.poll() == (True, "")
    assert ticket.drained


def test_invalid_range_is_a_drained_failure(pair):
    engines, _, target = pair
    pointer_max = (1 << (8 * ctypes.sizeof(ctypes.c_void_p))) - 1
    ticket = engines[0].submit_transfer(
        engines[1].get_endpoint(),
        pointer_max,
        ctypes.addressof(target),
        2,
        engines[0].TransferOpcode.Write,
    )
    done, error = ticket.poll()
    assert done and error
    assert ticket.drained
    assert ticket.poll() == (done, error)


def test_ticket_retains_engine(pair):
    engines, source, target = pair
    reference = weakref.ref(engines[0])
    ticket = engines[0].submit_transfer(
        engines[1].get_endpoint(),
        ctypes.addressof(source),
        ctypes.addressof(target),
        _SIZE,
        engines[0].TransferOpcode.Write,
    )
    engines[0] = None
    try:
        gc.collect()
        assert reference() is not None
        _wait(ticket)
        assert target.raw == source.raw
    finally:
        engines[0] = reference()


def test_uninitialized_engine_rejected():
    from mooncake.engine import TransferEngine

    engine = TransferEngine()
    with pytest.raises(RuntimeError, match="not initialized"):
        engine.get_endpoint()
    with pytest.raises(RuntimeError, match="refresh"):
        engine.refresh_remote("127.0.0.1:1")
    with pytest.raises(RuntimeError, match="not initialized"):
        engine.submit_transfer("127.0.0.1:1", 1, 1, 1, engine.TransferOpcode.Write)
