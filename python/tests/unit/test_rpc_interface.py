import asyncio
import gc
import os
import socket
import struct
import subprocess
import sys
import threading
import time
import weakref

import pytest


def _loopback_endpoint():
    with socket.socket() as port:
        port.bind(("127.0.0.1", 0))
        return port.getsockname()


@pytest.fixture
def rpc_peers(monkeypatch):
    engine = pytest.importorskip("mooncake.engine")
    monkeypatch.setenv("MC_RPC_PROTOCOL", "tcp")
    host, port = _loopback_endpoint()
    address = f"{host}:{port}"
    server = engine.RpcInterface()
    assert server.initialize_server(address, thread_count=2, timeout_seconds=5)
    assert server.start_server_async()
    try:
        client = engine.RpcInterface()
        assert client.initialize_client(timeout_seconds=5)
        yield server, client, address
    finally:
        server.stop_server()


def test_rpc_tensor_view_outlives_receive_callback(rpc_peers):
    server, client, address = rpc_peers
    payload = b"\x00\xff\x01"
    # The RPC tensor header carries the dtype, rank and four dimension slots.
    header = struct.pack("=II4q", 8, 1, len(payload), 0, 0, 0)
    views = []

    def receive(tensor):
        views.append(tensor.get_memory_view()[len(header) :])

    server.set_tensor_receive_callback(receive)
    assert client.send_data(address, header + payload) == 0
    (view,) = views
    assert view.obj is not None
    assert view.readonly
    assert view.tobytes() == payload


@pytest.mark.parametrize("method", ["send_data_async", "send_tensor_async"])
def test_rpc_async_send_uses_supplied_loop(rpc_peers, method):
    server, client, address = rpc_peers
    received = []
    server.set_data_receive_callback(lambda message: received.append(message["data"]))
    payload = b"\x00\xff\x01"
    request = payload
    if method == "send_tensor_async":
        torch = pytest.importorskip("torch")
        request = torch.tensor(list(payload), dtype=torch.uint8)

    async def send():
        loop = asyncio.get_running_loop()
        # Submit from a thread with no current loop and await on the supplied loop.
        future = await asyncio.to_thread(
            getattr(client, method), address, request, loop
        )
        assert future.get_loop() is loop
        assert await asyncio.wait_for(future, timeout=5) == 0

    asyncio.run(send())
    assert received == [payload]


@pytest.mark.parametrize(
    "register", ["set_data_receive_callback", "set_tensor_receive_callback"]
)
def test_rpc_receive_callback_cycle_is_collected(register):
    engine = pytest.importorskip("mooncake.engine")

    def install_callback():
        rpc = engine.RpcInterface()

        def callback(message):
            return rpc

        getattr(rpc, register)(callback)
        return weakref.ref(callback)

    callback_ref = install_callback()
    gc.collect()

    assert callback_ref() is None


def _shutdown_with_receive_in_flight(mode):
    from mooncake import engine

    gc.disable()
    endpoint = _loopback_endpoint()
    address = f"{endpoint[0]}:{endpoint[1]}"

    entered = threading.Event()
    resume = threading.Event()
    received = []

    def receive(message):
        entered.set()
        resume.wait()
        if mode == "worker_gc":
            gc.collect()
        received.append(message["data"])

    def start_server(receive_callback):
        rpc = engine.RpcInterface()
        # Let the listener close while the receive worker is still blocked.
        assert rpc.initialize_server(address, thread_count=2, timeout_seconds=5)
        rpc.set_data_receive_callback(receive_callback)
        callback_ref = None
        if mode != "destructor":
            # The callback in flight leaves this separate reference cycle
            # unreachable so GC can collect it while receive work is active.
            def tensor_receive(message):
                return rpc

            rpc.set_tensor_receive_callback(tensor_receive)
            callback_ref = weakref.ref(tensor_receive)
        assert rpc.start_server_async()
        return rpc, callback_ref

    server, callback_ref = start_server(receive)
    receive_ref = weakref.ref(receive)
    del receive
    client = engine.RpcInterface()
    assert client.initialize_client(timeout_seconds=5)
    payload = b"receive\0\xff"
    sender = threading.Thread(
        target=client.send_data, args=(address, payload), daemon=True
    )
    sender.start()
    assert entered.wait(5), "RPC receive callback did not start"

    if mode == "worker_gc":
        del server
        resume.set()
    else:

        def resume_after_listener_closes():
            # Leave the callback in flight until stop() has closed the socket.
            while not resume.is_set():
                try:
                    with socket.create_connection(endpoint, timeout=0.1):
                        pass
                except ConnectionRefusedError:
                    resume.set()
                    return
                except TimeoutError:
                    pass
                time.sleep(0.01)

        threading.Thread(target=resume_after_listener_closes, daemon=True).start()
        del server
        if mode == "gc":
            gc.collect()
        assert received == [payload], "Shutdown returned before the callback finished"

    sender.join(5)
    assert not sender.is_alive()
    assert received == [payload]
    if mode == "worker_gc":
        # Wait for the thread that joined the worker to release its callback.
        deadline = time.monotonic() + 5
        while receive_ref() is not None and time.monotonic() < deadline:
            time.sleep(0.01)
    assert receive_ref() is None
    if callback_ref is not None:
        gc.collect()
        assert callback_ref() is None


@pytest.mark.parametrize("mode", ["destructor", "gc", "worker_gc"])
def test_rpc_shutdown_drains_receive_workers(mode):
    pytest.importorskip("mooncake.engine")

    # A GIL deadlock in the child must fail the test without hanging pytest.
    subprocess.run(
        [sys.executable, __file__, mode],
        env={**os.environ, "MC_RPC_PROTOCOL": "tcp"},
        check=True,
        timeout=20,
    )


if __name__ == "__main__":
    _shutdown_with_receive_in_flight(sys.argv[1])
