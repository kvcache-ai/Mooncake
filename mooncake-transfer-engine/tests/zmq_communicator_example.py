#!/usr/bin/env python3
"""
ZMQ Communicator Comprehensive Example and Test Suite

This file demonstrates and tests all ZMQ Communicator features:
- All socket patterns (REQ/REP, PUB/SUB, PUSH/PULL, PAIR)
- Send methods (sync and async)
- Receive methods (callback mode and polling mode)
- Python object serialization (send_pyobj/recv_pyobj)
- Multipart messages (send_multipart/recv_multipart)
- JSON serialization (send_json/recv_json)
- String encoding (send_string/recv_string)
- Socket options and configuration

API Compatibility with PyZMQ:
=============================

| Operation              | PyZMQ                          | Mooncake ZMQ Communicator (compat)       |
|------------------------|--------------------------------|------------------------------------------|
| Create context         | ctx = zmq.Context()            | zmq = ZmqInterface()                     |
|                        |                                | zmq.initialize(ZmqConfig())              |
| Create socket          | sock = ctx.socket(zmq.PUB)     | sock = zmq.socket(ZmqSocketType.PUB)     |
| Bind                   | sock.bind("tcp://0.0.0.0:5556")| sock.bind("0.0.0.0:5556")                |
|                        |                                | sock.start_server()                      |
| Connect                | sock.connect("tcp://host:port")| sock.connect("host:port")                |
| Send data              | sock.send(b"data")             | sock.send(b"data")                        |
| Send Python obj        | sock.send_pyobj(obj)           | sock.send_pyobj(obj)                      |
| Send multipart         | sock.send_multipart(frames)    | sock.send_multipart(frames)              |
| Send JSON              | sock.send_json(obj)            | sock.send_json(obj)                       |
| Send string            | sock.send_string("text")       | sock.send_string("text")                  |
| Enable blocking recv   | N/A (default blocking)         | sock.set_polling_mode(True)              |
| Blocking receive       | data = sock.recv()             | msg = sock.recv()                        |
|                        |                                | data = msg['data']                       |
| Recv Python obj        | obj = sock.recv_pyobj()        | msg = sock.recv_pyobj()                  |
|                        |                                | obj = msg['obj']                         |
| Recv multipart         | frames = sock.recv_multipart() | msg = sock.recv_multipart()              |
|                        |                                | frames = msg['frames']                   |
| Recv JSON              | obj = sock.recv_json()         | msg = sock.recv_json()                   |
|                        |                                | obj = msg['obj']                         |
| Recv string            | text = sock.recv_string()      | msg = sock.recv_string()                 |
|                        |                                | text = msg['string']                     |
| Non-blocking recv      | sock.recv(flags=zmq.DONTWAIT)  | sock.recv(flags=1)                       |
| Subscribe              | sock.subscribe("topic")        | sock.subscribe("topic")                  |
| Set socket option      | sock.setsockopt(opt, val)      | sock.setsockopt(opt, val)                |
| Close socket           | sock.close()                   | sock.close()                             |
| Cleanup                | ctx.term()                     | zmq.term() / zmq.shutdown()              |

Note: The right column shows a PyZMQ-style compatibility view. Mooncake native API
still uses `socket_id` as the first argument internally.

Key Differences:
1. Mooncake now supports PyZMQ-style `sock` objects via `zmq.socket(...)`, but native API still exposes `socket_id`-first methods.
2. Mooncake returns dict with metadata (`source`, `topic`, etc.) instead of only payload.
3. Mooncake requires `start_server()` for bound sockets.
4. Mooncake polling receive APIs (`recv*`) require `sock.set_polling_mode(True)`.
5. Mooncake supports both callback mode (default) and polling mode (opt-in).

Usage:
------
Run all examples:
    python zmq_communicator_example.py

For recv_pyobj tests, enable pickle (required for Python object deserialization):
    MOONCAKE_ALLOW_PICKLE=1 python zmq_communicator_example.py

Debugging (e.g. segfaults): use verbose step-by-step logs and faulthandler:
    python zmq_communicator_example.py --verbose --test recv-pyobj
    MOONCAKE_ALLOW_PICKLE=1 python zmq_communicator_example.py -v -t recv-pyobj

Each example uses a two-end flow: server (bind) starts first and signals ready,
then client (connect) runs. Use --verbose to see phase logs.
"""

import os
import time
import threading
import sys
import argparse
import traceback

# Dump Python traceback on segfault (e.g. in C extension) to help debug
try:
    import faulthandler

    faulthandler.enable(all_threads=True)
except Exception:
    pass

# Verbose logging: set by --verbose or env ZMQ_EXAMPLE_VERBOSE=1
VERBOSE = os.environ.get("ZMQ_EXAMPLE_VERBOSE", "0").strip() in ("1", "yes", "true")


def log(role, msg, force=False):
    """Print a timestamped log line; use VERBOSE or force=True for step-level logs."""
    if VERBOSE or force:
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(f"  [{ts}] [{role}] {msg}", flush=True)


try:
    # Try to import ZMQ interface from mooncake
    from mooncake.engine import ZmqInterface, ZmqSocketType, ZmqConfig

    ZMQ_AVAILABLE = True
except ImportError as e:
    print(f"Error: ZMQ interface not available: {e}")
    print(
        "Please ensure mooncake is built with ZMQ support and PYTHONPATH is set correctly."
    )
    print(
        "Example: export PYTHONPATH=/root/Mooncake/build/mooncake-integration:$PYTHONPATH"
    )
    sys.exit(1)


def example_req_rep():
    """REQ/REP pattern example - full two-end flow: server binds first, then client connects."""
    print("\n=== REQ/REP Example ===")
    server_ready = threading.Event()

    def server_thread():
        log("REP", "Phase 1: create and bind REP socket")
        rep = ZmqInterface()
        rep.initialize(ZmqConfig())
        rep_sock = rep.socket(ZmqSocketType.REP)
        rep_sock.bind("0.0.0.0:5555")
        rep_sock.start_server()
        log("REP", "Phase 2: server started, signaling ready")
        server_ready.set()

        def handle_request(msg):
            print(f"[REP] Received: {msg['data']}")
            rep_sock.reply(b"Response from server")

        rep_sock.set_receive_callback(handle_request)
        time.sleep(5)

    server = threading.Thread(target=server_thread, daemon=True)
    server.start()
    ok = server_ready.wait(timeout=5.0)
    if not ok:
        raise RuntimeError("REP server did not become ready in time")
    log("REQ", "Phase 3: client connecting")
    time.sleep(0.3)

    req = ZmqInterface()
    req.initialize(ZmqConfig())
    req_sock = req.socket(ZmqSocketType.REQ)
    req_sock.connect("127.0.0.1:5555")
    log("REQ", "Phase 4: sending request")
    response = req_sock.request(b"Hello from client")
    print(f"[REQ] Got response: {response}")

    req_sock.close()
    print("REQ/REP example completed")


def example_pub_sub():
    """PUB/SUB pattern example - SUB binds first (server), then PUB binds and connects to SUB."""
    print("\n=== PUB/SUB Example ===")
    pub_endpoint = "0.0.0.0:5556"
    sub_endpoint = "0.0.0.0:5656"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("sensor.")
    sub_ready.set()

    def on_message(msg):
        print(f"[SUB] Topic: {msg['topic']}, Data: {msg['data']}")

    sub_sock.set_subscribe_callback(on_message)
    log("SUB", "Phase 2: SUB ready, starting PUB in background")
    time.sleep(0.5)

    def publisher_thread():
        pub_ready = sub_ready.wait(timeout=3.0)
        if not pub_ready:
            return
        log("PUB", "Phase 3: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5656")
        time.sleep(0.5)
        log("PUB", "Phase 4: publishing messages")
        pub_sock.publish("sensor.temp", b"25.3C")
        pub_sock.publish("sensor.humidity", b"60%")
        pub_sock.publish("other.data", b"ignored")
        print("[PUB] Published messages")
        time.sleep(3)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(4)
    sub_sock.close()
    print("PUB/SUB example completed")


def example_push_pull():
    """PUSH/PULL pattern example - PULL (worker) binds first, then PUSH connects."""
    print("\n=== PUSH/PULL Example ===")
    pull_ready = threading.Event()

    def worker_thread():
        log("PULL", "Phase 1: create PULL, bind, start_server")
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        pull_sock = pull.socket(ZmqSocketType.PULL)
        pull_sock.bind("0.0.0.0:5557")
        pull_sock.start_server()

        def process_task(msg):
            print(f"[PULL] Processing: {msg['data']}")

        pull_sock.set_pull_callback(process_task)
        log("PULL", "Phase 2: worker ready")
        pull_ready.set()
        time.sleep(5)

    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    if not pull_ready.wait(timeout=5.0):
        raise RuntimeError("PULL worker did not become ready in time")
    log("PUSH", "Phase 3: connect and send tasks")
    time.sleep(0.3)

    push = ZmqInterface()
    push.initialize(ZmqConfig())
    push_sock = push.socket(ZmqSocketType.PUSH)
    push_sock.connect("127.0.0.1:5557")
    for i in range(5):
        push_sock.push(f"Task {i}".encode())
        print(f"[PUSH] Sent task {i}")
    time.sleep(2)
    push_sock.close()
    print("PUSH/PULL example completed")


def example_pair():
    """PAIR pattern example - one peer binds, the other connects (no double bind/connect)."""
    print("\n=== PAIR Example ===")
    peer1_endpoint = "0.0.0.0:5558"
    peer1_ready = threading.Event()

    def peer1_thread():
        log("PAIR1", "Phase 1: bind and start_server (no connect)")
        pair1 = ZmqInterface()
        pair1.initialize(ZmqConfig())
        pair1_sock = pair1.socket(ZmqSocketType.PAIR)
        pair1_sock.bind(peer1_endpoint)
        pair1_sock.start_server()

        def on_message(msg):
            print(f"[PAIR1] Received: {msg['data']}")

        pair1_sock.set_receive_callback(on_message)
        peer1_ready.set()
        time.sleep(2)
        pair1_sock.send(b"Hello from peer1")
        time.sleep(5)

    peer1 = threading.Thread(target=peer1_thread, daemon=True)
    peer1.start()
    if not peer1_ready.wait(timeout=5.0):
        raise RuntimeError("PAIR peer1 did not become ready")
    log("PAIR2", "Phase 2: connect only (no bind) to peer1")
    time.sleep(0.3)

    pair2 = ZmqInterface()
    pair2.initialize(ZmqConfig())
    pair2_sock = pair2.socket(ZmqSocketType.PAIR)
    pair2_sock.connect("127.0.0.1:5558")

    def on_message(msg):
        print(f"[PAIR2] Received: {msg['data']}")

    pair2_sock.set_receive_callback(on_message)
    pair2_sock.send(b"Hello from peer2")
    time.sleep(3)
    pair2_sock.close()
    print("PAIR example completed")


def example_pyobj():
    """Python object serialization - SUB binds first, PUB connects and sends pyobj (callback mode)."""
    print("\n=== Python Object (Pyobj) Example ===")
    pub_endpoint = "0.0.0.0:5559"
    sub_endpoint = "0.0.0.0:5659"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server, set pyobj callback")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("")

    def on_pyobj(msg):
        print(f"[SUB] Received Python object: {msg['obj']}, Topic: {msg['topic']}")

    sub_sock.set_pyobj_receive_callback(on_pyobj)
    sub_ready.set()
    time.sleep(0.5)

    def publisher_thread():
        sub_ready.wait(timeout=3.0)
        log("PUB", "Phase 2: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5659")
        time.sleep(0.3)
        pub_sock.send_pyobj({"name": "Alice", "age": 30}, "user.info")
        pub_sock.send_pyobj([1, 2, 3, 4, 5], "data.list")
        pub_sock.send_pyobj({"status": "ok", "value": 42}, "system.status")
        print("[PUB] Published Python objects")
        time.sleep(3)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(4)
    sub_sock.close()
    print("Python object example completed")


def example_multipart():
    """Multipart messages - PULL binds first, PUSH connects and sends multipart."""
    print("\n=== Multipart Messages Example ===")
    pull_ready = threading.Event()

    def worker_thread():
        log("PULL", "Phase 1: bind, start_server, set multipart callback")
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        pull_sock = pull.socket(ZmqSocketType.PULL)
        pull_sock.bind("0.0.0.0:5560")
        pull_sock.start_server()

        def process_multipart(msg):
            frames = msg["frames"]
            task_id = frames[0].decode()
            task_type = frames[1].decode()
            task_data = frames[2]
            print(f"[PULL] Task {task_id}: type={task_type}, data_len={len(task_data)}")

        pull_sock.set_multipart_receive_callback(process_multipart)
        pull_ready.set()
        time.sleep(5)

    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    if not pull_ready.wait(timeout=5.0):
        raise RuntimeError("PULL did not become ready")
    log("PUSH", "Phase 2: connect and send multipart")
    time.sleep(0.3)
    push = ZmqInterface()
    push.initialize(ZmqConfig())
    push_sock = push.socket(ZmqSocketType.PUSH)
    push_sock.connect("127.0.0.1:5560")
    for i in range(3):
        frames = [
            f"task-{i}".encode(),
            b"process_image",
            b"\x00\x01\x02\x03" * 10,
        ]
        push_sock.send_multipart(frames)
        print(f"[PUSH] Sent multipart task {i}")
    time.sleep(2)
    push_sock.close()
    print("Multipart messages example completed")


def example_json():
    """JSON messages - SUB binds first, PUB connects and sends JSON (callback mode)."""
    print("\n=== JSON Messages Example ===")
    pub_endpoint = "0.0.0.0:5564"
    sub_endpoint = "0.0.0.0:5664"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server, set JSON callback")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("")

    def on_json(msg):
        print(f"[SUB] Received JSON: {msg['obj']}, Topic: {msg['topic']}")

    sub_sock.set_json_receive_callback(on_json)
    sub_ready.set()
    time.sleep(0.5)

    def publisher_thread():
        sub_ready.wait(timeout=3.0)
        log("PUB", "Phase 2: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5664")
        time.sleep(0.3)
        pub_sock.send_json({"name": "Alice", "age": 30, "role": "admin"}, "user.data")
        pub_sock.send_json({"temperature": 25.5, "humidity": 60}, "sensor.data")
        pub_sock.send_json([1, 2, 3, 4, 5], "list.data")
        print("[PUB] Published JSON messages")
        time.sleep(3)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(4)
    sub_sock.close()
    print("JSON messages example completed")


def example_string():
    """String messages - PULL binds first, PUSH connects and sends strings (callback mode)."""
    print("\n=== String Messages Example ===")
    pull_ready = threading.Event()

    def worker_thread():
        log("PULL", "Phase 1: bind, start_server, set string callback")
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        pull_sock = pull.socket(ZmqSocketType.PULL)
        pull_sock.bind("0.0.0.0:5565")
        pull_sock.start_server()

        def process_string(msg):
            print(f"[PULL] Received string: '{msg['string']}'")

        pull_sock.set_string_receive_callback(process_string)
        pull_ready.set()
        time.sleep(5)

    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    if not pull_ready.wait(timeout=5.0):
        raise RuntimeError("PULL did not become ready")
    log("PUSH", "Phase 2: connect and send strings")
    time.sleep(0.3)
    push = ZmqInterface()
    push.initialize(ZmqConfig())
    push_sock = push.socket(ZmqSocketType.PUSH)
    push_sock.connect("127.0.0.1:5565")
    messages = [
        "Hello, World!",
        "ZMQ is awesome",
        "Mooncake Transfer Engine",
        "中文消息测试",
    ]
    for i, msg in enumerate(messages):
        push_sock.send_string(msg)
        print(f"[PUSH] Sent string {i}: '{msg}'")
        time.sleep(0.2)
    time.sleep(2)
    push_sock.close()
    print("String messages example completed")


def example_recv_json_polling():
    """Blocking recv_json - SUB binds first, PUB connects and sends JSON; SUB uses polling recv_json."""
    print("\n=== Recv JSON Polling Mode Example ===")
    pub_endpoint = "0.0.0.0:5566"
    sub_endpoint = "0.0.0.0:5666"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server, set polling mode")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("status")
    sub_sock.set_polling_mode(True)
    sub_ready.set()
    time.sleep(0.5)

    def publisher_thread():
        sub_ready.wait(timeout=3.0)
        log("PUB", "Phase 2: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5666")
        time.sleep(0.5)
        objects = [
            {"status": "running", "progress": 33},
            {"status": "running", "progress": 66},
            {"status": "completed", "progress": 100},
        ]
        for i, obj in enumerate(objects):
            pub_sock.send_json(obj, "status")
            print(f"[PUB] Sent JSON {i}: {obj}")
            time.sleep(0.5)
        time.sleep(5)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    print("[SUB] Waiting for JSON messages with blocking recv_json()...")
    for i in range(3):
        try:
            msg = sub_sock.recv_json()
            print(f"[SUB] Received JSON: {msg['obj']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            break
    sub_sock.close()
    print("Recv JSON polling mode example completed")


def example_recv_string_polling():
    """Blocking recv_string - PULL (worker) binds first, PUSH sends; second PULL connects and uses polling recv_string."""
    print("\n=== Recv String Polling Mode Example ===")
    pull_ready = threading.Event()

    def worker_thread():
        log("PULL(worker)", "Phase 1: bind, start_server (no callback, just hold port)")
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        pull_sock = pull.socket(ZmqSocketType.PULL)
        pull_sock.bind("0.0.0.0:5567")
        pull_sock.start_server()
        pull_ready.set()
        time.sleep(10)

    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    if not pull_ready.wait(timeout=5.0):
        raise RuntimeError("PULL worker did not become ready")
    time.sleep(0.3)

    def producer_thread():
        log("PUSH", "Phase 2: connect and send strings")
        push = ZmqInterface()
        push.initialize(ZmqConfig())
        push_sock = push.socket(ZmqSocketType.PUSH)
        push_sock.connect("127.0.0.1:5567")
        time.sleep(0.5)
        messages = ["First message", "Second message", "Third message"]
        for i, msg in enumerate(messages):
            push_sock.send_string(msg)
            print(f"[PUSH] Sent string {i}: '{msg}'")
            time.sleep(0.5)
        time.sleep(5)

    producer = threading.Thread(target=producer_thread, daemon=True)
    producer.start()
    time.sleep(0.5)

    log("PULL(consumer)", "Phase 3: connect, set polling mode, recv_string")
    pull = ZmqInterface()
    pull.initialize(ZmqConfig())
    pull_sock = pull.socket(ZmqSocketType.PULL)
    pull_sock.connect("127.0.0.1:5567")
    pull_sock.set_polling_mode(True)
    time.sleep(1)
    print("[PULL] Waiting for string messages with blocking recv_string()...")
    for i in range(3):
        try:
            msg = pull_sock.recv_string()
            print(f"[PULL] Received string: '{msg['string']}'")
        except Exception as e:
            print(f"[PULL] Recv error: {e}")
            break
    pull_sock.close()
    print("Recv string polling mode example completed")


def example_recv_polling():
    """Blocking recv example - SUB binds first, then PUB connects and publishes; SUB uses polling recv."""
    print("\n=== Recv Polling Mode Example ===")
    pub_endpoint = "0.0.0.0:5561"
    sub_endpoint = "0.0.0.0:5661"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server, set polling mode")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("data")
    sub_sock.set_polling_mode(True)
    sub_ready.set()
    time.sleep(0.5)

    def publisher_thread():
        sub_ready.wait(timeout=3.0)
        log("PUB", "Phase 2: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5661")
        time.sleep(0.5)
        log("PUB", "Phase 3: publishing messages")
        for i in range(3):
            pub_sock.publish("data", f"Message {i}".encode())
            print(f"[PUB] Published message {i}")
            time.sleep(0.5)
        time.sleep(5)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    print("[SUB] Waiting for messages with blocking recv()...")
    for i in range(3):
        try:
            log("SUB", f"recv() call {i+1}/3 ...", force=True)
            msg = sub_sock.recv()
            print(f"[SUB] Received via recv(): {msg['data']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            break
    sub_sock.close()
    print("Recv polling mode example completed")


def example_recv_pyobj_polling():
    """Blocking recv_pyobj example - SUB binds first, PUB connects and sends pyobj; SUB uses polling recv_pyobj."""
    print("\n=== Recv Pyobj Polling Mode Example ===")
    allow_pickle = os.environ.get("MOONCAKE_ALLOW_PICKLE", "")
    if allow_pickle not in ("1", "yes", "true"):
        log(
            "SUB",
            "MOONCAKE_ALLOW_PICKLE not set; recv_pyobj may raise or segfault. Set MOONCAKE_ALLOW_PICKLE=1 to enable.",
            force=True,
        )

    pub_endpoint = "0.0.0.0:5562"
    sub_endpoint = "0.0.0.0:5662"
    sub_ready = threading.Event()

    log("SUB", "Phase 1: create SUB, bind, start_server, set polling mode")
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_sock = sub.socket(ZmqSocketType.SUB)
    sub_sock.bind(sub_endpoint)
    sub_sock.start_server()
    sub_sock.subscribe("objects")
    sub_sock.set_polling_mode(True)
    sub_ready.set()
    time.sleep(0.5)

    def publisher_thread():
        sub_ready.wait(timeout=3.0)
        log("PUB", "Phase 2: create PUB, bind, start_server, connect to SUB")
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_sock = pub.socket(ZmqSocketType.PUB)
        pub_sock.bind(pub_endpoint)
        pub_sock.start_server()
        pub_sock.connect("127.0.0.1:5662")
        time.sleep(0.5)
        objects = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            [1, 2, 3, 4, 5],
        ]
        log("PUB", "Phase 3: sending Python objects via send_pyobj")
        for i, obj in enumerate(objects):
            pub_sock.send_pyobj(obj, "objects")
            print(f"[PUB] Sent object {i}: {obj}")
            time.sleep(0.5)
        time.sleep(5)

    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(1.0)

    print("[SUB] Waiting for Python objects with blocking recv_pyobj()...")
    for i in range(3):
        try:
            log("SUB", f"recv_pyobj() call {i+1}/3 (before C++ call)...", force=True)
            msg = sub_sock.recv_pyobj()
            log(
                "SUB",
                f"recv_pyobj() call {i+1}/3 returned (building result dict)...",
                force=True,
            )
            print(f"[SUB] Received object: {msg['obj']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            log("SUB", f"Exception type: {type(e).__name__}", force=True)
            break
    log("SUB", "Closing socket and exiting", force=True)
    sub_sock.close()
    print("Recv pyobj polling mode example completed")


def example_recv_multipart_polling():
    """Blocking recv_multipart - PULL (worker) binds first, PUSH sends; second PULL connects and uses polling recv_multipart."""
    print("\n=== Recv Multipart Polling Mode Example ===")
    pull_ready = threading.Event()

    def worker_thread():
        log("PULL(worker)", "Phase 1: bind, start_server")
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        pull_sock = pull.socket(ZmqSocketType.PULL)
        pull_sock.bind("0.0.0.0:5563")
        pull_sock.start_server()
        pull_ready.set()
        time.sleep(10)

    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    if not pull_ready.wait(timeout=5.0):
        raise RuntimeError("PULL worker did not become ready")
    time.sleep(0.3)

    def producer_thread():
        log("PUSH", "Phase 2: connect and send multipart")
        push = ZmqInterface()
        push.initialize(ZmqConfig())
        push_sock = push.socket(ZmqSocketType.PUSH)
        push_sock.connect("127.0.0.1:5563")
        time.sleep(0.5)
        for i in range(3):
            frames = [f"task-{i}".encode(), b"process", b"data" * 10]
            push_sock.send_multipart(frames)
            print(f"[PUSH] Sent multipart message {i}")
            time.sleep(0.5)
        time.sleep(5)

    producer = threading.Thread(target=producer_thread, daemon=True)
    producer.start()
    time.sleep(0.5)

    log("PULL(consumer)", "Phase 3: connect, set polling mode, recv_multipart")
    pull = ZmqInterface()
    pull.initialize(ZmqConfig())
    pull_sock = pull.socket(ZmqSocketType.PULL)
    pull_sock.connect("127.0.0.1:5563")
    pull_sock.set_polling_mode(True)
    time.sleep(1)
    print("[PULL] Waiting for multipart messages with blocking recv_multipart()...")
    for i in range(3):
        try:
            msg = pull_sock.recv_multipart()
            frames = msg["frames"]
            print(
                f"[PULL] Received {len(frames)} frames: "
                f"{frames[0].decode()}, {frames[1].decode()}, data_len={len(frames[2])}"
            )
        except Exception as e:
            print(f"[PULL] Recv error: {e}")
            break
    pull_sock.close()
    print("Recv multipart polling mode example completed")


def main():
    print("=" * 70)
    print("ZMQ Communicator Comprehensive Test Suite")
    print("=" * 70)
    print("\nThis test suite covers all ZMQ Communicator features:")
    print("  - Communication patterns: REQ/REP, PUB/SUB, PUSH/PULL, PAIR")
    print("  - Receive modes: Callback-based and Polling-based (ZMQ-compatible)")
    print("  - Message types: Data, Python objects (pickle), Multipart, JSON, String")
    print("  - Both synchronous and asynchronous operations")
    print("\nTip: Use --test <name> to run a specific test")
    print("     Use --list to see available tests")
    print()

    test_results = {"passed": [], "failed": []}

    # Test list - reuse the test_map structure
    tests = [
        ("REQ/REP Pattern", example_req_rep),
        ("PUB/SUB Pattern", example_pub_sub),
        ("PUSH/PULL Pattern", example_push_pull),
        ("PAIR Pattern", example_pair),
        ("Python Object Serialization", example_pyobj),
        ("Multipart Messages", example_multipart),
        ("JSON Messages", example_json),
        ("String Messages", example_string),
        ("Blocking Recv (Polling Mode)", example_recv_polling),
        ("Blocking Recv Pyobj", example_recv_pyobj_polling),
        ("Blocking Recv Multipart", example_recv_multipart_polling),
        ("Blocking Recv JSON", example_recv_json_polling),
        ("Blocking Recv String", example_recv_string_polling),
    ]

    for test_name, test_func in tests:
        try:
            print(f"\n{'='*70}")
            print(f"Running: {test_name}")
            print(f"{'='*70}")
            test_func()
            test_results["passed"].append(test_name)
            print(f"✓ {test_name} - PASSED")
        except Exception as e:
            test_results["failed"].append((test_name, str(e)))
            print(f"✗ {test_name} - FAILED: {e}")
            traceback.print_exc()

    # Print summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {len(test_results['passed'])}")
    print(f"Failed: {len(test_results['failed'])}")

    if test_results["passed"]:
        print("\n✓ Passed tests:")
        for test in test_results["passed"]:
            print(f"  - {test}")

    if test_results["failed"]:
        print("\n✗ Failed tests:")
        for test, error in test_results["failed"]:
            print(f"  - {test}: {error}")

    print("\n" + "=" * 70)
    if len(test_results["failed"]) == 0:
        print("All tests PASSED! ✓")
    else:
        print(f"Some tests FAILED ({len(test_results['failed'])} failures)")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ZMQ Communicator Test Suite - Comprehensive tests for all ZMQ features",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python zmq_communicator_example.py
  
  # Run specific test
  python zmq_communicator_example.py --test req-rep
  
  # List available tests
  python zmq_communicator_example.py --list
  
Available tests:
  req-rep       - REQ/REP request-response pattern
  pub-sub       - PUB/SUB publish-subscribe pattern
  push-pull     - PUSH/PULL pipeline pattern
  pair          - PAIR exclusive pair pattern
  pyobj         - Python object serialization
  multipart     - Multipart messages
  json          - JSON serialization
  string        - String encoding/decoding
  recv-polling  - Blocking receive (polling mode)
  recv-pyobj    - Blocking receive for Python objects
  recv-multipart- Blocking receive for multipart messages
  recv-json     - Blocking receive for JSON
  recv-string   - Blocking receive for strings
  all           - Run all tests (default)
""",
    )

    parser.add_argument(
        "--test",
        "-t",
        choices=[
            "req-rep",
            "pub-sub",
            "push-pull",
            "pair",
            "pyobj",
            "multipart",
            "json",
            "string",
            "recv-polling",
            "recv-pyobj",
            "recv-multipart",
            "recv-json",
            "recv-string",
            "all",
        ],
        default="all",
        help="Run specific test (default: all)",
    )

    parser.add_argument(
        "--list", "-l", action="store_true", help="List available tests and exit"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose step-by-step logging (for debugging segfaults and connection order)",
    )

    args = parser.parse_args()
    if args.verbose:
        VERBOSE = True  # module-level; no 'global' needed

    if args.list:
        print("Available tests:")
        print("  req-rep       - REQ/REP request-response pattern")
        print("  pub-sub       - PUB/SUB publish-subscribe pattern")
        print("  push-pull     - PUSH/PULL pipeline pattern")
        print("  pair          - PAIR exclusive pair pattern")
        print("  pyobj         - Python object serialization")
        print("  multipart     - Multipart messages")
        print("  json          - JSON serialization")
        print("  string        - String encoding/decoding")
        print("  recv-polling  - Blocking receive (polling mode)")
        print("  recv-pyobj    - Blocking receive for Python objects")
        print("  recv-multipart- Blocking receive for multipart messages")
        print("  recv-json     - Blocking receive for JSON")
        print("  recv-string   - Blocking receive for strings")
        print("  all           - Run all tests (default)")
        sys.exit(0)

    # Map test names to functions
    test_map = {
        "req-rep": ("REQ/REP Pattern", example_req_rep),
        "pub-sub": ("PUB/SUB Pattern", example_pub_sub),
        "push-pull": ("PUSH/PULL Pattern", example_push_pull),
        "pair": ("PAIR Pattern", example_pair),
        "pyobj": ("Python Object Serialization", example_pyobj),
        "multipart": ("Multipart Messages", example_multipart),
        "json": ("JSON Messages", example_json),
        "string": ("String Messages", example_string),
        "recv-polling": ("Blocking Recv (Polling Mode)", example_recv_polling),
        "recv-pyobj": ("Blocking Recv Pyobj", example_recv_pyobj_polling),
        "recv-multipart": ("Blocking Recv Multipart", example_recv_multipart_polling),
        "recv-json": ("Blocking Recv JSON", example_recv_json_polling),
        "recv-string": ("Blocking Recv String", example_recv_string_polling),
    }

    # Run specific test or all tests
    if args.test != "all":
        test_name, test_func = test_map[args.test]
        print("=" * 70)
        print(f"Running single test: {test_name}")
        print("=" * 70)
        try:
            test_func()
            print(f"\n✓ {test_name} - PASSED")
        except Exception as e:
            print(f"\n✗ {test_name} - FAILED: {e}")
            traceback.print_exc()
            sys.exit(1)
    else:
        main()
