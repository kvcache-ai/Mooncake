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

| Operation              | PyZMQ                          | Mooncake ZMQ Communicator           |
|------------------------|--------------------------------|-------------------------------------|
| Create context         | ctx = zmq.Context()            | zmq = ZmqInterface()                |
|                        |                                | zmq.initialize(ZmqConfig())         |
| Create socket          | sock = ctx.socket(zmq.PUB)     | sock = zmq.create_socket(PUB)       |
| Bind                   | sock.bind("tcp://0.0.0.0:5556")| zmq.bind(sock, "0.0.0.0:5556")      |
|                        |                                | zmq.start_server(sock)              |
| Connect                | sock.connect("tcp://host:port")| zmq.connect(sock, "host:port")      |
| Send data              | sock.send(b"data")             | zmq.publish(sock, "topic", b"data") |
| Send Python obj        | sock.send_pyobj(obj)           | zmq.send_pyobj(sock, obj, "topic")  |
| Send multipart         | sock.send_multipart(frames)    | zmq.send_multipart(sock, frames)    |
| Send JSON              | sock.send_json(obj)            | zmq.send_json(sock, obj, "topic")   |
| Send string            | sock.send_string("text")       | zmq.send_string(sock, "text", "t")  |
| Blocking receive       | data = sock.recv()             | msg = zmq.recv(sock)                |
|                        |                                | data = msg['data']                  |
| Recv Python obj        | obj = sock.recv_pyobj()        | msg = zmq.recv_pyobj(sock)          |
|                        |                                | obj = msg['obj']                    |
| Recv multipart         | frames = sock.recv_multipart() | msg = zmq.recv_multipart(sock)      |
|                        |                                | frames = msg['frames']              |
| Recv JSON              | obj = sock.recv_json()         | msg = zmq.recv_json(sock)           |
|                        |                                | obj = msg['obj']                    |
| Recv string            | text = sock.recv_string()      | msg = zmq.recv_string(sock)         |
|                        |                                | text = msg['string']                |
| Non-blocking recv      | sock.recv(flags=zmq.DONTWAIT)  | zmq.recv(sock, flags=1)             |
| Subscribe              | sock.subscribe("topic")        | zmq.subscribe(sock, "topic")        |
| Set socket option      | sock.setsockopt(opt, val)      | zmq.set_socket_option(sock, opt, v) |
| Close socket           | sock.close()                   | zmq.close_socket(sock)              |
| Cleanup                | ctx.term()                     | zmq.shutdown()                      |

Key Differences:
1. Mooncake uses socket_id (int) instead of socket objects
2. Mooncake returns dict with metadata (source, topic) instead of just data
3. Mooncake requires start_server() for bound sockets
4. Mooncake requires set_polling_mode(True) to enable blocking recv
5. Mooncake supports both callback mode (default) and polling mode (opt-in)

Usage:
------
Run all examples:
    python zmq_communicator_example.py

Each example demonstrates a different communication pattern and can be used
as a reference for implementing your own ZMQ-based applications.
"""

import time
import threading
import sys
import argparse
import traceback

try:
    # Try to import ZMQ interface from mooncake
    from mooncake.engine import ZmqInterface, ZmqSocketType, ZmqConfig
    ZMQ_AVAILABLE = True
except ImportError as e:
    print(f"Error: ZMQ interface not available: {e}")
    print("Please ensure mooncake is built with ZMQ support and PYTHONPATH is set correctly.")
    print("Example: export PYTHONPATH=/root/Mooncake/build/mooncake-integration:$PYTHONPATH")
    sys.exit(1)


def example_req_rep():
    """REQ/REP pattern example"""
    print("\n=== REQ/REP Example ===")

    # Server
    def server_thread():
        rep = ZmqInterface()
        rep.initialize(ZmqConfig())
        socket_id = rep.create_socket(ZmqSocketType.REP)
        rep.bind(socket_id, "0.0.0.0:5555")
        rep.start_server(socket_id)
        
        def handle_request(msg):
            print(f"[REP] Received: {msg['data']}")
            rep.reply(socket_id, b"Response from server")
        
        rep.set_receive_callback(socket_id, handle_request)

        # Keep server running
        time.sleep(5)

    # Start server in background
    server = threading.Thread(target=server_thread, daemon=True)
    server.start()
    time.sleep(1)  # Wait for server to start

    # Client
    req = ZmqInterface()
    req.initialize(ZmqConfig())
    socket_id = req.create_socket(ZmqSocketType.REQ)
    req.connect(socket_id, "127.0.0.1:5555")
    
    response = req.request(socket_id, b"Hello from client")
    print(f"[REQ] Got response: {response}")
    
    req.close_socket(socket_id)
    print("REQ/REP example completed")


def example_pub_sub():
    """PUB/SUB pattern example"""
    print("\n=== PUB/SUB Example ===")

    pub_endpoint = "0.0.0.0:5556"
    sub_endpoint = "0.0.0.0:5656"

    # Subscriber (binds + starts server)
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(socket_id, sub_endpoint)
    sub.start_server(socket_id)
    sub.subscribe(socket_id, "sensor.")
    
    def on_message(msg):
        print(f"[SUB] Topic: {msg['topic']}, Data: {msg['data']}")
    
    sub.set_subscribe_callback(socket_id, on_message)

    # Publisher (binds + explicitly connects to subscriber endpoint)
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_socket = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(pub_socket, pub_endpoint)
        pub.start_server(pub_socket)
        pub.connect(pub_socket, "127.0.0.1:5656")
        
        # Publish messages
        pub.publish(pub_socket, "sensor.temp", b"25.3C")
        pub.publish(pub_socket, "sensor.humidity", b"60%")
        pub.publish(pub_socket, "other.data", b"ignored")  # Not subscribed
        print("[PUB] Published messages")
        
        # Keep publisher running
        time.sleep(5)

    # Start publisher in background
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    
    time.sleep(3)  # Wait for messages
    
    sub.close_socket(socket_id)
    print("PUB/SUB example completed")


def example_push_pull():
    """PUSH/PULL pattern example"""
    print("\n=== PUSH/PULL Example ===")

    # Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5557")
        pull.start_server(socket_id)
        
        def process_task(msg):
            print(f"[PULL] Processing: {msg['data']}")
        
        pull.set_pull_callback(socket_id, process_task)

        # Keep worker running
        time.sleep(5)

    # Start worker in background
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)  # Wait for worker to start

    # Producer
    push = ZmqInterface()
    push.initialize(ZmqConfig())
    socket_id = push.create_socket(ZmqSocketType.PUSH)
    push.connect(socket_id, "127.0.0.1:5557")
    
    for i in range(5):
        push.push(socket_id, f"Task {i}".encode())
        print(f"[PUSH] Sent task {i}")
    
    time.sleep(2)  # Wait for tasks to be processed
    
    push.close_socket(socket_id)
    print("PUSH/PULL example completed")


def example_pair():
    """PAIR pattern example"""
    print("\n=== PAIR Example ===")

    peer1_endpoint = "0.0.0.0:5558"
    peer2_endpoint = "0.0.0.0:5658"

    # Peer 1
    def peer1_thread():
        pair1 = ZmqInterface()
        pair1.initialize(ZmqConfig())
        socket_id = pair1.create_socket(ZmqSocketType.PAIR)
        pair1.bind(socket_id, peer1_endpoint)
        pair1.start_server(socket_id)
        # To allow the bind-side to send without specifying target_endpoint,
        # also connect to the peer's endpoint explicitly.
        pair1.connect(socket_id, "127.0.0.1:5658")
        
        def on_message(msg):
            print(f"[PAIR1] Received: {msg['data']}")
        
        pair1.set_receive_callback(socket_id, on_message)
        
        time.sleep(2)
        pair1.send(socket_id, b"Hello from peer1")

        # Keep peer running
        time.sleep(5)

    # Start peer1 in background
    peer1 = threading.Thread(target=peer1_thread, daemon=True)
    peer1.start()
    time.sleep(1)  # Wait for peer1 to start

    # Peer 2
    pair2 = ZmqInterface()
    pair2.initialize(ZmqConfig())
    socket_id = pair2.create_socket(ZmqSocketType.PAIR)
    pair2.bind(socket_id, peer2_endpoint)
    pair2.start_server(socket_id)  # PAIR also needs server for receiving
    pair2.connect(socket_id, "127.0.0.1:5558")
    
    def on_message(msg):
        print(f"[PAIR2] Received: {msg['data']}")
    
    pair2.set_receive_callback(socket_id, on_message)
    
    pair2.send(socket_id, b"Hello from peer2")
    
    time.sleep(3)  # Wait for bidirectional messages
    
    pair2.close_socket(socket_id)
    print("PAIR example completed")


def example_pyobj():
    """Python object serialization example (send_pyobj/recv_pyobj)"""
    print("\n=== Python Object (Pyobj) Example ===")
    
    pub_endpoint = "0.0.0.0:5559"
    sub_endpoint = "0.0.0.0:5659"
    
    # Subscriber (binds + starts server)
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_socket = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(sub_socket, sub_endpoint)
    sub.start_server(sub_socket)
    sub.subscribe(sub_socket, "")  # Subscribe to all topics
    
    def on_pyobj(msg):
        print(f"[SUB] Received Python object: {msg['obj']}, Topic: {msg['topic']}")
    
    sub.set_pyobj_receive_callback(sub_socket, on_pyobj)

    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        socket_id = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(socket_id, pub_endpoint)
        pub.start_server(socket_id)
        pub.connect(socket_id, "127.0.0.1:5659")
        
        # Send Python objects directly (no manual serialization needed)
        pub.send_pyobj(socket_id, {"name": "Alice", "age": 30}, "user.info")
        pub.send_pyobj(socket_id, [1, 2, 3, 4, 5], "data.list")
        pub.send_pyobj(socket_id, {"status": "ok", "value": 42}, "system.status")
        print("[PUB] Published Python objects")
        
        time.sleep(5)
    
    # Start publisher
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(0.5)
    
    time.sleep(3)
    sub.close_socket(sub_socket)
    print("Python object example completed")


def example_multipart():
    """Multipart messages example (send_multipart/recv_multipart)"""
    print("\n=== Multipart Messages Example ===")
    
    # Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5560")
        pull.start_server(socket_id)
        
        def process_multipart(msg):
            frames = msg['frames']
            task_id = frames[0].decode()
            task_type = frames[1].decode()
            task_data = frames[2]
            print(f"[PULL] Task {task_id}: type={task_type}, data_len={len(task_data)}")
        
        pull.set_multipart_receive_callback(socket_id, process_multipart)
        time.sleep(5)
    
    # Start worker
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)
    
    # Producer
    push = ZmqInterface()
    push.initialize(ZmqConfig())
    socket_id = push.create_socket(ZmqSocketType.PUSH)
    push.connect(socket_id, "127.0.0.1:5560")
    
    # Send multipart messages (multiple frames per message)
    for i in range(3):
        frames = [
            f"task-{i}".encode(),     # Frame 1: Task ID
            b"process_image",         # Frame 2: Task type
            b"\x00\x01\x02\x03" * 10  # Frame 3: Binary data
        ]
        push.send_multipart(socket_id, frames)
        print(f"[PUSH] Sent multipart task {i}")
    
    time.sleep(2)
    push.close_socket(socket_id)
    print("Multipart messages example completed")


def example_json():
    """JSON messages example (send_json/recv_json)"""
    print("\n=== JSON Messages Example ===")
    
    pub_endpoint = "0.0.0.0:5564"
    sub_endpoint = "0.0.0.0:5664"
    
    # Subscriber
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    sub_socket = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(sub_socket, sub_endpoint)
    sub.start_server(sub_socket)
    sub.subscribe(sub_socket, "")  # Subscribe to all topics
    
    def on_json(msg):
        print(f"[SUB] Received JSON: {msg['obj']}, Topic: {msg['topic']}")
    
    sub.set_json_receive_callback(sub_socket, on_json)

    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        socket_id = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(socket_id, pub_endpoint)
        pub.start_server(socket_id)
        pub.connect(socket_id, "127.0.0.1:5664")
        
        # Send JSON objects directly (no manual serialization needed)
        pub.send_json(socket_id, {"name": "Alice", "age": 30, "role": "admin"}, "user.data")
        pub.send_json(socket_id, {"temperature": 25.5, "humidity": 60}, "sensor.data")
        pub.send_json(socket_id, [1, 2, 3, 4, 5], "list.data")
        print("[PUB] Published JSON messages")
        
        time.sleep(5)
    
    # Start publisher
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(0.5)
    
    time.sleep(3)
    sub.close_socket(sub_socket)
    print("JSON messages example completed")


def example_string():
    """String messages example (send_string/recv_string)"""
    print("\n=== String Messages Example ===")
    
    # Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5565")
        pull.start_server(socket_id)
        
        def process_string(msg):
            print(f"[PULL] Received string: '{msg['string']}'")
        
        pull.set_string_receive_callback(socket_id, process_string)
        time.sleep(5)
    
    # Start worker
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)
    
    # Producer
    push = ZmqInterface()
    push.initialize(ZmqConfig())
    socket_id = push.create_socket(ZmqSocketType.PUSH)
    push.connect(socket_id, "127.0.0.1:5565")
    
    # Send string messages (automatic UTF-8 encoding)
    messages = [
        "Hello, World!",
        "ZMQ is awesome",
        "Mooncake Transfer Engine",
        "中文消息测试",  # Chinese text
    ]
    for i, msg in enumerate(messages):
        push.send_string(socket_id, msg)
        print(f"[PUSH] Sent string {i}: '{msg}'")
        time.sleep(0.2)
    
    time.sleep(2)
    push.close_socket(socket_id)
    print("String messages example completed")


def example_recv_json_polling():
    """Blocking recv_json example using polling mode"""
    print("\n=== Recv JSON Polling Mode Example ===")
    
    pub_endpoint = "0.0.0.0:5566"
    sub_endpoint = "0.0.0.0:5666"
    
    # Subscriber using polling mode
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(socket_id, sub_endpoint)
    sub.start_server(socket_id)
    sub.subscribe(socket_id, "status")
    
    # Enable polling mode
    sub.set_polling_mode(socket_id, True)
    
    time.sleep(0.5)  # Wait for server to start
    
    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_socket = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(pub_socket, pub_endpoint)
        pub.start_server(pub_socket)
        pub.connect(pub_socket, "127.0.0.1:5666")
        
        # Send JSON objects
        objects = [
            {"status": "running", "progress": 33},
            {"status": "running", "progress": 66},
            {"status": "completed", "progress": 100}
        ]
        for i, obj in enumerate(objects):
            pub.send_json(pub_socket, obj, "status")
            print(f"[PUB] Sent JSON {i}: {obj}")
            time.sleep(0.5)
        
        time.sleep(5)
    
    # Start publisher
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    
    # Blocking receive JSON objects
    print("[SUB] Waiting for JSON messages with blocking recv_json()...")
    for i in range(3):
        try:
            msg = sub.recv_json(socket_id)  # Blocks until message arrives
            print(f"[SUB] Received JSON: {msg['obj']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            break
    
    sub.close_socket(socket_id)
    print("Recv JSON polling mode example completed")


def example_recv_string_polling():
    """Blocking recv_string example using polling mode"""
    print("\n=== Recv String Polling Mode Example ===")
    
    # Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5567")
        pull.start_server(socket_id)
        time.sleep(10)
    
    # Start worker
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)
    
    # Producer
    def producer_thread():
        push = ZmqInterface()
        push.initialize(ZmqConfig())
        socket_id = push.create_socket(ZmqSocketType.PUSH)
        push.connect(socket_id, "127.0.0.1:5567")
        
        time.sleep(0.5)
        
        messages = ["First message", "Second message", "Third message"]
        for i, msg in enumerate(messages):
            push.send_string(socket_id, msg)
            print(f"[PUSH] Sent string {i}: '{msg}'")
            time.sleep(0.5)
        
        time.sleep(5)
    
    # Start producer
    producer = threading.Thread(target=producer_thread, daemon=True)
    producer.start()
    time.sleep(0.3)
    
    # Consumer using polling mode
    pull = ZmqInterface()
    pull.initialize(ZmqConfig())
    socket_id = pull.create_socket(ZmqSocketType.PULL)
    pull.connect(socket_id, "127.0.0.1:5567")
    
    # Enable polling mode
    pull.set_polling_mode(socket_id, True)
    
    time.sleep(1)  # Wait for messages
    
    # Blocking receive string messages
    print("[PULL] Waiting for string messages with blocking recv_string()...")
    for i in range(3):
        try:
            msg = pull.recv_string(socket_id)  # Blocks until message arrives
            print(f"[PULL] Received string: '{msg['string']}'")
        except Exception as e:
            print(f"[PULL] Recv error: {e}")
            break
    
    pull.close_socket(socket_id)
    print("Recv string polling mode example completed")


def example_recv_polling():
    """Blocking recv example using polling mode (ZMQ-compatible)"""
    print("\n=== Recv Polling Mode Example ===")
    
    pub_endpoint = "0.0.0.0:5561"
    sub_endpoint = "0.0.0.0:5661"
    
    # Subscriber using polling mode (blocking recv)
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(socket_id, sub_endpoint)
    sub.start_server(socket_id)
    sub.subscribe(socket_id, "data")
    
    # Enable polling mode (required for recv)
    sub.set_polling_mode(socket_id, True)
    
    time.sleep(0.5)  # Wait for server to start
    
    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_socket = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(pub_socket, pub_endpoint)
        pub.start_server(pub_socket)
        pub.connect(pub_socket, "127.0.0.1:5661")
        
        # Publish messages
        for i in range(3):
            pub.publish(pub_socket, "data", f"Message {i}".encode())
            print(f"[PUB] Published message {i}")
            time.sleep(0.5)
        
        time.sleep(5)
    
    # Start publisher
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    
    # Blocking receive (like ZMQ's recv)
    print("[SUB] Waiting for messages with blocking recv()...")
    for i in range(3):
        try:
            msg = sub.recv(socket_id)  # Blocks until message arrives
            print(f"[SUB] Received via recv(): {msg['data']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            break
    
    sub.close_socket(socket_id)
    print("Recv polling mode example completed")


def example_recv_pyobj_polling():
    """Blocking recv_pyobj example using polling mode"""
    print("\n=== Recv Pyobj Polling Mode Example ===")
    
    pub_endpoint = "0.0.0.0:5562"
    sub_endpoint = "0.0.0.0:5662"
    
    # Subscriber using polling mode
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.bind(socket_id, sub_endpoint)
    sub.start_server(socket_id)
    sub.subscribe(socket_id, "objects")
    
    # Enable polling mode
    sub.set_polling_mode(socket_id, True)
    
    time.sleep(0.5)  # Wait for server to start
    
    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        pub_socket = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(pub_socket, pub_endpoint)
        pub.start_server(pub_socket)
        pub.connect(pub_socket, "127.0.0.1:5662")
        
        # Send Python objects
        objects = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            [1, 2, 3, 4, 5]
        ]
        for i, obj in enumerate(objects):
            pub.send_pyobj(pub_socket, obj, "objects")
            print(f"[PUB] Sent object {i}: {obj}")
            time.sleep(0.5)
        
        time.sleep(5)
    
    # Start publisher
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    
    # Blocking receive Python objects
    print("[SUB] Waiting for Python objects with blocking recv_pyobj()...")
    for i in range(3):
        try:
            msg = sub.recv_pyobj(socket_id)  # Blocks until message arrives
            print(f"[SUB] Received object: {msg['obj']}, topic: {msg['topic']}")
        except Exception as e:
            print(f"[SUB] Recv error: {e}")
            break
    
    sub.close_socket(socket_id)
    print("Recv pyobj polling mode example completed")


def example_recv_multipart_polling():
    """Blocking recv_multipart example using polling mode"""
    print("\n=== Recv Multipart Polling Mode Example ===")
    
    # Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5563")
        pull.start_server(socket_id)
        time.sleep(10)
    
    # Start worker
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)
    
    # Producer
    def producer_thread():
        push = ZmqInterface()
        push.initialize(ZmqConfig())
        socket_id = push.create_socket(ZmqSocketType.PUSH)
        push.connect(socket_id, "127.0.0.1:5563")
        
        time.sleep(0.5)
        
        for i in range(3):
            frames = [
                f"task-{i}".encode(),
                b"process",
                b"data" * 10
            ]
            push.send_multipart(socket_id, frames)
            print(f"[PUSH] Sent multipart message {i}")
            time.sleep(0.5)
        
        time.sleep(5)
    
    # Start producer
    producer = threading.Thread(target=producer_thread, daemon=True)
    producer.start()
    time.sleep(0.3)
    
    # Consumer using polling mode
    pull = ZmqInterface()
    pull.initialize(ZmqConfig())
    socket_id = pull.create_socket(ZmqSocketType.PULL)
    pull.connect(socket_id, "127.0.0.1:5563")
    
    # Enable polling mode
    pull.set_polling_mode(socket_id, True)
    
    time.sleep(1)  # Wait for messages
    
    # Blocking receive multipart messages
    print("[PULL] Waiting for multipart messages with blocking recv_multipart()...")
    for i in range(3):
        try:
            msg = pull.recv_multipart(socket_id)  # Blocks until message arrives
            frames = msg['frames']
            print(f"[PULL] Received {len(frames)} frames: "
                  f"{frames[0].decode()}, {frames[1].decode()}, data_len={len(frames[2])}")
        except Exception as e:
            print(f"[PULL] Recv error: {e}")
            break
    
    pull.close_socket(socket_id)
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
    
    test_results = {
        'passed': [],
        'failed': []
    }
    
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
            test_results['passed'].append(test_name)
            print(f"✓ {test_name} - PASSED")
        except Exception as e:
            test_results['failed'].append((test_name, str(e)))
            print(f"✗ {test_name} - FAILED: {e}")
            traceback.print_exc()
    
    # Print summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {len(test_results['passed'])}")
    print(f"Failed: {len(test_results['failed'])}")
    
    if test_results['passed']:
        print("\n✓ Passed tests:")
        for test in test_results['passed']:
            print(f"  - {test}")
    
    if test_results['failed']:
        print("\n✗ Failed tests:")
        for test, error in test_results['failed']:
            print(f"  - {test}: {error}")
    
    print("\n" + "=" * 70)
    if len(test_results['failed']) == 0:
        print("All tests PASSED! ✓")
    else:
        print(f"Some tests FAILED ({len(test_results['failed'])} failures)")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='ZMQ Communicator Test Suite - Comprehensive tests for all ZMQ features',
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
"""
    )
    
    parser.add_argument('--test', '-t', 
                        choices=['req-rep', 'pub-sub', 'push-pull', 'pair', 
                                'pyobj', 'multipart', 'json', 'string',
                                'recv-polling', 'recv-pyobj', 'recv-multipart',
                                'recv-json', 'recv-string', 'all'],
                        default='all',
                        help='Run specific test (default: all)')
    
    parser.add_argument('--list', '-l', action='store_true',
                        help='List available tests and exit')
    
    args = parser.parse_args()
    
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
        'req-rep': ("REQ/REP Pattern", example_req_rep),
        'pub-sub': ("PUB/SUB Pattern", example_pub_sub),
        'push-pull': ("PUSH/PULL Pattern", example_push_pull),
        'pair': ("PAIR Pattern", example_pair),
        'pyobj': ("Python Object Serialization", example_pyobj),
        'multipart': ("Multipart Messages", example_multipart),
        'json': ("JSON Messages", example_json),
        'string': ("String Messages", example_string),
        'recv-polling': ("Blocking Recv (Polling Mode)", example_recv_polling),
        'recv-pyobj': ("Blocking Recv Pyobj", example_recv_pyobj_polling),
        'recv-multipart': ("Blocking Recv Multipart", example_recv_multipart_polling),
        'recv-json': ("Blocking Recv JSON", example_recv_json_polling),
        'recv-string': ("Blocking Recv String", example_recv_string_polling),
    }
    
    # Run specific test or all tests
    if args.test != 'all':
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
