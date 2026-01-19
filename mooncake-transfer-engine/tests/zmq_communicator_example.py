#!/usr/bin/env python3
"""
ZMQ Communicator Example
Demonstrates all communication patterns
"""

import time
import threading
import zmq
from engine import ZmqInterface, ZmqSocketType, ZmqConfig


def example_req_rep():
    """REQ/REP pattern example"""
    print("\n=== REQ/REP Example ===")

#Server
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

#Keep server running
        time.sleep(5)

#Start server in background
    server = threading.Thread(target=server_thread, daemon=True)
    server.start()
    time.sleep(1)  # Wait for server to start

#Client
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

    # Publisher (binds)
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        socket_id = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(socket_id, "0.0.0.0:5556")
        pub.start_server(socket_id)
        
        # Wait for subscriber to connect
        time.sleep(1)
        
        # Publish messages
        pub.publish(socket_id, "sensor.temp", b"25.3C")
        pub.publish(socket_id, "sensor.humidity", b"60%")
        pub.publish(socket_id, "other.data", b"ignored")  # Not subscribed
        print("[PUB] Published messages")
        
        # Keep publisher running
        time.sleep(5)

    # Start publisher in background
    publisher = threading.Thread(target=publisher_thread, daemon=True)
    publisher.start()
    time.sleep(0.5)  # Wait for publisher to start

    # Subscriber (connects)
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.connect(socket_id, "127.0.0.1:5556")
    sub.subscribe(socket_id, "sensor.")
    
    def on_message(msg):
        print(f"[SUB] Topic: {msg['topic']}, Data: {msg['data']}")
    
    sub.set_subscribe_callback(socket_id, on_message)
    
    time.sleep(3)  # Wait for messages
    
    sub.close_socket(socket_id)
    print("PUB/SUB example completed")


def example_push_pull():
    """PUSH/PULL pattern example"""
    print("\n=== PUSH/PULL Example ===")

#Worker
    def worker_thread():
        pull = ZmqInterface()
        pull.initialize(ZmqConfig())
        socket_id = pull.create_socket(ZmqSocketType.PULL)
        pull.bind(socket_id, "0.0.0.0:5557")
        pull.start_server(socket_id)
        
        def process_task(msg):
            print(f"[PULL] Processing: {msg['data']}")
        
        pull.set_pull_callback(socket_id, process_task)

#Keep worker running
        time.sleep(5)

#Start worker in background
    worker = threading.Thread(target=worker_thread, daemon=True)
    worker.start()
    time.sleep(1)  # Wait for worker to start

#Producer
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

#Peer 1
    def peer1_thread():
        pair1 = ZmqInterface()
        pair1.initialize(ZmqConfig())
        socket_id = pair1.create_socket(ZmqSocketType.PAIR)
        pair1.bind(socket_id, "0.0.0.0:5558")
        pair1.start_server(socket_id)
        
        def on_message(msg):
            print(f"[PAIR1] Received: {msg['data']}")
        
        pair1.set_receive_callback(socket_id, on_message)
        
        time.sleep(2)
        pair1.send(socket_id, b"Hello from peer1")

#Keep peer running
        time.sleep(5)

#Start peer1 in background
    peer1 = threading.Thread(target=peer1_thread, daemon=True)
    peer1.start()
    time.sleep(1)  # Wait for peer1 to start

#Peer 2
    pair2 = ZmqInterface()
    pair2.initialize(ZmqConfig())
    socket_id = pair2.create_socket(ZmqSocketType.PAIR)
    pair2.connect(socket_id, "127.0.0.1:5558")
    
    def on_message(msg):
        print(f"[PAIR2] Received: {msg['data']}")
    
    pair2.set_receive_callback(socket_id, on_message)
    pair2.start_server(socket_id)  # PAIR also needs server for receiving
    
    pair2.send(socket_id, b"Hello from peer2")
    
    time.sleep(3)  # Wait for bidirectional messages
    
    pair2.close_socket(socket_id)
    print("PAIR example completed")


def example_pyobj():
    """Python object serialization example (send_pyobj/recv_pyobj)"""
    print("\n=== Python Object (Pyobj) Example ===")
    
    # Publisher
    def publisher_thread():
        pub = ZmqInterface()
        pub.initialize(ZmqConfig())
        socket_id = pub.create_socket(ZmqSocketType.PUB)
        pub.bind(socket_id, "0.0.0.0:5559")
        pub.start_server(socket_id)
        
        time.sleep(1)  # Wait for subscriber
        
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
    
    # Subscriber
    sub = ZmqInterface()
    sub.initialize(ZmqConfig())
    socket_id = sub.create_socket(ZmqSocketType.SUB)
    sub.connect(socket_id, "127.0.0.1:5559")
    sub.subscribe(socket_id, "")  # Subscribe to all topics
    
    def on_pyobj(msg):
        print(f"[SUB] Received Python object: {msg['obj']}, Topic: {msg['topic']}")
    
    sub.set_pyobj_receive_callback(socket_id, on_pyobj)
    
    time.sleep(3)
    sub.close_socket(socket_id)
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


def main():
    print("ZMQ Communicator Examples")
    print("=" * 50)
    
    try:
        example_req_rep()
        example_pub_sub()
        example_push_pull()
        example_pair()
        example_pyobj()       # New: Python object serialization
        example_multipart()   # New: Multipart messages
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
