#!/usr/bin/env python3
"""
ZMQ Communicator Bandwidth Test Tool

Tests bandwidth for different ZMQ communication patterns:
- REQ/REP: Request-Response
- PUB/SUB: Publish-Subscribe
- PUSH/PULL: Pipeline
- PAIR: Exclusive Pair

Usage:
    # Test REQ/REP pattern
    python zmq_communicator_bandwidth_test.py req-rep server --url 0.0.0.0:9005
    python zmq_communicator_bandwidth_test.py req-rep client --url 127.0.0.1:9005 --threads 4

    # Test PUB/SUB pattern
    python zmq_communicator_bandwidth_test.py pub-sub publisher --url 0.0.0.0:9006
    python zmq_communicator_bandwidth_test.py pub-sub subscriber --url 127.0.0.1:9006 --threads 4

    # Test PUSH/PULL pattern
    python zmq_communicator_bandwidth_test.py push-pull puller --url 0.0.0.0:9007
    python zmq_communicator_bandwidth_test.py push-pull pusher --url 127.0.0.1:9007 --threads 4

    # Test PAIR pattern
    python zmq_communicator_bandwidth_test.py pair server --url 0.0.0.0:9008
    python zmq_communicator_bandwidth_test.py pair client --url 127.0.0.1:9008
"""

import time
import threading
import argparse
import sys

try:
    # Try to import ZMQ interface from mooncake
    from mooncake.engine import ZmqInterface, ZmqSocketType, ZmqConfig
    ZMQ_AVAILABLE = True
except ImportError as e:
    print(f"Error: ZMQ interface not available: {e}")
    print("Please ensure mooncake is built with ZMQ support and installed.")
    sys.exit(1)


class AtomicCounter:
    """Thread-safe counter for bandwidth statistics"""
    def __init__(self, initial=0):
        self._value = initial
        self._lock = threading.Lock()

    def inc(self, num=1):
        with self._lock:
            self._value += num
            return self._value

    def dec(self, num=1):
        with self._lock:
            self._value -= num
            return self._value

    def get_and_reset(self):
        with self._lock:
            r = self._value
            self._value = 0
            return r


# Global statistics
counter = AtomicCounter()
data_size = 1024 * 1024  # Default 1MB
test_data = None


def print_bandwidth():
    """Print bandwidth statistics every second"""
    while True:
        time.sleep(1)
        val = counter.get_and_reset()
        if val == 0:
            continue
        bandwidth_gbps = 8 * val * data_size / (1024*1024*1024)
        print(f"Bandwidth: {bandwidth_gbps:.2f} Gb/s ({val * data_size / (1024*1024):.0f} MB/s)")


# ============================================================================
# REQ/REP Pattern Tests
# ============================================================================

def run_req_rep_server(bind_url, data_size_mb=1):
    """Run REQ/REP server (REP socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[REQ/REP] Starting REP server on {bind_url}, data size: {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create REP socket
    socket_id = zmq.create_socket(ZmqSocketType.REP)
    zmq.bind(socket_id, bind_url)
    zmq.start_server(socket_id)
    
    # Set receive callback
    def handle_request(data):
        counter.inc()
        # Send reply
        zmq.reply(socket_id, test_data)
    
    zmq.set_receive_callback(socket_id, handle_request)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    print(f"REP server started, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer stopping...")
        zmq.shutdown()


def run_req_rep_client(target_url, num_threads=8, data_size_mb=1):
    """Run REQ/REP client (REQ socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[REQ/REP] Starting REQ client, connecting to {target_url}, {num_threads} threads, {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create REQ socket
    socket_id = zmq.create_socket(ZmqSocketType.REQ)
    zmq.connect(socket_id, target_url)
    
    # Send function for worker threads
    def send_requests():
        while True:
            try:
                response = zmq.request(socket_id, test_data)
                if response:
                    counter.inc()
                else:
                    time.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(0.1)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    # Start worker threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=send_requests, daemon=True)
        t.start()
        threads.append(t)
    
    print(f"REQ client started with {num_threads} threads, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nClient stopping...")
        zmq.shutdown()


# ============================================================================
# PUB/SUB Pattern Tests
# ============================================================================

def run_pub_sub_publisher(bind_url, data_size_mb=1, num_threads=8):
    """Run PUB/SUB publisher (PUB socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PUB/SUB] Starting PUB publisher on {bind_url}, data size: {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create PUB socket
    socket_id = zmq.create_socket(ZmqSocketType.PUB)
    zmq.bind(socket_id, bind_url)
    
    # Publishing function
    def publish_loop():
        while True:
            try:
                result = zmq.publish(socket_id, "data", test_data)
                if result >= 0:
                    counter.inc()
                else:
                    time.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(0.1)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    # Start publisher threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=publish_loop, daemon=True)
        t.start()
        threads.append(t)
    
    print(f"PUB publisher started with {num_threads} threads, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nPublisher stopping...")
        zmq.shutdown()


def run_pub_sub_subscriber(target_url, data_size_mb=1):
    """Run PUB/SUB subscriber (SUB socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PUB/SUB] Starting SUB subscriber, connecting to {target_url}, {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create SUB socket
    socket_id = zmq.create_socket(ZmqSocketType.SUB)
    zmq.connect(socket_id, target_url)
    zmq.subscribe(socket_id, "data")  # Subscribe to "data" topic
    
    # Set receive callback
    def handle_message(data):
        counter.inc()
    
    zmq.set_subscribe_callback(socket_id, handle_message)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    print(f"SUB subscriber started, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSubscriber stopping...")
        zmq.shutdown()


# ============================================================================
# PUSH/PULL Pattern Tests
# ============================================================================

def run_push_pull_puller(bind_url, data_size_mb=1):
    """Run PUSH/PULL puller (PULL socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PUSH/PULL] Starting PULL worker on {bind_url}, data size: {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create PULL socket
    socket_id = zmq.create_socket(ZmqSocketType.PULL)
    zmq.bind(socket_id, bind_url)
    zmq.start_server(socket_id)
    
    # Set receive callback
    def handle_task(data):
        counter.inc()
    
    zmq.set_pull_callback(socket_id, handle_task)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    print(f"PULL worker started, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nWorker stopping...")
        zmq.shutdown()


def run_push_pull_pusher(target_url, num_threads=8, data_size_mb=1):
    """Run PUSH/PULL pusher (PUSH socket)"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PUSH/PULL] Starting PUSH sender, connecting to {target_url}, {num_threads} threads, {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create PUSH socket
    socket_id = zmq.create_socket(ZmqSocketType.PUSH)
    zmq.connect(socket_id, target_url)
    
    # Push function for worker threads
    def push_tasks():
        while True:
            try:
                result = zmq.push(socket_id, test_data)
                if result >= 0:
                    counter.inc()
                else:
                    time.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(0.1)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    # Start worker threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=push_tasks, daemon=True)
        t.start()
        threads.append(t)
    
    print(f"PUSH sender started with {num_threads} threads, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSender stopping...")
        zmq.shutdown()


# ============================================================================
# PAIR Pattern Tests
# ============================================================================

def run_pair_server(bind_url, data_size_mb=1):
    """Run PAIR server"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PAIR] Starting PAIR server on {bind_url}, data size: {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create PAIR socket
    socket_id = zmq.create_socket(ZmqSocketType.PAIR)
    zmq.bind(socket_id, bind_url)
    zmq.start_server(socket_id)
    
    # Set receive callback
    def handle_message(data):
        counter.inc()
        # Echo back
        zmq.send(socket_id, test_data)
    
    zmq.set_receive_callback(socket_id, handle_message)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    print(f"PAIR server started, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer stopping...")
        zmq.shutdown()


def run_pair_client(target_url, data_size_mb=1):
    """Run PAIR client"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"[PAIR] Starting PAIR client, connecting to {target_url}, {data_size_mb}MB")
    
    config = ZmqConfig()
    config.pool_size = 100
    
    zmq = ZmqInterface()
    zmq.initialize(config)
    
    # Create PAIR socket
    socket_id = zmq.create_socket(ZmqSocketType.PAIR)
    zmq.connect(socket_id, target_url)
    
    # Send function
    def send_loop():
        while True:
            try:
                result = zmq.send(socket_id, test_data)
                if result >= 0:
                    counter.inc()
                else:
                    time.sleep(0.01)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(0.1)
    
    # Start bandwidth statistics
    stats_thread = threading.Thread(target=print_bandwidth, daemon=True)
    stats_thread.start()
    
    # Start send thread
    send_thread = threading.Thread(target=send_loop, daemon=True)
    send_thread.start()
    
    print(f"PAIR client started, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nClient stopping...")
        zmq.shutdown()


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='ZMQ Communicator Bandwidth Test Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='pattern', help='Communication pattern')
    
    # REQ/REP pattern
    req_rep = subparsers.add_parser('req-rep', help='Request-Response pattern')
    req_rep.add_argument('mode', choices=['server', 'client'], help='Run mode')
    req_rep.add_argument('--url', default='0.0.0.0:9005', help='URL address')
    req_rep.add_argument('--threads', type=int, default=8, help='Number of client threads')
    req_rep.add_argument('--data-size', type=int, default=1, help='Data size in MB')
    
    # PUB/SUB pattern
    pub_sub = subparsers.add_parser('pub-sub', help='Publish-Subscribe pattern')
    pub_sub.add_argument('mode', choices=['publisher', 'subscriber'], help='Run mode')
    pub_sub.add_argument('--url', default='0.0.0.0:9006', help='URL address')
    pub_sub.add_argument('--threads', type=int, default=8, help='Number of publisher threads')
    pub_sub.add_argument('--data-size', type=int, default=1, help='Data size in MB')
    
    # PUSH/PULL pattern
    push_pull = subparsers.add_parser('push-pull', help='Pipeline pattern')
    push_pull.add_argument('mode', choices=['pusher', 'puller'], help='Run mode')
    push_pull.add_argument('--url', default='0.0.0.0:9007', help='URL address')
    push_pull.add_argument('--threads', type=int, default=8, help='Number of pusher threads')
    push_pull.add_argument('--data-size', type=int, default=1, help='Data size in MB')
    
    # PAIR pattern
    pair = subparsers.add_parser('pair', help='Exclusive pair pattern')
    pair.add_argument('mode', choices=['server', 'client'], help='Run mode')
    pair.add_argument('--url', default='0.0.0.0:9008', help='URL address')
    pair.add_argument('--data-size', type=int, default=1, help='Data size in MB')
    
    args = parser.parse_args()
    
    if not args.pattern:
        parser.print_help()
        sys.exit(1)
    
    # Route to appropriate function
    if args.pattern == 'req-rep':
        if args.mode == 'server':
            run_req_rep_server(args.url, args.data_size)
        else:
            run_req_rep_client(args.url, args.threads, args.data_size)
    
    elif args.pattern == 'pub-sub':
        if args.mode == 'publisher':
            run_pub_sub_publisher(args.url, args.data_size, args.threads)
        else:
            run_pub_sub_subscriber(args.url, args.data_size)
    
    elif args.pattern == 'push-pull':
        if args.mode == 'puller':
            run_push_pull_puller(args.url, args.data_size)
        else:
            run_push_pull_pusher(args.url, args.threads, args.data_size)
    
    elif args.pattern == 'pair':
        if args.mode == 'server':
            run_pair_server(args.url, args.data_size)
        else:
            run_pair_client(args.url, args.data_size)


if __name__ == "__main__":
    main()
