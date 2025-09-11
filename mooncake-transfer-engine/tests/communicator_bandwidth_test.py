import torch
import numpy as np
import time
import sys
import threading
import struct
import argparse
from typing import List, Tuple, Dict, Any

import mooncake.engine as engine

class AtomicCounter:
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

    def get(self):
        with self._lock:
            r = self._value
            self._value = 0
            return r

counter = AtomicCounter()

# Global variable to store data size
data_size = 1024 * 1024  # Default 1MB
test_data = None

def print_qps():
    while True:
        time.sleep(1)
        val = counter.get()
        if val == 0:   
            continue
        print("bandwidth:", 8 * val * data_size / (1000 * 1000 * 1000), "Gb/s")

def send_data(client, target_url):
    while True:
        result = client.send_data(target_url, test_data)
        counter.inc()

def run_server(bind_url, data_size_mb=1):
    """Run server mode"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"Starting server on {bind_url} with {data_size_mb}MB data packets")
    
    CoroRPCInterface = engine.coro_rpc_interface.CoroRPCInterface
    server = CoroRPCInterface()
    server.initialize(bind_url, 8, 30, 4)
    server.start_server()
    
    # Start QPS statistics thread
    thread = threading.Thread(target=print_qps)
    thread.daemon = True
    thread.start()
    
    print(f"Server started on {bind_url}, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer stopping...")

def run_client(target_url, num_threads=8, data_size_mb=1):
    """Run client mode"""
    global data_size, test_data
    data_size = data_size_mb * 1024 * 1024
    test_data = b'\x00' * data_size
    
    print(f"Starting client, connecting to {target_url} with {num_threads} threads, {data_size_mb}MB data packets")
    
    CoroRPCInterface = engine.coro_rpc_interface.CoroRPCInterface
    client = CoroRPCInterface()
    client.initialize("", 0, 30, 100)
    
    # Start QPS statistics thread
    qps_thread = threading.Thread(target=print_qps)
    qps_thread.daemon = True
    qps_thread.start()
    
    # Start multiple sending threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=send_data, args=(client, target_url))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    print(f"Client started with {num_threads} threads, press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nClient stopping...")

def main():
    parser = argparse.ArgumentParser(description='Mooncake Communication Bandwidth Test Tool')
    parser.add_argument('mode', choices=['server', 'client'], 
                       help='Run mode: server or client')
    parser.add_argument('--url', default='127.0.0.1:9004',
                       help='URL address (default: 127.0.0.1:9004)')
    parser.add_argument('--threads', type=int, default=8,
                       help='Number of client threads (default: 8)')
    parser.add_argument('--data-size', type=int, default=1,
                       help='Data packet size in MB (default: 1)')

    args = parser.parse_args()
    
    if args.mode == 'server':
        # Server mode, URL as bind address
        bind_url = f"0.0.0.0:{args.url.split(':')[-1]}" if ':' in args.url else f"0.0.0.0:{args.url}"
        run_server(bind_url, args.data_size)
    else:
        # Client mode, URL as target address
        run_client(args.url, args.threads, args.data_size)

if __name__ == "__main__":
    main()