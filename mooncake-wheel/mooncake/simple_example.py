#!/usr/bin/env python3
"""
Simple send/recv API usage example
"""

import torch
import time
from transfer_engine_exp import Sender, Receiver, TransferConfig, send, recv

# Configuration
SENDER_HOST = "192.168.0.143"  # Real IP for sender
RECEIVER_HOST = "192.168.0.144"  # Real IP for receiver
SENDER_ZMQ_HOST = "192.168.0.144"  # ZMQ communication host for sender (connects to receiver)
RECEIVER_ZMQ_HOST = "192.168.0.143"  # ZMQ communication host for receiver (connects to sender)

def example_sender():
    """Sender example"""
    print("=== Sender Example ===")
    
    # Create test data
    data = {
        "message": "Hello from sender!",
        "tensor1": torch.randn(3, 4, dtype=torch.float32),
        "tensor2": torch.ones(2, 2, dtype=torch.int64),
        "description": "Test data with tensors"
    }
    
    print("Sending data:")
    for key, value in data.items():
        print(f"  {key}: {type(value)} - {value}")
        if isinstance(value, torch.Tensor):
            print(f"    Shape: {value.shape}")
    
    # # Method 1: Use convenience function
    #print("\nUsing convenience function...")
    #success = send(data, SENDER_HOST, f"{RECEIVER_HOST}:12345")
    #print(f"Send result: {success}")
    
    # Method 2: Use class with custom ZMQ host
    print("\nUsing Sender class with custom ZMQ host...")
    config = TransferConfig(SENDER_HOST, zmq_host=SENDER_ZMQ_HOST)
    sender = Sender(config)
    success = sender.send_dict(data, f"{RECEIVER_HOST}:12345")
    print(f"Send result: {success}")
    
    # Method 3: Use new API for single key-value pairs
    print("\nUsing new API for single key-value pairs...")
    success = sender.send("tensor", torch.randn(2, 3), f"{RECEIVER_HOST}:12345")
    print(f"Send result: {success}")

def example_receiver():
    """Receiver example"""
    print("=== Receiver Example ===")
    
    # Method 1: Use convenience function
    # print("Using convenience function...")
    # print("Waiting for data...")
    # data = recv(RECEIVER_HOST)
    
    # if data:
    #     print("Received data:")
    #     for key, value in data.items():
    #         print(f"  {key}: {type(value)} - {value}")
    #         if isinstance(value, torch.Tensor):
    #             print(f"    Shape: {value.shape}")
    # else:
    #     print("No data received")
    
    # Method 2: Use class with custom ZMQ host
    print("\nUsing Receiver class with custom ZMQ host...")
    config = TransferConfig(RECEIVER_HOST, zmq_host=RECEIVER_ZMQ_HOST)
    receiver = Receiver(config)
    print("Waiting for data...")
    data = receiver.recv_dict()
    
    if data:
        print("Received data:")
        for key, value in data.items():
            print(f"  {key}: {type(value)} - {value}")
            if isinstance(value, torch.Tensor):
                print(f"    Shape: {value.shape}")
    
    # Method 3: Use new API for single key-value pairs
    print("\nUsing new API for single key-value pairs...")
    print("Waiting for key-value pairs...")
    try:
        key, value = receiver.recv()
        print(f"Received {key}: {type(value)} - {value}")
        if isinstance(value, torch.Tensor):
            print(f"  Shape: {value.shape}")
    except Exception as e:
        print(f"Error receiving data: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "sender":
            example_sender()
        elif mode == "receiver":
            example_receiver()
        else:
            print("Usage: python simple_example.py [sender|receiver|simple]")
    else:
        print("Simple Transfer API Example")
        print("=" * 40)
        print("Configuration:")
        print(f"  Sender Host: {SENDER_HOST}")
        print(f"  Receiver Host: {RECEIVER_HOST}")
        print(f"  Sender ZMQ Host: {SENDER_ZMQ_HOST}")
        print(f"  Receiver ZMQ Host: {RECEIVER_ZMQ_HOST}")
        print("\nRun with:")
        print("  python simple_example.py sender    # Sender example")
        print("  python simple_example.py receiver  # Receiver example")
