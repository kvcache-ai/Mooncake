#!/usr/bin/env python3
"""
Test enhanced CoroRPC tensor rebuilding functionality
"""

import torch
import numpy as np
import asyncio
import threading
import time
import sys

try:
    import mooncake.engine as engine
    print("Successfully imported mooncake.engine")
    CoroRPCInterface = engine.coro_rpc_interface.CoroRPCInterface
    print("Successfully imported CoroRPCInterface")
except ImportError as e:
    print(f"Failed to import mooncake: {e}")
    sys.exit(1)
except AttributeError as e:
    print(f"Failed to import CoroRPCInterface: {e}")
    sys.exit(1)


def test_enhanced_tensor_rebuilding():
    print("\n=== Testing Enhanced Tensor Rebuilding ===")

    # Create server and client instances
    server = CoroRPCInterface()
    client = CoroRPCInterface()

    # Store received tensors
    received_tensors = []

    def tensor_receive_callback(received_tensor):
        print(f"Received tensor from: {received_tensor.source_address}")
        print(f"Original data size: {len(received_tensor.data)} bytes")
        print(f"Shape info: {received_tensor.shape}")
        print(f"Dtype info: {received_tensor.dtype}")

        try:
            # Use enhanced rebuild functionality to reconstruct tensor
            rebuilt_tensor = received_tensor.rebuild_tensor()
            received_tensors.append(rebuilt_tensor)

            print("Successfully rebuilt tensor:")
            print(f"  - Shape: {rebuilt_tensor.shape}")
            print(f"  - Dtype: {rebuilt_tensor.dtype}")
            print(f"  - Device: {rebuilt_tensor.device}")
            print(f"  - Data sample: {rebuilt_tensor.flatten()[:5]}")

        except Exception as e:
            print(f"Failed to rebuild tensor: {e}")
            import traceback
            traceback.print_exc()

    try:
        # Initialize server and client
        server_addr = "127.0.0.1:8888"
        if not server.initialize(server_addr, 1, 30, 4):
            print("Server initialization failed")
            return False

        if not client.initialize("", 0, 30, 4):
            print("Client initialization failed")
            return False

        # Set tensor receive callback
        server.set_tensor_receive_callback(tensor_receive_callback)

        # Start server asynchronously
        if not server.start_server_async():
            print("Failed to start server")
            return False

        print(f"Server started on {server_addr}")
        time.sleep(1)  # Wait for server to start

        # Connect client to server
        if not client.add_remote_connection(server_addr):
            print("Failed to connect to server")
            return False

        print("Client connected to server")
        time.sleep(0.5)  # Wait for connection establishment

        # Define test cases with various tensor types
        test_cases = [
            ("Float32 2D", torch.randn(3, 4, dtype=torch.float32)),
            ("Int64 1D", torch.arange(10, dtype=torch.int64)),
            ("Float64 3D", torch.ones(2, 3, 4, dtype=torch.float64)),
            ("Int32 Vector", torch.tensor([1, 2, 3, 4, 5], dtype=torch.int32)),
            ("Bool Matrix", torch.tensor([[True, False], [False, True]], dtype=torch.bool)),
        ]

        for test_name, original_tensor in test_cases:
            print(f"\n--- Testing {test_name} ---")
            print("Original tensor:")
            print(f"  - Shape: {original_tensor.shape}")
            print(f"  - Dtype: {original_tensor.dtype}")
            print(f"  - Data sample: {original_tensor.flatten()[:5]}")

            # Send tensor
            result = client.send_tensor(server_addr, original_tensor)
            print(f"Send result: {result}")

            if result < 0:
                print(f"Failed to send {test_name}")
                continue

            # Wait for reception and processing
            time.sleep(1)

            if len(received_tensors) == 0:
                print(f"No tensor received for {test_name}")
                continue

            # Validate the rebuilt tensor
            rebuilt_tensor = received_tensors[-1]

            # Check shape
            if tuple(rebuilt_tensor.shape) != tuple(original_tensor.shape):
                print(f"Shape mismatch: {rebuilt_tensor.shape} vs {original_tensor.shape}")
                continue

            # Check data type
            if rebuilt_tensor.dtype != original_tensor.dtype:
                print(f"Dtype mismatch: {rebuilt_tensor.dtype} vs {original_tensor.dtype}")
                continue

            # Check data content (move to CPU for comparison)
            try:
                if torch.allclose(rebuilt_tensor.cpu(), original_tensor.cpu(), atol=1e-6):
                    print(f"{test_name} passed - data integrity verified")
                else:
                    print(f"{test_name} failed - data content mismatch")
                    print(f"  Original: {original_tensor.flatten()[:5]}")
                    print(f"  Rebuilt:  {rebuilt_tensor.flatten()[:5]}")
            except Exception as e:
                print(f"{test_name} failed - comparison error: {e}")

        print(f"\nEnhanced tensor rebuilding test completed")
        print(f"Total tensors processed: {len(received_tensors)}")

        return len(received_tensors) == len(test_cases)

    except Exception as e:
        print(f"Test failed with exception: {e}")
        import traceback
        traceback.pri