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

    # Store received tensors and callback status
    received_tensors = []
    callback_info = {
        'called_count': 0,
        'success_count': 0,
        'error_count': 0,
        'errors': []
    }

    def tensor_receive_callback(received_tensor):
        callback_info['called_count'] += 1
        
        print(f"\n=== CALLBACK #{callback_info['called_count']} TRIGGERED ===")
        print(f"Received tensor from: {received_tensor.source_address}")
        
        # Use safe method to get data size
        data_size = received_tensor.get_data_size()
        print(f"Data size: {data_size} bytes")
            
        print(f"Shape info: {received_tensor.shape}")
        print(f"Dtype info: {received_tensor.dtype}")
        
        # Check if total_bytes is available
        if hasattr(received_tensor, 'total_bytes'):
            print(f"Total bytes (from metadata): {received_tensor.total_bytes}")

        try:
            # Use enhanced rebuild functionality to reconstruct tensor
            print("Attempting to rebuild tensor...")
            print(f"Tensor metadata - Shape: {received_tensor.shape}, Dtype: {received_tensor.dtype}")
            
            # Now try the actual rebuild
            rebuilt_tensor = received_tensor.rebuild_tensor()
            
            received_tensors.append(rebuilt_tensor)
            callback_info['success_count'] += 1

            print("✅ Successfully rebuilt tensor:")
            print(f"  - Shape: {rebuilt_tensor.shape}")
            print(f"  - Dtype: {rebuilt_tensor.dtype}")
            print(f"  - Device: {rebuilt_tensor.device}")
            print(f"  - Data sample: {rebuilt_tensor.flatten()[:5]}")

        except Exception as e:
            callback_info['error_count'] += 1
            callback_info['errors'].append(str(e))
            print(f"❌ Failed to rebuild tensor: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"=== CALLBACK #{callback_info['called_count']} COMPLETED ===\n")

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

            # Check if callback was triggered for this tensor
            expected_callbacks = test_cases.index((test_name, original_tensor)) + 1
            if callback_info['called_count'] < expected_callbacks:
                print(f"❌ No callback received for {test_name}")
                continue

            if len(received_tensors) == 0:
                print(f"❌ No tensor received for {test_name}")
                continue

            # Validate the rebuilt tensor
            rebuilt_tensor = received_tensors[-1]

            # Check shape
            if tuple(rebuilt_tensor.shape) != tuple(original_tensor.shape):
                print(f"❌ Shape mismatch: {rebuilt_tensor.shape} vs {original_tensor.shape}")
                continue

            # Check data type
            if rebuilt_tensor.dtype != original_tensor.dtype:
                print(f"❌ Dtype mismatch: {rebuilt_tensor.dtype} vs {original_tensor.dtype}")
                continue

            # Check data content (move to CPU for comparison)
            try:
                if torch.allclose(rebuilt_tensor.cpu(), original_tensor.cpu(), atol=1e-6):
                    print(f"✅ {test_name} passed - data integrity verified")
                else:
                    print(f"❌ {test_name} failed - data content mismatch")
                    print(f"  Original: {original_tensor.flatten()[:5]}")
                    print(f"  Rebuilt:  {rebuilt_tensor.flatten()[:5]}")
            except Exception as e:
                print(f"❌ {test_name} failed - comparison error: {e}")

        # Print summary
        print(f"\n=== TEST SUMMARY ===")
        print(f"Total callbacks received: {callback_info['called_count']}")
        print(f"Successful rebuilds: {callback_info['success_count']}")
        print(f"Failed rebuilds: {callback_info['error_count']}")
        print(f"Total tensors processed: {len(received_tensors)}")
        
        if callback_info['errors']:
            print(f"Errors encountered:")
            for i, error in enumerate(callback_info['errors'], 1):
                print(f"  {i}. {error}")

        success = (callback_info['called_count'] == len(test_cases) and 
                  callback_info['success_count'] == len(test_cases) and
                  len(received_tensors) == len(test_cases))
        
        print(f"Enhanced tensor rebuilding test {'✅ PASSED' if success else '❌ FAILED'}")
        return success

    except Exception as e:
        print(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            server.stop_server()
        except:
            pass

if __name__ == "__main__":
    success = test_enhanced_tensor_rebuilding()
    print(f"\nFinal result: {'✅ SUCCESS' if success else '❌ FAILURE'}")
    sys.exit(0 if success else 1)