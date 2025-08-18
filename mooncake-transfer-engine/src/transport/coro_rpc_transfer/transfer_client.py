#!/usr/bin/env python3
import asyncio
import torch
import numpy as np
import coro_rpc_transfer
import time

async def send_tensor_to_server():
    print("Starting tensor transfer client...")
    
    try:
        client = coro_rpc_transfer.py_coro_rpc_client_pool("127.0.0.1:8801")
        print(" Connected to server at 127.0.0.1:8801")
    except Exception as e:
        print(f" Failed to connect to server: {e}")
        return
    
    print("\n Creating test tensors...")
    
    test_tensors = [
        {
            "name": "Float32 Matrix",
            "tensor": torch.randn(3, 4, dtype=torch.float32),
            "description": "3x4 random float32 matrix"
        },
        {
            "name": "Int64 Vector", 
            "tensor": torch.arange(10, dtype=torch.int64),
            "description": "Vector [0, 1, 2, ..., 9]"
        },
        {
            "name": "Bool Tensor",
            "tensor": torch.tensor([True, False, True, False], dtype=torch.bool),
            "description": "Boolean tensor [T, F, T, F]"
        },
        {
            "name": "Large Float Tensor",
            "tensor": torch.ones(100, 50, dtype=torch.float32),
            "description": "100x50 ones tensor"
        }
    ]
    
    loop = asyncio.get_running_loop()
    
    for i, test_case in enumerate(test_tensors, 1):
        tensor = test_case["tensor"]
        print(f"\n Test {i}: {test_case['name']}")
        print(f"   Description: {test_case['description']}")
        print(f"   Shape: {tensor.shape}")
        print(f"   Dtype: {tensor.dtype}")
        print(f"   Size: {tensor.numel()} elements, {tensor.numel() * tensor.element_size()} bytes")
        
        try:
            print(f"   Sending tensor...")
            start_time = time.time()
            
            future = client.async_send_tensor(loop, tensor)
            result = await future
            
            end_time = time.time()
            transfer_time = (end_time - start_time) * 1000
            
            print(f"   Transfer time: {transfer_time:.2f}ms")
            
            if hasattr(result, 'code') and result.code == 0:
                print(f"   Success! Server response: {result.str_view()}")
            else:
                print(f"   Failed! Error code: {getattr(result, 'code', 'unknown')}")
                if hasattr(result, 'err_msg'):
                    print(f"       Error message: {result.err_msg}")
            
        except Exception as e:
            print(f"   Exception during transfer: {e}")
        
        await asyncio.sleep(0.5)
    
    print(f"\n All tensor transfer tests completed!")

async def send_simple_message():
    print("\n Testing simple message transfer...")
    
    try:
        client = coro_rpc_transfer.py_coro_rpc_client_pool("127.0.0.1:8801")
        loop = asyncio.get_running_loop()
        
        test_message = b"Hello from Python client!"
        print(f"   Sending: {test_message.decode()}")
        
        future = client.async_send_msg(loop, test_message)
        result = await future
        
        if hasattr(result, 'code') and result.code == 0:
            print(f"    Message sent successfully!")
            print(f"    Server response: {result.str_view()}")
        else:
            print(f"    Message failed! Error: {getattr(result, 'err_msg', 'unknown')}")
            
    except Exception as e:
        print(f"    Exception: {e}")

def create_custom_tensor():
    print("\n Creating custom tensor...")
    
    data = np.array([
        [1.0, 2.0, 3.0],
        [4.0, 5.0, 6.0],
        [7.0, 8.0, 9.0]
    ], dtype=np.float32)
    
    tensor = torch.from_numpy(data)
    print(f"Custom tensor:\n{tensor}")
    
    return tensor

async def main():
    print(" Mooncake Tensor Transfer Client")
    print("=" * 50)
    
    print(" Waiting for server to be ready...")
    await asyncio.sleep(2)
    
    await send_simple_message()
    
    await send_tensor_to_server()
    
    print("\n Testing custom tensor...")
    custom_tensor = create_custom_tensor()
    
    try:
        client = coro_rpc_transfer.py_coro_rpc_client_pool("127.0.0.1:8801") 
        loop = asyncio.get_running_loop()
        
        future = client.async_send_tensor(loop, custom_tensor)
        result = await future
        
        if hasattr(result, 'code') and result.code == 0:
            print(" Custom tensor sent successfully!")
        else:
            print(f" Custom tensor failed: {getattr(result, 'err_msg', 'unknown')}")
            
    except Exception as e:
        print(f" Exception with custom tensor: {e}")
    
    print("\n Client finished!")

if __name__ == "__main__":
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    
    asyncio.run(main())