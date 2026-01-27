import os
os.environ["MC_USE_IPV6"] = "1"
os.environ["MC_GID_INDEX"] = "3"

import torch
from mooncake.engine import TransferEngine
import mooncake_async
import sys


def run_demo():
    print("Initializing Mooncake Engine...")
    engine_py = TransferEngine()

    local_hostname = "fd03:4515:1100:a90::1"
    ret = engine_py.initialize(
        local_hostname, "P2PHANDSHAKE", "rdma", "mlx5_bond_0")
    if ret != 0:
        print(f"Failed to initialize engine. Return code: {ret}")
        return
    rpc_port = engine_py.get_rpc_port()
    print(f"Engine listening on port: {rpc_port}")

    # Get underlying pointer
    engine_ptr = engine_py.get_engine_ptr()
    print(f"Engine Ptr: {hex(engine_ptr)}")

    # Prepare Tensors on GPU
    device = torch.device("cuda:0")
    N = 1024 * 1024 * 64  # 64MB

    print(f"Allocating tensors ({N/1024/1024} MB)...")
    src_tensor = torch.ones(N, dtype=torch.uint8, device=device)
    dst_tensor = torch.zeros(N, dtype=torch.uint8, device=device)

    # Register Memory for RDMA
    print("Registering memory...")
    ret = engine_py.register_memory(src_tensor.data_ptr(), src_tensor.nbytes)
    if ret != 0:
        print("Failed to register src memory")
        return

    ret = engine_py.register_memory(dst_tensor.data_ptr(), dst_tensor.nbytes)
    if ret != 0:
        print("Failed to register dst memory")
        return

    copy_stream = torch.cuda.Stream(device)
    print("Starting Async Transfer Demo...")

    target_name = local_hostname
    if ":" in local_hostname:
        target_name = f"[{local_hostname}]:{rpc_port}"
    else:
        target_name = f"{local_hostname}:{rpc_port}"
    print(target_name)
    
     # 为了后面的并行
    a = torch.randn(4096, 4096, device=device)
    b = torch.randn(4096, 4096, device=device)

    print("Submitting async_write to Copy Stream...")
    for i in range(10):
        print(f"do sync iter {i}", flush=True)
        src_tensor.fill_(i)
        dst_tensor.zero_()
        event = torch.cuda.default_stream().record_event()
        copy_stream.wait_event(event)
        with torch.cuda.stream(copy_stream):
            mooncake_async.async_write(
                engine_ptr,
                target_name,
                src_tensor.data_ptr(),
                dst_tensor.data_ptr(),  # In real RDMA, this is the offset/address in the remote segment
                src_tensor.nbytes
            )
        event = copy_stream.record_event()
        # 观察有没有并行
        c = torch.matmul(a, b)
        torch.cuda.default_stream().wait_event(event)
        assert torch.all(dst_tensor == i), f"dst_tensor error! Expected {i}, got {dst_tensor}"
    torch.cuda.synchronize()
    
    print("Starting Batched Async Write Demo...")
    # Split the tensor into 4 chunks
    chunk_size = N // 4
    src_ptrs = [src_tensor.data_ptr() + i * chunk_size for i in range(4)]
    dst_ptrs = [dst_tensor.data_ptr() + i * chunk_size for i in range(4)]
    lengths = [chunk_size] * 4

    for i in range(10):
        print(f"do batched write iter {i}", flush=True)
        src_tensor.fill_(i + 100) # Use different values
        dst_tensor.zero_()
        
        event = torch.cuda.default_stream().record_event()
        copy_stream.wait_event(event)
        
        with torch.cuda.stream(copy_stream):
            mooncake_async.batched_async_write(
                engine_ptr,
                target_name,
                src_ptrs,
                dst_ptrs,
                lengths
            )
        
        event = copy_stream.record_event()
        c = torch.matmul(a, b)
        torch.cuda.default_stream().wait_event(event)
        
        assert torch.all(dst_tensor == (i + 100)), f"batched dst_tensor error! Expected {i+100}"

    torch.cuda.synchronize()

    # --- New Read Tests ---
    print("Starting Async Read Demo...")
    for i in range(10):
        print(f"do async read iter {i}", flush=True)
        # Prepare data on "remote" (dst_tensor) to be read into "local" (src_tensor)
        dst_tensor.fill_(i + 50) 
        src_tensor.zero_()
        
        event = torch.cuda.default_stream().record_event()
        copy_stream.wait_event(event)
        
        # Read from dst_tensor (remote) to src_tensor (local)
        with torch.cuda.stream(copy_stream):
            mooncake_async.async_read(
                engine_ptr,
                target_name,
                src_tensor.data_ptr(), # local
                dst_tensor.data_ptr(), # remote
                src_tensor.nbytes
            )
        
        event = copy_stream.record_event()
        c = torch.matmul(a, b)
        torch.cuda.default_stream().wait_event(event)
        
        assert torch.all(src_tensor == (i + 50)), f"read src_tensor error! Expected {i+50}"
    torch.cuda.synchronize()

    print("Starting Batched Async Read Demo...")
    for i in range(10):
        print(f"do batched read iter {i}", flush=True)
        dst_tensor.fill_(i + 200)
        src_tensor.zero_()
        
        event = torch.cuda.default_stream().record_event()
        copy_stream.wait_event(event)
        
        with torch.cuda.stream(copy_stream):
            mooncake_async.batched_async_read(
                engine_ptr,
                target_name,
                src_ptrs, # local chunks
                dst_ptrs, # remote chunks
                lengths
            )
        
        event = copy_stream.record_event()
        c = torch.matmul(a, b)
        torch.cuda.default_stream().wait_event(event)
        
        assert torch.all(src_tensor == (i + 200)), f"batched read src_tensor error! Expected {i+200}"

    torch.cuda.synchronize()
    print("All operations completed.")

    # Cleanup
    engine_py.unregister_memory(src_tensor.data_ptr())
    engine_py.unregister_memory(dst_tensor.data_ptr())


if __name__ == "__main__":
    run_demo()
