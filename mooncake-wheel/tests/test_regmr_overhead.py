#!/usr/bin/env python3
"""
Test script for directly measuring memset and reg_mr overhead

This script is designed to test:
1. memset overhead during Buffer initialization
2. reg_mr (memory registration) overhead
3. Performance difference between async vs sync memset

Usage:
    # Run as a module
    python -m mooncake-wheel.tests.test_memset_regmr_overhead
    
    # Or run directly
    cd mooncake-wheel/tests
    python test_memset_regmr_overhead.py
"""

import os
import sys
import torch
import torch.distributed as dist
import time
import numpy as np
import unittest
import ctypes

# Add project root to path for importing mooncake module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from mooncake.mooncake_ep_buffer import Buffer
from mooncake.engine import TransferEngine

# Note: PyTorch doesn't have torch.cuda.memset, so we use tensor.fill_(0)
# which internally calls cudaMemset. For async, we use fill_ with streams.


def test_cuda_memset_overhead():
    """Test CUDA memset overhead (sync vs async)"""
    print("\n" + "=" * 60)
    print("Testing CUDA memset overhead (sync vs async)")
    print("=" * 60)
    
    if not torch.cuda.is_available():
        print("Skipped: CUDA not available")
        return
    
    sizes = [1 * 1024 * 1024,      # 1 MB
             10 * 1024 * 1024,     # 10 MB
             100 * 1024 * 1024,    # 100 MB
             1024 * 1024 * 1024]   # 1 GB
    
    for size in sizes:
        print(f"\nTest size: {size / 1e6:.2f} MB")
        
        # Allocate memory
        gpu_mem = torch.empty(size // 4, dtype=torch.float32, device='cuda')
        stream = torch.cuda.Stream()
        
        # Test sync memset (using tensor.fill_ which internally uses memset)
        sync_times = []
        for _ in range(10):
            torch.cuda.synchronize()
            start = time.perf_counter()
            
            # Use tensor.fill_ which internally calls cudaMemset
            gpu_mem.fill_(0)
            torch.cuda.synchronize()
            
            end = time.perf_counter()
            sync_times.append((end - start) * 1e6)
        
        # Test async memset (using tensor.fill_ with stream)
        async_times = []
        for _ in range(10):
            torch.cuda.synchronize()
            start = time.perf_counter()
            
            with torch.cuda.stream(stream):
                # Use tensor.fill_ which can work with streams
                gpu_mem.fill_(0)
            stream.synchronize()
            
            end = time.perf_counter()
            async_times.append((end - start) * 1e6)
        
        sync_avg = np.mean(sync_times)
        async_avg = np.mean(async_times)
        
        print(f"  Sync memset: {sync_avg:.2f} us (avg)")
        print(f"  Async memset: {async_avg:.2f} us (avg)")
        if sync_avg > 0:
            print(f"  Improvement: {((sync_avg - async_avg) / sync_avg * 100):.1f}%")
        
        del gpu_mem
        torch.cuda.empty_cache()


def test_ctrl_buf_init_overhead():
    """Test ctrl_buf initialization overhead"""
    print("\n" + "=" * 60)
    print("Testing ctrl_buf initialization overhead (simulated)")
    print("=" * 60)
    
    if not torch.cuda.is_available():
        print("Skipped: CUDA not available")
        return
    
    CTRL_BUF_SIZE = 1024 * 1024 * 1024  # 1 GB
    
    print(f"ctrl_buf size: {CTRL_BUF_SIZE / 1e6:.2f} MB")
    
    # Test 1: Allocation only, no memset
    times_alloc_only = []
    for _ in range(5):
        torch.cuda.synchronize()
        start = time.perf_counter()
        
        mem = torch.empty(CTRL_BUF_SIZE // 4, dtype=torch.float32, device='cuda')
        torch.cuda.synchronize()
        
        end = time.perf_counter()
        times_alloc_only.append((end - start) * 1e6)
        del mem
        torch.cuda.empty_cache()
    
    # Test 2: Allocation + sync memset
    times_alloc_sync_memset = []
    for _ in range(5):
        torch.cuda.synchronize()
        start = time.perf_counter()
        
        mem = torch.empty(CTRL_BUF_SIZE // 4, dtype=torch.float32, device='cuda')
        # Use tensor.fill_ which internally calls cudaMemset
        mem.fill_(0)
        torch.cuda.synchronize()
        
        end = time.perf_counter()
        times_alloc_sync_memset.append((end - start) * 1e6)
        del mem
        torch.cuda.empty_cache()
    
    # Test 3: Allocation + async memset
    times_alloc_async_memset = []
    stream = torch.cuda.Stream()
    for _ in range(5):
        torch.cuda.synchronize()
        start = time.perf_counter()
        
        mem = torch.empty(CTRL_BUF_SIZE // 4, dtype=torch.float32, device='cuda')
        with torch.cuda.stream(stream):
            # Use tensor.fill_ which can work with streams
            mem.fill_(0)
        stream.synchronize()
        torch.cuda.synchronize()
        
        end = time.perf_counter()
        times_alloc_async_memset.append((end - start) * 1e6)
        del mem
        torch.cuda.empty_cache()
    
    alloc_only_avg = np.mean(times_alloc_only)
    sync_memset_avg = np.mean(times_alloc_sync_memset)
    async_memset_avg = np.mean(times_alloc_async_memset)
    
    print(f"\nResults:")
    print(f"  Allocation only: {alloc_only_avg:.2f} us")
    print(f"  Allocation + sync memset: {sync_memset_avg:.2f} us")
    print(f"  Allocation + async memset: {async_memset_avg:.2f} us")
    print(f"\nSync memset overhead: {sync_memset_avg - alloc_only_avg:.2f} us")
    print(f"Async memset overhead: {async_memset_avg - alloc_only_avg:.2f} us")
    if (sync_memset_avg - alloc_only_avg) > 0:
        improvement = ((sync_memset_avg - async_memset_avg) / (sync_memset_avg - alloc_only_avg) * 100)
        print(f"Improvement: {improvement:.1f}%")


def test_transfer_engine_reg_mr_overhead():
    """Test Transfer Engine reg_mr overhead"""
    print("\n" + "=" * 60)
    print("Testing Transfer Engine reg_mr overhead")
    print("=" * 60)
    
    if not torch.cuda.is_available():
        print("Skipped: CUDA not available")
        return
    
    try:
        # Initialize Transfer Engine
        # Note: TransferEngine requires initialize() with parameters, not init()
        # For testing reg_mr overhead, we can use a minimal setup
        engine = TransferEngine()
        
        # Get local hostname for initialization
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        local_server_name = f"{local_ip}:0"  # Port 0 means auto-assign
        
        # Initialize with P2PHANDSHAKE mode (no external metadata server needed)
        ret = engine.initialize(local_server_name, "P2PHANDSHAKE", "rdma", "")
        if ret != 0:
            print(f"Skipped: Transfer Engine initialization failed with code {ret}")
            print("Note: This test requires RDMA support. Trying TCP instead...")
            ret = engine.initialize(local_server_name, "P2PHANDSHAKE", "tcp", "")
            if ret != 0:
                print(f"Skipped: Transfer Engine initialization failed with TCP as well: {ret}")
                return
        
        # Test memory registration with different sizes
        sizes = [1 * 1024 * 1024,      # 1 MB
                 10 * 1024 * 1024,     # 10 MB
                 100 * 1024 * 1024,     # 100 MB
                 1024 * 1024 * 1024]    # 1 GB
        
        for size in sizes:
            print(f"\nTest size: {size / 1e6:.2f} MB")
            
            # Allocate GPU memory
            torch.cuda.synchronize()
            start_alloc = time.perf_counter()
            gpu_mem = torch.empty(size // 4, dtype=torch.float32, device='cuda')
            torch.cuda.synchronize()
            alloc_time = (time.perf_counter() - start_alloc) * 1e6
            
            # Test registration overhead
            times = []
            for i in range(5):
                torch.cuda.synchronize()
                start = time.perf_counter()
                
                # Register memory
                # Note: Python API uses register_memory (not registerLocalMemory)
                # and it only takes 2 parameters: buffer_addr (uintptr_t) and capacity (size_t)
                addr = gpu_mem.data_ptr()
                ret = engine.register_memory(addr, size)
                
                torch.cuda.synchronize()
                end = time.perf_counter()
                
                if ret == 0:
                    reg_time = (end - start) * 1e6
                    times.append(reg_time)
                    
                    # Unregister
                    engine.unregister_memory(addr)
                else:
                    print(f"  Registration failed: {ret}")
            
            if times:
                avg_time = np.mean(times)
                min_time = np.min(times)
                max_time = np.max(times)
                
                print(f"  Allocation time: {alloc_time:.2f} us")
                print(f"  Registration time - avg: {avg_time:.2f} us, min: {min_time:.2f} us, max: {max_time:.2f} us")
                print(f"  Total overhead: {alloc_time + avg_time:.2f} us")
            
            del gpu_mem
            torch.cuda.empty_cache()
    except Exception as e:
        print(f"Skipped: Transfer Engine initialization failed: {e}")


def test_buffer_init_overhead():
    """Test Buffer initialization overhead (includes memset and reg_mr)"""
    print("\n" + "=" * 60)
    print("Testing Buffer initialization overhead")
    print("=" * 60)
    
    if not dist.is_initialized():
        print("Skipped: Distributed environment not initialized")
        print("Hint: Requires at least 2 GPUs and distributed environment")
        return
    
    if not torch.cuda.is_available():
        print("Skipped: CUDA not available")
        return
    
    try:
        num_tokens, hidden, num_ranks, num_experts = 128, 7168, 2, 288
        num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(num_tokens, hidden, num_ranks, num_experts)
        
        print(f"Buffer size: {num_ep_buffer_bytes / 1e6:.2f} MB")
        
        # Warmup
        for _ in range(3):
            try:
                buffer = Buffer(dist.group.WORLD, num_ep_buffer_bytes=num_ep_buffer_bytes)
                del buffer
                torch.cuda.empty_cache()
            except:
                pass
        
        # Test multiple initializations
        times = []
        for i in range(10):
            torch.cuda.synchronize()
            start = time.perf_counter()
            
            buffer = Buffer(dist.group.WORLD, num_ep_buffer_bytes=num_ep_buffer_bytes)
            
            torch.cuda.synchronize()
            end = time.perf_counter()
            
            init_time = (end - start) * 1e6  # Convert to microseconds
            times.append(init_time)
            
            print(f"  Run {i+1}: {init_time:.2f} us")
            
            del buffer
            torch.cuda.empty_cache()
            time.sleep(0.1)  # Brief rest
        
        avg_time = np.mean(times[1:])  # Exclude first run
        min_time = np.min(times[1:])
        max_time = np.max(times[1:])
        std_time = np.std(times[1:])
        
        print(f"\nResults:")
        print(f"  Average: {avg_time:.2f} us")
        print(f"  Min: {min_time:.2f} us")
        print(f"  Max: {max_time:.2f} us")
        print(f"  Std dev: {std_time:.2f} us")
        
        return avg_time
    except Exception as e:
        print(f"Skipped: Buffer initialization test failed: {e}")
        import traceback
        traceback.print_exc()


class TestMemsetRegMrOverhead(unittest.TestCase):
    """Unittest wrapper class for running with pytest or unittest"""
    
    def test_cuda_memset(self):
        """Test CUDA memset overhead"""
        test_cuda_memset_overhead()
    
    def test_ctrl_buf_init(self):
        """Test ctrl_buf initialization overhead"""
        test_ctrl_buf_init_overhead()
    
    def test_transfer_engine_reg_mr(self):
        """Test Transfer Engine reg_mr overhead"""
        test_transfer_engine_reg_mr_overhead()


def main():
    """Main function"""
    print("Memset and reg_mr Overhead Test")
    print("=" * 60)
    
    # Initialize distributed environment (if needed)
    # Note: Distributed tests require multiple processes, so we skip them by default
    # Users can manually set up distributed environment if needed
    dist_initialized = False
    
    # Check if distributed environment is already initialized
    if dist.is_initialized():
        dist_initialized = True
        print(f"Using existing distributed environment: rank={dist.get_rank()}, world_size={dist.get_world_size()}")
    else:
        # Don't try to initialize distributed environment automatically
        # This requires multiple processes and proper setup
        print("Note: Distributed environment not initialized")
        print("Tests that require distributed environment (Buffer initialization) will be skipped")
        print("To enable distributed tests, initialize the environment before running this script:")
        print("  - Use torch.multiprocessing.spawn with multiple processes")
        print("  - Or manually initialize dist.init_process_group() in each process")
        print("  - Or set environment variables and ensure all processes are running")
    
    try:
        # Test 1: CUDA memset overhead (no distributed env needed)
        test_cuda_memset_overhead()
        
        # Test 2: ctrl_buf initialization overhead (no distributed env needed)
        test_ctrl_buf_init_overhead()
        
        # Test 3: Transfer Engine reg_mr overhead (no distributed env needed)
        test_transfer_engine_reg_mr_overhead()
        
        # Test 4: Buffer initialization overhead (requires distributed environment)
        if dist_initialized:
            test_buffer_init_overhead()
        else:
            print("\n" + "=" * 60)
            print("Skipping Buffer initialization test (requires distributed environment)")
            print("=" * 60)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if dist_initialized:
            try:
                dist.destroy_process_group()
            except:
                pass


if __name__ == "__main__":
    # If run as script, execute main
    # If run as unittest, execute TestMemsetRegMrOverhead class
    import sys
    
    # Check if we should run with multiprocessing (for distributed tests)
    if len(sys.argv) > 1 and sys.argv[1] == '--distributed':
        # Run with multiprocessing for distributed tests
        import torch.multiprocessing as mp
        
        def run_distributed_test(rank, world_size):
            """Run test in a distributed process"""
            os.environ['MASTER_ADDR'] = '127.0.0.1'
            os.environ['MASTER_PORT'] = '8361'
            dist.init_process_group(backend='mooncake', rank=rank, world_size=world_size)
            
            try:
                main()
            finally:
                dist.destroy_process_group()
        
        world_size = int(os.getenv('WORLD_SIZE', '2'))
        if world_size < 2:
            print("Error: WORLD_SIZE must be at least 2 for distributed tests")
            sys.exit(1)
        
        mp.spawn(run_distributed_test, args=(world_size,), nprocs=world_size, join=True)
    elif len(sys.argv) > 1 and sys.argv[1] == '--unittest':
        unittest.main(argv=sys.argv[1:])
    else:
        main()
