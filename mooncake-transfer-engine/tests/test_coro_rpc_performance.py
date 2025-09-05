#!/usr/bin/env python3
"""
CoroRPC Performance Testing Suite
Tests bandwidth performance for data and tensor interfaces
"""

import torch
import numpy as np
import time
import sys
import threading
from typing import List, Tuple, Dict, Any

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


class PerformanceTestResults:
    """Container for performance test results"""
    
    def __init__(self):
        self.data_results: List[Dict[str, Any]] = []
        self.tensor_results: List[Dict[str, Any]] = []
        
    def add_data_result(self, size_mb: float, time_ms: float, bandwidth_mbps: float):
        self.data_results.append({
            'size_mb': size_mb,
            'time_ms': time_ms,
            'bandwidth_mbps': bandwidth_mbps
        })
        
    def add_tensor_result(self, tensor_type: str, shape: tuple, size_mb: float, 
                         time_ms: float, bandwidth_mbps: float):
        self.tensor_results.append({
            'tensor_type': tensor_type,
            'shape': shape,
            'size_mb': size_mb,
            'time_ms': time_ms,
            'bandwidth_mbps': bandwidth_mbps
        })
        
    def print_summary(self):
        print("\n" + "="*60)
        print("PERFORMANCE TEST RESULTS SUMMARY")
        print("="*60)
        
        if self.data_results:
            print("\nDATA INTERFACE PERFORMANCE:")
            print(f"{'Size (MB)':<12} {'Time (ms)':<12} {'Bandwidth (MB/s)':<16}")
            print("-" * 40)
            for result in self.data_results:
                print(f"{result['size_mb']:<12.2f} {result['time_ms']:<12.2f} {result['bandwidth_mbps']:<16.2f}")
                
        if self.tensor_results:
            print("\nTENSOR INTERFACE PERFORMANCE:")
            print(f"{'Type':<12} {'Shape':<20} {'Size (MB)':<12} {'Time (ms)':<12} {'Bandwidth (MB/s)':<16}")
            print("-" * 80)
            for result in self.tensor_results:
                shape_str = str(result['shape'])[:18]
                print(f"{result['tensor_type']:<12} {shape_str:<20} {result['size_mb']:<12.2f} "
                      f"{result['time_ms']:<12.2f} {result['bandwidth_mbps']:<16.2f}")


class CoroRPCPerformanceTester:
    """Main performance testing class"""
    
    def __init__(self):
        self.server = None
        self.client = None
        self.server_addr = "127.0.0.1:8889"  # Use different port to avoid conflicts
        self.results = PerformanceTestResults()
        
        # Callback tracking
        self.data_received_count = 0
        self.tensor_received_count = 0
        self.data_receive_times = []
        self.tensor_receive_times = []
        self.receive_lock = threading.Lock()
        
    def setup(self) -> bool:
        """Initialize server and client"""
        print("Setting up CoroRPC performance test environment...")
        
        try:
            # Create server and client instances
            self.server = CoroRPCInterface()
            self.client = CoroRPCInterface()
            
            # Initialize server
            if not self.server.initialize(self.server_addr, 1, 30, 4):
                print("ERROR: Failed to initialize server")
                return False
                
            # Initialize client
            if not self.client.initialize("", 0, 30, 4):
                print("ERROR: Failed to initialize client")
                return False
                
            # Set up callbacks
            self.server.set_data_receive_callback(self._data_receive_callback)
            self.server.set_tensor_receive_callback(self._tensor_receive_callback)
            
            # Start server
            if not self.server.start_server_async():
                print("ERROR: Failed to start server")
                return False
                
            print(f"Server started on {self.server_addr}")
            time.sleep(1)  # Wait for server startup
            
            # Connect client to server
            if not self.client.add_remote_connection(self.server_addr):
                print("ERROR: Failed to connect client to server")
                return False
                
            print("Client connected to server")
            time.sleep(0.5)  # Wait for connection establishment
            
            return True
            
        except Exception as e:
            print(f"ERROR: Setup failed with exception: {e}")
            return False
    
    def teardown(self):
        """Clean up resources"""
        try:
            if self.server:
                self.server.stop_server()
        except:
            pass
            
    def _data_receive_callback(self, received_data):
        """Callback for data reception"""
        with self.receive_lock:
            self.data_received_count += 1
            self.data_receive_times.append(time.time())
            source_address = received_data.get("source", "unknown")
            data = received_data.get("data", b"")
            print(f"Data callback #{self.data_received_count}: received {len(data)} bytes from {source_address}")
    
    def _tensor_receive_callback(self, received_tensor):
        """Callback for tensor reception"""
        with self.receive_lock:
            self.tensor_received_count += 1
            self.tensor_receive_times.append(time.time())
            print(f"Tensor callback #{self.tensor_received_count}: received tensor from {received_tensor.source_address}")
    
    def test_data_interface_simple(self) -> bool:
        """Simple test for data interface to verify correctness"""
        print("\n--- Testing Data Interface (Simple) ---")
        
        # Test with small data size first
        test_data = b"Hello, CoroRPC Performance Test!"
        data_size_mb = len(test_data) / (1024 * 1024)
        
        print(f"Sending {len(test_data)} bytes ({data_size_mb:.6f} MB)")
        
        # Reset counters
        with self.receive_lock:
            self.data_received_count = 0
            self.data_receive_times.clear()
        
        # Send data and measure time
        start_time = time.time()
        result = self.client.send_data(self.server_addr, test_data)
        send_time = time.time()
        
        if result < 0:
            print(f"ERROR: Failed to send data, result: {result}")
            return False
            
        print(f"Data sent successfully in {(send_time - start_time)*1000:.2f} ms")
        
        # Wait for reception
        max_wait_time = 5.0  # 5 seconds timeout
        wait_start = time.time()
        
        while self.data_received_count == 0 and (time.time() - wait_start) < max_wait_time:
            time.sleep(0.1)
            
        if self.data_received_count == 0:
            print("ERROR: No data received within timeout")
            return False
            
        print(f"SUCCESS: Data interface test passed - sent and received {len(test_data)} bytes")
        return True
    
    def test_tensor_interface_simple(self) -> bool:
        """Simple test for tensor interface to verify correctness"""
        print("\n--- Testing Tensor Interface (Simple) ---")
        
        # Create a small test tensor
        test_tensor = torch.randn(10, 10, dtype=torch.float32)
        tensor_size_mb = test_tensor.numel() * test_tensor.element_size() / (1024 * 1024)
        
        print(f"Sending tensor {test_tensor.shape} ({tensor_size_mb:.6f} MB)")
        
        # Reset counters
        with self.receive_lock:
            self.tensor_received_count = 0
            self.tensor_receive_times.clear()
        
        # Send tensor and measure time
        start_time = time.time()
        result = self.client.send_tensor(self.server_addr, test_tensor)
        send_time = time.time()
        
        if result < 0:
            print(f"ERROR: Failed to send tensor, result: {result}")
            return False
            
        print(f"Tensor sent successfully in {(send_time - start_time)*1000:.2f} ms")
        
        # Wait for reception
        max_wait_time = 5.0  # 5 seconds timeout
        wait_start = time.time()
        
        while self.tensor_received_count == 0 and (time.time() - wait_start) < max_wait_time:
            time.sleep(0.1)
            
        if self.tensor_received_count == 0:
            print("ERROR: No tensor received within timeout")
            return False
            
        print(f"SUCCESS: Tensor interface test passed - sent and received tensor {test_tensor.shape}")
        return True
    
    def test_data_bandwidth_performance(self, sizes_mb: List[float]) -> bool:
        """Test data interface bandwidth performance with various sizes"""
        print("\n--- Testing Data Interface Bandwidth Performance ---")
        
        for size_mb in sizes_mb:
            print(f"\nTesting data size: {size_mb} MB")
            
            # Create test data
            data_size_bytes = int(size_mb * 1024 * 1024)
            test_data = bytes(range(256)) * (data_size_bytes // 256 + 1)
            test_data = test_data[:data_size_bytes]
            
            # Reset counters before each test
            with self.receive_lock:
                self.data_received_count = 0
                self.data_receive_times.clear()
            
            # Measure send time
            start_time = time.time()
            result = self.client.send_data(self.server_addr, test_data)
            end_time = time.time()
            
            if result < 0:
                print(f"ERROR: Failed to send {size_mb} MB data")
                continue
                
            elapsed_ms = (end_time - start_time) * 1000
            bandwidth_mbps = size_mb / (elapsed_ms / 1000) if elapsed_ms > 0 else 0
            
            print(f"  Size: {size_mb:.2f} MB")
            print(f"  Time: {elapsed_ms:.2f} ms")
            print(f"  Bandwidth: {bandwidth_mbps:.2f} MB/s")
            
            self.results.add_data_result(size_mb, elapsed_ms, bandwidth_mbps)
            
            # Wait for reception with timeout
            max_wait_time = 2.0
            wait_start = time.time()
            while self.data_received_count == 0 and (time.time() - wait_start) < max_wait_time:
                time.sleep(0.1)
            
            if self.data_received_count > 0:
                print(f"  Reception confirmed: callback received")
            else:
                print(f"  WARNING: No reception callback within {max_wait_time}s timeout")
            
            # Wait between tests
            time.sleep(0.2)
            
        return True
    
    def test_tensor_bandwidth_performance(self, tensor_configs: List[Tuple[str, tuple, torch.dtype]]) -> bool:
        """Test tensor interface bandwidth performance with various tensor types"""
        print("\n--- Testing Tensor Interface Bandwidth Performance ---")
        
        for tensor_name, shape, dtype in tensor_configs:
            print(f"\nTesting tensor: {tensor_name} {shape}")
            
            # Create test tensor
            if dtype == torch.bool:
                test_tensor = torch.randint(0, 2, shape, dtype=dtype).bool()
            elif dtype in [torch.int32, torch.int64]:
                test_tensor = torch.randint(-100, 100, shape, dtype=dtype)
            else:
                test_tensor = torch.randn(shape, dtype=dtype)
            
            tensor_size_mb = test_tensor.numel() * test_tensor.element_size() / (1024 * 1024)
            
            # Reset counters before each test
            with self.receive_lock:
                self.tensor_received_count = 0
                self.tensor_receive_times.clear()
            
            # Measure send time
            start_time = time.time()
            result = self.client.send_tensor(self.server_addr, test_tensor)
            end_time = time.time()
            
            if result < 0:
                print(f"ERROR: Failed to send tensor {tensor_name}")
                continue
                
            elapsed_ms = (end_time - start_time) * 1000
            bandwidth_mbps = tensor_size_mb / (elapsed_ms / 1000) if elapsed_ms > 0 else 0
            
            print(f"  Type: {tensor_name}")
            print(f"  Shape: {shape}")
            print(f"  Size: {tensor_size_mb:.2f} MB")
            print(f"  Time: {elapsed_ms:.2f} ms")
            print(f"  Bandwidth: {bandwidth_mbps:.2f} MB/s")
            
            self.results.add_tensor_result(tensor_name, shape, tensor_size_mb, elapsed_ms, bandwidth_mbps)
            
            # Wait for reception with timeout
            max_wait_time = 2.0
            wait_start = time.time()
            while self.tensor_received_count == 0 and (time.time() - wait_start) < max_wait_time:
                time.sleep(0.1)
            
            if self.tensor_received_count > 0:
                print(f"  Reception confirmed: callback received")
            else:
                print(f"  WARNING: No reception callback within {max_wait_time}s timeout")
            
            # Wait between tests
            time.sleep(0.2)
            
        return True
    
    def test_data_bandwidth_performance_large_scale(self, sizes_mb: List[float]) -> bool:
        """Test data interface bandwidth performance with large data sizes (optimized for GB scale)"""
        print("\n--- Testing Data Interface Bandwidth Performance (Large Scale) ---")
        
        for size_mb in sizes_mb:
            print(f"\nTesting large data size: {size_mb} MB ({size_mb/1024:.2f} GB)")
            
            # Create test data efficiently for large sizes
            data_size_bytes = int(size_mb * 1024 * 1024)
            print(f"  Allocating {data_size_bytes} bytes ({data_size_bytes/(1024*1024*1024):.2f} GB)...")
            
            try:
                # Use more efficient data generation for large sizes
                # Create a pattern and repeat it to avoid memory issues
                pattern_size = min(1024 * 1024, data_size_bytes)  # 1MB pattern max
                pattern = bytes(range(256)) * (pattern_size // 256 + 1)
                pattern = pattern[:pattern_size]
                
                # For very large data, we create it in chunks
                if data_size_bytes > 100 * 1024 * 1024:  # If > 100MB
                    # Create data as repeated pattern
                    repeat_count = data_size_bytes // len(pattern)
                    remainder = data_size_bytes % len(pattern)
                    test_data = pattern * repeat_count + pattern[:remainder]
                else:
                    test_data = bytes(range(256)) * (data_size_bytes // 256 + 1)
                    test_data = test_data[:data_size_bytes]
                
                print(f"  Data allocated successfully: {len(test_data)} bytes")
                
            except MemoryError:
                print(f"  ERROR: Not enough memory to allocate {size_mb} MB")
                continue
            except Exception as e:
                print(f"  ERROR: Failed to create test data: {e}")
                continue
            
            # Reset counters before each test
            with self.receive_lock:
                self.data_received_count = 0
                self.data_receive_times.clear()
            
            # Measure send time
            print(f"  Starting transmission...")
            start_time = time.time()
            result = self.client.send_data(self.server_addr, test_data)
            end_time = time.time()
            
            if result < 0:
                print(f"  ERROR: Failed to send {size_mb} MB data")
                continue
                
            elapsed_ms = (end_time - start_time) * 1000
            elapsed_seconds = elapsed_ms / 1000
            bandwidth_mbps = size_mb / elapsed_seconds if elapsed_seconds > 0 else 0
            bandwidth_gbps = bandwidth_mbps / 1024
            
            print(f"  Size: {size_mb:.1f} MB ({size_mb/1024:.2f} GB)")
            print(f"  Time: {elapsed_ms:.1f} ms ({elapsed_seconds:.2f} seconds)")
            print(f"  Bandwidth: {bandwidth_mbps:.1f} MB/s ({bandwidth_gbps:.3f} GB/s)")
            
            self.results.add_data_result(size_mb, elapsed_ms, bandwidth_mbps)
            
            # Wait for reception with longer timeout for large data
            max_wait_time = max(10.0, size_mb / 100)  # At least 10s, or 1s per 100MB
            print(f"  Waiting for reception confirmation (timeout: {max_wait_time:.1f}s)...")
            wait_start = time.time()
            while self.data_received_count == 0 and (time.time() - wait_start) < max_wait_time:
                time.sleep(0.5)  # Check less frequently for large transfers
            
            if self.data_received_count > 0:
                reception_time = self.data_receive_times[0] - start_time
                print(f"  Reception confirmed: callback received after {reception_time:.2f}s")
            else:
                print(f"  WARNING: No reception callback within {max_wait_time:.1f}s timeout")
            
            # Clean up large data object
            del test_data
            
            # Wait between tests (longer for large data)
            time.sleep(1.0)
            
        return True
    
    def test_tensor_bandwidth_performance_large_scale(self, tensor_configs: List[Tuple[str, tuple, torch.dtype]]) -> bool:
        """Test tensor interface bandwidth performance with large tensors (optimized for GB scale)"""
        print("\n--- Testing Tensor Interface Bandwidth Performance (Large Scale) ---")
        
        for tensor_name, shape, dtype in tensor_configs:
            print(f"\nTesting large tensor: {tensor_name} {shape}")
            
            # Calculate expected size
            numel = 1
            for dim in shape:
                numel *= dim
            
            element_size = torch.tensor([], dtype=dtype).element_size()
            expected_size_mb = numel * element_size / (1024 * 1024)
            expected_size_gb = expected_size_mb / 1024
            
            print(f"  Expected size: {expected_size_mb:.1f} MB ({expected_size_gb:.2f} GB)")
            print(f"  Creating tensor...")
            
            try:
                # Create test tensor with memory monitoring
                if dtype == torch.bool:
                    test_tensor = torch.randint(0, 2, shape, dtype=dtype).bool()
                elif dtype in [torch.int32, torch.int64]:
                    test_tensor = torch.randint(-100, 100, shape, dtype=dtype)
                else:
                    test_tensor = torch.randn(shape, dtype=dtype)
                
                actual_size_mb = test_tensor.numel() * test_tensor.element_size() / (1024 * 1024)
                print(f"  Tensor created successfully: {actual_size_mb:.1f} MB")
                
            except RuntimeError as e:
                if "out of memory" in str(e).lower():
                    print(f"  ERROR: Out of memory creating tensor: {e}")
                    continue
                else:
                    print(f"  ERROR: Failed to create tensor: {e}")
                    continue
            except Exception as e:
                print(f"  ERROR: Failed to create tensor: {e}")
                continue
            
            tensor_size_mb = test_tensor.numel() * test_tensor.element_size() / (1024 * 1024)
            
            # Reset counters before each test
            with self.receive_lock:
                self.tensor_received_count = 0
                self.tensor_receive_times.clear()
            
            # Measure send time
            print(f"  Starting tensor transmission...")
            start_time = time.time()
            result = self.client.send_tensor(self.server_addr, test_tensor)
            end_time = time.time()
            
            if result < 0:
                print(f"  ERROR: Failed to send tensor {tensor_name}")
                continue
                
            elapsed_ms = (end_time - start_time) * 1000
            elapsed_seconds = elapsed_ms / 1000
            bandwidth_mbps = tensor_size_mb / elapsed_seconds if elapsed_seconds > 0 else 0
            bandwidth_gbps = bandwidth_mbps / 1024
            
            print(f"  Type: {tensor_name}")
            print(f"  Shape: {shape}")
            print(f"  Size: {tensor_size_mb:.1f} MB ({tensor_size_mb/1024:.2f} GB)")
            print(f"  Time: {elapsed_ms:.1f} ms ({elapsed_seconds:.2f} seconds)")
            print(f"  Bandwidth: {bandwidth_mbps:.1f} MB/s ({bandwidth_gbps:.3f} GB/s)")
            
            self.results.add_tensor_result(tensor_name, shape, tensor_size_mb, elapsed_ms, bandwidth_mbps)
            
            # Wait for reception with longer timeout for large tensors
            max_wait_time = max(10.0, tensor_size_mb / 100)  # At least 10s, or 1s per 100MB
            print(f"  Waiting for reception confirmation (timeout: {max_wait_time:.1f}s)...")
            wait_start = time.time()
            while self.tensor_received_count == 0 and (time.time() - wait_start) < max_wait_time:
                time.sleep(0.5)  # Check less frequently for large transfers
            
            if self.tensor_received_count > 0:
                reception_time = self.tensor_receive_times[0] - start_time
                print(f"  Reception confirmed: callback received after {reception_time:.2f}s")
            else:
                print(f"  WARNING: No reception callback within {max_wait_time:.1f}s timeout")
            
            # Clean up large tensor
            del test_tensor
            
            # Wait between tests (longer for large tensors)
            time.sleep(1.0)
            
        return True


def main():
    """Main test function"""
    print("CoroRPC Performance Testing Suite")
    print("="*50)
    
    tester = CoroRPCPerformanceTester()
    
    try:
        # Setup
        if not tester.setup():
            print("FAILED: Setup failed")
            return False
            
        # Run simple correctness tests first
        print("\nPhase 1: Correctness Verification")
        print("-" * 40)
        
        if not tester.test_data_interface_simple():
            print("FAILED: Data interface simple test failed")
            return False
            
        if not tester.test_tensor_interface_simple():
            print("FAILED: Tensor interface simple test failed")
            return False
            
        print("SUCCESS: All correctness tests passed!")
        
        # Run basic performance tests (small sizes for verification)
        print("\nPhase 2: Basic Performance Testing")
        print("-" * 40)
        
        # Test small data sizes first
        small_data_sizes = [0.001, 0.01, 0.1]  # 1KB, 10KB, 100KB
        if not tester.test_data_bandwidth_performance(small_data_sizes):
            print("FAILED: Data bandwidth performance test failed")
            return False
            
        # Test small tensors
        small_tensor_configs = [
            ("Float32_Small", (100, 100), torch.float32),
            ("Int64_Small", (50, 50), torch.int64),
            ("Bool_Small", (200, 200), torch.bool),
        ]
        if not tester.test_tensor_bandwidth_performance(small_tensor_configs):
            print("FAILED: Tensor bandwidth performance test failed")
            return False
        
        # Additional test with medium sizes for better performance insights
        print("\nPhase 3: Medium-scale Performance Testing")
        print("-" * 40)
        
        # Test medium data sizes
        medium_data_sizes = [1.0, 5.0, 10.0]  # 1MB, 5MB, 10MB
        if not tester.test_data_bandwidth_performance(medium_data_sizes):
            print("FAILED: Medium data bandwidth performance test failed")
            return False
            
        # Test medium tensors
        medium_tensor_configs = [
            ("Float32_Medium", (500, 500), torch.float32),      # ~1MB
            ("Int64_Medium", (1024, 256), torch.int64),         # ~2MB  
            ("Float64_Medium", (512, 512), torch.float64),      # ~2MB
        ]
        if not tester.test_tensor_bandwidth_performance(medium_tensor_configs):
            print("FAILED: Medium tensor bandwidth performance test failed")
            return False
        
        # Optional large-scale performance testing (1GB scale)
        print("\nPhase 4: Large-scale Performance Testing (1GB)")
        print("-" * 40)
        print("WARNING: This phase will test ~1GB data transfers and may take several minutes")
        
        # Test large data sizes (around 1GB)
        large_data_sizes = [100.0, 500.0, 1000.0]  # 100MB, 500MB, 1GB
        if not tester.test_data_bandwidth_performance_large_scale(large_data_sizes):
            print("FAILED: Large data bandwidth performance test failed")
            return False
            
        # Test large tensors (around 1GB)
        large_tensor_configs = [
            ("Float32_Large", (8192, 8192), torch.float32),     # ~256MB
            ("Float32_XLarge", (16384, 8192), torch.float32),   # ~512MB  
            ("Float32_XXLarge", (16384, 16384), torch.float32), # ~1GB
        ]
        if not tester.test_tensor_bandwidth_performance_large_scale(large_tensor_configs):
            print("FAILED: Large tensor bandwidth performance test failed")
            return False
        
        # Print results
        tester.results.print_summary()
        
        print("\nSUCCESS: All performance tests completed!")
        return True
        
    except Exception as e:
        print(f"ERROR: Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        tester.teardown()


if __name__ == "__main__":
    success = main()
    print(f"\nFinal result: {'SUCCESS' if success else 'FAILURE'}")
    sys.exit(0 if success else 1)
