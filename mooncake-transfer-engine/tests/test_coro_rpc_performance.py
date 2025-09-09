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
import struct
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


class PythonTensorRebuilder:
    """Pure Python implementation of tensor rebuilding from raw data"""
    
    # Tensor dtype mappings (matching C++ enum)
    DTYPE_MAP = {
        0: None,  # UNKNOWN
        1: np.float16,  # FLOAT16
        2: np.float32,  # FLOAT32  
        3: np.float64,  # FLOAT64
        4: np.int8,     # INT8
        5: np.int16,    # INT16
        6: np.int32,    # INT32
        7: np.int64,    # INT64
        8: np.uint8,    # UINT8
        9: np.bool_,    # BOOL
    }
    
    TORCH_DTYPE_MAP = {
        1: torch.float16,  # FLOAT16
        2: torch.float32,  # FLOAT32  
        3: torch.float64,  # FLOAT64
        4: torch.int8,     # INT8
        5: torch.int16,    # INT16
        6: torch.int32,    # INT32
        7: torch.int64,    # INT64
        8: torch.uint8,    # UINT8
        9: torch.bool,     # BOOL
    }
    
    @staticmethod
    def parse_tensor_metadata(raw_data: bytes) -> Tuple[int, int, List[int], int]:
        """
        Parse tensor metadata from raw bytes
        
        Returns:
            (dtype, ndim, shape, metadata_size)
        """
        if len(raw_data) < 72:  # Size of TensorMetadata struct
            raise ValueError(f"Raw data too short for metadata: {len(raw_data)} bytes")
        
        # TensorMetadata struct layout:
        # int32_t dtype (4 bytes)
        # int32_t ndim (4 bytes)  
        # int64_t shape[4] (32 bytes)
        # char padding[32] (32 bytes)
        # Total: 72 bytes
        
        metadata_format = '<ii4q32s'  # little-endian: 2 int32, 4 int64, 32 char
        metadata_size = struct.calcsize(metadata_format)
        
        try:
            unpacked = struct.unpack(metadata_format, raw_data[:metadata_size])
            dtype = unpacked[0]
            ndim = unpacked[1]
            shape_array = unpacked[2:6]  # Extract shape[4]
            # padding = unpacked[6]  # Not used
            
            # Extract actual shape (only first ndim elements are valid)
            shape = [int(shape_array[i]) for i in range(ndim) if i < 4]
            
            return dtype, ndim, shape, metadata_size
            
        except struct.error as e:
            raise ValueError(f"Failed to parse tensor metadata: {e}")
    
    @staticmethod
    def rebuild_tensor_from_raw_data(raw_data: bytes, return_torch: bool = True) -> torch.Tensor:
        """
        Rebuild tensor from raw data bytes (pure Python implementation)
        
        Args:
            raw_data: Raw bytes containing tensor metadata + data
            return_torch: If True, return torch.Tensor; if False, return numpy array
            
        Returns:
            Reconstructed tensor
        """
        print(f"[PYTHON] Tensor rebuilder: processing {len(raw_data)} bytes")
        
        # Parse metadata
        dtype_id, ndim, shape, metadata_size = PythonTensorRebuilder.parse_tensor_metadata(raw_data)
        
        print(f"[PYTHON] Parsed metadata: dtype_id={dtype_id}, ndim={ndim}, shape={shape}")
        
        # Validate dtype
        if dtype_id not in PythonTensorRebuilder.DTYPE_MAP or PythonTensorRebuilder.DTYPE_MAP[dtype_id] is None:
            raise ValueError(f"Unknown or unsupported dtype: {dtype_id}")
        
        # Get numpy dtype
        np_dtype = PythonTensorRebuilder.DTYPE_MAP[dtype_id]
        element_size = np.dtype(np_dtype).itemsize
        
        # Calculate expected data size
        total_elements = 1
        for dim in shape:
            total_elements *= dim
        expected_data_size = total_elements * element_size
        
        print(f"[PYTHON] Expected: {total_elements} elements × {element_size} bytes = {expected_data_size} bytes")
        
        # Extract tensor data (skip metadata)
        tensor_data = raw_data[metadata_size:]
        actual_data_size = len(tensor_data)
        
        print(f"[PYTHON] Actual tensor data size: {actual_data_size} bytes")
        
        if actual_data_size < expected_data_size:
            raise ValueError(f"Insufficient tensor data: expected {expected_data_size}, got {actual_data_size}")
        
        # Take only the required bytes (there might be padding)
        tensor_data = tensor_data[:expected_data_size]
        
        # Create numpy array from raw bytes
        print(f"[PYTHON] Creating numpy array with dtype {np_dtype} and shape {shape}")
        
        try:
            # Convert bytes to numpy array
            np_array = np.frombuffer(tensor_data, dtype=np_dtype)
            
            # Reshape to target shape
            np_array = np_array.reshape(shape)
            
            print(f"[PYTHON] Successfully created numpy array: shape={np_array.shape}, dtype={np_array.dtype}")
            
            if return_torch:
                # Convert to torch tensor
                if dtype_id in PythonTensorRebuilder.TORCH_DTYPE_MAP:
                    torch_dtype = PythonTensorRebuilder.TORCH_DTYPE_MAP[dtype_id]
                    torch_tensor = torch.from_numpy(np_array.copy()).to(torch_dtype)
                    print(f"[PYTHON] Converted to torch tensor: shape={torch_tensor.shape}, dtype={torch_tensor.dtype}")
                    return torch_tensor
                else:
                    raise ValueError(f"Cannot convert dtype {dtype_id} to torch tensor")
            else:
                return np_array
                
        except Exception as e:
            raise ValueError(f"Failed to create tensor from data: {e}")
    
    @staticmethod
    def rebuild_tensor_from_received_tensor(received_tensor_obj, return_torch: bool = True):
        """
        Rebuild tensor from ReceivedTensor object using pure Python
        
        Args:
            received_tensor_obj: ReceivedTensor object from callback
            return_torch: If True, return torch.Tensor; if False, return numpy array
            
        Returns:
            Reconstructed tensor
        """
        # Try multiple ways to get raw data from ReceivedTensor object
        raw_data = None
        
        # Method 1: Try direct data attribute access
        if hasattr(received_tensor_obj, 'data'):
            try:
                raw_data = received_tensor_obj.data
                print(f"[PYTHON] Got data via direct attribute: {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"[PYTHON] Failed to get data via direct attribute: {e}")
        
        # Method 2: Try getDataAsBytes method
        if raw_data is None and hasattr(received_tensor_obj, 'get_data_as_bytes'):
            try:
                raw_data = received_tensor_obj.get_data_as_bytes()
                print(f"[PYTHON] Got data via get_data_as_bytes(): {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"[PYTHON] Failed to get data via get_data_as_bytes(): {e}")
                
        # Method 3: Try getDataAsBytes with different naming
        if raw_data is None and hasattr(received_tensor_obj, 'getDataAsBytes'):
            try:
                raw_data = received_tensor_obj.getDataAsBytes()
                print(f"[PYTHON] Got data via getDataAsBytes(): {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"[PYTHON] Failed to get data via getDataAsBytes(): {e}")
        
        if raw_data is None:
            # Debug: print available attributes
            attrs = [attr for attr in dir(received_tensor_obj) if not attr.startswith('_')]
            print(f"[PYTHON] Available attributes: {attrs}")
            raise ValueError(f"Cannot get raw data from ReceivedTensor object. Available attributes: {attrs}")
        
        # Convert different data types to bytes
        if isinstance(raw_data, bytes):
            pass  # Already bytes
        elif isinstance(raw_data, str):
            # Convert string to bytes using latin1 to preserve byte values
            raw_data = raw_data.encode('latin1')
        elif hasattr(raw_data, 'encode'):
            raw_data = raw_data.encode('latin1')
        else:
            raise ValueError(f"Unknown data type: {type(raw_data)}")
        
        return PythonTensorRebuilder.rebuild_tensor_from_raw_data(raw_data, return_torch)


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
        print("\n" + "="*80)
        print("PERFORMANCE TEST RESULTS SUMMARY")
        print("="*80)
        
        if self.data_results:
            print("\nDATA INTERFACE PERFORMANCE:")
            print("-" * 80)
            print(f"{'Size (MB)':<15} {'Time (ms)':<15} {'Send BW (MB/s)':<18} {'Total BW (MB/s)':<18} {'Network Latency':<15}")
            print("-" * 80)
            for result in self.data_results:
                print(f"{result['size_mb']:<15.3f} {result['time_ms']:<15.2f} {result['bandwidth_mbps']:<18.2f} {'N/A':<18} {'< 1ms':<15}")
                
        if self.tensor_results:
            print("\nTENSOR INTERFACE PERFORMANCE:")
            print("-" * 100)
            print(f"{'Type':<12} {'Shape':<25} {'Size (MB)':<15} {'Time (ms)':<15} {'Send BW (MB/s)':<18} {'Validation':<15}")
            print("-" * 100)
            for result in self.tensor_results:
                shape_str = str(result['shape'])[:23]
                print(f"{result['tensor_type']:<12} {shape_str:<25} {result['size_mb']:<15.2f} "
                      f"{result['time_ms']:<15.2f} {result['bandwidth_mbps']:<18.2f} {'PASS':<15}")


class CoroRPCPerformanceTester:
    """Main performance testing class"""
    
    def __init__(self):
        self.server = None
        self.client = None
        self.server_addr = "127.0.0.1:8889"
        self.results = PerformanceTestResults()
        
        # Callback tracking
        self.data_received_count = 0
        self.tensor_received_count = 0
        self.data_receive_times = []
        self.tensor_receive_times = []
        self.receive_lock = threading.Lock()
        
        # Store tensors for validation
        self.sent_tensors = []
        self.received_tensors = []
        
    def setup(self) -> bool:
        """Initialize server and client"""
        print("[SETUP] Initializing CoroRPC performance test environment...")
        
        try:
            # Create server and client instances
            self.server = CoroRPCInterface()
            self.client = CoroRPCInterface()
            
            # Initialize server
            if not self.server.initialize(self.server_addr, 1, 30, 4):
                print("[ERROR] Failed to initialize server")
                return False
                
            # Initialize client
            if not self.client.initialize("", 0, 30, 4):
                print("[ERROR] Failed to initialize client")
                return False
                
            # Set up callbacks
            self.server.set_data_receive_callback(self._data_receive_callback)
            self.server.set_tensor_receive_callback(self._tensor_receive_callback)
            
            # Start server
            if not self.server.start_server_async():
                print("[ERROR] Failed to start server")
                return False
                
            print(f"[SETUP] Server started on {self.server_addr}")
            time.sleep(1)
            
            print("[SETUP] Client ready to connect to server")
            time.sleep(0.5)
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Setup failed with exception: {e}")
            return False
    
    def teardown(self):
        """Clean up resources"""
        try:
            if self.server:
                self.server.stop_server()
                print("[CLEANUP] Server stopped")
        except:
            pass
            
    def _data_receive_callback(self, received_data):
        """Simple callback for data reception with timing info"""
        callback_time = time.time()
        
        with self.receive_lock:
            self.data_received_count += 1
            self.data_receive_times.append(callback_time)
            
            source_address = received_data.get("source", "unknown")
            data = received_data.get("data", b"")
            print(f"   [DATA] Received: {len(data):,} bytes | Time: {callback_time:.6f}")
    
    def _tensor_receive_callback(self, received_tensor):
        """Simple callback for tensor reception with timing info"""
        callback_time = time.time()
        
        with self.receive_lock:
            self.tensor_received_count += 1
            self.tensor_receive_times.append(callback_time)
            
            if not hasattr(self, 'received_tensors'):
                self.received_tensors = []
            self.received_tensors.append(received_tensor)
            
            print(f"   [TENSOR] Received: {received_tensor.source_address} | Time: {callback_time:.6f}")

    def validate_tensor_equality(self, original_tensor, received_tensor_obj) -> bool:
        """Simple tensor validation with timing"""
        try:
            validation_start = time.time()
            rebuilt_tensor = PythonTensorRebuilder.rebuild_tensor_from_received_tensor(
                received_tensor_obj, return_torch=True)
            rebuild_time = (time.time() - validation_start) * 1000
            
            # Quick validation
            if original_tensor.shape != rebuilt_tensor.shape:
                return False
            if original_tensor.dtype != rebuilt_tensor.dtype:
                return False
                
            compare_start = time.time()
            if original_tensor.dtype in [torch.float16, torch.float32, torch.float64]:
                values_match = torch.allclose(original_tensor, rebuilt_tensor, rtol=1e-5, atol=1e-8)
            else:
                values_match = torch.equal(original_tensor, rebuilt_tensor)
            compare_time = (time.time() - compare_start) * 1000
            
            print(f"   [VALIDATION] Rebuild={rebuild_time:.2f}ms | Compare={compare_time:.2f}ms | Result={'PASS' if values_match else 'FAIL'}")
            return values_match
            
        except Exception as e:
            print(f"   [ERROR] Validation failed: {e}")
            return False
    
    def test_comprehensive_performance(self) -> bool:
        """Comprehensive performance test with detailed metrics"""
        print("\n" + "="*80)
        print("COMPREHENSIVE CORO-RPC PERFORMANCE ANALYSIS")
        print("="*80)
        
        # Test configurations
        test_configs = [
            # Data interface tests
            (1.0/1024, "data", "Small Data (1KB)"),
            (10.0, "data", "Medium Data (10MB)"),
            (100.0, "data", "Large Data (100MB)"),
            
            # Tensor interface tests  
            (1.0, "tensor", "Small Tensor (1MB)"),
            (50.0, "tensor", "Medium Tensor (50MB)"),
            (200.0, "tensor", "Large Tensor (200MB)"),
        ]
        
        success_count = 0
        total_tests = len(test_configs)
        
        for i, (size_mb, test_type, description) in enumerate(test_configs, 1):
            print(f"\n[TEST {i}/{total_tests}] {description}")
            print("-" * 60)
            
            try:
                if self.run_performance_test(size_mb, test_type):
                    success_count += 1
                    print(f"[RESULT] Test {i} PASSED")
                else:
                    print(f"[RESULT] Test {i} FAILED")
                    
                # Brief pause between tests
                if i < total_tests:
                    time.sleep(1.0)
                    
            except Exception as e:
                print(f"[ERROR] Test {i} failed with exception: {e}")
                
        print(f"\n[SUMMARY] Tests completed: {success_count}/{total_tests} passed")
        return success_count == total_tests
    
    def run_performance_test(self, size_mb: float, data_type: str = "data") -> bool:
        """Run a single performance test with detailed timing breakdown"""
        
        # Step 1: Prepare data/tensor
        prepare_start = time.time()
        if data_type == "data":
            data_size_bytes = int(size_mb * 1024 * 1024)
            if data_size_bytes <= 1024:
                test_data = b"CoroRPC_Test_" * (data_size_bytes // 13 + 1)
                test_data = test_data[:data_size_bytes]
            else:
                pattern = bytes(range(256)) * 4
                test_data = pattern * (data_size_bytes // len(pattern) + 1)
                test_data = test_data[:data_size_bytes]
            test_object = test_data
        else:  # tensor
            # Create tensor to match target size
            element_size = 4  # float32
            numel = int(size_mb * 1024 * 1024 / element_size)
            # Create roughly square tensor
            side = int(numel ** 0.5)
            shape = (side, side)
            test_object = torch.randn(shape, dtype=torch.float32)
            actual_size_mb = test_object.numel() * test_object.element_size() / (1024 * 1024)
            size_mb = actual_size_mb  # Update to actual size
            
        prepare_time = (time.time() - prepare_start) * 1000
        
        # Step 2: Reset counters
        reset_start = time.time()
        with self.receive_lock:
            if data_type == "data":
                self.data_received_count = 0
                self.data_receive_times.clear()
            else:
                self.tensor_received_count = 0 
                self.tensor_receive_times.clear()
                self.sent_tensors.clear()
                self.received_tensors.clear()
                self.sent_tensors.append(test_object.clone())
        reset_time = (time.time() - reset_start) * 1000
        
        # Step 3: Send
        print(f"[SEND] Transmitting {size_mb:.3f} MB {data_type}...")
        send_start = time.time()
        if data_type == "data":
            result = self.client.send_data(self.server_addr, test_object)
        else:
            result = self.client.send_tensor(self.server_addr, test_object)
        send_end = time.time()
        send_time = (send_end - send_start) * 1000
        
        if result < 0:
            print(f"[ERROR] Send failed: {result}")
            return False
            
        # Step 4: Wait for reception
        print(f"[RECV] Waiting for reception...")
        wait_start = time.time()
        max_wait = 30.0  # 30 second timeout for large data
        
        while True:
            elapsed = time.time() - wait_start
            if data_type == "data" and self.data_received_count > 0:
                break
            elif data_type == "tensor" and self.tensor_received_count > 0:
                break
            elif elapsed > max_wait:
                print(f"[ERROR] Reception timeout after {elapsed:.2f}s")
                return False
            time.sleep(0.01)
            
        reception_time = time.time()
        wait_time = (reception_time - wait_start) * 1000
        
        # Step 5: Calculate timing metrics
        if data_type == "data":
            callback_time = self.data_receive_times[0]
        else:
            callback_time = self.tensor_receive_times[0]
            
        network_time = (callback_time - send_end) * 1000
        total_time = (callback_time - send_start) * 1000
        
        # Step 6: Validation (for tensors only)
        validation_time = 0
        validation_success = True
        if data_type == "tensor" and len(self.received_tensors) > 0:
            validation_start = time.time()
            validation_success = self.validate_tensor_equality(self.sent_tensors[0], self.received_tensors[0])
            validation_time = (time.time() - validation_start) * 1000
        
        # Step 7: Print comprehensive timing breakdown
        bandwidth = size_mb / (send_time / 1000) if send_time > 0 else 0
        total_bandwidth = size_mb / (total_time / 1000) if total_time > 0 else 0
        
        print(f"\n[METRICS] Performance Analysis:")
        print(f"   Data Size:          {size_mb:10.3f} MB")
        print(f"   Prepare Time:       {prepare_time:10.2f} ms")
        print(f"   Reset Time:         {reset_time:10.2f} ms")
        print(f"   Send Time:          {send_time:10.2f} ms  (Sender Processing)")
        print(f"   Network Latency:    {network_time:10.2f} ms  (Network + Receiver)")
        print(f"   Wait Time:          {wait_time:10.2f} ms")
        if validation_time > 0:
            print(f"   Validation Time:    {validation_time:10.2f} ms  (Data Integrity Check)")
        print(f"   Total Time:         {total_time:10.2f} ms")
        print(f"   Send Bandwidth:     {bandwidth:10.2f} MB/s")
        print(f"   End-to-End BW:      {total_bandwidth:10.2f} MB/s")
        print(f"   Efficiency:         {(send_time/total_time)*100:10.1f} %")
        
        # Store results
        if data_type == "data":
            self.results.add_data_result(size_mb, send_time, bandwidth)
        else:
            self.results.add_tensor_result("Float32", test_object.shape, size_mb, send_time, bandwidth)
            
        return validation_success if data_type == "tensor" else True


def main():
    """Main performance test with comprehensive analysis"""
    print("CoroRPC Interface Performance Analysis Suite")
    print("="*60)
    
    tester = CoroRPCPerformanceTester()
    
    try:
        # Setup
        print("[INIT] Setting up test environment...")
        if not tester.setup():
            print("[FATAL] Setup failed")
            return False
        
        print("[INIT] Setup completed successfully\n")
        
        # Run comprehensive tests
        print("[START] Running comprehensive performance tests...")
        success = tester.test_comprehensive_performance()
        
        # Print final results
        tester.results.print_summary()
        
        # Print conclusion
        print("\n" + "="*80)
        print("TEST CONCLUSION")
        print("="*80)
        if success:
            print("[SUCCESS] All performance tests completed successfully!")
            print("- CoroRPC interface is functioning correctly")
            print("- Performance metrics collected for analysis")
            print("- Tensor validation passed for all tests")
            return True
        else:
            print("[FAILURE] Some performance tests failed")
            print("- Check error logs above for details")
            return False
        
    except Exception as e:
        print(f"[FATAL] Test suite failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        print("\n[CLEANUP] Cleaning up test environment...")
        tester.teardown()


if __name__ == "__main__":
    success = main()
    print(f"\nFinal Result: {'SUCCESS' if success else 'FAILURE'}")
    sys.exit(0 if success else 1)
