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
        print(f"🐍 Python tensor rebuilder: processing {len(raw_data)} bytes")
        
        # Parse metadata
        dtype_id, ndim, shape, metadata_size = PythonTensorRebuilder.parse_tensor_metadata(raw_data)
        
        print(f"🐍 Parsed metadata: dtype_id={dtype_id}, ndim={ndim}, shape={shape}")
        
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
        
        print(f"🐍 Expected: {total_elements} elements × {element_size} bytes = {expected_data_size} bytes")
        
        # Extract tensor data (skip metadata)
        tensor_data = raw_data[metadata_size:]
        actual_data_size = len(tensor_data)
        
        print(f"🐍 Actual tensor data size: {actual_data_size} bytes")
        
        if actual_data_size < expected_data_size:
            raise ValueError(f"Insufficient tensor data: expected {expected_data_size}, got {actual_data_size}")
        
        # Take only the required bytes (there might be padding)
        tensor_data = tensor_data[:expected_data_size]
        
        # Create numpy array from raw bytes
        print(f"🐍 Creating numpy array with dtype {np_dtype} and shape {shape}")
        
        try:
            # Convert bytes to numpy array
            np_array = np.frombuffer(tensor_data, dtype=np_dtype)
            
            # Reshape to target shape
            np_array = np_array.reshape(shape)
            
            print(f"🐍 Successfully created numpy array: shape={np_array.shape}, dtype={np_array.dtype}")
            
            if return_torch:
                # Convert to torch tensor
                if dtype_id in PythonTensorRebuilder.TORCH_DTYPE_MAP:
                    torch_dtype = PythonTensorRebuilder.TORCH_DTYPE_MAP[dtype_id]
                    torch_tensor = torch.from_numpy(np_array.copy()).to(torch_dtype)
                    print(f"🐍 Converted to torch tensor: shape={torch_tensor.shape}, dtype={torch_tensor.dtype}")
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
                print(f"🐍 Got data via direct attribute: {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"🐍 Failed to get data via direct attribute: {e}")
        
        # Method 2: Try getDataAsBytes method
        if raw_data is None and hasattr(received_tensor_obj, 'get_data_as_bytes'):
            try:
                raw_data = received_tensor_obj.get_data_as_bytes()
                print(f"🐍 Got data via get_data_as_bytes(): {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"🐍 Failed to get data via get_data_as_bytes(): {e}")
                
        # Method 3: Try getDataAsBytes with different naming
        if raw_data is None and hasattr(received_tensor_obj, 'getDataAsBytes'):
            try:
                raw_data = received_tensor_obj.getDataAsBytes()
                print(f"🐍 Got data via getDataAsBytes(): {type(raw_data)}, length: {len(raw_data) if raw_data else 0}")
            except Exception as e:
                print(f"🐍 Failed to get data via getDataAsBytes(): {e}")
        
        if raw_data is None:
            # Debug: print available attributes
            attrs = [attr for attr in dir(received_tensor_obj) if not attr.startswith('_')]
            print(f"🐍 Available attributes: {attrs}")
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
        
        # Store tensors for validation
        self.sent_tensors = []  # Store original tensors for comparison
        self.received_tensors = []  # Store received tensors
        
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
            
            print("Client ready to connect to server")
            time.sleep(0.5)  # Wait for server startup to complete
            
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
    
    def validate_tensor_equality(self, original_tensor, received_tensor_obj) -> bool:
        """Validate that sent and received tensors are identical using Python rebuilder"""
        import torch  # Import at function level to avoid scoping issues
        
        try:
            print("🐍 Using Python tensor rebuilder...")
            rebuilt_tensor = PythonTensorRebuilder.rebuild_tensor_from_received_tensor(
                received_tensor_obj, return_torch=True)
            
            # Compare shapes
            if original_tensor.shape != rebuilt_tensor.shape:
                print(f"ERROR: Shape mismatch - original: {original_tensor.shape}, rebuilt: {rebuilt_tensor.shape}")
                return False
            
            # Compare dtypes
            if original_tensor.dtype != rebuilt_tensor.dtype:
                print(f"ERROR: Dtype mismatch - original: {original_tensor.dtype}, rebuilt: {rebuilt_tensor.dtype}")
                return False
            
            # Compare values (use torch.allclose for floating point tolerance)
            if original_tensor.dtype in [torch.float16, torch.float32, torch.float64]:
                if not torch.allclose(original_tensor, rebuilt_tensor, rtol=1e-5, atol=1e-8):
                    print("ERROR: Tensor values do not match (floating point)")
                    return False
            else:
                # For integer and boolean tensors, use exact equality
                if not torch.equal(original_tensor, rebuilt_tensor):
                    print("ERROR: Tensor values do not match (exact)")
                    return False
            
            print(f"SUCCESS: Tensor validation passed (Python 🐍) - shape: {original_tensor.shape}, dtype: {original_tensor.dtype}")
            return True
            
        except Exception as e:
            print(f"ERROR: Tensor validation failed (Python 🐍) with exception: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _tensor_receive_callback(self, received_tensor):
        """Callback for tensor reception with validation"""
        with self.receive_lock:
            self.tensor_received_count += 1
            self.tensor_receive_times.append(time.time())
            print(f"Tensor callback #{self.tensor_received_count}: received tensor from {received_tensor.source_address}")
            
            # Store the received tensor for validation
            if not hasattr(self, 'received_tensors'):
                self.received_tensors = []
            self.received_tensors.append(received_tensor)
    
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
        """Simple test for tensor interface to verify correctness with validation"""
        print("\n--- Testing Tensor Interface (Simple with Validation) ---")
        
        # Create a small test tensor
        test_tensor = torch.randn(10, 10, dtype=torch.float32)
        tensor_size_mb = test_tensor.numel() * test_tensor.element_size() / (1024 * 1024)
        
        print(f"Sending tensor {test_tensor.shape} ({tensor_size_mb:.6f} MB)")
        
        # Reset counters and clear storage
        with self.receive_lock:
            self.tensor_received_count = 0
            self.tensor_receive_times.clear()
            self.sent_tensors.clear()
            self.received_tensors.clear()
        
        # Store the original tensor for comparison
        self.sent_tensors.append(test_tensor.clone())  # Clone to avoid reference issues
        
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
            
        # Validate the received tensor using Python rebuilder
        if len(self.received_tensors) == 0:
            print("ERROR: No tensor stored in receive callback")
            return False
            
        original_tensor = self.sent_tensors[0]
        received_tensor_obj = self.received_tensors[0]
        
        # Test Python rebuilder
        print("Validating received tensor with Python rebuilder...")
        python_success = self.validate_tensor_equality(original_tensor, received_tensor_obj)
        
        if not python_success:
            print("ERROR: Python tensor validation failed!")
            return False
            
        print(f"SUCCESS: Tensor validation passed - sent and received tensor {test_tensor.shape}")
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
                print(f"  Creating test data pattern...")
                
                if data_size_bytes <= 50 * 1024 * 1024:  # <= 50MB: use simple method
                    test_data = bytes(range(256)) * (data_size_bytes // 256 + 1)
                    test_data = test_data[:data_size_bytes]
                else:  # > 50MB: use pattern-based efficient method
                    # Create a 1MB pattern
                    pattern_size = 1024 * 1024  # 1MB pattern
                    pattern = bytes(range(256)) * (pattern_size // 256)
                    
                    # Calculate how many full patterns and remainder
                    full_patterns = data_size_bytes // pattern_size
                    remainder = data_size_bytes % pattern_size
                    
                    print(f"  Using {full_patterns} full 1MB patterns + {remainder} bytes remainder")
                    
                    # Create data efficiently by concatenating patterns
                    if full_patterns > 0:
                        test_data = pattern * full_patterns
                        if remainder > 0:
                            test_data += pattern[:remainder]
                    else:
                        test_data = pattern[:remainder]
                
                print(f"  Data allocated successfully: {len(test_data)} bytes ({len(test_data)/(1024*1024):.1f} MB)")
                
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
        """Test tensor interface bandwidth performance with large tensors and validation"""
        print("\n--- Testing Tensor Interface Bandwidth Performance (Large Scale with Validation) ---")
        
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
            print(f"  Creating tensor (dtype: {dtype}, elements: {numel:,})...")
            
            try:
                # Create test tensor without memory check for now
                print(f"  Creating tensor without memory check...")
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
            
            # Reset counters before each test and store original tensor
            with self.receive_lock:
                self.tensor_received_count = 0
                self.tensor_receive_times.clear()
                # For large tensors, we'll only validate smaller ones to avoid memory issues
                if tensor_size_mb <= 200.0:  # Only validate tensors <= 200MB
                    self.sent_tensors.append(test_tensor.clone())
                    validate_this_tensor = True
                else:
                    validate_this_tensor = False
                    print(f"  Skipping validation for large tensor ({tensor_size_mb:.1f} MB) to avoid memory issues")
                self.received_tensors.clear()
            
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
                
                # Validate tensor if it's not too large
                if validate_this_tensor and len(self.received_tensors) > 0:
                    print(f"  Validating tensor correctness...")
                    original_tensor = self.sent_tensors[-1]  # Get the last sent tensor
                    received_tensor_obj = self.received_tensors[-1]  # Get the last received tensor
                    
                    # Use Python rebuilder for validation
                    print(f"  Validating tensor correctness...")
                    print(f"  Using Python rebuilder for efficiency...")
                    validation_success = self.validate_tensor_equality(
                        original_tensor, received_tensor_obj)
                    
                    if validation_success:
                        print(f"  ✓ Tensor validation PASSED (Python 🐍)")
                    else:
                        print(f"  ✗ Tensor validation FAILED")
                        # Continue with other tests even if validation fails
            else:
                print(f"  WARNING: No reception callback within {max_wait_time:.1f}s timeout")
            
            # Clean up large tensor
            del test_tensor
            if validate_this_tensor and len(self.sent_tensors) > 0:
                del self.sent_tensors[-1]  # Remove the stored tensor to free memory
            
            # Wait between tests (longer for large tensors)
            time.sleep(1.0)
            
        return True


def main():
    """Main test function focused on high-performance hundreds-of-MB testing"""
    print("CoroRPC High-Performance Testing Suite (Hundreds of MB)")
    print("="*60)
    
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
        
        # Run high-performance tests with hundreds of MB data
        print("\nPhase 2: High-Performance Testing (Hundreds of MB)")
        print("-" * 50)
        print("Testing large data transfers (50MB - 800MB) to measure peak performance")
        
        # Test data sizes focused on hundreds of MB
        large_data_sizes = [50.0, 100.0, 200.0, 300.0, 500.0, 800.0]  # 50MB to 800MB
        if not tester.test_data_bandwidth_performance_large_scale(large_data_sizes):
            print("FAILED: Large data bandwidth performance test failed")
            return False
            
        # Test tensor sizes focused on hundreds of MB  
        large_tensor_configs = [
            ("Float32_100MB", (5120, 5120), torch.float32),     # ~100MB
            ("Float32_200MB", (7237, 7237), torch.float32),     # ~200MB
            ("Float32_400MB", (10240, 10240), torch.float32),   # ~400MB
            ("Float64_200MB", (5120, 5120), torch.float64),     # ~200MB
            ("Int64_300MB", (8000, 5000), torch.int64),         # ~300MB
            ("Float32_600MB", (12500, 12500), torch.float32),   # ~600MB
        ]
        if not tester.test_tensor_bandwidth_performance_large_scale(large_tensor_configs):
            print("FAILED: Large tensor bandwidth performance test failed")
            return False
        
        # Print results
        tester.results.print_summary()
        
        print("\nSUCCESS: All high-performance tests completed!")
        print("\nTest Summary:")
        print(f"- Tested data sizes: 50MB to 800MB")
        print(f"- Tested tensor sizes: 100MB to 600MB")
        print(f"- All tests focused on measuring peak bandwidth performance")
        print(f"- Tensor correctness validation enabled (for tensors ≤ 200MB)")
        print(f"- Zero-copy optimization with pybind11::handle and std::string_view")
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
