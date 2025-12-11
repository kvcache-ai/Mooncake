import os
import sys
import json
import time
import argparse
import unittest
import torch
import numpy as np
from dataclasses import dataclass
from mooncake.store import MooncakeDistributedStore

# ==========================================
#  Global Variables & Configuration
# ==========================================

# Global Store instance to ensure only one connection is established during the entire test session
GLOBAL_STORE = None
GLOBAL_CONFIG = None

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 4 * 1024 * 1024 * 1024  # 4 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 2 * 1024 * 1024 * 1024    # 2 GB
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_CHECK_SERVER = False

def parse_global_segment_size(value) -> int:
    """Parse human-readable size strings (e.g., '4GB') into bytes."""
    if isinstance(value, int): return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            return int(s[:-2].strip()) * 1024**3
        return int(s)
    return int(value)

@dataclass
class MooncakeStoreConfig:
    local_hostname: str
    metadata_server: str
    global_segment_size: int
    local_buffer_size: int
    protocol: str
    device_name: str
    master_server_address: str
    master_metrics_port: int
    check_server: bool

    @staticmethod
    def load_from_env() -> "MooncakeStoreConfig":
        """Load configuration from environment variables."""
        if not os.getenv("MOONCAKE_MASTER"):
            raise ValueError("Environment variable 'MOONCAKE_MASTER' is not set.")
        return MooncakeStoreConfig(
            local_hostname=os.getenv("LOCAL_HOSTNAME", "localhost"),
            metadata_server=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
            global_segment_size=parse_global_segment_size(
                os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", DEFAULT_GLOBAL_SEGMENT_SIZE)
            ),
            local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
            protocol=os.getenv("MOONCAKE_PROTOCOL", "tcp"),
            device_name=os.getenv("MOONCAKE_DEVICE", ""),
            master_server_address=os.getenv("MOONCAKE_MASTER"),
            master_metrics_port=int(os.getenv("MOONCAKE_MASTER_METRICS_PORT", DEFAULT_MASTER_METRICS_PORT)),
            check_server=bool(os.getenv("MOONCAKE_CHECK_SERVER", DEFAULT_CHECK_SERVER)),
        )

def create_store_connection():
    """Create and connect to the Store (called only once by setUpModule)."""
    store = MooncakeDistributedStore()
    config = MooncakeStoreConfig.load_from_env()
    print(f"[{os.getpid()}] Connecting to Mooncake Master at {config.master_server_address} using {config.protocol}...")
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup mooncake store, error code: {rc}")
    return store, config

def generate_tensors(num_tensors, size_mb):
    """Generate random Tensors suitable for TP testing."""
    size_bytes = int(size_mb * 1024 * 1024)
    element_size = 4  # float32
    num_elements = size_bytes // element_size
    dim = int(np.sqrt(num_elements))
    dim = (dim // 8) * 8  # Adjust dimension to be divisible by common TP sizes (2, 4, 8)
    
    # Use random data and ensure the tensor is contiguous in memory
    tensors = [torch.randn(dim, dim, dtype=torch.float32).contiguous() for _ in range(num_tensors)]
    # Use timestamp to prevent key collision in rare edge cases (though we remove_all anyway)
    keys = [f"test_tensor_{i}_{int(time.time()*1000)}" for i in range(num_tensors)]
    return keys, tensors

# ==========================================
#  Module Level Setup/Teardown (Hooks)
# ==========================================

def setUpModule():
    """Executed once before all tests in this file: establishes the global connection."""
    global GLOBAL_STORE, GLOBAL_CONFIG
    try:
        GLOBAL_STORE, GLOBAL_CONFIG = create_store_connection()
        print("✅ Global Store connection established.")
    except Exception as e:
        print(f"❌ Failed to establish global store connection: {e}")
        sys.exit(1)

def tearDownModule():
    """Executed once after all tests in this file: closes the global connection."""
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None

# ==========================================
#  Base Test Class
# ==========================================

class MooncakeTestBase(unittest.TestCase):
    def setUp(self):
        """Executed before each test method (test_xxx)."""
        # 1. Access the global connection
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")
        
        self.store = GLOBAL_STORE
        self.config = GLOBAL_CONFIG
        
        # 2. [Critical] Clean environment before the test starts
        # This ensures no stale data from previous tests affects the current one
        self.store.remove_all()

# ==========================================
#  Functional Tests
# ==========================================

class TestMooncakeFunctional(MooncakeTestBase):
    def test_01_basic_put_get(self):
        """Verify basic put and get functionality."""
        key = "func_test_single"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)
        
        # Perform Put
        rc = self.store.put_tensor(key, tensor)
        self.assertEqual(rc, 0, f"put_tensor failed with rc={rc}")
        self.assertTrue(self.store.is_exist(key), "Key not found after put")
        
        # Perform Get
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved, "Get returned None")
        self.assertTrue(torch.equal(tensor, retrieved), "Data mismatch between original and retrieved tensor")

    def test_02_tp_single_tensor(self):
        """Verify TP (Tensor Parallelism) splitting and reconstruction for a single Tensor."""
        tp_size = 4
        split_dim = 1
        key = "func_test_tp_single"
        
        # Create a small tensor (e.g., 16MB)
        _, tensors = generate_tensors(1, 16)
        target_tensor = tensors[0]

        # 1. Put with TP
        rc = self.store.put_tensor_with_tp(key, target_tensor, tp_size=tp_size, split_dim=split_dim)
        self.assertEqual(rc, 0, "put_tensor_with_tp failed")

        # 2. Verify existence of shards (White-box check: key_tp_0, key_tp_1...)
        for rank in range(tp_size):
            shard_key = f"{key}_tp_{rank}"
            self.assertTrue(self.store.is_exist(shard_key), f"Shard key {shard_key} is missing in store")

        # 3. Get shards and Reconstruct
        slices = []
        expected_chunks = target_tensor.chunk(tp_size, split_dim)
        for rank in range(tp_size):
            t_slice = self.store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
            self.assertIsNotNone(t_slice, f"Slice for rank {rank} is None")
            self.assertTrue(torch.equal(t_slice, expected_chunks[rank]), f"Data mismatch for rank {rank}")
            slices.append(t_slice)

        reconstructed = torch.cat(slices, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, target_tensor), "Reconstructed tensor does not match original")

    def test_03_tp_batch(self):
        """Verify TP splitting and reconstruction for a Batch of Tensors."""
        tp_size = 2
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8) # Small size for functional testing
        
        # 1. Batch Put with TP
        results = self.store.batch_put_tensor_with_tp(keys, tensors, tp_size=tp_size, split_dim=split_dim)
        self.assertTrue(all(r == 0 for r in results), f"Batch put failed. Results: {results}")

        # 2. Batch Get per Rank
        all_shards = [] # List of lists: [ [shards_rank0...], [shards_rank1...] ]
        for rank in range(tp_size):
            shards = self.store.batch_get_tensor_with_tp(keys, tp_rank=rank, tp_size=tp_size)
            self.assertEqual(len(shards), num_tensors)
            all_shards.append(shards)

        # 3. Verify & Reconstruct
        for i in range(num_tensors):
            original = tensors[i]
            expected_chunks = original.chunk(tp_size, split_dim)
            reconstruction_parts = []
            
            for rank in range(tp_size):
                shard = all_shards[rank][i]
                self.assertTrue(torch.equal(shard, expected_chunks[rank]), 
                                f"Tensor {i} Rank {rank} data mismatch")
                reconstruction_parts.append(shard)
            
            recon = torch.cat(reconstruction_parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, original), f"Tensor {i} final reconstruction mismatch")

# ==========================================
#  Performance/Benchmark Tests
# ==========================================

class TestMooncakeBenchmark(MooncakeTestBase):
    # Benchmark Settings
    BENCH_ITERATIONS = 5
    TENSOR_SIZE_MB = 64
    TOTAL_SIZE_GB = 1 

    def setUp(self):
        """Benchmark-specific setUp."""
        # 1. Call parent setUp to clean the store (remove_all)
        super().setUp()
        
        # 2. Generate test data
        total_bytes = self.TOTAL_SIZE_GB * 1024**3
        tensor_bytes = self.TENSOR_SIZE_MB * 1024**2
        self.num_tensors = max(1, total_bytes // tensor_bytes)
        
        print(f"\n[Gen] Generating {self.num_tensors} tensors (~{self.TENSOR_SIZE_MB}MB each)...")
        self.keys, self.tensors = generate_tensors(self.num_tensors, self.TENSOR_SIZE_MB)
        self.total_bits = (tensor_bytes * self.num_tensors) * 8

    def _print_perf(self, name, times):
        avg_time = np.mean(times)
        avg_gbps = (self.total_bits / 1e9) / avg_time
        print(f"👉 [Result] {name:30} | Avg Time: {avg_time:.4f}s | Throughput: {avg_gbps:.2f} Gbps")

    def test_benchmark_01_batch_put_get(self):
        """Benchmark: Standard Batch Put/Get."""
        put_times = []
        get_times = []

        print(f"--- Running Standard Batch Benchmark ({self.BENCH_ITERATIONS} iters) ---")
        for i in range(self.BENCH_ITERATIONS):
            # Clean store before each iteration for "cold" writes
            self.store.remove_all()
            
            # Measure Put
            t0 = time.perf_counter()
            self.store.batch_put_tensor(self.keys, self.tensors)
            put_times.append(time.perf_counter() - t0)

            # Measure Get
            t0 = time.perf_counter()
            res = self.store.batch_get_tensor(self.keys)
            get_times.append(time.perf_counter() - t0)
            self.assertEqual(len(res), len(self.tensors))

        self._print_perf("Standard Batch Put", put_times)
        self._print_perf("Standard Batch Get", get_times)

    def test_benchmark_02_tp_batch(self):
        """Benchmark: TP Batch Put/Get."""
        tp_size = 4
        split_dim = 0
        put_times = []
        get_times = []

        print(f"--- Running TP Batch Benchmark (TP={tp_size}) ---")
        for i in range(self.BENCH_ITERATIONS):
            self.store.remove_all()

            # Measure TP Put (Auto-chunking)
            t0 = time.perf_counter()
            self.store.batch_put_tensor_with_tp(self.keys, self.tensors, tp_size=tp_size, split_dim=split_dim)
            put_times.append(time.perf_counter() - t0)

            # Measure TP Get (Simulating gathering all ranks)
            t_get_start = time.perf_counter()
            for rank in range(tp_size):
                res = self.store.batch_get_tensor_with_tp(self.keys, tp_rank=rank, tp_size=tp_size)
                self.assertEqual(len(res), len(self.tensors))
            get_times.append(time.perf_counter() - t_get_start)

        self._print_perf(f"TP Batch Put (TP={tp_size})", put_times)
        self._print_perf(f"TP Batch Get (TP={tp_size})", get_times)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake Distributed Store Tests")
    parser.add_argument("--mode", type=str, default="all", choices=["all", "func", "perf"],
                        help="Run 'func' (functional correctness), 'perf' (benchmark), or 'all' (default)")
    
    # Parse args, keeping unknown args for unittest (e.g., -v)
    args, unknown = parser.parse_known_args()
    
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    if args.mode in ["all", "func"]:
        print(">> Loading Functional Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeFunctional))
    
    if args.mode in ["all", "perf"]:
        print(">> Loading Performance Benchmark Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeBenchmark))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with proper status code
    sys.exit(not result.wasSuccessful())