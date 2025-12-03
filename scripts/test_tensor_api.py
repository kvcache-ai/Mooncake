from dataclasses import dataclass
import json
import torch
from mooncake.store import MooncakeDistributedStore
import os
import sys
import time
import argparse
import numpy as np

TENSOR_SIZE_MB = 64
TOTAL_BATCH_SIZE_GB = 1

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 4 * 1024 * 1024 * 1024  # 4 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 2 * 1024 * 1024 * 1024 # 2 GB
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_CHECK_SERVER = False
TENSOR_SIZE_BYTES = int(TENSOR_SIZE_MB * 1024 * 1024)
TOTAL_BATCH_SIZE_BYTES = int(TOTAL_BATCH_SIZE_GB * 1024 * 1024 * 1024)
NUM_TENSORS = TOTAL_BATCH_SIZE_BYTES // TENSOR_SIZE_BYTES

if TOTAL_BATCH_SIZE_BYTES % TENSOR_SIZE_BYTES != 0:
    print(f"Error: Total batch size {TOTAL_BATCH_SIZE_GB} GB is not "
          f"evenly divisible by tensor size {TENSOR_SIZE_MB} MB.",
          file=sys.stderr)
    sys.exit(1)

def _parse_global_segment_size(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            num = s[:-2].strip()
            if not num:
                raise ValueError("Invalid global_segment_size: missing number before 'gb'")
            return int(num) * 1024 * 1024 * 1024
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
    def from_file() -> "MooncakeStoreConfig":
        """Load the config from a JSON file."""
        file_path = os.getenv(DEFAULT_MOONCAKE_CONFIG_PATH_ENV)
        try:
            with open(file_path) as fin:
                config = json.load(fin)
        except Exception as e:
            raise RuntimeError(f"Failed to load config from {file_path}: {str(e)}")

        return MooncakeStoreConfig(
            local_hostname=config.get("local_hostname"),
            metadata_server=config.get("metadata_server"),
            global_segment_size=_parse_global_segment_size(
                config.get("global_segment_size", DEFAULT_GLOBAL_SEGMENT_SIZE)
            ),
            local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
            protocol=config.get("protocol", "tcp"),
            device_name=config.get("device_name", ""),
            master_server_address=config.get("master_server_address"),
            master_metrics_port=config.get("master_metrics_port", DEFAULT_MASTER_METRICS_PORT),
            check_server=config.get("check_server", DEFAULT_CHECK_SERVER),
        )

    @staticmethod
    def load_from_env() -> "MooncakeStoreConfig":
        """Load config from a file specified in the environment variable.
        export MOONCAKE_MASTER=10.13.3.232:50051
        export MOONCAKE_PROTOCOL="rdma"
        export MOONCAKE_DEVICE=""
        export MOONCAKE_TE_META_DATA_SERVER="P2PHANDSHAKE"
        """
        # other required environment variables...
        if not os.getenv("MOONCAKE_MASTER"):
            raise ValueError("The environment variable 'MOONCAKE_MASTER' is not set.")
        return MooncakeStoreConfig(
            local_hostname=os.getenv("LOCAL_HOSTNAME", "localhost"),
            metadata_server=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
            global_segment_size=_parse_global_segment_size(
                os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", DEFAULT_GLOBAL_SEGMENT_SIZE)
            ),
            local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
            protocol=os.getenv("MOONCAKE_PROTOCOL", "tcp"),
            device_name=os.getenv("MOONCAKE_DEVICE", ""),
            master_server_address=os.getenv("MOONCAKE_MASTER"),
            master_metrics_port=int(os.getenv("MOONCAKE_MASTER_METRICS_PORT", DEFAULT_MASTER_METRICS_PORT)),
            check_server=bool(os.getenv("MOONCAKE_CHECK_SERVER", DEFAULT_CHECK_SERVER)),
        )

def run_benchmark(num_iterations):
    store = MooncakeDistributedStore()

    print("==========================================================")
    print("       Mooncake Distributed Store Benchmark & Test")
    print("==========================================================")

    try:
        # 1. Configuration & Setup
        config = MooncakeStoreConfig.load_from_env()
        print(f"Configuration:")
        print(f"  Master:            {config.master_server_address}")
        print(f"  Protocol:          {config.protocol}")
        print(f"  Tensor Size:       {TENSOR_SIZE_MB} MB")
        print(f"  Tensors per Batch: {NUM_TENSORS}")

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
            print(f"❌ Failed to setup mooncake store, error code: {rc}", file=sys.stderr)
            sys.exit(1)
        print("✅ Mooncake store setup successful.")

        # 2. Data Preparation
        print("\n[Data Gen] Generating random tensors...")
        # Create tensors that are large enough and have even dims for easy splitting
        # Ensure dimensions are divisible by common TP sizes (2, 4, 8)
        element_size = 4 # float32
        num_elements = TENSOR_SIZE_BYTES // element_size
        dim = int(np.sqrt(num_elements))
        # Adjust dim to be divisible by 8 for clean TP tests
        dim = (dim // 8) * 8

        tensors_list = [
            torch.randn(dim, dim, dtype=torch.float32).contiguous()
            for _ in range(NUM_TENSORS)
        ]
        keys_list = [f"bench_tensor_{i}" for i in range(NUM_TENSORS)]
        print(f"  Created {NUM_TENSORS} tensors of shape [{dim}, {dim}] (approx {TENSOR_SIZE_MB} MB each)")

        # ----------------------------------------
        # Test 1: batch_put_tensor
        # ----------------------------------------
        print(f"\n--- Test 1: batch_put_tensor ({num_iterations} iters) ---")
        # Warmup / Cleanup
        store.remove_all()
        put_times = []
        for i in range(num_iterations):
            store.remove_all()

            start_time = time.perf_counter()
            results = store.batch_put_tensor(keys_list, tensors_list)
            end_time = time.perf_counter()

            if not all(r == 0 for r in results):
                print(f"  Iter {i+1}: ❌ FAILED (rc={results})", file=sys.stderr)
                continue

            elapsed = end_time - start_time
            put_times.append(elapsed)
            gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / (elapsed * 1e9)
            print(f"  Iter {i+1}: {elapsed:.4f}s ({gbps:.2f} Gbps)")

        avg_put = np.mean(put_times)
        print(f"  -> Avg PUT: {avg_put:.4f}s | {(TOTAL_BATCH_SIZE_BYTES * 8) / (avg_put * 1e9):.2f} Gbps")

        # ----------------------------------------
        # Test 2: batch_get_tensor
        # ----------------------------------------
        print(f"\n--- Test 2: batch_get_tensor ({num_iterations} iters) ---")

        # Ensure data exists
        if not store.is_exist(keys_list[0]):
             store.batch_put_tensor(keys_list, tensors_list)

        get_times = []
        for i in range(num_iterations):
            start_time = time.perf_counter()
            retrieved = store.batch_get_tensor(keys_list)
            end_time = time.perf_counter()

            if len(retrieved) != NUM_TENSORS or retrieved[0] is None:
                print(f"  Iter {i+1}: ❌ FAILED (Data missing)", file=sys.stderr)
                continue

            elapsed = end_time - start_time
            get_times.append(elapsed)
            gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / (elapsed * 1e9)
            print(f"  Iter {i+1}: {elapsed:.4f}s ({gbps:.2f} Gbps)")

        avg_get = np.mean(get_times)
        print(f"  -> Avg GET: {avg_get:.4f}s | {(TOTAL_BATCH_SIZE_BYTES * 8) / (avg_get * 1e9):.2f} Gbps")

        # ----------------------------------------
        # Test 3: TP Awareness (Single Tensor)
        # ----------------------------------------
        print(f"\n--- Test 3: Tensor Parallelism (TP) Awareness ---")
        new_tp_key = "optimized_tp_tensor"
        target_tensor = tensors_list[0]
        tp_size = 4
        split_dim = 1 # Test Column Split this time

        print(f"  Step 1: Calling put_tensor_with_tp(key='{new_tp_key}', tp_size={tp_size}, split_dim={split_dim})")
        start_time = time.perf_counter()

        if not hasattr(store, "put_tensor_with_tp"):
             print("  ❌ Error: store.put_tensor_with_tp not found. Please update C++ library.")
             sys.exit(1)

        rc = store.put_tensor_with_tp(new_tp_key, target_tensor, tp_size=tp_size, split_dim=split_dim)
        if rc != 0:
            print(f"  ❌ put_tensor_with_tp failed with rc={rc}")
            sys.exit(1)
        end_time = time.perf_counter()
        print(f"  ✅ Put Complete. Time: {end_time - start_time:.4f}s")

        print("  Step 2: Verifying underlying shard keys exist...")
        expected_shard_key = f"{new_tp_key}_tp_0"
        if store.is_exist(expected_shard_key):
             print(f"    ✅ Shard key '{expected_shard_key}' found in store.")
        else:
             print(f"    ❌ Shard key '{expected_shard_key}' NOT found! (Logic error in C++ put?)")

        print(f"  Step 3: Retrieving slices using get_tensor_with_tp")
        slices = []
        start_time = time.perf_counter()
        expected_chunks = target_tensor.chunk(tp_size, split_dim)

        for rank in range(tp_size):
            t_slice = store.get_tensor_with_tp(new_tp_key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
            if t_slice is None:
                print(f"    ❌ Rank {rank} failed.")
                sys.exit(1)
            if not torch.equal(t_slice, expected_chunks[rank]):
                print(f"    ❌ Rank {rank} data mismatch!")
                sys.exit(1)
            if not t_slice.is_contiguous():
                print(f"    ❌ Rank {rank} tensor is NOT contiguous!")
            slices.append(t_slice)
        end_time = time.perf_counter()

        reconstructed = torch.cat(slices, dim=split_dim)
        if torch.equal(reconstructed, target_tensor):
            print(f"  ✅ Reconstruction Successful. Time: {end_time - start_time:.4f}s")
        else:
            print(f"  ❌ Reconstruction Data Mismatch!")

        # ----------------------------------------
        # Test 4: Batch TP Awareness
        # ----------------------------------------
        print(f"\n--- Test 4: Batch Tensor Parallelism (TP) Awareness ---")
        batch_tp_keys = [f"batch_tp_{i}" for i in range(NUM_TENSORS)]
        tp_size = 4
        split_dim = 0 # Test Row Split this time

        print(f"  Step 1: Calling batch_put_tensor_with_tp(tp_size={tp_size}, split_dim={split_dim})")
        start_time = time.perf_counter()

        if not hasattr(store, "batch_put_tensor_with_tp"):
             print("  ❌ Error: store.batch_put_tensor_with_tp not found. Please update C++ library.")
             sys.exit(1)

        store.remove_by_regex("batch_tp_.*") # Cleanup before test

        results = store.batch_put_tensor_with_tp(
            batch_tp_keys, tensors_list, tp_size=tp_size, split_dim=split_dim
        )
        if not all(r == 0 for r in results):
            print(f"  ❌ batch_put_tensor_with_tp failed with rc={results}")
            sys.exit(1)
        end_time = time.perf_counter()
        gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / ((end_time - start_time) * 1e9)
        print(f"  ✅ Batch Put with TP Complete. Time: {end_time - start_time:.4f}s ({gbps:.2f} Gbps)")

        print("  Step 2: Verifying underlying shard keys...")
        shard_keys_to_check = [f"{key}_tp_{rank}" for key in batch_tp_keys for rank in range(tp_size)]
        exist_results = store.batch_is_exist(shard_keys_to_check)
        if all(r == 1 for r in exist_results):
            print(f"    ✅ All {len(shard_keys_to_check)} shard keys correctly created.")
        else:
            print(f"    ❌ Mismatch in created shard keys! Check C++ logic.")
            sys.exit(1)

        print(f"  Step 3: Retrieving all slices for all ranks via batch_get_tensor_with_tp")

        if not hasattr(store, "batch_get_tensor_with_tp"):
             print("  ❌ Error: store.batch_get_tensor_with_tp not found. Please update C++ library.")
             sys.exit(1)

        all_retrieved_shards = []
        total_get_time = 0

        for rank in range(tp_size):
            start_time = time.perf_counter()
            # Get all shards for the current rank in one go
            shards_for_rank = store.batch_get_tensor_with_tp(
                batch_tp_keys, tp_rank=rank, tp_size=tp_size
            )
            end_time = time.perf_counter()
            get_time = end_time - start_time
            total_get_time += get_time
            gbps = (TOTAL_BATCH_SIZE_BYTES / tp_size * 8) / (get_time * 1e9)
            print(f"    Rank {rank} get took: {get_time:.4f}s ({gbps:.2f} Gbps)")

            if len(shards_for_rank) != NUM_TENSORS or any(s is None for s in shards_for_rank):
                print(f"    ❌ Rank {rank} batch get failed: received {len(shards_for_rank)} items, some might be None.")
                sys.exit(1)

            all_retrieved_shards.append(shards_for_rank)

        avg_get_gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / (total_get_time * 1e9)
        print(f"  ✅ All ranks retrieved. Total Get Time: {total_get_time:.4f}s, Avg Throughput: {avg_get_gbps:.2f} Gbps")

        print("  Step 4: Verifying correctness and reconstructing...")
        for i in range(NUM_TENSORS):
            original_tensor = tensors_list[i]
            expected_chunks = original_tensor.chunk(tp_size, split_dim)

            # Gather shards for this specific tensor from all ranks
            reconstruction_slices = []
            for rank in range(tp_size):
                retrieved_shard = all_retrieved_shards[rank][i]

                # Verify data against local chunk
                if not torch.equal(retrieved_shard, expected_chunks[rank]):
                    print(f"    ❌ Data mismatch for key '{batch_tp_keys[i]}', rank {rank}!")
                    sys.exit(1)

                reconstruction_slices.append(retrieved_shard)

            # Reconstruct and verify against original
            reconstructed_tensor = torch.cat(reconstruction_slices, dim=split_dim)
            if not torch.equal(reconstructed_tensor, original_tensor):
                print(f"    ❌ Reconstruction failed for key '{batch_tp_keys[i]}'")
                sys.exit(1)

        print("  ✅ Batch TP data verification and reconstruction successful for all tensors.")

        print("\n✅ All Tests Passed.")

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        print("\nCleaning up...")
        if 'store' in locals() and store:
            store.remove_all()
            store.close()
        print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake Benchmark & TP Test")
    parser.add_argument("-n", "--iterations", type=int, default=5, help="Benchmark iterations")
    args = parser.parse_args()

    run_benchmark(args.iterations)