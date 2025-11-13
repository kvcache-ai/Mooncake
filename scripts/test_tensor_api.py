from dataclasses import dataclass
import json
import torch
from mooncake.store import MooncakeDistributedStore
import os
import sys
import time
import argparse
import numpy as np

TENSOR_SIZE_MB = 32
TOTAL_BATCH_SIZE_GB = 1

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 4 * 1024 * 1024 * 1024  # 4 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 2 * 1024 * 1024 * 1024 # 2 MB
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
                raise ValueError(
                    "Invalid global_segment_size: missing number before 'gb'"
                )
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
            # Zero copy interface does not need local buffer
            local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
            protocol=config.get("protocol", "tcp"),
            device_name=config.get("device_name", ""),
            master_server_address=config.get("master_server_address"),
            master_metrics_port=config.get(
                "master_metrics_port", DEFAULT_MASTER_METRICS_PORT
            ),
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
            # Zero copy interface does not need local buffer
            local_buffer_size=DEFAULT_LOCAL_BUFFER_SIZE,
            protocol=os.getenv("MOONCAKE_PROTOCOL", "tcp"),
            device_name=os.getenv("MOONCAKE_DEVICE", ""),
            master_server_address=os.getenv("MOONCAKE_MASTER"),
            master_metrics_port=int(
                os.getenv("MOONCAKE_MASTER_METRICS_PORT", DEFAULT_MASTER_METRICS_PORT)
            ),
            check_server=bool(os.getenv("MOONCAKE_CHECK_SERVER", DEFAULT_CHECK_SERVER)),
        )

def run_benchmark(num_iterations):
    store = MooncakeDistributedStore()

    print("--- Mooncake Tensor Performance Benchmark ---")
    print(f"Configuration:")
    print(f"  Tensor Size:       {TENSOR_SIZE_MB} MB")
    print(f"  Total Batch Size:  {TOTAL_BATCH_SIZE_GB} GB")
    print(f"  Tensors per Batch: {NUM_TENSORS} (1024MB / 32MB)")
    print(f"  Iterations:        {num_iterations}")

    try:
        config = MooncakeStoreConfig.load_from_env()
        print(f"  Hostname:          {config.local_hostname}")
        print(f"  Metadata Server:   {config.metadata_server}")
        print(f"  Master Address:    {config.master_server_address}")
        print(f"  Protocol:          {config.protocol}")

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
            print(f"Failed to setup mooncake store, error code: {rc}", file=sys.stderr)
            sys.exit(1)
        print("\nMooncake store setup successful.")

        print("Preparing test data (this may take a moment)...")
        elements_per_tensor = TENSOR_SIZE_BYTES // 4

        tensors_list = [
            torch.randn(elements_per_tensor, dtype=torch.float32)
            for _ in range(NUM_TENSORS)
        ]
        keys_list = [f"perf_tensor_{i}" for i in range(NUM_TENSORS)]
        print(f"Data prepared: {NUM_TENSORS} tensors, {TENSOR_SIZE_MB} MB each.")

        # ----------------------------------------
        # Test 1: batch_put_tensor
        # ----------------------------------------
        print(f"\n--- Benchmarking batch_put_tensor ({num_iterations} iterations) ---")
        put_times = []
        for i in range(num_iterations):
            store.remove_all()

            start_time = time.perf_counter()
            results = store.batch_put_tensor(keys_list, tensors_list)
            end_time = time.perf_counter()

            if not all(r == 0 for r in results):
                print(f"  Iteration {i+1}: FAILED (rc={results})", file=sys.stderr)
                continue

            elapsed_time = end_time - start_time
            put_times.append(elapsed_time)

            # (total_bytes * 8 bits/byte) / (time * 1024^3 Giga) = Gbps
            throughput_gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / (elapsed_time * (1024**3))
            print(f"  Iteration {i+1}: {elapsed_time:.4f} s ({throughput_gbps:.2f} Gbps)")

        if put_times:
            avg_put_time = np.mean(put_times)
            avg_put_throughput = (TOTAL_BATCH_SIZE_BYTES * 8) / (avg_put_time * (1024**3))
            print(f"Average PUT Time: {avg_put_time:.4f} s")
            print(f"Average PUT Throughput: {avg_put_throughput:.2f} Gbps")
        else:
            print("PUT test failed to complete.")

        # ----------------------------------------
        # Test 2: batch_get_tensor
        # ----------------------------------------
        print(f"\n--- Benchmarking batch_get_tensor ({num_iterations} iterations) ---")

        print("  (Pre-populating data for GET test...)")
        store.remove_all()
        rc = store.batch_put_tensor(keys_list, tensors_list)
        if not all(r == 0 for r in rc):
             print("  Failed to pre-populate data for GET test!", file=sys.stderr)
             sys.exit(1)

        get_times = []
        for i in range(num_iterations):
            start_time = time.perf_counter()
            retrieved_tensors = store.batch_get_tensor(keys_list)
            end_time = time.perf_counter()

            if len(retrieved_tensors) != NUM_TENSORS or retrieved_tensors[0] is None:
                print(f"  Iteration {i+1}: FAILED (Data not retrieved correctly)", file=sys.stderr)
                continue

            elapsed_time = end_time - start_time
            get_times.append(elapsed_time)

            throughput_gbps = (TOTAL_BATCH_SIZE_BYTES * 8) / (elapsed_time * (1024**3))
            print(f"  Iteration {i+1}: {elapsed_time:.4f} s ({throughput_gbps:.2f} Gbps)")

        if get_times:
            avg_get_time = np.mean(get_times)
            avg_get_throughput = (TOTAL_BATCH_SIZE_BYTES * 8) / (avg_get_time * (1024**3))
            print(f"Average GET Time: {avg_get_time:.4f} s")
            print(f"Average GET Throughput: {avg_get_throughput:.2f} Gbps")
        else:
            print("GET test failed to complete.")

        print("\n✅ Benchmark finished.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        print("Cleaning up and closing store...")
        store.remove_all()
        store.close()
        print("Store closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake Tensor API Performance Benchmark")
    parser.add_argument(
        "-n", "--iterations",
        type=int,
        default=5,
        help="Number of iterations for each test (default: 5)"
    )
    args = parser.parse_args()

    run_benchmark(args.iterations)