from dataclasses import dataclass
import json
import torch
from mooncake.store import MooncakeDistributedStore
from mooncake.store import ReplicateConfig
import os
import sys
import time
import argparse
import numpy as np

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 400 * 1024 * 1024 * 1024  # 4 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 200 * 1024 * 1024 * 1024 # 2 MB
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_CHECK_SERVER = False

def verify_tensor_equality(original, received, rtol=0, atol=0, verbose=True):
    """
    验证两个张量是否完全一致（逐元素精确比较）。
    
    参数:
        original: 原始张量 (numpy.ndarray 或 torch.Tensor)
        received: 接收到的张量 (numpy.ndarray 或 torch.Tensor)
        rtol: 相对容差（默认 0，要求完全相等）
        atol: 绝对容差（默认 0，要求完全相等）
        verbose: 是否打印详细信息（默认 True）
    
    返回:
        bool: True 表示完全一致，False 表示有差异
    """
    # 转换为 NumPy 数组（处理 PyTorch 张量）
    def to_numpy(x):
        if isinstance(x, torch.Tensor):
            if x.is_cuda:
                x = x.cpu()
            return x.detach().numpy()
        elif isinstance(x, np.ndarray):
            return x
        else:
            raise TypeError(f"Unsupported tensor type: {type(x)}")

    try:
        orig_np = to_numpy(original)
        recv_np = to_numpy(received)
    except Exception as e:
        if verbose:
            print(f"❌ Error converting tensors: {e}")
        return False

    # 检查形状
    if orig_np.shape != recv_np.shape:
        if verbose:
            print(f"❌ Shape mismatch: original {orig_np.shape} vs received {recv_np.shape}")
        return False

    # 检查数据类型
    if orig_np.dtype != recv_np.dtype:
        if verbose:
            print(f"❌ Dtype mismatch: original {orig_np.dtype} vs received {recv_np.dtype}")
        return False

    # 精确比较（逐元素）
    if np.array_equal(orig_np, recv_np):
        if verbose:
            print("✅ Tensors are identical!")
        return True
    else:
        # 找出第一个不一致的位置（用于调试）
        diff_mask = orig_np != recv_np
        diff_indices = np.where(diff_mask)
        if len(diff_indices[0]) > 0:
            first_diff_idx = tuple(idx[0] for idx in diff_indices)
            orig_val = orig_np[first_diff_idx]
            recv_val = recv_np[first_diff_idx]
            if verbose:
                print(f"❌ Tensors differ at index {first_diff_idx}")
                print(f"   Original: {orig_val}")
                print(f"   Received: {recv_val}")
                print(f"   Difference: {abs(orig_val - recv_val)}")
        return False

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

def run_benchmark(num_iterations, num_elements):
    store = MooncakeDistributedStore()

    print("--- Mooncake Tensor Performance Benchmark ---")
    print(f"Configuration:")
    print(f"  Num Elements:      {num_elements}")
    print(f"  Iterations:        {num_iterations}")

    try:
        config = MooncakeStoreConfig.load_from_env()
        print(f"  Hostname:          {config.local_hostname}")
        print(f"  Metadata Server:   {config.metadata_server}")
        print(f"  Master Address:    {config.master_server_address}")
        print(f"  Protocol:          {config.protocol}")

        print("debug: config: ", config)
        rc = store.setup(
            config.local_hostname,
            config.metadata_server,
            config.global_segment_size,
            config.local_buffer_size,
            config.protocol,
            config.device_name,
            config.master_server_address,
        )
        print("debug: store: ", store)
        if rc != 0:
            print(f"Failed to setup mooncake store, error code: {rc}", file=sys.stderr)
            sys.exit(1)
        print("\nMooncake store setup successful.")

        print("Preparing test data (this may take a moment)...")

        key = "perf_tensor_0"
        tensor = torch.randn(num_elements, dtype=torch.float32, device='cpu')
        element_size = tensor.element_size()
        tensor_size_in_bit = 8 * num_elements * element_size

        # ----------------------------------------
        # Test 1: batch_put_tensor
        # ----------------------------------------
        print(f"\n--- Benchmarking batch_put_tensor ({num_iterations} iterations) ---")
        put_times = []
        
        # Test default constructor
        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        print(repconfig)
        for i in range(num_iterations+1):
            store.remove_all()
            print("start to pub")

            start_time = time.perf_counter()
            results = store.pub_tensor(key, tensor, repconfig)
            end_time = time.perf_counter()

            # drop the first one, because it's not stable.
            if i == 0:
                continue

            elapsed_time = end_time - start_time
            put_times.append(elapsed_time)

            # (total_bytes * 8 bits/byte) / (time * 1024^3 Giga) = Gbps
            throughput_gbps = (tensor_size_in_bit * repconfig.replica_num) / (elapsed_time * (1024**3))
            print(f"  Iteration {i+1}: {elapsed_time:.4f} s ({throughput_gbps:.2f} Gbps)")

        if put_times:
            avg_put_time = np.mean(put_times)
            avg_put_throughput = (tensor_size_in_bit * repconfig.replica_num) / (avg_put_time * (1024**3))
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
        results = store.pub_tensor(key, tensor, repconfig)

        get_times = []
        start_time = time.perf_counter()
        retrieved_tensor = store.get_tensor(key)
        end_time = time.perf_counter()

        if retrieved_tensor is None:
            print(f"  Iteration {i+1}: FAILED (Data not retrieved correctly)", file=sys.stderr)

        elapsed_time = end_time - start_time
        get_times.append(elapsed_time)

        throughput_gbps = (tensor_size_in_bit * 8) / (elapsed_time * (1024**3))
        print(f"  Iteration {i+1}: {elapsed_time:.4f} s ({throughput_gbps:.2f} Gbps)")
        verify_tensor_equality(tensor, retrieved_tensor)

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

    elements = [32 * 1024 * 1024, 128 * 1024 * 1024, 512 * 1024 * 1024, 1 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024, 16 * 1024 * 1024 * 1024, 32 * 1024 * 1024 * 1024]
    for element in elements:
        run_benchmark(args.iterations, element)
