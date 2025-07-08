import argparse
import time
import statistics
import logging
import ctypes
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Any
from tqdm import tqdm
import os
from mooncake.store import MooncakeDistributedStore

# Disable memcpy optimization
os.environ["MC_STORE_MEMCPY"] = "0"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('stress_cluster_benchmark')


@dataclass
class BatchResult:
    """Encapsulates the results of a batch operation with error handling."""
    
    keys: List[str]
    return_codes: List[int]
    operation_type: str
    
    def num_succeeded(self) -> int:
        """Return the number of successful operations."""
        if self.operation_type == "prefill":
            # For put operations: 0 = success, non-zero = error
            return sum(1 for code in self.return_codes if code == 0)
        else:
            # For get operations: positive = success (bytes read), negative = error
            return sum(1 for code in self.return_codes if code > 0)
    
    def num_failed(self) -> int:
        """Return the number of failed operations."""
        return len(self.return_codes) - self.num_succeeded()
    
    def get_failed_keys_with_codes(self) -> List[tuple[str, int]]:
        """Return a list of (key, error_code) tuples for failed operations."""
        failed = []
        for i, code in enumerate(self.return_codes):
            is_failed = (code != 0) if self.operation_type == "prefill" else (code < 0)
            if is_failed:
                failed.append((self.keys[i], code))
        return failed
    
    def log_failures(self, max_failures_to_log: int = 10):
        """Log detailed information about failed operations."""
        failed_ops = self.get_failed_keys_with_codes()
        if not failed_ops:
            return
        
        logger.warning(f"Batch {self.operation_type} had {len(failed_ops)} failures:")
        for i, (key, error_code) in enumerate(failed_ops[:max_failures_to_log]):
            logger.warning(f"  {key}: error_code={error_code}")
        
        if len(failed_ops) > max_failures_to_log:
            logger.warning(f"  ... and {len(failed_ops) - max_failures_to_log} more failures")


class PerformanceTracker:
    """Tracks and calculates performance metrics for operations."""

    def __init__(self):
        self.operation_latencies: List[float] = []
        self.operation_sizes: List[int] = []

    def record_operation(self, latency_seconds: float, data_size_bytes: int):
        """Record a single operation's performance."""
        self.operation_latencies.append(latency_seconds)
        self.operation_sizes.append(data_size_bytes)

    def get_statistics(self) -> Dict[str, Any]:
        """Calculate and return comprehensive performance statistics."""
        if not self.operation_latencies:
            return {"error": "No operations recorded"}

        total_time = sum(self.operation_latencies)
        total_operations = len(self.operation_latencies)
        total_bytes = sum(self.operation_sizes)

        # Convert latencies to milliseconds for reporting
        latencies_ms = [lat * 1000 for lat in self.operation_latencies]

        # Calculate percentiles
        p90_latency = statistics.quantiles(latencies_ms, n=10)[8] if len(latencies_ms) >= 10 else max(latencies_ms)
        p99_latency = statistics.quantiles(latencies_ms, n=100)[98] if len(latencies_ms) >= 100 else max(latencies_ms)

        # Calculate throughput metrics
        ops_per_second = total_operations / total_time if total_time > 0 else 0
        bytes_per_second = total_bytes / total_time if total_time > 0 else 0
        mbps = bytes_per_second / (1024 * 1024)  # MB/s

        return {
            "total_operations": total_operations,
            "total_time_seconds": total_time,
            "total_bytes": total_bytes,
            "p90_latency_ms": p90_latency,
            "p99_latency_ms": p99_latency,
            "mean_latency_ms": statistics.mean(latencies_ms),
            "operations_per_second": ops_per_second,
            "throughput_mbps": mbps,
            "throughput_bytes_per_second": bytes_per_second
        }


class TestInstance:
    def __init__(self, args):
        self.args = args
        self.store = None
        self.performance_tracker = PerformanceTracker()
        self.buffer_array = None
        self.buffer_ptr = None

    def setup(self):
        """Initialize the MooncakeDistributedStore and allocate registered memory."""
        self.store = MooncakeDistributedStore()

        # Setup store
        protocol = self.args.protocol
        device_name = self.args.device_name
        local_hostname = self.args.local_hostname
        metadata_server = self.args.metadata_server
        global_segment_size = self.args.global_segment_size * 1024 * 1024
        local_buffer_size = self.args.local_buffer_size * 1024 * 1024
        master_server_address = self.args.master_server

        logger.info(f"Setting up {self.args.role} instance with batch_size={self.args.batch_size}")
        logger.info(f"  Protocol: {protocol}, Device: {device_name}")
        logger.info(f"  Global segment: {global_segment_size // (1024*1024)} MB")
        logger.info(f"  Local buffer: {local_buffer_size // (1024*1024)} MB")

        retcode = self.store.setup(local_hostname, metadata_server, global_segment_size,
                                  local_buffer_size, protocol, device_name, master_server_address)
        if retcode:
            logger.error(f"Store setup failed with return code {retcode}")
            exit(1)

        # Allocate and register memory buffer using numpy for better performance
        buffer_size = self.args.batch_size * self.args.value_length
        self.buffer_array = np.zeros(buffer_size, dtype=np.uint8)
        self.buffer_ptr = self.buffer_array.ctypes.data

        retcode = self.store.register_buffer(self.buffer_ptr, buffer_size)
        if retcode:
            logger.error(f"Buffer registration failed with return code {retcode}")
            exit(1)

        logger.info(f"Allocated and registered {buffer_size // (1024*1024)} MB buffer for zero-copy operations")
        time.sleep(1)

    def _calculate_total_batches(self) -> int:
        """Calculate the total number of batches needed."""
        return (self.args.max_requests + self.args.batch_size - 1) // self.args.batch_size

    def _run_benchmark(self, operation_type: str, operation_func):
        """Generic benchmark runner for both prefill and decode operations."""
        logger.info(f"Starting {operation_type} operations: {self.args.max_requests} requests")
        logger.info(f"Batch size: {self.args.batch_size}, Value size: {self.args.value_length // (1024*1024)} MB")

        total_operations = 0
        total_failed_operations = 0
        total_batches = self._calculate_total_batches()
        
        # Initialize progress bar for batch tracking
        with tqdm(total=total_batches, 
                  desc=f"{operation_type.capitalize()} batches",
                  unit="batch",
                  postfix={"failed_ops": 0}) as pbar:
            
            while total_operations < self.args.max_requests:
                # Calculate batch size for this iteration
                remaining = self.args.max_requests - total_operations
                current_batch_size = min(self.args.batch_size, remaining)
                
                # Prepare batch data
                keys = [f"key{total_operations + i}" for i in range(current_batch_size)]
                
                # Measure batch operation latency
                op_start = time.perf_counter()
                return_codes = operation_func(keys, current_batch_size)
                op_end = time.perf_counter()

                operation_latency = op_end - op_start
                
                # Process results using BatchResult
                batch_result = BatchResult(keys, return_codes, operation_type)
                
                # Log failures if any (but limit verbosity)
                if batch_result.num_failed() > 0:
                    batch_result.log_failures(max_failures_to_log=3)
                
                successful_ops = batch_result.num_succeeded()
                failed_ops = batch_result.num_failed()
                total_failed_operations += failed_ops

                if successful_ops > 0:
                    # Record successful operations
                    total_data_size = successful_ops * self.args.value_length
                    self.performance_tracker.record_operation(operation_latency, total_data_size)

                total_operations += current_batch_size
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix({"failed_ops": total_failed_operations})

        logger.info(f"{operation_type.capitalize()} phase completed. Failed operations: {total_failed_operations}")
        self._print_performance_stats(operation_type.upper())
        
        logger.info(f"Waiting {self.args.wait_time} seconds...")
        time.sleep(self.args.wait_time)

    def prefill(self):
        """Execute prefill operations using zero-copy batch put."""
        def put_batch(keys: List[str], batch_size: int) -> List[int]:
            # Prepare data in the registered buffer using numpy operations
            for i in range(batch_size):
                start_idx = i * self.args.value_length
                end_idx = start_idx + self.args.value_length
                
                # Simple pattern: fill with key index for each key
                key_index = int(keys[i].replace("key", ""))
                pattern = key_index % 256
                self.buffer_array[start_idx:end_idx] = pattern

            # Prepare buffer pointers and sizes for batch operation
            buffer_ptrs = []
            sizes = []
            for i in range(batch_size):
                offset = i * self.args.value_length
                buffer_ptrs.append(self.buffer_ptr + offset)
                sizes.append(self.args.value_length)

            return self.store.batch_put_from(keys, buffer_ptrs, sizes)

        self._run_benchmark("prefill", put_batch)

    def decode(self):
        """Execute decode operations using zero-copy batch get."""
        def get_batch(keys: List[str], batch_size: int) -> List[int]:
            # Prepare buffer pointers and sizes for batch operation
            buffer_ptrs = []
            sizes = []
            for i in range(batch_size):
                offset = i * self.args.value_length
                buffer_ptrs.append(self.buffer_ptr + offset)
                sizes.append(self.args.value_length)

            return self.store.batch_get_into(keys, buffer_ptrs, sizes)

        self._run_benchmark("decode", get_batch)

    def _print_performance_stats(self, operation_type: str):
        """Print comprehensive performance statistics."""
        stats = self.performance_tracker.get_statistics()

        if "error" in stats:
            logger.info(f"No performance data available for {operation_type}: {stats['error']}")
            return

        logger.info(f"\n=== {operation_type} PERFORMANCE STATISTICS ===")
        logger.info(f"Total operations: {stats['total_operations']}")
        logger.info(f"Total time: {stats['total_time_seconds']:.2f} seconds")
        logger.info(f"Total data transferred: {stats['total_bytes'] / (1024*1024):.2f} MB")
        logger.info(f"Latency metrics:")
        logger.info(f"  Mean latency: {stats['mean_latency_ms']:.2f} ms")
        logger.info(f"  P90 latency:  {stats['p90_latency_ms']:.2f} ms")
        logger.info(f"  P99 latency:  {stats['p99_latency_ms']:.2f} ms")
        logger.info(f"Throughput metrics:")
        logger.info(f"  Operations/sec: {stats['operations_per_second']:.2f}")
        logger.info(f"  Throughput:     {stats['throughput_mbps']:.2f} MB/s")
        logger.info(f"===============================================\n")


def parse_arguments():
    """Parse command-line arguments for the stress test."""
    parser = argparse.ArgumentParser(
        description="Mooncake Distributed Store Zero-Copy Batch Benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Role configuration
    parser.add_argument("--role", type=str, choices=["prefill", "decode"], required=True,
                       help="Role of this instance: prefill (producer) or decode (consumer)")

    # Network and connection settings
    parser.add_argument("--protocol", type=str, default="rdma", help="Communication protocol to use")
    parser.add_argument("--device-name", type=str, default="erdma_0", help="Network device name for RDMA")
    parser.add_argument("--local-hostname", type=str, default="localhost", help="Local hostname")
    parser.add_argument("--metadata-server", type=str, default="http://127.0.0.1:8080/metadata", help="Metadata server address")
    parser.add_argument("--master-server", type=str, default="localhost:50051", help="Master server address")

    # Memory and storage settings
    parser.add_argument("--global-segment-size", type=int, default=10000, help="Global segment size in MB")
    parser.add_argument("--local-buffer-size", type=int, default=512, help="Local buffer size in MB")

    # Test parameters
    parser.add_argument("--max-requests", type=int, default=1200, help="Maximum number of requests to process")
    parser.add_argument("--value-length", type=int, default=4*1024*1024, help="Size of each value in bytes")
    parser.add_argument("--batch-size", type=int, default=1, help="Batch size for operations")
    parser.add_argument("--wait-time", type=int, default=20, help="Wait time in seconds after operations complete")

    return parser.parse_args()


def main():
    """Main entry point for the stress test."""
    args = parse_arguments()

    logger.info("=== Mooncake Zero-Copy Batch Benchmark ===")
    logger.info(f"Role: {args.role.upper()}")
    logger.info(f"Protocol: {args.protocol}")
    logger.info(f"Max requests: {args.max_requests}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Value size: {args.value_length // (1024*1024)} MB")
    logger.info("=" * 50)

    try:
        tester = TestInstance(args)
        tester.setup()

        if args.role == "decode":
            tester.decode()
        else:
            tester.prefill()

        logger.info("Test completed successfully!")

    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        raise


if __name__ == '__main__':
    main()
