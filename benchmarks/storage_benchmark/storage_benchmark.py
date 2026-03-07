#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""
Mooncake KVCache Storage Benchmark Tool
"""

import argparse
import json
import time
import os
import statistics
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

# ============================================================================
# Constants
# ============================================================================

BLOCK_SIZE_TOKENS = 512  # Number of tokens per block
DEFAULT_BYTES_PER_TOKEN = 2048  # 7B model FP16 (2KB per token)
BLOCK_SIZE_BYTES = BLOCK_SIZE_TOKENS * DEFAULT_BYTES_PER_TOKEN  # 1MB per block
MIN_LATENCY_MS = 0.001  # Minimum latency in milliseconds (1 microsecond)

# Model KVCache sizes (bytes per token, based on LMCache calculator)
# Source: https://lmcache.ai/kv_cache_calculator.html
MODEL_BYTES_PER_TOKEN = {
    # Small models (7B-13B)
    "llama-2-7b": 512,
    "llama-2-13b": 800,
    "llama-3-8b": 128,
    "mistral-7b": 128,
    "qwen-14b": 40,
    "gemma-7b": 224,

    # Large models (70B-405B)
    "llama-2-70b": 320,
    "llama-3-70b": 320,
    "llama-3.1-405b": 516018,  # ~504 KB/token
    "mixtral-8x7b": 128,
    "mixtral-8x22b": 224,
    "qwen-72b": 320,
    "qwen-110b": 320,

    # Extra large models
    "deepseek-v3": 1749384,  # ~1.67 MB/token
    "glm-4.6": 156991,  # ~153 KB/token

    # Legacy/default
    "default": DEFAULT_BYTES_PER_TOKEN,
}

# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class KVCacheRequest:
    """KVCache request

    Attributes:
        timestamp: Request timestamp in milliseconds
        hash_ids: List of block IDs (each ID corresponds to a 512-token block)
        input_length: Input token count
        output_length: Output token count
    """
    timestamp: float
    hash_ids: List[int]
    input_length: int
    output_length: int

# ============================================================================
# Storage Layer: Offset Allocator
# ============================================================================

class OffsetAllocatorStorage:
    """High-performance block storage based on Offset Allocator

    Architecture:
    -----------
    1. Single large file stores all blocks (avoids file explosion)
    2. Uses offset to manage file space (similar to Mooncake's OffsetAllocator)
    3. hash_id -> offset mapping stored in memory (fast lookup)

    Block Organization:
    -----------
    Each block corresponds to 512 tokens, fixed size 1MB:
    - hash_id[0] -> block_0 (tokens [0...511])     -> offset 0
    - hash_id[1] -> block_1 (tokens [512...1023])  -> offset 1
    - hash_id[i] -> block_i (tokens [i*512...(i+1)*512-1]) -> offset i

    Performance Advantages:
    -----------
    - Only one file, no file explosion
    - Offset reuse, reduces memory allocation
    - pread/pwrite, thread-safe, no seek needed
    - Keep fd open, reduces open/close overhead
    - Metadata in memory, O(1) lookup

    Attributes:
        storage_dir: Storage directory path
        block_size_bytes: Block size in bytes
        max_blocks: Maximum number of blocks
        hash_id_to_offset: hash_id -> offset mapping
        free_offsets: List of reusable offsets
        next_offset: Next allocatable offset
    """

    def __init__(self, storage_dir: str, bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN,
                 max_blocks: int = 100000):
        """Initialize Offset Allocator storage

        Args:
            storage_dir: Storage directory path
            bytes_per_token: Bytes per token
            max_blocks: Maximum number of blocks (determines file size)
        """
        self.storage_dir = Path(storage_dir)
        self.bytes_per_token = bytes_per_token
        self.block_size_tokens = BLOCK_SIZE_TOKENS
        self.block_size_bytes = self.block_size_tokens * self.bytes_per_token
        self.max_blocks = max_blocks

        # Create storage directory
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # Single large file
        self.storage_file = self.storage_dir / "kvcache_storage.bin"
        self.file_size = self.max_blocks * self.block_size_bytes

        # Initialize storage file
        if not self.storage_file.exists():
            self._init_storage_file()

        # hash_id -> offset mapping (metadata, in memory)
        self.hash_id_to_offset: Dict[int, int] = {}

        # Offset allocator (free list)
        self.free_offsets: List[int] = []
        self.next_offset = 0

        # File descriptor (keep open, avoid repeated open/close)
        self.fd = None

        # Statistics
        self.stats = {
            'read_count': 0,
            'write_count': 0,
            'read_bytes': 0,
            'write_bytes': 0,
            'read_latencies_ms': [],
            'write_latencies_ms': [],
        }

    # ========================================================================
    # Internal Methods
    # ========================================================================

    def _init_storage_file(self):
        """Initialize storage file (pre-allocate space)

        Create sparse file to avoid actual disk space usage until data is written
        """
        with open(self.storage_file, 'wb') as f:
            f.seek(self.file_size - 1)
            f.write(b'\0')
            f.flush()
            os.fsync(f.fileno())

    def _get_fd(self):
        """Get file descriptor (lazy open)

        Returns:
            int: File descriptor
        """
        if self.fd is None:
            # Use O_RDWR | O_CREAT, no O_DIRECT (Python compatibility)
            self.fd = os.open(self.storage_file, os.O_RDWR | os.O_CREAT)
        return self.fd

    def _allocate_offset(self) -> int:
        """Allocate a new offset

        Prioritize reusing freed offsets, otherwise allocate new offset

        Returns:
            int: Allocated offset
        """
        if self.free_offsets:
            return self.free_offsets.pop()
        offset = self.next_offset
        self.next_offset += 1
        return offset

    def _free_offset(self, offset: int):
        """Free offset for reuse

        Args:
            offset: Offset to free
        """
        self.free_offsets.append(offset)

    # ========================================================================
    # Public Interface
    # ========================================================================

    def block_exists(self, hash_id: int) -> bool:
        """Check if block exists

        Args:
            hash_id: Unique block identifier

        Returns:
            bool: Whether block exists
        """
        return hash_id in self.hash_id_to_offset

    def read_block(self, hash_id: int) -> float:
        """Read block using pread

        Args:
            hash_id: Unique block identifier

        Returns:
            float: Read latency in milliseconds
        """
        if hash_id not in self.hash_id_to_offset:
            return MIN_LATENCY_MS

        offset = self.hash_id_to_offset[hash_id]
        file_offset = offset * self.block_size_bytes

        start = time.time()

        try:
            fd = self._get_fd()
            data = os.pread(fd, self.block_size_bytes, file_offset)
            latency_ms = (time.time() - start) * 1000.0

            self.stats['read_count'] += 1
            self.stats['read_bytes'] += len(data)
            self.stats['read_latencies_ms'].append(latency_ms)
            return latency_ms
        except OSError as e:
            print(f"Error reading block {hash_id} at offset {file_offset}: {e}")
            return MIN_LATENCY_MS

    def write_block(self, hash_id: int) -> float:
        """Write block using pwrite

        Args:
            hash_id: Unique block identifier

        Returns:
            float: Write latency in milliseconds
        """
        # Allocate offset
        offset = self._allocate_offset()
        file_offset = offset * self.block_size_bytes

        start = time.time()

        # Generate simulated data
        data = os.urandom(self.block_size_bytes)

        try:
            fd = self._get_fd()
            written = os.pwrite(fd, data, file_offset)

            # Ensure data persistence (fsync)
            os.fsync(fd)

            latency_ms = (time.time() - start) * 1000.0

            # Update mapping
            self.hash_id_to_offset[hash_id] = offset

            self.stats['write_count'] += 1
            self.stats['write_bytes'] += written
            self.stats['write_latencies_ms'].append(latency_ms)
            return latency_ms
        except OSError as e:
            print(f"Error writing block {hash_id} at offset {file_offset}: {e}")
            return MIN_LATENCY_MS

    def close(self):
        """Close file"""
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def get_stats(self) -> Dict:
        """Get statistics

        Returns:
            Dict: Dictionary containing read/write statistics
        """
        def calc_stats(latencies):
            """Calculate latency statistics"""
            if not latencies:
                return {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}
            return {
                'avg_ms': statistics.mean(latencies),
                **calc_percentiles(latencies),
            }

        return {
            'read': {
                'count': self.stats['read_count'],
                'mb': self.stats['read_bytes'] / 1024 / 1024,
                **calc_stats(self.stats['read_latencies_ms'])
            },
            'write': {
                'count': self.stats['write_count'],
                'mb': self.stats['write_bytes'] / 1024 / 1024,
                **calc_stats(self.stats['write_latencies_ms'])
            },
            'total_blocks': len(self.hash_id_to_offset),
            'free_blocks': len(self.free_offsets),
        }


# ============================================================================
# Benchmark Layer
# ============================================================================

class StorageBenchmark:
    """KVCache storage benchmark

    Based on Mooncake OffsetAllocator + vLLM PagedAttention implementation:

    Example:
    -----
    Request A: [1, 2, 4]
    -> hash_id 1 -> not exist, write block_1 (offset=0, 1MB)
    -> hash_id 2 -> not exist, write block_2 (offset=1, 1MB)
    -> hash_id 4 -> not exist, write block_4 (offset=2, 1MB)

    Request B: [1, 2, 4, 6]
    -> hash_id 1 -> exists, read block_1 (offset=0) ✓ prefix reuse
    -> hash_id 2 -> exists, read block_2 (offset=1) ✓ prefix reuse
    -> hash_id 4 -> exists, read block_4 (offset=2) ✓ prefix reuse
    -> hash_id 6 -> not exist, write block_6 (offset=3, 1MB)

    Performance Advantages:
    ---------
    - Single file operation, no file explosion
    - Offset reuse, reduces memory allocation
    - pread/pwrite, thread-safe
    """

    def __init__(self, storage_dir: str, bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN,
                 max_blocks: int = 100000):
        """Initialize benchmark

        Args:
            storage_dir: Storage directory
            bytes_per_token: Bytes per token
            max_blocks: Maximum number of blocks
        """
        self.storage = OffsetAllocatorStorage(storage_dir, bytes_per_token, max_blocks)
        self.bytes_per_token = bytes_per_token

        # Statistics
        self.stats = {
            'total_requests': 0,
            'total_blocks': 0,
            'read_blocks': 0,
            'write_blocks': 0,
            'prefix_hit_blocks': 0,  # Number of prefix hit blocks
            'request_latencies_ms': [],
        }

    def process_request(self, req: KVCacheRequest) -> float:
        """Process a KVCache request

        Based on vLLM's prefix caching mechanism:
        - Each hash_id corresponds to an independent block
        - Prefix reuse achieved through hash_id matching

        Args:
            req: KVCache request

        Returns:
            float: Request latency in milliseconds
        """
        self.stats['total_requests'] += 1
        self.stats['total_blocks'] += len(req.hash_ids)

        start_time = time.time()
        total_latency = 0.0

        # Process each hash_id (in order)
        for hash_id in req.hash_ids:
            if self.storage.block_exists(hash_id):
                # Block exists, read (reuse cached block)
                total_latency += self.storage.read_block(hash_id)
                self.stats['read_blocks'] += 1
                self.stats['prefix_hit_blocks'] += 1  # Count all cache hits as prefix reuse
            else:
                # Block doesn't exist, write (new block)
                total_latency += self.storage.write_block(hash_id)
                self.stats['write_blocks'] += 1

        latency_ms = total_latency if total_latency > 0 else MIN_LATENCY_MS
        self.stats['request_latencies_ms'].append(latency_ms)

        return latency_ms

    def get_stats(self) -> Dict:
        """Get statistics

        Returns:
            Dict: Statistics dictionary
        """
        storage_stats = self.storage.get_stats()

        request_latencies = self.stats['request_latencies_ms']

        if request_latencies:
            latency_stats = {
                'avg_ms': statistics.mean(request_latencies),
                **calc_percentiles(request_latencies),
            }
        else:
            latency_stats = {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}

        total_blocks = self.stats['total_blocks']
        read_blocks = self.stats['read_blocks']
        write_blocks = self.stats['write_blocks']

        return {
            'total_requests': self.stats['total_requests'],
            'total_blocks': total_blocks,
            'read_blocks': read_blocks,
            'write_blocks': write_blocks,
            'prefix_hit_blocks': self.stats['prefix_hit_blocks'],
            'block_hit_rate': read_blocks / total_blocks if total_blocks > 0 else 0,
            'write_ratio': write_blocks / total_blocks if total_blocks > 0 else 0,
            'tokens_per_block': BLOCK_SIZE_TOKENS,  # Fixed block size in tokens
            'latency': latency_stats,
            'storage': storage_stats,
        }

    def close(self):
        """Close storage"""
        self.storage.close()


# ============================================================================
# Utility Functions
# ============================================================================

def calc_percentiles(data: List[float]) -> Dict[str, float]:
    """Calculate latency percentiles

    Uses linear interpolation for accurate percentile calculation.
    This is more accurate than statistics.quantiles() for small datasets.

    Args:
        data: List of latency values in milliseconds

    Returns:
        Dict containing p50, p95, p99 percentiles
    """
    if not data:
        return {'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}

    # Sort data for percentile calculation
    sorted_data = sorted(data)
    n = len(sorted_data)

    def get_percentile(p: float) -> float:
        """Get percentile using linear interpolation

        Args:
            p: Percentile (0-100)

        Returns:
            Value at percentile
        """
        index = (n - 1) * p / 100
        lower = int(index)
        upper = min(lower + 1, n - 1)

        if lower == upper:
            return sorted_data[lower]

        # Linear interpolation
        weight = index - lower
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight

    return {
        'p50_ms': get_percentile(50),
        'p95_ms': get_percentile(95),
        'p99_ms': get_percentile(99),
    }


# ============================================================================
# Trace Loader
# ============================================================================

class TraceLoader:
    """Load KVCache trace"""

    def __init__(self, trace_path: str):
        """Initialize trace loader

        Args:
            trace_path: Trace file path
        """
        self.trace_path = trace_path
        self.requests = []
        self._load_trace()

    def _load_trace(self):
        """Load trace file"""
        with open(self.trace_path, 'r') as f:
            for line in f:
                req = json.loads(line.strip())
                self.requests.append(KVCacheRequest(
                    timestamp=req['timestamp'],
                    hash_ids=req['hash_ids'],
                    input_length=req['input_length'],
                    output_length=req['output_length']
                ))

    def get_requests(self) -> List[KVCacheRequest]:
        """Get request list

        Returns:
            List[KVCacheRequest]: Request list
        """
        return self.requests


# ============================================================================
# Benchmark Runner
# ============================================================================

def run_benchmark(trace_path: str, storage_dir: str, bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN,
                   max_requests: Optional[int] = None, max_blocks: int = 100000,
                   replay_timestamps: bool = False, time_scale: float = 1.0) -> Dict:
    """Run benchmark

    Args:
        trace_path: Trace file path
        storage_dir: Storage directory
        bytes_per_token: Bytes per token
        max_requests: Maximum number of requests (None = all)
        max_blocks: Maximum number of blocks
        replay_timestamps: Whether to replay timestamps from trace (simulate realistic timing)
        time_scale: Time scaling factor (1.0=real-time, 0.1=10x speed, 10.0=0.1x speed)

    Returns:
        Dict: Benchmark results
    """
    print(f"\n{'='*80}")
    print(f"Running: {Path(trace_path).name}")
    print(f"Architecture: Offset Allocator (Mooncake style)")
    print(f"Block size: {BLOCK_SIZE_TOKENS} tokens/block (fixed, {BLOCK_SIZE_BYTES} bytes)")
    print(f"Storage: Single large file with offset-based block management")
    print(f"Bytes per token: {bytes_per_token}")
    print(f"Max blocks: {max_blocks}")
    print(f"Timestamp replay: {'Enabled' if replay_timestamps else 'Disabled'}")
    if replay_timestamps:
        scale_desc = 'real-time' if time_scale == 1.0 else f'{1/time_scale:.1f}x speed' if time_scale < 1.0 else f'{time_scale}x slower'
        print(f"Time scale: {time_scale}x ({scale_desc})")
    print(f"{'='*80}")

    # Load trace
    loader = TraceLoader(trace_path)
    requests = loader.get_requests()

    if max_requests:
        requests = requests[:max_requests]

    print(f"Loaded {len(requests)} requests")

    # Show timestamp range
    if replay_timestamps and requests:
        timestamps = [req.timestamp for req in requests]
        time_span_ms = max(timestamps) - min(timestamps)
        print(f"Timestamp range: {min(timestamps):.1f} - {max(timestamps):.1f} ms (span: {time_span_ms:.1f} ms)")

    # Create benchmark instance
    benchmark = StorageBenchmark(storage_dir, bytes_per_token, max_blocks)

    # Run benchmark
    start_time = time.time()
    total_io_time = 0.0  # Actual I/O time (excluding sleep)
    last_timestamp = None
    base_time = time.time()

    for i, req in enumerate(requests):
        # Replay by timestamps
        sleep_time = 0.0
        if replay_timestamps and last_timestamp is not None:
            # Calculate time interval from previous request
            delta_ms = req.timestamp - last_timestamp
            sleep_time = delta_ms / 1000.0 / time_scale  # Apply time scaling

            if sleep_time > 0:
                time.sleep(sleep_time)

        # Process request (measure I/O time)
        req_start = time.time()
        benchmark.process_request(req)
        req_io_time = time.time() - req_start
        total_io_time += req_io_time

        # Record current request timestamp
        last_timestamp = req.timestamp

        # Progress output
        if (i + 1) % 100 == 0:
            if replay_timestamps:
                elapsed_wall_time = time.time() - base_time
                simulated_time = (req.timestamp - requests[0].timestamp) / 1000.0 / time_scale
                print(f"  Processed {i + 1}/{len(requests)}... (wall: {elapsed_wall_time:.1f}s, simulated: {simulated_time:.1f}s, io: {total_io_time:.1f}s)")
            else:
                print(f"  Processed {i + 1}/{len(requests)}...")

    elapsed = time.time() - start_time

    # Get statistics
    stats = benchmark.get_stats()
    benchmark.close()

    # Calculate actual I/O time (excluding sleep)
    io_time = total_io_time if replay_timestamps else elapsed

    return {
        'trace_file': Path(trace_path).name,
        'total_requests': len(requests),
        'simulation_time_s': elapsed,
        'io_time_s': io_time,  # Actual I/O time
        'wall_time_s': elapsed,  # Wall time (including sleep)
        'requests_per_second': len(requests) / io_time if io_time > 0 else 0,  # Based on I/O time
        'timestamp_replay_enabled': replay_timestamps,
        'time_scale': time_scale,
        **stats,
    }


# ============================================================================
# Result Output
# ============================================================================

def print_results(results: List[Dict]):
    """Print benchmark results

    Args:
        results: List of benchmark results
    """
    for i, r in enumerate(results, 1):
        print(f"\n{'='*80}")
        print(f"  [{i}/{len(results)}] {r['trace_file']}")
        print(f"{'='*80}")

        print(f"\n[Performance Overview]")
        print(f"  Total Requests:           {r['total_requests']:,}")
        print(f"  Queries Per Second (QPS): {r['requests_per_second']:.2f}")
        print(f"  Cache Hit Rate:           {r['block_hit_rate']:.2%}")
        print(f"  Write Ratio:              {r['write_ratio']:.2%}")
        print(f"  Total Blocks:             {r['total_blocks']:,}")
        print(f"    Read Blocks:            {r['read_blocks']:,}")
        print(f"    Write Blocks:           {r['write_blocks']:,}")
        print(f"    Prefix Hits:            {r['prefix_hit_blocks']:,}")

        print(f"\n[Latency Analysis]")
        req_lat = r['latency']
        print(f"  Request Latency (End-to-End): Avg={req_lat['avg_ms']:.2f}ms, P50={req_lat['p50_ms']:.2f}ms, P95={req_lat['p95_ms']:.2f}ms, P99={req_lat['p99_ms']:.2f}ms")
        read_lat = r['storage']['read']
        write_lat = r['storage']['write']
        print(f"  Single I/O Operation (Per Block):")
        print(f"    Read:  Avg={read_lat.get('avg_ms', 0):.3f}ms, P50={read_lat.get('p50_ms', 0):.3f}ms, P95={read_lat.get('p95_ms', 0):.3f}ms, P99={read_lat.get('p99_ms', 0):.3f}ms")
        print(f"    Write: Avg={write_lat.get('avg_ms', 0):.3f}ms, P50={write_lat.get('p50_ms', 0):.3f}ms, P95={write_lat.get('p95_ms', 0):.3f}ms, P99={write_lat.get('p99_ms', 0):.3f}ms")

        print(f"\n[I/O & Bandwidth]")
        print(f"  Total Read I/O:      {r['storage']['read']['mb']:>10.1f} MB  ({r['storage']['read']['count']:,} ops)")
        print(f"  Total Write I/O:     {r['storage']['write']['mb']:>10.1f} MB  ({r['storage']['write']['count']:,} ops)")
        io_time = r['io_time_s']
        bandwidth = (r['storage']['read']['mb'] + r['storage']['write']['mb']) / io_time
        print(f"  Effective Bandwidth:   {bandwidth:>10.1f} MB/s")

        print(f"\n[Storage Details]")
        print(f"  Blocks in Use:       {r['storage']['total_blocks']:>10,}")
        print(f"  Free Blocks:         {r['storage']['free_blocks']:>10,}")
        print(f"  Tokens per Block:    {r['tokens_per_block']:>10,}")
        print(f"  Block Size:          {r['tokens_per_block'] * 2048 / 1024 / 1024:>10.2f} MB")

        print(f"\n[Execution Time]")
        if r.get('timestamp_replay_enabled'):
            print(f"  Wall Time (Total):     {r['wall_time_s']:>10.2f} s")
            print(f"  I/O Time (Actual):     {r['io_time_s']:>10.2f} s")
            print(f"  Sleep Time (Replay):   {r['wall_time_s'] - r['io_time_s']:>10.2f} s")
        else:
            print(f"  Total Execution Time: {r['wall_time_s']:>10.2f} s")

    print(f"\n{'='*80}\n")


# ============================================================================
# Main Program
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Mooncake KVCache Storage Benchmark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick test (100 requests)
  python storage_benchmark.py --scenario=toolagent --max-requests=100

  # Test with large model preset (Llama-3.1-405B)
  python storage_benchmark.py --scenario=toolagent --model=llama-3.1-405b --max-requests=100

  # Test with Deepseek V3 (extra large model)
  python storage_benchmark.py --scenario=toolagent --model=deepseek-v3 --max-requests=100

  # Realistic replay (with timestamps, 10x speed)
  python storage_benchmark.py --scenario=toolagent --max-requests=1000 \\
      --replay-timestamps --time-scale=0.1

  # All scenarios with custom bytes_per_token
  python storage_benchmark.py --scenario=all --bytes-per-token=512

Available model presets:
  Small models (7B-13B): llama-2-7b, llama-2-13b, llama-3-8b, mistral-7b, qwen-14b, gemma-7b
  Large models (70B-405B): llama-2-70b, llama-3-70b, llama-3.1-405b, mixtral-8x7b, mixtral-8x22b, qwen-72b, qwen-110b
  Extra large models: deepseek-v3, glm-4.6

For more information: tools/STORAGE_BENCHMARK_README.md
        """
    )

    parser.add_argument('--trace-dir', type=str, default='../../FAST25-release/traces',
                       help='Trace files directory')
    parser.add_argument('--scenario', type=str, choices=['conversation', 'synthetic', 'toolagent', 'all'],
                       default='toolagent', help='Test scenario')
    parser.add_argument('--storage-dir', type=str, default='/tmp/mooncake_bench',
                       help='Storage directory')
    parser.add_argument('--model', type=str, choices=list(MODEL_BYTES_PER_TOKEN.keys()),
                       default='default',
                       help=f'Model preset (overrides --bytes-per-token). Available: {", ".join(MODEL_BYTES_PER_TOKEN.keys())}')
    parser.add_argument('--bytes-per-token', type=int, default=DEFAULT_BYTES_PER_TOKEN,
                       help='Bytes per token (default %d, overridden by --model if specified)' % DEFAULT_BYTES_PER_TOKEN)
    parser.add_argument('--max-requests', type=int, default=None,
                       help='Maximum number of requests (default: unlimited)')
    parser.add_argument('--max-blocks', type=int, default=100000,
                       help='Maximum number of blocks in storage file (determines file size)')
    parser.add_argument('--replay-timestamps', action='store_true',
                       help='Enable timestamp replay (simulate realistic request timing)')
    parser.add_argument('--time-scale', type=float, default=1.0,
                       help='Time scaling factor (1.0=real-time, 0.1=10x speed, 10.0=0.1x speed)')

    args = parser.parse_args()

    # Print benchmark header
    print(f"\n{'='*80}")
    print(f"{'Mooncake KVCache Storage Benchmark':^80}")
    print(f"{'='*80}")

    # Determine bytes_per_token (model preset takes precedence)
    bytes_per_token = MODEL_BYTES_PER_TOKEN.get(args.model, args.bytes_per_token)
    if args.model != 'default':
        print(f"Using model preset: {args.model} ({bytes_per_token} bytes/token, ~{bytes_per_token/1024:.1f} KB/token)")
    else:
        print(f"Using custom bytes_per_token: {bytes_per_token}")

    # Determine test scenarios
    scenarios = ['conversation', 'synthetic', 'toolagent'] if args.scenario == 'all' else [args.scenario]
    trace_files = {
        'conversation': 'conversation_trace.jsonl',
        'synthetic': 'synthetic_trace.jsonl',
        'toolagent': 'toolagent_trace.jsonl'
    }

    # Run benchmarks
    results = []
    for scenario in scenarios:
        trace_path = Path(args.trace_dir) / trace_files[scenario]
        if trace_path.exists():
            result = run_benchmark(
                str(trace_path),
                str(Path(args.storage_dir) / scenario),
                bytes_per_token,
                args.max_requests,
                args.max_blocks,
                args.replay_timestamps,
                args.time_scale
            )
            results.append(result)
        else:
            print(f"Warning: Trace file not found: {trace_path}")

    # Print results
    if results:
        print_results(results)


if __name__ == '__main__':
    main()
