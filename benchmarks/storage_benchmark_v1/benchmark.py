#!/usr/bin/env python3
"""
Mooncake KVCache Storage Benchmark

Complete benchmark tool with CLI interface.
"""

import argparse
import json
import os
import sys
import time
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Dict, Any

from storage import SSDStorage
from layout import KVLayout, get_model_config, create_layout


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class KVCacheRequest:
    """KVCache request"""
    timestamp: float
    hash_ids: List[int]
    input_length: int
    output_length: int


# ============================================================================
# Trace Replay
# ============================================================================

class TraceReplay:
    """Trace replay handler"""

    def __init__(self, trace_path: str):
        self.trace_path = trace_path

    def replay(self) -> Iterator[dict]:
        """Replay trace workload"""
        with open(self.trace_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def load_all(self) -> List[KVCacheRequest]:
        """Load all requests"""
        return [
            KVCacheRequest(
                timestamp=req.get('timestamp', 0),
                hash_ids=req.get('hash_ids', []),
                input_length=req.get('input_length', 0),
                output_length=req.get('output_length', 0),
            )
            for req in self.replay()
        ]


# ============================================================================
# Storage Benchmark
# ============================================================================

class StorageBenchmark:
    """KVCache storage benchmark

    Uses KVLayout interface to support different architectures.
    - MLA: Each (sequence_id, layer_id) pair maps to ONE complete entry
    - Future: Other architectures can be added by implementing KVLayout
    """

    def __init__(self, storage_dir: str, model_config: dict = None,
                 layout: KVLayout = None,
                 page_size_tokens: int = 64,
                 max_pages: int = 100000,
                 fsync_mode: str = 'none', fsync_batch_size: int = 100):
        """Initialize benchmark

        Args:
            storage_dir: Directory for storage files
            model_config: Model configuration dict (used if layout not provided)
            layout: KVLayout instance (overrides model_config if provided)
            page_size_tokens: Tokens per page (default 64)
            max_pages: Maximum number of pages to store
            fsync_mode: When to fsync ('none', 'batch', 'always', 'end')
            fsync_batch_size: Number of writes between fsync in batch mode
        """
        if layout is None:
            if model_config is None:
                # Use default model config
                model_config = get_model_config("glm5")
            self.model_config = model_config
            self.layout = create_layout(model_config, page_size_tokens)
        else:
            self.layout = layout
            self.model_config = model_config or {}

        # Get layout metadata
        self.num_layers = self._get_num_layers()
        self.has_indexer = self._get_has_indexer()

        # Get page size from layout
        # All KVLayout implementations must provide value_size_bytes
        self.page_size_bytes = self.layout.value_size_bytes

        # Initialize storage
        self.storage = SSDStorage(
            storage_dir=storage_dir,
            page_size=self.page_size_bytes,
            max_pages=max_pages,
            fsync_mode=fsync_mode,
            fsync_batch_size=fsync_batch_size
        )

        # Statistics
        self.stats = {
            'total_requests': 0,
            'total_tokens': 0,
            'read_pages': 0,
            'write_pages': 0,
            'page_hits': 0,
            'request_latencies_ms': [],
        }

    def _get_num_layers(self) -> int:
        """Get number of layers from layout or config"""
        # For MLA layout, we can get this from the layout instance
        if hasattr(self.layout, 'num_layers'):
            return self.layout.num_layers
        return self.model_config.get('num_layers', 0)

    def _get_has_indexer(self) -> bool:
        """Check if layout uses indexer"""
        if hasattr(self.layout, 'index_head_dim'):
            return self.layout.index_head_dim > 0
        return self.model_config.get('index_head_dim', 0) > 0

    def process_request(self, req: KVCacheRequest) -> float:
        """Process a KVCache request using the layout interface"""
        self.stats['total_requests'] += 1

        total_tokens = req.input_length + req.output_length
        self.stats['total_tokens'] += total_tokens

        start_time = time.perf_counter()
        total_latency = 0.0
        io_operations = 0

        # Use layout to generate keys for this request
        for entry in self.layout.get_entries(req):
            key = entry.key
            if self.storage.exists(key):
                start_io = time.perf_counter()
                val = self.storage.get(key)
                latency = (time.perf_counter() - start_io) * 1000.0
                if val is not None:
                    total_latency += latency
                    io_operations += 1
                self.stats['read_pages'] += 1
                self.stats['page_hits'] += 1
            else:
                from storage.interface import KVValue
                dummy_value = KVValue(data=bytes(entry.value_size_bytes), page_count=1, token_count=self.layout.page_size_tokens)
                start_io = time.perf_counter()
                self.storage.put(key, dummy_value)
                latency = (time.perf_counter() - start_io) * 1000.0
                total_latency += latency
                io_operations += 1
                self.stats['write_pages'] += 1

        # Only record latency if we actually performed IO operations
        if io_operations > 0 and total_latency > 0:
            latency_ms = total_latency
        else:
            latency_ms = 0.0

        if latency_ms > 0:
            self.stats['request_latencies_ms'].append(latency_ms)
        return latency_ms

    def get_stats(self) -> Dict:
        """Get statistics"""
        storage_stats = self.storage.get_stats()
        request_latencies = self.stats['request_latencies_ms']

        if request_latencies:
            sorted_latencies = sorted(request_latencies)
            n = len(sorted_latencies)

            def get_percentile(p: float) -> float:
                idx = int(n * p)
                return sorted_latencies[idx] if idx < n else sorted_latencies[-1]

            latency_stats = {
                'avg_ms': statistics.mean(request_latencies),
                'p50_ms': sorted_latencies[n // 2],
                'p95_ms': get_percentile(0.95),
                'p99_ms': get_percentile(0.99),
            }
        else:
            latency_stats = {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}

        total_pages = self.stats['read_pages'] + self.stats['write_pages']

        return {
            'total_requests': self.stats['total_requests'],
            'total_tokens': self.stats['total_tokens'],
            'total_pages': total_pages,
            'read_pages': self.stats['read_pages'],
            'write_pages': self.stats['write_pages'],
            'page_hits': self.stats['page_hits'],
            'page_hit_rate': self.stats['read_pages'] / total_pages if total_pages > 0 else 0,
            'write_ratio': self.stats['write_pages'] / total_pages if total_pages > 0 else 0,
            'num_layers': self.num_layers,
            'has_indexer': self.has_indexer,
            'latency': latency_stats,
            'storage': storage_stats,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self, force_sync: bool = True):
        self.storage.close(force_sync=force_sync)


# ============================================================================
# Benchmark Runner
# ============================================================================

def run_benchmark(trace_path: str, storage_dir: str, model_config: dict,
                  max_requests: int = None, max_pages: int = 100000,
                  page_size_tokens: int = 64,
                  replay_timestamps: bool = False, time_scale: float = 1.0,
                  fsync_mode: str = 'none', fsync_batch_size: int = 100) -> Dict:
    """Run benchmark"""
    print(f"\n{'='*80}")
    print(f"Running: {Path(trace_path).name}")
    print(f"Model: {model_config['name']}")
    print(f"Layers: {model_config['num_layers']}")
    print(f"Page size: {page_size_tokens} tokens")
    print(f"{'='*80}")

    # Load trace
    replay = TraceReplay(trace_path)
    requests = replay.load_all()

    if max_requests:
        requests = requests[:max_requests]

    print(f"Loaded {len(requests)} requests")

    # Run benchmark
    with StorageBenchmark(
        storage_dir=storage_dir,
        model_config=model_config,
        page_size_tokens=page_size_tokens,
        max_pages=max_pages,
        fsync_mode=fsync_mode,
        fsync_batch_size=fsync_batch_size
    ) as benchmark:

        start_time = time.perf_counter()
        total_io_time = 0.0
        last_timestamp = None

        for i, req in enumerate(requests):
            if replay_timestamps and last_timestamp is not None:
                delta_ms = req.timestamp - last_timestamp
                sleep_time = delta_ms / 1000.0 / time_scale
                if sleep_time > 0:
                    time.sleep(sleep_time)

            req_start = time.perf_counter()
            benchmark.process_request(req)
            req_io_time = time.perf_counter() - req_start
            total_io_time += req_io_time

            last_timestamp = req.timestamp

            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{len(requests)}...")

        elapsed = time.perf_counter() - start_time
        stats = benchmark.get_stats()

    io_time = total_io_time if replay_timestamps else elapsed

    return {
        'trace_file': Path(trace_path).name,
        'total_requests': len(requests),
        'io_time_s': io_time,
        'requests_per_second': len(requests) / io_time if io_time > 0 else 0,
        'model': model_config['name'],
        'fsync_mode': fsync_mode,
        **stats,
    }


# ============================================================================
# Output Formatting
# ============================================================================

def print_results(results: List[Dict]):
    """Print benchmark results"""
    for i, r in enumerate(results, 1):
        print(f"\n{'='*80}")
        print(f"  [{i}/{len(results)}] {r['trace_file']}")
        print(f"{'='*80}")

        print(f"\n[Performance]")
        print(f"  Model:           {r.get('model', 'N/A')}")
        print(f"  Requests:        {r['total_requests']:,}")
        print(f"  Tokens:          {r.get('total_tokens', 0):,}")
        print(f"  QPS:             {r['requests_per_second']:.2f}")
        print(f"  Hit Rate:        {r['page_hit_rate']:.2%}")

        latency = r.get('latency', {})
        print(f"\n[Latency]")
        print(f"  Avg:  {latency.get('avg_ms', 0):.3f} ms")
        print(f"  P50:  {latency.get('p50_ms', 0):.3f} ms")
        print(f"  P95:  {latency.get('p95_ms', 0):.3f} ms")
        print(f"  P99:  {latency.get('p99_ms', 0):.3f} ms")

        storage = r.get('storage', {})
        print(f"\n[Storage]")
        print(f"  Read:  {storage.get('read', {}).get('count', 0):,} ops ({storage.get('read', {}).get('mb', 0):.2f} MB)")
        print(f"  Write: {storage.get('write', {}).get('count', 0):,} ops ({storage.get('write', {}).get('mb', 0):.2f} MB)")


# ============================================================================
# CLI Entry Point
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Mooncake KVCache Storage Benchmark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--trace-dir', type=str, default='../../FAST25-release/traces',
                       help='Trace files directory')
    parser.add_argument('--scenario', type=str, choices=['conversation', 'synthetic', 'toolagent', 'all'],
                       default='toolagent', help='Test scenario')
    parser.add_argument('--storage-dir', type=str, default='/tmp/mooncake_bench',
                       help='Storage directory')
    parser.add_argument('--model', type=str, default='glm5', choices=['glm5', 'kimi-k2.6'],
                       help='Model preset')
    parser.add_argument('--page-size-tokens', type=int, default=64,
                       help='Page size in tokens (default: 64)')
    parser.add_argument('--max-requests', type=int, default=None,
                       help='Maximum number of requests')
    parser.add_argument('--max-pages', type=int, default=100000,
                       help='Maximum number of pages')
    parser.add_argument('--fsync-mode', type=str, choices=['batch', 'always', 'end', 'none'],
                       default='none', help='When to fsync')
    parser.add_argument('--fsync-batch-size', type=int, default=100,
                       help='Number of writes between fsync')

    args = parser.parse_args()

    print(f"\n{'='*80}")
    print(f"{'Mooncake KVCache Storage Benchmark':^80}")
    print(f"{'='*80}")

    model_config = get_model_config(args.model)
    print(f"Model: {args.model} ({model_config['num_layers']} layers)")

    # Determine scenarios
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
                model_config,
                args.max_requests,
                args.max_pages,
                args.page_size_tokens,
                False,
                1.0,
                args.fsync_mode,
                args.fsync_batch_size
            )
            results.append(result)
        else:
            print(f"Warning: Trace file not found: {trace_path}")

    # Print results
    # Print results
    if results:
        print_results(results)
    else:
        print("Error: No trace files were successfully processed.", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
