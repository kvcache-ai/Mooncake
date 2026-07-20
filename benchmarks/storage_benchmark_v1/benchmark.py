#!/usr/bin/env python3
"""
Mooncake KVCache Storage Benchmark

Complete benchmark tool with CLI interface.
"""

import argparse
import json
import sys
import time
import statistics
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Dict, Any

from storage import DiskHashTable
from layout import get_model_config, create_layout


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class KVCacheRequest:
    """KVCache request from trace"""
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

    def load_all(self) -> List[KVCacheRequest]:
        """Load all requests from trace file"""
        requests = []
        with open(self.trace_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    req = json.loads(line)
                    requests.append(KVCacheRequest(
                        timestamp=req.get('timestamp', 0),
                        hash_ids=req.get('hash_ids', []),
                        input_length=req.get('input_length', 0),
                        output_length=req.get('output_length', 0),
                    ))
        return requests


# ============================================================================
# Storage Benchmark
# ============================================================================

class StorageBenchmark:
    """KVCache storage benchmark

    Processes KVCache requests using layout-generated access patterns.
    """

    def __init__(self, storage_dir: str, model_config: dict,
                 page_size_tokens: int = 512,
                 max_pages: int = 100000,
                 fsync_mode: str = 'none', fsync_batch_size: int = 100):
        """Initialize benchmark

        Args:
            storage_dir: Directory for storage files
            model_config: Model configuration dict
            page_size_tokens: Tokens per page (default: 512)
            max_pages: Maximum number of pages
            fsync_mode: When to fsync ('none', 'batch', 'always', 'end')
            fsync_batch_size: Number of writes between fsync in batch mode
        """
        self.model_config = model_config
        self.layout = create_layout(model_config, page_size_tokens)
        self.page_size_bytes = self.layout.value_size_bytes

        # Initialize storage
        self.storage = DiskHashTable(
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

    def process_request(self, req: KVCacheRequest) -> float:
        """Process a KVCache request

        Args:
            req: KVCache request

        Returns:
            Total latency in milliseconds
        """
        self.stats['total_requests'] += 1
        self.stats['total_tokens'] += req.input_length + req.output_length

        total_latency = 0.0

        # Process each access requirement from layout
        for access in self.layout.get_operations(req):
            if self.storage.exists(access.page_id):
                # Page exists, perform READ
                total_latency += self.storage.read(
                    access.page_id,
                    offset_in_page=access.offset_in_page,
                    length=access.length
                )
                self.stats['read_pages'] += 1
                self.stats['page_hits'] += 1
            else:
                # Page doesn't exist, perform WRITE
                total_latency += self.storage.write(
                    access.page_id,
                    offset_in_page=access.offset_in_page,
                    length=access.length
                )
                self.stats['write_pages'] += 1

        latency_ms = total_latency if total_latency > 0 else 0.0
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

def get_max_page_id(requests: List[KVCacheRequest]) -> int:
    max_id = 0
    for req in requests:
        if req.hash_ids:
            max_id = max(max_id, max(req.hash_ids))
    return max_id


def run_benchmark(trace_path: str, storage_dir: str, model_config: dict,
                  max_requests: int = None, max_pages: int = None,
                  page_size_tokens: int = 512,
                  fsync_mode: str = 'none', fsync_batch_size: int = 100) -> Dict:
    """Run benchmark

    Args:
        trace_path: Trace file path
        storage_dir: Storage directory
        model_config: Model configuration
        max_requests: Maximum number of requests (None = all)
        max_pages: Maximum number of pages (None = auto-calculate)
        page_size_tokens: Tokens per page
        fsync_mode: When to fsync
        fsync_batch_size: Number of writes between fsync

    Returns:
        Benchmark results dictionary
    """
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

    # Find max page_id from trace
    max_page_id = get_max_page_id(requests)
    max_pages_needed = max_page_id + 1  # page_id is 0-based

    # Create layout to get page size
    layout = create_layout(model_config, page_size_tokens)
    page_size_bytes = layout.value_size_bytes

    # Determine max_pages
    if max_pages is None:
        # Use max page_id from trace
        max_pages = max_pages_needed
        # Round up to next thousand for cleaner numbers
        max_pages = ((max_pages + 999) // 1000) * 1000
    else:
        # User specified max_pages
        pass

    max_size_gb = max_pages * page_size_bytes / (1024**3)
    trace_size_gb = max_pages_needed * page_size_bytes / (1024**3)

    print(f"\n[Storage Configuration]")
    print(f"  Max page_id in trace:             {max_page_id:,}")
    print(f"  Pages needed (trace):             {max_pages_needed:,}")
    print(f"  Trace storage size:               {trace_size_gb:.2f} GB")
    print(f"  Max pages configured:             {max_pages:,}")
    print(f"  Max storage available:            {max_size_gb:.2f} GB")

    if max_pages_needed > max_pages:
        shortfall = max_pages_needed - max_pages
        shortfall_gb = shortfall * page_size_bytes / (1024**3)
        compression_ratio = max_pages / max_pages_needed
        print(f"\n  ⚠️  Storage insufficient: {shortfall:,} pages shortfall ({shortfall_gb:.2f} GB)")
        print(f"  ⚠️  Consider increasing --max-pages to at least {max_pages_needed:,} for full simulation")
    else:
        surplus = max_pages - max_pages_needed
        surplus_pct = (surplus / max_pages) * 100 if max_pages > 0 else 0
        print(f"  ✓ Direct mapping: all {max_pages_needed:,} logical pages uniquely mapped")

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
        try:
            for i, req in enumerate(requests):
                benchmark.process_request(req)
                # Print progress for each request
                elapsed = time.perf_counter() - start_time
                qps = (i + 1) / elapsed if elapsed > 0 else 0
                stats = benchmark.get_stats()
                storage = stats.get('storage', {})
                read_latency = storage.get('read', {}).get('avg_ms', 0)
                write_latency = storage.get('write', {}).get('avg_ms', 0)
                read_mb = storage.get('read', {}).get('mb', 0)
                write_mb = storage.get('write', {}).get('mb', 0)
                read_time = storage.get('read', {}).get('time_s', 0)
                write_time = storage.get('write', {}).get('time_s', 0)
                read_mbps = read_mb / read_time if read_time > 0 else 0
                write_mbps = write_mb / write_time if write_time > 0 else 0
                print(f"  [{i+1:5d}/{len(requests)}] ids={len(req.hash_ids):3d} "
                      f"tokens={req.input_length+req.output_length:6d} | "
                      f"QPS={qps:7.2f} | "
                      f"R={stats['read_pages']:6d} ({read_latency:6.2f}ms, {read_mbps:6.1f}MB/s) | "
                      f"W={stats['write_pages']:6d} ({write_latency:6.2f}ms, {write_mbps:6.1f}MB/s)")
        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"Interrupted! Showing partial results:")
            print(f"{'='*80}")
            elapsed = time.perf_counter() - start_time
            stats = benchmark.get_stats()
            print_results([{
                'trace_file': Path(trace_path).name,
                'total_requests': i + 1,
                'io_time_s': elapsed,
                'requests_per_second': (i + 1) / elapsed if elapsed > 0 else 0,
                'model': model_config['name'],
                'fsync_mode': fsync_mode,
                **stats,
            }])
            sys.exit(0)

        elapsed = time.perf_counter() - start_time
        stats = benchmark.get_stats()

    return {
        'trace_file': Path(trace_path).name,
        'total_requests': len(requests),
        'io_time_s': elapsed,
        'requests_per_second': len(requests) / elapsed if elapsed > 0 else 0,
        'model': model_config['name'],
        'fsync_mode': fsync_mode,
        **stats,
    }


# ============================================================================
# Output Formatting
# ============================================================================

def format_storage_stats(stats: Dict, title: str = "Storage"):
    """Format storage statistics with clear read/write separation"""
    storage = stats.get('storage', {})
    read_stats = storage.get('read', {})
    write_stats = storage.get('write', {})

    output = []
    output.append(f"\n[{title}]")

    # General info
    output.append(f"\n[General]")
    output.append(f"  Model:            {stats.get('model', 'N/A')}")
    output.append(f"  Requests:         {stats.get('total_requests', 0):,}")
    output.append(f"  Tokens:           {stats.get('total_tokens', 0):,}")
    output.append(f"  Total I/O Time:    {stats.get('io_time_s', 0):.3f} s")
    output.append(f"  QPS:              {stats.get('requests_per_second', 0):.2f}")
    output.append(f"  Hit Rate:         {stats.get('page_hit_rate', 0):.2%}")

    # Read Stats
    output.append(f"\n[Read Operations]")
    output.append(f"  Count:            {read_stats.get('count', 0):,}")
    output.append(f"  Data Volume:      {read_stats.get('mb', 0):.2f} MB")
    read_time = read_stats.get('time_s', 0)
    read_mbps = read_stats.get('mb', 0) / read_time if read_time > 0 else 0
    output.append(f"  Total Time:       {read_time:.3f} s")
    output.append(f"  Bandwidth:        {read_mbps:.2f} MB/s")
    output.append(f"  Latency:")
    output.append(f"    Avg:            {read_stats.get('avg_ms', 0):.3f} ms")
    output.append(f"    P50:            {read_stats.get('p50_ms', 0):.3f} ms")
    output.append(f"    P95:            {read_stats.get('p95_ms', 0):.3f} ms")
    output.append(f"    P99:            {read_stats.get('p99_ms', 0):.3f} ms")

    # Write Stats
    output.append(f"\n[Write Operations]")
    output.append(f"  Count:            {write_stats.get('count', 0):,}")
    output.append(f"  Data Volume:      {write_stats.get('mb', 0):.2f} MB")
    write_time = write_stats.get('time_s', 0)
    write_mbps = write_stats.get('mb', 0) / write_time if write_time > 0 else 0
    output.append(f"  Total Time:       {write_time:.3f} s")
    output.append(f"  Bandwidth:        {write_mbps:.2f} MB/s")
    output.append(f"  Latency:")
    output.append(f"    Avg:            {write_stats.get('avg_ms', 0):.3f} ms")
    output.append(f"    P50:            {write_stats.get('p50_ms', 0):.3f} ms")
    output.append(f"    P95:            {write_stats.get('p95_ms', 0):.3f} ms")
    output.append(f"    P99:            {write_stats.get('p99_ms', 0):.3f} ms")

    # Storage Info
    output.append(f"\n[Storage Info]")
    output.append(f"  Max Pages:        {storage.get('max_pages', 0):,}")
    output.append(f"  Written Pages:    {storage.get('written_pages', 0):,}")
    output.append(f"  Sync Count:       {storage.get('sync_count', 0):,}")

    return "\n".join(output)


def print_results(results: List[Dict]):
    """Print benchmark results"""
    for i, r in enumerate(results, 1):
        print(f"\n{'='*80}")
        print(f"  [{i}/{len(results)}] {r['trace_file']}")
        print(f"{'='*80}")
        print(format_storage_stats(r))


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
    parser.add_argument('--page-size-tokens', type=int, default=512,
                       help='Page size in tokens (default: 512)')
    parser.add_argument('--max-requests', type=int, default=None,
                       help='Maximum number of requests')
    parser.add_argument('--max-pages', type=int, default=2000,
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
                args.fsync_mode,
                args.fsync_batch_size
            )
            results.append(result)
        else:
            print(f"Warning: Trace file not found: {trace_path}")

    # Print results
    if results:
        print_results(results)
    else:
        print("Error: No trace files were successfully processed.", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
