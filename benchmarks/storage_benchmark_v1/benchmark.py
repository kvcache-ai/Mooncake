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
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor, as_completed
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
            'request_io_latencies_ms': [],
            'request_wall_latencies_ms': [],
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

        request_start = time.perf_counter()
        io_latency_ms = 0.0

        # Process each access requirement from layout
        for access in self.layout.get_operations(req):
            if self.storage.exists(access.page_id):
                # Page exists, perform READ
                io_latency_ms += self.storage.read(
                    access.page_id,
                    offset_in_page=access.offset_in_page,
                    length=access.length
                )
                self.stats['read_pages'] += 1
                self.stats['page_hits'] += 1
            else:
                # Page doesn't exist, perform WRITE
                io_latency_ms += self.storage.write(
                    access.page_id,
                    offset_in_page=access.offset_in_page,
                    length=access.length
                )
                self.stats['write_pages'] += 1

        wall_latency_ms = (time.perf_counter() - request_start) * 1000.0
        self.stats['request_io_latencies_ms'].append(io_latency_ms)
        self.stats['request_wall_latencies_ms'].append(wall_latency_ms)
        return io_latency_ms

    def get_stats(self) -> Dict:
        """Get statistics"""
        storage_stats = self.storage.get_stats()
        request_io_latencies = self.stats['request_io_latencies_ms']
        request_wall_latencies = self.stats['request_wall_latencies_ms']

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
            'request_io_latency': latency_stats(request_io_latencies),
            'request_wall_latency': latency_stats(request_wall_latencies),
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


def parse_csv_floats(value: str) -> List[float]:
    return [float(item.strip()) for item in value.split(',') if item.strip()]


def wait_for_replay_time(req: KVCacheRequest, base_timestamp: float,
                         start_time: float, replay_scale: float):
    if replay_scale <= 0 or req.timestamp == 0:
        return
    target_time = (start_time +
                   max(0.0, req.timestamp - base_timestamp) /
                   (1000.0 * replay_scale))
    delay = target_time - time.perf_counter()
    if delay > 0:
        time.sleep(delay)


def latency_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}

    sorted_values = sorted(values)

    def get_percentile(p: float) -> float:
        if len(sorted_values) == 1:
            return sorted_values[0]
        rank = (len(sorted_values) - 1) * p
        lower = int(rank)
        upper = min(lower + 1, len(sorted_values) - 1)
        weight = rank - lower
        return (sorted_values[lower] * (1.0 - weight) +
                sorted_values[upper] * weight)

    return {
        'avg_ms': statistics.mean(values),
        'p50_ms': get_percentile(0.50),
        'p95_ms': get_percentile(0.95),
        'p99_ms': get_percentile(0.99),
    }


def snapshot_thread_stats(benchmark: StorageBenchmark) -> Dict[str, Any]:
    storage = benchmark.storage
    total_pages = benchmark.stats['read_pages'] + benchmark.stats['write_pages']
    return {
        'total_requests': benchmark.stats['total_requests'],
        'total_tokens': benchmark.stats['total_tokens'],
        'read_pages': benchmark.stats['read_pages'],
        'write_pages': benchmark.stats['write_pages'],
        'page_hits': benchmark.stats['page_hits'],
        'request_io_latencies_ms': list(
            benchmark.stats['request_io_latencies_ms']
        ),
        'request_wall_latencies_ms': list(
            benchmark.stats['request_wall_latencies_ms']
        ),
        'read_bytes': storage.stats['read_bytes'],
        'write_bytes': storage.stats['write_bytes'],
        'read_time_s': storage.stats['read_time_s'],
        'write_time_s': storage.stats['write_time_s'],
        'read_latencies_ms': list(storage.stats['read_latencies_ms']),
        'write_latencies_ms': list(storage.stats['write_latencies_ms']),
        'sync_count': storage.stats['sync_count'],
        'max_pages': storage.max_pages,
        'written_pages': len(storage._written_pages),
        'total_pages': total_pages,
    }


def aggregate_thread_stats(thread_stats: List[Dict[str, Any]]) -> Dict:
    total_requests = sum(s['total_requests'] for s in thread_stats)
    total_tokens = sum(s['total_tokens'] for s in thread_stats)
    read_pages = sum(s['read_pages'] for s in thread_stats)
    write_pages = sum(s['write_pages'] for s in thread_stats)
    page_hits = sum(s['page_hits'] for s in thread_stats)
    total_pages = read_pages + write_pages

    request_io_latencies = []
    request_wall_latencies = []
    read_latencies = []
    write_latencies = []
    for stats in thread_stats:
        request_io_latencies.extend(stats['request_io_latencies_ms'])
        request_wall_latencies.extend(stats['request_wall_latencies_ms'])
        read_latencies.extend(stats['read_latencies_ms'])
        write_latencies.extend(stats['write_latencies_ms'])

    read_bytes = sum(s['read_bytes'] for s in thread_stats)
    write_bytes = sum(s['write_bytes'] for s in thread_stats)
    read_time = sum(s['read_time_s'] for s in thread_stats)
    write_time = sum(s['write_time_s'] for s in thread_stats)

    return {
        'total_requests': total_requests,
        'total_tokens': total_tokens,
        'total_pages': total_pages,
        'read_pages': read_pages,
        'write_pages': write_pages,
        'page_hits': page_hits,
        'page_hit_rate': read_pages / total_pages if total_pages > 0 else 0,
        'write_ratio': write_pages / total_pages if total_pages > 0 else 0,
        'request_io_latency': latency_stats(request_io_latencies),
        'request_wall_latency': latency_stats(request_wall_latencies),
        'storage': {
            'read': {
                'count': read_pages,
                'mb': read_bytes / 1024 / 1024,
                'time_s': read_time,
                **latency_stats(read_latencies),
            },
            'write': {
                'count': write_pages,
                'mb': write_bytes / 1024 / 1024,
                'time_s': write_time,
                **latency_stats(write_latencies),
            },
            'sync_count': sum(s['sync_count'] for s in thread_stats),
            'max_pages': sum(s['max_pages'] for s in thread_stats),
            'written_pages': sum(s['written_pages'] for s in thread_stats),
            'page_hits': page_hits,
            'page_misses': write_pages,
        },
    }


def print_progress(done: int, total: int, start_time: float,
                   stats: Dict, req: KVCacheRequest = None,
                   suffix: str = ""):
    elapsed = time.perf_counter() - start_time
    qps = done / elapsed if elapsed > 0 else 0
    storage = stats.get('storage', {})
    read_stats = storage.get('read', {})
    write_stats = storage.get('write', {})
    read_time = read_stats.get('time_s', 0)
    write_time = write_stats.get('time_s', 0)
    read_mbps = read_stats.get('mb', 0) / read_time if read_time > 0 else 0
    write_mbps = write_stats.get('mb', 0) / write_time if write_time > 0 else 0

    if req is None:
        req_info = ""
    else:
        req_info = (f" ids={len(req.hash_ids):3d} "
                    f"tokens={req.input_length + req.output_length:6d} |")

    print(f"  [{done:5d}/{total}]{req_info} QPS={qps:7.2f} | "
          f"R={stats['read_pages']:6d} "
          f"({read_stats.get('avg_ms', 0):6.2f}ms, {read_mbps:6.1f}MB/s) | "
          f"W={stats['write_pages']:6d} "
          f"({write_stats.get('avg_ms', 0):6.2f}ms, {write_mbps:6.1f}MB/s)"
          f"{suffix}")


def should_print_progress(done: int, total: int, progress_interval: int) -> bool:
    if done >= total:
        return True
    return progress_interval > 0 and done % progress_interval == 0


def run_single_thread(benchmark: StorageBenchmark,
                      requests: List[KVCacheRequest],
                      replay_scale: float,
                      progress_interval: int) -> Dict[str, Any]:
    start_time = time.perf_counter()
    base_timestamp = requests[0].timestamp if requests else 0
    completed = 0

    for req in requests:
        wait_for_replay_time(req, base_timestamp, start_time, replay_scale)
        benchmark.process_request(req)
        completed += 1
        if should_print_progress(completed, len(requests), progress_interval):
            print_progress(completed, len(requests), start_time,
                           benchmark.get_stats(), req)

    return {
        'completed': completed,
        'elapsed': time.perf_counter() - start_time,
        'stats': benchmark.get_stats(),
    }


def run_multi_thread(benchmarks: List[StorageBenchmark],
                     requests: List[KVCacheRequest],
                     replay_scale: float) -> Dict[str, Any]:
    start_time = time.perf_counter()
    base_timestamp = requests[0].timestamp if requests else 0
    total_requests = len(requests) * len(benchmarks)
    completed = 0

    def run_worker(thread_id: int):
        benchmark = benchmarks[thread_id]
        for req in requests:
            wait_for_replay_time(req, base_timestamp, start_time, replay_scale)
            benchmark.process_request(req)
        return snapshot_thread_stats(benchmark)

    thread_stats = []
    with ThreadPoolExecutor(max_workers=len(benchmarks)) as executor:
        futures = [
            executor.submit(run_worker, thread_id)
            for thread_id in range(len(benchmarks))
        ]
        for future in as_completed(futures):
            worker_stats = future.result()
            thread_stats.append(worker_stats)
            completed += worker_stats['total_requests']
            print_progress(completed, total_requests, start_time,
                           aggregate_thread_stats(thread_stats),
                           suffix=" | completed worker")

    return {
        'completed': completed,
        'elapsed': time.perf_counter() - start_time,
        'stats': aggregate_thread_stats(thread_stats),
    }


def run_benchmark(trace_path: str, storage_dir: str, model_config: dict,
                  max_requests: int = None, max_pages: int = None,
                  page_size_tokens: int = 512,
                  fsync_mode: str = 'none', fsync_batch_size: int = 100,
                  threads: int = 1, replay_scale: float = 0.0,
                  progress_interval: int = 100) -> Dict:
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
        threads: Benchmark client worker threads
        replay_scale: Timestamp replay multiplier; 0 runs unpaced
        progress_interval: Print progress every N requests; 0 disables progress

    Returns:
        Benchmark results dictionary
    """
    print(f"\n{'='*80}")
    print(f"Running: {Path(trace_path).name}")
    print(f"Model: {model_config['name']}")
    print(f"Layers: {model_config['num_layers']}")
    print(f"Page size: {page_size_tokens} tokens")
    print(f"Threads: {threads}")
    print(f"Fast-forward: {replay_scale:g}x" if replay_scale > 0 else "Fast-forward: unpaced")
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
    if threads > 1:
        print(f"  Max pages across threads:         {max_pages * threads:,}")
    print(f"  Max storage available:            {max_size_gb:.2f} GB")
    if threads > 1:
        print(f"  Max storage across threads:       {max_size_gb * threads:.2f} GB")

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

    try:
        if threads <= 1:
            with StorageBenchmark(
                storage_dir=storage_dir,
                model_config=model_config,
                page_size_tokens=page_size_tokens,
                max_pages=max_pages,
                fsync_mode=fsync_mode,
                fsync_batch_size=fsync_batch_size
            ) as benchmark:
                result = run_single_thread(benchmark, requests, replay_scale,
                                           progress_interval)
        else:
            with ExitStack() as stack:
                benchmarks = [
                    stack.enter_context(StorageBenchmark(
                        storage_dir=str(Path(storage_dir) / f"thread_{thread_id}"),
                        model_config=model_config,
                        page_size_tokens=page_size_tokens,
                        max_pages=max_pages,
                        fsync_mode=fsync_mode,
                        fsync_batch_size=fsync_batch_size
                    ))
                    for thread_id in range(threads)
                ]
                result = run_multi_thread(benchmarks, requests, replay_scale)
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"Interrupted! Showing partial results:")
        print(f"{'='*80}")
        result = result if 'result' in locals() else {
            'completed': 0,
            'elapsed': 0,
            'stats': {},
        }
        print_results([{
            'trace_file': Path(trace_path).name,
            'total_requests': result['completed'],
            'io_time_s': result['elapsed'],
            'requests_per_second': (
                result['completed'] / result['elapsed']
                if result['elapsed'] > 0 else 0
            ),
            'model': model_config['name'],
            'fsync_mode': fsync_mode,
            'threads': threads,
            'replay_scale': replay_scale,
            **result['stats'],
        }])
        sys.exit(0)

    return {
        'trace_file': Path(trace_path).name,
        'total_requests': result['completed'],
        'io_time_s': result['elapsed'],
        'requests_per_second': (
            result['completed'] / result['elapsed'] if result['elapsed'] > 0 else 0
        ),
        'model': model_config['name'],
        'fsync_mode': fsync_mode,
        'threads': threads,
        'replay_scale': replay_scale,
        **result['stats'],
    }


# ============================================================================
# Output Formatting
# ============================================================================

def format_storage_stats(stats: Dict, title: str = "Storage"):
    """Format storage statistics with clear read/write separation"""
    storage = stats.get('storage', {})
    read_stats = storage.get('read', {})
    write_stats = storage.get('write', {})
    request_wall = stats.get('request_wall_latency', {})
    request_io = stats.get('request_io_latency', {})

    output = []
    output.append(f"\n[{title}]")

    # General info
    output.append(f"\n[General]")
    output.append(f"  Model:            {stats.get('model', 'N/A')}")
    output.append(f"  Threads:          {stats.get('threads', 1)}")
    replay_scale = stats.get('replay_scale', 0)
    output.append(f"  Fast-forward:     {f'{replay_scale:g}x' if replay_scale else 'unpaced'}")
    output.append(f"  Requests:         {stats.get('total_requests', 0):,}")
    output.append(f"  Tokens:           {stats.get('total_tokens', 0):,}")
    output.append(f"  Total I/O Time:    {stats.get('io_time_s', 0):.3f} s")
    output.append(f"  QPS:              {stats.get('requests_per_second', 0):.2f}")
    output.append(f"  Hit Rate:         {stats.get('page_hit_rate', 0):.2%}")

    # Request Stats
    output.append(f"\n[Request Wall Latency]")
    output.append(f"  Avg:              {request_wall.get('avg_ms', 0):.3f} ms")
    output.append(f"  P50:              {request_wall.get('p50_ms', 0):.3f} ms")
    output.append(f"  P95:              {request_wall.get('p95_ms', 0):.3f} ms")
    output.append(f"  P99:              {request_wall.get('p99_ms', 0):.3f} ms")

    output.append(f"\n[Request Storage I/O Latency]")
    output.append(f"  Avg:              {request_io.get('avg_ms', 0):.3f} ms")
    output.append(f"  P50:              {request_io.get('p50_ms', 0):.3f} ms")
    output.append(f"  P95:              {request_io.get('p95_ms', 0):.3f} ms")
    output.append(f"  P99:              {request_io.get('p99_ms', 0):.3f} ms")

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
    parser.add_argument('--threads', type=int, default=1,
                       help='Number of benchmark client worker threads')
    parser.add_argument('--replay-scales', type=str, default='0',
                       help='Comma-separated trace fast-forward speeds; 0 means unpaced')
    parser.add_argument('--progress-interval', type=int, default=100,
                       help='Print progress every N requests; 0 disables per-request progress')

    args = parser.parse_args()
    if args.threads < 1:
        parser.error('--threads must be at least 1')
    if args.progress_interval < 0:
        parser.error('--progress-interval must be non-negative')

    print(f"\n{'='*80}")
    print(f"{'Mooncake KVCache Storage Benchmark':^80}")
    print(f"{'='*80}")

    model_config = get_model_config(args.model)
    print(f"Model: {args.model} ({model_config['num_layers']} layers)")
    replay_scales = parse_csv_floats(args.replay_scales)
    if not replay_scales:
        parser.error('--replay-scales must include at least one value')
    if any(scale < 0 for scale in replay_scales):
        parser.error('--replay-scales values must be non-negative')

    # Determine scenarios
    scenarios = ['conversation', 'synthetic', 'toolagent'] if args.scenario == 'all' else [args.scenario]
    trace_files = {
        'conversation': 'conversation_trace.jsonl',
        'synthetic': 'synthetic_trace.jsonl',
        'toolagent': 'toolagent_trace.jsonl'
    }

    # Run benchmarks
    results = []
    use_scale_subdirs = len(replay_scales) > 1 or replay_scales[0] != 0
    for scenario in scenarios:
        trace_path = Path(args.trace_dir) / trace_files[scenario]
        if trace_path.exists():
            for replay_scale in replay_scales:
                run_dir = Path(args.storage_dir) / scenario
                if use_scale_subdirs:
                    run_dir = run_dir / f"replay_{replay_scale:g}x"
                result = run_benchmark(
                    str(trace_path),
                    str(run_dir),
                    model_config,
                    args.max_requests,
                    args.max_pages,
                    args.page_size_tokens,
                    args.fsync_mode,
                    args.fsync_batch_size,
                    args.threads,
                    replay_scale,
                    args.progress_interval
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
