#!/usr/bin/env python3
"""
Unified benchmark harness for Mooncake Store batch fast path evaluation.

This script is intentionally lightweight:
- it drives store put/get/remove operations from Python
- it records throughput and latency percentiles
- it optionally scrapes master metrics for cache hit rate and memory usage

The script is designed as a reproducible evaluation helper for the
`feat/part-2-batch-fast-path` branch.
"""

import argparse
import json
import os
import random
import statistics
import string
import time
import urllib.request
from dataclasses import dataclass, asdict


try:
    from mooncake.store import MooncakeDistributedStore
except ImportError as exc:
    raise SystemExit(
        "Failed to import mooncake.store. Build/install Mooncake first."
    ) from exc


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def random_payload(size):
    alphabet = string.ascii_letters.encode()
    return bytes(alphabet[i % len(alphabet)] for i in range(size))


def fetch_text(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_prometheus_gauge(text, metric_name):
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(metric_name):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return float(parts[-1])
                except ValueError:
                    return None
    return None


@dataclass
class OperationStats:
    operations: int
    throughput_ops: float
    p50_ms: float
    p99_ms: float
    avg_ms: float


class BenchmarkRunner:
    def __init__(self, args):
        self.args = args
        self.store = MooncakeDistributedStore()
        self.reference = {}

    def setup(self):
        ret = self.store.setup(
            os.getenv("LOCAL_HOSTNAME", "localhost"),
            os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"),
            int(os.getenv("MC_GLOBAL_SEGMENT_SIZE", 512 * 1024 * 1024)),
            int(os.getenv("MC_LOCAL_BUFFER_SIZE", 256 * 1024 * 1024)),
            os.getenv("PROTOCOL", "tcp"),
            os.getenv("DEVICE_NAME", ""),
            os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        )
        if ret != 0:
            raise RuntimeError(f"store.setup failed: {ret}")

    def close(self):
        try:
            self.store.close()
        except Exception:
            pass

    def prefill(self, count):
        payload = random_payload(self.args.value_size)
        for i in range(count):
            key = f"prefill_{i}"
            rc = self.store.put(key, payload)
            if rc == 0:
                self.reference[key] = payload

    def run_put(self):
        latencies = []
        payload = random_payload(self.args.value_size)
        start = time.perf_counter()
        i = 0
        while time.perf_counter() - start < self.args.duration_seconds:
            key = f"put_{i}"
            op_start = time.perf_counter()
            rc = self.store.put(key, payload)
            elapsed_ms = (time.perf_counter() - op_start) * 1000.0
            if rc == 0:
                self.reference[key] = payload
                latencies.append(elapsed_ms)
            i += 1
            if self.args.put_qps > 0:
                time.sleep(1.0 / self.args.put_qps)
        return self._stats_from_latencies(latencies)

    def run_get(self):
        latencies = []
        keys = list(self.reference.keys())
        if not keys:
            return self._stats_from_latencies(latencies)
        start = time.perf_counter()
        while time.perf_counter() - start < self.args.duration_seconds:
            key = random.choice(keys)
            op_start = time.perf_counter()
            _ = self.store.get(key)
            latencies.append((time.perf_counter() - op_start) * 1000.0)
            if self.args.get_qps > 0:
                time.sleep(1.0 / self.args.get_qps)
        return self._stats_from_latencies(latencies)

    def run_remove(self):
        latencies = []
        keys = list(self.reference.keys())
        start = time.perf_counter()
        idx = 0
        while keys and time.perf_counter() - start < self.args.duration_seconds:
            key = keys[idx % len(keys)]
            op_start = time.perf_counter()
            rc = self.store.remove(key)
            elapsed_ms = (time.perf_counter() - op_start) * 1000.0
            if rc == 0:
                latencies.append(elapsed_ms)
                self.reference.pop(key, None)
                keys = list(self.reference.keys())
                idx = 0
            else:
                idx += 1
            if self.args.remove_qps > 0:
                time.sleep(1.0 / self.args.remove_qps)
        return self._stats_from_latencies(latencies)

    def scrape_master_metrics(self):
        if not self.args.master_metrics:
            return {}
        text = fetch_text(self.args.master_metrics)
        result = {
            "overall_hit_rate": parse_prometheus_gauge(text, "overall_hit_rate"),
            "valid_get_rate": parse_prometheus_gauge(text, "valid_get_rate"),
            "master_allocated_bytes": parse_prometheus_gauge(
                text, "master_allocated_bytes"
            ),
            "master_total_capacity_bytes": parse_prometheus_gauge(
                text, "master_total_capacity_bytes"
            ),
        }
        allocated = result.get("master_allocated_bytes")
        total = result.get("master_total_capacity_bytes")
        if allocated is not None and total not in (None, 0):
            result["memory_utilization_ratio"] = allocated / total
        else:
            result["memory_utilization_ratio"] = None
        return result

    def _stats_from_latencies(self, latencies):
        total_seconds = self.args.duration_seconds if self.args.duration_seconds > 0 else 1
        ops = len(latencies)
        avg = statistics.mean(latencies) if latencies else 0.0
        return OperationStats(
            operations=ops,
            throughput_ops=ops / total_seconds,
            p50_ms=percentile(latencies, 50),
            p99_ms=percentile(latencies, 99),
            avg_ms=avg,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--prefill-count", type=int, default=1000)
    parser.add_argument("--value-size", type=int, default=4096)
    parser.add_argument("--put-qps", type=float, default=0.0)
    parser.add_argument("--get-qps", type=float, default=0.0)
    parser.add_argument("--remove-qps", type=float, default=0.0)
    parser.add_argument("--master-metrics", type=str, default="")
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()

    runner = BenchmarkRunner(args)
    runner.setup()
    try:
        runner.prefill(args.prefill_count)
        put_stats = runner.run_put()
        get_stats = runner.run_get()
        remove_stats = runner.run_remove()
        metrics = runner.scrape_master_metrics()

        result = {
            "config": {
                "duration_seconds": args.duration_seconds,
                "prefill_count": args.prefill_count,
                "value_size": args.value_size,
                "put_qps": args.put_qps,
                "get_qps": args.get_qps,
                "remove_qps": args.remove_qps,
                "master_metrics": args.master_metrics,
            },
            "put": asdict(put_stats),
            "get": asdict(get_stats),
            "remove": asdict(remove_stats),
            "master_metrics": metrics,
        }

        text = json.dumps(result, indent=2, sort_keys=True)
        print(text)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text)
    finally:
        runner.close()


if __name__ == "__main__":
    main()
