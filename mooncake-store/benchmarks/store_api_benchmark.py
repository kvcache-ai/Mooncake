#!/usr/bin/env python3
"""Mooncake Store API benchmark and stress-test utility.

This script exercises the Python MooncakeDistributedStore API against a running
mooncake_master. It supports local smoke tests, public API coverage, repeated
benchmarks, mixed read/write stress workloads, concurrency scans, multi-client
stress tests, long-steady trend analysis, baseline comparison, RSS tracking,
and Prometheus metrics snapshots from the master admin endpoint.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import ctypes
import importlib
import json
import os
import platform
import random
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence


BYTES_PER_MIB = 1024 * 1024
PUBLIC_API_OPERATIONS = [
    "put",
    "get",
    "is_exist",
    "batch_is_exist",
    "get_size",
    "put_batch",
    "get_batch",
    "upsert",
    "upsert_batch",
    "remove_by_regex",
    "remove",
    "batch_remove",
    "remove_all",
]
MULTI_CLIENT_OPERATION = "multi_client_mixed"
CONCURRENT_OPERATIONS = {"put", "get", "remove", "upsert", "is_exist", "get_size"}
MIX_PROFILES = {
    "put-heavy": {"put": 0.70, "get": 0.25, "remove": 0.05},
    "get-heavy": {"put": 0.20, "get": 0.75, "remove": 0.05},
    "mixed": {"put": 0.45, "get": 0.50, "remove": 0.05},
}
DEFAULT_CONCURRENCY_SCAN = [1, 4, 8, 16]
OBJECT_NOT_FOUND_CODE = -704
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_MASTER_METRICS_URL = f"http://127.0.0.1:{DEFAULT_MASTER_METRICS_PORT}/metrics"

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]


def parse_size(value: str) -> int:
    text = value.strip().lower()
    multipliers = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
    }
    for suffix, multiplier in sorted(
        multipliers.items(), key=lambda item: -len(item[0])
    ):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * multiplier)
    return int(text)


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def mean_and_stdev(values: Sequence[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, stdev


def coefficient_of_variation(values: Sequence[float]) -> float:
    mean, stdev = mean_and_stdev(values)
    if mean == 0.0:
        return 0.0
    return stdev / mean


def run_command(command: Sequence[str], cwd: Path | None = None) -> str | None:
    try:
        return subprocess.check_output(
            list(command),
            cwd=str(cwd) if cwd else None,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def parse_key_value_lines(text: str | None, separator: str = ":") -> dict[str, str]:
    values: dict[str, str] = {}
    if not text:
        return values
    for line in text.splitlines():
        if separator not in line:
            continue
        key, value = line.split(separator, 1)
        values[key.strip()] = value.strip()
    return values


def split_host_port(address: str) -> tuple[str, int]:
    if address.count(":") == 1:
        host, port = address.rsplit(":", 1)
        return host, int(port)
    parsed = urllib.parse.urlparse(f"//{address}")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"address must be host:port, got {address!r}")
    return parsed.hostname, parsed.port


def wait_for_port(host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {host}:{port}: {last_error}")


def wait_for_http_metadata(metadata_url: str, timeout: float) -> None:
    parsed = urllib.parse.urlparse(metadata_url)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"metadata URL must include host and port: {metadata_url}")
    wait_for_port(parsed.hostname, parsed.port, timeout)

    probe = metadata_url
    separator = "&" if "?" in probe else "?"
    probe = f"{probe}{separator}key=__mooncake_store_api_benchmark_probe__"
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(probe, timeout=1.0):
                return
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 404):
                return
            last_error = exc
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise TimeoutError(
        f"timed out waiting for metadata service {metadata_url}: {last_error}"
    )


def fetch_http_text(url: str, timeout: float) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            raw_body = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return {
                "available": True,
                "url": url,
                "http_status": response.getcode(),
                "content_type": response.headers.get("Content-Type"),
                "bytes": len(raw_body),
                "latency_ms": (time.perf_counter() - started_at) * 1000.0,
                "raw_text": raw_body.decode(charset, errors="replace"),
            }
    except urllib.error.HTTPError as exc:
        raw_body = exc.read()
        return {
            "available": False,
            "url": url,
            "http_status": exc.code,
            "bytes": len(raw_body),
            "latency_ms": (time.perf_counter() - started_at) * 1000.0,
            "error": str(exc),
            "raw_text": raw_body.decode("utf-8", errors="replace"),
        }
    except Exception as exc:
        return {
            "available": False,
            "url": url,
            "latency_ms": (time.perf_counter() - started_at) * 1000.0,
            "error": f"{type(exc).__name__}: {exc}",
        }


def prometheus_summary(raw_text: str) -> dict[str, Any]:
    metric_names: set[str] = set()
    sample_count = 0
    first_values: dict[str, float] = {}
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        sample_count += 1
        token = line.split(None, 1)[0]
        metric_name = token.split("{", 1)[0]
        if not metric_name:
            continue
        metric_names.add(metric_name)
        if metric_name in first_values:
            continue
        parts = line.rsplit(None, 1)
        if len(parts) != 2:
            continue
        try:
            first_values[metric_name] = float(parts[1])
        except ValueError:
            continue
    return {
        "line_count": len(raw_text.splitlines()),
        "sample_count": sample_count,
        "metric_count": len(metric_names),
        "metric_names": sorted(metric_names),
        "first_values": first_values,
    }


def collect_prometheus_endpoint(url: str, timeout: float) -> dict[str, Any]:
    result = fetch_http_text(url, timeout)
    raw_text = result.get("raw_text")
    if isinstance(raw_text, str):
        result["prometheus"] = prometheus_summary(raw_text)
    return result


def replace_url_path(url: str, path: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse(parsed._replace(path=path, query="", fragment=""))


def collect_component_metrics(url: str, timeout: float) -> dict[str, Any]:
    return {
        "metrics": collect_prometheus_endpoint(url, timeout),
        "summary": fetch_http_text(replace_url_path(url, "/metrics/summary"), timeout),
        "health": fetch_http_text(replace_url_path(url, "/health"), timeout),
    }


def collect_metrics_snapshots(args: argparse.Namespace) -> dict[str, Any]:
    if args.skip_metrics:
        return {
            "enabled": False,
            "reason": "--skip-metrics was set",
        }

    snapshot: dict[str, Any] = {
        "enabled": True,
        "timestamp_unix": time.time(),
        "timeout_s": args.metrics_timeout,
    }
    snapshot["master"] = collect_component_metrics(
        args.master_metrics_url, args.metrics_timeout
    )

    if args.client_metrics_url:
        snapshot["client"] = collect_component_metrics(
            args.client_metrics_url, args.metrics_timeout
        )
    else:
        snapshot["client"] = {
            "available": False,
            "reason": (
                "RealClient exposes /metrics only when its embedded HTTP server "
                "is enabled; this benchmark does not enable that C++ gflag. "
                "Pass --client-metrics-url to collect an externally enabled endpoint."
            ),
        }

    if args.worker_metrics_url:
        snapshot["worker"] = collect_component_metrics(
            args.worker_metrics_url, args.metrics_timeout
        )
    else:
        snapshot["worker"] = {
            "available": False,
            "reason": (
                "No equivalent worker HTTP metrics endpoint was found in the "
                "Store benchmark path. Pass --worker-metrics-url if an external "
                "worker metrics endpoint is available."
            ),
        }
    return snapshot


def prepend_env_path(env: dict[str, str], name: str, paths: Iterable[Path]) -> None:
    existing = env.get(name, "")
    new_paths = [str(path) for path in paths if path and path.exists()]
    if existing:
        new_paths.append(existing)
    if new_paths:
        env[name] = os.pathsep.join(new_paths)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def git_info() -> dict[str, Any]:
    root = repo_root()
    status = run_command(["git", "status", "--porcelain"], cwd=root)
    return {
        "commit": run_command(["git", "rev-parse", "HEAD"], cwd=root),
        "branch": run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=root),
        "dirty": bool(status),
    }


def collect_cpu_info() -> dict[str, Any]:
    lscpu = parse_key_value_lines(run_command(["lscpu"]))
    return {
        "model_name": lscpu.get("Model name"),
        "architecture": lscpu.get("Architecture"),
        "cpu_count": os.cpu_count(),
        "sockets": lscpu.get("Socket(s)"),
        "cores_per_socket": lscpu.get("Core(s) per socket"),
        "threads_per_core": lscpu.get("Thread(s) per core"),
        "numa_nodes": lscpu.get("NUMA node(s)"),
    }


def collect_memory_info() -> dict[str, Any]:
    if psutil is not None:
        virtual_memory = psutil.virtual_memory()
        return {
            "total_bytes": int(virtual_memory.total),
            "available_bytes": int(virtual_memory.available),
        }

    meminfo = parse_key_value_lines(
        Path("/proc/meminfo").read_text() if Path("/proc/meminfo").exists() else ""
    )
    total_kib = int(meminfo.get("MemTotal", "0 kB").split()[0])
    available_kib = int(meminfo.get("MemAvailable", "0 kB").split()[0])
    return {
        "total_bytes": total_kib * 1024,
        "available_bytes": available_kib * 1024,
    }


def collect_gpu_info() -> dict[str, Any]:
    output = run_command(
        [
            "nvidia-smi",
            "--query-gpu=index,name,memory.total,driver_version",
            "--format=csv,noheader,nounits",
        ]
    )
    if output is None:
        return {"available": False, "reason": "nvidia-smi not found or failed"}
    gpus = []
    for line in output.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 4:
            continue
        index, name, memory_total_mib, driver_version = parts
        gpus.append(
            {
                "index": int(index),
                "name": name,
                "memory_total_mib": int(memory_total_mib),
                "driver_version": driver_version,
            }
        )
    return {"available": bool(gpus), "gpus": gpus}


def collect_environment_info() -> dict[str, Any]:
    return {
        "git": git_info(),
        "os": {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "glibc": platform.libc_ver(),
        },
        "python": {
            "version": sys.version.split()[0],
            "executable": sys.executable,
        },
        "cpu": collect_cpu_info(),
        "memory": collect_memory_info(),
        "gpu": collect_gpu_info(),
    }


def import_store_class(build_dir: Path | None) -> tuple[type, str]:
    if build_dir is not None:
        build_dir = build_dir.resolve()
        integration_dir = build_dir / "mooncake-integration"
        common_dir = build_dir / "mooncake-common"
        libasio = common_dir / "libasio.so"
        if libasio.exists():
            ctypes.CDLL(str(libasio), mode=getattr(ctypes, "RTLD_GLOBAL", 0))
        sys.path.insert(0, str(integration_dir))
        module = importlib.import_module("store")
        return module.MooncakeDistributedStore, "build-tree"

    try:
        module = importlib.import_module("mooncake.store")
        return module.MooncakeDistributedStore, "installed-package"
    except ImportError:
        module = importlib.import_module("store")
        return module.MooncakeDistributedStore, "top-level-module"


@dataclass
class MasterProcess:
    process: subprocess.Popen[str]
    log_path: Path
    log_file: Any

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log_file.close()


def start_master(args: argparse.Namespace) -> MasterProcess | None:
    if not args.start_master:
        return None

    master_bin = Path(args.master_bin) if args.master_bin else None
    if master_bin is None:
        if args.build_dir is None:
            raise ValueError("--start-master requires --master-bin or --build-dir")
        master_bin = Path(args.build_dir) / "mooncake-store" / "src" / "mooncake_master"
    master_bin = master_bin.resolve()
    if not master_bin.exists():
        raise FileNotFoundError(f"mooncake_master not found: {master_bin}")

    rpc_host, rpc_port = split_host_port(args.master_addr)
    metadata = urllib.parse.urlparse(args.metadata_url)
    if metadata.hostname is None or metadata.port is None:
        raise ValueError("--metadata-url must include host and port")

    env = os.environ.copy()
    if args.build_dir:
        build_dir = Path(args.build_dir).resolve()
        prepend_env_path(env, "LD_LIBRARY_PATH", [build_dir / "mooncake-common"])
    prepend_env_path(env, "LD_LIBRARY_PATH", [Path("/usr/local/lib")])

    if args.master_log:
        log_path = Path(args.master_log)
    else:
        fd, temp_path = tempfile.mkstemp(prefix="mooncake_master_", suffix=".log")
        os.close(fd)
        log_path = Path(temp_path)
    log_file = log_path.open("w")
    cmd = [
        str(master_bin),
        "--enable_http_metadata_server=true",
        f"--http_metadata_server_host={metadata.hostname}",
        f"--http_metadata_server_port={metadata.port}",
        f"--rpc_address={rpc_host}",
        f"--rpc_port={rpc_port}",
        "--enable_metric_reporting=true",
        f"--metrics_port={args.master_metrics_port}",
        "--logtostderr=true",
    ]
    cmd.extend(args.master_extra_flag)
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    master = MasterProcess(process=process, log_path=log_path, log_file=log_file)
    try:
        wait_for_http_metadata(args.metadata_url, args.startup_timeout)
        wait_for_port(rpc_host, rpc_port, args.startup_timeout)
        wait_for_port("127.0.0.1", args.master_metrics_port, args.startup_timeout)
    except Exception:
        master.stop()
        raise
    return master


@dataclass
class OperationStats:
    latencies_ms: list[float] = field(default_factory=list)
    api_calls: int = 0
    object_count: int = 0
    bytes_transferred: int = 0
    failures: int = 0
    misses: int = 0
    error_codes: dict[str, int] = field(default_factory=dict)
    miss_codes: dict[str, int] = field(default_factory=dict)
    started_at: float = 0.0
    ended_at: float = 0.0

    def begin(self) -> None:
        self.started_at = time.perf_counter()

    def finish(self) -> None:
        self.ended_at = time.perf_counter()

    def record(
        self, latency_seconds: float, objects: int, byte_count: int, failures: int = 0
    ) -> None:
        self.api_calls += 1
        self.object_count += objects
        self.bytes_transferred += byte_count
        self.failures += failures
        self.latencies_ms.append(latency_seconds * 1000.0)

    def record_error(self, code: int) -> None:
        key = str(code)
        self.error_codes[key] = self.error_codes.get(key, 0) + 1

    def record_miss(self, code: int = OBJECT_NOT_FOUND_CODE) -> None:
        self.misses += 1
        key = str(code)
        self.miss_codes[key] = self.miss_codes.get(key, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        wall_time = max(self.ended_at - self.started_at, 0.0)
        succeeded = self.object_count - self.failures
        return {
            "api_calls": self.api_calls,
            "objects": self.object_count,
            "succeeded": succeeded,
            "hits": max(succeeded - self.misses, 0),
            "misses": self.misses,
            "failures": self.failures,
            "error_codes": self.error_codes,
            "miss_codes": self.miss_codes,
            "bytes": self.bytes_transferred,
            "wall_time_s": wall_time,
            "api_calls_per_s": self.api_calls / wall_time if wall_time else 0.0,
            "objects_per_s": self.object_count / wall_time if wall_time else 0.0,
            "throughput_mib_s": self.bytes_transferred / wall_time / BYTES_PER_MIB
            if wall_time
            else 0.0,
            "latency_ms": {
                "min": min(self.latencies_ms) if self.latencies_ms else 0.0,
                "mean": statistics.mean(self.latencies_ms)
                if self.latencies_ms
                else 0.0,
                "p50": percentile(self.latencies_ms, 50),
                "p95": percentile(self.latencies_ms, 95),
                "p99": percentile(self.latencies_ms, 99),
                "max": max(self.latencies_ms) if self.latencies_ms else 0.0,
            },
        }


class MemoryTracker:
    def __init__(self, interval_s: float = 1.0) -> None:
        self.available = psutil is not None
        self.interval_s = interval_s
        self.process = psutil.Process() if self.available else None
        self.before_rss_bytes: int | None = None
        self.after_rss_bytes: int | None = None
        self.peak_rss_bytes: int | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def current_rss(self) -> int | None:
        if self.process is None:
            return None
        return int(self.process.memory_info().rss)

    def __enter__(self) -> "MemoryTracker":
        if not self.available:
            return self
        self.before_rss_bytes = self.current_rss()
        self.peak_rss_bytes = self.before_rss_bytes
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not self.available:
            return
        self.after_rss_bytes = self.current_rss()
        if self.after_rss_bytes is not None:
            self.peak_rss_bytes = max(self.peak_rss_bytes or 0, self.after_rss_bytes)
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 0.1)

    def _sample_loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            rss = self.current_rss()
            if rss is not None:
                self.peak_rss_bytes = max(self.peak_rss_bytes or 0, rss)

    def as_dict(self) -> dict[str, Any]:
        if not self.available:
            return {
                "available": False,
                "reason": "psutil is not installed; install psutil to enable RSS tracking",
            }
        before = self.before_rss_bytes or 0
        after = self.after_rss_bytes or before
        delta = after - before
        delta_percent = (delta / before * 100.0) if before else 0.0
        return {
            "available": True,
            "rss_before_bytes": before,
            "rss_after_bytes": after,
            "rss_delta_bytes": delta,
            "rss_delta_percent": delta_percent,
            "peak_rss_bytes": self.peak_rss_bytes or after,
            "sample_interval_s": self.interval_s,
        }


class TimeBucketRecorder:
    def __init__(self, interval_s: float, started_at: float) -> None:
        self.interval_s = interval_s
        self.started_at = started_at
        self.enabled = interval_s > 0
        self._lock = threading.Lock()
        self._buckets: dict[int, OperationStats] = {}
        self._operation_buckets: dict[int, dict[str, OperationStats]] = {}

    def record(
        self,
        operation: str,
        completed_at: float,
        latency_seconds: float,
        objects: int,
        byte_count: int,
        failures: int,
        misses: int = 0,
        error_code: int | None = None,
        miss_code: int | None = None,
    ) -> None:
        if not self.enabled:
            return
        elapsed = max(completed_at - self.started_at, 0.0)
        index = int(elapsed // self.interval_s)
        bucket_key = index
        with self._lock:
            bucket = self._buckets.get(bucket_key)
            if bucket is None:
                bucket = OperationStats(
                    started_at=index * self.interval_s,
                    ended_at=(index + 1) * self.interval_s,
                )
                self._buckets[bucket_key] = bucket
            operation_buckets = self._operation_buckets.setdefault(bucket_key, {})
            operation_bucket = operation_buckets.get(operation)
            if operation_bucket is None:
                operation_bucket = OperationStats(
                    started_at=index * self.interval_s,
                    ended_at=(index + 1) * self.interval_s,
                )
                operation_buckets[operation] = operation_bucket
            self._record_into_stats(
                bucket,
                latency_seconds,
                objects,
                byte_count,
                failures,
                misses,
                error_code,
                miss_code,
            )
            self._record_into_stats(
                operation_bucket,
                latency_seconds,
                objects,
                byte_count,
                failures,
                misses,
                error_code,
                miss_code,
            )

    @staticmethod
    def _record_into_stats(
        stats: OperationStats,
        latency_seconds: float,
        objects: int,
        byte_count: int,
        failures: int,
        misses: int,
        error_code: int | None,
        miss_code: int | None,
    ) -> None:
        stats.api_calls += 1
        stats.object_count += objects
        stats.bytes_transferred += byte_count
        stats.failures += failures
        stats.misses += misses
        stats.latencies_ms.append(latency_seconds * 1000.0)
        if error_code is not None:
            key = str(error_code)
            stats.error_codes[key] = stats.error_codes.get(key, 0) + 1
        if miss_code is not None:
            key = str(miss_code)
            stats.miss_codes[key] = stats.miss_codes.get(key, 0) + 1

    def as_dict(self, actual_duration_s: float) -> dict[str, Any]:
        if not self.enabled:
            return {
                "enabled": False,
                "reason": "--trend-interval must be greater than 0",
            }
        buckets = []
        for index in sorted(self._buckets):
            bucket = self._buckets[index]
            bucket_start = index * self.interval_s
            bucket_end = min((index + 1) * self.interval_s, actual_duration_s)
            bucket_duration = max(bucket_end - bucket_start, 0.0)
            bucket.started_at = 0.0
            bucket.ended_at = bucket_duration
            item = bucket.as_dict()
            item.update(
                {
                    "bucket_index": index,
                    "start_s": bucket_start,
                    "end_s": bucket_end,
                    "duration_s": bucket_duration,
                }
            )
            operations = {}
            for operation, stats in sorted(
                self._operation_buckets.get(index, {}).items()
            ):
                stats.started_at = 0.0
                stats.ended_at = bucket_duration
                operations[operation] = stats.as_dict()
            item["operations"] = operations
            buckets.append(item)
        return {
            "enabled": True,
            "interval_s": self.interval_s,
            "bucket_count": len(buckets),
            "buckets": buckets,
            "analysis": analyze_trend(buckets, self.interval_s),
        }


def analyze_trend(
    buckets: Sequence[dict[str, Any]], interval_s: float
) -> dict[str, Any]:
    minimum_bucket_duration = interval_s * 0.5
    valid = [
        bucket
        for bucket in buckets
        if bucket.get("duration_s", 0.0) >= minimum_bucket_duration
    ]
    if len(valid) < 2:
        return {
            "available": False,
            "reason": "at least two non-empty buckets are required",
        }

    def first_last_delta(path: Sequence[str]) -> dict[str, Any]:
        def get_path(source: dict[str, Any]) -> float:
            value: Any = source
            for part in path:
                if not isinstance(value, dict):
                    return 0.0
                value = value.get(part, 0.0)
            return float(value or 0.0)

        first = get_path(valid[0])
        last = get_path(valid[-1])
        delta = last - first
        delta_percent = None if first == 0.0 else delta / first * 100.0
        return {
            "first": first,
            "last": last,
            "delta": delta,
            "delta_percent": delta_percent,
        }

    throughput = first_last_delta(("api_calls_per_s",))
    p99 = first_last_delta(("latency_ms", "p99"))
    throughput_drop_percent = throughput.get("delta_percent")
    p99_growth_percent = p99.get("delta_percent")
    return {
        "available": True,
        "api_calls_per_s": throughput,
        "latency_p99_ms": p99,
        "performance_decline_detected": (
            throughput_drop_percent is not None and throughput_drop_percent < -10.0
        )
        or (p99_growth_percent is not None and p99_growth_percent > 20.0),
    }


def make_payload(size: int, salt: str = "") -> bytes:
    if size <= 0:
        raise ValueError("--value-size must be positive")
    pattern = f"MooncakeStoreBenchmark:{salt}:".encode()
    repeats = (size + len(pattern) - 1) // len(pattern)
    return (pattern * repeats)[:size]


def chunks(items: Sequence[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(items), size):
        yield list(items[offset : offset + size])


def setup_store(args: argparse.Namespace, StoreClass: type):
    store = StoreClass()
    ret = store.setup(
        args.local_hostname,
        args.metadata_url,
        args.global_segment_size_mb * BYTES_PER_MIB,
        args.local_buffer_size_mb * BYTES_PER_MIB,
        args.protocol,
        args.rdma_devices,
        args.master_addr,
    )
    if ret != 0:
        raise RuntimeError(f"store.setup failed with code {ret}")
    return store


def batch_put(store: Any, keys: Sequence[str], payload: bytes, batch_size: int) -> None:
    for group in chunks(keys, batch_size):
        ret = store.put_batch(group, [payload] * len(group))
        if ret != 0:
            raise RuntimeError(f"put_batch failed with code {ret}")


def batch_put_values(
    store: Any, keys: Sequence[str], values: Sequence[bytes], batch_size: int
) -> None:
    if len(keys) != len(values):
        raise ValueError("keys and values must have the same length")
    for offset in range(0, len(keys), batch_size):
        group_keys = list(keys[offset : offset + batch_size])
        group_values = list(values[offset : offset + batch_size])
        ret = store.put_batch(group_keys, group_values)
        if ret != 0:
            raise RuntimeError(f"put_batch failed with code {ret}")


def batch_remove(store: Any, keys: Sequence[str], batch_size: int) -> None:
    for group in chunks(keys, batch_size):
        store.batch_remove(group, True)


def warmup(store: Any, args: argparse.Namespace, payload: bytes) -> None:
    if args.warmup <= 0:
        return
    keys = [f"{args.key_prefix}:warmup:{i}" for i in range(args.warmup)]
    batch_put(store, keys, payload, max(1, min(args.batch_size, args.warmup)))
    for key in keys:
        store.get(key)
    batch_remove(store, keys, max(1, min(args.batch_size, args.warmup)))


def value_size_bounds(args: argparse.Namespace) -> tuple[int, int]:
    if args.value_size_min is not None:
        minimum = args.value_size_min
    else:
        minimum = max(1, args.value_size // 4)
    maximum = (
        args.value_size_max
        if args.value_size_max is not None
        else max(minimum, args.value_size * 4)
    )
    if minimum <= 0 or maximum <= 0:
        raise ValueError("value size bounds must be positive")
    if minimum > maximum:
        raise ValueError("--value-size-min must be <= --value-size-max")
    return minimum, maximum


def random_payload(rng: random.Random, minimum: int, maximum: int, salt: str) -> bytes:
    size = rng.randint(minimum, maximum)
    return make_payload(size, salt)


def choose_weighted(rng: random.Random, weights: dict[str, float]) -> str:
    sample = rng.random() * sum(weights.values())
    cumulative = 0.0
    for name, weight in weights.items():
        cumulative += weight
        if sample <= cumulative:
            return name
    return next(reversed(weights))


def run_put(store: Any, keys: Sequence[str], payload: bytes) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        ret = store.put(key, payload)
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 0 else 1
        if ret != 0:
            stats.record_error(ret)
        stats.record(elapsed, 1, len(payload), failed)
    stats.finish()
    return stats


def run_get(store: Any, keys: Sequence[str], value_size: int) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        value = store.get(key)
        elapsed = time.perf_counter() - start
        failed = 0 if len(value) == value_size else 1
        if failed:
            stats.record_error(-1)
        stats.record(elapsed, 1, len(value), failed)
    stats.finish()
    return stats


def run_remove(store: Any, keys: Sequence[str]) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        ret = store.remove(key, True)
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 0 else 1
        if ret != 0:
            stats.record_error(ret)
        stats.record(elapsed, 1, 0, failed)
    stats.finish()
    return stats


def run_upsert(store: Any, keys: Sequence[str], payload: bytes) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        ret = store.upsert(key, payload)
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 0 else 1
        if ret != 0:
            stats.record_error(ret)
        stats.record(elapsed, 1, len(payload), failed)
    stats.finish()
    return stats


def run_is_exist(store: Any, keys: Sequence[str]) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        ret = store.is_exist(key)
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 1 else 1
        if failed:
            stats.record_error(ret)
        stats.record(elapsed, 1, 0, failed)
    stats.finish()
    return stats


def run_get_size(store: Any, keys: Sequence[str], value_size: int) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for key in keys:
        start = time.perf_counter()
        actual_size = store.get_size(key)
        elapsed = time.perf_counter() - start
        failed = 0 if actual_size == value_size else 1
        if failed:
            stats.record_error(int(actual_size))
        stats.record(elapsed, 1, 0, failed)
    stats.finish()
    return stats


def run_put_batch(
    store: Any, keys: Sequence[str], payload: bytes, batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        start = time.perf_counter()
        ret = store.put_batch(group, [payload] * len(group))
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 0 else len(group)
        if ret != 0:
            stats.record_error(ret)
        stats.record(elapsed, len(group), len(group) * len(payload), failed)
    stats.finish()
    return stats


def run_get_batch(
    store: Any, keys: Sequence[str], value_size: int, batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        start = time.perf_counter()
        values = store.get_batch(group)
        elapsed = time.perf_counter() - start
        failures = 0
        byte_count = 0
        if len(values) != len(group):
            failures = len(group)
            stats.record_error(-1)
        else:
            for value in values:
                byte_count += len(value)
                if len(value) != value_size:
                    failures += 1
                    stats.record_error(-1)
        stats.record(elapsed, len(group), byte_count, failures)
    stats.finish()
    return stats


def run_upsert_batch(
    store: Any, keys: Sequence[str], payload: bytes, batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        start = time.perf_counter()
        ret = store.upsert_batch(group, [payload] * len(group))
        elapsed = time.perf_counter() - start
        failed = 0 if ret == 0 else len(group)
        if ret != 0:
            stats.record_error(ret)
        stats.record(elapsed, len(group), len(group) * len(payload), failed)
    stats.finish()
    return stats


def run_batch_is_exist(
    store: Any, keys: Sequence[str], batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        start = time.perf_counter()
        codes = store.batch_is_exist(group)
        elapsed = time.perf_counter() - start
        failures = 0
        if len(codes) != len(group):
            failures = len(group)
            stats.record_error(-1)
        else:
            for code in codes:
                if code != 1:
                    failures += 1
                    stats.record_error(code)
        stats.record(elapsed, len(group), 0, failures)
    stats.finish()
    return stats


def run_batch_remove(
    store: Any, keys: Sequence[str], batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        start = time.perf_counter()
        codes = store.batch_remove(group, True)
        elapsed = time.perf_counter() - start
        failures = 0
        for code in codes:
            if code != 0:
                failures += 1
                stats.record_error(code)
        stats.record(elapsed, len(group), 0, failures)
    stats.finish()
    return stats


def run_remove_by_regex(
    store: Any, keys: Sequence[str], batch_size: int
) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    for group in chunks(keys, batch_size):
        if not group:
            continue
        pattern = "^(?:" + "|".join(re.escape(key) for key in group) + ")$"
        start = time.perf_counter()
        removed = store.remove_by_regex(pattern, True)
        elapsed = time.perf_counter() - start
        failures = 0 if removed >= len(group) else len(group) - max(int(removed), 0)
        if failures:
            stats.record_error(int(removed))
        stats.record(elapsed, len(group), 0, failures)
    stats.finish()
    return stats


def run_remove_all(store: Any, keys: Sequence[str]) -> OperationStats:
    stats = OperationStats()
    stats.begin()
    start = time.perf_counter()
    removed = store.remove_all(True)
    elapsed = time.perf_counter() - start
    failures = 0 if removed >= len(keys) else len(keys) - max(int(removed), 0)
    if failures:
        stats.record_error(int(removed))
    stats.record(elapsed, max(int(removed), len(keys)), 0, failures)
    stats.finish()
    return stats


def split_evenly(items: Sequence[str], parts: int) -> list[list[str]]:
    partitions: list[list[str]] = [[] for _ in range(parts)]
    for index, item in enumerate(items):
        partitions[index % parts].append(item)
    return partitions


def merge_operation_stats(
    stats_list: Sequence[OperationStats], wall_time_s: float | None = None
) -> OperationStats:
    merged = OperationStats()
    for stats in stats_list:
        merged.latencies_ms.extend(stats.latencies_ms)
        merged.api_calls += stats.api_calls
        merged.object_count += stats.object_count
        merged.bytes_transferred += stats.bytes_transferred
        merged.failures += stats.failures
        merged.misses += stats.misses
        for code, count in stats.error_codes.items():
            merged.error_codes[code] = merged.error_codes.get(code, 0) + count
        for code, count in stats.miss_codes.items():
            merged.miss_codes[code] = merged.miss_codes.get(code, 0) + count
    if wall_time_s is not None:
        merged.started_at = 0.0
        merged.ended_at = wall_time_s
    elif stats_list:
        merged.started_at = min(stats.started_at for stats in stats_list)
        merged.ended_at = max(stats.ended_at for stats in stats_list)
    return merged


def operation_thread_stats(
    name: str,
    store: Any,
    keys: Sequence[str],
    payload: bytes,
) -> OperationStats:
    if name == "put":
        return run_put(store, keys, payload)
    if name == "get":
        return run_get(store, keys, len(payload))
    if name == "remove":
        return run_remove(store, keys)
    if name == "upsert":
        return run_upsert(store, keys, payload)
    if name == "is_exist":
        return run_is_exist(store, keys)
    if name == "get_size":
        return run_get_size(store, keys, len(payload))
    raise ValueError(f"concurrency is not supported for operation {name!r}")


def run_concurrent_operation(
    name: str,
    store: Any,
    args: argparse.Namespace,
    key_partitions: Sequence[Sequence[str]],
    payload: bytes,
) -> tuple[OperationStats, dict[str, Any]]:
    started_at = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        futures = [
            executor.submit(operation_thread_stats, name, store, partition, payload)
            for partition in key_partitions
        ]
        thread_stats = [future.result() for future in futures]
    wall_time_s = time.perf_counter() - started_at

    aggregate = merge_operation_stats(thread_stats, wall_time_s=wall_time_s)
    thread_results = []
    thread_api_throughputs = []
    thread_object_throughputs = []
    for index, stats in enumerate(thread_stats):
        stats_dict = stats.as_dict()
        thread_api_throughputs.append(stats_dict["api_calls_per_s"])
        thread_object_throughputs.append(stats_dict["objects_per_s"])
        thread_results.append(
            {
                "thread_index": index,
                "key_count": len(key_partitions[index]),
                "api_calls_per_s": stats_dict["api_calls_per_s"],
                "objects_per_s": stats_dict["objects_per_s"],
                "latency_ms": stats_dict["latency_ms"],
                "misses": stats_dict["misses"],
                "failures": stats_dict["failures"],
                "error_codes": stats_dict["error_codes"],
                "miss_codes": stats_dict["miss_codes"],
            }
        )

    mean_api_throughput, stdev_api_throughput = mean_and_stdev(thread_api_throughputs)
    mean_object_throughput, stdev_object_throughput = mean_and_stdev(
        thread_object_throughputs
    )
    extras = {
        "concurrency": args.concurrency,
        "concurrency_stats": {
            "total_ops_per_s": sum(thread_api_throughputs),
            "total_objects_per_s": sum(thread_object_throughputs),
            "thread_ops_per_s_mean": mean_api_throughput,
            "thread_ops_per_s_stdev": stdev_api_throughput,
            "thread_objects_per_s_mean": mean_object_throughput,
            "thread_objects_per_s_stdev": stdev_object_throughput,
            "latency_ms_merged": {
                "p50": percentile(aggregate.latencies_ms, 50),
                "p95": percentile(aggregate.latencies_ms, 95),
                "p99": percentile(aggregate.latencies_ms, 99),
            },
            "threads": thread_results,
        },
    }
    return aggregate, extras


def run_mixed_worker(
    store: Any,
    args: argparse.Namespace,
    worker_index: int,
    deadline: float,
    active_keys: list[str],
    active_sizes: dict[str, int],
    key_lock: threading.Lock,
    value_min: int,
    value_max: int,
    trend: TimeBucketRecorder | None = None,
) -> dict[str, OperationStats]:
    rng = random.Random(args.random_seed + worker_index)
    weights = MIX_PROFILES[args.mix]
    stats = {operation: OperationStats() for operation in weights}
    for operation_stats in stats.values():
        operation_stats.begin()

    sequence = 0
    while time.perf_counter() < deadline:
        operation = choose_weighted(rng, weights)
        if operation == "put":
            with key_lock:
                active_key_count = len(active_keys)
            if args.max_active_keys > 0 and active_key_count >= args.max_active_keys:
                operation = "get"
            else:
                sequence += 1
                key = f"{args.key_prefix}:mixed:t{worker_index}:{sequence}:{time.time_ns()}"
                payload = random_payload(rng, value_min, value_max, key)
                start = time.perf_counter()
                ret = store.put(key, payload)
                elapsed = time.perf_counter() - start
                completed_at = time.perf_counter()
                failed = 0 if ret == 0 else 1
                error_code = None
                if ret != 0:
                    stats["put"].record_error(ret)
                    error_code = ret
                else:
                    with key_lock:
                        active_keys.append(key)
                        active_sizes[key] = len(payload)
                stats["put"].record(elapsed, 1, len(payload), failed)
                if trend is not None:
                    trend.record(
                        "put",
                        completed_at,
                        elapsed,
                        1,
                        len(payload),
                        failed,
                        error_code=error_code,
                    )
                continue

        if operation == "get":
            with key_lock:
                if active_keys:
                    key = rng.choice(active_keys)
                    expected_size = active_sizes.get(key, 0)
                else:
                    key = ""
                    expected_size = 0
            if not key:
                continue

            start = time.perf_counter()
            value = store.get(key)
            elapsed = time.perf_counter() - start
            completed_at = time.perf_counter()
            failed = 0 if len(value) == expected_size else 1
            misses = 0
            error_code = None
            miss_code = None
            if failed:
                if len(value) == 0:
                    failed = 0
                    stats["get"].record_miss()
                    misses = 1
                    miss_code = OBJECT_NOT_FOUND_CODE
                else:
                    with key_lock:
                        key_still_active = key in active_sizes
                    if key_still_active:
                        stats["get"].record_error(-1)
                        error_code = -1
                    else:
                        failed = 0
                        stats["get"].record_miss()
                        misses = 1
                        miss_code = OBJECT_NOT_FOUND_CODE
            stats["get"].record(elapsed, 1, len(value), failed)
            if trend is not None:
                trend.record(
                    "get",
                    completed_at,
                    elapsed,
                    1,
                    len(value),
                    failed,
                    misses=misses,
                    error_code=error_code,
                    miss_code=miss_code,
                )
        elif operation == "remove":
            with key_lock:
                if active_keys:
                    key = rng.choice(active_keys)
                    active_sizes.pop(key, None)
                    try:
                        active_keys.remove(key)
                    except ValueError:
                        pass
                else:
                    key = ""
            if not key:
                continue

            start = time.perf_counter()
            ret = store.remove(key, True)
            elapsed = time.perf_counter() - start
            completed_at = time.perf_counter()
            failed = 0 if ret == 0 else 1
            misses = 0
            error_code = None
            miss_code = None
            if ret != 0:
                if ret != OBJECT_NOT_FOUND_CODE:
                    stats["remove"].record_error(ret)
                    error_code = ret
                else:
                    failed = 0
                    stats["remove"].record_miss(ret)
                    misses = 1
                    miss_code = ret
            stats["remove"].record(elapsed, 1, 0, failed)
            if trend is not None:
                trend.record(
                    "remove",
                    completed_at,
                    elapsed,
                    1,
                    0,
                    failed,
                    misses=misses,
                    error_code=error_code,
                    miss_code=miss_code,
                )

    for operation_stats in stats.values():
        operation_stats.finish()
    return stats


def run_mixed_workload(
    store: Any,
    args: argparse.Namespace,
    keys: Sequence[str],
) -> dict[str, Any]:
    value_min, value_max = value_size_bounds(args)
    rng = random.Random(args.random_seed)
    initial_values = [random_payload(rng, value_min, value_max, key) for key in keys]
    batch_remove(store, keys, args.batch_size)
    batch_put_values(store, keys, initial_values, args.batch_size)

    active_keys = list(keys)
    active_sizes = {key: len(value) for key, value in zip(keys, initial_values)}
    key_lock = threading.Lock()
    duration_s = args.duration if args.duration > 0 else 30.0
    started_at = time.perf_counter()
    deadline = started_at + duration_s
    trend = TimeBucketRecorder(args.trend_interval, started_at)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        futures = [
            executor.submit(
                run_mixed_worker,
                store,
                args,
                worker_index,
                deadline,
                active_keys,
                active_sizes,
                key_lock,
                value_min,
                value_max,
                trend,
            )
            for worker_index in range(args.concurrency)
        ]
        worker_results = [future.result() for future in futures]
    wall_time_s = time.perf_counter() - started_at

    operation_results: dict[str, dict[str, Any]] = {}
    operation_stats = {}
    for operation in MIX_PROFILES[args.mix]:
        merged = merge_operation_stats(
            [worker_result[operation] for worker_result in worker_results],
            wall_time_s=wall_time_s,
        )
        operation_stats[operation] = merged
        operation_results[operation] = merged.as_dict()

    aggregate = merge_operation_stats(
        list(operation_stats.values()), wall_time_s=wall_time_s
    )
    result = aggregate.as_dict()
    with key_lock:
        remaining_keys = list(active_keys)
    if not args.skip_cleanup:
        batch_remove(store, remaining_keys, args.batch_size)
    result.update(
        {
            "duration_s": wall_time_s,
            "requested_duration_s": duration_s,
            "concurrency": args.concurrency,
            "mix": args.mix,
            "mix_weights": MIX_PROFILES[args.mix],
            "preload_key_count": len(keys),
            "remaining_key_count": len(remaining_keys),
            "max_active_keys": args.max_active_keys,
            "value_size_distribution": {
                "type": "uniform",
                "min": value_min,
                "max": value_max,
            },
            "aggregate": aggregate.as_dict(),
            "operations": operation_results,
            "trend": trend.as_dict(wall_time_s),
        }
    )
    return result


def run_smoke(store: Any, args: argparse.Namespace, payload: bytes) -> dict[str, Any]:
    key = f"{args.key_prefix}:smoke"
    put_code = store.put(key, payload)
    exists_after_put = store.is_exist(key)
    value = store.get(key)
    get_matches = value == payload
    remove_code = store.remove(key, True)
    exists_after_remove = store.is_exist(key)
    return {
        "put": put_code,
        "exists_after_put": exists_after_put,
        "get_matches": get_matches,
        "remove_force": remove_code,
        "exists_after_remove": exists_after_remove,
        "passed": put_code == 0
        and exists_after_put == 1
        and get_matches
        and remove_code == 0
        and exists_after_remove == 0,
    }


def run_operation(
    name: str,
    store: Any,
    args: argparse.Namespace,
    keys: Sequence[str],
    payload: bytes,
) -> dict[str, Any]:
    if name == "mixed":
        return run_mixed_workload(store, args, keys)

    cleanup_before = name in {
        "put",
        "put_batch",
        "upsert",
        "upsert_batch",
        "remove_by_regex",
        "remove_all",
    }
    cleanup_after = (not args.skip_cleanup) and name in {
        "put",
        "put_batch",
        "upsert",
        "upsert_batch",
        "get",
        "get_batch",
        "is_exist",
        "batch_is_exist",
        "get_size",
    }
    needs_populate = name in {
        "get",
        "get_batch",
        "remove",
        "batch_remove",
        "upsert",
        "upsert_batch",
        "is_exist",
        "batch_is_exist",
        "get_size",
        "remove_by_regex",
        "remove_all",
    }
    concurrent_run = args.concurrency > 1 and name in CONCURRENT_OPERATIONS
    key_partitions = (
        split_evenly(keys, args.concurrency) if concurrent_run else [list(keys)]
    )
    populate_keys = [key for partition in key_partitions for key in partition]

    if cleanup_before:
        batch_remove(store, populate_keys, args.batch_size)
    if needs_populate:
        batch_put(store, populate_keys, payload, args.batch_size)

    if concurrent_run:
        stats, extras = run_concurrent_operation(
            name, store, args, key_partitions, payload
        )
    else:
        runners: dict[str, Callable[[], OperationStats]] = {
            "put": lambda: run_put(store, keys, payload),
            "get": lambda: run_get(store, keys, len(payload)),
            "remove": lambda: run_remove(store, keys),
            "upsert": lambda: run_upsert(store, keys, payload),
            "is_exist": lambda: run_is_exist(store, keys),
            "get_size": lambda: run_get_size(store, keys, len(payload)),
            "put_batch": lambda: run_put_batch(store, keys, payload, args.batch_size),
            "get_batch": lambda: run_get_batch(
                store, keys, len(payload), args.batch_size
            ),
            "upsert_batch": lambda: run_upsert_batch(
                store, keys, payload, args.batch_size
            ),
            "batch_is_exist": lambda: run_batch_is_exist(store, keys, args.batch_size),
            "batch_remove": lambda: run_batch_remove(store, keys, args.batch_size),
            "remove_by_regex": lambda: run_remove_by_regex(
                store, keys, args.batch_size
            ),
            "remove_all": lambda: run_remove_all(store, keys),
        }
        stats = runners[name]()
        extras = {}

    if cleanup_after:
        batch_remove(store, populate_keys, args.batch_size)
    result = stats.as_dict()
    result.update(extras)
    return result


def summarize_repeats(iterations: Sequence[dict[str, Any]]) -> dict[str, Any]:
    metric_paths = {
        "api_calls_per_s": ("api_calls_per_s",),
        "objects_per_s": ("objects_per_s",),
        "throughput_mib_s": ("throughput_mib_s",),
        "latency_p50_ms": ("latency_ms", "p50"),
        "latency_p95_ms": ("latency_ms", "p95"),
        "latency_p99_ms": ("latency_ms", "p99"),
    }

    def get_path(source: dict[str, Any], path: Sequence[str]) -> float:
        value: Any = source
        for part in path:
            if not isinstance(value, dict):
                return 0.0
            value = value.get(part, 0.0)
        return float(value or 0.0)

    metrics = {}
    for metric_name, path in metric_paths.items():
        values = [get_path(iteration, path) for iteration in iterations]
        mean, stdev = mean_and_stdev(values)
        cov = coefficient_of_variation(values)
        metrics[metric_name] = {
            "mean": mean,
            "stdev": stdev,
            "cov": cov,
            "unstable": cov > 0.10,
        }
    return {
        "count": len(iterations),
        "metrics": metrics,
    }


def extract_repeat_iteration(
    iteration_number: int, result: dict[str, Any]
) -> dict[str, Any]:
    return {
        "iteration": iteration_number,
        "api_calls_per_s": result.get("api_calls_per_s", 0.0),
        "objects_per_s": result.get("objects_per_s", 0.0),
        "throughput_mib_s": result.get("throughput_mib_s", 0.0),
        "latency_ms": {
            "p50": result.get("latency_ms", {}).get("p50", 0.0),
            "p95": result.get("latency_ms", {}).get("p95", 0.0),
            "p99": result.get("latency_ms", {}).get("p99", 0.0),
        },
        "misses": result.get("misses", 0),
        "failures": result.get("failures", 0),
        "memory": result.get("memory", {}),
    }


def aggregate_memory_reports(iterations: Sequence[dict[str, Any]]) -> dict[str, Any]:
    memory_reports = [iteration.get("memory", {}) for iteration in iterations]
    if not memory_reports:
        return {}
    if not all(report.get("available", False) for report in memory_reports):
        return memory_reports[-1]

    before_values = [
        int(report.get("rss_before_bytes", 0)) for report in memory_reports
    ]
    after_values = [int(report.get("rss_after_bytes", 0)) for report in memory_reports]
    delta_values = [int(report.get("rss_delta_bytes", 0)) for report in memory_reports]
    peak_values = [int(report.get("peak_rss_bytes", 0)) for report in memory_reports]
    delta_percent_values = [
        float(report.get("rss_delta_percent", 0.0)) for report in memory_reports
    ]
    return {
        "available": True,
        "rss_before_bytes": before_values[0],
        "rss_after_bytes": after_values[-1],
        "rss_delta_bytes": after_values[-1] - before_values[0],
        "rss_delta_percent": (
            (after_values[-1] - before_values[0]) / before_values[0] * 100.0
        )
        if before_values[0]
        else 0.0,
        "peak_rss_bytes": max(peak_values),
        "per_repeat_rss_delta_bytes_mean": statistics.mean(delta_values),
        "per_repeat_rss_delta_percent_mean": statistics.mean(delta_percent_values),
        "sample_interval_s": memory_reports[-1].get("sample_interval_s"),
    }


def run_operation_with_repeats(
    name: str,
    store: Any,
    args: argparse.Namespace,
    keys: Sequence[str],
    payload: bytes,
) -> dict[str, Any]:
    iterations = []
    for repeat_index in range(args.repeat):
        repeat_keys = (
            keys
            if args.repeat == 1
            else [f"{key}:repeat:{repeat_index}" for key in keys]
        )
        with MemoryTracker() as memory:
            result = run_operation(name, store, args, repeat_keys, payload)
        result["memory"] = memory.as_dict()
        if args.repeat == 1:
            return result
        iterations.append(extract_repeat_iteration(repeat_index + 1, result))

    final_result = dict(result)
    final_result["memory"] = aggregate_memory_reports(iterations)
    final_result["repeat"] = {
        "count": args.repeat,
        "iterations": iterations,
        "summary": summarize_repeats(iterations),
    }
    return final_result


def parse_concurrency_scan(value: str | None) -> list[int]:
    if value is None or value == "":
        return []
    if value == "auto":
        return DEFAULT_CONCURRENCY_SCAN
    values = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        concurrency = int(item)
        if concurrency <= 0:
            raise ValueError("--concurrency-scan values must be positive")
        values.append(concurrency)
    if not values:
        raise ValueError(
            "--concurrency-scan must contain at least one concurrency value"
        )
    return values


def trend_entry(
    operation: str, concurrency: int, result: dict[str, Any]
) -> dict[str, Any]:
    return {
        "operation": operation,
        "concurrency": concurrency,
        "api_calls_per_s": result.get("api_calls_per_s", 0.0),
        "objects_per_s": result.get("objects_per_s", 0.0),
        "throughput_mib_s": result.get("throughput_mib_s", 0.0),
        "misses": result.get("misses", 0),
        "latency_p50_ms": result.get("latency_ms", {}).get("p50", 0.0),
        "latency_p95_ms": result.get("latency_ms", {}).get("p95", 0.0),
        "latency_p99_ms": result.get("latency_ms", {}).get("p99", 0.0),
        "failures": result.get("failures", 0),
    }


def run_concurrency_scan(
    operations: Sequence[str],
    store: Any,
    args: argparse.Namespace,
    payload: bytes,
) -> dict[str, Any]:
    scan_values = parse_concurrency_scan(args.concurrency_scan)
    original_concurrency = args.concurrency
    runs = []
    trend = []
    try:
        for concurrency in scan_values:
            args.concurrency = concurrency
            operation_results = {}
            for operation in operations:
                keys = [
                    f"{args.key_prefix}:scan:c{concurrency}:{operation}:{i}"
                    for i in range(args.num_keys)
                ]
                result = run_operation_with_repeats(
                    operation, store, args, keys, payload
                )
                operation_results[operation] = result
                trend.append(trend_entry(operation, concurrency, result))
            runs.append(
                {
                    "concurrency": concurrency,
                    "results": operation_results,
                }
            )
    finally:
        args.concurrency = original_concurrency

    return {
        "operation": args.operation,
        "concurrency_values": scan_values,
        "runs": runs,
        "trend": trend,
    }


def child_output_path(parent_output: str | None, label: str) -> Path:
    if parent_output:
        output_path = Path(parent_output)
        stem = output_path.stem or "store_api_benchmark"
        suffix = output_path.suffix or ".json"
        return output_path.with_name(f"{stem}.{label}{suffix}")
    fd, temp_path = tempfile.mkstemp(
        prefix=f"mooncake_store_{label}_",
        suffix=".json",
    )
    os.close(fd)
    return Path(temp_path)


def build_multi_client_child_command(
    args: argparse.Namespace,
    client_index: int,
    output_path: Path,
    key_prefix: str,
) -> list[str]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--operation",
        "mixed",
        "--build-dir",
        str(Path(args.build_dir).resolve()),
        "--metadata-url",
        args.metadata_url,
        "--master-addr",
        args.master_addr,
        "--master-metrics-port",
        str(args.master_metrics_port),
        "--master-metrics-url",
        args.master_metrics_url,
        "--metrics-timeout",
        str(args.metrics_timeout),
        "--num-keys",
        str(args.num_keys),
        "--value-size",
        str(args.value_size),
        "--batch-size",
        str(args.batch_size),
        "--concurrency",
        str(args.concurrency),
        "--duration",
        str(args.duration if args.duration > 0 else 30.0),
        "--mix",
        args.mix,
        "--random-seed",
        str(args.random_seed + client_index * 1009),
        "--warmup",
        str(args.warmup),
        "--key-prefix",
        key_prefix,
        "--max-active-keys",
        str(args.max_active_keys),
        "--protocol",
        args.protocol,
        "--local-hostname",
        args.local_hostname,
        "--global-segment-size-mb",
        str(args.global_segment_size_mb),
        "--local-buffer-size-mb",
        str(args.local_buffer_size_mb),
        "--trend-interval",
        str(args.trend_interval),
        "--output-json",
        str(output_path),
        "--skip-metrics",
    ]
    if args.value_size_min is not None:
        command.extend(["--value-size-min", str(args.value_size_min)])
    if args.value_size_max is not None:
        command.extend(["--value-size-max", str(args.value_size_max)])
    if args.rdma_devices:
        command.extend(["--rdma-devices", args.rdma_devices])
    if args.skip_cleanup:
        command.append("--skip-cleanup")
    return command


def run_child_benchmark_process(
    command: Sequence[str],
    output_path: Path,
    client_index: int | None,
    key_prefix: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    process = subprocess.Popen(
        list(command),
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate()
    report = json.loads(output_path.read_text()) if output_path.exists() else None
    process_result = {
        "client_index": client_index,
        "returncode": process.returncode,
        "output_json": str(output_path),
        "key_prefix": key_prefix,
        "command": list(command),
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-4000:],
        "result": extract_mixed_result(report) if report is not None else {},
    }
    return process_result, report


def extract_mixed_result(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("results", {}).get("mixed", {})


def aggregate_multi_client_results(
    client_reports: Sequence[dict[str, Any]], wall_time_s: float
) -> dict[str, Any]:
    mixed_results = [extract_mixed_result(report) for report in client_reports]
    api_calls = sum(int(result.get("api_calls", 0)) for result in mixed_results)
    objects = sum(int(result.get("objects", 0)) for result in mixed_results)
    bytes_transferred = sum(int(result.get("bytes", 0)) for result in mixed_results)
    failures = sum(int(result.get("failures", 0)) for result in mixed_results)
    misses = sum(int(result.get("misses", 0)) for result in mixed_results)
    return {
        "api_calls": api_calls,
        "objects": objects,
        "bytes": bytes_transferred,
        "failures": failures,
        "misses": misses,
        "wall_time_s": wall_time_s,
        "api_calls_per_s": api_calls / wall_time_s if wall_time_s else 0.0,
        "objects_per_s": objects / wall_time_s if wall_time_s else 0.0,
        "throughput_mib_s": bytes_transferred / wall_time_s / BYTES_PER_MIB
        if wall_time_s
        else 0.0,
        "failure_rate": failures / api_calls if api_calls else 0.0,
        "miss_rate": misses / api_calls if api_calls else 0.0,
    }


def compare_multi_client_to_baseline(
    baseline_result: dict[str, Any],
    aggregate: dict[str, Any],
) -> dict[str, Any]:
    comparison = {}
    metric_paths = {
        "api_calls_per_s": ("api_calls_per_s",),
        "objects_per_s": ("objects_per_s",),
        "throughput_mib_s": ("throughput_mib_s",),
        "failure_rate": ("failure_rate",),
    }

    def get_path(source: dict[str, Any], path: Sequence[str]) -> float:
        value: Any = source
        for part in path:
            if not isinstance(value, dict):
                return 0.0
            value = value.get(part, 0.0)
        return float(value or 0.0)

    for metric, path in metric_paths.items():
        baseline_value = get_path(baseline_result, path)
        current_value = get_path(aggregate, path)
        delta = current_value - baseline_value
        comparison[metric] = {
            "single_client": baseline_value,
            "multi_client": current_value,
            "delta": delta,
            "delta_percent": None
            if baseline_value == 0.0
            else delta / baseline_value * 100.0,
        }
    return comparison


def run_multi_client_mixed(args: argparse.Namespace) -> dict[str, Any]:
    if args.multi_client_count <= 1:
        raise ValueError("--multi-client-count must be greater than 1")
    if not args.build_dir:
        raise ValueError("multi-client benchmark requires --build-dir")

    baseline = None
    baseline_report = None
    if not args.skip_single_client_baseline:
        baseline_output = child_output_path(args.output_json, "single_client")
        baseline_key_prefix = f"{args.key_prefix}:single_client"
        baseline_command = build_multi_client_child_command(
            args,
            0,
            baseline_output,
            baseline_key_prefix,
        )
        baseline, baseline_report = run_child_benchmark_process(
            baseline_command,
            baseline_output,
            None,
            baseline_key_prefix,
        )

    output_paths = [
        child_output_path(args.output_json, f"client{client_index}")
        for client_index in range(args.multi_client_count)
    ]
    commands = [
        build_multi_client_child_command(
            args,
            client_index,
            output_paths[client_index],
            f"{args.key_prefix}:client{client_index}",
        )
        for client_index in range(args.multi_client_count)
    ]
    started_at = time.perf_counter()
    processes = [
        subprocess.Popen(
            command,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for command in commands
    ]
    clients = []
    try:
        for client_index, process in enumerate(processes):
            stdout, stderr = process.communicate()
            report: dict[str, Any] | None = None
            if output_paths[client_index].exists():
                report = json.loads(output_paths[client_index].read_text())
            clients.append(
                {
                    "client_index": client_index,
                    "returncode": process.returncode,
                    "output_json": str(output_paths[client_index]),
                    "key_prefix": f"{args.key_prefix}:client{client_index}",
                    "command": commands[client_index],
                    "stdout_tail": stdout[-4000:],
                    "stderr_tail": stderr[-4000:],
                    "result": extract_mixed_result(report)
                    if report is not None
                    else {},
                }
            )
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    wall_time_s = time.perf_counter() - started_at

    client_reports = []
    for output_path in output_paths:
        if output_path.exists():
            client_reports.append(json.loads(output_path.read_text()))
    aggregate = aggregate_multi_client_results(client_reports, wall_time_s)
    successful_clients = sum(1 for client in clients if client["returncode"] == 0)
    baseline_result = (
        extract_mixed_result(baseline_report) if baseline_report is not None else {}
    )
    return {
        "client_count": args.multi_client_count,
        "client_concurrency": args.concurrency,
        "total_worker_threads": args.multi_client_count * args.concurrency,
        "duration_s": wall_time_s,
        "requested_duration_s": args.duration if args.duration > 0 else 30.0,
        "successful_clients": successful_clients,
        "failed_clients": args.multi_client_count - successful_clients,
        "single_client_baseline": baseline,
        "aggregate": aggregate,
        "baseline_comparison": compare_multi_client_to_baseline(
            baseline_result, aggregate
        )
        if baseline_result
        else {},
        "clients": clients,
        "failures": args.multi_client_count
        - successful_clients
        + aggregate["failures"],
    }


def git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root()),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def find_report_metric_nodes(node: Any, prefix: str = "") -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    if not isinstance(node, dict):
        return found
    if "latency_ms" in node or "api_calls_per_s" in node or "objects_per_s" in node:
        found[prefix or "$"] = node
    for key, value in node.items():
        if isinstance(value, dict):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            found.update(find_report_metric_nodes(value, child_prefix))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    child_prefix = (
                        f"{prefix}.{key}[{index}]" if prefix else f"{key}[{index}]"
                    )
                    found.update(find_report_metric_nodes(item, child_prefix))
    return found


def metric_value(node: dict[str, Any], metric: str) -> float | None:
    if metric.startswith("latency_ms."):
        _, percentile_name = metric.split(".", 1)
        value = node.get("latency_ms", {}).get(percentile_name)
    else:
        value = node.get(metric)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def compare_reports(
    baseline: dict[str, Any], current: dict[str, Any]
) -> dict[str, Any]:
    baseline_nodes = find_report_metric_nodes(baseline.get("results", {}))
    current_nodes = find_report_metric_nodes(current.get("results", {}))
    metrics = [
        "api_calls_per_s",
        "objects_per_s",
        "throughput_mib_s",
        "latency_ms.p50",
        "latency_ms.p95",
        "latency_ms.p99",
        "misses",
        "failures",
    ]
    comparisons: dict[str, Any] = {}
    for path, current_node in current_nodes.items():
        baseline_node = baseline_nodes.get(path)
        if baseline_node is None:
            continue
        path_comparison = {}
        for metric in metrics:
            baseline_value = metric_value(baseline_node, metric)
            current_value = metric_value(current_node, metric)
            if baseline_value is None or current_value is None:
                continue
            delta = current_value - baseline_value
            delta_percent = (
                None if baseline_value == 0 else delta / baseline_value * 100.0
            )
            path_comparison[metric] = {
                "baseline": baseline_value,
                "current": current_value,
                "delta": delta,
                "delta_percent": delta_percent,
            }
        if path_comparison:
            comparisons[path] = path_comparison
    return {
        "baseline_path": None,
        "matched_metric_nodes": len(comparisons),
        "metrics": comparisons,
    }


def has_failures(node: Any) -> bool:
    if isinstance(node, dict):
        failures = node.get("failures")
        if isinstance(failures, (int, float)) and failures > 0:
            return True
        return any(has_failures(value) for value in node.values())
    if isinstance(node, list):
        return any(has_failures(item) for item in node)
    return False


def build_report(
    args: argparse.Namespace,
    import_mode: str,
    master: MasterProcess | None,
    results: dict[str, Any],
    metrics_snapshot: dict[str, Any],
) -> dict[str, Any]:
    return {
        "metadata": {
            "timestamp_unix": time.time(),
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "git_commit": git_commit(),
            "store_import": import_mode,
            "master_log": str(master.log_path) if master else None,
        },
        "environment": collect_environment_info(),
        "config": {
            "operation": args.operation,
            "num_keys": args.num_keys,
            "value_size": args.value_size,
            "value_size_min": args.value_size_min,
            "value_size_max": args.value_size_max,
            "batch_size": args.batch_size,
            "concurrency": args.concurrency,
            "concurrency_scan": args.concurrency_scan,
            "repeat": args.repeat,
            "warmup": args.warmup,
            "duration": args.duration,
            "trend_interval": args.trend_interval,
            "mix": args.mix,
            "random_seed": args.random_seed,
            "max_active_keys": args.max_active_keys,
            "multi_client_count": args.multi_client_count,
            "skip_single_client_baseline": args.skip_single_client_baseline,
            "protocol": args.protocol,
            "local_hostname": args.local_hostname,
            "metadata_url": args.metadata_url,
            "master_addr": args.master_addr,
            "master_metrics_url": args.master_metrics_url,
            "client_metrics_url": args.client_metrics_url,
            "worker_metrics_url": args.worker_metrics_url,
            "global_segment_size_mb": args.global_segment_size_mb,
            "local_buffer_size_mb": args.local_buffer_size_mb,
            "build_dir": str(Path(args.build_dir).resolve())
            if args.build_dir
            else None,
            "start_master": args.start_master,
        },
        "results": results,
        "metrics_snapshot": metrics_snapshot,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an end-to-end Mooncake Store API benchmark."
    )
    parser.add_argument(
        "--operation",
        default="smoke",
        choices=[
            "smoke",
            "all",
            "mixed",
            MULTI_CLIENT_OPERATION,
            *PUBLIC_API_OPERATIONS,
        ],
        help="operation to run",
    )
    parser.add_argument(
        "--num-keys", type=int, default=100, help="number of benchmark keys"
    )
    parser.add_argument(
        "--value-size",
        type=parse_size,
        default=parse_size("4K"),
        help="value size per object, supports suffixes like 4K, 1M, 2G",
    )
    parser.add_argument(
        "--batch-size", type=int, default=16, help="batch size for batch APIs"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="thread count for concurrent API and mixed-workload benchmarks",
    )
    parser.add_argument(
        "--concurrency-scan",
        nargs="?",
        const="auto",
        default="",
        help="run a concurrency sweep; pass comma-separated values or use no value for 1,4,8,16",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="repeat each benchmark operation N times and report stability metrics",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="duration in seconds for mixed workloads; default is 30 when mixed is selected",
    )
    parser.add_argument(
        "--trend-interval",
        type=float,
        default=10.0,
        help="seconds per time bucket for mixed workload trend output; use 0 to disable",
    )
    parser.add_argument(
        "--mix",
        default="mixed",
        choices=sorted(MIX_PROFILES),
        help="operation ratio profile for --operation mixed",
    )
    parser.add_argument(
        "--value-size-min",
        type=parse_size,
        default=None,
        help="minimum value size for mixed workload random distribution",
    )
    parser.add_argument(
        "--value-size-max",
        type=parse_size,
        default=None,
        help="maximum value size for mixed workload random distribution",
    )
    parser.add_argument(
        "--random-seed", type=int, default=2026, help="random seed for mixed workload"
    )
    parser.add_argument("--warmup", type=int, default=10, help="warmup object count")
    parser.add_argument("--key-prefix", default="store_api_bench", help="key prefix")
    parser.add_argument(
        "--max-active-keys",
        type=int,
        default=0,
        help="cap live keys during mixed workloads; 0 means unlimited",
    )
    parser.add_argument(
        "--multi-client-count",
        type=int,
        default=2,
        help="number of independent child Store clients for --operation multi_client_mixed",
    )
    parser.add_argument(
        "--skip-single-client-baseline",
        action="store_true",
        help="skip the single-client baseline run before --operation multi_client_mixed",
    )

    parser.add_argument(
        "--protocol", default="tcp", choices=["tcp", "rdma"], help="transfer protocol"
    )
    parser.add_argument(
        "--rdma-devices", default="", help="comma-separated RDMA devices"
    )
    parser.add_argument(
        "--local-hostname",
        default="127.0.0.1",
        help="local hostname passed to store.setup",
    )
    parser.add_argument("--metadata-url", default="http://127.0.0.1:8080/metadata")
    parser.add_argument("--master-addr", default="127.0.0.1:50051")
    parser.add_argument(
        "--master-metrics-port", type=int, default=DEFAULT_MASTER_METRICS_PORT
    )
    parser.add_argument("--master-metrics-url", default=DEFAULT_MASTER_METRICS_URL)
    parser.add_argument("--client-metrics-url", default="")
    parser.add_argument("--worker-metrics-url", default="")
    parser.add_argument("--metrics-timeout", type=float, default=2.0)
    parser.add_argument(
        "--skip-metrics", action="store_true", help="skip HTTP metrics collection"
    )
    parser.add_argument("--global-segment-size-mb", type=int, default=512)
    parser.add_argument("--local-buffer-size-mb", type=int, default=128)

    parser.add_argument(
        "--build-dir", help="CMake build directory for build-tree Python module"
    )
    parser.add_argument(
        "--start-master", action="store_true", help="start mooncake_master for this run"
    )
    parser.add_argument("--master-bin", help="path to mooncake_master")
    parser.add_argument("--master-log", help="path for mooncake_master stdout/stderr")
    parser.add_argument("--startup-timeout", type=float, default=20.0)
    parser.add_argument(
        "--master-extra-flag",
        action="append",
        default=[],
        help="extra flag passed to mooncake_master, repeatable",
    )

    parser.add_argument("--output-json", help="write JSON report to this path")
    parser.add_argument(
        "--compare", help="compare current report with a baseline JSON report"
    )
    parser.add_argument(
        "--skip-cleanup", action="store_true", help="leave benchmark keys in the store"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.num_keys <= 0:
        raise ValueError("--num-keys must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    if args.repeat <= 0:
        raise ValueError("--repeat must be positive")
    if args.duration < 0:
        raise ValueError("--duration must be non-negative")
    if args.trend_interval < 0:
        raise ValueError("--trend-interval must be non-negative")
    if args.multi_client_count <= 0:
        raise ValueError("--multi-client-count must be positive")
    if args.max_active_keys < 0:
        raise ValueError("--max-active-keys must be non-negative")
    if args.metrics_timeout <= 0:
        raise ValueError("--metrics-timeout must be positive")
    if args.master_metrics_port <= 0:
        raise ValueError("--master-metrics-port must be positive")
    if (
        args.master_metrics_url == DEFAULT_MASTER_METRICS_URL
        and args.master_metrics_port != DEFAULT_MASTER_METRICS_PORT
    ):
        args.master_metrics_url = f"http://127.0.0.1:{args.master_metrics_port}/metrics"
    value_size_bounds(args)
    if psutil is None:
        print(
            "warning: psutil is not installed; RSS memory tracking will be disabled",
            file=sys.stderr,
        )

    args.build_dir = str(Path(args.build_dir).resolve()) if args.build_dir else None
    StoreClass, import_mode = import_store_class(
        Path(args.build_dir) if args.build_dir else None
    )

    master = start_master(args)
    store = None
    try:
        results: dict[str, Any] = {}
        if args.operation == MULTI_CLIENT_OPERATION:
            if not args.start_master:
                raise ValueError(
                    "--operation multi_client_mixed requires --start-master"
                )
            results[MULTI_CLIENT_OPERATION] = run_multi_client_mixed(args)
        else:
            store = setup_store(args, StoreClass)
            payload = make_payload(args.value_size)
            warmup(store, args, payload)
            if args.operation == "smoke":
                with MemoryTracker() as memory:
                    results["smoke"] = run_smoke(store, args, payload)
                results["smoke"]["memory"] = memory.as_dict()
            else:
                operations = (
                    PUBLIC_API_OPERATIONS
                    if args.operation == "all"
                    else [args.operation]
                )
                if args.concurrency_scan:
                    results["concurrency_scan"] = run_concurrency_scan(
                        operations, store, args, payload
                    )
                else:
                    for operation in operations:
                        keys = [
                            f"{args.key_prefix}:{operation}:{i}"
                            for i in range(args.num_keys)
                        ]
                        results[operation] = run_operation_with_repeats(
                            operation, store, args, keys, payload
                        )

        metrics_snapshot = collect_metrics_snapshots(args)
        report = build_report(args, import_mode, master, results, metrics_snapshot)
        if args.compare:
            baseline_path = Path(args.compare)
            baseline = json.loads(baseline_path.read_text())
            report["comparison"] = compare_reports(baseline, report)
            report["comparison"]["baseline_path"] = str(baseline_path.resolve())
        output = json.dumps(report, indent=2, sort_keys=True)
        print(output)
        if args.output_json:
            Path(args.output_json).write_text(output + "\n")

        if "smoke" in results and not results["smoke"]["passed"]:
            return 1
        return 1 if has_failures(results) else 0
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass
        if master is not None:
            master.stop()


if __name__ == "__main__":
    sys.exit(main())
