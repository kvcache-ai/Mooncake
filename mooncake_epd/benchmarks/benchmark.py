"""
EPD 流水线性能基准测试

对比 EPD 分离模式 vs 单机模式的性能差异：
- 吞吐量（requests/s）
- 延迟（ms）
- TTFT（Time to First Token）
- 传输带宽
- Agent 克隆延迟
"""

import time
import json
import logging
from typing import Dict, Any, List
from dataclasses import dataclass

import torch
import numpy as np

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core import (
    MooncakeTransferWrapper,
    create_encoder_worker,
    create_prefill_worker,
    create_decode_worker,
    EPDPipeline,
)
from agent import AgentStateCloner, AgentPDScheduler, AgentRequest, AgentType

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """基准测试结果"""
    name: str
    mode: str  # "epd_disagg" or "single_node"
    metrics: Dict[str, float]
    config: Dict[str, Any]


class EPDBenchmark:
    """EPD 流水线基准测试"""

    def __init__(self, num_devices: int = 4):
        self.num_devices = min(num_devices, torch.cuda.device_count())
        self.results: List[BenchmarkResult] = []

    def benchmark_transfer_bandwidth(self) -> BenchmarkResult:
        """测试 Transfer Engine 带宽"""
        transfer = MooncakeTransferWrapper(protocol="tcp")
        transfer.initialize()

        sizes = [1024, 10240, 102400, 1048576]  # 元素数
        metrics = {}

        for size in sizes:
            tensor = torch.randn(size, dtype=torch.float32, device="cuda:0" if torch.cuda.is_available() else "cpu")
            target = torch.device("cuda:1" if torch.cuda.device_count() > 1 else "cuda:0")

            # warmup
            for _ in range(3):
                transfer.transfer_tensor(tensor, target)
            transfer.reset_stats()

            # benchmark
            iterations = 20
            start = time.perf_counter()
            for _ in range(iterations):
                transfer.transfer_tensor(tensor, target)
            elapsed = time.perf_counter() - start

            nbytes = size * 4 * iterations
            bandwidth_gbps = (nbytes * 8) / elapsed / 1e9
            metrics[f"transfer_{size}_gbps"] = round(bandwidth_gbps, 3)

        stats = transfer.get_stats()
        transfer.shutdown()

        result = BenchmarkResult(
            name="transfer_bandwidth",
            mode="tcp",
            metrics=metrics,
            config={"num_devices": self.num_devices, "protocol": "tcp"},
        )
        self.results.append(result)
        return result

    def benchmark_epd_pipeline(
        self,
        num_iterations: int = 10,
        seq_len: int = 128,
        max_new_tokens: int = 32,
    ) -> BenchmarkResult:
        """测试 EPD 流水线端到端性能"""
        device_e = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        device_p = torch.device("cuda:1" if torch.cuda.device_count() > 1 else device_e)
        device_d = torch.device("cuda:2" if torch.cuda.device_count() > 2 else device_e)

        transfer = MooncakeTransferWrapper(protocol="tcp")
        transfer.initialize()

        encoder = create_encoder_worker(device=device_e, transfer_engine=transfer)
        prefill = create_prefill_worker(device=device_p, transfer_engine=transfer)
        decode = create_decode_worker(device=device_d)

        pipeline = EPDPipeline(
            encoder_worker=encoder,
            prefill_worker=prefill,
            decode_worker=decode,
            transfer_engine=transfer,
            encoder_device=device_e,
            prefill_device=device_p,
            decode_device=device_d,
        )

        # warmup
        pixel_values = torch.randn(1, 3, 448, 448, device=device_e)
        input_ids = torch.randint(0, 32000, (1, seq_len), device=device_e)
        pipeline.process(pixel_values, input_ids, max_new_tokens=4)
        pipeline.reset_stats()

        # benchmark
        latencies = []
        ttfts = []
        throughputs = []

        for i in range(num_iterations):
            pixel_values = torch.randn(1, 3, 448, 448, device=device_e)
            input_ids = torch.randint(0, 32000, (1, seq_len), device=device_e)

            _, stats = pipeline.process(
                pixel_values, input_ids, max_new_tokens=max_new_tokens
            )
            latencies.append(stats.total_time_ms)
            ttfts.append(stats.ttft_ms)
            throughputs.append(stats.tokens_per_second)

        metrics = {
            "avg_latency_ms": round(np.mean(latencies), 2),
            "p50_latency_ms": round(np.percentile(latencies, 50), 2),
            "p99_latency_ms": round(np.percentile(latencies, 99), 2),
            "avg_ttft_ms": round(np.mean(ttfts), 2),
            "avg_throughput_tps": round(np.mean(throughputs), 2),
            "total_requests": num_iterations,
        }

        result = BenchmarkResult(
            name="epd_pipeline",
            mode="epd_disagg",
            metrics=metrics,
            config={
                "seq_len": seq_len,
                "max_new_tokens": max_new_tokens,
                "num_iterations": num_iterations,
            },
        )
        self.results.append(result)
        transfer.shutdown()
        return result

    def benchmark_agent_cloning(
        self, num_branches_list: List[int] = None
    ) -> BenchmarkResult:
        """测试 Agent State Cloning 性能"""
        if num_branches_list is None:
            num_branches_list = [2, 4, 8, 16]

        cloner = AgentStateCloner()
        metrics = {}

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        # 模拟 KV Cache
        num_layers = 32
        num_heads = 8
        seq_len = 256
        head_dim = 128

        key_cache = torch.randn(num_layers, 1, num_heads, seq_len, head_dim, device=device)
        value_cache = torch.randn(num_layers, 1, num_heads, seq_len, head_dim, device=device)
        kv_cache = (key_cache, value_cache)

        cloner.register_kv_cache("base_0", kv_cache)

        for num_branches in num_branches_list:
            clone_times = []
            for _ in range(5):
                start = time.perf_counter()
                branches = cloner.fork_branches("base_0", num_branches)
                elapsed_ms = (time.perf_counter() - start) * 1000
                clone_times.append(elapsed_ms)

                # cleanup
                for b in branches:
                    cloner.release_branch(b.branch_id)

            metrics[f"clone_{num_branches}_branches_ms"] = round(np.mean(clone_times), 3)
            metrics[f"clone_{num_branches}_per_branch_ms"] = round(
                np.mean(clone_times) / num_branches, 3
            )

        result = BenchmarkResult(
            name="agent_cloning",
            mode="zero_copy",
            metrics=metrics,
            config={"num_layers": num_layers, "seq_len": seq_len},
        )
        self.results.append(result)
        return result

    def benchmark_prefix_caching(
        self, num_requests: int = 20, cache_hit_ratio: float = 0.5
    ) -> BenchmarkResult:
        """测试 Hidden State 前缀缓存性能"""
        from agent import HiddenStatePrefixCache

        cache = HiddenStatePrefixCache()
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        # 准备几种不同的图像
        num_unique_images = max(1, int(num_requests * (1 - cache_hit_ratio)))
        images = [torch.randn(1, 3, 448, 448, device=device) for _ in range(num_unique_images)]

        # 预填充缓存
        encoder = create_encoder_worker(device=device)
        for img in images:
            output = encoder.encode_images(img)
            cache.put(img, output.hidden_states, output.metadata)

        # benchmark
        cache_times = []
        no_cache_times = []
        hits = 0

        for i in range(num_requests):
            img = images[i % num_unique_images]

            # with cache
            start = time.perf_counter()
            cached = cache.get(img)
            if cached is not None:
                hits += 1
            cache_times.append((time.perf_counter() - start) * 1000)

            # without cache (always compute)
            start = time.perf_counter()
            encoder.encode_images(img)
            no_cache_times.append((time.perf_counter() - start) * 1000)

        metrics = {
            "cache_hit_rate": hits / num_requests,
            "avg_cache_lookup_ms": round(np.mean(cache_times), 4),
            "avg_encode_no_cache_ms": round(np.mean(no_cache_times), 2),
            "speedup": round(np.mean(no_cache_times) / max(np.mean(cache_times), 0.001), 1),
            "cache_stats": cache.get_stats(),
        }

        result = BenchmarkResult(
            name="prefix_caching",
            mode="lru_cache",
            metrics=metrics,
            config={
                "num_requests": num_requests,
                "num_unique_images": num_unique_images,
            },
        )
        self.results.append(result)
        return result

    def run_all(self) -> Dict[str, Any]:
        """运行所有基准测试"""
        logger.info("=" * 60)
        logger.info("Running EPD Pipeline Benchmarks")
        logger.info("=" * 60)

        all_results = {}

        logger.info("\n--- Transfer Bandwidth ---")
        r = self.benchmark_transfer_bandwidth()
        all_results[r.name] = {"mode": r.mode, "metrics": r.metrics}

        logger.info("\n--- EPD Pipeline ---")
        r = self.benchmark_epd_pipeline()
        all_results[r.name] = {"mode": r.mode, "metrics": r.metrics}

        logger.info("\n--- Agent Cloning ---")
        r = self.benchmark_agent_cloning()
        all_results[r.name] = {"mode": r.mode, "metrics": r.metrics}

        logger.info("\n--- Prefix Caching ---")
        r = self.benchmark_prefix_caching()
        all_results[r.name] = {"mode": r.mode, "metrics": r.metrics}

        return all_results

    def save_results(self, output_path: str):
        """保存结果到 JSON"""
        data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "num_devices": self.num_devices,
            "results": [
                {
                    "name": r.name,
                    "mode": r.mode,
                    "metrics": r.metrics,
                    "config": r.config,
                }
                for r in self.results
            ],
        }
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Results saved to {output_path}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    benchmark = EPDBenchmark()
    results = benchmark.run_all()

    output_dir = os.path.join(os.path.dirname(__file__), "..", "benchmarks")
    os.makedirs(output_dir, exist_ok=True)
    benchmark.save_results(os.path.join(output_dir, "benchmark_results.json"))

    print("\n" + "=" * 60)
    print("Benchmark Results Summary")
    print("=" * 60)
    for name, data in results.items():
        print(f"\n[{name}] ({data['mode']})")
        for k, v in data["metrics"].items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
