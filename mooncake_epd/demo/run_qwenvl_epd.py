"""
Qwen3-VL EPD 端到端 Demo

展示基于 Qwen3-VL 多模态模型的 EPD 三阶段分离推理流程。
包括：
1. 图像编码（Vision Encoder）
2. 多模态 Prefill（文本 + 视觉特征）
3. 自回归 Decode
4. EPD 分离 vs 单机模式对比
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

torch = None
np = None
MooncakeTransferWrapper = None
create_encoder_worker = None
create_prefill_worker = None
create_decode_worker = None
EPDPipeline = None
AgentStateCloner = None
AgentPDScheduler = None
AgentRequest = None
AgentType = None
HiddenStatePrefixCache = None


def _ensure_demo_dependencies():
    global torch, np
    global MooncakeTransferWrapper, create_encoder_worker, create_prefill_worker
    global create_decode_worker, EPDPipeline
    global AgentStateCloner, AgentPDScheduler, AgentRequest, AgentType, HiddenStatePrefixCache
    if torch is not None:
        return
    import torch as _torch
    import numpy as _np
    from mooncake_epd.core import (
        MooncakeTransferWrapper as _MooncakeTransferWrapper,
        create_encoder_worker as _create_encoder_worker,
        create_prefill_worker as _create_prefill_worker,
        create_decode_worker as _create_decode_worker,
        EPDPipeline as _EPDPipeline,
    )
    from mooncake_epd.agent import (
        AgentPDScheduler as _AgentPDScheduler,
        AgentRequest as _AgentRequest,
        AgentStateCloner as _AgentStateCloner,
        AgentType as _AgentType,
    )
    from mooncake_epd.agent.prefix_cache import (
        HiddenStatePrefixCache as _HiddenStatePrefixCache,
    )

    torch = _torch
    np = _np
    MooncakeTransferWrapper = _MooncakeTransferWrapper
    create_encoder_worker = _create_encoder_worker
    create_prefill_worker = _create_prefill_worker
    create_decode_worker = _create_decode_worker
    EPDPipeline = _EPDPipeline
    AgentStateCloner = _AgentStateCloner
    AgentPDScheduler = _AgentPDScheduler
    AgentRequest = _AgentRequest
    AgentType = _AgentType
    HiddenStatePrefixCache = _HiddenStatePrefixCache

logger = logging.getLogger(__name__)


class Qwen3VLEPDDemo:
    """
    Qwen3-VL EPD 端到端推理 Demo

    演示内容：
    1. 基础 EPD 分离推理
    2. Agent 状态克隆（Tree-of-Thought）
    3. Hidden State 前缀缓存
    4. 调度策略对比
    """

    def __init__(
        self,
        model_name: str = "Qwen/Qwen2.5-VL-7B-Instruct",
        use_mock: bool = True,
        encoder_device: str = "cuda:0",
        prefill_device: str = "cuda:1",
        decode_device: str = "cuda:2",
    ):
        _ensure_demo_dependencies()
        self.model_name = model_name
        self.use_mock = use_mock

        if torch.cuda.is_available() and torch.cuda.device_count() >= 3:
            self.encoder_device = torch.device(encoder_device)
            self.prefill_device = torch.device(prefill_device)
            self.decode_device = torch.device(decode_device)
        else:
            self.encoder_device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
            self.prefill_device = self.encoder_device
            self.decode_device = self.encoder_device

        self.model = None
        self.tokenizer = None
        self.processor = None
        self.pipeline = None
        self.transfer = None

    def setup(self):
        """初始化环境和模型"""
        logger.info("Setting up EPD Demo environment...")

        # 初始化 Transfer Engine
        self.transfer = MooncakeTransferWrapper(protocol="tcp")
        self.transfer.initialize()

        # 创建 Workers
        if self.use_mock:
            logger.info("Using Mock models for demonstration")
            encoder = create_encoder_worker(
                device=self.encoder_device,
                transfer_engine=self.transfer,
                hidden_dim=256,
            )
            prefill = create_prefill_worker(
                device=self.prefill_device,
                transfer_engine=self.transfer,
                hidden_dim=512,
                num_layers=4,
                num_kv_heads=4,
                num_attention_heads=8,
                vision_hidden_dim=256,
                vocab_size=1000,
            )
            decode = create_decode_worker(
                device=self.decode_device,
                hidden_dim=512,
                num_layers=4,
                num_kv_heads=4,
                num_attention_heads=8,
                vocab_size=1000,
            )
        else:
            logger.info(f"Loading Qwen3-VL model: {self.model_name}")
            self._load_real_model()
            return

        self.pipeline = EPDPipeline(
            encoder_worker=encoder,
            prefill_worker=prefill,
            decode_worker=decode,
            transfer_engine=self.transfer,
            encoder_device=self.encoder_device,
            prefill_device=self.prefill_device,
            decode_device=self.decode_device,
        )

        logger.info("Setup complete!")

    def _load_real_model(self):
        """加载真实的 Qwen3-VL 模型"""
        try:
            from transformers import AutoTokenizer, AutoProcessor
            from vllm import LLM

            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name, trust_remote_code=True
            )
            self.processor = AutoProcessor.from_pretrained(
                self.model_name, trust_remote_code=True
            )

            logger.info(
                "For real model EPD disaggregation, use vLLM with "
                "MooncakeConnector. See disagg_prefill_decode.md for details."
            )
        except ImportError as e:
            logger.warning(f"Could not load real model: {e}. Using mock models.")
            self.use_mock = True
            self.setup()

    def demo_basic_epd(self) -> Dict[str, Any]:
        """
        Demo 1: 基础 EPD 分离推理

        模拟多模态输入（图像 + 文本）经过 E→P→D 三阶段处理。
        """
        print("\n" + "=" * 60)
        print("Demo 1: Basic EPD Disaggregated Inference")
        print("=" * 60)

        # 模拟图像输入
        pixel_values = torch.randn(1, 3, 448, 448, device=self.encoder_device)
        input_ids = torch.randint(0, 1000, (1, 64), device=self.encoder_device)

        print(f"  Image shape: {list(pixel_values.shape)}")
        print(f"  Input tokens: {input_ids.shape[1]}")
        print(f"  Encoder GPU: {self.encoder_device}")
        print(f"  Prefill GPU: {self.prefill_device}")
        print(f"  Decode GPU: {self.decode_device}")

        output, stats = self.pipeline.process(
            pixel_values=pixel_values,
            input_ids=input_ids,
            max_new_tokens=8,
        )

        result = stats.summary()
        print(f"\n  Pipeline Statistics:")
        for k, v in result.items():
            print(f"    {k}: {v}")

        if output.generated_text:
            print(f"\n  Generated text: {output.generated_text[:200]}")

        return result

    def demo_agent_cloning(self) -> Dict[str, Any]:
        """
        Demo 2: Agent 状态克隆（Tree-of-Thought）

        模拟 Agent fork 出多个思考分支的场景。
        """
        print("\n" + "=" * 60)
        print("Demo 2: Agent State Cloning (Tree-of-Thought)")
        print("=" * 60)

        cloner = AgentStateCloner()

        device = self.encoder_device
        num_layers, num_heads, seq_len, head_dim = 32, 8, 256, 128

        key_cache = torch.randn(num_layers, 1, num_heads, seq_len, head_dim, device=device)
        value_cache = torch.randn(num_layers, 1, num_heads, seq_len, head_dim, device=device)

        print(f"  KV Cache shape: [{num_layers}, 1, {num_heads}, {seq_len}, {head_dim}]")
        kv_size_mb = (key_cache.nelement() + value_cache.nelement()) * 4 / (1024 * 1024)
        print(f"  KV Cache size: {kv_size_mb:.1f} MB")

        # 注册基础 KV Cache
        cloner.register_kv_cache("agent_base", (key_cache, value_cache))

        # Fork 4 个思考分支
        num_branches = 4
        print(f"\n  Forking {num_branches} thinking branches...")

        start = time.perf_counter()
        branches = cloner.fork_branches("agent_base", num_branches)
        clone_time_ms = (time.perf_counter() - start) * 1000

        print(f"  Clone time: {clone_time_ms:.3f} ms (zero-copy)")
        print(f"  Per-branch time: {clone_time_ms / num_branches:.3f} ms")

        # 模拟各分支生成
        for i, branch in enumerate(branches):
            branch.score = np.random.uniform(0.3, 0.9)
            branch.tokens_generated = np.random.randint(10, 50)
            print(f"  Branch {i}: score={branch.score:.3f}, tokens={branch.tokens_generated}")

        # 剪枝：保留 top-2
        cloner.prune_branches(keep_top_k=2)

        stats = cloner.get_stats()
        mem = cloner.get_memory_usage()

        print(f"\n  After pruning (keep top-2):")
        print(f"    Active branches: {stats['active_branches']}")
        print(f"    Memory usage: {mem['total_mb']:.1f} MB")

        return {
            "clone_time_ms": round(clone_time_ms, 3),
            "per_branch_ms": round(clone_time_ms / num_branches, 3),
            "kv_cache_size_mb": round(kv_size_mb, 1),
            **stats,
        }

    def demo_prefix_caching(self) -> Dict[str, Any]:
        """
        Demo 3: Hidden State 前缀缓存

        展示图像前缀缓存减少重复 Encoder 计算的效果。
        """
        print("\n" + "=" * 60)
        print("Demo 3: Hidden State Prefix Caching")
        print("=" * 60)

        cache = HiddenStatePrefixCache()
        device = self.encoder_device

        # 准备 5 张不同的图像
        num_unique = 5
        images = [torch.randn(1, 3, 448, 448, device=device) for _ in range(num_unique)]

        # 预编码并缓存
        encoder = create_encoder_worker(device=device)
        print(f"  Pre-encoding {num_unique} unique images...")

        for i, img in enumerate(images):
            output = encoder.encode_images(img)
            cache.put(img, output.hidden_states, output.metadata)
            print(f"    Image {i}: encoded and cached")

        # 模拟 20 次请求（含重复）
        num_requests = 20
        print(f"\n  Processing {num_requests} requests (with repeated images)...")

        cache_times = []
        encode_times = []
        hits = 0

        for i in range(num_requests):
            img = images[i % num_unique]

            # 查缓存
            start = time.perf_counter()
            cached = cache.get(img)
            cache_time = (time.perf_counter() - start) * 1000

            if cached is not None:
                hits += 1
                cache_times.append(cache_time)
            else:
                start = time.perf_counter()
                encoder.encode_images(img)
                encode_time = (time.perf_counter() - start) * 1000
                encode_times.append(encode_time)

        cache_stats = cache.get_stats()
        print(f"\n  Results:")
        print(f"    Cache hit rate: {hits}/{num_requests} ({cache_stats['hit_rate']:.1%})")
        if cache_times:
            print(f"    Avg cache lookup: {np.mean(cache_times):.4f} ms")
        if encode_times:
            print(f"    Avg encode (no cache): {np.mean(encode_times):.2f} ms")
        print(f"    Cache size: {cache_stats['cache_size_mb']:.1f} MB")

        return cache_stats

    def demo_scheduling(self) -> Dict[str, Any]:
        """
        Demo 4: Agent PD 调度策略

        展示思考型和交互型 Agent 的不同调度策略。
        """
        print("\n" + "=" * 60)
        print("Demo 4: Agent PD Disaggregation Scheduling")
        print("=" * 60)

        scheduler = AgentPDScheduler(
            prefill_workers=["prefill_gpu0", "prefill_gpu1"],
            decode_workers=["decode_gpu2", "decode_gpu3"],
        )

        # 模拟混合负载
        requests = [
            AgentRequest("think_1", AgentType.THINKING, priority=10, max_tokens=512),
            AgentRequest("think_2", AgentType.THINKING, priority=8, max_tokens=1024),
            AgentRequest("chat_1", AgentType.INTERACTIVE, priority=5, max_tokens=64),
            AgentRequest("chat_2", AgentType.INTERACTIVE, priority=3, max_tokens=128),
            AgentRequest("hybrid_1", AgentType.HYBRID, priority=7, max_tokens=256),
        ]

        # 模拟 Worker 负载
        scheduler.update_load("prefill_gpu0", current_load=0.8, gpu_utilization=85)
        scheduler.update_load("prefill_gpu1", current_load=0.3, gpu_utilization=30)
        scheduler.update_load("decode_gpu2", current_load=0.5, avg_latency_ms=15)
        scheduler.update_load("decode_gpu3", current_load=0.2, avg_latency_ms=8)

        print("  Worker loads:")
        print("    prefill_gpu0: load=0.8, gpu=85%")
        print("    prefill_gpu1: load=0.3, gpu=30%")
        print("    decode_gpu2: load=0.5, latency=15ms")
        print("    decode_gpu3: load=0.2, latency=8ms")

        print(f"\n  Routing {len(requests)} requests:")
        routings = scheduler.batch_route(requests)

        for req, route in zip(requests, routings):
            print(
                f"    {req.request_id} ({req.agent_type.value}, pri={req.priority}): "
                f"P={route['prefill_worker']}, D={route['decode_worker']}"
            )

        stats = scheduler.get_stats()
        return stats

    def run_all_demos(self):
        """运行所有 Demo"""
        print("\n" + "#" * 60)
        print("# Qwen3-VL EPD Disaggregation Demo")
        print("# 基于 Mooncake 的多模态推理 EPD 分离与 Agent 状态协同")
        print("#" * 60)

        self.setup()

        results = {}

        results["basic_epd"] = self.demo_basic_epd()
        results["agent_cloning"] = self.demo_agent_cloning()
        results["prefix_caching"] = self.demo_prefix_caching()
        results["scheduling"] = self.demo_scheduling()

        # 汇总
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(json.dumps(results, indent=2, ensure_ascii=False, default=str))

        return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(
        description=(
            "Legacy mock EPD smoke demo. For the real vLLM/Qwen-VL hot path use "
            "mooncake_epd/scripts/run_real_qwenvl_epd_demo.py."
        )
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run the old mock-only smoke demo explicitly.",
    )
    args = parser.parse_args()

    if not args.mock:
        print(
            json.dumps(
                {
                    "status": "not_run",
                    "reason": "mock demo is no longer the default real EPD demo",
                    "real_demo_command": "python mooncake_epd/scripts/run_real_qwenvl_epd_demo.py --model /path/to/Qwen-VL --encoder-device cuda:0 --prefill-gpu 1 --decode-gpu 2",
                    "mock_smoke_command": "python mooncake_epd/demo/run_qwenvl_epd.py --mock",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    demo = Qwen3VLEPDDemo(use_mock=True)
    demo.run_all_demos()


if __name__ == "__main__":
    main()
