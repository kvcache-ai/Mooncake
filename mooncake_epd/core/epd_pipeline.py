"""
EPD Pipeline - 三阶段流水线编排
"""

import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

import torch

from .transfer_engine import MooncakeTransferWrapper
from .encoder_worker import EncoderWorker
from .prefill_worker import PrefillWorker
from .decode_worker import DecodeWorker

logger = logging.getLogger(__name__)

MAX_STATS_HISTORY = 10000


@dataclass
class PipelineStats:
    encode_time_ms: float = 0.0
    transfer_e2p_time_ms: float = 0.0
    prefill_time_ms: float = 0.0
    transfer_p2d_time_ms: float = 0.0
    decode_time_ms: float = 0.0
    total_time_ms: float = 0.0
    tokens_generated: int = 0
    tokens_per_second: float = 0.0
    ttft_ms: float = 0.0

    def summary(self) -> Dict[str, float]:
        return {
            "encode_ms": round(self.encode_time_ms, 2),
            "transfer_e2p_ms": round(self.transfer_e2p_time_ms, 2),
            "prefill_ms": round(self.prefill_time_ms, 2),
            "transfer_p2d_ms": round(self.transfer_p2d_time_ms, 2),
            "decode_ms": round(self.decode_time_ms, 2),
            "total_ms": round(self.total_time_ms, 2),
            "tokens": self.tokens_generated,
            "tps": round(self.tokens_per_second, 2),
            "ttft_ms": round(self.ttft_ms, 2),
        }


class EPDPipeline:
    """
    EPD 三阶段分离流水线

    Image → [Encoder GPU] → Hidden States → [Transfer E→P] →
    [Prefill GPU] → KV Cache → [Transfer P→D] → [Decode GPU] → Text
    """

    def __init__(
        self,
        encoder_worker: EncoderWorker,
        prefill_worker: PrefillWorker,
        decode_worker: DecodeWorker,
        transfer_engine: Optional[MooncakeTransferWrapper] = None,
        encoder_device: torch.device = torch.device("cuda:0"),
        prefill_device: torch.device = torch.device("cuda:1"),
        decode_device: torch.device = torch.device("cuda:2"),
    ):
        self.encoder = encoder_worker
        self.prefill = prefill_worker
        self.decode = decode_worker
        self.transfer = transfer_engine
        self.encoder_device = encoder_device
        self.prefill_device = prefill_device
        self.decode_device = decode_device
        self._stats_history: List[PipelineStats] = []

    def process(
        self,
        pixel_values: torch.Tensor,
        input_ids: torch.Tensor,
        image_sizes: Optional[List[Tuple[int, int]]] = None,
        max_new_tokens: int = 128,
        temperature: float = 0.7,
    ) -> Tuple[Any, PipelineStats]:
        pipeline_start = time.perf_counter()
        stats = PipelineStats()

        # E: Encoder
        t0 = time.perf_counter()
        encoder_output = self.encoder.encode_images(pixel_values, image_sizes)
        stats.encode_time_ms = (time.perf_counter() - t0) * 1000

        # Transfer E→P
        t0 = time.perf_counter()
        if self.transfer is not None:
            hidden_states = self.transfer.transfer_hidden_states(
                encoder_output.hidden_states,
                encoder_output.metadata,
                target_device=self.prefill_device,
            )[0]
        else:
            hidden_states = encoder_output.hidden_states.to(self.prefill_device)
        stats.transfer_e2p_time_ms = (time.perf_counter() - t0) * 1000

        # P: Prefill
        t0 = time.perf_counter()
        prefill_output = self.prefill.prefill(
            input_ids=input_ids, hidden_states=hidden_states,
        )
        stats.prefill_time_ms = (time.perf_counter() - t0) * 1000

        # Transfer P→D
        t0 = time.perf_counter()
        if self.transfer is not None and prefill_output.kv_cache is not None:
            kv_cache = self.transfer.transfer_kv_cache(
                prefill_output.kv_cache, target_device=self.decode_device,
            )
        elif prefill_output.kv_cache is not None:
            kv_cache = (
                prefill_output.kv_cache[0].to(self.decode_device),
                prefill_output.kv_cache[1].to(self.decode_device),
            )
        else:
            kv_cache = None
        stats.transfer_p2d_time_ms = (time.perf_counter() - t0) * 1000

        # D: Decode — derive first token using same temperature
        if prefill_output.logits is not None:
            if temperature > 0:
                probs = torch.softmax(prefill_output.logits / temperature, dim=-1)
                first_token = torch.multinomial(probs, num_samples=1)
            else:
                first_token = torch.argmax(prefill_output.logits, dim=-1, keepdim=True)
        else:
            first_token = input_ids[:, -1:]

        t0 = time.perf_counter()
        decode_output = self.decode.decode(
            input_ids=first_token,
            kv_cache=kv_cache,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
        )
        stats.decode_time_ms = (time.perf_counter() - t0) * 1000

        stats.total_time_ms = (time.perf_counter() - pipeline_start) * 1000
        stats.tokens_generated = decode_output.metadata["num_generated_tokens"]
        stats.tokens_per_second = decode_output.tokens_per_second
        # TTFT includes first decode step
        stats.ttft_ms = (
            stats.encode_time_ms + stats.transfer_e2p_time_ms
            + stats.prefill_time_ms + stats.transfer_p2d_time_ms
            + (stats.decode_time_ms / max(stats.tokens_generated, 1))
        )

        if len(self._stats_history) < MAX_STATS_HISTORY:
            self._stats_history.append(stats)

        return decode_output, stats

    def process_batch(
        self, batch: List[Dict[str, Any]], max_new_tokens: int = 128,
    ) -> List[Tuple[Any, PipelineStats]]:
        return [
            self.process(
                pixel_values=item["pixel_values"],
                input_ids=item["input_ids"],
                image_sizes=item.get("image_sizes"),
                max_new_tokens=max_new_tokens,
            )
            for item in batch
        ]

    def get_aggregate_stats(self) -> Dict[str, Any]:
        if not self._stats_history:
            return {}
        keys = [
            "encode_time_ms", "transfer_e2p_time_ms", "prefill_time_ms",
            "transfer_p2d_time_ms", "decode_time_ms", "total_time_ms", "ttft_ms",
        ]
        agg = {}
        for key in keys:
            values = [getattr(s, key, 0) for s in self._stats_history]
            agg[key] = {
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
            }
        return agg

    def reset_stats(self):
        self._stats_history.clear()
        if self.transfer:
            self.transfer.reset_stats()

    def shutdown(self):
        if self.transfer:
            self.transfer.shutdown()
