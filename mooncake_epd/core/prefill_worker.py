"""
Prefill Worker - Prefill 阶段

接收 Vision Encoder 输出的 Hidden States，结合文本 token 执行 Prefill，
生成 KV Cache 并通过 Transfer Engine 传输到 Decode 节点。
"""

import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

import torch
import torch.nn as nn

logger = logging.getLogger(__name__)


@dataclass
class PrefillOutput:
    """Prefill 阶段输出"""
    kv_cache: Tuple[torch.Tensor, torch.Tensor]  # (key_cache, value_cache)
    logits: torch.Tensor                          # 最后一个 token 的 logits
    metadata: Dict[str, Any]
    prefill_time_ms: float
    request_id: str


class PrefillWorker:
    """
    Prefill Worker

    接收 Encoder 的 Hidden States 和文本 token，执行完整的 Prefill 计算，
    生成 KV Cache 后通过 Mooncake Transfer Engine 传输到 Decode 节点。
    """

    def __init__(
        self,
        model: nn.Module,
        device: torch.device,
        transfer_engine=None,
        max_batch_size: int = 8,
        max_seq_len: int = 4096,
    ):
        self.model = model
        self.device = device
        self.transfer_engine = transfer_engine
        self.max_batch_size = max_batch_size
        self.max_seq_len = max_seq_len
        self._request_counter = 0

    @torch.no_grad()
    def prefill(
        self,
        input_ids: torch.Tensor,
        hidden_states: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
    ) -> PrefillOutput:
        """
        执行 Prefill 计算。

        Args:
            input_ids: [batch, seq_len] 输入 token IDs
            hidden_states: [batch, num_visual_tokens, hidden_dim] 视觉特征（可选）
            attention_mask: [batch, seq_len] attention mask
            position_ids: [batch, seq_len] position IDs

        Returns:
            PrefillOutput: 包含 KV Cache 和 logits
        """
        start_time = time.perf_counter()

        input_ids = input_ids.to(self.device)

        model_kwargs = {
            "input_ids": input_ids,
            "use_cache": True,
        }
        if attention_mask is not None:
            model_kwargs["attention_mask"] = attention_mask.to(self.device)
        if position_ids is not None:
            model_kwargs["position_ids"] = position_ids.to(self.device)

        # 如果有视觉 hidden states，通过 inputs_embeds 注入
        if hidden_states is not None:
            hidden_states = hidden_states.to(self.device)
            model_kwargs["hidden_states_prefix"] = hidden_states

        outputs = self.model(**model_kwargs)

        kv_cache = None
        if hasattr(outputs, "past_key_values") and outputs.past_key_values is not None:
            key_caches = []
            value_caches = []
            for layer_kv in outputs.past_key_values:
                key_caches.append(layer_kv[0])
                value_caches.append(layer_kv[1])
            kv_cache = (
                torch.stack(key_caches),
                torch.stack(value_caches),
            )

        logits = outputs.logits[:, -1, :] if hasattr(outputs, "logits") else None

        prefill_time_ms = (time.perf_counter() - start_time) * 1000

        self._request_counter += 1
        request_id = f"prefill_{self._request_counter}_{int(time.time() * 1000)}"

        metadata = {
            "input_shape": list(input_ids.shape),
            "has_visual_input": hidden_states is not None,
            "num_kv_layers": len(outputs.past_key_values) if kv_cache else 0,
        }

        output = PrefillOutput(
            kv_cache=kv_cache,
            logits=logits,
            metadata=metadata,
            prefill_time_ms=prefill_time_ms,
            request_id=request_id,
        )

        logger.info(
            f"Prefill {request_id}: seq_len={input_ids.shape[-1]}, "
            f"time={prefill_time_ms:.2f}ms"
        )

        return output

    def prefill_and_transfer(
        self,
        input_ids: torch.Tensor,
        hidden_states: Optional[torch.Tensor] = None,
        target_device: Optional[torch.device] = None,
    ) -> PrefillOutput:
        """
        执行 Prefill 并传输 KV Cache 到 Decode 节点（P→D）。
        """
        output = self.prefill(input_ids, hidden_states)

        if self.transfer_engine is not None and target_device is not None and output.kv_cache is not None:
            transferred_kv = self.transfer_engine.transfer_kv_cache(
                output.kv_cache, target_device=target_device
            )
            output.kv_cache = transferred_kv
            transfer_stats = self.transfer_engine.get_stats()
            logger.info(f"Transferred KV Cache: {transfer_stats.summary()}")

        return output


class MockLLMForPrefill(nn.Module):
    """
    模拟 LLM Prefill，用于测试 EPD 流水线。
    模拟 Qwen-VL 的 LLM 部分的 Prefill 输出。
    """

    def __init__(
        self,
        vocab_size: int = 32000,
        hidden_dim: int = 4096,
        num_layers: int = 32,
        num_kv_heads: int = 8,
        num_attention_heads: int = 32,
        vision_hidden_dim: int = 1024,
    ):
        super().__init__()
        self.vocab_size = vocab_size
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.num_kv_heads = num_kv_heads
        self.num_attention_heads = num_attention_heads
        self.head_dim = hidden_dim // num_attention_heads

        self.embed = nn.Embedding(vocab_size, hidden_dim)
        self.vision_proj = nn.Linear(vision_hidden_dim, hidden_dim)
        self.output_proj = nn.Linear(hidden_dim, vocab_size, bias=False)

    def forward(
        self,
        input_ids: torch.Tensor,
        use_cache: bool = True,
        attention_mask=None,
        position_ids=None,
        hidden_states_prefix=None,
        **kwargs,
    ):
        batch_size, seq_len = input_ids.shape
        hidden = self.embed(input_ids)

        if hidden_states_prefix is not None:
            hidden_states_prefix = self.vision_proj(hidden_states_prefix)
            hidden = torch.cat([hidden_states_prefix, hidden], dim=1)
            seq_len = hidden.shape[1]

        kv_cache = []
        for _ in range(self.num_layers):
            k = torch.randn(
                batch_size, self.num_kv_heads, seq_len, self.head_dim,
                device=input_ids.device, dtype=hidden.dtype
            )
            v = torch.randn(
                batch_size, self.num_kv_heads, seq_len, self.head_dim,
                device=input_ids.device, dtype=hidden.dtype
            )
            kv_cache.append((k, v))

        logits = self.output_proj(hidden[:, -1:, :])

        class Output:
            def __init__(self, logits, past_key_values):
                self.logits = logits
                self.past_key_values = past_key_values

        return Output(logits, kv_cache)


def create_prefill_worker(
    device: torch.device,
    transfer_engine=None,
    model: Optional[nn.Module] = None,
    **model_kwargs,
) -> PrefillWorker:
    """创建 Prefill Worker"""
    if model is None:
        model = MockLLMForPrefill(**model_kwargs)
    model = model.to(device).eval()
    return PrefillWorker(model=model, device=device, transfer_engine=transfer_engine)
