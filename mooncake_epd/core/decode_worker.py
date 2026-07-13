"""
Decode Worker - Decode 阶段

接收 Prefill 传输的 KV Cache，执行自回归解码。
"""

import time
import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

import torch
import torch.nn as nn

logger = logging.getLogger(__name__)


@dataclass
class DecodeOutput:
    generated_ids: torch.Tensor
    generated_text: Optional[str]
    decode_time_ms: float
    tokens_per_second: float
    metadata: Dict[str, Any]
    request_id: str


class DecodeWorker:
    """接收 KV Cache 执行自回归解码"""

    def __init__(
        self,
        model: nn.Module,
        device: torch.device,
        tokenizer=None,
        max_new_tokens: int = 256,
        eos_token_id: int = 2,
    ):
        self.model = model
        self.device = device
        self.tokenizer = tokenizer
        self.max_new_tokens = max_new_tokens
        self.eos_token_id = eos_token_id
        self._request_counter = 0

    @torch.no_grad()
    def decode(
        self,
        input_ids: torch.Tensor,
        kv_cache: Optional[Tuple[torch.Tensor, torch.Tensor]] = None,
        max_new_tokens: Optional[int] = None,
        temperature: float = 0.7,
    ) -> DecodeOutput:
        max_tokens = max_new_tokens or self.max_new_tokens
        start_time = time.perf_counter()

        input_ids = input_ids.to(self.device)
        generated = [input_ids]
        past_kv = None

        if kv_cache is not None:
            past_kv = self._rebuild_past_key_values(kv_cache)

        for step in range(max_tokens):
            model_kwargs = {
                "input_ids": input_ids,
                "use_cache": True,
            }
            if past_kv is not None:
                model_kwargs["past_key_values"] = past_kv

            outputs = self.model(**model_kwargs)

            logits = outputs.logits[:, -1, :] if hasattr(outputs, "logits") else outputs

            if temperature > 0:
                probs = torch.softmax(logits / temperature, dim=-1)
                next_token = torch.multinomial(probs, num_samples=1)
            else:
                next_token = torch.argmax(logits, dim=-1, keepdim=True)

            generated.append(next_token)

            if hasattr(outputs, "past_key_values") and outputs.past_key_values is not None:
                past_kv = outputs.past_key_values

            if next_token.item() == self.eos_token_id:
                break

            input_ids = next_token

        generated_ids = torch.cat(generated, dim=1)
        decode_time_ms = (time.perf_counter() - start_time) * 1000

        num_generated = generated_ids.shape[1]
        tps = num_generated / (decode_time_ms / 1000) if decode_time_ms > 0 else 0

        generated_text = None
        if self.tokenizer is not None:
            generated_text = self.tokenizer.decode(
                generated_ids[0].tolist(), skip_special_tokens=True,
            )

        self._request_counter += 1
        request_id = f"decode_{self._request_counter}_{int(time.time() * 1000)}"

        return DecodeOutput(
            generated_ids=generated_ids,
            generated_text=generated_text,
            decode_time_ms=decode_time_ms,
            tokens_per_second=tps,
            metadata={
                "num_generated_tokens": num_generated,
                "eos_reached": len(generated) > 1 and generated[-1].item() == self.eos_token_id,
            },
            request_id=request_id,
        )

    def _rebuild_past_key_values(self, kv_cache):
        key_cache, value_cache = kv_cache
        return [(key_cache[i], value_cache[i]) for i in range(key_cache.shape[0])]


class MockLLMForDecode(nn.Module):
    """模拟 LLM Decode"""

    def __init__(
        self,
        vocab_size: int = 32000,
        hidden_dim: int = 4096,
        num_layers: int = 32,
        num_kv_heads: int = 8,
        num_attention_heads: int = 32,
    ):
        super().__init__()
        self.vocab_size = vocab_size
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.num_kv_heads = num_kv_heads
        self.head_dim = hidden_dim // num_attention_heads
        self.output_proj = nn.Linear(hidden_dim, vocab_size, bias=False)

    def forward(self, input_ids, use_cache=True, past_key_values=None, **kwargs):
        batch_size = input_ids.shape[0]
        logits = torch.randn(
            batch_size, 1, self.vocab_size,
            device=input_ids.device, dtype=torch.float32,
        )
        kv_cache = []
        for _ in range(self.num_layers):
            k = torch.randn(
                batch_size, self.num_kv_heads, 1, self.head_dim,
                device=input_ids.device,
            )
            v = torch.randn(
                batch_size, self.num_kv_heads, 1, self.head_dim,
                device=input_ids.device,
            )
            kv_cache.append((k, v))

        class Output:
            def __init__(self, logits, past_key_values):
                self.logits = logits
                self.past_key_values = past_key_values

        return Output(logits, kv_cache)


def create_decode_worker(
    device: torch.device,
    tokenizer=None,
    model: Optional[nn.Module] = None,
    **model_kwargs,
) -> DecodeWorker:
    if model is None:
        model = MockLLMForDecode(**model_kwargs)
    model = model.to(device).eval()
    return DecodeWorker(model=model, device=device, tokenizer=tokenizer)
