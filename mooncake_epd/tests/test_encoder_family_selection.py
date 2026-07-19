from __future__ import annotations

from types import SimpleNamespace

import torch

from mooncake_epd.core.epd_workers import EncoderWorker
from mooncake_epd.scripts.epd_encoder_service import (
    resolve_encoder_family,
    resolve_encoder_runtime,
)


def test_auto_encoder_family_uses_checkpoint_model_type():
    assert resolve_encoder_family("auto", "qwen3_vl") == "qwen3_vl"
    assert resolve_encoder_family("auto", "qwen2_5_vl") == "qwen2_5_vl"
    assert resolve_encoder_family("auto", "qwen2_5_omni") == "qwen2_5_omni"


def test_encoder_family_aliases_and_invalid_values_are_fail_closed():
    assert resolve_encoder_family("qwen25_vl") == "qwen2_5_vl"
    assert resolve_encoder_family("omni") == "qwen2_5_omni"
    try:
        resolve_encoder_family("unsupported")
    except ValueError as exc:
        assert "unsupported encoder_family" in str(exc)
    else:  # pragma: no cover - regression guard for fail-open routing.
        raise AssertionError("unsupported family must not silently route to Qwen3-VL")


def test_generic_encoder_worker_accepts_qwen25_without_deepstack_config():
    model = SimpleNamespace(
        config=SimpleNamespace(
            model_type="qwen2_5_vl",
            vision_config=SimpleNamespace(),
        )
    )
    worker = EncoderWorker(model, processor=SimpleNamespace(), device=torch.device("cpu"))
    assert worker.deepstack_indices == []


def test_native_vllm_encoder_runtime_is_explicit_and_qwen3_only():
    assert resolve_encoder_runtime("hf", "qwen3_vl") == "transformers"
    assert resolve_encoder_runtime("vllm", "qwen3_vl") == "vllm_native"

    try:
        resolve_encoder_runtime("vllm_native", "qwen2_5_vl")
    except ValueError as exc:
        assert "requires encoder_family=qwen3_vl" in str(exc)
    else:  # pragma: no cover - regression guard for unsafe runtime fallback.
        raise AssertionError("native vLLM runtime must reject unsupported model families")
