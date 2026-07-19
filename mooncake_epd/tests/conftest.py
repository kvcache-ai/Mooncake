from __future__ import annotations

import gc
import os
from pathlib import Path
from typing import Iterable

import pytest
import torch


MODEL_PATH = Path(
    os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
)


def get_test_gpu_id(role: str, default: int) -> int:
    """Return the CUDA device id for a real-model test role.

    Real-model tests originally used a fixed 3/4/5/6 layout.  On shared GPU
    machines that can accidentally collide with long-running serving jobs.  The
    test still uses real models/tensors; this helper only makes device placement
    explicit and reproducible via environment variables.

    Priority:
    1. ``MOONCAKE_EPD_TEST_GPU_<ROLE>`` (e.g. ``..._ENC``)
    2. ``MOONCAKE_EPD_TEST_GPUS`` comma list mapped as ENC,PRE,DEC,BASE
    3. the historical default passed by the test module
    """

    role = str(role).upper()
    direct = os.getenv(f"MOONCAKE_EPD_TEST_GPU_{role}")
    if direct not in {None, ""}:
        return int(str(direct).strip())
    raw = os.getenv("MOONCAKE_EPD_TEST_GPUS", "")
    if raw.strip():
        order = ["ENC", "PRE", "DEC", "BASE"]
        items = [item.strip() for item in raw.split(",") if item.strip()]
        if role in order and order.index(role) < len(items):
            return int(items[order.index(role)])
    return int(default)


def get_test_gpu_device(role: str, default: int) -> torch.device:
    return torch.device(f"cuda:{get_test_gpu_id(role, default)}")


def require_real_model_and_gpus(*gpu_ids: int) -> None:
    if not MODEL_PATH.exists():
        pytest.skip(f"real model not found: {MODEL_PATH}")
    if not torch.cuda.is_available():
        pytest.skip("CUDA is not available")
    n = torch.cuda.device_count()
    missing = [idx for idx in gpu_ids if idx >= n]
    if missing:
        pytest.skip(f"required GPU indices not available: {missing} / count={n}")


def cleanup_torch(*objs, devices: Iterable[int] = ()) -> None:
    for obj in objs:
        try:
            del obj
        except Exception:
            pass
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        for idx in devices:
            try:
                with torch.cuda.device(idx):
                    torch.cuda.empty_cache()
            except Exception:
                pass


def pytest_configure(config):
    config.addinivalue_line("markers", "unit: deterministic unit-level CPU validation")
    config.addinivalue_line("markers", "integration: multi-component local integration validation")
    config.addinivalue_line("markers", "gpu_single_node: requires one host with the configured GPU resources")
    config.addinivalue_line("markers", "gpu_multi_node: requires two or more configured GPU hosts")
    config.addinivalue_line("markers", "rdma: requires a real RDMA transport and evidence metadata")
    config.addinivalue_line("markers", "benchmark: emits benchmark artifacts; never implies a claim by itself")
    config.addinivalue_line("markers", "real_model: requires loading the real Qwen3-VL model")
    config.addinivalue_line("markers", "gpu: requires CUDA GPUs")
    config.addinivalue_line("markers", "mooncake: requires running Mooncake services")
    config.addinivalue_line("markers", "slow: long-running validation")
    config.addinivalue_line("markers", "synthetic: tensor-only or synthetic-data validation")
