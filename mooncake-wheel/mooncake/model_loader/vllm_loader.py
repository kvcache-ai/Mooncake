# SPDX-License-Identifier: Apache-2.0
"""vLLM model loader plugin: ``--load-format mooncake``."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_REGISTERED = False


def _parse_extra_config(load_config: Any) -> dict:
    extra = getattr(load_config, "model_loader_extra_config", None) or {}
    if isinstance(extra, str):
        extra = json.loads(extra) if extra.strip() else {}
    if not isinstance(extra, dict):
        raise TypeError("model_loader_extra_config must be a dict or JSON object")
    return dict(extra)


def _resolve_url(model_config: Any, extra: dict) -> str:
    if "url" in extra:
        return str(extra["url"])
    model_path = getattr(model_config, "model", None) or getattr(
        model_config, "model_path", None
    )
    if model_path and str(model_path).startswith("mooncake://"):
        return str(model_path)
    raise ValueError(
        "Mooncake model loader requires model_loader_extra_config['url'] "
        "or a model path of the form mooncake:///<model_name>"
    )


def _resolve_rank(extra: dict) -> int:
    if "rank" in extra:
        return int(extra["rank"])
    try:
        from torch.distributed import get_rank, is_initialized

        if is_initialized():
            return int(get_rank())
    except Exception:
        pass
    return 0


def register() -> None:
    """Register the Mooncake loader with vLLM (safe to call multiple times)."""
    global _REGISTERED
    if _REGISTERED:
        return

    try:
        from vllm.model_executor.model_loader import register_model_loader
        from vllm.model_executor.model_loader.base_loader import BaseModelLoader
    except ImportError:
        logger.debug("vLLM not installed; skipping mooncake model loader plugin")
        return

    @register_model_loader("mooncake")
    class MooncakeModelLoader(BaseModelLoader):
        """Load weights from Mooncake Store into an initialized vLLM model."""

        def download_model(self, model_config) -> None:
            # Weights already live in Mooncake Store; nothing to download.
            return None

        def load_weights(self, model, model_config) -> None:
            from mooncake.model_loader.config import MooncakeStoreLoaderConfig
            from mooncake.model_loader.mooncake_store_connector import (
                create_mooncake_connector,
            )
            from mooncake.model_loader.remote_weight_io import RemoteWeightIO

            extra = _parse_extra_config(self.load_config)
            url = _resolve_url(model_config, extra)
            rank = _resolve_rank(extra)
            cfg = (
                MooncakeStoreLoaderConfig.from_mapping(extra)
                if extra
                else MooncakeStoreLoaderConfig.load_from_env()
            )
            with create_mooncake_connector(url, config=cfg) as connector:
                with RemoteWeightIO(url, connector=connector) as io:
                    io.load_weights_into(model, rank=rank)

    _REGISTERED = True
    logger.info("Registered vLLM model loader load_format=mooncake")


# Auto-register on import when vLLM is present.
register()
