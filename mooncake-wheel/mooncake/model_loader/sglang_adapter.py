# SPDX-License-Identifier: Apache-2.0
"""SGLang adapter: Mooncake remote load_format + launch helpers."""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_PATCHED = False
_ORIGINAL_GET_MODEL_LOADER = None


def register() -> None:
    """Patch SGLang ``get_model_loader`` to honor ``load_format=mooncake``.

    Upstream SGLang already supports ``--load-format remote`` with an in-tree
    Mooncake connector. This patch adds an explicit ``mooncake`` load format that
    uses Mooncake's first-party ``RemoteWeightIO`` / ``MooncakeStoreConnector``.
    """
    global _PATCHED, _ORIGINAL_GET_MODEL_LOADER
    if _PATCHED:
        return

    try:
        from sglang.srt.model_loader import loader as sgl_loader
    except ImportError:
        logger.debug("SGLang not installed; skipping mooncake SGLang adapter")
        return

    _ORIGINAL_GET_MODEL_LOADER = sgl_loader.get_model_loader

    def get_model_loader(load_config, *args, **kwargs):
        load_format = load_config.load_format
        value = getattr(load_format, "value", load_format)
        if str(value).lower() == "mooncake":
            return MooncakeSglangModelLoader(load_config)
        return _ORIGINAL_GET_MODEL_LOADER(load_config, *args, **kwargs)

    sgl_loader.get_model_loader = get_model_loader
    _PATCHED = True
    logger.info("Patched SGLang get_model_loader for load_format=mooncake")


class MooncakeSglangModelLoader:
    """Minimal SGLang-compatible loader using :class:`RemoteWeightIO`."""

    def __init__(self, load_config: Any):
        self.load_config = load_config

    def download_model(self, model_config) -> None:
        return None

    def load_model(self, model_config, device_config=None, **kwargs):
        """Initialize and fill the model when SGLang provides init hooks.

        Prefer calling engines with ``RemoteWeightIO.load_weights_into`` after
        they construct the architecture. When used via the patched
        ``get_model_loader``, Fall back to SGLang's DefaultModelLoader init path
        if available, then fill weights from Mooncake.
        """
        # Delegate architecture construction to DefaultModelLoader when possible.
        try:
            from sglang.srt.model_loader.loader import DefaultModelLoader
            from sglang.srt.configs.load_config import LoadConfig, LoadFormat

            temp = LoadConfig(load_format=LoadFormat.DUMMY)
            dummy = DefaultModelLoader(temp)
            # Newer SGLang signatures vary; try common call shapes.
            try:
                model = dummy.load_model(
                    model_config=model_config, device_config=device_config
                )
            except TypeError:
                model = dummy.load_model(
                    model_config=model_config,
                    device_config=device_config,
                    **kwargs,
                )
        except Exception as e:
            raise RuntimeError(
                "MooncakeSglangModelLoader could not initialize the empty model "
                "via DefaultModelLoader(DUMMY). Use --load-format remote with "
                "mooncake:/// URLs on a SGLang build that includes the remote "
                f"loader, or call RemoteWeightIO directly. Underlying error: {e}"
            ) from e

        self.load_weights(model, model_config)
        return model.eval() if hasattr(model, "eval") else model

    def load_weights(self, model, model_config) -> None:
        import json

        from mooncake.model_loader.config import MooncakeStoreLoaderConfig
        from mooncake.model_loader.mooncake_store_connector import (
            create_mooncake_connector,
        )
        from mooncake.model_loader.remote_weight_io import RemoteWeightIO

        extra = getattr(self.load_config, "model_loader_extra_config", None) or {}
        if isinstance(extra, str):
            extra = json.loads(extra) if extra.strip() else {}

        model_path = getattr(model_config, "model_path", None) or getattr(
            model_config, "path", None
        )
        url = extra.get("url") or model_path
        if not url or not str(url).startswith("mooncake://"):
            raise ValueError(
                "SGLang mooncake loader requires model path mooncake:///<name> "
                "or model_loader_extra_config.url"
            )

        rank = int(extra.get("rank", 0))
        try:
            from sglang.srt.distributed import get_tensor_model_parallel_rank

            rank = int(get_tensor_model_parallel_rank())
        except Exception:
            pass

        cfg = (
            MooncakeStoreLoaderConfig.from_mapping(extra)
            if extra
            else MooncakeStoreLoaderConfig.load_from_env()
        )
        with create_mooncake_connector(str(url), config=cfg) as connector:
            with RemoteWeightIO(str(url), connector=connector) as io:
                io.load_weights_into(model, rank=rank)
