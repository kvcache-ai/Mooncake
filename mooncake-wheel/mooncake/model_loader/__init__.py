# SPDX-License-Identifier: Apache-2.0
"""Mooncake remote model loader: Store connector + engine plugins.

This package ports the SGLang RemoteModelLoader / MooncakeStoreConnector pattern
into Mooncake so engines can save/load weights via ``mooncake:///<model_name>``.

Typical flow::

    # Seed from local safetensors
    from mooncake.model_loader import seed_model_from_files
    seed_model_from_files("/path/to/ckpt", "mooncake:///my-model", tp_rank=0)

    # Or save from an initialized nn.Module
    from mooncake.model_loader import save_model
    save_model(model, "mooncake:///my-model", model_path="/path/to/ckpt", rank=0)

    # Load into an empty model
    from mooncake.model_loader import load_weights_into
    load_weights_into(model, "mooncake:///my-model", rank=0)

vLLM::

    vllm serve ... --load-format mooncake \\
      --model-loader-extra-config '{"url":"mooncake:///my-model"}'

SGLang (compatible remote format, upstream)::

    python -m sglang.launch_server --load-format remote \\
      --model mooncake:///my-model --tokenizer-path ...

SGLang (Mooncake ``load_format=mooncake`` via launch wrapper)::

    python -m mooncake.model_loader.sglang_launch --load-format mooncake \\
      --model mooncake:///my-model ...
"""

from mooncake.model_loader.config import (
    COMMON_REMOTE_MODEL_FILES,
    MooncakeStoreLoaderConfig,
    parse_model_name,
)
from mooncake.model_loader.mooncake_store_connector import (
    MooncakeStoreConnector,
    create_mooncake_connector,
)
from mooncake.model_loader.connector import create_remote_connector
from mooncake.model_loader.remote_weight_io import (
    RemoteWeightIO,
    filter_subtensors,
    load_weights_into,
    save_model,
)
from mooncake.model_loader.seed_from_files import seed_model_from_files

__all__ = [
    "COMMON_REMOTE_MODEL_FILES",
    "MooncakeStoreConnector",
    "MooncakeStoreLoaderConfig",
    "RemoteWeightIO",
    "create_mooncake_connector",
    "create_remote_connector",
    "filter_subtensors",
    "load_weights_into",
    "parse_model_name",
    "save_model",
    "seed_model_from_files",
]
