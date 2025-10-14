"""
Mooncake model loader utilities.

This module provides:
  - MooncakeModelLoader: a loader that pulls model weights (PyTorch tensors)
    from Mooncake Distributed Store via put_tensor/get_tensor wire format.
  - save_model_to_mooncake: helper to push a model's parameters to Mooncake
    with a manifest for later fast loading.

It is designed to be used in projects like sglang/vLLM-style loaders, but the
utilities themselves are standalone aside from optional sglang dependencies.

Usage (typical):

  from mooncake.model_loader_mooncake import MooncakeModelLoader, save_model_to_mooncake

  # Save (on training/checkpointing side)
  store = create_store(**setup_kwargs)
  save_model_to_mooncake(model, store, prefix="my-model/weights", tp_rank=rank)

  # Load (on inference side)
  loader = MooncakeModelLoader(load_config)
  model = loader.load_model(model_config=model_config, device_config=device_config)

Notes:
  - This module tries to import the Mooncake Python extension as
    "from mooncake import store as mc_store" first, then falls back to
    plain "import store as mc_store" for development environments.
  - For seamless integration with sglang, this class optionally depends on
    sglang.srt APIs. If not installed, an informative error will be raised
    when the functionality is used.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple
import importlib

import torch
from torch import nn

logger = logging.getLogger(__name__)

# Import Mooncake Python binding (packaged as mooncake.store) with importlib
_mc_mod = None
try:  # prefer packaged path
    _mc_mod = importlib.import_module("mooncake.store")
except Exception:  # pragma: no cover - dev fallback path
    try:
        _mc_mod = importlib.import_module("store")
    except Exception as _e:  # pragma: no cover
        _mc_mod = None
        logger.debug("Mooncake store extension not found: %s", _e)

if _mc_mod is None:  # pragma: no cover
    raise ImportError(
        "Cannot import Mooncake store extension. Ensure the wheel is installed and contains mooncake.store (store.so)."
    )

mc_store = _mc_mod


# Optional sglang integration (defined dynamically if present)
class BaseModelLoader:  # lightweight base to keep this module importable
    def __init__(self, load_config):
        self.load_config = load_config


def create_store(**setup_kwargs) -> Any:
    """Create and initialize a MooncakeDistributedStore.

    Two initialization modes are supported (choose one):
      - setup: expects keys matching MooncakeDistributedStore.setup signature.
      - init_all: expects keys matching MooncakeDistributedStore.init_all signature.

    Example:
      create_store(
        setup=dict(
          local_hostname="node1",
          metadata_server="127.0.0.1:2379",
          global_segment_size=256<<20,
          local_buffer_size=256<<20,
          protocol="tcp",
          rdma_devices="",
          master_server_addr="127.0.0.1:50051",
        )
      )
    """
    store = mc_store.MooncakeDistributedStore()
    if "setup" in setup_kwargs:
        cfg = setup_kwargs["setup"]
        store.setup(
            cfg["local_hostname"],
            cfg["metadata_server"],
            cfg.get("global_segment_size", 1024 * 1024 * 16),
            cfg.get("local_buffer_size", 1024 * 1024 * 16),
            cfg.get("protocol", "tcp"),
            cfg.get("rdma_devices", ""),
            cfg.get("master_server_addr", "127.0.0.1:50051"),
        )
    elif "init_all" in setup_kwargs:
        cfg = setup_kwargs["init_all"]
        store.init_all(
            cfg.get("protocol", "tcp"),
            cfg["device_name"],
            cfg.get("mount_segment_size", 1024 * 1024 * 16),
        )
    else:
        raise ValueError("create_store requires a 'setup' or 'init_all' config block")
    return store


def _maybe_define_sglang_loader() -> None:
    """Define MooncakeModelLoader integrated with sglang if available."""
    try:
        mdl_loader = importlib.import_module("sglang.srt.model_loader.loader")
        mdl_utils = importlib.import_module("sglang.srt.model_loader.utils")
        dist_mod = importlib.import_module("sglang.srt.distributed")
    except Exception as e:  # sglang not available; skip defining class
        logger.debug("sglang not available for MooncakeModelLoader: %s", e)
        return

    Base = getattr(mdl_loader, "BaseModelLoader")
    _initialize_model = getattr(mdl_loader, "_initialize_model")
    device_loading_context = getattr(mdl_loader, "device_loading_context")
    set_default_torch_dtype = getattr(mdl_utils, "set_default_torch_dtype")
    get_tp_rank = getattr(dist_mod, "get_tensor_model_parallel_rank")

    class MooncakeModelLoader(Base):  # type: ignore
        """Model loader that pulls tensors from Mooncake by keys (sglang integrated)."""

        DEFAULT_THREADS = 8

        def __init__(self, load_config):
            super().__init__(load_config)
            extra = getattr(load_config, "model_loader_extra_config", None) or {}
            self.prefix: str = extra.get("prefix")
            if not self.prefix:
                raise ValueError("MooncakeModelLoader requires extra_config.prefix")

            self.io_threads: int = int(extra.get("io_threads", self.DEFAULT_THREADS))
            self.rank_scope: str = str(extra.get("rank_scope", "sharded"))

            mc_cfg = extra.get("mooncake", {})
            self._store = create_store(**mc_cfg)

        def _manifest_key(self) -> str:
            return f"{self.prefix}/manifest.json"

        def _weight_key(self, param_name: str, tp_rank: int) -> str:
            if self.rank_scope == "sharded":
                return f"{self.prefix}/rank_{tp_rank}/{param_name}"
            return f"{self.prefix}/{param_name}"

        def _load_manifest(self) -> List[str]:
            raw = self._store.get(self._manifest_key())
            if raw is None or len(raw) == 0:
                raise RuntimeError(f"Manifest not found or empty: {self._manifest_key()}")
            try:
                return json.loads(raw.decode("utf-8"))
            except Exception as e:  # pragma: no cover
                raise RuntimeError(
                    f"Invalid manifest content at {self._manifest_key()}: {e}"
                ) from e

        def _weights_iterator_parallel(
            self, names: List[str], tp_rank: int
        ) -> Generator[Tuple[str, torch.Tensor], None, None]:
            with ThreadPoolExecutor(max_workers=self.io_threads) as pool:
                futures = {}
                for name in names:
                    key = self._weight_key(name, tp_rank)
                    futures[name] = pool.submit(self._store.get_tensor, key)

                for name in names:
                    tensor = futures[name].result()
                    if tensor is None:
                        raise RuntimeError(f"Missing tensor for {name}")
                    if not isinstance(tensor, torch.Tensor):
                        raise TypeError(f"Invalid object for {name}: {type(tensor)}")
                    yield name, tensor.contiguous()

        def download_model(self, model_config) -> None:
            self._load_manifest()

        def load_model(self, *, model_config, device_config) -> nn.Module:  # type: ignore[override]
            target_device = torch.device(device_config.device)
            tp_rank = get_tp_rank()

            with set_default_torch_dtype(model_config.dtype):
                with target_device:
                    model = _initialize_model(model_config, self.load_config)

                names = self._load_manifest()
                weights_iter = self._weights_iterator_parallel(names, tp_rank)

                model.load_weights(weights_iter)

                for _, module in model.named_modules():
                    quant_method = getattr(module, "quant_method", None)
                    if quant_method is not None:
                        with device_loading_context(module, target_device):
                            quant_method.process_weights_after_loading(module)

            return model.eval()

    globals()["MooncakeModelLoader"] = MooncakeModelLoader


# call definition hook at import time
_maybe_define_sglang_loader()


def save_model_to_mooncake(
    model: nn.Module,
    store: Any,
    prefix: str,
    tp_rank: int = 0,
    use_gpu_direct: bool = True,
    io_threads: int = 8,
) -> None:
    """Push current rank's parameters to Mooncake with a manifest.

    - Writes `{prefix}/manifest.json` as the ordered parameter list.
    - Writes tensors under `{prefix}/rank_{tp_rank}/{param_fqn}`.
    - If `use_gpu_direct=False`, tensors are moved to CPU before upload.
    """
    state = model.state_dict()
    names = list(state.keys())

    # Manifest first to advertise expected content (can be moved to last with a ready flag)
    manifest_key = f"{prefix}/manifest.json"
    payload = json.dumps(names).encode("utf-8")
    rc = store.put(manifest_key, payload)
    if rc != 0:  # pragma: no cover
        raise RuntimeError(f"Failed to write manifest to {manifest_key}, rc={rc}")

    def _upload_one(name: str):
        key = f"{prefix}/rank_{tp_rank}/{name}"
        t = state[name]
        if not t.is_contiguous():
            t = t.contiguous()
        if not use_gpu_direct and t.device.type != "cpu":
            t = t.cpu()
        ret = store.put_tensor(key, t)
        if ret != 0:
            raise RuntimeError(f"put_tensor failed for {name} with code {ret}")

    # Parallelize uploads
    with ThreadPoolExecutor(max_workers=io_threads) as pool:
        futures = [pool.submit(_upload_one, n) for n in names]
        for f in as_completed(futures):
            f.result()


__all__ = [
    "MooncakeModelLoader",
    "save_model_to_mooncake",
    "create_store",
]
