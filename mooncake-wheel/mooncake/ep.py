import importlib
import os
import re

import torch
import torch.distributed as dist

torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")

USE_MUSA = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}

try:
    backend_module = importlib.import_module("mooncake.ep" + version_suffix)
except ModuleNotFoundError:
    raise ImportError(
        f"Mooncake EP was not built against torch=={torch_version}.\n"
        f"Open an issue at https://github.com/kvcache-ai/Mooncake/issues."
    )
globals().update({k: v for k, v in backend_module.__dict__.items() if not k.startswith("_")})

# PG module is optional — it may not be built (e.g. MUSA builds where PG
# source depends on CUDA headers).  Fall back to a simple all-ones tensor.
try:
    backend_compat_module = importlib.import_module("mooncake.pg" + version_suffix)
    globals().update({k: v for k, v in backend_compat_module.__dict__.items() if not k.startswith("_")})
except (ModuleNotFoundError, ImportError):
    def get_active_ranks(group):
        size = dist.get_world_size(group)
        if USE_MUSA:
            device = "musa:" + str(torch.musa.current_device())
        else:
            device = "cuda:" + str(torch.cuda.current_device())
        return torch.ones(size, dtype=torch.int32, device=device)