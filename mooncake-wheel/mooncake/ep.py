import importlib
import os
import re

import torch

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

# The PG module provides the mooncake process-group backend and utilities
# like get_active_ranks.  On MUSA the PG module is not yet ported, so we
# skip the import and provide lightweight Python fallbacks.
if not USE_MUSA:
    try:
        backend_compat_module = importlib.import_module("mooncake.pg" + version_suffix)
    except ModuleNotFoundError:
        raise ImportError(
            f"Mooncake PG was not built against torch=={torch_version}.\n"
            f"Open an issue at https://github.com/kvcache-ai/Mooncake/issues."
        )
    globals().update({k: v for k, v in backend_compat_module.__dict__.items() if not k.startswith("_")})
else:
    # MUSA fallback: provide get_active_ranks as a simple wrapper that
    # returns an all-ones tensor (all ranks active).  Fault tolerance
    # (rank failure detection) is not yet supported on MUSA.
    import torch.distributed as dist

    def get_active_ranks(group):
        size = dist.get_world_size(group)
        device = "musa:" + str(torch.musa.current_device())
        return torch.ones(size, dtype=torch.int32, device=device)