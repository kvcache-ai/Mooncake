import importlib
import re

import torch

torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")

try:
    backend_module = importlib.import_module("mooncake.pg" + version_suffix)
except ModuleNotFoundError:
    raise ImportError(
        f"Mooncake PG was not built against torch=={torch_version}.\n"
        f"Open an issue at https://github.com/kvcache-ai/Mooncake/issues."
    )
globals().update({k: v for k, v in backend_module.__dict__.items() if not k.startswith("_")})
