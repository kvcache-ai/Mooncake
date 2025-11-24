import importlib
from packaging.version import Version

import torch

torch_version = Version(torch.__version__).base_version
version_suffix = "_" + torch_version.replace(".", "_")

try:
    backend_module = importlib.import_module("mooncake.ep" + version_suffix)
except ModuleNotFoundError:
    raise ImportError(
        f"Mooncake EP was not built against torch=={torch_version}.\n"
        f"Open an issue at https://github.com/kvcache-ai/Mooncake/issues."
    )
globals().update({k: v for k, v in backend_module.__dict__.items() if not k.startswith("_")})
