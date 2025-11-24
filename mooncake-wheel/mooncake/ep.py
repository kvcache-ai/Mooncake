import importlib
from packaging.version import Version
import torch
torch_version = Version(torch.__version__).base_version
version_suffix = "_" + torch_version.replace(".", "_")

backend_module = importlib.import_module(f"mooncake.ep_backend{version_suffix}")
globals().update({k: v for k, v in backend_module.__dict__.items() if not k.startswith("_")})

from mooncake.ep_cpp import *
