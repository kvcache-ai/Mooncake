"""Optional host bridge; the CUDA READ/wait kernels come from pinned DeepEP."""

import functools
import os
from pathlib import Path


@functools.lru_cache(maxsize=1)
def load_bridge():
    from torch.utils.cpp_extension import CUDA_HOME, load

    root = Path(os.environ.get("MOONCAKE_DEEPEP_ROOT", ""))
    nccl = Path(os.environ.get("EP_NCCL_ROOT_DIR", ""))
    if not (root / "csrc/kernels/engram/engram.hpp").is_file():
        raise RuntimeError(
            "Set MOONCAKE_DEEPEP_ROOT to the pinned DeepEP source checkout"
        )
    if not (nccl / "include/nccl_device.h").is_file():
        raise RuntimeError("EP_NCCL_ROOT_DIR must contain NCCL with GIN/GDAKI support")
    source = Path(__file__).with_name("_engram_deepep_bridge.cpp")
    bridge = load(
        name="mooncake_engram_deepep",
        sources=[
            str(source),
            str(root / "csrc/kernels/comm/context.cpp"),
            str(root / "csrc/kernels/driver/driver.cpp"),
        ],
        extra_include_paths=[
            str(root),
            str(root / "deep_ep/include"),
            str(root / "third-party/deep_jit/include"),
            str(nccl / "include"),
            str(Path(CUDA_HOME) / "include/cccl"),
        ],
        extra_cflags=["-std=c++20", "-O2", "-Wno-deprecated-declarations"],
        extra_ldflags=[
            f"-L{nccl / 'lib'}",
            "-l:libnccl.so.2",
            f"-Wl,-rpath,{nccl / 'lib'}",
            "-lcuda",
        ],
        with_cuda=True,
    )
    # Never reset the JIT runtime when a later layer initializes: already
    # captured graphs may retain functions compiled by this runtime.
    bridge.init_jit(str(root / "deep_ep"), str(nccl))
    return bridge
