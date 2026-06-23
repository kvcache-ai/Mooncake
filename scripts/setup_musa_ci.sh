#!/usr/bin/env bash
set -euxo pipefail

MUSA_HOME="${MUSA_HOME:-/usr/local/musa}"
TORCH_MUSA_WHEEL_URL="${TORCH_MUSA_WHEEL_URL:-https://cloud.tsinghua.edu.cn/f/6213611820c34bb881fd/?dl=1}"
TORCH_MUSA_WHEEL="${RUNNER_TEMP:-/tmp}/torch_musa-2.9.0-cp310-cp310-linux_x86_64.whl"

append_env() {
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "$1" >> "${GITHUB_ENV}"
  fi
  export "$1"
}

append_path() {
  if [[ -n "${GITHUB_PATH:-}" ]]; then
    echo "$1" >> "${GITHUB_PATH}"
  fi
  export PATH="$1:${PATH}"
}

install_base_packages() {
  apt update -y
  apt install -y curl libopenblas0 python3-pip
  python3 -m pip install --no-cache-dir --upgrade pip
}

install_torch_stack() {
  python3 - <<'PY'
import sys
if sys.version_info[:2] != (3, 10):
    raise SystemExit(
        "torch_musa 2.9.0 wheel is cp310-only; this CI job must use Python 3.10"
    )
PY
  python3 -m pip install --no-cache-dir \
    torch==2.9.0 \
    --index-url https://download.pytorch.org/whl/cpu
  python3 -m pip install --no-cache-dir \
    'numpy<2' \
    packaging \
    torchada==0.1.66

  curl -L --fail --retry 8 --retry-all-errors --retry-delay 5 --connect-timeout 30 \
    -o "${TORCH_MUSA_WHEEL}" \
    "${TORCH_MUSA_WHEEL_URL}"
  python3 -m pip install --no-cache-dir --force-reinstall --no-deps "${TORCH_MUSA_WHEEL}"
}

setup_torch_musa_env() {
  torch_musa_includes=$(python3 - <<'PY'
import pathlib
import site

paths = []
for site_dir in site.getsitepackages():
    root = pathlib.Path(site_dir)
    if (root / "torch_musa").is_dir():
        # torchada's JIT helper includes torch_musa internal headers as
        # <torch_musa/...>; the vendor wheel ships them under site-packages.
        paths.append(str(root))
        generated = root / "torch_musa" / "share" / "generated_cuda_compatible" / "include"
        if generated.is_dir():
            paths.append(str(generated))
print(":".join(paths))
PY
  )

  torch_libs=$(python3 - <<'PY'
import pathlib
import site

paths = []
for site_dir in site.getsitepackages():
    for package in ("torch", "torch_musa"):
        lib = pathlib.Path(site_dir) / package / "lib"
        if lib.is_dir():
            paths.append(str(lib))
print(":".join(paths))
PY
  )

  musa_libs=$(
    {
      printf '%s\n' \
        "${MUSA_HOME}/lib" \
        "${MUSA_HOME}/mudnn/lib" \
        "${MUSA_HOME}/mccl/lib" \
        "${MUSA_HOME}/mufft/lib"
      find -L "${MUSA_HOME}" -type f -name 'lib*.so*' -printf '%h\n' 2>/dev/null
    } | awk 'NF && !seen[$0]++' | paste -sd: -
  )

  append_env "MUSA_HOME=${MUSA_HOME}"
  # torchada deliberately exposes the active accelerator root as CUDA_HOME so
  # existing CUDAExtension-based setup.py files do not need a CUDA-shaped shim.
  append_env "CUDA_HOME=${MUSA_HOME}"
  append_env "CPATH=${MUSA_HOME}/include:${torch_musa_includes}:${CPATH:-}"
  append_env "TORCHADA_PLATFORM=musa"
  append_env "TORCH_DEVICE_BACKEND_AUTOLOAD=0"
  append_env "LD_LIBRARY_PATH=${musa_libs}:${torch_libs}:${LD_LIBRARY_PATH:-}"
  append_path "${MUSA_HOME}/bin"
}

verify_env() {
  python3 - <<'PY'
import importlib.metadata
import os
import torch
from torchada._platform import detect_platform

print("torch", torch.__version__)
print("torch_musa", importlib.metadata.version("torch_musa"))
print("torchada", importlib.metadata.version("torchada"))
print("torchada platform", detect_platform().value)
print("CUDA_HOME", os.environ.get("CUDA_HOME"))
print("MUSA_HOME", os.environ.get("MUSA_HOME"))
PY
}

install_base_packages
install_torch_stack
setup_torch_musa_env
verify_env
