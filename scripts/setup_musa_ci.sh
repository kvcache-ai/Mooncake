#!/usr/bin/env bash
set -euxo pipefail

MUSA_HOME="${MUSA_HOME:-/usr/local/musa}"
MUSA_COMPAT_LIB_DIR="${RUNNER_TEMP:-/tmp}/mooncake-musa-libs"
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

setup_musa_library_compat() {
  rm -rf "${MUSA_COMPAT_LIB_DIR}"
  mkdir -p "${MUSA_COMPAT_LIB_DIR}"

  local roots=(
    "${MUSA_HOME}/lib"
    "${MUSA_HOME}/lib64"
    "${MUSA_HOME}/mudnn/lib"
    "${MUSA_HOME}/mccl/lib"
    "${MUSA_HOME}/mufft/lib"
    "${MUSA_HOME}/mublas/lib"
    "${MUSA_HOME}/musolver/lib"
    "${MUSA_HOME}/musparse/lib"
    "/driver/usr/local/musa/lib"
    "/driver/usr/lib/x86_64-linux-gnu"
    "/usr/lib/x86_64-linux-gnu"
  )

  find_musa_library() {
    local soname="$1"
    local base="${soname%%.so*}.so"
    local root candidate
    shopt -s nullglob
    for root in "${roots[@]}"; do
      for candidate in "${root}/${soname}" "${root}/${base}" "${root}/${base}".*; do
        if [[ -e "${candidate}" ]]; then
          printf '%s\n' "${candidate}"
          shopt -u nullglob
          return 0
        fi
      done
    done
    candidate=$(find -L /usr/local/musa /driver /usr/lib /lib \
      -type f \( -name "${soname}" -o -name "${base}" -o -name "${base}.*" \) \
      -print -quit 2>/dev/null || true)
    if [[ -n "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      shopt -u nullglob
      return 0
    fi
    shopt -u nullglob
    return 1
  }

  link_musa_soname() {
    local soname="$1"
    local target
    if target="$(find_musa_library "${soname}")"; then
      ln -sf "${target}" "${MUSA_COMPAT_LIB_DIR}/${soname}"
      return 0
    fi
    return 1
  }

  local soname
  for soname in \
    libmusart.so.4 \
    libmusa.so.1 \
    libmudnn.so.3 \
    libmccl.so.2 \
    libmublas.so.1 \
    libmublasLt.so.1 \
    libmusolver.so.1 \
    libmusparse.so \
    libmufft.so.1; do
    if ! link_musa_soname "${soname}"; then
      echo "warning: could not find ${soname} under ${MUSA_HOME}" >&2
    fi
  done
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
  # Keep torch_musa's generated CUDA-compatible headers out of global CPATH:
  # they intentionally shadow torch/ATen headers and can break unrelated JIT
  # builds such as torchada's import-time C++ ops.  MUSAExtension adds those
  # headers where they are needed for the actual extension build.
  append_env "CPATH=${MUSA_HOME}/include:${torch_musa_includes}:${CPATH:-}"
  append_env "TORCHADA_PLATFORM=musa"
  append_env "TORCH_DEVICE_BACKEND_AUTOLOAD=0"
  append_env "LD_LIBRARY_PATH=${MUSA_COMPAT_LIB_DIR}:${musa_libs}:${torch_libs}:${LD_LIBRARY_PATH:-}"
  append_path "${MUSA_HOME}/bin"
}

verify_env() {
  python3 - <<'PY'
import importlib.metadata
import os
import torch
import torch_musa
import torch_musa.utils.musa_extension as musa_extension
from torchada._platform import detect_platform

print("torch", torch.__version__)
print("torch_musa", importlib.metadata.version("torch_musa"))
print("torchada", importlib.metadata.version("torchada"))
print("torchada platform", detect_platform().value)
print("CUDA_HOME", os.environ.get("CUDA_HOME"))
print("MUSA_HOME", os.environ.get("MUSA_HOME"))
print("torch_musa file", torch_musa.__file__)
print("musa include paths", ":".join(musa_extension.include_paths(musa=True)))
PY
}

install_base_packages
install_torch_stack
setup_musa_library_compat
setup_torch_musa_env
verify_env
