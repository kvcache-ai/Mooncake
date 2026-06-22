#!/usr/bin/env bash
set -euxo pipefail

MUSA_HOME="${MUSA_HOME:-/usr/local/musa}"
CUDA_COMPAT_HOME="${RUNNER_TEMP:-/tmp}/mooncake-musa-cuda"
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

python_has_module() {
  python3 - "$1" <<'PY'
import importlib.util
import sys
sys.exit(0 if importlib.util.find_spec(sys.argv[1]) else 1)
PY
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

  curl -L --fail --retry 3 --connect-timeout 20 \
    -o "${TORCH_MUSA_WHEEL}" \
    "${TORCH_MUSA_WHEEL_URL}"
  python3 -m pip install --no-cache-dir --force-reinstall --no-deps "${TORCH_MUSA_WHEEL}"
}

write_nvcc_wrapper() {
  mkdir -p "${CUDA_COMPAT_HOME}/bin"
  cat > "${CUDA_COMPAT_HOME}/bin/nvcc" <<'SH'
#!/usr/bin/env bash
args=()
if [[ -d "${CUDA_HOME:-}/include" ]]; then
  args+=(-I"${CUDA_HOME}/include")
fi
skip_compiler_options=0
for arg in "$@"; do
  if [[ ${skip_compiler_options} -eq 1 ]]; then
    arg="${arg%\'}"
    arg="${arg#\'}"
    args+=("${arg}")
    skip_compiler_options=0
    continue
  fi
  case "${arg}" in
    --expt-relaxed-constexpr)
      ;;
    --cuda-gpu-arch=*)
      args+=("--offload-arch=${arg#--cuda-gpu-arch=}")
      ;;
    --compiler-options)
      skip_compiler_options=1
      ;;
    *.cu)
      mu_source="${TMPDIR:-/tmp}/$(basename "${arg%.cu}").mu"
      cp "${arg}" "${mu_source}"
      args+=("${mu_source}")
      ;;
    *)
      args+=("${arg}")
      ;;
  esac
done
exec /usr/local/musa/bin/mcc "${args[@]}"
SH
  chmod +x "${CUDA_COMPAT_HOME}/bin/nvcc"
}

setup_torch_musa_env() {
  # Keep CUDAExtension/torchada on their expected CUDA_HOME-shaped layout while
  # using the real MUSA SDK and torch_musa wheel.  This is a symlink tree, not a
  # generated CUDA shim and not a patch to /usr/local/musa.
  rm -rf "${CUDA_COMPAT_HOME}"
  mkdir -p "${CUDA_COMPAT_HOME}"
  ln -s "${MUSA_HOME}/include" "${CUDA_COMPAT_HOME}/include"
  ln -s "${MUSA_HOME}/lib" "${CUDA_COMPAT_HOME}/lib64"
  write_nvcc_wrapper

  append_env "MUSA_HOME=${MUSA_HOME}"
  append_env "CUDA_HOME=${CUDA_COMPAT_HOME}"
  append_env "MOONCAKE_MUSA_USE_CUDA_COMPAT_SHIMS=0"
  append_env "CPATH=${MUSA_HOME}/include:${CPATH:-}"
  append_env "TORCHADA_PLATFORM=musa"
  append_env "TORCH_DEVICE_BACKEND_AUTOLOAD=0"
  append_path "${CUDA_COMPAT_HOME}/bin"

  # Export MUSA and PyTorch library directories before any build subprocess tries
  # to load torch_musa extension libraries.  Do not import torch_musa while
  # computing these paths: its extension modules need this LD_LIBRARY_PATH first.
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
  append_env "LD_LIBRARY_PATH=${musa_libs}:${torch_libs}:${LD_LIBRARY_PATH:-}"
}

verify_env() {
  python3 - <<'PY'
import os
import torch
import torch_musa
import torchada

print("torch", torch.__version__)
print("torch_musa", getattr(torch_musa, "__version__", "unknown"))
print("torchada", getattr(torchada, "__version__", "unknown"))
print("torchada platform", torchada.get_platform().value)
print("CUDA_HOME", os.environ.get("CUDA_HOME"))
print("MUSA_HOME", os.environ.get("MUSA_HOME"))
print("MOONCAKE_MUSA_USE_CUDA_COMPAT_SHIMS", os.environ.get("MOONCAKE_MUSA_USE_CUDA_COMPAT_SHIMS"))
PY
}

install_base_packages
install_torch_stack
setup_torch_musa_env
verify_env
