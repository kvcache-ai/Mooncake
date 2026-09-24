#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/build/build-wheel-ubuntu-docker.sh
#
# Build the Python wheels inside a pinned Ubuntu Docker image while reusing
# scripts/build/build-wheel.sh as the single packaging implementation.
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
GIT_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
REPO_ROOT=${GIT_ROOT}
REPO_PATH_IN_MOUNT=
if [[ ! -f "${REPO_ROOT}/Cargo.toml" ]]; then
  REPO_PATH_IN_MOUNT=mooncake-store/store-rs
  REPO_ROOT="${GIT_ROOT}/${REPO_PATH_IN_MOUNT}"
fi
MOUNT_ROOT=${GIT_ROOT}

UBUNTU_VERSION=${UBUNTU_VERSION:-22.04}
PYTHON_VERSION=${PYTHON_VERSION:-system}
DOCKER_IMAGE=${DOCKER_IMAGE:-}
PYTHON_TAG=
MOUNT_POINT_IN_CONTAINER=${MOUNT_POINT_IN_CONTAINER:-/work}
WORKDIR_IN_CONTAINER=${WORKDIR_IN_CONTAINER:-${MOUNT_POINT_IN_CONTAINER}}
if [[ -n "${REPO_PATH_IN_MOUNT}" && "${WORKDIR_IN_CONTAINER}" == "${MOUNT_POINT_IN_CONTAINER}" ]]; then
  WORKDIR_IN_CONTAINER="${MOUNT_POINT_IN_CONTAINER}/${REPO_PATH_IN_MOUNT}"
fi
CACHE_DIR=${MOONCAKE_DOCKER_CACHE_DIR:-"${REPO_ROOT}/target/docker-wheel-cache"}
CACHE_DIR_IN_CONTAINER=${CACHE_DIR_IN_CONTAINER:-/cache}
REBUILD_IMAGE=${REBUILD_IMAGE:-0}
PULL_IMAGE=${PULL_IMAGE:-1}
CN_MIRROR=${CN_MIRROR:-1}
CN_RUSTUP_DIST_SERVER=${CN_RUSTUP_DIST_SERVER:-https://mirrors.ustc.edu.cn/rust-static}
CN_RUSTUP_UPDATE_ROOT=${CN_RUSTUP_UPDATE_ROOT:-https://mirrors.ustc.edu.cn/rust-static/rustup}
CN_PIP_INDEX_URL=${CN_PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}
CN_PIP_TRUSTED_HOST=${CN_PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}
CN_CRATES_REGISTRY=${CN_CRATES_REGISTRY:-sparse+https://mirrors.ustc.edu.cn/crates.io-index/}

usage() {
  cat <<'EOF'
Usage: scripts/build/build-wheel-ubuntu-docker.sh [maturin build args...]

Build the Mooncake Python wheels inside an Ubuntu Docker image and write the
same artifacts as scripts/build/build-wheel.sh:

  dist/wheels/mooncake_store_rs-*.whl
  dist/bin/mooncake-store-client
  dist/bin/mooncake-store-admin

Environment:
  UBUNTU_VERSION              Ubuntu base image tag (default: 22.04)
  PYTHON_VERSION              Python runtime in the builder image: system, 3.10, 3.11, 3.12
  DOCKER_IMAGE                Builder image name (default: mooncake-store-wheel:ubuntu-${UBUNTU_VERSION}-py<version>)
  DOCKER_PLATFORM             Optional Docker platform, e.g. linux/amd64
  DOCKER_NETWORK              Optional network for docker run, e.g. host
  DOCKER_BUILD_NETWORK        Optional network for docker build
  REBUILD_IMAGE=1             Rebuild the builder image even if it exists
  PULL_IMAGE=0                Do not pull the Ubuntu base image while rebuilding
  CN_MIRROR=0                 Disable the default China mirrors for rustup/pip/cargo
  MOONCAKE_DOCKER_CACHE_DIR   Cache for HOME/CARGO_HOME/upstream CMake builds
                              (default: target/docker-wheel-cache)
  CACHE_DIR_IN_CONTAINER      Container mountpoint for the cache (default: /cache)
  CN_RUSTUP_DIST_SERVER       Rust toolchain mirror (default: USTC rust-static)
  CN_RUSTUP_UPDATE_ROOT       Rustup metadata mirror (default: USTC rust-static/rustup)
  CN_PIP_INDEX_URL            Python package mirror (default: Tsinghua PyPI)
  CN_PIP_TRUSTED_HOST         Trusted host for CN_PIP_INDEX_URL
  CN_CRATES_REGISTRY          Cargo sparse registry mirror (default: USTC crates.io)

Forwarded build environment:
  BUILD_JOBS
  DIST_DIR
  WHEEL_VENV
  MOONCAKE_UPSTREAM_DIR
  MOONCAKE_UPSTREAM_BUILD_DIR
  PYTHON
  HTTP_PROXY, HTTPS_PROXY, NO_PROXY and lowercase variants
  PIP_INDEX_URL, PIP_EXTRA_INDEX_URL, PIP_TRUSTED_HOST
  RUSTUP_DIST_SERVER, RUSTUP_UPDATE_ROOT
  CARGO_REGISTRIES_CRATES_IO_PROTOCOL, CARGO_NET_GIT_FETCH_WITH_CLI

Path rule:
  DIST_DIR, WHEEL_VENV, MOONCAKE_UPSTREAM_DIR, and
  MOONCAKE_UPSTREAM_BUILD_DIR must be relative paths or absolute paths inside
  this repository. External host paths are intentionally not mounted.

Examples:
  ./scripts/build/build-wheel-ubuntu-docker.sh
  PYTHON_VERSION=3.11 ./scripts/build/build-wheel-ubuntu-docker.sh
  BUILD_JOBS=16 ./scripts/build/build-wheel-ubuntu-docker.sh
  UBUNTU_VERSION=24.04 REBUILD_IMAGE=1 ./scripts/build/build-wheel-ubuntu-docker.sh
  DOCKER_NETWORK=host ./scripts/build/build-wheel-ubuntu-docker.sh --compatibility linux
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_command() {
  local command_name=$1
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "missing required command: ${command_name}" >&2
    exit 1
  fi
}

is_truthy() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES | on | ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

set_default_if_unset() {
  local name=$1
  local value=$2

  if [[ -z "${!name-}" ]]; then
    printf -v "${name}" '%s' "${value}"
    export "${name}"
  fi
}

configure_python_selection() {
  local py_basename=
  local inferred_version=system

  if [[ -n "${PYTHON:-}" ]]; then
    py_basename=$(basename -- "${PYTHON}")
    case "${py_basename}" in
      python3.10 | python3.11 | python3.12)
        inferred_version=${py_basename#python}
        ;;
      python3)
        inferred_version=system
        ;;
    esac
  fi

  if [[ "${PYTHON_VERSION}" == "system" && "${inferred_version}" != "system" ]]; then
    PYTHON_VERSION=${inferred_version}
  fi

  case "${PYTHON_VERSION}" in
    system)
      ;;
    3.10 | 3.11 | 3.12)
      if [[ -z "${PYTHON:-}" ]]; then
        PYTHON="python${PYTHON_VERSION}"
        export PYTHON
      elif [[ -n "${py_basename}" && "${py_basename}" != "python${PYTHON_VERSION}" ]]; then
        echo "PYTHON_VERSION=${PYTHON_VERSION} conflicts with PYTHON=${PYTHON}" >&2
        exit 1
      fi
      ;;
    *)
      echo "unsupported PYTHON_VERSION: ${PYTHON_VERSION} (expected system, 3.10, 3.11, or 3.12)" >&2
      exit 1
      ;;
  esac

  PYTHON_TAG="py${PYTHON_VERSION//./}"
  if [[ -z "${DOCKER_IMAGE}" ]]; then
    DOCKER_IMAGE="mooncake-store-wheel:ubuntu-${UBUNTU_VERSION}-${PYTHON_TAG}"
  fi
}

configure_cn_mirrors() {
  local cargo_config
  local managed_header="# managed by build-wheel-ubuntu-docker.sh"

  cargo_config="${CACHE_DIR}/cargo/config.toml"

  if ! is_truthy "${CN_MIRROR}"; then
    if [[ -f "${cargo_config}" ]] && grep -qF "${managed_header}" "${cargo_config}"; then
      rm -f "${cargo_config}"
    fi
    return 0
  fi

  set_default_if_unset RUSTUP_DIST_SERVER "${CN_RUSTUP_DIST_SERVER}"
  set_default_if_unset RUSTUP_UPDATE_ROOT "${CN_RUSTUP_UPDATE_ROOT}"
  set_default_if_unset PIP_INDEX_URL "${CN_PIP_INDEX_URL}"
  set_default_if_unset PIP_TRUSTED_HOST "${CN_PIP_TRUSTED_HOST}"
  set_default_if_unset CARGO_REGISTRIES_CRATES_IO_PROTOCOL sparse
  set_default_if_unset CARGO_NET_GIT_FETCH_WITH_CLI true

  if [[ -f "${cargo_config}" ]] && ! grep -qF "${managed_header}" "${cargo_config}"; then
    return 0
  fi

  cat >"${cargo_config}" <<EOF
# managed by build-wheel-ubuntu-docker.sh
[source.crates-io]
replace-with = "cn-mirror"

[source.cn-mirror]
registry = "${CN_CRATES_REGISTRY}"

[net]
git-fetch-with-cli = true
EOF
}

map_repo_path() {
  local name=$1
  local value=$2
  local relative

  if [[ "${value}" != /* ]]; then
    printf '%s\n' "${value}"
    return 0
  fi

  if [[ "${value}" == "${MOUNT_ROOT}" ]]; then
    printf '%s\n' "${MOUNT_POINT_IN_CONTAINER}"
    return 0
  fi

  if [[ "${value}" == "${MOUNT_ROOT}/"* ]]; then
    relative=${value#"${MOUNT_ROOT}/"}
    printf '%s/%s\n' "${MOUNT_POINT_IN_CONTAINER}" "${relative}"
    return 0
  fi

  echo "${name} must point inside the repository for Docker builds: ${value}" >&2
  exit 1
}

append_env_if_set() {
  local name=$1
  local value=${!name-}

  if [[ -n "${value}" ]]; then
    DOCKER_RUN_ARGS+=(-e "${name}=${value}")
  fi
}

append_repo_path_env_if_set() {
  local name=$1
  local value=${!name-}
  local mapped

  if [[ -z "${value}" ]]; then
    return 0
  fi

  mapped=$(map_repo_path "${name}" "${value}")
  DOCKER_RUN_ARGS+=(-e "${name}=${mapped}")
}

build_image() {
  local -a docker_build_args=(
    build
    --build-arg "UBUNTU_VERSION=${UBUNTU_VERSION}"
    --build-arg "PYTHON_VERSION=${PYTHON_VERSION}"
    --tag "${DOCKER_IMAGE}"
  )
  local name
  local value

  if [[ -n "${DOCKER_PLATFORM:-}" ]]; then
    docker_build_args+=(--platform "${DOCKER_PLATFORM}")
  fi
  if [[ -n "${DOCKER_BUILD_NETWORK:-}" ]]; then
    docker_build_args+=(--network "${DOCKER_BUILD_NETWORK}")
  elif [[ -n "${DOCKER_NETWORK:-}" ]]; then
    docker_build_args+=(--network "${DOCKER_NETWORK}")
  fi
  if is_truthy "${REBUILD_IMAGE}"; then
    docker_build_args+=(--no-cache)
  fi
  if is_truthy "${PULL_IMAGE}"; then
    docker_build_args+=(--pull)
  fi

  for name in \
    HTTP_PROXY \
    HTTPS_PROXY \
    NO_PROXY \
    http_proxy \
    https_proxy \
    no_proxy \
    RUSTUP_DIST_SERVER \
    RUSTUP_UPDATE_ROOT; do
    value=${!name-}
    if [[ -n "${value}" ]]; then
      docker_build_args+=(--build-arg "${name}=${value}")
    fi
  done

  docker "${docker_build_args[@]}" - <<'DOCKERFILE'
ARG UBUNTU_VERSION=22.04
ARG PYTHON_VERSION=system
FROM ubuntu:${UBUNTU_VERSION}

ARG PYTHON_VERSION
ARG RUSTUP_DIST_SERVER
ARG RUSTUP_UPDATE_ROOT

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    file \
    git \
    libasio-dev \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    libgrpc++-dev \
    libgrpc-dev \
    libgtest-dev \
    libhiredis-dev \
    libibverbs-dev \
    libjemalloc-dev \
    libjsoncpp-dev \
    libmsgpack-dev \
    libnuma-dev \
    libprotobuf-dev \
    libssl-dev \
    libunwind-dev \
    liburing-dev \
    libxxhash-dev \
    libyaml-cpp-dev \
    libzstd-dev \
    ninja-build \
    gnupg \
    patchelf \
    pkg-config \
    protobuf-compiler \
    protobuf-compiler-grpc \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3-venv \
    python3-wheel \
    software-properties-common \
    unzip \
    wget \
    zlib1g-dev \
 && if [[ "${PYTHON_VERSION}" != "system" ]]; then add-apt-repository -y ppa:deadsnakes/ppa; fi \
 && if [[ "${PYTHON_VERSION}" != "system" ]]; then apt-get update; fi \
 && case "${PYTHON_VERSION}" in \
      system) \
        ;; \
      3.10 | 3.11 | 3.12) \
        apt-get install -y --no-install-recommends \
          "python${PYTHON_VERSION}" \
          "python${PYTHON_VERSION}-dev" \
          "python${PYTHON_VERSION}-venv" \
        ;; \
      *) \
        echo "unsupported PYTHON_VERSION: ${PYTHON_VERSION}" >&2 \
        && exit 1 \
        ;; \
    esac \
 && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN if [[ -n "${RUSTUP_DIST_SERVER}" ]]; then export RUSTUP_DIST_SERVER="${RUSTUP_DIST_SERVER}"; fi \
 && if [[ -n "${RUSTUP_UPDATE_ROOT}" ]]; then export RUSTUP_UPDATE_ROOT="${RUSTUP_UPDATE_ROOT}"; fi \
 && curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable \
 && chmod -R a+rwX "${RUSTUP_HOME}" "${CARGO_HOME}" \
 && rustc --version \
 && cargo --version \
 && python3 --version \
 && cmake --version
DOCKERFILE
}

require_command docker
configure_python_selection
HOST_UID=${SUDO_UID:-$(id -u)}
HOST_GID=${SUDO_GID:-$(id -g)}
DEFAULT_UPSTREAM_BUILD_DIR="${CACHE_DIR_IN_CONTAINER}/upstream-build/ubuntu-${UBUNTU_VERSION}-${PYTHON_TAG}/build-wheel-compat"

if [[ "${CACHE_DIR}" != /* ]]; then
  CACHE_DIR="${REPO_ROOT}/${CACHE_DIR}"
fi
mkdir -p "${CACHE_DIR}/cargo" "${CACHE_DIR}/home" "${CACHE_DIR}/upstream-build"
if [[ "$(id -u)" -eq 0 && -n "${SUDO_UID:-}" ]]; then
  chown -R "${HOST_UID}:${HOST_GID}" "${CACHE_DIR}"
fi
configure_cn_mirrors

if is_truthy "${REBUILD_IMAGE}" || ! docker image inspect "${DOCKER_IMAGE}" >/dev/null 2>&1; then
  build_image
fi

declare -a DOCKER_RUN_ARGS=(
  run
  --rm
  --init
  --user "${HOST_UID}:${HOST_GID}"
  -e "HOME=${CACHE_DIR_IN_CONTAINER}/home"
  -e "CARGO_HOME=${CACHE_DIR_IN_CONTAINER}/cargo"
  -e "RUSTUP_HOME=/usr/local/rustup"
  -e "WHEEL_VENV=${CACHE_DIR_IN_CONTAINER}/venv"
  -e "MOONCAKE_UPSTREAM_BUILD_DIR=${DEFAULT_UPSTREAM_BUILD_DIR}"
  -e "PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  -e "CONTAINER_MOUNT_ROOT=${MOUNT_POINT_IN_CONTAINER}"
  -e "CONTAINER_WORKDIR=${WORKDIR_IN_CONTAINER}"
  -v "${MOUNT_ROOT}:${MOUNT_POINT_IN_CONTAINER}"
  -v "${CACHE_DIR}:${CACHE_DIR_IN_CONTAINER}"
  -w "${WORKDIR_IN_CONTAINER}"
)

if [[ -t 0 && -t 1 ]]; then
  DOCKER_RUN_ARGS+=(-it)
fi
if [[ -n "${DOCKER_PLATFORM:-}" ]]; then
  DOCKER_RUN_ARGS+=(--platform "${DOCKER_PLATFORM}")
fi
if [[ -n "${DOCKER_NETWORK:-}" ]]; then
  DOCKER_RUN_ARGS+=(--network "${DOCKER_NETWORK}")
fi

append_env_if_set BUILD_JOBS
append_env_if_set PYTHON
append_env_if_set HTTP_PROXY
append_env_if_set HTTPS_PROXY
append_env_if_set NO_PROXY
append_env_if_set http_proxy
append_env_if_set https_proxy
append_env_if_set no_proxy
append_env_if_set PIP_INDEX_URL
append_env_if_set PIP_EXTRA_INDEX_URL
append_env_if_set PIP_TRUSTED_HOST
append_env_if_set RUSTUP_DIST_SERVER
append_env_if_set RUSTUP_UPDATE_ROOT
append_env_if_set CARGO_REGISTRIES_CRATES_IO_PROTOCOL
append_env_if_set CARGO_NET_GIT_FETCH_WITH_CLI

append_repo_path_env_if_set DIST_DIR
append_repo_path_env_if_set WHEEL_VENV
append_repo_path_env_if_set MOONCAKE_UPSTREAM_DIR
append_repo_path_env_if_set MOONCAKE_UPSTREAM_BUILD_DIR

cat <<EOF
ubuntu: ${UBUNTU_VERSION}
image:  ${DOCKER_IMAGE}
python: ${PYTHON_VERSION}
repo:   ${REPO_ROOT}
cache:  ${CACHE_DIR}
mirror: $(if is_truthy "${CN_MIRROR}"; then printf 'cn'; else printf 'off'; fi)
EOF

docker "${DOCKER_RUN_ARGS[@]}" \
  "${DOCKER_IMAGE}" \
  bash -lc '
set -euo pipefail

configure_git_safe_directories() {
  local root=$1

  git config --global --add safe.directory "${root}"
  git config --global --add safe.directory "${root}/*"
}

mkdir -p "${HOME}" "${CARGO_HOME}"
configure_git_safe_directories "${CONTAINER_MOUNT_ROOT}"
cd "${CONTAINER_WORKDIR}"
exec ./scripts/build/build-wheel.sh "$@"
' bash "$@"
