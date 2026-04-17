#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/build/build-wheel-ubuntu-docker.sh
#
# Build the Python wheels inside a pinned Ubuntu Docker image while reusing
# scripts/build/build-wheel.sh as the single packaging implementation.
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)

UBUNTU_VERSION=${UBUNTU_VERSION:-22.04}
DOCKER_IMAGE=${DOCKER_IMAGE:-"mooncake-store-wheel:ubuntu-${UBUNTU_VERSION}"}
WORKDIR_IN_CONTAINER=${WORKDIR_IN_CONTAINER:-/work}
CACHE_DIR=${MOONCAKE_DOCKER_CACHE_DIR:-"${REPO_ROOT}/target/docker-wheel-cache"}
CACHE_DIR_IN_CONTAINER=${CACHE_DIR_IN_CONTAINER:-/cache}
REBUILD_IMAGE=${REBUILD_IMAGE:-0}
PULL_IMAGE=${PULL_IMAGE:-1}

usage() {
  cat <<'EOF'
Usage: scripts/build/build-wheel-ubuntu-docker.sh [maturin build args...]

Build the Mooncake Python wheels inside an Ubuntu Docker image and write the
same artifacts as scripts/build/build-wheel.sh:

  dist/wheels/mooncake-*.whl
  dist/wheels/mooncake_pro-*.whl
  dist/bin/mooncake-store-client
  dist/bin/mooncake-store-admin

Environment:
  UBUNTU_VERSION              Ubuntu base image tag (default: 22.04)
  DOCKER_IMAGE                Builder image name (default: mooncake-store-wheel:ubuntu-${UBUNTU_VERSION})
  DOCKER_PLATFORM             Optional Docker platform, e.g. linux/amd64
  DOCKER_NETWORK              Optional network for docker run, e.g. host
  DOCKER_BUILD_NETWORK        Optional network for docker build
  REBUILD_IMAGE=1             Rebuild the builder image even if it exists
  PULL_IMAGE=0                Do not pull the Ubuntu base image while rebuilding
  MOONCAKE_DOCKER_CACHE_DIR   Cache for container HOME/CARGO_HOME (default: target/docker-wheel-cache)
  CACHE_DIR_IN_CONTAINER      Container mountpoint for the cache (default: /cache)

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

map_repo_path() {
  local name=$1
  local value=$2
  local relative

  if [[ "${value}" != /* ]]; then
    printf '%s\n' "${value}"
    return 0
  fi

  if [[ "${value}" == "${REPO_ROOT}" ]]; then
    printf '%s\n' "${WORKDIR_IN_CONTAINER}"
    return 0
  fi

  if [[ "${value}" == "${REPO_ROOT}/"* ]]; then
    relative=${value#"${REPO_ROOT}/"}
    printf '%s/%s\n' "${WORKDIR_IN_CONTAINER}" "${relative}"
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
FROM ubuntu:${UBUNTU_VERSION}

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
    unzip \
    wget \
    zlib1g-dev \
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

if [[ "${CACHE_DIR}" != /* ]]; then
  CACHE_DIR="${REPO_ROOT}/${CACHE_DIR}"
fi
mkdir -p "${CACHE_DIR}/cargo" "${CACHE_DIR}/home"

if is_truthy "${REBUILD_IMAGE}" || ! docker image inspect "${DOCKER_IMAGE}" >/dev/null 2>&1; then
  build_image
fi

declare -a DOCKER_RUN_ARGS=(
  run
  --rm
  --init
  --user "$(id -u):$(id -g)"
  -e "HOME=${CACHE_DIR_IN_CONTAINER}/home"
  -e "CARGO_HOME=${CACHE_DIR_IN_CONTAINER}/cargo"
  -e "RUSTUP_HOME=/usr/local/rustup"
  -e "WHEEL_VENV=${CACHE_DIR_IN_CONTAINER}/venv"
  -e "PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  -e "CONTAINER_WORKDIR=${WORKDIR_IN_CONTAINER}"
  -v "${REPO_ROOT}:${WORKDIR_IN_CONTAINER}"
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
repo:   ${REPO_ROOT}
cache:  ${CACHE_DIR}
EOF

docker "${DOCKER_RUN_ARGS[@]}" \
  "${DOCKER_IMAGE}" \
  bash -lc '
set -euo pipefail
mkdir -p "${HOME}" "${CARGO_HOME}"
git config --global --add safe.directory "${CONTAINER_WORKDIR}"
cd "${CONTAINER_WORKDIR}"
exec ./scripts/build/build-wheel.sh "$@"
' bash "$@"
