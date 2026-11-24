# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG CUDA_VERSION=13.0.3
ARG UBUNTU_VERSION=22.04
ARG PYTHON_VERSION=3.10
ARG TORCH_VERSION=2.14.1
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
ARG MOONCAKE_GO_INSTALL_PREFIX=/usr/local
ARG MOONCAKE_WORK_DIR=/workspace
ARG MOONCAKE_DEPS_DIR=/opt/mooncake-deps
ARG TORCH_WHEEL_DIR=${MOONCAKE_DEPS_DIR}/torch-wheels
ARG PYTHON_WHEEL_DIR=${MOONCAKE_DEPS_DIR}/python-wheels

FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION} AS system-deps

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG PYTHON_VERSION
ARG PYPA_INDEX_URL
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Install base build utilities and the requested Python version via deadsnakes PPA
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        ninja-build \
        software-properties-common \
        pkg-config && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-dev \
        python${PYTHON_VERSION}-venv && \
    curl -sS ${PYPA_INDEX_URL}/get-pip.py | python${PYTHON_VERSION} && \
    update-alternatives --install /usr/bin/python  python  /usr/bin/python${PYTHON_VERSION} 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1 && \
    apt-get purge -y --auto-remove software-properties-common && \
    rm -rf /var/lib/apt/lists/*

ARG MOONCAKE_WORK_DIR
WORKDIR ${MOONCAKE_WORK_DIR}
ARG MOONCAKE_GO_INSTALL_PREFIX
ENV PATH="${MOONCAKE_GO_INSTALL_PREFIX}/go/bin:${PATH}"
# Install system dependencies independently of the source and submodule.
COPY dependencies.sh ./
RUN bash dependencies.sh -y --skip-submodules

ARG MOONCAKE_DEPS_DIR
ARG TORCH_WHEEL_DIR
ARG PYTHON_WHEEL_DIR
ENV GOMODCACHE=${MOONCAKE_DEPS_DIR}/go-mod-cache \
    YLT_ARCHIVE=${MOONCAKE_DEPS_DIR}/yalantinglibs.tar.gz \
    PYBIND11_DIR=${MOONCAKE_DEPS_DIR}/pybind11

FROM system-deps AS torch-wheels
ARG TORCH_VERSION
RUN python${PYTHON_VERSION} -m pip download --only-binary=:all: \
    --dest "$TORCH_WHEEL_DIR" "torch==${TORCH_VERSION}"

FROM system-deps AS python-wheels
COPY mooncake-wheel/pyproject.toml /tmp/mooncake-pyproject.toml
RUN python${PYTHON_VERSION} -m pip install --upgrade pip tomli && \
    python${PYTHON_VERSION} - <<'PYTHON'
import tomli
from pathlib import Path

with open("/tmp/mooncake-pyproject.toml", "rb") as manifest:
    metadata = tomli.load(manifest)
Path("/tmp/build-requirements.txt").write_text(
    "\n".join(metadata["build-system"]["requires"] + ["build", "auditwheel", "patchelf"])
)
Path("/tmp/runtime-requirements.txt").write_text("\n".join(metadata["project"]["dependencies"]))
PYTHON
COPY scripts/conductor-wheel-api-requirements.txt scripts/conductor-wheel-e2e-requirements.txt /tmp/
# Resolve Torch and Mooncake runtime requirements together. Seed the download
# directory with links so pip can reuse Torch wheels without copying them.
RUN --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    python${PYTHON_VERSION} -m pip wheel --wheel-dir "$PYTHON_WHEEL_DIR/build" \
        -r /tmp/build-requirements.txt -r /tmp/conductor-wheel-e2e-requirements.txt && \
    mkdir -p "$PYTHON_WHEEL_DIR/runtime" && \
    ln -s "$TORCH_WHEEL_DIR"/*.whl "$PYTHON_WHEEL_DIR/runtime/" && \
    python${PYTHON_VERSION} -m pip download --only-binary=:all: --find-links="$TORCH_WHEEL_DIR" \
        --dest "$PYTHON_WHEEL_DIR/runtime" -r /tmp/runtime-requirements.txt "$TORCH_WHEEL_DIR"/torch-*.whl && \
    find "$PYTHON_WHEEL_DIR/runtime" -type l -delete && \
    cp /tmp/build-requirements.txt /tmp/runtime-requirements.txt "$PYTHON_WHEEL_DIR/"

FROM system-deps AS go-deps
COPY mooncake-common/etcd/go.mod mooncake-common/etcd/go.sum /tmp/etcd/
RUN cd /tmp/etcd && go mod download

FROM system-deps AS ylt-deps
ARG GH_MIRROR
COPY mooncake-common/FindYLT.cmake /tmp/FindYLT.cmake
RUN mkdir -p "$MOONCAKE_DEPS_DIR" && \
    cmake -DGH_MIRROR="${GH_MIRROR}" -DYLT_ARCHIVE="$YLT_ARCHIVE" -P /tmp/FindYLT.cmake

# Key the download on the submodule URL and gitlink, independently of the
# surrounding Git index and history.
FROM system-deps AS pybind11-pin
COPY .gitmodules /tmp/.gitmodules
RUN --mount=type=bind,source=.git,target=${MOONCAKE_WORK_DIR}/.git \
    git config -f /tmp/.gitmodules --get submodule.extern/pybind11.url > /tmp/pybind11-url && \
    git ls-files --stage -- extern/pybind11 | \
    awk '$1 == "160000" && $3 == 0 { print $2; found = 1 } END { exit !found }' > /tmp/pybind11-revision

FROM system-deps AS pybind11-deps
COPY --from=pybind11-pin /tmp/pybind11-url /tmp/pybind11-revision /tmp/
RUN git init -q "$PYBIND11_DIR" && \
    cd "$PYBIND11_DIR" && \
    git remote add origin "$(cat /tmp/pybind11-url)" && \
    git fetch --depth=1 origin "$(cat /tmp/pybind11-revision)" && \
    git checkout --detach FETCH_HEAD && \
    git submodule update --init --recursive

FROM system-deps AS source
RUN --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    python${PYTHON_VERSION} -m pip install --no-index --find-links="$TORCH_WHEEL_DIR" "$TORCH_WHEEL_DIR"/torch-*.whl
ARG TORCH_VERSION
RUN --network=none --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    --mount=type=bind,from=python-wheels,source=${PYTHON_WHEEL_DIR},target=${PYTHON_WHEEL_DIR} \
    python${PYTHON_VERSION} -m pip install --no-index --find-links="$PYTHON_WHEEL_DIR/build" \
        -r "$PYTHON_WHEEL_DIR/build-requirements.txt" && \
    python${PYTHON_VERSION} -m pip install --no-index --find-links="$PYTHON_WHEEL_DIR/runtime" \
        --find-links="$TORCH_WHEEL_DIR" -r "$PYTHON_WHEEL_DIR/runtime-requirements.txt" "torch==${TORCH_VERSION}"
COPY --from=go-deps ${GOMODCACHE} ${GOMODCACHE}
COPY --from=ylt-deps ${YLT_ARCHIVE} ${YLT_ARCHIVE}
COPY --from=pybind11-deps ${PYBIND11_DIR} ${PYBIND11_DIR}
COPY . .
RUN --network=none mkdir -p extern && rm -rf extern/pybind11 && mv "$PYBIND11_DIR" extern/pybind11

FROM source AS builder

ARG CMAKE_BUILD_TYPE=Release
ARG ENABLE_DEBUG_SYMBOLS=ON
ARG TORCH_CUDA_ARCH_LIST="8.0;9.0;10.3"
ARG CLEAN_BUILD_ARTIFACTS=0
ARG BUILD_JOBS=2
ENV TORCH_CUDA_ARCH_LIST=${TORCH_CUDA_ARCH_LIST} \
    MAX_JOBS=${BUILD_JOBS} \
    GOPROXY=off \
    PIP_NO_INDEX=1 \
    SKIP_WHEEL_BUILD_DEPS=1 \
    WHEEL_BUILD_NO_ISOLATION=1

# Build and package in one layer so deleted intermediates never enter a snapshot.
# FREE_BUILD_DIR lets the wheel packager remove the native tree after staging
# its inputs, before creating and repairing the wheel.
# The wheel smoke tests connect to a local server without a proxy.
RUN --network=none --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    --mount=type=bind,from=python-wheels,source=${PYTHON_WHEEL_DIR},target=${PYTHON_WHEEL_DIR} \
    unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY && \
    CUDA_STUB_DIR="${CUDA_HOME:-/usr/local/cuda}/lib64/stubs" && \
    export PIP_FIND_LINKS="${PYTHON_WHEEL_DIR}/build ${PYTHON_WHEEL_DIR}/runtime ${TORCH_WHEEL_DIR}" && \
    mkdir -p build && \
    cd build && \
    cmake -G Ninja .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_CUDA=ON \
        -DWITH_EP=ON \
        -DWITH_CONDUCTOR=ON \
        -DSTORE_USE_ETCD=ON \
        -DYLT_ARCHIVE="$YLT_ARCHIVE" \
        -DPython3_EXECUTABLE=/usr/bin/python${PYTHON_VERSION} \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_DEBUG_SYMBOLS="${ENABLE_DEBUG_SYMBOLS}" && \
    export LIBRARY_PATH="$CUDA_STUB_DIR:$LIBRARY_PATH" && \
    cmake --build . --parallel "${BUILD_JOBS}" && \
    cd "$MOONCAKE_WORK_DIR" && \
    export PATH=/usr/local/nvidia/bin:/usr/local/nvidia/lib64:$PATH && \
    export LD_LIBRARY_PATH="$CUDA_STUB_DIR:$LD_LIBRARY_PATH" && \
    export LIBRARY_PATH="$CUDA_STUB_DIR:$LIBRARY_PATH" && \
    mkdir -p build/mooncake-transfer-engine/nvlink-allocator && \
    cd mooncake-transfer-engine/nvlink-allocator && \
    bash build.sh ../../build/mooncake-transfer-engine/nvlink-allocator/ && \
    cd "$MOONCAKE_WORK_DIR" && \
    if [ "${CLEAN_BUILD_ARTIFACTS}" = "1" ]; then go clean -cache; fi && \
    FREE_BUILD_DIR="${CLEAN_BUILD_ARTIFACTS}" OUTPUT_DIR=dist ./scripts/build_wheel.sh && \
    if [ "${CLEAN_BUILD_ARTIFACTS}" = "1" ]; then rm -rf mooncake-wheel/build; fi && \
    python${PYTHON_VERSION} -m pip install mooncake-wheel/dist/*.whl && \
    ln -sf "$CUDA_STUB_DIR/libcuda.so" "$CUDA_STUB_DIR/libcuda.so.1" && \
    python${PYTHON_VERSION} -m mooncake.pg --prebuild

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu${UBUNTU_VERSION} AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Use the same Python and pip bootstrap defaults as the builder.
ARG PYTHON_VERSION
ARG PYPA_INDEX_URL
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Install runtime dependencies and the requested Python version
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        software-properties-common \
        ibverbs-providers \
        rdma-core \
        libibverbs1 \
        librdmacm1 \
        libnuma1 \
        liburing2 \
        libyaml-0-2 \
        libcurl4 && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python${PYTHON_VERSION} && \
    curl -sS ${PYPA_INDEX_URL}/get-pip.py | python${PYTHON_VERSION} && \
    update-alternatives --install /usr/bin/python  python  /usr/bin/python${PYTHON_VERSION} 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1 && \
    apt-get purge -y --auto-remove software-properties-common curl && \
    rm -rf /var/lib/apt/lists/*

ARG TORCH_WHEEL_DIR
ARG PYTHON_WHEEL_DIR

# Keep the Torch installation cached across Mooncake metadata changes.
RUN --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    python${PYTHON_VERSION} -m pip install --no-index --find-links="$TORCH_WHEEL_DIR" "$TORCH_WHEEL_DIR"/torch-*.whl

ARG TORCH_VERSION
RUN --mount=type=bind,from=torch-wheels,source=${TORCH_WHEEL_DIR},target=${TORCH_WHEEL_DIR} \
    --mount=type=bind,from=python-wheels,source=${PYTHON_WHEEL_DIR},target=${PYTHON_WHEEL_DIR} \
    python${PYTHON_VERSION} -m pip install --no-index --find-links="$TORCH_WHEEL_DIR" \
        --find-links="$PYTHON_WHEEL_DIR/runtime" -r "$PYTHON_WHEEL_DIR/runtime-requirements.txt" "torch==${TORCH_VERSION}"

# Copy wheels produced in builder stage and install them via pip
ARG MOONCAKE_WORK_DIR
COPY --from=builder ${MOONCAKE_WORK_DIR}/mooncake-wheel/dist /tmp/mooncake-wheel
COPY --from=builder /root/.cache/mooncake/pg_jit /root/.cache/mooncake/pg_jit
COPY --chmod=755 scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN python${PYTHON_VERSION} -m pip install --no-index --no-deps /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
