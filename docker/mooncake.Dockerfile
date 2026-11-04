# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG CUDA_VERSION=13.0.3
ARG UBUNTU_VERSION=22.04

FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION} AS build-deps

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG PYTHON_VERSION=3.10
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
ARG CMAKE_BUILD_TYPE=Release
ARG TORCH_VERSION=2.14.1
ARG TORCH_CUDA_ARCH_LIST="8.0;9.0;10.3"
# CI can opt in to removing /workspace/build from the builder layer.
ARG CLEAN_BUILD_ARTIFACTS=0
# Bound both CMake/Ninja and the PyTorch JIT build on small CI runners.
ARG BUILD_JOBS=2

ENV PYTHON_VERSION=${PYTHON_VERSION} \
    TORCH_CUDA_ARCH_LIST=${TORCH_CUDA_ARCH_LIST} \
    MAX_JOBS=${BUILD_JOBS} \
    PATH="/usr/local/go/bin:${PATH}"

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

WORKDIR /workspace
# Install system dependencies before copying source so edits reuse this layer;
# submodules are initialized in the source stage after the repository is copied.
COPY dependencies.sh /workspace/dependencies.sh
RUN bash dependencies.sh -y --skip-submodules

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
# Wheels only: the runtime stage installs offline from this directory, and an
# sdist could not be built there without network access.
RUN python${PYTHON_VERSION} -m pip wheel --wheel-dir /opt/python-wheels/build -r /tmp/build-requirements.txt && \
    python${PYTHON_VERSION} -m pip download --only-binary=:all: --dest /opt/python-wheels/runtime -r /tmp/runtime-requirements.txt && \
    cp /tmp/runtime-requirements.txt /opt/python-wheels/runtime-requirements.txt && \
    python${PYTHON_VERSION} -m pip download --only-binary=:all: --dest /opt/python-wheels/torch "torch==${TORCH_VERSION}" && \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/opt/python-wheels/build -r /tmp/build-requirements.txt && \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/opt/python-wheels/torch "torch==${TORCH_VERSION}"

COPY mooncake-common/etcd/go.mod mooncake-common/etcd/go.sum mooncake-common/etcd/*.go /workspace/mooncake-common/etcd/
RUN cd /workspace/mooncake-common/etcd && go mod tidy

FROM build-deps AS source
ENV PIP_NO_INDEX=1 \
    PIP_FIND_LINKS="/opt/python-wheels/build /opt/python-wheels/runtime /opt/python-wheels/torch" \
    SKIP_WHEEL_BUILD_DEPS=1 \
    WHEEL_BUILD_NO_ISOLATION=1
COPY . /workspace
RUN git submodule sync --recursive && git submodule update --init --recursive

FROM source AS builder

# Configure and build the wheel in one layer, then remove build/ only after
# auditwheel has resolved libraries from it. The large intermediate tree is
# therefore not retained in the builder image or BuildKit cache.
RUN mkdir -p build && \
    cd build && \
    cmake -G Ninja .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_CUDA=ON \
        -DWITH_EP=ON \
        -DSTORE_USE_ETCD=ON \
        -DYLT_DOWNLOAD_DIR=/workspace/.cache/yalantinglibs \
        -DPython3_EXECUTABLE=/usr/bin/python${PYTHON_VERSION} \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    export LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LIBRARY_PATH && \
    cmake --build . --parallel "${BUILD_JOBS}" && \
    cd /workspace && \
    export PATH=/usr/local/nvidia/bin:/usr/local/nvidia/lib64:$PATH && \
    export LD_LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LD_LIBRARY_PATH && \
    export LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LIBRARY_PATH && \
    mkdir -p build/mooncake-transfer-engine/nvlink-allocator && \
    cd mooncake-transfer-engine/nvlink-allocator && \
    bash build.sh ../../build/mooncake-transfer-engine/nvlink-allocator/ && \
    cd /workspace && \
    OUTPUT_DIR=dist ./scripts/build_wheel.sh && \
    python${PYTHON_VERSION} -m pip install mooncake-wheel/dist/*.whl && \
    ln -sf /usr/local/cuda/lib64/stubs/libcuda.so /usr/local/cuda/lib64/stubs/libcuda.so.1 && \
    python${PYTHON_VERSION} -m mooncake.pg --prebuild && \
    if [ "${CLEAN_BUILD_ARTIFACTS}" = "1" ]; then \
        rm -rf build; \
    fi

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu${UBUNTU_VERSION} AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Inherit build-args so the runtime stage installs the matching interpreter
ARG PYTHON_VERSION=3.10
ARG TORCH_VERSION=2.14.1
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
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

# Install the prefetched runtime dependencies without retaining wheel archives.
RUN --mount=type=bind,from=build-deps,source=/opt/python-wheels,target=/tmp/python-wheels \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/tmp/python-wheels/torch "torch==${TORCH_VERSION}" && \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/tmp/python-wheels/runtime -r /tmp/python-wheels/runtime-requirements.txt

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
COPY --from=builder /root/.cache/mooncake/pg_jit /root/.cache/mooncake/pg_jit
COPY --chmod=755 scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN python${PYTHON_VERSION} -m pip install --no-index --no-deps /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
