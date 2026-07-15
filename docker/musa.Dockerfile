# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG TORCH_VERSION=2.9.1.post1
ARG PYTHON_VERSION=3.10
ARG MUSA_VERSION=5.2.0
ARG UBUNTU_VERSION=22.04
ARG BASE_IMAGE=mthreads/pytorch:${TORCH_VERSION}-py${PYTHON_VERSION}-musa${MUSA_VERSION}-devel-ubuntu${UBUNTU_VERSION}-amd64

FROM ${BASE_IMAGE} AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

ARG PYTHON_VERSION
ARG CMAKE_BUILD_TYPE=Release
ARG PYPI_INDEX_URL=https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple
# Empty, unlike the CUDA image: the EP/PG extensions must build against the
# MUSA torch already installed here. Any value makes BuildEpExt.cmake
# pip-install that version from download.pytorch.org, replacing the MUSA torch
# stack with a CUDA build.
ARG EP_TORCH_VERSIONS=""

ENV PYTHON_VERSION=${PYTHON_VERSION} \
    BUILD_WITH_EP=1 \
    EP_TORCH_VERSIONS=${EP_TORCH_VERSIONS} \
    PIP_INDEX_URL=${PYPI_INDEX_URL} \
    PATH="/usr/local/go/bin:${PATH}"

WORKDIR /workspace
COPY . /workspace

# Install Mooncake dependencies (submodules, yalantinglibs, Go, etc.)
RUN bash dependencies.sh -y

# Configure & build Mooncake
RUN mkdir -p build && \
    cd build && \
    cmake -G Ninja .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_MUSA=ON \
        -DWITH_EP=ON \
        -DSTORE_USE_ETCD=ON \
        -DPython3_EXECUTABLE=/usr/bin/python${PYTHON_VERSION} \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    cmake --build . -j"$(nproc)"

# Build nvlink allocator to make wheel self-contained for MUSA paths
RUN mkdir -p build/mooncake-transfer-engine/nvlink-allocator && \
    cd mooncake-transfer-engine/nvlink-allocator && \
    bash build.sh --use-mcc ../../build/mooncake-transfer-engine/nvlink-allocator/

# Build the Python wheel from local sources
RUN OUTPUT_DIR=dist ./scripts/build_wheel.sh

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM ${BASE_IMAGE} AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG PYTHON_VERSION
ARG PYPI_INDEX_URL=https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple

# Install runtime dependencies required by Mooncake
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ibverbs-providers \
        rdma-core \
        libibverbs1 \
        librdmacm1 \
        libnuma1 \
        liburing2 \
        libyaml-0-2 \
        libcurl4 && \
    rm -rf /var/lib/apt/lists/*

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
RUN PIP_INDEX_URL=${PYPI_INDEX_URL} python${PYTHON_VERSION} -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
