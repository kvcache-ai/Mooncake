# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG MUSA_VERSION=rc4.3.0
ARG UBUNTU_VERSION=22.04

FROM mthreads/musa:${MUSA_VERSION}-devel-ubuntu${UBUNTU_VERSION}-amd64 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

ARG PYTHON_VERSION=3.10
ARG CMAKE_BUILD_TYPE=Release

ENV PYTHON_VERSION=${PYTHON_VERSION} \
    PATH="/usr/local/go/bin:${PATH}"

# Install base build utilities and python bindings
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        python3 \
        python3-dev \
        python3-pip \
        python-is-python3 \
        pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

# Install Mooncake dependencies (yalantinglibs, Go, etc.)
RUN bash dependencies.sh -y

# Configure & build Mooncake
RUN mkdir -p build && \
    cd build && \
    cmake .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_MUSA=ON \
        -DSTORE_USE_ETCD=ON \
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
FROM mthreads/musa:${MUSA_VERSION}-devel-ubuntu${UBUNTU_VERSION}-amd64 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install runtime dependencies required by Mooncake
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ibverbs-providers \
        rdma-core \
        libibverbs1 \
        librdmacm1 \
        libnuma1 \
        liburing2 \
        libyaml-0-2 && \
    rm -rf /var/lib/apt/lists/*

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
RUN python3 -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
