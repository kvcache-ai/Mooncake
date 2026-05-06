# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG CUDA_VERSION=12.8.1
ARG UBUNTU_VERSION=22.04

FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION} AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

ARG PYTHON_VERSION=3.10
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
ARG CMAKE_BUILD_TYPE=Release
ARG EP_TORCH_VERSIONS="2.9.1"
ARG TORCH_CUDA_ARCH_LIST="8.0;9.0"

ENV PYTHON_VERSION=${PYTHON_VERSION} \
    BUILD_WITH_EP=1 \
    EP_TORCH_VERSIONS=${EP_TORCH_VERSIONS} \
    TORCH_CUDA_ARCH_LIST=${TORCH_CUDA_ARCH_LIST} \
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
COPY . /workspace

# Install Mooncake dependencies (yalantinglibs, Go, etc.)
RUN bash dependencies.sh -y

# Configure & build Mooncake
RUN mkdir -p build && \
    cd build && \
    cmake -G Ninja .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_CUDA=ON \
        -DWITH_EP=ON \
        -DSTORE_USE_ETCD=ON \
        -DPython3_EXECUTABLE=/usr/bin/python${PYTHON_VERSION} \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    export LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LIBRARY_PATH && \
    cmake --build .

# Build nvlink allocator to make wheel self-contained for CUDA paths
RUN export PATH=/usr/local/nvidia/bin:/usr/local/nvidia/lib64:$PATH && \
    export LD_LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LD_LIBRARY_PATH && \
    export LIBRARY_PATH=/usr/local/cuda/lib64/stubs:$LIBRARY_PATH && \
    mkdir -p build/mooncake-transfer-engine/nvlink-allocator && \
    cd mooncake-transfer-engine/nvlink-allocator && \
    bash build.sh ../../build/mooncake-transfer-engine/nvlink-allocator/

# Build the Python wheel from local sources
RUN OUTPUT_DIR=dist ./scripts/build_wheel.sh

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu${UBUNTU_VERSION} AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Inherit build-args so the runtime stage installs the matching interpreter
ARG PYTHON_VERSION=3.10
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

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
COPY --chmod=755 scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN python${PYTHON_VERSION} -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
