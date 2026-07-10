# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG UBUNTU_VERSION=22.04

FROM ubuntu:${UBUNTU_VERSION} AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

ARG PYTHON_VERSION=3.10
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
ARG CMAKE_BUILD_TYPE=Release

ENV PYTHON_VERSION=${PYTHON_VERSION} \
    PATH="/usr/local/go/bin:${PATH}" \
    GOPROXY="https://goproxy.cn,https://goproxy.io,direct"

# Install base build utilities and Python (Ubuntu 22.04 has Python 3.10 built-in)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        ninja-build \
        software-properties-common \
        pkg-config \
        build-essential \
        cmake \
        liburing-dev \
        libnuma-dev \
        libzstd-dev \
        libxxhash-dev \
        libcurl4-openssl-dev \
        libyaml-cpp-dev \
        libssl-dev \
        uuid-dev \
        libre2-dev \
        python3 \
        python3-dev \
        python3-venv \
        python3-pip && \
    update-alternatives --install /usr/bin/python  python  /usr/bin/python3 1 && \
    apt-get purge -y --auto-remove software-properties-common && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

# Install Mooncake dependencies (yalantinglibs, Go, etc.)
RUN bash dependencies.sh -y

# Configure & build Mooncake (no CUDA, no RDMA)
# Build Go K8s lease wrapper first to generate the header, then build everything else
RUN mkdir -p build && \
    cd build && \
    cmake -G Ninja .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_CUDA=OFF \
        -DWITH_EP=OFF \
        -DSTORE_USE_K8S_LEASE=ON \
        -DPython3_EXECUTABLE=/usr/bin/python3 \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    cmake --build . --parallel 2 --target build_k8s_lease_wrapper && \
    cmake --build . --parallel 2

# Build the Python wheel from local sources
RUN OUTPUT_DIR=dist ./scripts/build_wheel.sh

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM ubuntu:${UBUNTU_VERSION} AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Inherit build-args so the runtime stage installs the matching interpreter
ARG PYTHON_VERSION=3.10
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Install runtime dependencies (Ubuntu 22.04 has Python 3.10 built-in)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libnuma1 \
        liburing2 \
        libyaml-0-2 \
        libcurl4 \
        libibverbs1 \
        python3 \
        python3-pip && \
    apt-get purge -y --auto-remove curl && \
    rm -rf /var/lib/apt/lists/*

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
COPY scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN chmod 755 /usr/local/bin/mooncake-hicache-sizing
RUN python3 -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
