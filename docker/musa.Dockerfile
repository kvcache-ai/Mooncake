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
        python3-distutils \
        python3-venv \
        python3-pip \
        python3-setuptools \
        python-is-python3 \
        pkg-config && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip \
    && pip install build wheel setuptools auditwheel

WORKDIR /workspace
COPY . /workspace

# Ensure submodules are available when .git is present (skips quietly otherwise)
RUN if [ -d .git ]; then git submodule update --init --recursive; fi

# Install Mooncake dependencies (yalantinglibs, Go, etc.)
RUN bash -x dependencies.sh -y

# Configure & build Mooncake
RUN mkdir -p build && \
    cd build && \
    cmake .. \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_MUSA=ON \
        -DSTORE_USE_ETCD=ON \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    cmake --build . -j"$(nproc)" && \
    cmake --install .

# Build nvlink allocator to make wheel self-contained for MUSA paths
RUN cd mooncake-transfer-engine/nvlink-allocator && \
    bash build.sh --use-mcc ../../build/mooncake-transfer-engine/nvlink-allocator/

# Build the Python wheel from local sources
ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}
RUN OUTPUT_DIR=dist ./scripts/build_wheel.sh

###############################################################################
# Stage 2: install the freshly built wheel into a runtime image
###############################################################################
FROM mthreads/musa:${MUSA_VERSION}-devel-ubuntu${UBUNTU_VERSION}-amd64 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# Install runtime dependencies required by Mooncake
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        netcat \
        curl \
        python3 \
        python3-venv \
        python3-pip \
        python-is-python3 \
        infiniband-diags \
        ibverbs-providers \
        rdma-core \
        libibverbs1 \
        libibverbs-dev \
        ibverbs-utils \
        librdmacm1 \
        libnuma1 \
        liburing2 \
        libyaml-0-2 && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
RUN pip install --upgrade pip setuptools wheel

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
RUN pip install /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel

WORKDIR /workspace
CMD ["/bin/bash"]
