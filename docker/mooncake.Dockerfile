# syntax=docker/dockerfile:1.7

###############################################################################
# Stage 1: build Mooncake from source and produce a Python wheel
###############################################################################
ARG CUDA_VERSION=13.0.3
ARG UBUNTU_VERSION=22.04
ARG TORCH_VERSION=2.14.1

FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION} AS system-deps

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG PYTHON_VERSION=3.10
ARG PYPA_INDEX_URL=https://bootstrap.pypa.io
ENV PYTHON_VERSION=${PYTHON_VERSION} \
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

FROM system-deps AS torch-wheels
ARG TORCH_VERSION
RUN python${PYTHON_VERSION} -m pip download --only-binary=:all: \
    --dest /opt/torch-wheels "torch==${TORCH_VERSION}"

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
# Resolve Torch and Mooncake runtime requirements together. Seed the download
# directory with links so pip can reuse Torch wheels without copying them.
RUN --mount=type=bind,from=torch-wheels,source=/opt/torch-wheels,target=/tmp/torch-wheels \
    python${PYTHON_VERSION} -m pip wheel --wheel-dir /opt/python-wheels/build -r /tmp/build-requirements.txt && \
    mkdir -p /opt/python-wheels/runtime && \
    ln -s /tmp/torch-wheels/*.whl /opt/python-wheels/runtime/ && \
    python${PYTHON_VERSION} -m pip download --only-binary=:all: --find-links=/tmp/torch-wheels \
        --dest /opt/python-wheels/runtime -r /tmp/runtime-requirements.txt /tmp/torch-wheels/torch-*.whl && \
    find /opt/python-wheels/runtime -type l -delete && \
    cp /tmp/build-requirements.txt /tmp/runtime-requirements.txt /opt/python-wheels/

FROM system-deps AS go-deps
ENV GOMODCACHE=/opt/go-mod-cache
COPY mooncake-common/etcd/go.mod mooncake-common/etcd/go.sum /tmp/etcd/
RUN cd /tmp/etcd && go mod download

FROM system-deps AS ylt-deps
ARG GH_MIRROR
COPY mooncake-common/FindYLT.cmake /tmp/FindYLT.cmake
RUN cmake -DGH_MIRROR="${GH_MIRROR}" -DYLT_ARCHIVE=/opt/yalantinglibs.tar.gz -P /tmp/FindYLT.cmake

FROM system-deps AS source
RUN --mount=type=bind,from=torch-wheels,source=/opt/torch-wheels,target=/tmp/torch-wheels \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/tmp/torch-wheels /tmp/torch-wheels/torch-*.whl
ENV GOMODCACHE=/opt/go-mod-cache
COPY --from=go-deps /opt/go-mod-cache /opt/go-mod-cache
COPY --from=ylt-deps /opt/yalantinglibs.tar.gz /opt/yalantinglibs.tar.gz
COPY . /workspace
RUN git submodule sync --recursive && git submodule update --init --recursive

FROM source AS builder

ARG TORCH_VERSION
ARG CMAKE_BUILD_TYPE=Release
ARG TORCH_CUDA_ARCH_LIST="8.0;9.0;10.3"
ARG CLEAN_BUILD_ARTIFACTS=0
ARG BUILD_JOBS=2
ENV TORCH_CUDA_ARCH_LIST=${TORCH_CUDA_ARCH_LIST} \
    MAX_JOBS=${BUILD_JOBS} \
    GOPROXY=off \
    PIP_NO_INDEX=1 \
    SKIP_WHEEL_BUILD_DEPS=1 \
    WHEEL_BUILD_NO_ISOLATION=1

# Configure and build the wheel in one layer, then remove build/ only after
# auditwheel has resolved libraries from it. The large intermediate tree is
# not retained in the builder layer when CLEAN_BUILD_ARTIFACTS=1.
RUN --mount=type=bind,from=torch-wheels,source=/opt/torch-wheels,target=/tmp/torch-wheels \
    --mount=type=bind,from=python-wheels,source=/opt/python-wheels,target=/tmp/python-wheels \
    python${PYTHON_VERSION} -m pip install --find-links=/tmp/python-wheels/build -r /tmp/python-wheels/build-requirements.txt && \
    export PIP_FIND_LINKS="/tmp/python-wheels/runtime /tmp/torch-wheels" && \
    python${PYTHON_VERSION} -m pip install -r /tmp/python-wheels/runtime-requirements.txt "torch==${TORCH_VERSION}" && \
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
        -DYLT_ARCHIVE=/opt/yalantinglibs.tar.gz \
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

# Keep the Torch installation cached across Mooncake metadata changes.
RUN --mount=type=bind,from=torch-wheels,source=/opt/torch-wheels,target=/tmp/torch-wheels \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/tmp/torch-wheels /tmp/torch-wheels/torch-*.whl

ARG TORCH_VERSION
RUN --mount=type=bind,from=torch-wheels,source=/opt/torch-wheels,target=/tmp/torch-wheels \
    --mount=type=bind,from=python-wheels,source=/opt/python-wheels,target=/tmp/python-wheels \
    python${PYTHON_VERSION} -m pip install --no-index --find-links=/tmp/torch-wheels \
        --find-links=/tmp/python-wheels/runtime -r /tmp/python-wheels/runtime-requirements.txt "torch==${TORCH_VERSION}"

# Copy wheels produced in builder stage and install them via pip
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
COPY --from=builder /root/.cache/mooncake/pg_jit /root/.cache/mooncake/pg_jit
COPY --chmod=755 scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN python${PYTHON_VERSION} -m pip install --no-index --no-deps /tmp/mooncake-wheel/*.whl && rm -rf /tmp/mooncake-wheel /root/.cache/pip

CMD ["/bin/bash"]
