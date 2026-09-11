# syntax=docker/dockerfile:1.7

###############################################################################
# Mooncake Intel XPU (oneAPI SYCL) development / build image.
#
# USE_XPU is a native, direct-link SYCL build: the XPU platform sources include
# <sycl/sycl.hpp> and link libsycl directly, so the whole Transfer Engine build
# must use the Intel DPC++ compiler (icpx).
#
# Base image: intel/pytorch:xpu. It already ships the Intel GPU runtime
# (level-zero / libze_loader, the OpenCL ICD, and the graphics compute runtime)
# plus a torch build with XPU support, but -- like vLLM's Dockerfile.xpu, which
# only consumes prebuilt torch-xpu wheels -- it does NOT include a SYCL
# compiler. We add one via Intel's oneAPI apt repo
# (intel-oneapi-compiler-dpcpp-cpp -> icpx). The result is a single image that
# can BUILD the native XPU platform *and* run it against a real Intel GPU (or
# fall back to the OpenCL CPU device for tent_xpu_platform_test on a GPU-less
# host), with torch available for XPU integration work.
#
# Build:
#   docker build -f docker/xpu.Dockerfile -t mooncake-xpu:dev .
# Run the XPU platform test inside the image:
#   docker run --rm --device /dev/dri mooncake-xpu:dev \
#     bash -lc '. /opt/intel/oneapi/setvars.sh >/dev/null && \
#               ctest --test-dir build-xpu -R tent_xpu_platform_test --output-on-failure'
###############################################################################

ARG BASE_IMAGE=intel/pytorch:xpu-2.13.0-ubuntu24.04-20260907

FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/usr/local/go/bin:${PATH}"

ARG CMAKE_BUILD_TYPE=RelWithDebInfo
# Pin the DPC++ compiler version to keep image builds reproducible.
ARG DPCPP_VERSION=2026.1

# Add Intel's oneAPI apt repository (the base image only carries the GPU
# *runtime* repo, intel-gpu-*, not the compiler), then install the DPC++
# compiler (icpx) plus the build utilities Mooncake needs. The base image
# provides cmake but not ninja/git/RDMA dev headers.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl gpg && \
    curl -fsSL https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
        | gpg --dearmor -o /usr/share/keyrings/oneapi-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" \
        > /etc/apt/sources.list.d/oneAPI.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        intel-oneapi-compiler-dpcpp-cpp-${DPCPP_VERSION} \
        build-essential \
        cmake \
        git \
        ninja-build \
        pkg-config \
        libibverbs-dev \
        librdmacm-dev \
        libnuma-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

# Install Mooncake dependencies (submodules, Go, system libs, etc.).
RUN bash dependencies.sh -y

# Configure and build the Transfer Engine with the native XPU platform. icpx is
# selected as the C/C++ compiler because USE_XPU links SYCL directly;
# common.cmake enforces this and fails fast otherwise. Store/P2P components are
# disabled to keep the DPC++ build focused on the XPU transfer path.
RUN . /opt/intel/oneapi/setvars.sh >/dev/null && \
    cmake -G Ninja -S . -B build-xpu \
        -DCMAKE_C_COMPILER=icx \
        -DCMAKE_CXX_COMPILER=icpx \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
        -DUSE_TENT=ON \
        -DUSE_XPU=ON \
        -DBUILD_UNIT_TESTS=ON \
        -DWITH_STORE=OFF \
        -DWITH_STORE_RUST=OFF \
        -DWITH_P2P_STORE=OFF && \
    cmake --build build-xpu --target tent_xpu_platform_test -j "$(nproc)"

CMD ["bash"]
