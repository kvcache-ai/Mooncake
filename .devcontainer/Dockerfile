# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Modified by Hygon Information Technology Co., Ltd., 2026.

FROM ubuntu:22.04

WORKDIR /workspaces

RUN apt-get update -y \
    && apt-get install -y vim git wget build-essential cmake net-tools tcpdump

RUN apt-get install -y libibverbs-dev \
    libunwind-dev \
    libgoogle-glog-dev \
    libjsoncpp-dev \
    libnuma-dev \
    libpython3-dev \
    libboost-dev \
    libssl-dev \
    libgrpc-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    pybind11-dev \
    libcurl4-openssl-dev \
    libhiredis-dev \
    libyaml-cpp-dev \
    libjemalloc-dev \
    libzstd-dev \
    libmsgpack-dev \
    libgflags-dev \
    libxxhash-dev \
    pkg-config \
    patchelf

COPY install-go.sh /tmp/install-go.sh
RUN bash /tmp/install-go.sh && rm -f /tmp/install-go.sh

RUN git clone https://github.com/alibaba/yalantinglibs.git \
    && cd yalantinglibs \
    && mkdir -p build \
    && cd build \
    && cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF \
    && cmake --build . -j$(nproc) \
    && cmake --install .

ENV GOPROXY='https://goproxy.cn'
ENV PATH=/usr/local/go/bin:$PATH

CMD ["bash"]
