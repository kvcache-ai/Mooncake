FROM ubuntu:22.04

WORKDIR /workspaces

RUN apt-get update -y \
    && apt-get install -y \
        vim git wget build-essential python3-venv net-tools tcpdump

ENV MOONCAKE_TOOLCHAIN=/opt/mooncake-toolchain

RUN python3 -m venv ${MOONCAKE_TOOLCHAIN}
RUN ${MOONCAKE_TOOLCHAIN}/bin/python3 -m pip install --upgrade pip cmake ninja

ARG BUILDARCH
ADD https://go.dev/dl/go1.27.4.linux-$BUILDARCH.tar.gz
RUN ln -s ../go/bin/go ${MOONCAKE_TOOLCHAIN}/bin/go
RUN ln -s ../go/bin/gofmt ${MOONCAKE_TOOLCHAIN}/bin/gofmt

ENV PATH=${MOONCAKE_TOOLCHAIN}/bin:$PATH

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

CMD ["bash"]
