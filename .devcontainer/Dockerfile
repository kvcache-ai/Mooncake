FROM ubuntu:22.04

WORKDIR /workspaces

RUN apt-get update -y \
    && apt-get install -y vim git wget build-essential cmake net-tools tcpdump

RUN apt-get install -y libibverbs-dev \
    libunwind-dev \
    libgoogle-glog-dev \
    libgtest-dev \
    libjsoncpp-dev \
    libnuma-dev \
    libpython3-dev \
    libboost-all-dev \
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
    pkg-config \
    patchelf

RUN GO_VERSION="1.23.8" && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then GOARCH="arm64"; \
    elif [ "$ARCH" = "x86_64" ]; then GOARCH="amd64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    wget https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz \
    && tar -C /usr/local -xzf go${GO_VERSION}.linux-${GOARCH}.tar.gz \
    && rm go${GO_VERSION}.linux-${GOARCH}.tar.gz

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
