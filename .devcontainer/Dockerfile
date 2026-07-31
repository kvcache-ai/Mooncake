FROM ubuntu:22.04

WORKDIR /workspaces

RUN apt-get update -y \
    && apt-get install -y vim git wget build-essential cmake net-tools tcpdump

RUN apt-get install -y libibverbs-dev \
    libunwind-dev \
    libnuma-dev \
    libpython3-dev \
    libboost-dev \
    libssl-dev \
    libgrpc-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    libcurl4-openssl-dev \
    libjemalloc-dev \
    libmsgpack-dev \
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

ENV GOPROXY='https://goproxy.cn'
ENV PATH=/usr/local/go/bin:$PATH

CMD ["bash"]
