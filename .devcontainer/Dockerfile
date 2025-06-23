FROM ubuntu:22.04

ARG ARCH
ARG GO_VERSION=1.23.10

WORKDIR /workspaces

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    vim \
    git \
    wget \
    build-essential \
    cmake \
    net-tools \
    tcpdump \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    libibverbs-dev \
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
    && rm -rf /var/lib/apt/lists/*

RUN ARCH=$(uname -m) && \
    case $ARCH in \
        x86_64) GO_ARCH="amd64";; \
        aarch64) GO_ARCH="arm64";; \
        *) echo "Unsupported architecture: $ARCH" >&2; exit 1;; \
    esac && \
    wget "https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" \
    && tar -C /usr/local -xzf "go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" \
    && rm "go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
    
RUN git clone https://github.com/alibaba/yalantinglibs.git \
    && cd yalantinglibs \
    && mkdir -p build \
    && cd build \
    && cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF \
    && cmake --build . -j$(nproc) \
    && cmake --install . \
    && cd /workspaces \
    && rm -rf yalantinglibs

ENV GOPROXY='https://goproxy.cn'
ENV PATH=/usr/local/go/bin:$PATH

CMD ["bash"]
