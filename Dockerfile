# Base Image from NVIDIA
FROM pytorch/pytorch:2.7.1-cuda12.8-cudnn9-devel
ARG GO_VERSION=1.23.10
ENV PYTHON_VERSION=3.12
WORKDIR /app

COPY . .

ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:$PATH

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    vim \
    git \
    wget \
    build-essential \
    cmake \
    net-tools \
    tcpdump \
    curl \
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
    
ENV GOPROXY='https://goproxy.cn'
ENV PATH=/usr/local/go/bin:$PATH

# Execute installation in the container
RUN bash dependencies.sh \
     && apt autoremove -y \
     && apt clean -y \
     && rm -rf /tmp/* /var/tmp/* \
     && find /var/cache/apt/archives /var/lib/apt/lists -not -name lock -type f -delete \
     && find /var/cache -type f -delete

RUN rm -rf build || true \
     && mkdir build && cd build \
     && cmake .. && make -j && make install

