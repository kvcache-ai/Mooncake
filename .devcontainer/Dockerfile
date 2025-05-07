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
    libhiredis-dev

RUN wget https://go.dev/dl/go1.22.12.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.22.12.linux-amd64.tar.gz

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
