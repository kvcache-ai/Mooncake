# Build Guide

This document describes how to build Mooncake.

## Automatic

### Recommended Version
- OS: Ubuntu 22.04 LTS+
- cmake: 3.16.x
- gcc: 9.4+

### Steps
1. Install dependencies, stable Internet connection is required:
   ```bash
   bash dependencies.sh
   ```

2. In the root directory of this project, run the following commands:
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

## Manual

### Recommended Version
- cmake: 3.22.x
- boost-devel: 1.66.x
- grpc: 1.27.x
- googletest: 1.12.x
- gcc: 10.2.1
- go: 1.19+
- hiredis
- curl

### Steps

1. Install dependencies from system software repository:
    ```bash
    # For debian/ubuntu
    apt-get install -y build-essential \
                   cmake \
                   libibverbs-dev \
                   libgoogle-glog-dev \
                   libgtest-dev \
                   libjsoncpp-dev \
                   libnuma-dev \
                   libcurl4-openssl-dev \
                   libhiredis-dev

    # For centos/alibaba linux os
    yum install cmake \
                gflags-devel \
                glog-devel \
                libibverbs-devel \
                numactl-devel \
                gtest \
                gtest-devel \
                boost-devel \
                openssl-devel \
                protobuf-devel \
                hiredis-devel \
                libcurl-devel \
                protobuf-compiler 
    ```
    NOTE: You may need to install gtest, glog, gflags from source code:
    ```bash
    git clone https://github.com/gflags/gflags
    git clone https://github.com/google/glog
    git clone https://github.com/abseil/googletest.git
    ```

1. (optional). if you want to use GPU,
    First, follow the instructions in https://docs.nvidia.com/cuda/cuda-installation-guide-linux/ to install CUDA (ensure to select the `nvidia-fs` option to enable proper `cuFile` functionality). After that:
    1) Refer to Section 3.7 in https://docs.nvidia.com/cuda/gpudirect-rdma/ to install `nvidia-peermem` for enabling GPU-Direct RDMA:
    2) Configure `LIBRARY_PATH` and `LD_LIBRARY_PATH` for compile-time and runtime linking of `cuFile`, `cudart`, and other libraries:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/cuda/lib64
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64
    ```

2. Install grpc (v1.27.x)
    ```bash
    git clone https://github.com/grpc/grpc.git --depth 1 --branch v1.27.x
    cd grpc/
    git submodule update --init
    mkdir cmake-build
    cd cmake-build/
    cmake .. -DBUILD_SHARED_LIBS=ON \
            -DgRPC_INSTALL=ON \
            -DgRPC_BUILD_TESTS=OFF \
            -DgRPC_BUILD_CSHARP_EXT=OFF \
            -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
            -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
            -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
            -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
            -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
            -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
            -DgRPC_BACKWARDS_COMPATIBILITY_MODE=ON \
            -DgRPC_ZLIB_PROVIDER=package \
            -DgRPC_SSL_PROVIDER=package
    make -j`nproc`
    make install
    ```
    If `git submodule update --init` fails, please check Internet connection.

3. Install `cpprestsdk`
    ```bash
    git clone https://github.com/microsoft/cpprestsdk.git
    cd cpprestsdk
    mkdir build && cd build
    cmake .. -DCPPREST_EXCLUDE_WEBSOCKETS=ON
    make -j$(nproc) && make install
    ```

4. Install etcd-cpp-apiv3
    ```bash
    git clone https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
    cd etcd-cpp-apiv3
    mkdir build && cd build
    cmake ..
    make -j$(nproc)
    make install
    ```
    NOTE: If you meet the following outputs:
    ```
    /usr/local/bin/grpc_cpp_plugin error while loading shared libraries: libprotoc.so.3.11.2.0: cannot open shared object file: No such file or directory
    ```
    You should first find the location of `libprotoc.so.3.11.2.0`, e.g., `/usr/local/lib64`, and append this directory to the `LD_LIBRARY_PATH` environment variable as like:
    ```bash
    echo $LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=/usr/local/lib/:/usr/local/lib64/
    ```

5. In the root directory of this project, run the following commands:
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

## Advanced Compile Options
Mooncake supports the following advanced compile options:
- `-DUSE_CUDA=[ON|OFF]`: Enable GPU Direct RDMA & NVMe-of support. 
- `-DUSE_CXL=[ON|OFF]`: Enable CXL protocols. 
- `-DWITH_P2P_STORE=[ON|OFF]`: Enable Golang support and build P2P Store. 
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`: Enable Rust language support.
- `-DUSE_REDIS=[ON|OFF]`: Enable Redis as metadata server in Mooncake (`hiredis` required).
- `-DUSE_HTTP=[ON|OFF]`: Enable Http as metadata server in Mooncake (`curl` required).
