# Build Guide

This document describes how to build Mooncake from source.

- Note: we have prebuild the pypi wheel for Ubuntu 22.04/24.04, you can simply install mooncake with pip/pip3.
   ```bash
   pip3 install mooncake-transfer-engine --upgrade
   ```
- To install with the Mooncake Backend and Mooncake EP support, use the following command:
   ```bash
   # replace torch2.8.0 with the corresponding version
   pip3 install mooncake-transfer-engine==0.3.7+ep --upgrade
   ```

## Automatic

### Recommended Version
- OS: Ubuntu 22.04 LTS+
- cmake: 3.20.x
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
3. Install Mooncake python package and mooncake_master executable
   ```bash
   sudo make install
   ```

## Manual

### Recommended Version
- cmake: 3.22.x
- boost-devel: 1.66.x
- googletest: 1.12.x
- gcc: 10.2.1
- go: 1.22+
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
                       libunwind-dev \
                       libpython3-dev \
                       libboost-all-dev \
                       libssl-dev \
                       pybind11-dev \
                       libcurl4-openssl-dev \
                       libhiredis-dev \
                       pkg-config \
                       patchelf

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
                hiredis-devel \
                libcurl-devel
    ```

    NOTE: You may need to install gtest, glog, gflags from source code:
    ```bash
    git clone https://github.com/gflags/gflags
    git clone https://github.com/google/glog
    git clone https://github.com/abseil/googletest.git
    ```

2. If you want to compile the GPUDirect support module, first follow the instructions in https://docs.nvidia.com/cuda/cuda-installation-guide-linux/ to install CUDA (ensure to enable `nvidia-fs` for proper `cuFile` module compilation). After that:
    1) Follow Section 3.7 in https://docs.nvidia.com/cuda/gpudirect-rdma/ to install `nvidia-peermem` for enabling GPU-Direct RDMA
    2) Configure `LIBRARY_PATH` and `LD_LIBRARY_PATH` to ensure linking of `cuFile`, `cudart`, and other libraries during compilation:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/cuda/lib64
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64
    ```

3. Install yalantinglibs
    ```bash
    git clone https://github.com/alibaba/yalantinglibs.git
    cd yalantinglibs
    mkdir build && cd build
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
    make -j$(nproc)
    make install
    ```

4. In the root directory of this project, run the following commands:
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

5. Install Mooncake python package and mooncake_master executable
   ```bash
   make install
   ```

## Use Mooncake in Docker Containers
Mooncake supports Docker-based deployment. What you need is to get Docker image by `docker pull alogfans/mooncake`.
For the the container to use the host's network resources, you need to add the `--device` option when starting the container. The following is an example.

```
# In host
sudo docker run --net=host --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm --ulimit memlock=-1 -t -i mooncake:v0.9.0 /bin/bash
# Run transfer engine in container
cd /Mooncake-main/build/mooncake-transfer-engine/example
./transfer_engine_bench --device_name=ibp6s0 --metadata_server=10.1.101.3:2379 --mode=target --local_server_name=10.1.100.3
```

## Advanced Compile Options
The following options can be used during `cmake ..` to specify whether to compile certain components of Mooncake.
- `-DUSE_CUDA=[ON|OFF]`: Enable GPU Direct RDMA and NVMe-of support
- `-DUSE_CXL=[ON|OFF]`: Enable CXL support
- `-DWITH_STORE=[ON|OFF]`: Build Mooncake Store component
- `-DWITH_P2P_STORE=[ON|OFF]`: Enable Golang support and build P2P Store component, require go 1.23+
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`: Enable Rust support
- `-DUSE_REDIS=[ON|OFF]`: Enable Redis-based metadata service
- `-DUSE_HTTP=[ON|OFF]`: Enable Http-based metadata service
- `-DUSE_ETCD=[ON|OFF]`: Enable etcd-based metadata service, require go 1.23+
- `-DSTORE_USE_ETCD=[ON|OFF]`: Enable etcd-based failover for Mooncake Store, require go 1.23+
- `-DBUILD_SHARED_LIBS=[ON|OFF]`: Build Transfer Engine as shared library, default is OFF
- `-DBUILD_UNIT_TESTS=[ON|OFF]`: Build unit tests, default is ON
- `-DBUILD_EXAMPLES=[ON|OFF]`: Build examples, default is ON
