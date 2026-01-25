# Build Guide

This document describes how to build Mooncake.

## PyPI Package
Install the Mooncake Transfer Engine package from PyPI, which includes both Mooncake Transfer Engine and Mooncake Store Python bindings:

**For CUDA-enabled systems:**
```bash
pip install mooncake-transfer-engine
```
📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine/](https://pypi.org/project/mooncake-transfer-engine/)

**For non-CUDA systems:**
```bash
pip install mooncake-transfer-engine-non-cuda
```
📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine-non-cuda/](https://pypi.org/project/mooncake-transfer-engine-non-cuda/)

> **Note**: The CUDA version includes Mooncake-EP and GPU topology detection, requiring CUDA 12.1+. The non-CUDA version is for environments without CUDA dependencies.

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

3. If you want to compile the Moore Mthreads GPUDirect support module, first follow the instructions in https://docs.mthreads.com/musa-sdk/musa-sdk-doc-online/install_guide to install MUSA. After that:
    1) Install `mthreads-peermem` for enabling GPU-Direct RDMA
    2) Configure `LIBRARY_PATH` and `LD_LIBRARY_PATH` to ensure linking of `musart`, and other libraries during compilation:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/musa/lib
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/musa/lib

4. Install yalantinglibs
    ```bash
    git clone https://github.com/alibaba/yalantinglibs.git
    cd yalantinglibs
    mkdir build && cd build
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
    make -j$(nproc)
    make install
    ```

5. In the root directory of this project, run the following commands:
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

6. Install Mooncake python package and mooncake_master executable
   ```bash
   make install
   ```

## Use Mooncake in Docker Containers
Mooncake supports Docker-based deployment. What you need is to get Docker image by `docker pull alogfans/mooncake`.
For the container to use the host's network resources, you need to add the `--device` option when starting the container. The following is an example.

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
- `-DUSE_MUSA=[ON|OFF]`: Enable Moore Threads GPU support via MUSA
- `-DUSE_HIP=[ON|OFF]`: Enable AMD GPU support via HIP/ROCm
- `-DUSE_INTRA_NVLINK=[ON|OFF]`: Enable intranode nvlink transport
- `-DUSE_CXL=[ON|OFF]`: Enable CXL support
- `-DWITH_STORE=[ON|OFF]`: Build Mooncake Store component
- `-DWITH_P2P_STORE=[ON|OFF]`: Enable Golang support and build P2P Store component, require go 1.23+
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`: Enable Rust support
- `-DUSE_REDIS=[ON|OFF]`: Enable Redis-based metadata service
- `-DUSE_HTTP=[ON|OFF]`: Enable Http-based metadata service
- `-DUSE_ETCD=[ON|OFF]`: Enable etcd-based metadata service, require go 1.23+
- `-DSTORE_USE_ETCD=[ON|OFF]`: Enable etcd-based failover for Mooncake Store, require go 1.23+. **Note:** `-DUSE_ETCD` and `-DSTORE_USE_ETCD` are two independent options. Enabling `-DSTORE_USE_ETCD` does **not** depend on `-DUSE_ETCD`
- `-DSTORE_USE_JEMALLOC=[ON|OFF]`: Use jemalloc allocator in mooncake_master for better memory management, default is ON
- `-DBUILD_SHARED_LIBS=[ON|OFF]`: Build Transfer Engine as shared library, default is OFF
- `-DBUILD_UNIT_TESTS=[ON|OFF]`: Build unit tests, default is ON
- `-DBUILD_EXAMPLES=[ON|OFF]`: Build examples, default is ON
