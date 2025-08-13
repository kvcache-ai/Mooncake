# 编译指南

本文档叙述了 Mooncake 框架的源码编译安装方法。

- 注意: 我们已经为 Ubuntu 22.04/24.04 预先构建了 pypi 安装包，您可以使用 pip/pip3 以更简单的方式安装和使用 mooncake。
   ```bash
   pip3 install mooncake-transfer-engine --upgrade
   ```

## 自动安装

### 建议版本
- OS: Ubuntu 22.04 LTS+
- cmake: 3.20.x
- gcc: 9.4+

### 步骤
1. 安装依赖项（需要稳定的网络连接）：
   ```bash
   bash dependencies.sh
   ```

2. 在项目根目录下运行以下命令：
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

3. 安装 Mooncake python 包和 mooncake_master 可执行文件：
   ```bash
   make install
   ```

## 手动安装

### 建议版本
- cmake: 3.22.x
- boost-devel: 1.66.x
- grpc: 1.27.x
- googletest: 1.12.x
- gcc: 10.2.1
- go: 1.22+
- hiredis
- curl

### 编译步骤

1. 通过系统源下载安装下列第三方库：
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
                hiredis-devel \
                libcurl-devel
    ```

    注意：如果源没有gtest, glog, gflags, 则需要通过源码安装

    ```bash
    git clone https://github.com/gflags/gflags
    git clone https://github.com/google/glog
    git clone https://github.com/abseil/googletest.git
    ```

3. 如果你要编译 GPUDirect 支持模块，首先需按照 https://docs.nvidia.com/cuda/cuda-installation-guide-linux/ 的指引安装 CUDA (确保启用 `nvidia-fs` 以正确编译 `cuFile` 模块)。之后:
    1) 按照 https://docs.nvidia.com/cuda/gpudirect-rdma/ 的第 3.7 节说明安装 `nvidia-peermem` 以启用 GPU-Direct RDMA
    2) 配置 `LIBRARY_PATH` 和 `LD_LIBRARY_PATH` 以确保编译过程期间链入 `cuFile`, `cudart` 等库:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/cuda/lib64
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64
    ```

4. 安装 yalantinglibs
    ```bash
    git clone https://github.com/alibaba/yalantinglibs.git
    cd yalantinglibs
    mkdir build && cd build
    cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
    make -j$(nproc)
    make install
    ```

5. 进入项目根目录，运行下列命令进行编译
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

6. 安装 Mooncake python 包和 mooncake_master 可执行文件
   ```bash
   make install
   ```

## 高级编译选项
在执行 `cmake ..` 期间可以使用下列选项指定是否编译 Mooncake 的某些组件。
- `-DUSE_CUDA=[ON|OFF]`: 启用 GPU Direct RDMA 及 NVMe-of 支持
- `-DUSE_CXL=[ON|OFF]`: 启用 CXL 支持
- `-DWITH_STORE=[ON|OFF]`: 编译 Mooncake Store 组件
- `-DWITH_P2P_STORE=[ON|OFF]`: 启用 Golang 支持并编译 P2P Store 组件，需要 go 1.23+
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`: 启用 Rust 支持
- `-DUSE_REDIS=[ON|OFF]`: 启用基于 Redis 的元数据服务
- `-DUSE_HTTP=[ON|OFF]`: 启用基于 Http 的元数据服务
- `-DUSE_ETCD=[ON|OFF]`: 启用基于 etcd 的元数据服务，需要 go 1.23+
- `-DSTORE_USE_ETCD=[ON|OFF]`: 启用基于 etcd 的 Mooncake Store 容错机制，需要 go 1.23+
- `-DBUILD_SHARED_LIBS=[ON|OFF]`: 将 Transfer Engine 编译为共享库，默认为 OFF
- `-DBUILD_UNIT_TESTS=[ON|OFF]`: 编译单元测试，默认为 ON
- `-DBUILD_EXAMPLES=[ON|OFF]`: 编译示例程序，默认为 ON
- `-DUSE_ASCEND_DIRECT=[ON|OFF]`: 启用 Ascend Direct RDMA 及 HCCS 支持
