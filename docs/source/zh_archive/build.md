# 编译指南

本文档叙述了 Mooncake 框架的源码编译安装方法。

- 注意:
   - 我们已经为 Ubuntu 22.04/24.04 预先构建了 pypi 安装包，您可以使用 pip/pip3 以更简单的方式安装和使用 mooncake。
      ```bash
      pip3 install mooncake-transfer-engine --upgrade
      ```
   - 从版本0.3.7开始，PyPi 源的 wheel 包要求用户环境已安装 cuda，如果您的环境报如下错误：
     ```bash
     Traceback (most recent call last):
     File "<string>", line 1, in <module>
      ImportError: libcudart.so.12: cannot open shared object file: No such file or directory
     ```
     请切换安装如下 PyPI 源的 wheel 包:
     ```bash
      pip install mooncake-transfer-engine-non-cuda
      ```
     或者请附带 `-DUSE_CUDA=OFF` 使用源码编译安装。

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
                   libhiredis-dev \
                   libjemalloc-dev

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
                libcurl-devel \
                jemalloc-devel
    ```

    注意：如果源没有gtest, glog, gflags, 则需要通过源码安装

    ```bash
    git clone https://github.com/gflags/gflags
    git clone https://github.com/google/glog
    git clone https://github.com/abseil/googletest.git
    ```

2. 如果你要编译Nvidia GPUDirect 支持模块，首先需按照 https://docs.nvidia.com/cuda/cuda-installation-guide-linux/ 的指引安装 CUDA (确保启用 `nvidia-fs` 以正确编译 `cuFile` 模块)。之后:
    1) 按照 https://docs.nvidia.com/cuda/gpudirect-rdma/ 的第 3.7 节说明安装 `nvidia-peermem` 以启用 GPU-Direct RDMA
    2) 配置 `LIBRARY_PATH` 和 `LD_LIBRARY_PATH` 以确保编译过程期间链入 `cuFile`, `cudart` 等库:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/cuda/lib64
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64
    ```

3. 如果你要编译Moore Threads GPUDirect RDMA 支持模块，首先需按照 https://docs.mthreads.com/musa-sdk/musa-sdk-doc-online/install_guide 的指引安装 MUSA SDK。之后:
    1) 安装 `mthreads-peermem` 以启用 GPU-Direct RDMA
    2) 配置 `LIBRARY_PATH` 和 `LD_LIBRARY_PATH` 以确保编译过程期间链入 `musart` 等库:
    ```bash
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/musa/lib
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/musa/lib
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
- `-DUSE_MUSA=[ON|OFF]`: 通过 MUSA 启用对摩尔线程 GPU 的支持
- `-DUSE_HIP=[ON|OFF]`: 通过 HIP/ROCm 启用对 AMD GPU 的支持
- `-DUSE_CXL=[ON|OFF]`: 启用 CXL 支持
- `-DWITH_STORE=[ON|OFF]`: 编译 Mooncake Store 组件
- `-DWITH_P2P_STORE=[ON|OFF]`: 启用 Golang 支持并编译 P2P Store 组件，需要 go 1.23+
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`: 启用 Rust 支持
- `-DUSE_REDIS=[ON|OFF]`: 启用基于 Redis 的元数据服务
- `-DUSE_HTTP=[ON|OFF]`: 启用基于 Http 的元数据服务
- `-DUSE_ETCD=[ON|OFF]`: 启用基于 etcd 的元数据服务，需要 go 1.23+
- `-DSTORE_USE_ETCD=[ON|OFF]`: 启用基于 etcd 的 Mooncake Store 容错机制，需要 go 1.23+。**注意：** `-DUSE_ETCD` 和 `-DSTORE_USE_ETCD` 是两个相互独立的选项。启用 `-DSTORE_USE_ETCD` 并**不依赖**于 `-DUSE_ETCD`。
- `-DSTORE_USE_JEMALLOC=[ON|OFF]`: 在 mooncake_master 中使用 jemalloc 内存分配器以获得更好的内存管理，默认为 ON
- `-DBUILD_SHARED_LIBS=[ON|OFF]`: 将 Transfer Engine 编译为共享库，默认为 OFF
- `-DBUILD_UNIT_TESTS=[ON|OFF]`: 编译单元测试，默认为 ON
- `-DBUILD_EXAMPLES=[ON|OFF]`: 编译示例程序，默认为 ON
- `-DUSE_ASCEND_DIRECT=[ON|OFF]`: 启用 Ascend Direct RDMA 及 HCCS 支持
- `-DUSE_MUSA=[ON|OFF]`: 启用Moore Threads GPUDirect RDMA

## 在 Docker 容器中使用 Mooncake

Mooncake 支持基于 Docker 的部署。您可以通过以下命令获取 Docker 镜像：

```bash
docker pull alogfans/mooncake
```

为了让容器能够使用主机的网络资源（特别是 InfiniBand RDMA），您需要在启动容器时添加 --device 选项。以下是使用示例：

## 在宿主机中运行容器
```bash
sudo docker run --net=host \
                --device=/dev/infiniband/uverbs0 \
                --device=/dev/infiniband/rdma_cm \
                --ulimit memlock=-1 \
                -t -i mooncake:v0.9.0 /bin/bash
```


## 进入容器后，运行 transfer engine 示例

```bash
cd /app/build/mooncake-transfer-engine/example
./transfer_engine_bench --device_name=ibp6s0 \
                        --metadata_server=10.1.101.3:2379 \
                        --mode=target \
                        --local_server_name=10.1.100.3

```

注意事项：

--device 参数将宿主机的 RDMA 设备映射到容器内

--ulimit memlock=-1 解除内存锁定限制，RDMA 操作需要

--net=host 让容器使用宿主机的网络命名空间

