# 快速使用指南

Mooncake 由 Transfer Engine、Managed Store、P2P Store 等组件组成，这些组件可按应用需求分别进行编译和使用。

## 编译
Mooncake 目前仅支持 Linux 操作系统，并且依赖以下软件：
- Go 1.20+
- GCC 9.4+
- CMake 3.16+
- etcd 3.2+

1. 克隆源码仓库
   ```bash
   git clone https://github.com/kvcache-ai/mooncake-dev.git
   ```
2. 进入源码目录
   ```bash
   cd mooncake
   ```

3. 切换到目标分支，如 `v0.1`
   ```bash
   git checkout v0.1
   ```

   > The development branch often involves large changes, so do not use the clients compiled in the "development branch" for the production environment.

5. 安装依赖库（需要 root 权限）
   ```bash
   bash dependencies.sh
   ```

5a. (optional) 配置 GPU 相关依赖

   首先按照 https://docs.nvidia.com/cuda/cuda-installation-guide-linux/ 中的说明安装 cuda（需要勾选 nvidia-fs 选项, 以便于 cufile 的正常使用），之后：

   1) 参考 https://docs.nvidia.com/cuda/gpudirect-rdma/ 的 3.7 节，安装 nvidia-peermem 以启用 gpu-direct RDMA:

   2) 配置 `LIBRARY_PATH` 和 `LD_LIBRARY_PATH`，用于编译和运行时链接 cufile, cudart 等库：
   ```bash
   export $LIBRARY_PATH=$LIBRARY_PATH:/usr/local/cuda/lib64
   export $LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64
   ```

6. 编译 Mooncake 组件
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   ```

### 高级编译选项
Mooncake 支持在执行 `cmake` 命令期间添加下列高级编译选项：
- `-DUSE_CUDA=[ON|OFF]`：编译 Transfer Engine 时启用或关闭 GPU Direct RDMA 功能的支持。仅支持 NVIDIA CUDA，需要事先安装相应的依赖库。（不包含在 `dependencies.sh` 脚本中）。默认关闭。
- `-DUSE_CXL=[ON|OFF]`：编译 Transfer Engine 时启用或关闭 CXL 协议的支持。默认关闭。
- `-DWITH_P2P_STORE=[ON|OFF]`：编译 P2P Store 及示例程序，默认开启。
- `-DWITH_ALLOCATOR=[ON|OFF]`：编译 Managed Store 所用的中心分配器模块，默认开启。
- `-DWITH_WITH_RUST_EXAMPLE=[ON|OFF]`：编译 Transfer Engine 时启用或关闭 Rust 语言支持，默认关闭。


## Transfer Engine Bench 使用方法
编译 Transfer Engine 成功后，可在 `build/mooncake-transfer-engine/example` 目录下产生测试程序 `transfer_engine_bench`。该工具通过调用 Transfer Engine 接口，发起节点从目标节点的 DRAM 处反复读取/写入数据块，以展示 Transfer Engine 的基本用法，并可用于测量读写吞吐率。目前 Transfer Engine Bench 工具可用于 RDMA 协议（GPUDirect 正在测试） 及 TCP 协议。

1. **启动 `metadata` 服务。** 该服务用于 Mooncake 各类元数据的集中高可用管理，包括 Transfer Engine 的内部连接状态等。需确保发起节点和目标节点都能顺利通达该 metadata 服务，因此需要注意：
   - metadata 服务的监听 IP 不应为 127.0.0.1，需结合网络环境确定。在实验环境中，可使用 0.0.0.0。
   - 在某些平台下，如果发起节点和目标节点设置了 `http_proxy` 或 `https_proxy` 环境变量，也会影响 Transfer Engine 与 metadata 服务的通信。

   Transfer Engine 支持多种 metadata 服务，包括 `etcd`, `redis` 和 `http`。下面以 `etcd` 和 `http` 为例说明如何启动 metadata 服务。

   1.1. **`etcd`**

   例如，可使用如下命令行启动 `etcd` 服务：
      ```bash
      # This is 10.0.0.1
      etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379
      ```

   1.2. **启动 `http` 作为 `metadata` 服务**

   例如，可使用 `mooncake-transfer-engine/example/http-metadata-server` 示例中的 `http` 服务：
      ```bash
      # This is 10.0.0.1
      # cd mooncake-transfer-engine/example/http-metadata-server
      go run . --addr=:8080
      ```

2. **启动目标节点。**
    ```bash
    # This is 10.0.0.2
    export MC_GID_INDEX=n
    ./transfer_engine_bench --mode=target \
                            --metadata_server=10.0.0.1:2379 \
                            --local_server_name=10.0.0.2:12345 \
                            --device_name=erdma_0
    ```
   各个参数的含义如下：
   - 环境变量 `MC_GID_INDEX` 的默认值为 3，是大多数 IB/RoCE 网络所用的 GID Index。对于阿里 eRDMA，需要设置为 1。
   - `--mode=target` 表示启动目标节点。目标节点不发起读写请求，只是被动按发起节点的要求供给或写入数据。
      > 注意：实际应用中可不区分目标节点和发起节点，每个节点可以向集群内其他节点自由发起读写请求。
   - `--metadata_server` 为元数据服务器地址（etcd 服务的完整地址）。
      > 如果使用 `http` 作为 `metadata` 服务，需要将 `--metadata_server` 参数改为 `--metadata_server=http://10.0.0.1:8080/metadata`。
   - `--local_server_name` 表示本机器地址，大多数情况下无需设置。如果不设置该选项，则该值等同于本机的主机名（即 `hostname(2)` ）。集群内的其它节点会使用此地址尝试与该节点进行带外通信，从而建立 RDMA 连接。
      > 注意：若带外通信失败则连接无法建立。因此，若有必要需修改集群所有节点的 `/etc/hosts` 文件，使得可以通过主机名定位到正确的节点。
   - `--device_name` 表示传输过程使用的 RDMA 网卡名称。
      > 提示：高级用户还可通过 `--nic_priority_matrix` 传入网卡优先级矩阵 JSON 文件，详细参考 Transfer Engine 的开发者手册。
   - 在仅支持 TCP 的网络环境中，可使用 `--protocol=tcp` 参数，此时不需要指定 `--device_name` 参数。

   也可通过拓扑自动发现功能基于操作系统配置自动生成网卡优先级矩阵，此时不需要指定传输过程使用的 RDMA 网卡名称。
   ```
   ./transfer_engine_bench --mode=target \
                           --metadata_server=10.0.0.1:2379 \
                           --local_server_name=10.0.0.2:12345 \
                           --auto_discovery
   ```

1. **启动发起节点。**
    ```bash
    # This is 10.0.0.3
    export MC_GID_INDEX=n
    ./transfer_engine_bench --metadata_server=10.0.0.1:2379 \
                            --segment_id=10.0.0.2:12345 \
                            --local_server_name=10.0.0.3:12346 \
                            --device_name=erdma_1
    ```
   各个参数的含义如下（其余同前）：
   - `--segment_id` 可以简单理解为目标节点的主机名，需要和启动目标节点时 `--local_server_name` 传入的值（如果有）保持一致。
   
   正常情况下，发起节点将开始进行传输操作，等待 10s 后回显“Test completed”信息，表明测试完成。

   发起节点还可以配置下列测试参数：`--operation`（可为 `"read"` 或 `"write"`）、`batch_size`、`block_size`、`duration`、`threads`, `use_vram` 等。



## P2P Store 使用与测试方法
按照上面步骤编译 P2P Store 成功后，可在 `build/mooncake-p2p-store` 目录下产生测试程序 `p2p-store-example`。该工具演示了 P2P Store 的使用方法，模拟了训练节点完成训练任务后，将模型数据迁移到大量推理节点的过程。目前仅支持 RDMA 协议。

1. **启动 `etcd` 服务。** 这与 Transfer Engine Bench 所述的方法是一致的。
   
2. **启动模拟训练节点。** 该节点将创建模拟模型文件，并向集群内公开。
   ```bash
   # This is 10.0.0.2
   export MC_GID_INDEX=n
   ./p2p-store-example --cmd=trainer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.2:12345 \
                       --device_name=erdma_0
   ```

3. **启动模拟推理节点。** 该节点会从模拟训练节点或其他模拟推理节点拉取数据。
   ```bash
   # This is 10.0.0.3
   export MC_GID_INDEX=n
   ./p2p-store-example --cmd=inferencer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.3:12346 \
                       --device_name=erdma_1
   ```
   测试完毕显示“ALL DONE”。

上述过程中，模拟推理节点检索数据来源由 P2P Store 内部逻辑实现，因此不需要用户提供训练节点的 IP。同样地，需要保证其他节点可使用本机主机名 `hostname(2)` 或创建节点期间填充的 `--local_server_name` 来访问这台机器。
