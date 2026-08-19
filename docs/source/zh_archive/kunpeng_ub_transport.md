# Kunpeng UB Transport 
Kunpeng UbTransport源代码路径为Mooncake/mooncake-transfer-engine/src/transport/kunpneg_transport，该路径下有UB协议的Transport对接代码和实现逻辑。

## 概述
UB（Unified Bus，统一总线） 是与RDMA、CXL、NVLink 和TCP处于同一抽象层的传输协议，属于可在应用层灵活选择的传输方案。目前 UB 协议有两个开源实现：URMA（远程内存访问语义）和 OBMM（Load/Store 语义）。

URMA（Unified Remote Memory Access，统一远程内存访问）是UB协议为上层应用提供的统一编程抽象与核心语义层。它基于 UB 协议低延迟、高带宽的底层特性，为远程共享内存的访问与操作提供统一的 API 和语义接口。

URMA 开源代码仓库：https://atomgit.com/openeuler/umdk

OBMM (Ownership Based Memory Management) 是面向超节点环境的内核内存管理系统，支持跨节点的物理内存共享。该系统通过内核模块 (obmm.ko) 和用户态库 (libobmm.so) 提供高效的远程内存访问能力。

OBMM 开源代码仓库：https://atomgit.com/openeuler/obmm

## 新增依赖
Kunpeng UbTransport在Mooncake本身依赖的基础上，新增了一部分URMA和OBMM的依赖：

- **硬件平台**: 支持原生UB互联架构的鲲鹏950 CPU
- **OS版本**: openEuler 24.03 (LTS-SP3) [下载链接](https://www.openeuler.openatom.cn/zh/download/#openEuler%2024.03%20LTS%20SP3)
- **URMA依赖**: UMDK: `yum install umdk-urma-devel` 或从[源码](https://atomgit.com/openeuler/umdk)构建。
- **协议优势**: URMA 提供类似 RDMA 的内存语义，针对鲲鹏芯片片上互联进行了优化

---

## 构建与编译

**前置条件**

- openEuler 24.03 (LTS-SP3) [下载链接](https://www.openeuler.openatom.cn/zh/download/#openEuler%2024.03%20LTS%20SP3)
- 已安装 UMDK: `yum install umdk-urma-devel` 或从[源码](https://atomgit.com/openeuler/umdk)构建

**CMake 配置**

```bash
# 克隆 Mooncake 仓库
git clone https://github.com/kvcache-ai/Mooncake.git 
cd Mooncake

# 启用 UB 传输层进行配置
mkdir build && cd build
cmake .. -DUSE_UB=ON \
         -DURMA_INCLUDE_DIR=/usr/include \
         -DURMA_LIBRARY=/usr/lib64/liburma.so

# 编译
make -j$(nproc)
```

**验证**

```bash
# 检查 UB 传输层是否已注册
./mooncake_server --list-transports
# 预期输出: rdma, tcp, nvlink, ub
```

---

## 运行与测试

**单节点基准测试**

```bash
# 终端 1: 目标端（Target）
./transfer_engine_bench \
    --mode=target \
    --protocol=ub \
    --device_name=urma0 \
    --local_server_name=127.0.0.1 \
    --metadata_server=P2PHANDSHAKE

# 终端 2: 发起端（Initiator）
./transfer_engine_bench \
    --mode=initiator \
    --protocol=ub \
    --device_name=urma0 \
    --metadata_server=P2PHANDSHAKE \
    --segment_size=8388608 \
    --batch_size=1\
    --segment_id=127.0.0.1:$PORT
```

**多设备基准测试**

```bash
# 自动发现多个 URMA 设备
./transfer_engine_bench \
    --protocol=ub \
    --device_name=urma0,urma1,urma2,urma3
```
