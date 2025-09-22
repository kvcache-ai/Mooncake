# Heterogeneous Ascend Transport

## 概述
Heterogeneous Ascend Transport 是一款面向异构推理场景的高性能数据传输库，专为充分发挥异构计算架构优势而设计。其核心功能与应用场景如下：

- **910B 设备**：执行 PREFILL 操作，承担高效推理计算任务
- **H20 设备**：执行 DECODE 操作，负责数据解码处理
- **Heterogeneous Ascend Transport**：实现跨设备 KVCACHE 传输，高效管理 910B NPU 显存与 H20 GPU 显存间的数据交换

> 当前版本仅支持 WRITE 语义，READ 语义支持将在后续版本中实现。

## 新增优化
HBM 到 DRAM 的拷贝带宽受数据块大小制约，2MB 以下的小数据块会导致带宽无法打满。我们通过 “数据聚合 + 流水线并行” 优化：先在 HBM 内将小数据块合并为 8MB 再搬运至 DRAM，同时让数据拷贝与 RDMA 传输并行执行。该方案有效掩盖了 HBM-DRAM 拷贝时延，显著降低了整体传输耗时。

## 编译说明
在 `mooncake-common/common.cmake` 配置文件中新增 `USE_ASCEND_HETEROGENEOUS` 编译选项，用于控制本功能的启用状态：

- **PREFILL 侧（910B）**：
  - 开启 `USE_ASCEND_HETEROGENEOUS` 编译选项
  - 重新编译 Mooncake 项目

- **DECODE 侧（H20）**：
  - 直接使用 Mooncake RDMA Transport
  - 启用 GPU Direct 功能（支持通过 RDMA 直接访问 GPU 显存）

## 参数配置
使用本功能需配置以下关键参数：

| 参数名 | 说明 |
|--------|------|
| `source` | 源地址（910B NPU 显存地址） |
| `target_offset` | 目标地址偏移量（H20 GPU 显存地址） |
| `opcode` | 操作码（当前仅支持 `WRITE`） |

## 测试指南

### 测试用例
提供以下测试程序：
- **发起端（910B）**：`transfer_engine_heterogeneous_ascend_perf_initiator.cpp`
- **目标端（H20）**：`rdma_transport_test.cpp`（复用现有测试）

编译完成后，测试程序位于：
```
build/mooncake-transfer-engine/example/
build/mooncake-transfer-engine/test/
```

测试程序支持通过 `DEFINE_string` 参数进行配置，具体参数请参考测试文件开头的参数列表。

### 测试流程
当配置 `metadata_server=P2PHANDSHAKE` 时，系统将自动选择可用端口以避免冲突，测试步骤如下：

1. **启动目标节点**
   - 查看目标节点日志（位于 `transfer_engine.cpp`）
   - 记录如下格式的监听信息：
     ```
     Transfer Engine RPC using <协议> listening on <IP>:<端口>
     ```

2. **配置发起节点**
   - 修改 `--segment_id` 参数值为1中打印的目标节点实际监听地址（格式：`<IP>:<端口>`）

3. **启动发起节点**完成连接测试

### 典型测试命令
**目标端（H20）**：
```bash
./rdma_transport_test --mode=target --local_server_name=10.10.10.10 --metadata_server=P2PHANDSHAKE --operation=write --protocol=rdma --device_name=mlx5_1 --use_vram=true --gpu_id=0
```

**发起端（910B）**：
```bash
./transfer_engine_heterogeneous_ascend_perf_initiator --mode=initiator --local_server_name=10.10.10.10 --metadata_server=P2PHANDSHAKE --operation=write --npu_id=1 --segment_id=10.10.10.10:12345 --device_name=mlx5_1 --block_size=65536 --batch_size=128
```