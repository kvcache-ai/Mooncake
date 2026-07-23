# Mooncake SSD Pool 多层接入架构 — EBS KVCacheStore 接入方案

## 概述

Mooncake 的 SSD pool 管理采用分层架构，每一层都提供**自管理**和 **SDK 托管**两种模式。SDK（如 EBS KVCacheStore）可以在任意层次接入，接管该层的能力，而其余层次仍由 Mooncake 自管理。

```text
┌─────────────────────────────────────────────────────────────┐
│                       元数据管理层                            │
│                                                             │
│   Exist / BatchExist / 对象生命周期 / replica 跟踪            │
│                                                             │
│   ┌──────────────────────┐   ┌───────────────────────────┐  │
│   │ Mooncake metadata    │   │ SDK metadata              │  │
│   │ shard (自管理)        │   │ SDK Exist/上报 metadata   │  │
│   └──────────────────────┘   └───────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                     磁盘管理与维护层                          │
│                                                             │
│   水位 / GC / 健康检测 / 恢复重建 / 调度                      │
│                                                             │
│   ┌──────────────────────┐   ┌───────────────────────────┐  │
│   │ Mooncake 自维护       │   │ SDK 上报                  │  │
│   │ probe / heartbeat /  │   │ 设备状态 / 容量 / 故障事件  │  │
│   │ maintenance plan     │   │ → StorageDeviceMetadata   │  │
│   └──────────────────────┘   └───────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                       Executor 层                            │
│                                                             │
│   数据 I/O: Store / Retrieve / Delete / Iterate              │
│                                                             │
│   ┌──────────────────────┐   ┌───────────────────────────┐  │
│   │ NvmeKvExecutor       │   │ SDK Executor              │  │
│   │ (io_uring/ioctl/     │   │ (EBS SDK Put/Get/         │  │
│   │  libnvme/stub)       │   │  Delete/List)             │  │
│   └──────────────────────┘   └───────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

每一层的"自管理"和"SDK 托管"可以独立组合。例如：

- **全 Mooncake**：NvmeKvExecutor + Mooncake 磁盘管理 + Mooncake metadata shard
- **Executor 托管**：SDK Executor 做数据 I/O，Mooncake 管理磁盘和元数据
- **Executor + 磁盘管理 托管**：SDK 做数据 I/O 并上报设备状态，Mooncake 管理元数据
- **全 SDK 托管**：SDK 做数据 I/O、上报设备状态、提供 Exist 查询

## Executor 层

Executor 层负责数据面的 KV 命令执行。

**Mooncake 自有**：`NvmeKvCommandExecutor`，通过 io_uring / ioctl / libnvme 对 NVMe KV namespace 发 Store / Retrieve / Delete / List 命令。支持本地盘和 NVMe-oF 远端盘。

**SDK 接入**：SDK 提供等价的 KV 命令能力（Put / Get / Delete / List），通过 SDK connector 替代 NvmeKvCommandExecutor。数据路径走 SDK 自己的栈（EFC → DPU → HPN → 盘古），不经过 Mooncake 的 Transfer Engine 或 NVMe passthrough。

接入方式：实现 `StorageBackendInterface`，在 `BatchOffload` / `BatchLoad` / `IsExist` / `ScanMeta` 中调用 SDK 的 KV 接口。

## 磁盘管理与维护层

磁盘管理层负责设备健康、容量水位、GC、恢复重建和调度决策。

**Mooncake 自维护**：Mooncake 通过 probe worker 周期性探测设备健康，驱动 HEALTHY → DEGRADED → FAILED 状态转换；通过 allocator 跟踪容量水位；通过 maintenance plan 识别 GC 和 recovery 候选设备；通过 rebuild job 做数据重建。

**SDK 上报**：SDK 侧拥有更丰富的设备信息（物理盘状态、EC 重建进度、namespace 映射、容量水位线、故障事件）。SDK 通过 `StorageDeviceMetadataUpdate` 向 Mooncake 上报变更，Mooncake 的磁盘管理层接收后更新设备视图。

两者可以叠加：SDK 上报底层设备事件，Mooncake 在上层做调度决策（是否移除分配、是否触发 GC、是否启动 rebuild）。状态优先级：

```text
operator disable > SDK reported failure > Mooncake probe failure > SDK degraded > healthy
```

已有的 `StorageDeviceMetadata` 模型和 admin API（metadata / maintenance / probe / recovery）同时覆盖自维护和 SDK 上报两种模式，不需要额外接口。

## 元数据管理层

元数据管理层负责回答"某个 key 是否存在、在哪里、什么状态"。

**Mooncake metadata shard**：当前 ExistKey / BatchExistKey / GetReplicaList 通过 MasterService 的 metadata shard 查找。对象的 replica 位置、lease、lifecycle 由 Mooncake 管理。

**SDK 元数据能力**：SDK 自带分布式元数据系统，支持 Exist / BatchExist 查询。两种使用方式：

1. **查询委托**：Mooncake 的 ExistKey 在本地 shard 未命中时，fallback 到 `StorageBackendInterface::IsExist`，由 SDK 回答。
2. **事件上报**：SDK 在 key 写入、淘汰、删除时向 Mooncake 上报元数据变更事件，Mooncake 据此维护本地 shard 中的记录。这样后续查询仍走本地 shard，保持低延迟。

两种方式可以组合：事件上报保持 shard 热数据同步，查询委托作为 shard miss 时的兜底。

## EBS KVCacheStore 的位置

EBS KVCacheStore 作为一个完整的 SDK，可以在全部三层接入：

| 层次 | EBS 提供 | Mooncake 角色 |
|------|---------|--------------|
| Executor | EBS SDK Put/Get/Delete/List（EFC → DPU → HPN → 盘古） | 不参与数据 I/O |
| 磁盘管理 | 上报 EC 设备状态、容量水位、故障事件 | 综合 SDK 上报和自身 probe，做调度和维护决策 |
| 元数据 | 提供 Exist/BatchExist 查询 + LRU 淘汰事件上报 | 维护本地 shard 映射，fallback 到 SDK 查询 |

EBS 场景特性（EC 12+3 高可靠、盘古 append 写入、DPU 隔离、Quota/QoS）对 Mooncake 透明，通过 `opaque_provider_metadata` 携带，不影响管理层的通用逻辑。

## 待对齐问题

1. SDK 的 LRU 淘汰是回调通知还是需要 Mooncake 轮询？
2. 单台 GPU 服务器对接一个 EBS volume 还是共享集群级 volume？
3. SDK BatchPut/BatchGet 的错误粒度：全成功/全失败，还是 per-key？
4. SDK connector 的生命周期：跟 MasterService 还是跟 client？
