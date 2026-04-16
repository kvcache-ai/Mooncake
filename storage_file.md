# Mooncake SSD-Offload 实现与配置分析

## 1. SSD-Offload 实现机制

Mooncake 的 SSD-offload 功能是作为 Real Client 进程内部的后台子系统实现的，对应用程序透明。其核心实现组件如下：

### 1.1 核心组件

| 组件 | 功能描述 |
|------|---------|
| **FileStorage** | 顶层协调器，管理存储后端、暂存缓冲区和后台线程 |
| **StorageBackendInterface** | 存储后端抽象接口，有三种实现：<br>- Bucket Backend<br>- FilePerKey Backend<br>- Offset Allocator Backend |
| **Heartbeat Thread** | 定期与 Master 通信，获取需要 offload 的对象列表 |
| **ClientBuffer** | 预注册的、O_DIRECT 对齐的暂存区，用于零拷贝读取 |

### 1.2 工作流程

#### 1.2.1 Offload 流程（内存 → SSD）

1. **心跳触发**：FileStorage 内部的心跳线程定期向 Master 发送心跳请求
2. **获取 offload 列表**：Master 返回需要 offload 的对象列表（key→size 映射）
3. **异步 offload**：心跳线程将这些对象写入 SSD
4. **通知完成**：offload 完成后，向 Master 发送完成通知

#### 1.2.2 Load 流程（SSD → 内存）

1. **内存查找**：应用程序调用 Get 时，首先在分布式内存缓存中查找
2. **SSD 回退**：如果内存中没有找到，自动回退到从 SSD 读取
3. **零拷贝读取**：使用 ClientBuffer 进行零拷贝读取，直接加载到应用程序内存

### 1.3 架构图

```
┌─────────────────────────────────────────────────────────┐
│                  Application (vLLM, etc.)               │
└──────────────────────────┬──────────────────────────────┘
                           │ MooncakeDistributedStore API
                           ▼
┌─────────────────────────────────────────────────────────┐
│                      Real Client                        │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │                  FileStorage                     │   │
│  │  ┌────────────┐  ┌──────────────────────────┐    │   │
│  │  │  Heartbeat │  │   ClientBuffer (staging) │    │   │
│  │  │   Thread   │  └──────────────────────────┘    │   │
│  │  └─────┬──────┘                                  │   │
│  │        │ offload / load                          │   │
│  │        ▼                                         │   │
│  │  ┌─────────────────────────────────────────┐     │   │
│  │  │        StorageBackendInterface          │     │   │
│  │  │  ┌───────────┐ ┌──────────┐ ┌────────┐  │     │   │
│  │  │  │  Bucket   │ │FilePerKey│ │Offset  │  │     │   │
│  │  │  │  Backend  │ │ Backend  │ │Alloc.  │  │     │   │
│  │  │  └───────────┘ └──────────┘ └────────┘  │     │   │
│  │  └─────────────────────────────────────────┘     │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │         In-memory distributed KV cache           │   │
│  │              (Transfer Engine / RDMA)            │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                           │
                    Local SSD / NVMe
```

## 2. 具体配置方式

Mooncake 的 SSD-offload 功能主要通过 `FileStorageConfig` 结构体进行配置，支持从环境变量加载配置。

### 2.1 主要配置参数

| 配置项 | 默认值 | 描述 | 环境变量 |
|--------|-------|------|----------|
| **storage_backend_type** | StorageBackendType::kBucket | 存储后端类型 | MC_STORE_STORAGE_BACKEND_TYPE |
| **storage_filepath** | "/data/file_storage" | 数据文件存储路径 | MC_STORE_STORAGE_FILEPATH |
| **local_buffer_size** | 1280 * 1024 * 1024 (~1.2 GB) | 本地客户端缓冲区大小 | MC_STORE_LOCAL_BUFFER_SIZE |
| **scanmeta_iterator_keys_limit** | 20000 | 每次 Scan 调用返回的最大键数 | MC_STORE_SCANMETA_ITERATOR_KEYS_LIMIT |
| **total_keys_limit** | 10,000,000 | 最大键总数 | MC_STORE_TOTAL_KEYS_LIMIT |
| **total_size_limit** | 2 TB | 最大存储总大小 | MC_STORE_TOTAL_SIZE_LIMIT |
| **heartbeat_interval_seconds** | 10 | 心跳间隔（秒） | MC_STORE_HEARTBEAT_INTERVAL_SECONDS |
| **client_buffer_gc_interval_seconds** | 1 | 客户端缓冲区垃圾回收间隔 | MC_STORE_CLIENT_BUFFER_GC_INTERVAL_SECONDS |
| **client_buffer_gc_ttl_ms** | 5000 | 客户端缓冲区生存时间 | MC_STORE_CLIENT_BUFFER_GC_TTL_MS |
| **use_uring** | false | 是否使用 io_uring 进行文件 I/O | MC_STORE_USE_URING |

### 2.2 启用方式

在 RealClient 的 `setup_internal` 方法中，通过设置 `enable_offload` 参数为 `true` 来启用 SSD-offload 功能：

```cpp
// 代码示例
auto result = real_client->setup_internal(
    "localhost",                    // local_hostname
    "metadata_server:50051",        // metadata_server
    16 * 1024 * 1024,               // global_segment_size
    16 * 1024 * 1024,               // local_buffer_size
    "tcp",                          // protocol
    "",                             // rdma_devices
    "master_server:50051",          // master_server_addr
    nullptr,                        // transfer_engine
    "",                             // ipc_socket_path
    50052,                          // local_rpc_port
    true                            // enable_offload = true
);
```

### 2.3 配置加载流程

1. **默认配置**：`FileStorageConfig` 提供了默认配置值
2. **环境变量覆盖**：通过 `FileStorageConfig::FromEnvironment()` 从环境变量加载配置
3. **手动配置**：也可以直接设置 `FileStorageConfig` 对象的字段

## 3. RealClient 实例数量

在 Mooncake 集群中，**每个节点通常运行一个 RealClient 实例**。这个实例负责：

1. 管理该节点上的分布式内存缓存
2. 处理该节点上的 SSD-offload 操作
3. 与 Master 服务器通信

具体来说：

- **单节点部署**：只有一个 RealClient 实例
- **多节点集群**：每个节点运行一个 RealClient 实例，所有实例通过 Master 服务器协调工作

这种设计确保了每个节点的本地资源（内存、SSD）得到充分利用，同时通过分布式架构实现了横向扩展。

## 4. 关键技术特性

### 4.1 透明性

SSD-offload 功能对应用程序完全透明，应用程序不需要修改任何代码即可享受扩展的缓存容量。

### 4.2 零拷贝 RDMA

热路径（内存中的数据）通过零拷贝 RDMA 传输，保持高性能。

### 4.3 异步 offload

offload 操作在后台异步执行，不影响应用程序的正常运行。

### 4.4 多种存储后端

支持三种存储后端，可以根据不同的使用场景选择合适的后端：

- **Bucket Backend**：适用于大量小对象
- **FilePerKey Backend**：适用于少量大对象
- **Offset Allocator Backend**：平衡性能和空间利用率

## 5. vLLM Work 进程的 Global Segment 配置

### 5.1 结论

**是的，vLLM 的每个 work 进程都需要配置独立的 global segment**。

### 5.2 详细分析

#### vLLM 与 Mooncake 的集成架构

vLLM 使用多进程架构（每个 GPU 对应一个 work 进程），而 Mooncake 的 RealClient 实例是与进程绑定的。根据文档和代码分析：

- 每个 vLLM work 进程都是独立的 Mooncake RealClient 实例
- 每个 RealClient 实例需要自己的内存资源来存储或处理 KV 缓存
- Global segment 是 RealClient 用于存储分布式 KV 缓存的核心内存资源

#### RealClient 的 Global Segment 机制

从 `real_client.cpp` 的代码可以看出：

```cpp
// If global_segment_size is 0, skip mount segment;
// If global_segment_size is larger than max_mr_size, split to multiple
// mapped_shms.
if (protocol == "cxl") {
    // CXL 特定处理
} else {
    auto max_mr_size = globalConfig().max_mr_size;     // Max segment size
    uint64_t total_glbseg_size = global_segment_size;  // For logging
    uint64_t current_glbseg_size = 0;                  // For logging

    while (global_segment_size > 0) {
        size_t segment_size = std::min(global_segment_size, max_mr_size);
        global_segment_size -= segment_size;
        current_glbseg_size += segment_size;
        LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                  << current_glbseg_size << " of " << total_glbseg_size;

        size_t mapped_size = segment_size;
        // 挂载分段...
    }
}
```

- Global segment 是 RealClient 初始化时的必要参数（默认值为 `DEFAULT_GLOBAL_SEGMENT_SIZE`）
- 如果 `global_segment_size` 大于 0，RealClient 会将其分割成多个分段并挂载
- 每个分段都会被注册到分布式系统中，用于存储 KV 缓存

#### vLLM Work 进程的配置需求

从 vLLM 集成文档可以看出：

- 每个 vLLM worker 需要一个唯一的 bootstrap 端口（`VLLM_MOONCAKE_BOOTSTRAP_PORT`）
- 对于 TP/DP 部署，每个 worker 的端口计算方式为：`base_port + dp_rank * tp_size + tp_rank`

这表明每个 vLLM work 进程都是独立的网络实体，需要自己的资源配置，包括：
- 独立的 bootstrap 端口
- 独立的内存资源（global segment）
- 独立的网络连接

### 5.3 最佳实践

在 vLLM 环境中配置 Mooncake 时：

1. **为每个 work 进程分配独立的 global segment**：
   - 大小应根据每个 GPU 的内存容量和工作负载来确定
   - 可以通过环境变量或配置文件为每个进程设置不同的值

2. **合理分配资源**：
   - 确保 global segment 的总大小不超过系统可用内存
   - 考虑与其他进程（如 GPU 驱动、操作系统）的内存竞争

3. **使用默认配置作为起点**：
   - Mooncake 提供了合理的默认配置（`DEFAULT_GLOBAL_SEGMENT_SIZE`）
   - 可以根据实际性能测试结果进行调整

## 6. 总结

Mooncake 的 SSD-offload 功能通过在 RealClient 内部实现的 FileStorage 子系统，实现了分布式内存缓存到本地 SSD 的透明扩展。它通过定期心跳机制与 Master 协调 offload 操作，支持多种存储后端，并通过环境变量提供了灵活的配置方式。在集群环境中，每个节点通常运行一个 RealClient 实例，共同构成分布式缓存系统。对于 vLLM 的多进程架构，每个 work 进程都需要配置独立的 global segment，因为每个进程都是独立的 Mooncake RealClient 实例，需要自己的内存资源来存储和处理 KV 缓存。

## 7. 关键问题分析

### 7.1 多个 RealClient 是否会 offload 同一个 key 到 SSD？

**结论：不会**

**分析依据：**
- **Master 协调机制**：从 `FileStorage::Heartbeat` 方法可以看到，每个 RealClient 通过心跳向 Master 发送状态，Master 返回需要 offload 的对象列表。
- **Key 归属唯一性**：在分布式缓存系统中，每个 key 通常有明确的归属节点，Master 会根据 key 的归属关系分配 offload 任务。
- **避免重复操作**：从 `FileStorage::OffloadObjects` 方法可以看到，offload 是一个资源密集型操作，系统设计上会避免多个 RealClient 重复处理同一个 key。

### 7.2 Master 是否会等 RealClient offload 到 SSD 才淘汰出内存？

**结论：会**

**分析依据：**
- **数据安全保证**：从 `FileStorage::OffloadObjects` 方法可以看到，offload 完成后会调用 `client_->NotifyOffloadSuccess` 通知 Master。
- **内存读取依赖**：从 `FileStorage::BatchQuerySegmentSlices` 方法可以看到，offload 过程需要从内存中读取数据，如果 Master 提前淘汰内存，会导致 offload 失败。
- **异步执行但有序完成**：虽然 offload 操作是异步执行的，但系统设计上会确保在数据安全写入 SSD 后才进行内存淘汰。

### 7.3 核心实现细节

#### 7.3.1 Offload 流程
1. **心跳触发**：`FileStorage` 内部的心跳线程定期向 Master 发送心跳请求
2. **获取 offload 列表**：Master 返回需要 offload 的对象列表（key→size 映射）
3. **异步 offload**：心跳线程将这些对象写入 SSD
4. **通知完成**：offload 完成后，向 Master 发送 `NotifyOffloadSuccess` 通知
5. **Master 处理**：Master 收到通知后，确认数据已安全存储到 SSD，然后淘汰内存中的对应数据

#### 7.3.2 核心代码路径
- **心跳与任务获取**：`FileStorage::Heartbeat()` → `client_->OffloadObjectHeartbeat()`
- **数据 offload**：`FileStorage::OffloadObjects()` → `storage_backend_->BatchOffload()`
- **完成通知**：`complete_handler` → `client_->NotifyOffloadSuccess()`
- **内存淘汰**：Master 收到 offload 成功通知后执行内存淘汰

### 7.4 设计优势

1. **数据安全性**：确保数据在写入 SSD 后才从内存中淘汰，避免数据丢失
2. **资源利用率**：通过 Master 协调，避免多个 RealClient 重复 offload 同一个 key
3. **异步处理**：offload 操作在后台异步执行，不影响应用程序的正常运行
4. **透明性**：对应用程序完全透明，无需修改代码即可享受扩展的缓存容量
