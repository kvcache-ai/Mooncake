# Mooncake 与对象存储对接设计文档

## 1. 概述

Mooncake 是一个专为 LLM 推理场景设计的高性能分布式键值缓存存储引擎，通过构建多级缓存池（DRAM/SSD）提升推理效率。Mooncake 提供了完整的对象存储对接接口，支持数据持久化到本地文件系统或其他对象存储系统。

## 2. 架构设计

### 2.1 整体架构

Mooncake Store 包含两个核心组件：

- **Master Service**：负责集群存储空间管理、节点状态监控、对象元数据维护和空间分配
- **Client**：兼具客户端和存储服务端双重角色
  - 作为客户端：接收上层应用的 Put/Get/Remove 等请求
  - 作为服务端：提供内存/磁盘存储空间，处理其他客户端的数据传输请求

### 2.2 对象存储对接架构

```
┌─────────────┐       ┌──────────────┐       ┌──────────────────┐
│  应用层     │       │  Mooncake    │       │   存储后端       │
│             │──────>│  Client      │──────>│                  │
│             │       │              │       │  - FilePerKey    │
│             │       │  - Client    │       │  - Bucket        │
│             │       │  - FileStorage│       │  - OffsetAllocator│
└─────────────┘       └──────────────┘       └──────────────────┘
```

## 3. 核心接口设计

### 3.1 存储后端抽象接口

**文件路径**：`include/storage_backend.h`

```cpp
class StorageBackendInterface {
public:
    virtual tl::expected<void, ErrorCode> Init() = 0;
    
    virtual tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        std::function<void(const std::string& evicted_key)> eviction_handler = nullptr) = 0;
    
    virtual tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) = 0;
    
    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;
    
    virtual tl::expected<bool, ErrorCode> IsEnableOffloading() = 0;
    
    virtual tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) = 0;
};
```

### 3.2 本地文件存储实现

**文件路径**：`include/storage_backend.h`

```cpp
class StorageBackend {
public:
    // 构造函数和工厂方法
    explicit StorageBackend(const std::string& root_dir, const std::string& fsdir, bool enable_eviction = true);
    static std::shared_ptr<StorageBackend> Create(const std::string& root_dir, const std::string& fsdir, bool enable_eviction = true);
    
    // 初始化和空间管理
    tl::expected<void, ErrorCode> Init(uint64_t quota_bytes);
    bool InitQuotaEvict();
    
    // 对象存储操作
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(const std::string& path, const std::vector<Slice>& slices, const std::string& key = "");
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(const std::string& path, const std::string& str, const std::string& key = "");
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(const std::string& path, std::span<const char> data, const std::string& key = "");
    
    // 对象加载操作
    tl::expected<void, ErrorCode> LoadObject(const std::string& path, std::vector<Slice>& slices, int64_t length);
    tl::expected<void, ErrorCode> LoadObject(const std::string& path, std::string& str, int64_t length);
    
    // 对象删除操作
    void RemoveFile(const std::string& path);
    void RemoveByRegex(const std::string& key);
    void RemoveAll();
};
```

### 3.3 分桶存储后端

**文件路径**：`include/storage_backend.h`

```cpp
class BucketStorageBackend : public StorageBackendInterface {
public:
    // 批量卸载对象到存储
    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        std::function<void(const std::string& evicted_key)> eviction_handler = nullptr) override;
    
    // 批量加载对象
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;
    
    // 初始化存储后端
    tl::expected<void, ErrorCode> Init() override;
    
    // 检查对象是否存在
    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;
    
    // 扫描元数据
    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;
    
    // 检查是否允许继续卸载
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;
};
```

### 3.4 文件存储管理器

**文件路径**：`include/file_storage.h`

```cpp
class FileStorage {
public:
    // 构造函数
    FileStorage(const FileStorageConfig& config, std::shared_ptr<Client> client, const std::string& local_rpc_addr);
    
    // 初始化
    tl::expected<void, ErrorCode> Init();
    
    // 批量获取对象
    struct BatchGetResult {
        uint64_t batch_id;
        std::vector<uint64_t> pointers;
    };
    tl::expected<BatchGetResult, ErrorCode> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);
    
    // 释放批量缓冲区
    bool ReleaseBuffer(uint64_t batch_id);
};
```

## 4. 核心工作流程

### 4.1 Put 操作流程

**文件路径**：`src/client_service.cpp`

```cpp
tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key, std::vector<Slice>& slices, const ReplicateConfig& config) {
    // 1. 准备 slice 长度信息
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }
    
    // 2. 调用 MasterClient::PutStart() 分配存储空间
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        // 处理错误
        return tl::unexpected(start_result.error());
    }
    
    // 3. 处理磁盘副本（如果启用）
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin(); it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // 异步存储到本地文件
                PutToLocalFile(key, slices, replica.get_disk_descriptor());
                break;
            }
        }
    }
    
    // 4. 处理内存副本
    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            // 传输数据到内存副本
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                // 撤销 put 操作
                master_client_.PutRevoke(key, ReplicaType::MEMORY);
                return tl::unexpected(transfer_err);
            }
        }
    }
    
    // 5. 调用 MasterClient::PutEnd() 标记操作完成
    auto end_result = master_client_.PutEnd(key, ReplicaType::MEMORY);
    if (!end_result) {
        return tl::unexpected(end_result.error());
    }
    
    return {};
}
```

### 4.2 磁盘持久化流程

**文件路径**：`src/client_service.cpp`

```cpp
void Client::PutToLocalFile(const std::string& key, const std::vector<Slice>& slices, const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;
    
    // 计算总大小并合并数据
    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }
    
    std::string value;
    value.reserve(total_size);
    for (const auto& slice : slices) {
        value.append(static_cast<char*>(slice.ptr), slice.size);
    }
    
    // 异步写入到存储后端
    write_thread_pool_.enqueue([this, backend = storage_backend_, key, value = std::move(value), path = disk_descriptor.file_path] {
        // 存储对象
        auto store_result = backend->StoreObject(path, value, key);
        
        if (!store_result) {
            // 存储失败，撤销 put 操作
            LOG(ERROR) << "Failed to store object for key: " << key;
            master_client_.PutRevoke(key, ReplicaType::DISK);
            return;
        }
        
        // 处理被驱逐的对象
        for (const auto& evicted_key : store_result.value()) {
            master_client_.EvictDiskReplica(evicted_key, ReplicaType::DISK);
        }
        
        // 标记磁盘副本写入完成
        master_client_.PutEnd(key, ReplicaType::DISK);
    });
}
```

### 4.3 数据传输流程

**文件路径**：`src/client_service.cpp`

```cpp
ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices, TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }
    
    // 通过 TransferSubmitter 提交传输请求
    auto future = transfer_submitter_->submit(replica_descriptor, slices, op_code);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }
    
    // 等待传输完成
    return future->get();
}
```

## 5. 存储管理功能

### 5.1 数据驱逐策略

Mooncake 支持基于 FIFO 的数据驱逐策略，当存储空间不足时，会自动删除最早写入的对象：

```cpp
FileRecord StorageBackend::SelectFileToEvictByFIFO() {
    std::shared_lock<std::shared_mutex> lock(file_queue_mutex_);
    if (file_write_queue_.empty()) {
        return FileRecord();
    }
    return file_write_queue_.front();
}
```

### 5.2 批量操作支持

Mooncake 支持批量加载和卸载对象，提高存储效率：

```cpp
tl::expected<void, ErrorCode> StorageBackendAdaptor::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    // 批量加载对象
    for (auto& [key, slice] : batched_slices) {
        std::string path = GetFilePath(key);
        std::string value;
        auto load_result = storage_backend_->LoadObject(path, value, slice.size);
        if (load_result) {
            memcpy(slice.ptr, value.data(), value.size());
        } else {
            return load_result;
        }
    }
    return {};
}
```

## 6. 配置和初始化

### 6.1 存储后端配置

**文件路径**：`include/storage_backend.h`

```cpp
struct FileStorageConfig {
    // 存储后端类型
    StorageBackendType storage_backend_type = StorageBackendType::kBucket;
    
    // 数据文件存储路径
    std::string storage_filepath = "/data/file_storage";
    
    // 本地客户端缓冲区大小
    int64_t local_buffer_size = 1280 * kMB;  // ~1.2 GB
    
    // 全局限制
    int64_t total_keys_limit = 10'000'000;  // 最大总键数
    int64_t total_size_limit = 2ULL * 1024 * 1024 * 1024 * 1024;  // 最大总存储大小 (2 TB)
    
    // 心跳间隔（秒）
    uint32_t heartbeat_interval_seconds = 10;
    
    // 是否使用 io_uring 进行文件 I/O
    bool use_uring = false;
    
    // 从环境变量创建配置
    static FileStorageConfig FromEnvironment();
};
```

## 7. 接口调用示例

### 7.1 初始化客户端

```cpp
// 创建并初始化 Mooncake 客户端
std::shared_ptr<Client> client = Client::Create();

// 初始化参数
std::string local_hostname = "127.0.0.1:8000";
std::string metadata_connstring = "etcd://127.0.0.1:2379";
std::string protocol = "rdma";
std::string master_server_entry = "127.0.0.1:9000";

// 初始化客户端
ErrorCode init_result = client->Init(local_hostname, metadata_connstring, protocol, nullptr, master_server_entry);
if (init_result != ErrorCode::OK) {
    LOG(ERROR) << "Failed to initialize client: " << toString(init_result);
    return;
}
```

### 7.2 存储对象

```cpp
// 准备数据
std::string key = "test_key";
std::vector<Slice> slices;
char data[1024] = "Hello, Mooncake!";
slices.emplace_back(data, sizeof(data));

// 复制配置
ReplicateConfig config;
config.replica_num = 2;  // 2 个副本
config.with_soft_pin = false;

// 存储对象
auto put_result = client->Put(key, slices, config);
if (!put_result) {
    LOG(ERROR) << "Failed to put object: " << toString(put_result.error());
} else {
    LOG(INFO) << "Successfully put object: " << key;
}
```

### 7.3 读取对象

```cpp
// 准备接收缓冲区
std::vector<Slice> read_slices;
char read_buffer[1024];
read_slices.emplace_back(read_buffer, sizeof(read_buffer));

// 读取对象
auto get_result = client->Get(key, read_slices);
if (!get_result) {
    LOG(ERROR) << "Failed to get object: " << toString(get_result.error());
} else {
    LOG(INFO) << "Successfully get object: " << key;
    LOG(INFO) << "Data: " << std::string(read_buffer, sizeof(read_buffer));
}
```

## 8. 总结

Mooncake 提供了完整的对象存储对接架构，包括：

1. **灵活的存储后端接口**：支持多种存储实现方式（FilePerKey、Bucket、OffsetAllocator）
2. **高性能的数据传输**：基于 Transfer Engine 实现零拷贝、多网卡聚合传输
3. **完善的存储管理功能**：支持数据驱逐、批量操作、元数据管理
4. **异步持久化机制**：提高写入性能，减少延迟
5. **可配置的存储策略**：支持不同的存储需求和场景

Mooncake 的对象存储接口设计简洁易用，同时提供了高性能和灵活性，能够满足 LLM 推理等高性能场景的存储需求。