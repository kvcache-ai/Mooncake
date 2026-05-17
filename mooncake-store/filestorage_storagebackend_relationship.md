# FileStorage 与 StorageBackend 的关系分析

## 1. 概述

在 Mooncake 存储系统中，`FileStorage` 和 `StorageBackend` 是两个核心组件，它们通过明确的职责划分和接口抽象形成了一种层次化的架构关系。这种设计既保证了存储实现的灵活性，又提供了统一的上层接口。

## 2. 核心组件定义

### 2.1 StorageBackend 家族

StorageBackend 是 Mooncake 中负责**实际数据持久化**的组件，它提供了直接与存储介质（通常是本地文件系统）交互的能力。

#### 2.1.1 StorageBackendInterface

**文件路径**：`include/storage_backend.h`

这是所有存储后端的**抽象接口**，定义了存储操作的统一 API 契约：

```cpp
class StorageBackendInterface {
public:
    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual tl::expected<int64_t, ErrorCode> BatchOffload(...) = 0;
    virtual tl::expected<void, ErrorCode> BatchLoad(...) = 0;
    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;
    virtual tl::expected<bool, ErrorCode> IsEnableOffloading() = 0;
    virtual tl::expected<void, ErrorCode> ScanMeta(...) = 0;
};
```

#### 2.1.2 StorageBackend

**文件路径**：`include/storage_backend.h`

这是**本地文件存储的具体实现**，提供了基于文件系统的存储功能，支持将每个对象存储为单个文件：

```cpp
class StorageBackend {
public:
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(...);
    tl::expected<void, ErrorCode> LoadObject(...);
    void RemoveFile(...);
    // ...
};
```

#### 2.1.3 StorageBackendAdaptor

**文件路径**：`include/storage_backend.h`

这是一个**适配器类**，用于将旧的 `StorageBackend` 适配到新的 `StorageBackendInterface` 接口：

```cpp
class StorageBackendAdaptor : public StorageBackendInterface {
    // 实现 StorageBackendInterface 接口
    // 内部使用 StorageBackend 进行实际的存储操作
};
```

#### 2.1.4 BucketStorageBackend

**文件路径**：`include/storage_backend.h`

这是**分桶存储的实现**，将多个对象存储在一个文件中，提高存储效率：

```cpp
class BucketStorageBackend : public StorageBackendInterface {
    // 实现 StorageBackendInterface 接口
    // 使用分桶策略存储对象
};
```

#### 2.1.5 OffsetAllocatorStorageBackend

**文件路径**：`include/storage_backend.h`

这是**基于偏移分配器的存储实现**，使用固定大小的文件和偏移量来管理对象存储：

```cpp
class OffsetAllocatorStorageBackend : public StorageBackendInterface {
    // 实现 StorageBackendInterface 接口
    // 使用偏移分配器管理存储
};
```

### 2.2 FileStorage

**文件路径**：`include/file_storage.h`

`FileStorage` 是 Mooncake 中负责**对象存储管理**的高级组件，它不仅仅是简单的存储，还提供了与 Mooncake 其他组件的集成和协调功能：

```cpp
class FileStorage {
public:
    tl::expected<void, ErrorCode> Init();
    tl::expected<BatchGetResult, ErrorCode> BatchGet(...);
    bool ReleaseBuffer(uint64_t batch_id);
    // ...
};
```

## 3. 关系模型

### 3.1 层次化架构

`FileStorage` 和 `StorageBackend` 形成了一种**层次化的架构关系**：

```
┌─────────────────────────────────────────────────────────────────┐
│                         FileStorage                            │
│  - 客户端缓冲区管理                                             │
│  - 批量操作支持                                               │
│  - 心跳机制                                                   │
│  - 与 Master Service 通信                                      │
│  - 对象卸载和加载的协调                                        │
└───────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     StorageBackendInterface                     │
│  - 定义存储操作的统一 API                                       │
└───────────────────────────────┬─────────────────────────────────┘
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                    │
            ▼                    ▼                    ▼
┌───────────────────┐ ┌─────────────────────┐ ┌──────────────────────────┐
│BucketStorageBackend│ │StorageBackendAdaptor│ │OffsetAllocatorStorageBackend│
│ - 分桶存储实现    │ │ - 适配旧的StorageBackend│ │ - 基于偏移分配器的存储实现│
└───────────────────┘ └─────────────┬───────┘ └──────────────────────────┘
                                     │
                                     ▼
                             ┌───────────────────┐
                             │   StorageBackend  │
                             │ - 本地文件存储实现 │
                             └───────────────────┘
```

### 3.2 依赖关系

- `FileStorage` **依赖** `StorageBackendInterface` 来实现实际的存储功能
- `StorageBackendInterface` 有多种**实现类**，包括 `BucketStorageBackend`、`StorageBackendAdaptor` 和 `OffsetAllocatorStorageBackend`
- `StorageBackendAdaptor` **依赖** `StorageBackend` 来实现具体的存储操作

## 4. 交互流程

### 4.1 初始化流程

```cpp
// FileStorage 构造函数
FileStorage::FileStorage(const FileStorageConfig& config, std::shared_ptr<Client> client, const std::string& local_rpc_addr) {
    // 1. 验证配置
    if (!config.Validate()) {
        throw std::invalid_argument("Invalid FileStorage configuration");
    }
    
    // 2. 根据配置创建 StorageBackend 实例
    auto create_storage_backend_result = CreateStorageBackend(config_);
    if (!create_storage_backend_result) {
        throw std::runtime_error("Failed to create storage backend");
    }
    
    // 3. 保存 StorageBackend 实例
    storage_backend_ = create_storage_backend_result.value();
    
    // 4. 其他初始化...
}

// 初始化 StorageBackend
tl::expected<void, ErrorCode> FileStorage::Init() {
    // ... 其他初始化 ...
    
    // 初始化 StorageBackend
    auto init_storage_backend_result = storage_backend_->Init();
    if (!init_storage_backend_result) {
        return init_storage_backend_result;
    }
    
    // ... 其他初始化 ...
}
```

### 4.2 对象卸载流程

```cpp
tl::expected<void, ErrorCode> FileStorage::OffloadObjects(const std::unordered_map<std::string, int64_t>& offloading_objects) {
    // 1. 准备卸载数据
    std::vector<std::vector<std::string>> buckets_keys;
    if (auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_)) {
        // 如果是分桶存储，进行分桶操作
        auto allocate_res = bucket_backend->AllocateOffloadingBuckets(offloading_objects, buckets_keys);
        // ...
    } else {
        // 否则，直接使用所有键
        // ...
    }
    
    // 2. 定义完成回调
    auto complete_handler = [this](const std::vector<std::string>& keys, std::vector<StorageObjectMetadata>& metadatas) -> ErrorCode {
        // 通知 Master 卸载成功
        auto result = client_->NotifyOffloadSuccess(keys, metadatas);
        // ...
    };
    
    // 3. 执行卸载
    for (const auto& keys : buckets_keys) {
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        
        // 查询数据切片
        auto query_result = BatchQuerySegmentSlices(keys, batch_object);
        if (!query_result) {
            continue;
        }
        
        // 执行批量卸载
        auto offload_res = storage_backend_->BatchOffload(batch_object, complete_handler, eviction_handler);
        // ...
    }
    
    return {};
}
```

### 4.3 对象加载流程

```cpp
tl::expected<FileStorage::BatchGetResult, ErrorCode> FileStorage::BatchGet(
    const std::vector<std::string>& keys, const std::vector<int64_t>& sizes) {
    // 1. 分配批量缓冲区
    auto allocate_res = AllocateBatch(keys, sizes);
    if (!allocate_res) {
        return tl::make_unexpected(allocate_res.error());
    }
    
    // 2. 执行批量加载
    auto allocated_batch = allocate_res.value();
    auto result = BatchLoad(allocated_batch->slices);
    if (!result) {
        return tl::make_unexpected(result.error());
    }
    
    // 3. 处理结果
    // ...
    
    return batch_result;
}

// 内部调用 StorageBackend 的 BatchLoad
tl::expected<void, ErrorCode> FileStorage::BatchLoad(std::unordered_map<std::string, Slice>& batch_object) {
    // 直接调用 StorageBackend 的 BatchLoad 方法
    return storage_backend_->BatchLoad(batch_object);
}
```

## 5. 设计意图

### 5.1 关注点分离

- **StorageBackend**：专注于**实际的数据持久化**，处理与存储介质的直接交互
- **FileStorage**：专注于**存储管理和协调**，处理与 Mooncake 其他组件的交互

这种分离使得：
- StorageBackend 可以独立演进，支持不同的存储策略（分桶、文件、偏移分配器）
- FileStorage 可以专注于提供高级功能，而不需要关心具体的存储实现

### 5.2 灵活性和扩展性

- 通过 `StorageBackendInterface` 接口，Mooncake 可以轻松支持新的存储后端实现
- FileStorage 不需要修改代码，就可以使用不同的存储后端
- 用户可以通过配置选择最适合自己需求的存储后端

### 5.3 性能优化

- 不同的 StorageBackend 实现针对不同的场景进行了优化
  - `BucketStorageBackend` 适合大量小对象的存储
  - `StorageBackend` 适合简单的文件存储
  - `OffsetAllocatorStorageBackend` 适合需要高效随机访问的场景
- FileStorage 提供了客户端缓冲区管理和批量操作支持，进一步提高了性能

## 6. 配置和选择

用户可以通过配置选择不同的 StorageBackend 实现：

```cpp
// FileStorageConfig 中的配置项
StorageBackendType storage_backend_type = StorageBackendType::kBucket;
```

也可以通过环境变量配置：

```bash
# 设置存储后端类型
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
# 或 file_per_key_storage_backend
# 或 offset_allocator_storage_backend
```

## 7. 总结

`FileStorage` 和 `StorageBackend` 的关系是一种**层次化的依赖关系**，其中：

1. **StorageBackend** 是底层组件，负责实际的数据持久化，有多种实现方式
2. **FileStorage** 是上层组件，负责存储管理和协调，依赖 StorageBackend 实现实际的存储功能
3. 通过 `StorageBackendInterface` 接口，实现了两者的解耦，提高了系统的灵活性和扩展性

这种设计使得 Mooncake 可以适应不同的存储需求和场景，同时提供高效的对象存储服务。