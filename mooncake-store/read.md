# Mooncake 项目读文件调用流程

## 概述

写文件调用的是 `Client::PutToLocalFile`，对应的读文件流程如下：

## 调用流程

### 1. 公开 API
- **Client::Get** ([client.cpp:348](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/client.cpp#L348))
  - 首先调用 `Query` 获取对象的副本位置信息
  - 然后调用内部的 `Get` 重载版本进行数据传输

### 2. 内部传输
- **Client::TransferRead** ([client.cpp:1294](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/client.cpp#L1294))
  - 调用 `TransferData` 执行实际的数据传输

- **Client::TransferData** ([client.cpp:1269](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/client.cpp#L1269))
  - 调用 `transfer_submitter_->submit` 提交传输任务

### 3. 传输策略选择
- **TransferSubmitter::submit** ([transfer_task.cpp:395](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/transfer_task.cpp#L395))
  - 如果是**内存副本** (memory replica):
    - 使用 `submitMemcpyOperation` (本地 memcpy) 或 `submitTransferEngineOperation` (RDMA/TCP)
  - 如果是**磁盘副本** (disk replica):
    - 使用 **submitFileReadOperation** ([transfer_task.cpp:537](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/transfer_task.cpp#L537))

### 4. 异步文件读取
- **FilereadWorkerPool** ([transfer_task.cpp:11](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/transfer_task.cpp#L11))
  - `FilereadWorkerPool::workerThread` ([transfer_task.cpp:62](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/transfer_task.cpp#L62)) 从任务队列中取出任务
  - 调用 `backend_->LoadObject` 从本地文件加载数据

### 5. 底层存储
- **StorageBackend::LoadObject** ([storage_backend.cpp:79](file:///c:/Users/liwei/src/kv/Mooncake/mooncake-store/src/storage_backend.cpp#L79))
  - 调用底层的 `StorageFile` 接口读取文件内容到 Slice 中

---

## 对比总结

| 写文件 | 读文件 |
|--------|--------|
| Client::Put | Client::Get |
| Client::TransferWrite | Client::TransferRead |
| TransferSubmitter::submit + WRITE opcode | TransferSubmitter::submit + READ opcode |
| 磁盘副本 → PutToLocalFile → backend_->StoreObject | 磁盘副本 → submitFileReadOperation → FilereadWorkerPool → backend_->LoadObject |

## 关键区别

**写文件**是同步调用 `PutToLocalFile`，而**读文件**是通过 `FilereadWorkerPool` 异步线程池来执行 `LoadObject`，这样可以避免阻塞主线程。
