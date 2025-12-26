# 批量写入 etcd 的时序问题分析

## 问题概述

批量写入 etcd 的方案可能存在以下时序问题：

1. **事件顺序问题**：批量写入可能导致事件顺序混乱
2. **竞态条件**：Standby 可能在不同时间看到不同批次的事件
3. **重复删除问题**：同一个 key 可能出现在多个批次中
4. **延迟导致的不一致**：批量延迟可能导致 Standby 看到过期数据

## 时序问题详细分析

### 问题 1：事件顺序混乱

#### 场景描述

```
时间线：
T1: 驱逐 key1 → 加入 batch1
T2: 驱逐 key2 → 加入 batch1
T3: 显式删除 key1 → 立即写入 etcd (单个事件)
T4: batch1 写入 etcd (包含 key1, key2)
```

**问题**：
- Standby 在 T3 看到 key1 被删除（显式删除）
- Standby 在 T4 又看到 key1 被删除（批量删除）
- 或者 Standby 先看到 T4 的批量删除，后看到 T3 的显式删除

#### 影响

1. **重复删除**：Standby 可能尝试删除同一个 key 两次
   - 影响：性能开销，但通常可以容忍（幂等操作）

2. **顺序混乱**：如果 key1 在 T3 被显式删除，但在 T4 的批量中又出现
   - 影响：Standby 可能看到"删除 → 存在 → 删除"的奇怪序列

### 问题 2：批量延迟导致的不一致

#### 场景描述

```
时间线：
T1: 驱逐 key1 → 加入 batch1（未写入 etcd）
T2: Standby 读取 key1 → 看到 key1 存在（因为 batch1 还没写入）
T3: batch1 写入 etcd（包含 key1 的删除）
T4: Standby Watch 到 key1 被删除
```

**问题**：
- T1-T3 期间，Standby 可能看到过期的 key1
- 如果 Standby 在 T2 读取 key1，然后在 T4 看到删除，可能导致不一致

#### 影响

1. **短暂的不一致**：Standby 可能在短时间内看到 Primary 已经删除的 key
   - 影响：可能导致 Standby 返回过期数据

2. **租约续约问题**：如果 Standby 在 T2 续约了 key1 的租约，但 key1 在 T1 已经被删除
   - 影响：Standby 可能续约了不存在的 key

### 问题 3：批量边界导致的事件丢失

#### 场景描述

```
时间线：
T1: 驱逐 key1 → 加入 batch1
T2: batch1 达到阈值（1000条）→ 开始写入 etcd
T3: 驱逐 key2 → 加入 batch2（新批次）
T4: batch1 写入完成
T5: Primary 崩溃
```

**问题**：
- batch1 中的 key1 已经写入 etcd（Standby 能看到）
- batch2 中的 key2 还未写入 etcd（Standby 看不到）
- 如果 Primary 在 T5 崩溃，batch2 中的事件会丢失

#### 影响

1. **部分事件丢失**：Standby 可能只看到部分删除事件
   - 影响：Standby 和 Primary 的数据不一致

2. **恢复困难**：Primary 恢复后，无法知道哪些 key 应该被删除
   - 影响：需要重新同步或清理

### 问题 4：Watch 顺序问题

#### 场景描述

```
时间线：
T1: batch1 写入 etcd (seq=100, keys=[key1, key2])
T2: 显式删除 key3 → 立即写入 etcd (seq=101)
T3: batch2 写入 etcd (seq=102, keys=[key4, key5])
```

**Standby Watch 顺序**：
- 如果 Standby 的 Watch 是顺序的，会按 seq=100, 101, 102 的顺序看到
- 但如果 etcd 的 Watch 有延迟，可能看到不同的顺序

#### 影响

1. **事件顺序保证**：etcd 的 Watch 保证顺序，但批量写入可能打乱逻辑顺序
   - 影响：Standby 可能看到"批量删除 key1 → 显式删除 key3 → 批量删除 key2"的序列

2. **时间戳混乱**：批量中的 key 可能有不同的实际删除时间，但共享同一个时间戳
   - 影响：Standby 无法区分 key 的实际删除顺序

## 解决方案

### 方案 1：时间戳 + 序列号（推荐）

#### 设计

每个 delete 事件包含：
- `timestamp`：实际删除时间（微秒精度）
- `sequence_id`：全局序列号（保证顺序）
- `batch_id`：批次 ID（用于去重）

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "events": [
    {
      "key": "key1",
      "timestamp": 1704110400123456,  // 实际删除时间
      "sequence_id": 1001,
      "source": "eviction"
    },
    {
      "key": "key2",
      "timestamp": 1704110400123457,
      "sequence_id": 1002,
      "source": "eviction"
    }
  ]
}
```

#### 优点

- 保持事件的实际顺序
- 支持去重（通过 sequence_id）
- 支持时间戳排序

#### 缺点

- 需要维护全局序列号
- 实现复杂度稍高

### 方案 2：去重机制

#### 设计

在 Standby 端维护一个"已删除 key"的集合，用于去重：

```cpp
class DeleteEventProcessor {
private:
    std::unordered_set<std::string> deleted_keys_;
    std::mutex mutex_;
    
public:
    void ProcessDeleteEvent(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 去重：如果已经删除过，跳过
        if (deleted_keys_.find(key) != deleted_keys_.end()) {
            VLOG(1) << "Key " << key << " already deleted, skipping";
            return;
        }
        
        // 执行删除
        DeleteKey(key);
        deleted_keys_.insert(key);
        
        // 定期清理 deleted_keys_（避免内存泄漏）
        if (deleted_keys_.size() > 100000) {
            CleanupDeletedKeys();
        }
    }
};
```

#### 优点

- 简单易实现
- 有效防止重复删除
- 性能开销小

#### 缺点

- 需要维护内存中的集合
- 需要定期清理（避免内存泄漏）

### 方案 3：版本号机制

#### 设计

每个 delete 事件包含版本号，Standby 只处理版本号更高的删除事件：

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "version": 100,  // 全局版本号
  "events": [
    {
      "key": "key1",
      "key_version": 50,  // key 的版本号
      "timestamp": 1704110400123456
    }
  ]
}
```

#### 优点

- 支持版本比较
- 可以检测过期事件

#### 缺点

- 需要维护版本号
- 实现复杂度高

### 方案 4：分离显式删除和批量删除

#### 设计

使用不同的 etcd key 前缀区分显式删除和批量删除：

```
mooncake-store/deletes/explicit/{key_hash}  # 显式删除
mooncake-store/deletes/batch/{batch_id}     # 批量删除
```

Standby 处理逻辑：
1. 先处理显式删除（优先级高）
2. 再处理批量删除（去重）

#### 优点

- 清晰区分两种删除类型
- 可以设置不同的优先级

#### 缺点

- 需要维护两套逻辑
- 可能增加 etcd key 数量

### 方案 5：事务保证原子性

#### 设计

使用 etcd 事务保证批量写入的原子性：

```cpp
void BatchedDeleteEventManager::FlushBatch() {
    // 构建事务
    etcd::Transaction txn;
    
    for (const auto& key : pending_keys_) {
        std::string etcd_key = BuildDeleteKey(key);
        txn.Put(etcd_key, SerializeDeleteEvent(key));
    }
    
    // 提交事务（原子性保证）
    auto result = etcd_client_.Commit(txn);
    if (!result.success) {
        LOG(ERROR) << "Failed to commit batch delete events";
        // 重试或持久化到缓冲区
    }
}
```

#### 优点

- 保证批量写入的原子性
- 要么全部成功，要么全部失败

#### 缺点

- etcd 事务有性能开销
- 如果批量很大，事务可能失败

## 推荐方案：组合方案

### 核心设计

1. **时间戳 + 序列号**：每个事件包含实际删除时间和序列号
2. **去重机制**：Standby 端维护已删除 key 集合
3. **分离显式删除和批量删除**：使用不同的 etcd key 前缀
4. **持久化缓冲区**：使用 DragonflyDB 作为缓冲区，避免数据丢失

### 实现示例

#### Primary 端

```cpp
class BatchedDeleteEventManager {
private:
    uint64_t global_sequence_id_{0};
    std::mutex sequence_mutex_;
    
    struct DeleteEvent {
        std::string key;
        uint64_t timestamp;      // 实际删除时间
        uint64_t sequence_id;     // 全局序列号
        std::string source;        // "explicit" or "eviction"
    };
    
    void FlushBatch() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (pending_events_.empty()) {
            return;
        }
        
        // 分配序列号
        uint64_t batch_start_seq = GetNextSequenceId(pending_events_.size());
        
        // 构建批量事件
        BatchDeleteEvent batch;
        batch.batch_id = GenerateBatchId();
        batch.version = batch_start_seq;
        
        for (size_t i = 0; i < pending_events_.size(); ++i) {
            auto& event = pending_events_[i];
            event.sequence_id = batch_start_seq + i;
            batch.events.push_back(event);
        }
        
        // 写入 etcd
        WriteBatchToEtcd(batch);
        
        pending_events_.clear();
    }
    
    uint64_t GetNextSequenceId(size_t count) {
        std::lock_guard<std::mutex> lock(sequence_mutex_);
        uint64_t start = global_sequence_id_;
        global_sequence_id_ += count;
        return start;
    }
};
```

#### Standby 端

```cpp
class DeleteEventProcessor {
private:
    std::unordered_map<std::string, uint64_t> deleted_keys_;  // key -> max_sequence_id
    std::mutex mutex_;
    
public:
    void ProcessBatchDeleteEvent(const BatchDeleteEvent& batch) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        for (const auto& event : batch.events) {
            // 去重：如果已经删除过，且序列号更小，跳过
            auto it = deleted_keys_.find(event.key);
            if (it != deleted_keys_.end() && it->second >= event.sequence_id) {
                VLOG(1) << "Key " << event.key 
                       << " already deleted with sequence_id=" << it->second
                       << ", skipping sequence_id=" << event.sequence_id;
                continue;
            }
            
            // 执行删除
            DeleteKey(event.key);
            deleted_keys_[event.key] = event.sequence_id;
        }
        
        // 定期清理（保留最近 100000 个 key）
        if (deleted_keys_.size() > 100000) {
            CleanupOldKeys();
        }
    }
    
    void ProcessExplicitDeleteEvent(const std::string& key, uint64_t sequence_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 显式删除优先级更高，直接删除
        DeleteKey(key);
        deleted_keys_[key] = sequence_id;
    }
};
```

## 时序问题总结

### 主要问题

1. **事件顺序混乱**：批量写入可能打乱事件的实际顺序
   - **解决方案**：使用时间戳 + 序列号

2. **重复删除**：同一个 key 可能出现在多个批次中
   - **解决方案**：Standby 端去重机制

3. **延迟不一致**：批量延迟可能导致 Standby 看到过期数据
   - **解决方案**：这是批量方案的固有特性，需要权衡

4. **数据丢失**：Primary 崩溃可能导致未写入的事件丢失
   - **解决方案**：持久化缓冲区（DragonflyDB）

### 推荐方案

**组合方案**：
1. 时间戳 + 序列号（保证顺序）
2. Standby 端去重（防止重复删除）
3. 分离显式删除和批量删除（优先级区分）
4. 持久化缓冲区（避免数据丢失）

这样可以最大程度地减少时序问题，同时保持批量写入的性能优势。

