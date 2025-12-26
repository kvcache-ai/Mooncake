# OpLog 序列号乱序时的回滚和重放方案

## 问题描述

当 Standby 检测到某个 key 的 `key_sequence_id` 乱序时（例如：收到了 `key_sequence_id=5`，但之前已经处理了 `key_sequence_id=6`），说明该 key 的 metadata 可能已经不一致。

### 问题场景

```
时间线：
1. Standby 收到 OpLog: sequence_id=100, key="obj1", key_sequence_id=5, op_type=PUT_END
2. Standby 应用成功，metadata 中 obj1 的 key_sequence_id = 5
3. Standby 收到 OpLog: sequence_id=102, key="obj1", key_sequence_id=6, op_type=PUT_END
4. Standby 应用成功，metadata 中 obj1 的 key_sequence_id = 6
5. Standby 收到 OpLog: sequence_id=101, key="obj1", key_sequence_id=5, op_type=REMOVE
   ❌ 乱序！key_sequence_id=5 < 当前值 6
```

**问题**：
- 该 key 的 metadata 可能已经不一致
- 需要修复该 key 的数据状态

## 解决方案

### 核心思路

**对于乱序的 key，执行回滚和重放**：
1. **回滚**：从 metadata_store 中删除该 key 的所有状态
2. **重放**：从该 key 第一次出现的 sequence_id 开始，从 etcd 重新读取所有 OpLog
3. **重写**：按正确顺序重新应用所有 OpLog，重建 metadata

### 架构设计

```
┌─────────────────────────────────────────────────────────┐
│         OpLogApplier::ApplyOpLogEntry()                 │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  检查 key_sequence_id 是否递增                          │
│  - 如果乱序 → 触发回滚和重放                            │
└─────────────────────────────────────────────────────────┘
         │
         ├─ 正常顺序
         │   │
         │   ▼
         │   ┌─────────────────────────────────────────┐
         │   │  正常应用 OpLog                          │
         │   └─────────────────────────────────────────┘
         │
         └─ 乱序
             │
             ▼
┌─────────────────────────────────────────────────────────┐
│  1. 回滚：删除该 key 的 metadata                       │
│     - metadata_store_->RemoveKey(key)                  │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  2. 从 etcd 重新读取该 key 的所有 OpLog                │
│     - ReadOpLogForKey(key, first_seq_id)               │
│     - 过滤出该 key 的条目                               │
│     - 按 sequence_id 排序                               │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  3. 按顺序重新应用所有 OpLog                           │
│     - 跳过时序检查（因为已经排序）                      │
│     - 重新构建 metadata                                 │
└─────────────────────────────────────────────────────────┘
```

## 实现设计

### 1. 方案 A：基于 etcd 的完整重放（推荐）

**优点**：
- 数据准确：从 etcd 读取保证数据正确
- 实现简单：不需要维护操作历史
- 内存友好：不需要额外存储
- 容错性好：即使本地状态丢失也能恢复

**缺点**：
- 需要从 etcd 读取：可能有网络 I/O 开销
- 可能较慢：如果该 key 的操作很多

#### 实现代码

```cpp
class OpLogApplier {
private:
    // 记录每个 key 的首次 sequence_id（用于回滚）
    std::unordered_map<std::string, uint64_t> key_first_sequence_id_;
    std::mutex key_first_sequence_mutex_;
    
    // 记录正在回滚的 key（防止并发回滚）
    std::set<std::string> keys_under_rollback_;
    std::mutex rollback_mutex_;
    
    EtcdOpLogStore* etcd_oplog_store_;

public:
    bool ApplyOpLogEntry(const OpLogEntry& entry) {
        // 1. 记录首次 sequence_id
        {
            std::lock_guard<std::mutex> lock(key_first_sequence_mutex_);
            if (key_first_sequence_id_.count(entry.object_key) == 0) {
                key_first_sequence_id_[entry.object_key] = entry.sequence_id;
            }
        }
        
        // 2. 检查 key 级别的时序性
        if (!CheckSequenceOrder(entry)) {
            LOG(ERROR) << "Key-level sequence order violation for key: "
                      << entry.object_key
                      << ", entry_seq=" << entry.key_sequence_id
                      << ", current_seq=" << GetKeySequenceId(entry.object_key);
            
            // 3. 触发回滚和重放（异步执行，不阻塞）
            std::thread([this, key = entry.object_key]() {
                RollbackAndReplayKey(key);
            }).detach();
            
            // 暂时跳过这个条目，等待回滚完成
            return false;
        }
        
        // 4. 正常应用
        switch (entry.op_type) {
            case OpType::PUT_END:
                ApplyPutEnd(entry);
                break;
            case OpType::PUT_REVOKE:
                ApplyPutRevoke(entry);
                break;
            case OpType::REMOVE:
                ApplyRemove(entry);
                break;
        }
        
        // 5. 更新 key_sequence_map_
        {
            std::lock_guard<std::mutex> lock(key_sequence_mutex_);
            key_sequence_map_[entry.object_key] = entry.key_sequence_id;
        }
        
        return true;
    }

private:
    bool RollbackAndReplayKey(const std::string& key) {
        // 1. 检查是否正在回滚（防止并发回滚）
        {
            std::lock_guard<std::mutex> lock(rollback_mutex_);
            if (keys_under_rollback_.count(key) > 0) {
                LOG(WARNING) << "Key is already under rollback: " << key;
                return false;
            }
            keys_under_rollback_.insert(key);
        }
        
        // 2. 获取该 key 的首次 sequence_id
        uint64_t first_seq_id;
        {
            std::lock_guard<std::mutex> lock(key_first_sequence_mutex_);
            auto it = key_first_sequence_id_.find(key);
            if (it == key_first_sequence_id_.end()) {
                LOG(ERROR) << "Cannot find first sequence_id for key: " << key;
                std::lock_guard<std::mutex> lock2(rollback_mutex_);
                keys_under_rollback_.erase(key);
                return false;
            }
            first_seq_id = it->second;
        }
        
        // 3. 回滚：从 metadata_store_ 中删除该 key
        LOG(INFO) << "Rolling back key: " << key 
                  << ", removing from metadata_store";
        metadata_store_->RemoveKey(key);
        
        // 4. 从 etcd 重新读取该 key 的所有 OpLog
        LOG(INFO) << "Re-reading OpLog for key: " << key
                  << " from sequence_id: " << first_seq_id;
        
        std::vector<OpLogEntry> key_entries;
        if (!ReadOpLogForKey(key, first_seq_id, key_entries)) {
            LOG(ERROR) << "Failed to read OpLog for key: " << key;
            std::lock_guard<std::mutex> lock(rollback_mutex_);
            keys_under_rollback_.erase(key);
            return false;
        }
        
        // 5. 按顺序重新应用所有 OpLog
        LOG(INFO) << "Replaying " << key_entries.size() 
                  << " OpLog entries for key: " << key;
        
        for (const auto& entry : key_entries) {
            // 重新应用（跳过时序检查，因为我们已经从 etcd 读取了正确的顺序）
            switch (entry.op_type) {
                case OpType::PUT_END:
                    ApplyPutEnd(entry);
                    break;
                case OpType::PUT_REVOKE:
                    ApplyPutRevoke(entry);
                    break;
                case OpType::REMOVE:
                    ApplyRemove(entry);
                    break;
            }
            
            // 更新 key_sequence_map_
            {
                std::lock_guard<std::mutex> lock(key_sequence_mutex_);
                key_sequence_map_[key] = entry.key_sequence_id;
            }
        }
        
        // 6. 清除回滚标记
        {
            std::lock_guard<std::mutex> lock(rollback_mutex_);
            keys_under_rollback_.erase(key);
        }
        
        LOG(INFO) << "Successfully replayed OpLog for key: " << key;
        return true;
    }

    bool ReadOpLogForKey(const std::string& key, 
                        uint64_t start_seq_id,
                        std::vector<OpLogEntry>& entries) {
        // 从 etcd 读取从 start_seq_id 开始的所有 OpLog
        std::vector<OpLogEntry> all_entries;
        const uint32_t batch_size = 10000;  // 批量读取
        
        uint64_t current_seq_id = start_seq_id;
        while (true) {
            std::vector<OpLogEntry> batch;
            if (!etcd_oplog_store_->ReadOpLogSince(current_seq_id, batch_size, batch)) {
                LOG(ERROR) << "Failed to read OpLog from etcd";
                return false;
            }
            
            if (batch.empty()) {
                break;  // 没有更多条目
            }
            
            // 过滤出该 key 的条目
            for (const auto& entry : batch) {
                if (entry.object_key == key) {
                    entries.push_back(entry);
                }
            }
            
            // 更新 current_seq_id
            if (batch.size() < batch_size) {
                break;  // 已读取完所有条目
            }
            current_seq_id = batch.back().sequence_id + 1;
        }
        
        // 按 sequence_id 排序（确保顺序正确）
        std::sort(entries.begin(), entries.end(),
                 [](const OpLogEntry& a, const OpLogEntry& b) {
                     return a.sequence_id < b.sequence_id;
                 });
        
        return true;
    }
};
```

### 2. 方案 B：基于操作历史的回滚（可选）

**优点**：
- 快速：不需要网络 I/O
- 高效：直接从内存读取

**缺点**：
- 需要额外内存：存储操作历史
- 实现复杂：需要维护历史记录
- 容错性差：如果历史丢失，无法恢复

#### 实现代码

```cpp
class OpLogApplier {
private:
    // 记录每个 key 的操作历史（用于回滚）
    struct KeyOperationHistory {
        std::vector<OpLogEntry> operations;  // 按顺序记录的操作
        uint64_t first_sequence_id{0};
    };
    std::unordered_map<std::string, KeyOperationHistory> key_history_;
    std::mutex key_history_mutex_;
    
    // 限制历史记录的大小（避免内存无限增长）
    static constexpr size_t kMaxHistorySize = 1000;

public:
    bool ApplyOpLogEntry(const OpLogEntry& entry) {
        // 1. 记录操作历史
        {
            std::lock_guard<std::mutex> lock(key_history_mutex_);
            auto& history = key_history_[entry.object_key];
            if (history.operations.empty()) {
                history.first_sequence_id = entry.sequence_id;
            }
            
            // 限制历史记录大小
            if (history.operations.size() < kMaxHistorySize) {
                history.operations.push_back(entry);
            } else {
                // 如果超过限制，只保留最近的操作
                history.operations.erase(history.operations.begin());
                history.operations.push_back(entry);
            }
        }
        
        // 2. 检查时序性
        if (!CheckSequenceOrder(entry)) {
            return RollbackAndReplayKey(entry.object_key);
        }
        
        // 3. 正常应用
        // ...
    }

private:
    bool RollbackAndReplayKey(const std::string& key) {
        std::lock_guard<std::mutex> lock(key_history_mutex_);
        
        auto it = key_history_.find(key);
        if (it == key_history_.end()) {
            LOG(ERROR) << "Cannot find history for key: " << key;
            return false;
        }
        
        // 1. 回滚：删除该 key 的 metadata
        metadata_store_->RemoveKey(key);
        
        // 2. 重新应用所有操作（从历史记录中）
        for (const auto& entry : it->second.operations) {
            // 重新应用
            switch (entry.op_type) {
                case OpType::PUT_END:
                    ApplyPutEnd(entry);
                    break;
                case OpType::PUT_REVOKE:
                    ApplyPutRevoke(entry);
                    break;
                case OpType::REMOVE:
                    ApplyRemove(entry);
                    break;
            }
            
            // 更新 key_sequence_map_
            {
                std::lock_guard<std::mutex> lock2(key_sequence_mutex_);
                key_sequence_map_[key] = entry.key_sequence_id;
            }
        }
        
        return true;
    }
};
```

## 关键设计点

### 1. 回滚起点的确定

**方案 A（推荐）**：
- 维护 `key_first_sequence_id_` 记录每个 key 第一次出现的 sequence_id
- 从该 sequence_id 开始重新读取所有 OpLog

**方案 B**：
- 维护操作历史，从历史记录中获取所有操作

### 2. 并发处理

**问题**：回滚期间，如果收到新的 OpLog 怎么办？

**解决方案**：
- 使用 `keys_under_rollback_` 标记正在回滚的 key
- 回滚期间，新的 OpLog 暂时跳过（返回 false）
- 回滚完成后，新的 OpLog 可以正常处理

```cpp
bool ApplyOpLogEntry(const OpLogEntry& entry) {
    // 检查是否正在回滚
    {
        std::lock_guard<std::mutex> lock(rollback_mutex_);
        if (keys_under_rollback_.count(entry.object_key) > 0) {
            LOG(WARNING) << "Key is under rollback, skipping entry: "
                        << entry.sequence_id;
            return false;  // 暂时跳过，等待回滚完成
        }
    }
    
    // 正常处理
    // ...
}
```

### 3. 性能优化

#### 3.1 异步回滚

**问题**：回滚和重放可能耗时，会阻塞新 OpLog 的处理

**解决方案**：异步执行回滚，不阻塞正常处理

```cpp
if (!CheckSequenceOrder(entry)) {
    // 异步回滚（不阻塞）
    std::thread([this, key = entry.object_key]() {
        RollbackAndReplayKey(key);
    }).detach();
    
    return false;  // 暂时跳过
}
```

#### 3.2 批量读取

**问题**：从 etcd 读取大量 OpLog 可能较慢

**解决方案**：批量读取，减少网络往返

```cpp
bool ReadOpLogForKey(const std::string& key, 
                    uint64_t start_seq_id,
                    std::vector<OpLogEntry>& entries) {
    const uint32_t batch_size = 10000;  // 批量读取
    uint64_t current_seq_id = start_seq_id;
    
    while (true) {
        std::vector<OpLogEntry> batch;
        etcd_oplog_store_->ReadOpLogSince(current_seq_id, batch_size, batch);
        // ...
    }
}
```

#### 3.3 限制回滚范围

**问题**：如果该 key 的操作非常多，回滚可能很耗时

**解决方案**：限制回滚范围，只回滚最近的操作

```cpp
bool RollbackAndReplayKey(const std::string& key) {
    // 只回滚最近 N 个操作
    const uint64_t max_rollback_ops = 1000;
    
    // 从 etcd 读取时，限制范围
    uint64_t start_seq_id = std::max(
        first_seq_id,
        GetLatestSequenceId() - max_rollback_ops
    );
    
    // ...
}
```

### 4. 错误处理

#### 4.1 回滚失败

**场景**：从 etcd 读取 OpLog 失败

**处理**：
- 记录错误日志
- 清除回滚标记
- 可以考虑触发全量同步

```cpp
if (!ReadOpLogForKey(key, first_seq_id, key_entries)) {
    LOG(ERROR) << "Failed to read OpLog for key: " << key;
    
    // 清除回滚标记
    {
        std::lock_guard<std::mutex> lock(rollback_mutex_);
        keys_under_rollback_.erase(key);
    }
    
    // 可选：触发全量同步
    // TriggerFullSync();
    
    return false;
}
```

#### 4.2 重复回滚

**场景**：同一个 key 多次触发回滚

**处理**：
- 使用 `keys_under_rollback_` 防止并发回滚
- 如果正在回滚，跳过新的回滚请求

### 5. 监控和告警

#### 5.1 记录乱序频率

```cpp
class OpLogApplier {
private:
    // 记录每个 key 的乱序次数
    std::unordered_map<std::string, uint64_t> key_violation_count_;
    std::mutex violation_count_mutex_;
    
    // 乱序阈值
    static constexpr uint64_t kMaxViolationsPerKey = 10;

public:
    bool ApplyOpLogEntry(const OpLogEntry& entry) {
        if (!CheckSequenceOrder(entry)) {
            // 记录乱序次数
            {
                std::lock_guard<std::mutex> lock(violation_count_mutex_);
                key_violation_count_[entry.object_key]++;
                
                if (key_violation_count_[entry.object_key] > kMaxViolationsPerKey) {
                    LOG(ERROR) << "Too many violations for key: " 
                              << entry.object_key
                              << ", count: " 
                              << key_violation_count_[entry.object_key];
                    
                    // 触发全量同步
                    TriggerFullSync();
                    return false;
                }
            }
            
            // 触发回滚
            // ...
        }
    }
};
```

#### 5.2 性能指标

- 回滚次数
- 回滚耗时
- 回滚成功率
- 乱序频率

## 方案对比

| 特性 | 方案 A（基于 etcd） | 方案 B（基于历史） |
|------|-------------------|------------------|
| **数据准确性** | 高（从 etcd 读取） | 中（依赖历史记录） |
| **实现复杂度** | 低 | 高 |
| **内存开销** | 低 | 高（需要存储历史） |
| **性能** | 中（需要网络 I/O） | 高（内存操作） |
| **容错性** | 高（可以从 etcd 恢复） | 低（历史可能丢失） |
| **适用场景** | 乱序不频繁、数据准确性要求高 | 乱序频繁、性能要求高 |

## 推荐方案

**推荐使用方案 A（基于 etcd 的完整重放）**，原因：

1. **数据准确性高**：从 etcd 读取保证数据正确
2. **实现简单**：不需要维护操作历史
3. **内存友好**：不需要额外存储
4. **容错性好**：即使本地状态丢失也能恢复

**优化建议**：
1. **异步回滚**：不阻塞正常处理
2. **批量读取**：减少网络往返
3. **限制范围**：只回滚最近的操作
4. **监控告警**：记录乱序频率，超过阈值时触发全量同步

## 测试场景

### 1. 正常乱序检测和回滚

```
1. Standby 收到 OpLog: sequence_id=100, key="obj1", key_sequence_id=5
2. 应用成功
3. Standby 收到 OpLog: sequence_id=102, key="obj1", key_sequence_id=6
4. 应用成功
5. Standby 收到 OpLog: sequence_id=101, key="obj1", key_sequence_id=5
6. 检测到乱序，触发回滚
7. 从 etcd 重新读取 obj1 的所有 OpLog
8. 按顺序重新应用
9. 验证 metadata 正确
```

### 2. 并发回滚保护

```
1. 检测到 key="obj1" 乱序，开始回滚
2. 回滚期间，收到新的 OpLog: key="obj1"
3. 检测到正在回滚，跳过新 OpLog
4. 回滚完成后，新的 OpLog 可以正常处理
```

### 3. 回滚失败处理

```
1. 检测到乱序，触发回滚
2. 从 etcd 读取 OpLog 失败
3. 记录错误日志
4. 清除回滚标记
5. 可选：触发全量同步
```

### 4. 频繁乱序处理

```
1. 某个 key 频繁乱序（超过阈值）
2. 记录告警
3. 触发全量同步
4. 避免频繁回滚影响性能
```

## 总结

### 核心方案

**对于乱序的 key，执行回滚和重放**：
1. 回滚：删除该 key 的 metadata
2. 重放：从 etcd 重新读取该 key 的所有 OpLog
3. 重写：按正确顺序重新应用所有 OpLog

### 关键实现

1. **回滚起点**：维护 `key_first_sequence_id_` 记录首次 sequence_id
2. **并发保护**：使用 `keys_under_rollback_` 防止并发回滚
3. **异步执行**：回滚在后台线程执行，不阻塞正常处理
4. **批量读取**：从 etcd 批量读取 OpLog，减少网络往返
5. **监控告警**：记录乱序频率，超过阈值时触发全量同步

### 优势

1. **数据准确性**：从 etcd 读取保证数据正确
2. **局部修复**：只影响乱序的 key，不影响其他 key
3. **自动恢复**：自动检测和修复数据不一致
4. **性能友好**：异步执行，不阻塞正常处理

### 注意事项

1. **性能影响**：回滚和重放可能耗时，需要异步执行
2. **并发处理**：回滚期间需要防止并发处理该 key
3. **范围限制**：可以限制回滚范围，只回滚最近的操作
4. **监控告警**：需要监控乱序频率，超过阈值时考虑全量同步

