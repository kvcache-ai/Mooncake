# 基于 etcd 的 Delete 事件同步方案

## 方案概述

将 Delete 事件写入 etcd，利用 etcd 的强一致性和 watch 机制，确保所有 Standby Master 都能看到 Delete 事件，即使 Primary Master 崩溃。

## 方案设计

### 1. etcd Key 结构设计

```
{etcd_prefix}/deletes/{cluster_id}/{key_hash}
```

示例：
```
mooncake-store/deletes/mooncake_cluster/abc123def456
```

**设计考虑**：
- 使用 `key_hash` 而不是原始 key，避免 etcd key 过长
- 使用 `cluster_id` 支持多集群隔离
- 使用统一的 `deletes` 前缀，便于批量管理

### 2. Delete 事件数据结构

```cpp
struct DeleteEvent {
    std::string key;              // 原始 key
    uint64_t timestamp;           // 删除时间戳
    ViewVersionId master_version; // Master view version（用于去重）
    std::string master_address;   // 执行删除的 Master 地址
};
```

序列化为 JSON 存储在 etcd value 中。

### 3. Primary Master：写入 Delete 事件

```cpp
auto MasterService::Remove(const std::string& key) 
    -> tl::expected<void, ErrorCode> {
    // 1. 执行本地删除
    auto result = RemoveLocal(key);
    if (!result) {
        return result;
    }
    
    // 2. 写入 Delete 事件到 etcd
    if (enable_ha_) {
        DeleteEvent event;
        event.key = key;
        event.timestamp = NowInMicroseconds();
        event.master_version = current_view_version_;
        event.master_address = local_address_;
        
        std::string etcd_key = BuildDeleteKey(key);
        std::string etcd_value = SerializeDeleteEvent(event);
        
        auto etcd_result = EtcdHelper::Put(etcd_key, etcd_value);
        if (etcd_result != ErrorCode::OK) {
            LOG(WARNING) << "Failed to write delete event to etcd: " 
                        << etcd_result
                        << ", but local delete succeeded";
            // 继续执行，不阻塞删除操作
        }
    }
    
    return {};
}
```

### 4. Standby Master：Watch Delete 事件

```cpp
class DeleteEventWatcher {
public:
    void StartWatching() {
        watch_thread_ = std::thread([this]() {
            WatchDeleteEvents();
        });
    }
    
private:
    void WatchDeleteEvents() {
        std::string watch_prefix = etcd_prefix_ + "/deletes/" + cluster_id_ + "/";
        
        // 使用 etcd watch 监听所有 delete 事件
        while (running_) {
            auto watch_result = EtcdHelper::WatchPrefix(watch_prefix);
            
            for (const auto& event : watch_result.events) {
                if (event.type == EventType::PUT) {
                    // 新的 Delete 事件
                    ProcessDeleteEvent(event.key, event.value);
                } else if (event.type == EventType::DELETE) {
                    // Delete 事件被清理（过期）
                    // 可以忽略
                }
            }
        }
    }
    
    void ProcessDeleteEvent(const std::string& etcd_key, 
                           const std::string& etcd_value) {
        // 1. 解析 Delete 事件
        DeleteEvent event = DeserializeDeleteEvent(etcd_value);
        
        // 2. 检查是否已经处理过（去重）
        if (processed_deletes_.count(event.key) > 0) {
            return;  // 已处理，跳过
        }
        
        // 3. 更新本地 metadata
        if (hot_standby_service_) {
            hot_standby_service_->ApplyDelete(event.key);
        }
        
        // 4. 标记为已处理
        processed_deletes_.insert(event.key);
    }
};
```

### 5. 事件清理机制

为了避免 etcd 中积累大量 Delete 事件，需要定期清理：

```cpp
class DeleteEventCleaner {
public:
    void StartCleaning() {
        cleaner_thread_ = std::thread([this]() {
            while (running_) {
                CleanOldDeleteEvents();
                std::this_thread::sleep_for(
                    std::chrono::minutes(cleanup_interval_minutes_));
            }
        });
    }
    
private:
    void CleanOldDeleteEvents() {
        std::string prefix = etcd_prefix_ + "/deletes/" + cluster_id_ + "/";
        
        // 获取所有 Delete 事件
        auto all_events = EtcdHelper::List(prefix);
        
        auto now = NowInMicroseconds();
        for (const auto& event : all_events) {
            DeleteEvent delete_event = DeserializeDeleteEvent(event.value);
            
            // 如果事件超过保留时间（如 1 小时），删除
            if (now - delete_event.timestamp > 
                kDeleteEventRetentionTimeUs) {
                EtcdHelper::Delete(event.key);
            }
        }
    }
};
```

---

## 方案优势

### 1. ✅ 利用现有基础设施

- etcd 已经在使用（用于 Leader 选举）
- 不需要引入新的消息队列组件
- 复用现有的 `EtcdHelper` 接口

### 2. ✅ 强一致性保证

- etcd 提供强一致性保证
- 所有 Standby Master 都能看到相同的 Delete 事件
- 即使 Primary 崩溃，事件仍然在 etcd 中

### 3. ✅ 实时同步

- etcd watch 机制可以实时推送 Delete 事件
- Standby Master 可以立即响应 Delete 事件
- 延迟通常在毫秒级

### 4. ✅ 持久化存储

- etcd 持久化存储，即使所有 Master 重启，事件仍然存在
- 新启动的 Master 可以从 etcd 恢复历史 Delete 事件

---

## 潜在问题和解决方案

### 问题 1：etcd 性能和容量限制

**问题描述**：
- etcd 不适合存储大量数据
- 大量 Delete 事件可能导致 etcd 性能下降
- etcd 有存储容量限制（默认 2GB）

**解决方案**：

#### 方案 A：批量写入 + 定期清理

```cpp
// 批量收集 Delete 事件
class DeleteEventBuffer {
    std::vector<DeleteEvent> buffer_;
    std::mutex mutex_;
    
    void Flush() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (buffer_.empty()) return;
        
        // 批量写入 etcd（使用事务）
        EtcdHelper::BatchPut(delete_events_);
        buffer_.clear();
    }
};
```

- 批量写入减少 etcd 压力
- 定期清理旧事件，控制 etcd 存储量

#### 方案 B：只存储关键 Delete 事件

```cpp
// 只存储"高风险"的 Delete 事件
bool ShouldStoreDeleteEvent(const std::string& key) {
    // 只存储：
    // 1. 最近活跃的 key（在 LRU 缓存中）
    // 2. 有特殊标记的 key
    // 3. 大对象的 key
    return IsRecentlyActive(key) || HasSpecialFlag(key) || IsLargeObject(key);
}
```

- 只存储可能被重用的 key 的 Delete 事件
- 普通 key 的 Delete 事件可以丢失（符合你的语义）

#### 方案 C：使用 etcd 的 TTL 自动过期

```cpp
// 写入 Delete 事件时设置 TTL
EtcdHelper::PutWithTTL(etcd_key, etcd_value, 
                      kDeleteEventTTLSeconds);  // 如 60 秒
```

- 利用 etcd 的 TTL 机制自动清理
- 不需要额外的清理线程

### 问题 2：etcd Watch 延迟

**问题描述**：
- etcd watch 可能有延迟（网络、负载等）
- 在 watch 延迟期间，可能错过 Delete 事件

**解决方案**：

#### 方案 A：Watch + 定期全量同步

```cpp
void SyncDeleteEvents() {
    // 1. Watch 实时事件
    StartWatching();
    
    // 2. 定期全量同步（作为兜底）
    sync_thread_ = std::thread([this]() {
        while (running_) {
            FullSyncDeleteEvents();
            std::this_thread::sleep_for(
                std::chrono::seconds(sync_interval_seconds_));
        }
    });
}

void FullSyncDeleteEvents() {
    // 获取 etcd 中所有 Delete 事件
    auto all_events = EtcdHelper::List(delete_prefix_);
    
    // 与本地 metadata 对比，补漏
    for (const auto& event : all_events) {
        if (!IsDeletedLocally(event.key)) {
            ProcessDeleteEvent(event.key, event.value);
        }
    }
}
```

#### 方案 B：使用 etcd 的 Revision 机制

```cpp
// 记录最后处理的 revision
int64_t last_processed_revision_ = 0;

void WatchDeleteEvents() {
    // 从上次的 revision 开始 watch
    auto watch_result = EtcdHelper::WatchFromRevision(
        delete_prefix_, last_processed_revision_);
    
    // 处理所有事件（包括历史事件）
    for (const auto& event : watch_result.events) {
        ProcessDeleteEvent(event);
        last_processed_revision_ = event.revision;
    }
}
```

### 问题 3：etcd 故障处理

**问题描述**：
- etcd 故障时，无法写入/读取 Delete 事件
- 需要降级策略

**解决方案**：

#### 方案 A：优雅降级

```cpp
auto MasterService::Remove(const std::string& key) 
    -> tl::expected<void, ErrorCode> {
    // 1. 执行本地删除（必须成功）
    auto result = RemoveLocal(key);
    if (!result) {
        return result;
    }
    
    // 2. 尝试写入 etcd（可选）
    if (enable_ha_ && etcd_available_) {
        auto etcd_result = WriteDeleteEventToEtcd(key);
        if (etcd_result != ErrorCode::OK) {
            LOG(WARNING) << "etcd unavailable, delete event not synced";
            // 继续执行，不阻塞
        }
    }
    
    return {};
}
```

- etcd 故障时，Delete 操作仍然成功
- 只是 Delete 事件可能丢失（符合你的语义）

#### 方案 B：重试机制

```cpp
void WriteDeleteEventWithRetry(const std::string& key) {
    int retries = 3;
    while (retries > 0) {
        auto result = EtcdHelper::Put(delete_key, delete_value);
        if (result == ErrorCode::OK) {
            return;
        }
        
        retries--;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(100 * (4 - retries)));
    }
    
    LOG(WARNING) << "Failed to write delete event after retries";
}
```

---

## 实现建议

### 阶段 1：基础实现

1. **实现 Delete 事件写入**：
   - 在 `MasterService::Remove` 中写入 etcd
   - 使用简单的 key-value 结构

2. **实现 Delete 事件 Watch**：
   - Standby Master 启动 watch 线程
   - 处理 Delete 事件，更新本地 metadata

3. **实现事件清理**：
   - 使用 TTL 或定期清理

### 阶段 2：优化

1. **批量写入**：减少 etcd 压力
2. **选择性存储**：只存储关键 Delete 事件
3. **全量同步**：作为 watch 的兜底

### 阶段 3：生产就绪

1. **监控和告警**：监控 etcd 性能和容量
2. **故障处理**：完善的降级策略
3. **性能测试**：验证大量 Delete 事件的性能

---

## 与现有方案的对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| **etcd Delete 事件** | ✅ 利用现有基础设施<br>✅ 强一致性<br>✅ 实时同步 | ⚠️ etcd 性能限制<br>⚠️ 需要清理机制 |
| **延迟物理删除** | ✅ 实现简单<br>✅ 不依赖外部组件 | ❌ 内存浪费 |
| **消息队列（EDQ/Kafka）** | ✅ 高性能<br>✅ 大容量 | ❌ 需要新组件<br>❌ 增加系统复杂度 |
| **Raft 协议** | ✅ 完全强一致 | ❌ 实现复杂<br>❌ 性能开销大 |

---

## 总结

**将 Delete 事件写入 etcd 的方案是可行的**，但需要注意：

1. **etcd 性能限制**：
   - 需要批量写入和定期清理
   - 或者只存储关键 Delete 事件

2. **Watch 延迟**：
   - 需要定期全量同步作为兜底
   - 或使用 revision 机制

3. **故障处理**：
   - 需要优雅降级策略
   - etcd 故障时，Delete 操作仍然成功

**推荐实现方式**：
- **基础版本**：写入所有 Delete 事件 + TTL 自动清理
- **优化版本**：只存储关键 Delete 事件 + 批量写入 + 定期全量同步

这个方案在**利用现有基础设施**和**解决 Delete 未同步问题**之间取得了很好的平衡。

