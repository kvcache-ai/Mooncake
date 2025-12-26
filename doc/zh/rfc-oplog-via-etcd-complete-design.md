# 基于 etcd 的 OpLog 主备同步完整方案

## 方案概述

使用 etcd 作为中间可靠性组件，实现 Primary Master 和 Standby Master 之间的 OpLog 同步。OpLog 只记录 PUT 和 DELETE 事件，通过 etcd 的 Watch 机制实现实时同步。

## 核心设计原则

1. **OpLog 只记录 PUT 和 DELETE 事件**：不记录 LEASE_RENEW，减少 OpLog 大小
2. **etcd 作为中间存储**：利用 etcd 的强一致性和 Watch 机制
3. **快照集成**：与现有快照机制集成，快照后可以清理旧的 OpLog
4. **时序保证**：通过 sequence_id 和 key 级别的版本控制保证时序

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│              Primary Master                              │
│                                                          │
│  ┌──────────────┐      ┌──────────────┐                │
│  │ MasterService│      │ OpLogManager │                │
│  │              │      │              │                │
│  │ PutEnd()     │─────▶│ Append()     │                │
│  │ Remove()     │      │              │                │
│  │ Eviction     │      └──────────────┘                │
│  └──────────────┘              │                        │
│                                 │                        │
│                                 ▼                        │
│                        ┌──────────────┐                 │
│                        │ EtcdOpLogStore│                │
│                        │              │                 │
│                        │ WriteOpLog() │                 │
│                        └──────────────┘                 │
│                                 │                        │
│                                 │ 写入 etcd              │
│                                 ▼                        │
│                        ┌──────────────┐                 │
│                        │     etcd     │                 │
│                        │              │                 │
│                        │ /oplog/{seq} │                 │
│                        └──────────────┘                 │
└─────────────────────────────────────────────────────────┘
         │
         │ Watch
         ▼
┌─────────────────────────────────────────────────────────┐
│              Standby Masters                            │
│                                                          │
│  ┌──────────────────────────────────────────────────┐ │
│  │ MasterServiceSupervisor                          │ │
│  │  - 检测 leader                                   │ │
│  │  - 启动/停止 HotStandbyService                   │ │
│  └──────────────────────────────────────────────────┘ │
│         │                                              │
│         ▼                                              │
│  ┌──────────────┐      ┌──────────────┐              │
│  │ OpLogWatcher  │      │ OpLogApplier  │              │
│  │              │      │              │              │
│  │ WatchEtcd()   │─────▶│ ApplyOpLog() │              │
│  │              │      │              │              │
│  └──────────────┘      └──────────────┘              │
│         │                      │                        │
│         │                      ▼                        │
│         │              ┌──────────────┐                │
│         │              │ MetadataStore│                │
│         │              │              │                │
│         │              │ 更新 metadata │                │
│         │              └──────────────┘                │
│         │                                              │
│         └──────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────┘
```

## etcd Key 设计

### 1. OpLog Entry Key

```
mooncake-store/oplog/{cluster_id}/{sequence_id}
```

**示例**：
```
mooncake-store/oplog/mooncake_cluster/1
mooncake-store/oplog/mooncake_cluster/2
mooncake-store/oplog/mooncake_cluster/3
...
```

**设计考虑**：
- 使用 `sequence_id` 作为 key 的一部分，保证顺序
- 支持按 sequence_id 范围查询
- 易于清理（删除指定 sequence_id 之前的 key）

### 2. 最新 Sequence ID Key

```
mooncake-store/oplog/{cluster_id}/latest
```

**用途**：
- 存储当前最新的 sequence_id
- Standby 可以快速获取最新的 sequence_id
- 用于快照时记录 OpLog 的 sequence_id

### 3. 快照 Sequence ID Key

```
mooncake-store/oplog/{cluster_id}/snapshot/{snapshot_id}/sequence_id
```

**用途**：
- 记录每个快照对应的 sequence_id
- 用于确定可以清理的 OpLog 范围

## OpLog Entry 数据结构

```cpp
struct OpLogEntry {
    uint64_t sequence_id{0};     // 全局递增序列号
    uint64_t timestamp_ms{0};    // 时间戳（毫秒）
    OpType op_type{OpType::PUT_END};  // PUT_END, PUT_REVOKE, REMOVE
    std::string object_key;      // 对象 key
    std::string payload;         // 可选负载（用于 PUT_END 时携带 replica 信息）
    uint32_t checksum{0};        // 校验和
    uint32_t prefix_hash{0};     // key 前缀哈希
    uint64_t key_sequence_id{0}; // 该 key 的操作序列号（用于时序保证）
};
```

**JSON 序列化格式**：
```json
{
  "sequence_id": 12345,
  "timestamp": 1704110400123,
  "op_type": "PUT_END",
  "key": "object_key_123",
  "payload": "optional_payload",
  "checksum": 1234567890,
  "prefix_hash": 987654321,
  "key_sequence_id": 5
}
```

## Primary 端实现

### 1. EtcdOpLogStore 类

```cpp
class EtcdOpLogStore {
public:
    EtcdOpLogStore(const std::string& etcd_endpoints, 
                   const std::string& cluster_id);
    
    // 写入 OpLog 到 etcd
    bool WriteOpLog(const OpLogEntry& entry);
    
    // 批量写入 OpLog（可选优化）
    bool WriteOpLogBatch(const std::vector<OpLogEntry>& entries);
    
    // 更新最新的 sequence_id
    bool UpdateLatestSequenceId(uint64_t sequence_id);
    
    // 记录快照对应的 sequence_id
    bool RecordSnapshotSequenceId(const std::string& snapshot_id, 
                                   uint64_t sequence_id);
    
    // 清理指定 sequence_id 之前的 OpLog
    bool CleanupOpLogBefore(uint64_t sequence_id);

private:
    std::string BuildOpLogKey(uint64_t sequence_id);
    std::string SerializeOpLogEntry(const OpLogEntry& entry);
    OpLogEntry DeserializeOpLogEntry(const std::string& data);
    
    std::string etcd_prefix_;
    std::string cluster_id_;
    // etcd client
};
```

### 2. 集成到 OpLogManager

```cpp
class OpLogManager {
public:
    // 设置 EtcdOpLogStore（可选，如果不设置则只写入内存）
    void SetEtcdOpLogStore(EtcdOpLogStore* store);
    
    uint64_t Append(OpType type, const std::string& key,
                    const std::string& payload = std::string()) {
        OpLogEntry entry;
        // ... 填充 entry ...
        
        // 写入内存 buffer
        buffer_.emplace_back(entry);
        
        // 写入 etcd（如果设置了）
        if (etcd_store_) {
            etcd_store_->WriteOpLog(entry);
            etcd_store_->UpdateLatestSequenceId(entry.sequence_id);
        }
        
        return entry.sequence_id;
    }
    
private:
    EtcdOpLogStore* etcd_store_{nullptr};
    // ... 其他成员 ...
};
```

### 3. 驱逐时记录 DELETE 事件

```cpp
void MasterService::BatchEvict(...) {
    // ... 驱逐逻辑 ...
    
    if (it->second.lease_timeout <= target_timeout) {
        std::string evicted_key = it->first;
        
        // 驱逐对象
        total_freed_size += it->second.size * it->second.GetMemReplicaCount();
        it->second.EraseReplica(ReplicaType::MEMORY);
        
        if (it->second.IsValid() == false) {
            // 对象完全无效，记录 DELETE 事件
            AppendOpLogAndNotify(OpType::REMOVE, evicted_key);
            it = shard.metadata.erase(it);
        } else {
            ++it;
        }
    }
}
```

## Standby 端实现

### 0. Standby 服务集成

**问题**：现有代码中，Standby 在等待 leader 选举期间只是阻塞等待，没有运行 Standby 服务来同步 OpLog。

**解决方案**：在 Standby 模式下并行运行 Standby 服务，watch etcd OpLog 并实时恢复 metadata。

**核心流程**：
1. `MasterServiceSupervisor` 检测到有 leader 时，启动 `HotStandbyService`
2. `HotStandbyService` 启动 `OpLogWatcher` watch etcd OpLog
3. 实时应用 OpLog 到本地 metadata store
4. 选举成功后，停止 Standby 服务并提升为 Primary

**详细设计请参考**：`doc/zh/rfc-standby-service-integration.md`

### 1. OpLogWatcher 类

```cpp
class OpLogWatcher {
public:
    OpLogWatcher(const std::string& etcd_endpoints,
                 const std::string& cluster_id,
                 OpLogApplier* applier);
    
    // 启动 Watch
    void Start();
    
    // 停止 Watch
    void Stop();
    
    // 从指定 sequence_id 开始读取历史 OpLog
    bool ReadOpLogSince(uint64_t start_seq_id, 
                       std::vector<OpLogEntry>& entries);

private:
    // Watch etcd OpLog 变化
    void WatchOpLog();
    
    // 处理 Watch 事件
    void HandleWatchEvent(const WatchEvent& event);
    
    std::string etcd_prefix_;
    std::string cluster_id_;
    OpLogApplier* applier_;
    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    uint64_t last_processed_sequence_id_{0};
};
```

### 2. OpLogApplier 类（时序保证）

```cpp
class OpLogApplier {
public:
    OpLogApplier(MetadataStore* metadata_store);
    
    // 应用 OpLog Entry（带时序检查）
    bool ApplyOpLogEntry(const OpLogEntry& entry);
    
    // 获取 key 的当前 sequence_id
    uint64_t GetKeySequenceId(const std::string& key) const;
    
    // 恢复处理状态
    void Recover(uint64_t last_applied_sequence_id);

private:
    // 检查时序性
    bool CheckSequenceOrder(const OpLogEntry& entry);
    
    // 应用 PUT_END
    void ApplyPutEnd(const OpLogEntry& entry);
    
    // 应用 PUT_REVOKE
    void ApplyPutRevoke(const OpLogEntry& entry);
    
    // 应用 REMOVE
    void ApplyRemove(const OpLogEntry& entry);
    
    MetadataStore* metadata_store_;
    
    // 记录每个 key 的最后 sequence_id（用于时序检查）
    std::unordered_map<std::string, uint64_t> key_sequence_map_;
    std::mutex key_sequence_mutex_;
    
    // 记录待处理的条目（用于处理序列号不连续的情况）
    std::map<uint64_t, OpLogEntry> pending_entries_;
    uint64_t expected_sequence_id_{1};
    std::mutex pending_mutex_;
};
```

### 3. 时序保证机制

```cpp
bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // 1. 检查全局序列号连续性
    if (entry.sequence_id != expected_sequence_id_) {
        if (entry.sequence_id > expected_sequence_id_) {
            // 序列号不连续，缓存待处理
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_entries_[entry.sequence_id] = entry;
            
            // 等待一段时间，看是否有缺失的条目到达
            ScheduleWaitForMissingEntries(entry.sequence_id);
            return false;
        } else {
            // 序列号小于期望值（可能是重复或乱序）
            LOG(WARNING) << "Received out-of-order OpLog entry: "
                        << "expected=" << expected_sequence_id_
                        << ", received=" << entry.sequence_id;
            return false;
        }
    }
    
    // 2. 检查 key 级别的时序性
    if (!CheckSequenceOrder(entry)) {
        LOG(ERROR) << "Key-level sequence order violation for key: "
                   << entry.object_key
                   << ", entry_seq=" << entry.key_sequence_id
                   << ", current_seq=" << GetKeySequenceId(entry.object_key);
        
        // 触发回滚和重放（异步执行）
        // 详细设计请参考：doc/zh/rfc-oplog-rollback-replay-on-sequence-violation.md
        RollbackAndReplayKey(entry.object_key);
        return false;
    }
    
    // 3. 应用 OpLog
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
        default:
            LOG(WARNING) << "Unknown OpType: " 
                        << static_cast<int>(entry.op_type);
            return false;
    }
    
    // 4. 更新状态
    {
        std::lock_guard<std::mutex> lock(key_sequence_mutex_);
        key_sequence_map_[entry.object_key] = entry.key_sequence_id;
    }
    
    expected_sequence_id_++;
    
    // 5. 处理待处理的条目
    ProcessPendingEntries();
    
    return true;
}

bool OpLogApplier::CheckSequenceOrder(const OpLogEntry& entry) {
    std::lock_guard<std::mutex> lock(key_sequence_mutex_);
    
    auto it = key_sequence_map_.find(entry.object_key);
    if (it == key_sequence_map_.end()) {
        // 新 key，允许
        return true;
    }
    
    // 检查 key_sequence_id 是否递增
    if (entry.key_sequence_id <= it->second) {
        // 序列号乱序，需要回滚和重放
        return false;
    }
    
    return true;
}
```

### 4. 初始同步流程

```cpp
class StandbyInitialSync {
public:
    void PerformInitialSync() {
        // Step 1: 从 Primary 获取快照
        MetadataSnapshot snapshot = RequestSnapshotFromPrimary();
        
        // Step 2: 获取快照对应的 sequence_id
        uint64_t snapshot_seq_id = snapshot.last_oplog_sequence_id;
        
        // Step 3: 应用快照
        metadata_store_->ImportSnapshot(snapshot);
        
        // Step 4: 从 etcd 读取快照后的 OpLog
        std::vector<OpLogEntry> entries;
        op_log_watcher_->ReadOpLogSince(snapshot_seq_id + 1, entries);
        
        // Step 5: 应用历史 OpLog
        for (const auto& entry : entries) {
            applier_->ApplyOpLogEntry(entry);
        }
        
        // Step 6: 开始 Watch 增量 OpLog
        op_log_watcher_->Start();
    }
};
```

## 快照集成

### 1. 快照时记录 Sequence ID

```cpp
class SnapshotManager {
public:
    MetadataSnapshot CreateSnapshot() {
        MetadataSnapshot snapshot;
        
        // 1. 导出 metadata
        snapshot.metadata = ExportMetadata();
        
        // 2. 记录当前的 OpLog sequence_id
        snapshot.last_oplog_sequence_id = oplog_manager_->GetLastSequenceId();
        
        // 3. 将快照信息写入 etcd
        std::string snapshot_id = GenerateSnapshotId();
        etcd_oplog_store_->RecordSnapshotSequenceId(
            snapshot_id, snapshot.last_oplog_sequence_id);
        
        // 4. 清理旧的 OpLog
        etcd_oplog_store_->CleanupOpLogBefore(
            snapshot.last_oplog_sequence_id);
        
        return snapshot;
    }
};
```

### 2. OpLog 清理策略

**方案：从 etcd 查询最小的 sequence_id，然后使用 DeleteRange 删除**

```cpp
bool EtcdOpLogStore::CleanupOpLogBefore(uint64_t target_sequence_id) {
    if (target_sequence_id <= 1) {
        return true;  // 没有需要清理的
    }
    
    // 1. 从 etcd 查询最小的 sequence_id
    uint64_t min_seq_id = GetMinSequenceId();
    
    // 2. 如果 min_seq_id >= target_sequence_id，无需清理
    if (min_seq_id >= target_sequence_id) {
        return true;
    }
    
    // 3. 执行 DeleteRange
    std::string start_key = BuildOpLogKey(min_seq_id);
    std::string end_key = BuildOpLogKey(target_sequence_id);
    
    int64_t deleted_count = 0;
    auto err = EtcdHelper::DeleteRange(
        start_key.c_str(), start_key.size(),
        end_key.c_str(), end_key.size(),
        deleted_count);
    
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to cleanup OpLog";
        return false;
    }
    
    LOG(INFO) << "Cleaned up " << deleted_count 
              << " OpLog entries from " << min_seq_id 
              << " to " << target_sequence_id;
    return true;
}

uint64_t EtcdOpLogStore::GetMinSequenceId() const {
    // 从 etcd 查询最小的 OpLog sequence_id
    std::string prefix = etcd_prefix_ + "/" + cluster_id_ + "/";
    std::string first_key, first_value;
    
    auto err = EtcdHelper::GetFirstKeyWithPrefix(
        prefix.c_str(), prefix.size(),
        first_key, first_value);
    
    if (err == ErrorCode::OK) {
        // 从 key 中提取 sequence_id
        uint64_t min_seq_id = ExtractSequenceIdFromKey(first_key);
        if (min_seq_id > 0) {
            return min_seq_id;
        }
    }
    
    // Fallback：从快照记录获取
    uint64_t last_snapshot_seq_id = GetLastSnapshotSequenceId();
    if (last_snapshot_seq_id > 0) {
        return last_snapshot_seq_id;
    }
    
    // 保守策略：从 1 开始
    return 1;
}
```

**详细实现请参考：`doc/zh/rfc-oplog-cleanup-start-sequence-id.md`**

## 时序保证机制详解

### 1. 全局序列号（sequence_id）

- **作用**：保证所有 OpLog 事件的全局顺序
- **生成**：Primary 端 OpLogManager 全局递增
- **检查**：Standby 端检查 sequence_id 是否连续

### 2. Key 级别序列号（key_sequence_id）

- **作用**：保证同一个 key 的操作顺序
- **生成**：Primary 端为每个 key 维护独立的序列号
- **检查**：Standby 端检查 key_sequence_id 是否递增

### 3. 序列号不连续处理

```cpp
void OpLogApplier::ScheduleWaitForMissingEntries(uint64_t missing_seq) {
    // 等待一段时间（如 1 秒）
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // 如果缺失的条目仍未到达，需要从 etcd 读取
    if (pending_entries_.find(missing_seq) == pending_entries_.end()) {
        RequestMissingOpLog(missing_seq);
    }
}

void OpLogApplier::RequestMissingOpLog(uint64_t missing_seq) {
    // 从 etcd 读取缺失的 OpLog
    OpLogEntry entry;
    if (ReadOpLogFromEtcd(missing_seq, entry)) {
        ApplyOpLogEntry(entry);
    } else {
        LOG(ERROR) << "Failed to read missing OpLog: seq=" << missing_seq;
        // 触发重新同步
        TriggerResync();
    }
}
```

## 实现步骤

### Phase 1：基础框架（优先级：高）

1. **实现 EtcdOpLogStore**
   - 写入 OpLog 到 etcd
   - 更新最新 sequence_id
   - 读取 OpLog 从 etcd

2. **集成到 OpLogManager**
   - 添加 EtcdOpLogStore 成员
   - 在 Append 时写入 etcd

3. **实现 OpLogWatcher**
   - Watch etcd OpLog 变化
   - 处理 Watch 事件

### Phase 2：Standby 端处理（优先级：高）

1. **实现 OpLogApplier**
   - 应用 OpLog Entry
   - 时序检查逻辑
   - 处理序列号不连续

2. **实现初始同步**
   - 从 Primary 获取快照
   - 读取历史 OpLog
   - 应用快照和 OpLog

### Phase 3：快照集成（优先级：中）

1. **快照时记录 sequence_id**
   - 在快照中记录 last_oplog_sequence_id
   - 写入 etcd

2. **OpLog 清理**
   - 实现 CleanupOpLogBefore
   - 定期清理旧的 OpLog

### Phase 4：优化（优先级：低）

1. **批量写入**
   - 实现 WriteOpLogBatch
   - 减少 etcd 写入次数

2. **压缩**
   - OpLog Entry 压缩
   - 减少 etcd 存储大小

## 关键设计要点

### 1. etcd Key 设计

- 使用顺序 Key：`mooncake-store/oplog/{cluster_id}/{sequence_id}`
- 支持按 sequence_id 范围查询
- 易于清理（删除指定 sequence_id 之前的 key）

### 2. 时序保证

- **全局序列号**：保证所有事件的全局顺序
- **Key 级别序列号**：保证同一 key 的操作顺序
- **序列号不连续处理**：检测并处理序列号不连续的情况
- **序列号乱序处理**：检测到 key 级别乱序时，执行回滚和重放（详细设计请参考：`doc/zh/rfc-oplog-rollback-replay-on-sequence-violation.md`）

### 3. 快照集成

- 快照时记录 sequence_id
- 快照后清理旧的 OpLog
- Standby 从快照点开始应用增量 OpLog

### 4. 故障恢复

- Standby 持久化处理状态
- 支持断点续传
- 发现不一致时触发重新同步

### 5. Standby 服务集成

**问题**：现有代码中，Standby 在等待 leader 选举期间只是阻塞等待，没有运行 Standby 服务来同步 OpLog。

**解决方案**：在 Standby 模式下并行运行 Standby 服务，watch etcd OpLog 并实时恢复 metadata。

**详细设计请参考**：`doc/zh/rfc-standby-service-integration.md`

### 6. Standby 提升为 Primary 时的 Lease 初始化

**问题**：Standby 上的对象 lease 都是 0（因为 OpLog 只包含 PUT_END，不包含续约信息），提升为 Primary 后所有对象会立即过期。

**解决方案**：在 `Promote()` 时，给所有 lease 为 0 的对象授予默认租约时间（`default_kv_lease_ttl`）。

**详细设计请参考**：`doc/zh/rfc-standby-promotion-lease-initialization.md`

### 7. 序列号乱序时的回滚和重放

**问题**：当检测到某个 key 的 `key_sequence_id` 乱序时，该 key 的 metadata 可能已经不一致。

**解决方案**：对于乱序的 key，执行回滚和重放：
1. **回滚**：从 metadata_store 中删除该 key 的所有状态
2. **重放**：从该 key 第一次出现的 sequence_id 开始，从 etcd 重新读取所有 OpLog
3. **重写**：按正确顺序重新应用所有 OpLog，重建 metadata

**关键设计**：
- 异步执行回滚，不阻塞正常处理
- 使用 `keys_under_rollback_` 防止并发回滚
- 从 etcd 批量读取 OpLog，减少网络往返
- 监控乱序频率，超过阈值时触发全量同步

**详细设计请参考**：`doc/zh/rfc-oplog-rollback-replay-on-sequence-violation.md`

### 8. key_sequence_map_ 内存清理策略

**问题**：Standby 端的 `OpLogApplier` 中，`key_sequence_map_` 用于跟踪每个 key 的 `key_sequence_id`。当 metadata 被删除后，这些条目仍然保留用于乱序检测，长期运行可能导致内存泄漏。

**解决方案**：实现定期清理机制：
1. **清理条件**：最后一次操作是 `REMOVE` 且距离当前超过 1 小时
2. **清理频率**：每小时扫描一次
3. **保留策略**：`PUT_END` 和 `PUT_REVOKE` 操作的 key 不清理（metadata 可能仍存在）

**关键设计**：
- 在 `ApplyOpLogEntry` 中触发清理检查，无需额外线程
- 只清理 `REMOVE` 操作且超过 1 小时的条目
- 1 小时的时间窗口足够处理网络延迟、重传等异常情况
- 有效控制内存占用，从潜在的 90MB+ 降低到约 `(活跃key数量 + 1万) × 90字节`

**详细设计请参考**：`doc/zh/rfc-oplog-key-sequence-map-cleanup.md`

## 与现有方案对比

| 特性 | 当前方案（gRPC 推送） | etcd Watch 方案 |
|------|----------------------|----------------|
| **时序保证** | 依赖网络顺序 | etcd 保证顺序 |
| **可靠性** | 需要 ACK 机制 | etcd 保证可靠性 |
| **断点续传** | 需要实现 | etcd 原生支持 |
| **数据持久化** | 需要额外实现 | etcd 自动持久化 |
| **快照集成** | 需要额外实现 | 易于集成 |
| **实现复杂度** | 高 | 中等 |

## 实施计划

详细的实施计划、优先级和时间估算请参考：`doc/zh/rfc-oplog-implementation-plan.md`

**实施阶段总览**：
- **Phase 1**：基础框架（P0，2-3 周）
- **Phase 2**：Standby 服务集成（P0，2-3 周）
- **Phase 3**：时序保证和容错（P1，2-3 周）
- **Phase 4**：快照集成和清理（P2，1-2 周）
- **Phase 5**：优化和完善（P3，1-2 周）

**总计**：8-13 周（约 2-3 个月）

## 总结

本方案利用 etcd 的强一致性和 Watch 机制，实现了可靠的 OpLog 同步。通过只记录 PUT 和 DELETE 事件，大幅减少了 OpLog 大小。通过全局和 key 级别的序列号，保证了时序性。通过与快照机制集成，实现了高效的 OpLog 清理。

