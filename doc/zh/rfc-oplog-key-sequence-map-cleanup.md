# OpLogApplier key_sequence_map_ 清理策略

## 问题描述

在 Standby 端的 `OpLogApplier` 中，`key_sequence_map_` 用于跟踪每个 key 的 `key_sequence_id`，以确保 OpLog 的顺序正确性。当 metadata 被删除（REMOVE 操作）后，`key_sequence_map_` 中的条目仍然保留，用于检测可能的乱序操作。

### 内存泄漏风险

如果 `key_sequence_map_` 中的条目一直不删除，长期运行可能导致内存泄漏：

- **内存占用**：每个条目约 90 字节（string key + uint64_t value + hash map 开销）
- **累积效应**：系统长期运行，可能有数百万个不同的 key 曾经存在过
- **极端场景**：如果每天创建 10 万个新 key，运行 100 天，累计 1000 万个不同的 key，内存占用可达 900MB

### 清理需求

需要在保证功能正确性的前提下，实现内存清理机制。

## 解决方案

### 核心策略

**清理条件**：
1. 最后一次操作是 `REMOVE`（DELETE）
2. 距离当前时间超过 1 小时

**清理频率**：每小时扫描一次

**保留策略**：
- `PUT_END` 和 `PUT_REVOKE` 操作的 key 不清理（metadata 可能仍存在）
- 即使超过 1 小时，只要最后操作不是 `REMOVE`，也保留

### 设计原理

1. **乱序检测时间窗口**：乱序检测一般只需要秒级的时间窗口，1 小时的保留时间足够处理网络延迟、重传等情况
2. **只清理 DELETE 操作**：因为 DELETE 操作的 metadata 已经不存在，且超过 1 小时后不太可能再出现乱序
3. **保留 PUT 操作**：PUT 操作的 metadata 可能仍存在，需要保留用于顺序检查

## 实现设计

### 数据结构

```cpp
class OpLogApplier {
private:
    struct KeySequenceInfo {
        uint64_t sequence_id{0};
        OpType last_op_type{OpType::PUT_END};
        std::chrono::steady_clock::time_point last_op_time;
        
        KeySequenceInfo() 
            : last_op_time(std::chrono::steady_clock::now()) {}
    };
    
    std::unordered_map<std::string, KeySequenceInfo> key_sequence_map_;
    mutable std::mutex key_sequence_mutex_;
    
    // 清理配置
    static constexpr std::chrono::hours kCleanupInterval{1};  // 每小时清理一次
    static constexpr std::chrono::hours kStaleThreshold{1};   // 1小时未访问则清理（仅限DELETE）
    
    std::chrono::steady_clock::time_point last_cleanup_time_;
};
```

### 核心方法

#### 1. 定期清理检查

```cpp
void OpLogApplier::PeriodicCleanup() {
    auto now = std::chrono::steady_clock::now();
    if (now - last_cleanup_time_ < kCleanupInterval) {
        return;  // 还没到清理时间
    }
    
    CleanupStaleKeySequences();
    last_cleanup_time_ = now;
}
```

#### 2. 清理过期条目

```cpp
void OpLogApplier::CleanupStaleKeySequences() {
    std::lock_guard<std::mutex> lock(key_sequence_mutex_);
    auto now = std::chrono::steady_clock::now();
    auto threshold = now - kStaleThreshold;
    
    size_t cleaned = 0;
    for (auto it = key_sequence_map_.begin(); 
         it != key_sequence_map_.end();) {
        const auto& info = it->second;
        
        // 清理条件：
        // 1. 最后一次操作是 REMOVE（DELETE）
        // 2. 且距离当前超过1小时
        if (info.last_op_type == OpType::REMOVE && 
            info.last_op_time < threshold) {
            it = key_sequence_map_.erase(it);
            cleaned++;
        } else {
            ++it;
        }
    }
    
    if (cleaned > 0) {
        LOG(INFO) << "Cleaned up " << cleaned 
                 << " stale key_sequence_map entries "
                 << "(REMOVE operations older than 1 hour)";
    }
}
```

#### 3. 应用 OpLog 时更新

```cpp
bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // 1. 检查顺序
    if (!CheckSequenceOrder(entry)) {
        // 处理乱序...
        return false;
    }
    
    // 2. 应用操作
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
    
    // 3. 更新 key_sequence_map_
    {
        std::lock_guard<std::mutex> lock(key_sequence_mutex_);
        auto& info = key_sequence_map_[entry.object_key];
        info.sequence_id = entry.key_sequence_id;
        info.last_op_type = entry.op_type;
        info.last_op_time = std::chrono::steady_clock::now();
    }
    
    // 4. 定期清理（每次应用时检查，避免额外线程）
    PeriodicCleanup();
    
    return true;
}
```

## 关键设计要点

### 1. 清理时机

- **触发方式**：在 `ApplyOpLogEntry` 中检查，无需额外线程
- **清理频率**：每小时执行一次
- **清理条件**：只清理 `REMOVE` 操作且超过 1 小时的条目

### 2. 安全性保证

- **保留 PUT 操作**：`PUT_END` 和 `PUT_REVOKE` 的 key 不清理，因为 metadata 可能仍存在
- **1 小时窗口**：足够处理网络延迟、重传等异常情况
- **线程安全**：使用 mutex 保护 `key_sequence_map_` 的访问

### 3. 内存占用控制

**清理前**：
- 假设系统长期运行，有 100 万个不同的 key 曾经存在过
- 内存占用：100万 × 90字节 ≈ 90MB

**清理后**：
- 假设系统每小时处理 10 万个 OpLog，其中 10% 是 REMOVE 操作
- `key_sequence_map_` 中最多保留：
  - 最近 1 小时的 REMOVE key：约 1 万个
  - 所有 PUT_END/PUT_REVOKE 的 key：取决于实际 metadata 数量
- 内存占用：约 `(活跃key数量 + 1万) × 90字节`

**内存节省**：从 90MB 降低到约 `(活跃key数量 + 1万) × 90字节`，通常远小于不清理的情况。

## 使用场景示例

### 场景 1：正常 REMOVE 操作

```
时间线：
1. Standby 收到 OpLog: sequence_id=100, key="obj1", key_sequence_id=5, op_type=PUT_END
   → 应用成功，key_sequence_map_["obj1"] = {seq:5, op:PUT_END, time:10:00}

2. Standby 收到 OpLog: sequence_id=101, key="obj1", key_sequence_id=6, op_type=REMOVE
   → 应用成功，key_sequence_map_["obj1"] = {seq:6, op:REMOVE, time:10:05}
   → metadata 被删除

3. 1小时后（11:05），清理扫描
   → 检测到 "obj1" 的 last_op_type=REMOVE 且超过1小时
   → 清理 key_sequence_map_["obj1"]
```

### 场景 2：乱序 REMOVE 操作

```
时间线：
1. Standby 收到 OpLog: sequence_id=100, key="obj1", key_sequence_id=5, op_type=PUT_END
   → 应用成功，key_sequence_map_["obj1"] = {seq:5, op:PUT_END, time:10:00}

2. Standby 收到 OpLog: sequence_id=102, key="obj1", key_sequence_id=6, op_type=PUT_END
   → 应用成功，key_sequence_map_["obj1"] = {seq:6, op:PUT_END, time:10:02}

3. Standby 收到 OpLog: sequence_id=101, key="obj1", key_sequence_id=5, op_type=REMOVE
   → 检测到乱序：entry.key_sequence_id(5) <= current(6)
   → 触发回滚和重放
   → key_sequence_map_["obj1"] 保留用于重放
```

### 场景 3：删除后重新创建

```
时间线：
1. key="obj1" 被 REMOVE，key_sequence_map_["obj1"] = {seq:6, op:REMOVE, time:10:00}

2. 30分钟后（10:30），Standby 收到 OpLog: sequence_id=200, key="obj1", key_sequence_id=7, op_type=PUT_END
   → 检查：key_sequence_map_["obj1"] 存在，seq=6（期望）
   → 应用成功，key_sequence_map_["obj1"] = {seq:7, op:PUT_END, time:10:30}
   → 不会被清理（因为 last_op_type=PUT_END）
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `kCleanupInterval` | 1 小时 | 清理检查的间隔时间 |
| `kStaleThreshold` | 1 小时 | REMOVE 操作超过此时间后可以清理 |

## 优势

1. **内存控制**：及时清理已删除且超过 1 小时的 key，有效控制内存占用
2. **安全性**：保留最近删除的 key，确保乱序检测的正确性
3. **简单高效**：无需额外线程，在应用 OpLog 时检查，实现简单
4. **精确清理**：只清理符合条件的条目，不影响活跃 key

## 注意事项

1. **清理时机**：清理在 `ApplyOpLogEntry` 中触发，如果长时间没有 OpLog，可能不会及时清理
2. **时间精度**：使用 `std::chrono::steady_clock`，不受系统时间调整影响
3. **线程安全**：所有对 `key_sequence_map_` 的访问都需要加锁保护

## 相关文档

- [OpLog 主备同步完整方案](./rfc-oplog-via-etcd-complete-design.md)
- [OpLog 序列号乱序时的回滚和重放方案](./rfc-oplog-rollback-replay-on-sequence-violation.md)

