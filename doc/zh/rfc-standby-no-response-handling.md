# Standby Master 无响应处理方案

## 问题分析

当 OpLog 从 Primary Master 同步到 Standby Master 时，如果 Standby 一直不响应，会导致以下问题：

### 1. **内存压力**
- `OpLogManager` 的 buffer 有上限（`kMaxBufferEntries_ = 100000`），但即使有上限，也可能导致：
  - 内存占用持续增长
  - 无法及时 truncate，导致 buffer 长期占用
  - 如果多个 Standby 都无响应，问题会放大

### 2. **数据丢失风险**
- 如果 buffer 满了，最老的 OpLog 会被丢弃（`pop_front()`）
- 如果 Standby 后来恢复，可能无法完整同步历史数据

### 3. **性能影响**
- 持续尝试发送失败的消息会消耗 CPU
- 阻塞其他正常 Standby 的同步（如果实现不当）

### 4. **故障检测缺失**
- 当前实现无法区分：
  - **网络分区**：Standby 节点正常，但网络不通
  - **节点故障**：Standby 节点宕机
  - **处理慢**：Standby 节点正常，但处理速度慢

## 解决方案设计

### 方案 1: 超时检测 + 故障隔离（推荐）

#### 1.1 添加超时检测机制

```cpp
struct StandbyState {
    std::shared_ptr<ReplicationStream> stream;
    uint64_t acked_seq_id{0};
    std::chrono::steady_clock::time_point last_ack_time;
    std::chrono::steady_clock::time_point last_send_time;  // 新增
    std::vector<OpLogEntry> pending_batch;
    
    // 新增：超时和重试状态
    enum class State {
        HEALTHY,        // 正常状态
        SLOW,          // 响应慢，但还在处理
        TIMEOUT,       // 超时，可能故障
        DISCONNECTED   // 已断开连接
    };
    State state{State::HEALTHY};
    uint32_t consecutive_failures{0};  // 连续失败次数
};
```

#### 1.2 实现超时检测逻辑

```cpp
class ReplicationService {
private:
    // 配置参数
    static constexpr uint32_t kAckTimeoutMs = 5000;      // ACK 超时时间（5秒）
    static constexpr uint32_t kSendTimeoutMs = 3000;     // 发送超时时间（3秒）
    static constexpr uint32_t kMaxConsecutiveFailures = 3; // 最大连续失败次数
    static constexpr uint32_t kHealthCheckIntervalMs = 1000; // 健康检查间隔（1秒）
    
    // 定期检查 Standby 健康状态
    void CheckStandbyHealth();
    
    // 标记 Standby 为故障状态
    void MarkStandbyUnhealthy(const std::string& standby_id);
    
    // 尝试恢复 Standby 连接
    void TryRecoverStandby(const std::string& standby_id);
};
```

#### 1.3 故障隔离策略

**策略 A: 暂停发送（推荐）**
- 当 Standby 超时或连续失败时，暂停向该 Standby 发送新的 OpLog
- 继续向其他健康的 Standby 发送
- 保留该 Standby 的 `acked_seq_id`，等待恢复后从断点继续

**策略 B: 降级处理**
- 将 Standby 标记为 `SLOW` 状态
- 降低发送频率（例如：每 10 个 OpLog 发送一次）
- 如果持续超时，再升级为 `TIMEOUT` 状态

#### 1.4 实现示例

```cpp
void ReplicationService::CheckStandbyHealth() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    
    for (auto& [standby_id, state] : standbys_) {
        // 检查连接状态
        if (!state.stream || !state.stream->IsConnected()) {
            state.state = StandbyState::State::DISCONNECTED;
            continue;
        }
        
        // 检查 ACK 超时
        auto ack_age = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - state.last_ack_time).count();
        
        if (ack_age > kAckTimeoutMs) {
            state.consecutive_failures++;
            
            if (state.consecutive_failures >= kMaxConsecutiveFailures) {
                state.state = StandbyState::State::TIMEOUT;
                LOG(WARNING) << "Standby " << standby_id 
                           << " marked as TIMEOUT after " 
                           << state.consecutive_failures << " failures";
                // 暂停向该 Standby 发送
            } else {
                state.state = StandbyState::State::SLOW;
                LOG(WARNING) << "Standby " << standby_id 
                           << " is slow (ack_age=" << ack_age << "ms)";
            }
        } else {
            // 恢复正常
            if (state.state != StandbyState::State::HEALTHY) {
                LOG(INFO) << "Standby " << standby_id << " recovered";
                state.state = StandbyState::State::HEALTHY;
                state.consecutive_failures = 0;
            }
        }
    }
}

void ReplicationService::BroadcastEntry(const OpLogEntry& entry) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    for (auto& [standby_id, state] : standbys_) {
        // 跳过故障的 Standby
        if (state.state == StandbyState::State::TIMEOUT ||
            state.state == StandbyState::State::DISCONNECTED) {
            continue;
        }
        
        state.pending_batch.push_back(entry);
        
        if (state.pending_batch.size() >= kBatchSize) {
            SendBatch(standby_id, state.pending_batch);
            state.pending_batch.clear();
        }
    }
}
```

### 方案 2: 真正的 ACK 机制

当前实现中，`acked_seq_id` 的更新是假设 `Send()` 成功就更新，这是不正确的。应该：

1. **发送时记录待确认的序列号**
2. **等待 Standby 的 ACK 响应**
3. **只有收到 ACK 后才更新 `acked_seq_id`**

```cpp
struct StandbyState {
    // ...
    std::map<uint64_t, std::chrono::steady_clock::time_point> pending_acks;  // seq_id -> send_time
    uint64_t last_sent_seq_id{0};  // 最后发送的序列号
};

void ReplicationService::SendBatch(const std::string& standby_id,
                                   const std::vector<OpLogEntry>& entries) {
    // ... 发送逻辑 ...
    
    if (success && !entries.empty()) {
        uint64_t last_seq = entries.back().sequence_id;
        state.last_sent_seq_id = last_seq;
        // 记录待确认的序列号
        state.pending_acks[last_seq] = std::chrono::steady_clock::now();
        // 注意：这里不更新 acked_seq_id，等收到 ACK 再更新
    }
}

// 处理 Standby 的 ACK 响应
void ReplicationService::OnAck(const std::string& standby_id, uint64_t acked_seq_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = standbys_.find(standby_id);
    if (it == standbys_.end()) {
        return;
    }
    
    auto& state = it->second;
    if (acked_seq_id > state.acked_seq_id) {
        state.acked_seq_id = acked_seq_id;
        state.last_ack_time = std::chrono::steady_clock::now();
        state.consecutive_failures = 0;  // 重置失败计数
        
        // 清理已确认的 pending_acks
        auto ack_it = state.pending_acks.begin();
        while (ack_it != state.pending_acks.end()) {
            if (ack_it->first <= acked_seq_id) {
                ack_it = state.pending_acks.erase(ack_it);
            } else {
                ++ack_it;
            }
        }
    }
}
```

### 方案 3: 流控（Backpressure）机制

如果 Standby 处理慢，应该限制发送速度，避免 Standby 内存溢出：

```cpp
struct StandbyState {
    // ...
    size_t in_flight_bytes{0};  // 正在传输的字节数
    size_t max_in_flight_bytes{10 * 1024 * 1024};  // 最大 10MB
    uint32_t pending_batch_count{0};  // 待确认的批次数量
    uint32_t max_pending_batches{10};  // 最大待确认批次
};

bool ReplicationService::CanSendToStandby(const StandbyState& state) const {
    // 检查流控条件
    if (state.in_flight_bytes >= state.max_in_flight_bytes) {
        return false;  // 超过流量限制
    }
    if (state.pending_batch_count >= state.max_pending_batches) {
        return false;  // 超过批次限制
    }
    return true;
}
```

### 方案 4: OpLog Truncate 策略

只有当**所有健康的 Standby** 都 ACK 了某个序列号后，才能安全地 truncate：

```cpp
uint64_t ReplicationService::GetMinAckedSequenceId() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    if (standbys_.empty()) {
        // 没有 Standby，可以 truncate 所有
        return oplog_manager_.GetLastSequenceId();
    }
    
    uint64_t min_acked = UINT64_MAX;
    for (const auto& [standby_id, state] : standbys_) {
        // 只考虑健康的 Standby
        if (state.state == StandbyState::State::HEALTHY ||
            state.state == StandbyState::State::SLOW) {
            min_acked = std::min(min_acked, state.acked_seq_id);
        }
    }
    
    return (min_acked == UINT64_MAX) ? 0 : min_acked;
}

// 定期调用，清理已确认的 OpLog
void ReplicationService::TruncateOpLog() {
    uint64_t min_acked = GetMinAckedSequenceId();
    if (min_acked > 0) {
        oplog_manager_.TruncateBefore(min_acked);
    }
}
```

### 方案 5: 重连和恢复机制

当 Standby 恢复后，应该能够从断点继续同步：

```cpp
void ReplicationService::TryRecoverStandby(const std::string& standby_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = standbys_.find(standby_id);
    if (it == standbys_.end()) {
        return;
    }
    
    auto& state = it->second;
    
    // 检查连接是否恢复
    if (state.stream && state.stream->IsConnected()) {
        // 从上次 ACK 的位置开始重新发送
        uint64_t start_seq = state.acked_seq_id + 1;
        auto entries = oplog_manager_.GetEntriesSince(start_seq, 1000);
        
        if (!entries.empty()) {
            LOG(INFO) << "Recovering Standby " << standby_id 
                     << " from seq_id=" << start_seq
                     << ", entries=" << entries.size();
            SendBatch(standby_id, entries);
            state.state = StandbyState::State::HEALTHY;
        }
    }
}
```

## 实施优先级

### Phase 1: 基础超时检测（必须）
1. 添加 `StandbyState::State` 枚举
2. 实现 `CheckStandbyHealth()` 定期检查
3. 在 `BroadcastEntry()` 中跳过故障 Standby
4. 添加配置参数（超时时间、最大失败次数）

### Phase 2: 真正的 ACK 机制（重要）
1. 修改 `SendBatch()` 不立即更新 `acked_seq_id`
2. 添加 `OnAck()` 方法处理 ACK 响应
3. 实现 `pending_acks` 跟踪机制

### Phase 3: 流控和 Truncate（优化）
1. 实现流控机制
2. 实现安全的 OpLog truncate
3. 添加监控指标（replication lag、failure rate）

### Phase 4: 恢复机制（完善）
1. 实现重连检测
2. 实现断点续传
3. 添加恢复日志

## 配置参数建议

```cpp
struct ReplicationConfig {
    uint32_t ack_timeout_ms = 5000;           // ACK 超时时间
    uint32_t send_timeout_ms = 3000;          // 发送超时时间
    uint32_t max_consecutive_failures = 3;     // 最大连续失败次数
    uint32_t health_check_interval_ms = 1000; // 健康检查间隔
    size_t max_in_flight_bytes = 10 * 1024 * 1024;  // 最大传输字节数
    uint32_t max_pending_batches = 10;        // 最大待确认批次
    bool enable_backpressure = true;          // 是否启用流控
};
```

## 监控指标

建议添加以下监控指标：

1. **Replication Lag**: 每个 Standby 的延迟（`primary_seq_id - acked_seq_id`）
2. **Failure Rate**: Standby 的失败率
3. **Timeout Count**: 超时次数
4. **Recovery Count**: 恢复次数
5. **OpLog Buffer Size**: OpLog buffer 当前大小
6. **Truncate Rate**: OpLog truncate 频率

## 总结

Standby 无响应是一个复杂的分布式系统问题，需要多层次的解决方案：

1. **超时检测**：及时发现故障
2. **故障隔离**：避免影响其他 Standby
3. **真正的 ACK**：准确跟踪同步进度
4. **流控**：保护 Standby 不被压垮
5. **安全 Truncate**：避免数据丢失
6. **恢复机制**：支持断点续传

建议先实施 Phase 1 和 Phase 2，这两个是最关键的。

