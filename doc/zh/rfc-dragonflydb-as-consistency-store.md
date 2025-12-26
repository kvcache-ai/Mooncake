# 使用 DragonflyDB 作为一致性中间存储组件的可行性分析

## 当前系统对 etcd 的使用场景

### 1. Leader Election（主从选举）
- **功能**：使用 etcd 的 Lease 机制和事务实现分布式锁
- **关键操作**：
  - `GrantLease()`：创建租约（TTL = 5秒）
  - `CreateWithLease()`：使用事务创建 key（原子性保证）
  - `KeepAlive()`：续租，保持 leader 身份
  - `WatchUntilDeleted()`：监听 leader key 删除，触发重新选举

### 2. Delete 事件同步
- **功能**：将 Delete 事件写入 etcd，确保所有 Standby 都能看到
- **关键操作**：
  - `Put()`：写入 Delete 事件
  - `Watch()`：Standby 监听 Delete 事件
  - 需要强一致性保证

### 3. Metadata 存储（部分场景）
- **功能**：存储部分 metadata 信息
- **关键操作**：
  - `Get()` / `Put()`：读写 metadata
  - `Update()`：带版本号的更新（使用事务）

## DragonflyDB 特性分析

### 优势
1. **高性能**：单机性能远超 Redis，适合高吞吐场景
2. **Redis 协议兼容**：可以使用现有的 Redis 客户端库
3. **内存数据库**：低延迟，适合实时同步场景
4. **数据持久化**：支持快照和 AOF

### 劣势和限制
1. **分布式一致性协议支持不明确**
   - 未明确支持 Raft/Paxos 等分布式一致性协议
   - 可能无法提供 etcd 级别的强一致性保证

2. **缺少关键特性**
   - **Lease/TTL 机制**：Redis 有 `EXPIRE`，但可能不如 etcd 的 Lease 精确
   - **事务原子性**：Redis 有 `MULTI/EXEC`，但可能不如 etcd 的事务强大
   - **Watch 机制**：Redis 有 `PUBSUB` 和 `KEYSpace notifications`，但可能不如 etcd 的 Watch 可靠
   - **版本号/Revision**：Redis 没有内置的版本号机制

3. **集群模式**
   - DragonflyDB 的集群模式可能使用主从复制或分片
   - 可能无法提供 etcd 的线性一致性（Linearizability）

## 使用方案对比

### 方案 A：完全替代 etcd（不推荐）

**优点**：
- 统一存储组件，简化架构
- 高性能，低延迟

**缺点**：
- **Leader Election 风险**：Redis 的 `SET NX EX` 可能不如 etcd 的事务可靠
- **一致性风险**：可能无法保证强一致性
- **Watch 机制**：Redis 的 PUBSUB 可能丢失消息
- **版本控制**：需要自己实现版本号机制

**实现示例**：
```cpp
// Leader Election（使用 Redis SET NX EX）
bool ElectLeader(const std::string& key, const std::string& value, int ttl) {
    // Redis: SET key value NX EX ttl
    // 问题：如果网络分区，可能出现多个 leader
}

// Delete 事件同步（使用 Redis PUBSUB）
void PublishDeleteEvent(const std::string& key) {
    // Redis: PUBLISH delete_channel delete_event_json
    // 问题：如果 Standby 断开连接，可能丢失消息
}
```

### 方案 B：混合方案（推荐）

**架构**：
- **etcd**：继续用于 Leader Election（强一致性要求）
- **DragonflyDB**：用于 OpLog 存储和 Delete 事件同步（高性能要求）

**优点**：
- 保留 etcd 的强一致性保证（Leader Election）
- 利用 DragonflyDB 的高性能（OpLog 和 Delete 事件）
- 各取所长

**缺点**：
- 需要维护两个存储组件
- 架构稍复杂

**实现示例**：
```cpp
// Leader Election：继续使用 etcd
ErrorCode ElectLeader() {
    return EtcdHelper::CreateWithLease(...);
}

// OpLog 存储：使用 DragonflyDB
class DragonflyOpLogStore {
    // 使用 Redis List 存储 OpLog
    // LPUSH oplog:entries {seq_id, op_type, key, payload}
    // LRANGE oplog:entries start end
};

// Delete 事件：使用 DragonflyDB Stream（Redis Stream）
void PublishDeleteEvent(const std::string& key) {
    // Redis Stream: XADD delete_stream * key value
    // Standby: XREAD BLOCK 0 STREAMS delete_stream $
}
```

### 方案 C：DragonflyDB 作为 OpLog 持久化存储（推荐）

**架构**：
- **etcd**：继续用于 Leader Election 和 Delete 事件（强一致性）
- **DragonflyDB**：仅用于 OpLog 的持久化存储和快速同步

**优点**：
- 最小化风险，只替换非关键路径
- OpLog 可以容忍一定程度的丢失（有快照机制）
- 利用 DragonflyDB 的高性能加速 OpLog 同步

**实现示例**：
```cpp
class DragonflyOpLogStore {
public:
    // 追加 OpLog 到 DragonflyDB
    void AppendOpLog(const OpLogEntry& entry) {
        // Redis List: LPUSH oplog:entries {json}
        // 或 Redis Stream: XADD oplog_stream * {json}
    }
    
    // Standby 从 DragonflyDB 拉取 OpLog
    std::vector<OpLogEntry> GetOpLogSince(uint64_t seq_id) {
        // Redis Stream: XREAD BLOCK 0 STREAMS oplog_stream last_id
        // 或 Redis List: LRANGE oplog:entries start end
    }
};
```

## 详细对比分析

### 1. Leader Election

| 特性 | etcd | DragonflyDB (Redis) | 结论 |
|------|------|---------------------|------|
| 原子性 | 事务保证 | SET NX EX（可能不够强） | **etcd 更可靠** |
| Lease 机制 | 原生支持 | EXPIRE（可能不够精确） | **etcd 更可靠** |
| Watch 可靠性 | 强一致性保证 | PUBSUB 可能丢失 | **etcd 更可靠** |
| 性能 | 中等 | 高 | DragonflyDB 更快 |

**建议**：Leader Election 继续使用 etcd

### 2. Delete 事件同步

| 特性 | etcd | DragonflyDB (Redis Stream) | 结论 |
|------|------|---------------------------|------|
| 一致性 | 强一致性 | 最终一致性（可能） | **etcd 更可靠** |
| 持久化 | 持久化 | 可配置持久化 | 两者都支持 |
| Watch/Stream | Watch 机制 | Stream 机制 | 两者都支持 |
| 性能 | 中等 | 高 | **DragonflyDB 更快** |
| 消息丢失 | 不会丢失 | 可能丢失（如果未持久化） | **etcd 更可靠** |

**建议**：
- **方案 1**：继续使用 etcd（如果强一致性要求高）
- **方案 2**：使用 DragonflyDB Stream + 持久化（如果性能要求高，可以容忍少量丢失）

### 3. OpLog 存储

| 特性 | 当前（内存） | DragonflyDB | 结论 |
|------|------------|-------------|------|
| 持久化 | 无 | 支持 | **DragonflyDB 更好** |
| 容量 | 有限（100K 条） | 大容量 | **DragonflyDB 更好** |
| 性能 | 极高 | 高 | 当前方案更快 |
| 一致性 | 不适用 | 最终一致性可接受 | 两者都可 |

**建议**：**可以使用 DragonflyDB**，因为：
- OpLog 可以容忍一定程度的丢失（有快照机制）
- 需要持久化以支持新 Standby 的初始同步
- 性能要求相对较低（异步同步）

## 推荐方案：混合架构

### 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                    Primary Master                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   etcd       │  │ DragonflyDB  │  │  OpLogManager │ │
│  │ (Leader      │  │ (OpLog Store)│  │  (Memory)    │ │
│  │  Election)   │  │              │  │              │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────┐
│                    Standby Masters                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   etcd       │  │ DragonflyDB  │  │  OpLogApplier│ │
│  │ (Watch       │  │ (Pull OpLog) │  │              │ │
│  │  Leader)     │  │              │  │              │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 具体实现

#### 1. Leader Election：继续使用 etcd
```cpp
// 保持不变
ErrorCode ElectLeader() {
    return EtcdHelper::CreateWithLease(...);
}
```

#### 2. OpLog 持久化：使用 DragonflyDB
```cpp
class DragonflyOpLogStore {
public:
    // 追加 OpLog（异步）
    void AppendOpLog(const OpLogEntry& entry) {
        // 使用 Redis Stream
        std::string json = SerializeOpLogEntry(entry);
        redis_->XAdd("oplog_stream", "*", {{"entry", json}});
    }
    
    // Standby 拉取 OpLog
    std::vector<OpLogEntry> GetOpLogSince(const std::string& last_id) {
        // XREAD BLOCK 0 STREAMS oplog_stream last_id
        auto messages = redis_->XRead({"oplog_stream"}, {last_id}, 1000);
        // 解析并返回
    }
};
```

#### 3. Delete 事件：可选方案

**选项 A：继续使用 etcd（推荐）**
- 保证强一致性
- 代码改动小

**选项 B：使用 DragonflyDB Stream**
- 高性能
- 需要处理消息丢失场景

## 实施建议

### Phase 1：OpLog 持久化到 DragonflyDB（低风险）

1. **实现 DragonflyOpLogStore**
   - 使用 Redis Stream 存储 OpLog
   - 异步写入，不阻塞主流程
   - 支持 Standby 拉取

2. **修改 OpLogManager**
   - 添加可选的持久化后端
   - 保持内存 buffer 不变（性能）

3. **修改 HotStandbyService**
   - 支持从 DragonflyDB 拉取 OpLog
   - 支持断点续传

**优点**：
- 风险低，不影响现有功能
- 可以逐步迁移
- 支持新 Standby 的初始同步

### Phase 2：评估 Delete 事件迁移（可选）

1. **实现 DragonflyDeleteEventStore**
   - 使用 Redis Stream
   - 添加持久化配置
   - 处理消息丢失场景

2. **对比测试**
   - 性能对比
   - 一致性测试
   - 故障场景测试

3. **决定是否迁移**
   - 如果性能提升明显且一致性可接受，则迁移
   - 否则继续使用 etcd

## 总结

### 可以使用 DragonflyDB 的场景

1. **OpLog 持久化存储**（推荐）
   - 优点：持久化、大容量、高性能
   - 风险：低（有快照机制兜底）

2. **Delete 事件同步**（可选）
   - 优点：高性能
   - 风险：中等（需要评估一致性要求）

### 不建议使用 DragonflyDB 的场景

1. **Leader Election**（不推荐）
   - 需要强一致性保证
   - etcd 的事务和 Lease 机制更可靠

### 推荐方案

**混合架构**：
- **etcd**：Leader Election + Delete 事件（强一致性）
- **DragonflyDB**：OpLog 持久化存储（高性能 + 持久化）

这样既保证了关键路径的强一致性，又利用了 DragonflyDB 的高性能优势。

