# 基于 etcd 批量压缩 Delete 事件方案

## 问题背景

### 当前设计回顾

根据之前的分析：
1. **驱逐事件频率极高**：可达 130,000 次/秒
2. **当前方案**：
   - 显式 Delete 事件 → 写入 etcd（强一致性）
   - 驱逐产生的 Delete 事件 → 不写入 etcd（由 Standby 自己根据租约到期决定）

### 新方案需求

用户提出：使用 etcd 作为中间媒介，对驱逐产生的 delete 事件进行**批量压缩组装**后写入 etcd，而不是每次驱逐都写入一次。

## 方案设计

### 1. 架构设计

```
┌─────────────────────────────────────────────────────────┐
│              Primary Master                              │
│                                                          │
│  ┌──────────────┐      ┌──────────────┐                │
│  │  Eviction    │      │  Delete      │                │
│  │  Thread      │      │  Event       │                │
│  │              │      │  Buffer      │                │
│  ┌──────────────┘      ┌──────────────┘                │
│         │                      │                         │
│         │ 驱逐事件              │ 显式 Delete            │
│         ▼                      ▼                         │
│  ┌──────────────────────────────────────┐               │
│  │   BatchedDeleteEventManager          │               │
│  │   - 批量收集 delete 事件              │               │
│  │   - 压缩/去重                          │               │
│  │   - 定时批量写入 etcd                  │               │
│  └──────────────────────────────────────┘               │
│         │                                                │
│         │ 批量写入                                        │
│         ▼                                                │
│  ┌──────────────┐                                       │
│  │     etcd     │                                       │
│  └──────────────┘                                       │
└─────────────────────────────────────────────────────────┘
         │
         │ Watch
         ▼
┌─────────────────────────────────────────────────────────┐
│              Standby Masters                            │
│  ┌──────────────────────────────────────┐               │
│  │   DeleteEventWatcher                │               │
│  │   - Watch etcd delete events         │               │
│  │   - 解压缩/应用 delete 事件            │               │
│  └──────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────┘
```

### 2. 批量压缩策略

#### 方案 A：时间窗口批量（推荐）

**原理**：
- 收集固定时间窗口内的所有 delete 事件（如 1 秒）
- 时间窗口到期后，批量写入 etcd
- 使用压缩格式减少数据量

**优点**：
- 简单易实现
- 延迟可控（最多 1 秒）
- 批量写入减少 etcd 压力

**缺点**：
- 固定延迟（1 秒）
- 如果事件很少，也会等待 1 秒

#### 方案 B：大小阈值批量

**原理**：
- 收集 delete 事件直到达到阈值（如 1000 条）
- 达到阈值后立即批量写入
- 同时设置最大等待时间（如 1 秒）

**优点**：
- 高吞吐时延迟低（立即写入）
- 低吞吐时延迟可控（最多 1 秒）

**缺点**：
- 实现稍复杂
- 需要同时考虑大小和时间两个维度

#### 方案 C：混合策略（推荐）

**原理**：
- 同时设置大小阈值（如 1000 条）和时间窗口（如 1 秒）
- 满足任一条件即批量写入
- 使用压缩格式减少数据量

**优点**：
- 兼顾性能和延迟
- 高吞吐时立即写入，低吞吐时定时写入

### 3. 压缩格式设计

#### 格式 A：JSON 数组（简单）

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "timestamp": 1704110400000,
  "keys": [
    "key1", "key2", "key3", ...
  ],
  "count": 1000
}
```

**优点**：
- 简单易实现
- 易于调试

**缺点**：
- 数据量大（每个 key 都是完整字符串）
- etcd value 大小限制（1.5MB）

#### 格式 B：前缀压缩（推荐）

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "timestamp": 1704110400000,
  "compressed": true,
  "format": "prefix_tree",
  "data": {
    "prefix1": ["suffix1", "suffix2", ...],
    "prefix2": ["suffix3", "suffix4", ...],
    ...
  },
  "count": 1000
}
```

**优点**：
- 压缩率高（如果 key 有共同前缀）
- 减少 etcd value 大小

**缺点**：
- 实现复杂
- 如果 key 没有共同前缀，压缩效果差

#### 格式 C：Bloom Filter + Key List（推荐用于大量 key）

**原理**：
- 使用 Bloom Filter 快速判断 key 是否存在
- 对于少量 key，直接存储完整列表
- 对于大量 key，使用 Bloom Filter + 采样

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "timestamp": 1704110400000,
  "count": 10000,
  "bloom_filter": "base64_encoded_bloom_filter",
  "sample_keys": ["key1", "key2", ...],  // 前 100 个 key 作为样本
  "hash_prefix": "abc123"  // 如果 key 有 hash 前缀，可以进一步压缩
}
```

**优点**：
- 压缩率极高（Bloom Filter 很小）
- 适合大量 key 的场景

**缺点**：
- 有误判率（Bloom Filter 特性）
- 需要额外存储完整 key 列表用于精确匹配

#### 格式 D：简单列表 + 压缩（推荐用于中等数量 key）

```json
{
  "batch_id": "2024-01-01T12:00:00.000Z",
  "timestamp": 1704110400000,
  "keys": ["key1", "key2", ...],  // 最多 1000 条
  "count": 1000
}
```

**优点**：
- 简单直接
- 无压缩开销
- 易于解析和应用

**缺点**：
- 如果 key 很长，数据量大
- 受 etcd value 大小限制

### 4. etcd Key 设计

#### 方案 A：单个 Key + 版本号

```
mooncake-store/deletes/batch/{batch_id}
```

**优点**：
- 简单
- 易于 Watch

**缺点**：
- 如果批量很大，单个 value 可能超过 etcd 限制（1.5MB）
- 需要处理 value 大小限制

#### 方案 B：分片 Key（推荐）

```
mooncake-store/deletes/batch/{batch_id}/shard/{shard_id}
```

**原理**：
- 将大批量分成多个 shard（每个 shard 最多 1000 条 key）
- 每个 shard 写入一个 etcd key
- 使用事务保证原子性

**优点**：
- 避免单个 value 过大
- 可以并行写入多个 shard
- 易于 Watch 和解析

**缺点**：
- 需要管理多个 key
- 需要处理部分写入失败的情况

#### 方案 C：Stream 模式（使用 etcd 的 Watch）

```
mooncake-store/deletes/stream/{sequence_id}
```

**原理**：
- 每个批量写入一个 sequence_id
- Standby Watch 连续的 sequence_id
- 支持断点续传

**优点**：
- 支持顺序处理
- 支持断点续传
- 易于实现流式处理

**缺点**：
- 需要管理 sequence_id
- 需要处理 sequence_id 跳跃的情况

### 5. 实现细节

#### 5.1 BatchedDeleteEventManager

```cpp
class BatchedDeleteEventManager {
public:
    struct BatchConfig {
        size_t max_batch_size = 1000;        // 最大批量大小
        uint32_t max_batch_interval_ms = 1000;  // 最大批量间隔（1秒）
    };

    // 添加 delete 事件到批量缓冲区
    void AddDeleteEvent(const std::string& key);

    // 强制刷新批量（立即写入）
    void Flush();

private:
    // 批量写入到 etcd
    void FlushBatch();

    // 压缩批量数据
    std::string CompressBatch(const std::vector<std::string>& keys);

    // 解压缩批量数据
    std::vector<std::string> DecompressBatch(const std::string& data);

    std::mutex mutex_;
    std::vector<std::string> pending_keys_;
    std::chrono::steady_clock::time_point last_flush_time_;
    BatchConfig config_;
    std::thread flush_thread_;
    std::atomic<bool> running_{false};
};
```

#### 5.2 批量写入逻辑

```cpp
void BatchedDeleteEventManager::FlushBatch() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (pending_keys_.empty()) {
        return;
    }

    // 压缩数据
    std::string compressed_data = CompressBatch(pending_keys_);
    
    // 检查大小限制
    if (compressed_data.size() > kMaxEtcdValueSize) {
        // 分片写入
        FlushBatchSharded(pending_keys_);
    } else {
        // 单个 key 写入
        FlushBatchSingle(compressed_data);
    }
    
    pending_keys_.clear();
    last_flush_time_ = std::chrono::steady_clock::now();
}
```

#### 5.3 Standby 端处理

```cpp
class DeleteEventWatcher {
public:
    // Watch etcd delete events
    void WatchDeleteEvents();

    // 处理批量 delete 事件
    void HandleBatchDeleteEvent(const std::string& batch_data);

private:
    // 解压缩并应用 delete 事件
    void ApplyDeleteEvents(const std::vector<std::string>& keys);
};
```

## 方案评估

### 优点

1. **减少 etcd 压力**
   - 从 130,000 次/秒 → 约 130 次/秒（批量 1000 条）
   - 减少 1000 倍写入压力

2. **保持高可靠性**
   - 仍然使用 etcd 的强一致性
   - Standby 可以通过 Watch 实时获取

3. **延迟可控**
   - 批量间隔可配置（如 1 秒）
   - 高吞吐时立即写入（大小阈值）

4. **压缩减少存储**
   - 使用压缩格式减少 etcd value 大小
   - 可以存储更多 delete 事件

### 缺点和挑战

1. **延迟问题**
   - 批量写入会有延迟（最多 1 秒）
   - 如果 Primary 在批量写入前崩溃，可能丢失部分 delete 事件

2. **数据丢失风险**
   - 如果 Primary 在批量写入前崩溃，缓冲区中的 delete 事件会丢失
   - **解决方案**：使用持久化缓冲区（如 DragonflyDB）或定期 checkpoint

3. **etcd 容量限制**
   - etcd value 大小限制（1.5MB）
   - 需要分片处理大批量

4. **压缩开销**
   - 压缩/解压缩有 CPU 开销
   - 需要权衡压缩率和性能

5. **Standby 处理复杂度**
   - 需要解压缩批量数据
   - 需要处理分片数据

### 与当前方案对比

| 特性 | 当前方案（不写入 etcd） | 新方案（批量写入 etcd） |
|------|------------------------|------------------------|
| **可靠性** | 中等（依赖租约同步） | 高（etcd 强一致性） |
| **延迟** | 0（实时） | 1 秒（批量延迟） |
| **etcd 压力** | 0 | 低（批量写入） |
| **数据丢失风险** | 低（Standby 自己决定） | 中等（批量缓冲区可能丢失） |
| **实现复杂度** | 低 | 中等 |
| **Standby 一致性** | 可能不一致（租约时间差） | 强一致（etcd 保证） |

## 推荐方案

### 混合方案（推荐）

**核心思想**：
1. **显式 Delete 事件**：立即写入 etcd（保持当前设计）
2. **驱逐 Delete 事件**：批量压缩写入 etcd（新方案）

**实现策略**：
- 使用**混合策略**（大小阈值 + 时间窗口）
  - 大小阈值：1000 条
  - 时间窗口：1 秒
- 使用**简单列表格式**（中等数量 key）
  - 如果 key 数量 > 1000，自动分片
- 使用**分片 Key** 避免单个 value 过大
- 添加**持久化缓冲区**（可选）
  - 使用 DragonflyDB 作为缓冲区
  - 定期 checkpoint 到 etcd

### 实施步骤

#### Phase 1：基础批量写入（低风险）

1. 实现 `BatchedDeleteEventManager`
2. 使用简单列表格式
3. 使用时间窗口批量（1 秒）
4. 单个 etcd key 写入

#### Phase 2：优化批量策略（中风险）

1. 添加大小阈值
2. 实现分片写入
3. 添加压缩格式

#### Phase 3：持久化缓冲区（可选，高风险）

1. 使用 DragonflyDB 作为缓冲区
2. 定期 checkpoint 到 etcd
3. 故障恢复机制

## 总结

### 方案可行性：✅ **可行**

**优点**：
- 大幅减少 etcd 压力（1000 倍减少）
- 保持高可靠性（etcd 强一致性）
- 延迟可控（1 秒内）

**需要注意**：
- 批量延迟（最多 1 秒）
- 数据丢失风险（需要持久化缓冲区）
- etcd 容量限制（需要分片）

### 建议

1. **先实现 Phase 1**（基础批量写入）
   - 验证方案可行性
   - 评估性能影响

2. **根据实际效果决定是否继续**
   - 如果效果良好，继续 Phase 2
   - 如果效果不佳，考虑其他方案

3. **关键指标**：
   - etcd 写入 QPS
   - Standby 同步延迟
   - 数据丢失率

