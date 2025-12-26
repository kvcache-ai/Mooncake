# Standby 提升为 Primary 时的 Lease 初始化方案

## 问题描述

当 Standby Master 被提升为 Primary Master 时，存在一个关键问题：**所有对象的 lease 都是 0（已过期）**。

### 问题根源

1. **OpLog 中只包含 PUT 和 DELETE 事件**
   - `PUT_END` 事件：在 Primary 上创建对象时，`lease_timeout` 被初始化为 0（立即过期）
   - `DELETE` 事件：删除对象
   - **不包含** `LEASE_RENEW` 事件（已从 OpLog 中移除）

2. **Standby 上的对象状态**
   - Standby 从 Primary 同步 OpLog，只收到 `PUT_END` 事件
   - 因此 Standby 上所有对象的 `lease_timeout` 都是 0（epoch）
   - Standby 不执行驱逐，所以不会检查 lease 是否过期

3. **提升为 Primary 后的影响**
   - 新 Primary 开始执行驱逐逻辑
   - 由于所有对象的 `lease_timeout` 都是 0，所有对象都会立即被判定为过期
   - 这会导致所有对象被立即驱逐，系统无法正常工作

### 问题场景示例

```
时间线：
1. Primary: PutEnd(key="obj1") → lease_timeout = 0
2. Primary: ExistKey(key="obj1") → lease_timeout = now + 5s (续约)
3. Standby: 同步 PUT_END 事件 → lease_timeout = 0 (没有续约信息)
4. Primary 崩溃
5. Standby 提升为 Primary
6. 新 Primary: 执行驱逐 → 所有对象 lease_timeout = 0 → 全部被驱逐 ❌
```

## 解决方案

### 方案：在提升时给所有对象授予默认租约

**核心思路**：当 Standby 被提升为 Primary 时，遍历所有 metadata，给每个对象授予一个默认的租约时间。

### 实现设计

#### 1. 在 `HotStandbyService::Promote()` 中添加 Lease 初始化逻辑

```cpp
std::unique_ptr<MasterService> HotStandbyService::Promote() {
    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion";
        return nullptr;
    }

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << applied_seq_id_.load();

    // Stop replication
    Stop();

    // 1. 创建新的 MasterService 实例
    auto master_service = std::make_unique<MasterService>(/* config */);

    // 2. 从 metadata_store_ 恢复 metadata 到新的 MasterService
    RestoreMetadataToMasterService(*master_service);

    // 3. 【关键】给所有对象授予默认租约
    InitializeLeasesForAllObjects(*master_service);

    // 4. 执行一次完整的驱逐清理（清理真正过期的对象）
    PerformFullEvictionCleanup(*master_service);

    LOG(INFO) << "Standby promoted to Primary successfully";
    return master_service;
}
```

#### 2. 实现 `InitializeLeasesForAllObjects()`

```cpp
void HotStandbyService::InitializeLeasesForAllObjects(MasterService& master_service) {
    LOG(INFO) << "Initializing leases for all objects after promotion";
    
    uint64_t default_lease_ttl = master_service.GetDefaultLeaseTtl();
    uint64_t default_soft_pin_ttl = master_service.GetDefaultSoftPinTtl();
    
    size_t initialized_count = 0;
    
    // 遍历所有 shard 中的所有 metadata
    for (auto& shard : master_service.GetMetadataShards()) {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        
        for (auto& [key, metadata] : shard.metadata) {
            // 检查 lease 是否过期（lease_timeout = 0 表示过期）
            if (metadata.IsLeaseExpired()) {
                // 授予默认租约
                metadata.GrantLease(default_lease_ttl, default_soft_pin_ttl);
                initialized_count++;
                
                VLOG(2) << "Initialized lease for key: " << key
                        << ", lease_ttl=" << default_lease_ttl;
            }
        }
    }
    
    LOG(INFO) << "Initialized leases for " << initialized_count 
              << " objects after promotion";
}
```

#### 3. 实现 `PerformFullEvictionCleanup()`

```cpp
void HotStandbyService::PerformFullEvictionCleanup(MasterService& master_service) {
    LOG(INFO) << "Performing full eviction cleanup after promotion";
    
    // 执行一次完整的驱逐，清理真正过期的对象
    // 注意：此时所有对象的 lease 都已经初始化，只有真正过期的对象才会被驱逐
    master_service.BatchEvict();
    
    LOG(INFO) << "Full eviction cleanup completed";
}
```

### 关键设计点

#### 1. 默认租约时间的选择

**选项 A：使用配置的 `default_kv_lease_ttl`**
- **优点**：简单，与正常操作一致
- **缺点**：可能给已经很久没有访问的对象也授予租约，导致内存浪费

**选项 B：使用较短的租约时间（如 1-2 秒）**
- **优点**：快速淘汰真正不活跃的对象
- **缺点**：可能误杀活跃对象

**推荐：选项 A（使用 `default_kv_lease_ttl`）**

**理由**：
1. 保守策略，避免误杀活跃对象
2. 如果对象真的不活跃，会在下次驱逐时被清理
3. 与正常操作一致，行为可预测

#### 2. 何时执行 Lease 初始化

**时机**：在 `Promote()` 方法中，在恢复 metadata 之后、开始服务请求之前

**流程**：
```
1. 停止 Standby 的复制循环
2. 创建新的 MasterService 实例
3. 恢复 metadata 到新的 MasterService
4. 【关键】初始化所有对象的 lease
5. 执行一次完整的驱逐清理
6. 开始服务请求
```

#### 3. 与驱逐清理的配合

**问题**：如果先初始化 lease，再执行驱逐，那么所有对象都有 lease，不会被驱逐？

**解答**：
- 初始化 lease 的目的是**防止误杀活跃对象**
- 驱逐清理的目的是**清理真正过期的对象**（基于 `put_start_time` 等条件）
- 实际上，在 Standby 提升时，所有对象都是"新"的（从 OpLog 恢复），所以应该都保留
- 如果某些对象在 Primary 崩溃前就已经过期，那么它们应该已经被 Primary 驱逐并产生 DELETE 事件，Standby 上不应该有这些对象

**更准确的驱逐逻辑**：
- 在 Standby 提升时，不应该基于 lease 进行驱逐
- 应该基于其他条件（如 `put_start_time` + `put_start_release_timeout_sec_`）进行清理
- 或者，在提升时**不执行驱逐**，让正常的驱逐循环来处理

**修正后的方案**：

```cpp
void HotStandbyService::Promote() {
    // ... 前面的步骤 ...
    
    // 3. 给所有对象授予默认租约
    InitializeLeasesForAllObjects(*master_service);
    
    // 4. 【可选】执行一次清理，但只清理明显无效的对象
    // 注意：不基于 lease 进行驱逐，因为所有对象的 lease 都是 0
    // 可以清理：put_start_time 过期的对象、没有完整 replica 的对象等
    CleanupInvalidObjects(*master_service);
    
    // 5. 启动 MasterService 的驱逐循环
    // 正常的驱逐循环会基于 lease 和其他条件进行驱逐
}
```

## 实现细节

### 1. 在 `MasterService` 中添加辅助方法

```cpp
class MasterService {
public:
    // 获取默认租约 TTL
    uint64_t GetDefaultLeaseTtl() const { return default_kv_lease_ttl_; }
    
    // 获取默认 Soft Pin TTL
    uint64_t GetDefaultSoftPinTtl() const { return default_kv_soft_pin_ttl_; }
    
    // 获取 metadata shards（用于遍历）
    std::vector<MetadataShard>& GetMetadataShards() { return metadata_shards_; }
    
    // ... 其他方法 ...
};
```

### 2. 在 `HotStandbyService` 中实现 Lease 初始化

```cpp
class HotStandbyService {
private:
    void InitializeLeasesForAllObjects(MasterService& master_service);
    void CleanupInvalidObjects(MasterService& master_service);
    
    // ... 其他成员 ...
};

void HotStandbyService::InitializeLeasesForAllObjects(MasterService& master_service) {
    LOG(INFO) << "Initializing leases for all objects after promotion";
    
    uint64_t default_lease_ttl = master_service.GetDefaultLeaseTtl();
    uint64_t default_soft_pin_ttl = master_service.GetDefaultSoftPinTtl();
    
    size_t initialized_count = 0;
    size_t skipped_count = 0;
    
    // 遍历所有 shard
    for (auto& shard : master_service.GetMetadataShards()) {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        
        for (auto& [key, metadata] : shard.metadata) {
            // 只初始化 lease 为 0 的对象
            if (metadata.IsLeaseExpired()) {
                metadata.GrantLease(default_lease_ttl, default_soft_pin_ttl);
                initialized_count++;
            } else {
                // 如果 lease 已经有效，说明可能是从快照恢复的，保留原值
                skipped_count++;
            }
        }
    }
    
    LOG(INFO) << "Lease initialization completed: "
              << initialized_count << " objects initialized, "
              << skipped_count << " objects skipped";
}
```

### 3. 清理无效对象（可选）

```cpp
void HotStandbyService::CleanupInvalidObjects(MasterService& master_service) {
    LOG(INFO) << "Cleaning up invalid objects after promotion";
    
    size_t cleaned_count = 0;
    auto now = std::chrono::steady_clock::now();
    
    // 遍历所有 shard
    for (auto& shard : master_service.GetMetadataShards()) {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            auto& [key, metadata] = *it;
            
            // 清理条件：
            // 1. put_start_time 过期且没有完整 replica
            // 2. 所有 replica 都无效
            bool should_cleanup = false;
            
            if (!metadata.HasCompletedReplicas() &&
                metadata.put_start_time + 
                master_service.GetPutStartReleaseTimeout() < now) {
                should_cleanup = true;
            } else if (!metadata.IsValid()) {
                should_cleanup = true;
            }
            
            if (should_cleanup) {
                VLOG(1) << "Cleaning up invalid object: " << key;
                it = shard.metadata.erase(it);
                cleaned_count++;
            } else {
                ++it;
            }
        }
    }
    
    LOG(INFO) << "Cleaned up " << cleaned_count << " invalid objects";
}
```

## 边界情况处理

### 1. 从快照恢复的场景

**场景**：Standby 从快照恢复，快照中可能包含 lease 信息

**处理**：
- 如果快照中包含 lease 信息，保留原值
- 如果快照中 lease 为 0，则初始化

**实现**：
```cpp
if (metadata.IsLeaseExpired()) {
    // lease 为 0，需要初始化
    metadata.GrantLease(default_lease_ttl, default_soft_pin_ttl);
} else {
    // lease 已有效，可能是从快照恢复的，保留原值
    skipped_count++;
}
```

### 2. 提升过程中的并发访问

**场景**：提升过程中，可能有其他线程访问 metadata

**处理**：
- 使用 `std::unique_lock` 保护每个 shard
- 提升过程应该是原子的（停止 Standby，创建 Primary）

### 3. 提升失败的处理

**场景**：提升过程中发生错误

**处理**：
- 记录错误日志
- 返回 `nullptr`，表示提升失败
- Standby 继续运行，等待下次提升机会

## 性能考虑

### 1. 遍历所有对象的开销

**影响**：
- 如果对象数量很大（如 100 万），遍历所有对象可能需要几秒

**优化**：
- 使用多线程并行处理不同 shard
- 批量处理，减少锁竞争

**实现**：
```cpp
void HotStandbyService::InitializeLeasesForAllObjects(MasterService& master_service) {
    auto& shards = master_service.GetMetadataShards();
    
    // 并行处理所有 shard
    std::vector<std::thread> threads;
    for (size_t i = 0; i < shards.size(); ++i) {
        threads.emplace_back([&shards, i, &master_service]() {
            auto& shard = shards[i];
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            
            for (auto& [key, metadata] : shard.metadata) {
                if (metadata.IsLeaseExpired()) {
                    metadata.GrantLease(
                        master_service.GetDefaultLeaseTtl(),
                        master_service.GetDefaultSoftPinTtl());
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
}
```

### 2. 提升时间窗口

**影响**：
- 提升过程需要时间，期间系统不可用

**优化**：
- 尽量减少提升时间
- 可以考虑在 Standby 阶段就预先初始化 lease（但这样 Standby 也需要维护 lease）

## 测试场景

### 1. 正常提升场景

```
1. Standby 同步了 1000 个对象的 PUT_END 事件
2. 所有对象的 lease_timeout = 0
3. Primary 崩溃
4. Standby 提升为 Primary
5. 验证：所有对象的 lease_timeout > now
6. 验证：系统可以正常服务请求
```

### 2. 从快照恢复的场景

```
1. Standby 从快照恢复，快照中包含 lease 信息
2. 部分对象的 lease_timeout > 0（从快照恢复）
3. 部分对象的 lease_timeout = 0（新同步的）
4. Standby 提升为 Primary
5. 验证：lease_timeout = 0 的对象被初始化
6. 验证：lease_timeout > 0 的对象保留原值
```

### 3. 大量对象的场景

```
1. Standby 同步了 100 万个对象
2. Standby 提升为 Primary
3. 验证：所有对象的 lease 都被初始化
4. 验证：提升时间在可接受范围内（< 10 秒）
```

## 总结

### 核心方案

**在 Standby 提升为 Primary 时，给所有 lease 为 0 的对象授予默认租约时间**

### 关键点

1. **时机**：在 `Promote()` 中，恢复 metadata 之后、开始服务之前
2. **租约时间**：使用 `default_kv_lease_ttl`（保守策略）
3. **清理**：可选，清理明显无效的对象（不基于 lease）
4. **性能**：并行处理多个 shard，减少提升时间

### 优势

1. **简单可靠**：逻辑清晰，易于实现和测试
2. **保守策略**：避免误杀活跃对象
3. **与现有机制兼容**：使用现有的 `GrantLease` 方法

### 注意事项

1. **提升时间**：如果对象数量很大，提升可能需要几秒
2. **内存影响**：给所有对象授予租约，可能暂时保留一些不活跃对象
3. **后续清理**：正常的驱逐循环会在后续清理不活跃对象

