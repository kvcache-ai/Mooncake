# Standby 服务集成方案

## 问题描述

在现有代码实现中，Standby Master 在 `MasterServiceSupervisor::Start()` 中只是阻塞等待 leader 失效（`WatchUntilDeleted`），没有运行 Standby 服务来同步 OpLog 和恢复 metadata。

### 现有代码的问题

```cpp
// MasterServiceSupervisor::Start()
mv_helper.ElectLeader(config_.local_hostname, view_version, lease_id);
// 这里会阻塞等待 leader 失效，期间 Standby 什么都不做
```

**问题**：
1. Standby 在等待期间不执行任何操作
2. 没有 watch etcd 的 OpLog
3. 没有实时恢复 metadata
4. 提升为 Primary 时，metadata 可能不完整

### 我们方案的需求

根据基于 etcd 的 OpLog 同步方案，Standby 需要：
1. **Watch etcd 的 OpLog**：实时接收 Primary 写入的 OpLog 事件
2. **实时恢复 metadata**：将 OpLog 应用到本地 metadata store
3. **在等待选举期间持续运行**：即使不是 leader，也要保持数据同步

## 解决方案

### 核心思路

**在 Standby 模式下并行运行 Standby 服务**：
- 检测到有 leader 时，启动 Standby 服务
- Standby 服务 watch etcd OpLog 并实时恢复 metadata
- 选举成功后，停止 Standby 服务并提升为 Primary

### 架构设计

```
┌─────────────────────────────────────────────────────────┐
│         MasterServiceSupervisor::Start()                │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  1. 检查当前是否有 leader                               │
│     - GetMasterView()                                   │
│     - 如果有 leader 且不是自己 → Standby 模式          │
│     - 如果没有 leader → 直接选举                        │
└─────────────────────────────────────────────────────────┘
         │
         ├─ 有 leader (Standby 模式)
         │   │
         │   ▼
         │   ┌─────────────────────────────────────────────┐
         │   │  2. 启动 Standby 服务                       │
         │   │     - 创建 HotStandbyService                │
         │   │     - 启动 ReplicationLoop (watch etcd)     │
         │   │     - 启动 VerificationLoop                 │
         │   └─────────────────────────────────────────────┘
         │           │
         │           ▼
         │   ┌─────────────────────────────────────────────┐
         │   │  3. 阻塞等待 leader 失效                    │
         │   │     - ElectLeader() (WatchUntilDeleted)    │
         │   │     - 期间 Standby 服务持续运行             │
         │   └─────────────────────────────────────────────┘
         │
         └─ 没有 leader (直接选举)
             │
             ▼
┌─────────────────────────────────────────────────────────┐
│  4. 选举成功                                             │
│     - 停止 Standby 服务（如果正在运行）                  │
│     - 检查是否准备好提升                                  │
│     - 等待 5 秒防止 split-brain                          │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  5. 提升为 Primary                                       │
│     - 调用 HotStandbyService::Promote()                 │
│     - 初始化所有对象的 lease                             │
│     - 创建 WrappedMasterService                          │
│     - 启动 RPC 服务器                                    │
└─────────────────────────────────────────────────────────┘
```

## 实现设计

### 1. 修改 MasterServiceSupervisor::Start()

```cpp
int MasterServiceSupervisor::Start() {
    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server.init_ibv();
        }

        LOG(INFO) << "Init leader election helper...";
        MasterViewHelper mv_helper;
        if (mv_helper.ConnectToEtcd(config_.etcd_endpoints) != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd endpoints: "
                       << config_.etcd_endpoints;
            return -1;
        }

        // 【新增】检查当前是否有 leader
        ViewVersionId current_version = 0;
        std::string current_master;
        auto ret = mv_helper.GetMasterView(current_master, current_version);
        
        // 【新增】如果有 leader 且不是自己，启动 Standby 服务
        std::unique_ptr<HotStandbyService> standby_service = nullptr;
        if (ret == ErrorCode::OK && current_master != config_.local_hostname) {
            LOG(INFO) << "Current leader: " << current_master 
                      << ", starting Standby service...";
            
            // 创建并启动 Standby 服务
            HotStandbyConfig standby_config;
            standby_config.standby_id = config_.local_hostname;
            standby_config.primary_address = current_master;
            standby_config.etcd_endpoints = config_.etcd_endpoints;
            standby_config.cluster_id = config_.cluster_id;
            standby_config.enable_verification = true;
            standby_config.max_replication_lag_entries = 1000;
            
            standby_service = std::make_unique<HotStandbyService>(standby_config);
            auto err = standby_service->Start(current_master);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to start Standby service: " << err;
                standby_service.reset();
            } else {
                LOG(INFO) << "Standby service started, watching OpLog from etcd";
            }
        }

        // 尝试选举（如果有 leader，会阻塞等待；如果没有，立即选举）
        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        mv_helper.ElectLeader(config_.local_hostname, view_version, lease_id);

        // 【新增】停止 Standby 服务（如果正在运行）
        if (standby_service) {
            LOG(INFO) << "Stopping Standby service before promotion...";
            standby_service->Stop();
            
            // 【新增】检查是否准备好提升
            if (!standby_service->IsReadyForPromotion()) {
                LOG(WARNING) << "Standby is not ready for promotion, "
                            << "lag: " << standby_service->GetSyncStatus().lag_entries
                            << " entries, but proceeding anyway due to leader election";
            }
        }

        // 防止 split-brain
        const int waiting_time = ETCD_MASTER_VIEW_LEASE_TTL;
        std::this_thread::sleep_for(std::chrono::seconds(waiting_time));

        LOG(INFO) << "Starting master service as Primary...";
        
        // 【新增】如果 Standby 服务存在，使用它来初始化 MasterService
        std::unique_ptr<MasterService> promoted_service = nullptr;
        if (standby_service) {
            promoted_service = standby_service->Promote();
            if (!promoted_service) {
                LOG(ERROR) << "Failed to promote Standby to Primary";
                // 继续使用新的 MasterService，但 metadata 可能不完整
            } else {
                LOG(INFO) << "Successfully promoted Standby to Primary";
            }
        }
        
        // 创建 WrappedMasterService
        // 注意：这里需要将 promoted_service 的 metadata 复制到新的 MasterService
        // 或者修改 WrappedMasterService 的构造方式，支持从 promoted_service 初始化
        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(config_, view_version));
        
        // TODO: 如果 promoted_service 存在，需要将其 metadata 复制到 wrapped_master_service
        // 这需要修改 WrappedMasterService 或 MasterService 的接口
        
        mooncake::RegisterRpcService(server, wrapped_master_service);

        // Start a thread to keep the leader alive
        auto keep_leader_thread =
            std::thread([&server, &mv_helper, lease_id]() {
                mv_helper.KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        async_simple::Future<coro_rpc::err_code> ec =
            server.async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            auto etcd_err = EtcdHelper::CancelKeepAlive(lease_id);
            if (etcd_err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to cancel keep leader alive: "
                           << etcd_err;
            }
            keep_leader_thread.join();
            return -1;
        }
        
        // Block until the server is stopped
        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        // If the server is closed due to internal errors, we need to manually
        // stop keep leader alive.
        auto etcd_err = EtcdHelper::CancelKeepAlive(lease_id);
        LOG(INFO) << "Cancel keep leader alive: " << etcd_err;
        keep_leader_thread.join();
    }
    return 0;
}
```

### 2. 修改 HotStandbyService::ReplicationLoop()

```cpp
void HotStandbyService::ReplicationLoop() {
    LOG(INFO) << "Replication loop started";

    // 【新增】创建 OpLogWatcher（使用 etcd Watch）
    OpLogWatcher oplog_watcher(
        config_.etcd_endpoints,
        config_.cluster_id,
        this);  // HotStandbyService 作为 OpLogApplier

    // 【新增】从上次处理的 sequence_id 开始读取历史 OpLog
    uint64_t start_seq_id = applied_seq_id_.load() + 1;
    if (start_seq_id > 1) {
        std::vector<OpLogEntry> historical_entries;
        if (oplog_watcher.ReadOpLogSince(start_seq_id, historical_entries)) {
            LOG(INFO) << "Read " << historical_entries.size() 
                      << " historical OpLog entries from sequence_id " 
                      << start_seq_id;
            
            // 应用历史 OpLog
            for (const auto& entry : historical_entries) {
                ApplyOpLogEntry(entry);
            }
        } else {
            LOG(WARNING) << "Failed to read historical OpLog, "
                        << "may need to perform full snapshot sync";
        }
    }

    // 【新增】启动 etcd Watch
    oplog_watcher.Start();
    is_connected_.store(true);
    LOG(INFO) << "OpLog watcher started, watching etcd for new OpLog entries";

    while (running_.load()) {
        // OpLogWatcher 会在后台线程中处理 Watch 事件
        // 当收到新 OpLog 时，会调用 ApplyOpLogEntry()
        
        // 定期检查同步状态
        auto status = GetSyncStatus();
        if (status.lag_entries > config_.max_replication_lag_entries) {
            LOG(WARNING) << "Replication lag is high: " 
                        << status.lag_entries << " entries";
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    // 【新增】停止 Watch
    oplog_watcher.Stop();
    is_connected_.store(false);
    LOG(INFO) << "Replication loop stopped";
}
```

### 3. 实现 OpLogWatcher（基于 etcd Watch）

```cpp
class OpLogWatcher {
public:
    OpLogWatcher(const std::string& etcd_endpoints,
                 const std::string& cluster_id,
                 OpLogApplier* applier)
        : etcd_endpoints_(etcd_endpoints),
          cluster_id_(cluster_id),
          applier_(applier),
          etcd_oplog_store_(etcd_endpoints, cluster_id) {
        etcd_prefix_ = "mooncake-store/oplog/" + cluster_id + "/";
    }

    void Start() {
        if (running_.load()) {
            LOG(WARNING) << "OpLogWatcher is already running";
            return;
        }
        
        running_.store(true);
        watch_thread_ = std::thread(&OpLogWatcher::WatchOpLogThreadFunc, this);
        LOG(INFO) << "OpLogWatcher started";
    }

    void Stop() {
        if (!running_.load()) {
            return;
        }
        
        running_.store(false);
        if (watch_thread_.joinable()) {
            watch_thread_.join();
        }
        LOG(INFO) << "OpLogWatcher stopped";
    }

    bool ReadOpLogSince(uint64_t start_seq_id, 
                       std::vector<OpLogEntry>& entries) {
        return etcd_oplog_store_.ReadOpLogSince(start_seq_id, 1000, entries);
    }

private:
    void WatchOpLogThreadFunc() {
        LOG(INFO) << "OpLog watch thread started";
        
        // 从上次处理的 sequence_id 开始 Watch
        uint64_t start_seq_id = last_processed_sequence_id_ + 1;
        std::string watch_prefix = etcd_prefix_;
        
        while (running_.load()) {
            try {
                // 使用 etcd Watch 监听 OpLog 变化
                // 这里需要使用 etcd 的 Watch API
                // 假设 EtcdHelper 提供了 WatchWithPrefix 方法
                auto watch_result = EtcdHelper::WatchWithPrefix(
                    watch_prefix.c_str(), 
                    watch_prefix.size(),
                    [this](const EtcdWatchEvent& event) {
                        HandleWatchEvent(event);
                    });
                
                if (!watch_result) {
                    LOG(ERROR) << "Watch failed, retrying...";
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception in watch thread: " << e.what();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        
        LOG(INFO) << "OpLog watch thread stopped";
    }

    void HandleWatchEvent(const EtcdWatchEvent& event) {
        if (event.type == EtcdWatchEventType::PUT) {
            // 解析 OpLog Entry
            OpLogEntry entry;
            if (DeserializeOpLogEntry(event.value, entry)) {
                // 应用 OpLog
                if (applier_->ApplyOpLogEntry(entry)) {
                    last_processed_sequence_id_ = entry.sequence_id;
                    VLOG(2) << "Applied OpLog entry: sequence_id=" 
                           << entry.sequence_id
                           << ", op_type=" << static_cast<int>(entry.op_type)
                           << ", key=" << entry.object_key;
                } else {
                    LOG(WARNING) << "Failed to apply OpLog entry: sequence_id=" 
                                << entry.sequence_id;
                }
            } else {
                LOG(ERROR) << "Failed to deserialize OpLog entry from key: "
                          << event.key;
            }
        } else if (event.type == EtcdWatchEventType::DELETE) {
            // OpLog 被清理，记录日志
            VLOG(1) << "OpLog entry deleted: " << event.key;
        }
    }

    std::string etcd_endpoints_;
    std::string cluster_id_;
    std::string etcd_prefix_;
    OpLogApplier* applier_;
    EtcdOpLogStore etcd_oplog_store_;

    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
};
```

### 4. HotStandbyService 实现 OpLogApplier 接口

```cpp
class HotStandbyService : public OpLogApplier {
public:
    // 实现 OpLogApplier 接口
    bool ApplyOpLogEntry(const OpLogEntry& entry) override {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 检查时序性
        if (!CheckSequenceOrder(entry)) {
            LOG(WARNING) << "Sequence order violation for entry: "
                        << entry.sequence_id;
            return false;
        }
        
        // 应用 OpLog
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
        
        applied_seq_id_.store(entry.sequence_id);
        return true;
    }

private:
    void ApplyPutEnd(const OpLogEntry& entry) {
        // 从 metadata_store_ 创建或更新 metadata
        // 这里需要实现完整的 metadata 恢复逻辑
        if (metadata_store_) {
            metadata_store_->entry_count++;
        }
    }

    void ApplyPutRevoke(const OpLogEntry& entry) {
        // 处理 PUT_REVOKE
        // ...
    }

    void ApplyRemove(const OpLogEntry& entry) {
        // 从 metadata_store_ 删除 metadata
        if (metadata_store_ && metadata_store_->entry_count > 0) {
            metadata_store_->entry_count--;
        }
    }

    bool CheckSequenceOrder(const OpLogEntry& entry) {
        // 检查全局序列号
        if (entry.sequence_id <= applied_seq_id_.load()) {
            LOG(WARNING) << "Received out-of-order entry: "
                        << "expected > " << applied_seq_id_.load()
                        << ", got " << entry.sequence_id;
            return false;
        }
        
        // 检查 key 级别的序列号
        // 这里需要维护 key_sequence_map_
        // ...
        
        return true;
    }
};
```

## 关键设计点

### 1. Standby 服务生命周期

```
启动阶段：
1. 检测到有 leader → 创建 HotStandbyService
2. 启动 ReplicationLoop → 读取历史 OpLog → 启动 Watch
3. 启动 VerificationLoop（可选）

运行阶段：
1. OpLogWatcher 持续 watch etcd
2. 收到新 OpLog → 调用 ApplyOpLogEntry
3. 实时更新 metadata_store_

提升阶段：
1. 选举成功 → 停止 Standby 服务
2. 调用 Promote() → 初始化 lease → 创建 MasterService
3. 启动 Primary 服务
```

### 2. 历史 OpLog 读取

**策略**：
- 从 `applied_seq_id_ + 1` 开始读取
- 如果 `applied_seq_id_` 为 0，说明是首次启动，需要从快照开始
- 批量读取（每次 1000 条），避免一次性读取过多

**实现**：
```cpp
uint64_t start_seq_id = applied_seq_id_.load() + 1;
if (start_seq_id == 1) {
    // 首次启动，需要从快照恢复
    // 或者从 sequence_id = 1 开始读取所有历史
}
std::vector<OpLogEntry> entries;
oplog_watcher.ReadOpLogSince(start_seq_id, entries);
```

### 3. etcd Watch 实现

**关键点**：
- 使用 `WatchWithPrefix` 监听 OpLog 前缀
- 处理 Watch 断开和重连
- 处理序列号不连续的情况

**Watch 前缀**：
```
mooncake-store/oplog/{cluster_id}/
```

### 4. 时序保证

**全局序列号**：
- 检查 `entry.sequence_id > applied_seq_id_`
- 如果序列号不连续，缓存待处理

**Key 级别序列号**：
- 维护 `key_sequence_map_` 记录每个 key 的最后 sequence_id
- 检查 `entry.key_sequence_id > key_sequence_map_[key]`

## 与现有方案的集成

### 1. 与快照机制集成

**场景**：Standby 首次启动或需要全量同步

**流程**：
1. 检测到 `applied_seq_id_ == 0` 或 lag 过大
2. 请求 Primary 的快照
3. 应用快照
4. 从快照的 `last_oplog_sequence_id` 开始读取增量 OpLog

### 2. 与提升机制集成

**流程**：
1. 选举成功
2. 停止 Standby 服务
3. 检查同步状态（`IsReadyForPromotion()`）
4. 调用 `Promote()` → 初始化 lease
5. 创建 MasterService 并启动

### 3. 与 OpLog 清理集成

**场景**：OpLog 被清理后，Watch 可能收到 DELETE 事件

**处理**：
- 记录警告日志
- 如果发现大量 OpLog 被删除，可能需要重新同步

## 错误处理和容错

### 1. Watch 断开

**处理**：
- 自动重连
- 从上次处理的 sequence_id 重新 Watch
- 如果重连失败，记录错误并重试

### 2. 序列号不连续

**处理**：
- 缓存待处理的条目
- 等待一段时间看是否有缺失的条目到达
- 如果超时，请求 Primary 或从 etcd 读取缺失的条目

### 3. 应用失败

**处理**：
- 记录错误日志
- 不更新 `applied_seq_id_`
- 继续处理后续条目（但可能影响一致性）

## 性能考虑

### 1. Watch 性能

- etcd Watch 是高效的，不会产生大量网络开销
- 批量处理 Watch 事件，减少锁竞争

### 2. 历史 OpLog 读取

- 批量读取（每次 1000 条）
- 并行应用（如果支持）

### 3. Metadata 更新

- 使用适当的锁粒度
- 考虑使用无锁数据结构（如果可能）

## 测试场景

### 1. 正常 Standby 运行

```
1. 启动 Standby，检测到有 leader
2. 启动 Standby 服务
3. Watch etcd OpLog
4. 实时应用 OpLog 到 metadata
5. 验证 metadata 与 Primary 一致
```

### 2. Standby 提升为 Primary

```
1. Standby 正在运行
2. Primary 失效
3. Standby 选举成功
4. 停止 Standby 服务
5. 提升为 Primary
6. 验证 metadata 完整性
```

### 3. Watch 断开重连

```
1. Standby 正在 Watch
2. etcd 连接断开
3. 自动重连
4. 从上次处理的 sequence_id 继续
5. 验证没有丢失 OpLog
```

### 4. 历史 OpLog 读取

```
1. Standby 重启
2. applied_seq_id_ = 1000
3. 读取 sequence_id >= 1001 的历史 OpLog
4. 应用历史 OpLog
5. 启动 Watch 监听新 OpLog
```

## 总结

### 核心方案

**在 Standby 模式下并行运行 Standby 服务，watch etcd OpLog 并实时恢复 metadata**

### 关键实现

1. **MasterServiceSupervisor**：检测 leader，启动/停止 Standby 服务
2. **HotStandbyService**：实现 OpLogApplier 接口，管理 Standby 生命周期
3. **OpLogWatcher**：watch etcd OpLog，处理 Watch 事件
4. **时序保证**：全局和 key 级别的序列号检查

### 优势

1. **实时同步**：Standby 实时接收并应用 OpLog
2. **数据完整性**：提升时 metadata 已完整
3. **自动恢复**：Watch 断开自动重连
4. **与现有方案兼容**：不影响现有的选举和提升逻辑

### 注意事项

1. **Watch 性能**：需要确保 etcd Watch 的性能
2. **序列号不连续**：需要处理缺失的 OpLog
3. **Metadata 恢复**：需要完整实现 metadata 的恢复逻辑
4. **提升时的数据迁移**：需要将 Standby 的 metadata 迁移到 Primary

