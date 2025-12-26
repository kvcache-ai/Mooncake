# 基于 etcd 的 OpLog 主备同步完整方案 RFC

## 1. 方案背景

### 1.1 当前系统架构

Mooncake Store 是一个高性能的分布式 KV 缓存存储引擎，专为 LLM 推理场景设计。系统采用 Master-Client 架构：

- **Master Service**：负责管理对象元数据（metadata）、空间分配、节点管理等
- **Client**：作为存储服务器提供内存段，同时作为客户端处理应用请求

### 1.2 高可用性需求

当前系统支持两种部署模式：

1. **默认模式**：单 Master 节点，部署简单但存在单点故障风险
2. **高可用模式（不稳定）**：多 Master 节点通过 etcd 进行 Leader 选举

**问题**：
- 高可用模式虽然实现了 Leader 选举，但 Standby Master 在等待期间不执行任何操作
- 没有实现数据同步机制，Standby 提升为 Primary 时 metadata 可能不完整
- 缺乏可靠的主备数据同步方案

### 1.3 业务场景

在 LLM 推理场景中，Master Service 需要：
- **高可用性**：Master 故障时能够快速切换，最小化服务中断时间
- **数据一致性**：Standby 必须与 Primary 保持数据一致
- **快速恢复**：故障恢复后能够快速恢复服务，无需长时间的数据重建

### 1.4 现有方案的问题

1. **无数据同步**：Standby Master 在等待选举期间不执行任何数据同步操作
2. **元数据丢失风险**：Primary 故障后，Standby 提升时 metadata 可能不完整
3. **恢复时间长**：需要重新从 Client 节点收集 metadata，恢复时间长
4. **数据不一致**：无法保证 Standby 与 Primary 的数据一致性

## 2. Goals（目标）

### 2.1 主要目标

1. **实现可靠的主备数据同步**
   - Primary Master 的所有 metadata 变更操作同步到 Standby Master
   - 保证 Standby 与 Primary 的数据一致性

2. **快速故障恢复**
   - Primary 故障后，Standby 能够快速提升为 Primary
   - 提升时 metadata 完整，无需长时间重建

3. **最小化 OpLog 大小**
   - 只记录关键的状态变更操作（PUT、DELETE）
   - 不记录租约续约等高频但非关键操作

4. **与现有系统集成**
   - 与现有的快照机制集成
   - 与现有的 Leader 选举机制集成
   - 不影响现有功能的正常运行

### 2.2 非功能性目标

1. **性能**：OpLog 同步不应显著影响 Primary 的性能
2. **可靠性**：利用 etcd 的强一致性保证数据可靠性
3. **可扩展性**：支持多个 Standby Master
4. **可维护性**：实现简单，易于理解和维护

## 3. Proposal（提案）

### 3.1 核心设计思路

**使用 etcd 作为中间可靠性组件，实现 OpLog 主备同步**：

1. **OpLog 机制**：Primary Master 记录所有状态变更操作到 OpLog
2. **etcd 存储**：OpLog 写入 etcd，利用 etcd 的强一致性和持久化能力
3. **Watch 机制**：Standby Master 通过 etcd Watch 机制实时接收 OpLog
4. **顺序保证**：通过全局 sequence_id 和 key 级别的 key_sequence_id 保证操作顺序

### 3.2 架构设计

#### 3.2.1 整体架构

整体架构图展示了 Primary Master、etcd Cluster 和 Standby Master 之间的交互关系：

![PlantUML Diagram](https://uml.planttext.com/plantuml/png/XLL1QnD15Bu7yX_6zgA149KYmOEqb0H5YyKSF1GfazrficHtPfsTBSGGi62Yr8g1Hb4R2R4jzD9O9OYchVwPx2QU_0lEx6oQtMwgUmXllldUUr_UjpCxRp58cMteW9WwAIIBX2KvXDLyEGcfKjGOKfXDKJnsYHMHWO2fGmt7OrP9moQaq01vg9GAbDXONIGweM0swpr1Ya8Cas24MOwLTGGeBmbnGKT1ZehMeAspBE4ixGa2rwx7O_6OoOl30W8porGp82s39MWnH6V0R2QTdSkcGIKU0_mvwm1M92E7wBgce4S0MY24HFZtpNkai0GnRqCzUX28i3DCKJr2ZX4gouSXcI5_Gur1CdahL1lS1AFkaNFwnjr-DJXjoPGGGMI4g_CSf_xUgUrBOZnM3Kq9SJ9Or6r_Hjo6kSoDyOnKo60UMWYi29edNGJdQ-Ia-vD9Pwzcqvd_JZfdcoAmY1pYP1b9kqsOtoDeqWITxj13o9IYxv0VJoSkcAQk-KG_ZYf738fnJ4mC8K4F9t_4isCYKrZH-Eni7gISZPPx-4dI0_k2xYlbN2yQkoQOuor1ytMAaltci7aGv8tt12-aahFTdPxxzWWOFknRUUwL4OdUYt7-tV70QIe7_PU3us-Y52QCdrUjK6I0h4LEHY8nsjWQzNOJYJyd7mIG1CDcsttH01PwR2Eie5LD3U4bL5wDxXtttCqzfrvp3jyDJxQT-bTdgy_bOHM8_b4T0LkdI71tdxhj_T-TljD_BH5dxzcmKH_y-7A6kDzh71dzUkwsskx7pd2d-wz-aGiaaP1dDj1rsMOPh5w-8bSFa47MqNYLuNbC8rYiB-uY3wCeVXUL-TNmSzJj11gal0iwLL7ayURJgwOgWLbMBwRfa26BoNtfyCBodR2KURxWNm4H_WK0)

**架构说明**：
- **Primary Master**：负责处理客户端请求，记录 OpLog 并写入 etcd
- **etcd Cluster**：作为中间存储，提供强一致性和 Watch 机制
- **Standby Master**：通过 Watch etcd 实时接收 OpLog，并应用到本地 metadata store

#### 3.2.2 数据流图

数据流图展示了从 Client 请求到 Standby 同步的完整流程：

![PlantUML Diagram](https://uml.planttext.com/plantuml/png/VLH1QnD15Bulx7zuraiAJNfVI6c8wKLZGcBfHGYJtN6pP3AxpaugGJnu4xHOi0Y284K4BxLUl1JyDtPZ_uLlPdSdiqamX_3oydtptllUDtEOIYBaVCOWJbWSrWCYIVqPYr-upZqveJCA2ICHTvrq6l6423A3CV6deOZdF6Z7B1Pm_qX_R4XAdyyfzscNfYa9QOj58GUVSac5wxWEyINosYp2ZEiWHKP-b10kOQSleXaH2-YI5C4xG58eKcl0Nl8e3hk4u_4vhAVwxuPY3TUHVg2nGwn9DLAbz2_NKUEEIKg1OcvRXHCY_KbHeOYtmLf9WjFai29UWmqbuS6uCbYHKeeqcz0_VZBgF7u0sOUpFxy_PxzUBx-_XMPJ_Pih1VM3KWiF-dFPuK5jIXTxq6WqThMeqIcHTALNgINoId4yrHr5Ob5j3_04cxnIiOogzEN5b-pDkLdmA0gUyYA79usiVFK4exa79oAILAjMmwb4fRor6XCgkbgFfoI2VUtJ_PTOwPL5pFUdlg5UBJUS-pxQ47TDrz1MXSgCsnXMOwknx8LK9hU8Aq7DEf2MRtHxARC_ROlIDxVdxxAhRnLRvDCUbBxqyW0wfyeijUpZJz0gs_eQ2nU1eXT-rTPW2qtfgBriRiSukmWgxFQ4-jDXeK9F15JK59T9kBkykRrvdrrzNLx-S1t0ZyKlvlFWEC7BY2-69EfIsivM3DE3kJGgMufJjnincYg4fQjXKeONFc_gxkBJt-lhZQRCMOEOCVNUjNWmeFWIBcgx6-3ScmDAydVcA1OFgS4PHveZDGYKmX5D_rDPbylHs38FBDNjdMzpaDcJbJERTvr3F0rVV1N-0m00)

**流程说明**：
1. Client 发送 `PutEnd` 请求到 Primary Master
2. Primary Master 通过 `OpLogManager` 记录操作，生成 sequence_id
3. `EtcdOpLogStore` 将 OpLog 写入 etcd
4. etcd 通过 Watch 机制通知 Standby Master
5. `OpLogWatcher` 接收事件并传递给 `OpLogApplier`
6. `OpLogApplier` 检查顺序并应用到 Standby 的 metadata store

#### 3.2.3 故障切换流程

故障切换流程图展示了从 Primary 故障到 Standby 提升为 Primary 的完整过程：

![PlantUML Diagram](https://uml.planttext.com/plantuml/png/dPNFQnD15CVl2_i_FEkbFRJuynA8j6X4iCL24IzUfhlFTEbcTvsTXFQMWlqHRMilbL8Y9IhMWwq8Ah6A_MUoa-I_S6PszbSiYERqvittEtdlyuRPwP0Hoker5_p0zQkJJuZZ-Wsaao4-hQDdeMbSOajOGmXSudYc4IuxNa0egS4YiPQhrAzxzctVzIbSlgj-UKboo1o68QdYZEjKFR3GOqXDmpI4Y3cM4n2FmTWyTMg4hi8S2SNs690GTCeqRCB88WaHa5dsY6-14SzUBFXqQaGO2nQGDXmB5-g1349VEzBbYEcUp_HfsgZaMNP4_Y2OzQkF2BEMT2awlaWs4mIkesKwbb3APU0dRwDkTt2-D-Xi3m--yTElK2xBlOJHv2r5eWJt4GD1jO4mYuAFQSYqtCuQAiKrI86D5CQZauEe_M72D8Z5d0PXM6W-Y-KfMPyb2PKcg_6yFGyZYwLTDw-z1LFAHGTPIt6rYb3MJdgIoaEb8UvGM31hWYKLh2fPnMEqM6gAMGUALDBWmq1SCt5L6P7NJVfi_DEPowKzv79v6Bbq7h4QSJ99lhy-F6oFZdT5ir13XSfAu52qOJmMJrmyPJtVE-WY4sA5w3-6x0V_FjpOymza58BaBFvoBzhPx7NFKYWnZMALQOdprA_v30jLfWVdwaiDqTRhwFX5jFqgnldOuztr_jx6u7oJju-WfkTTyCRqAovQBCPQ-BVu4KfdaFoF7e1oeLteFNRaYyiDBdtuV1kBb-Ql5yaJ840-rvaMuEeKH6jjVl8c8zpUYPvtvDwrAHYkxKIx6xpLvErMhdk0wyBtwJku4bBv2lGFdudbu7E7xsxrphQ6Fmu6f-_wnqVzi_TIVMCAUEjOF52zRfD_x4Idstp_Y_2aHqACMMflYfD_DiKG4aR3PglN_IKOUZR87cGlqs8XlaFok_0R)




```
**流程说明**：
1. **正常运行**：Primary 保持 Lease，Standby 通过 Watch 持续同步 OpLog
2. **Primary 故障**：Primary 的 Lease 过期，etcd 通知 Standby
3. **Standby 提升**：停止 Standby 服务，初始化 Lease，清理过期 metadata，开始 Leader 选举
```

### 3.3 核心组件设计

#### 3.3.1 OpLogManager（Primary 端）

**职责**：

- 记录所有状态变更操作（PUT_END、PUT_REVOKE、REMOVE）
- 生成全局 sequence_id 和 key 级别的 key_sequence_id
- 维护内存缓冲区（用于快速查询）

**关键方法**：
```cpp
class OpLogManager {
    uint64_t Append(OpType type, const std::string& key, 
                    const std::string& payload = "");
    std::vector<OpLogEntry> GetEntriesSince(uint64_t since_seq_id, 
                                            size_t limit = 1000) const;
    uint64_t GetLastSequenceId() const;
};
```

#### 3.3.2 EtcdOpLogStore（Primary 端）

**职责**：
- 将 OpLog 写入 etcd
- 更新最新的 sequence_id
- 记录快照对应的 sequence_id
- 清理旧的 OpLog

**etcd Key 设计**：
- OpLog Entry: `mooncake-store/oplog/{cluster_id}/{sequence_id}`
- Latest Sequence ID: `mooncake-store/oplog/{cluster_id}/latest`
- Snapshot Sequence ID: `mooncake-store/oplog/{cluster_id}/snapshot/{snapshot_id}/sequence_id`

#### 3.3.3 OpLogWatcher（Standby 端）

**职责**：
- Watch etcd 的 OpLog 变化
- 读取历史 OpLog（用于初始同步）
- 处理 Watch 事件并传递给 OpLogApplier

**关键方法**：
```cpp
class OpLogWatcher {
    void Start();
    void Stop();
    bool ReadOpLogSince(uint64_t start_seq_id, 
                        std::vector<OpLogEntry>& entries);
};
```

#### 3.3.4 OpLogApplier（Standby 端）

**职责**：
- 应用 OpLog Entry 到本地 metadata store
- 检查全局和 key 级别的顺序
- 处理序列号不连续和乱序情况
- 定期清理 key_sequence_map_（内存优化）

**关键方法**：
```cpp
class OpLogApplier {
    bool ApplyOpLogEntry(const OpLogEntry& entry);
    bool CheckSequenceOrder(const OpLogEntry& entry);
    void CleanupStaleKeySequences();
};
```

#### 3.3.5 HotStandbyService（Standby 端）

**职责**：
- 管理 Standby 模式的生命周期
- 协调 OpLogWatcher 和 OpLogApplier
- 处理 Standby 提升为 Primary 的逻辑

**关键方法**：
```cpp
class HotStandbyService {
    void StartStandby();
    void Stop();
    void Promote();
};
```

### 3.4 OpLog Entry 数据结构

```cpp
struct OpLogEntry {
    uint64_t sequence_id{0};        // 全局递增序列号
    uint64_t timestamp_ms{0};        // 时间戳（毫秒）
    OpType op_type{OpType::PUT_END}; // PUT_END, PUT_REVOKE, REMOVE
    std::string object_key;          // 对象 key
    std::string payload;             // 可选负载（用于 PUT_END 时携带 replica 信息）
    uint32_t checksum{0};           // 校验和
    uint32_t prefix_hash{0};        // key 前缀哈希
    uint64_t key_sequence_id{0};     // 该 key 的操作序列号（用于时序保证）
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

### 3.5 时序保证机制

#### 3.5.1 全局序列号（sequence_id）

- **作用**：保证所有 OpLog 事件的全局顺序
- **生成**：Primary 端 `OpLogManager` 全局递增生成
- **检查**：Standby 端检查 sequence_id 是否连续

#### 3.5.2 Key 级别序列号（key_sequence_id）

- **作用**：保证同一 key 的操作顺序
- **生成**：Primary 端对每个 key 单独递增
- **检查**：Standby 端检查 key_sequence_id 是否递增

#### 3.5.3 乱序处理

当检测到 key_sequence_id 乱序时：
1. **回滚**：从 metadata_store 中删除该 key 的所有状态
2. **重放**：从该 key 第一次出现的 sequence_id 开始，从 etcd 重新读取所有 OpLog
3. **重写**：按正确顺序重新应用所有 OpLog，重建 metadata

详细设计请参考：`doc/zh/rfc-oplog-rollback-replay-on-sequence-violation.md`

### 3.6 快照集成

#### 3.6.1 快照时记录 Sequence ID

- 快照生成时，记录当前的 OpLog sequence_id
- 将快照信息写入 etcd：`mooncake-store/oplog/{cluster_id}/snapshot/{snapshot_id}/sequence_id`

#### 3.6.2 OpLog 清理

- 快照生成后，可以清理快照之前的 OpLog
- 清理策略：查询 etcd 中最小存在的 sequence_id，使用 DeleteRange 删除

详细设计请参考：`doc/zh/rfc-oplog-cleanup-start-sequence-id.md`

### 3.7 Standby 服务集成

#### 3.7.1 问题

现有代码中，Standby 在等待 leader 选举期间只是阻塞等待，没有运行 Standby 服务来同步 OpLog。

#### 3.7.2 解决方案

在 `MasterServiceSupervisor::Start()` 中：
1. 检查当前是否有 leader
2. 如果有 leader 且不是自己 → 启动 Standby 服务（watch OpLog 并应用）
3. 选举成功后 → 停止 Standby 服务并提升为 Primary

详细设计请参考：`doc/zh/rfc-standby-service-integration.md`

### 3.8 Standby 提升为 Primary 时的 Lease 初始化

#### 3.8.1 问题

Standby 上的对象 lease 都是 0（因为 OpLog 只包含 PUT_END，不包含续约信息），提升为 Primary 后所有对象会立即过期。

#### 3.8.2 解决方案

在 `HotStandbyService::Promote()` 时：
1. 停止 Standby 服务
2. 遍历所有 metadata
3. 对于 lease_timeout = 0 的对象，授予默认租约时间（`default_kv_lease_ttl`）

详细设计请参考：`doc/zh/rfc-standby-promotion-lease-initialization.md`

### 3.9 内存优化：key_sequence_map_ 清理

#### 3.9.1 问题

Standby 端的 `key_sequence_map_` 用于跟踪每个 key 的 `key_sequence_id`。当 metadata 被删除后，这些条目仍然保留，长期运行可能导致内存泄漏。

#### 3.9.2 解决方案

实现定期清理机制：
- **清理条件**：最后一次操作是 `REMOVE` 且距离当前超过 1 小时
- **清理频率**：每小时扫描一次
- **保留策略**：`PUT_END` 和 `PUT_REVOKE` 操作的 key 不清理

详细设计请参考：`doc/zh/rfc-oplog-key-sequence-map-cleanup.md`

## 4. 实施计划

详细的实施计划、优先级和时间估算请参考：`doc/zh/rfc-oplog-implementation-plan.md`

**实施阶段总览**：
- **Phase 1**：基础框架（P0，2-3 周）
  - 实现 OpLogManager
  - 实现 EtcdOpLogStore
  - 实现 OpLogWatcher
  - 实现 OpLogApplier

- **Phase 2**：Standby 服务集成（P0，2-3 周）
  - 实现 HotStandbyService
  - 集成到 MasterServiceSupervisor
  - 实现 Standby 提升为 Primary

- **Phase 3**：时序保证和容错（P1，2-3 周）
  - 实现序列号检查
  - 实现乱序回滚和重放
  - 实现 key_sequence_map_ 清理

- **Phase 4**：快照集成和清理（P2，1-2 周）
  - 集成快照机制
  - 实现 OpLog 清理

- **Phase 5**：优化和完善（P3，1-2 周）
  - 批量写入优化
  - 性能调优

**总计**：8-13 周（约 2-3 个月）

## 5. 关键设计要点总结

1. **etcd 作为中间存储**：利用 etcd 的强一致性和 Watch 机制
2. **只记录关键操作**：PUT_END、PUT_REVOKE、REMOVE，不记录 LEASE_RENEW
3. **双重序列号保证**：全局 sequence_id + key 级别 key_sequence_id
4. **快照集成**：与现有快照机制集成，支持 OpLog 清理
5. **Standby 服务并行运行**：在等待选举期间持续同步数据
6. **内存优化**：定期清理 key_sequence_map_ 中的过期条目

## 6. 相关文档

- [OpLog 主备同步完整方案](./rfc-oplog-via-etcd-complete-design.md)
- [Standby 服务集成方案](./rfc-standby-service-integration.md)
- [Standby 提升为 Primary 时的 Lease 初始化](./rfc-standby-promotion-lease-initialization.md)
- [OpLog 序列号乱序时的回滚和重放方案](./rfc-oplog-rollback-replay-on-sequence-violation.md)
- [OpLog 清理策略](./rfc-oplog-cleanup-start-sequence-id.md)
- [key_sequence_map_ 清理策略](./rfc-oplog-key-sequence-map-cleanup.md)
- [实施计划](./rfc-oplog-implementation-plan.md)

