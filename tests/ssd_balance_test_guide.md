# SSD负载均衡验证测试指南

## 前置条件

- 编译完成 mooncake_store（含 `mooncake_master` 可执行文件）
- 编译完成 mooncake-wheel（含 Python `mooncake.store` 模块）
- 安装 Python 3

## 验证脚本

验证脚本位于 `mooncake-wheel/tests/verify_ssd_balance.py`，支持 5 个测试场景：

```
python verify_ssd_balance.py --test <test_name>
```

| test_name | 验证内容 |
|-----------|---------|
| `load_balancing` | **基础测试**：2个Client不对称SSD，验证数据按SSD空闲比例分布 |
| `ssd_high_watermark_blocking` | SSD达到90%高水位后Master拒绝新分配 |
| `ssd_eviction_protection` | 启用FIFO驱逐+强制禁用驱逐，验证已有SSD数据不被驱逐 |
| `ddr_admission` | DDR满时临时禁止写入，释放空间后自动恢复 |
| `all_ssd_full` | 所有节点SSD满后全局拒绝，释放后恢复 |

## 默认规模

DDR=4GB, SSD=16GB, Key=4MB（各测试可能使用不同规模）

## 注意事项

- **每次测试前清空SSD目录**：`rm -rf <SSD_PATH> && mkdir -p <SSD_PATH>`
- **MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS必须设为1**：默认10s会导致offload延迟过长
- **put()不抛异常**：`store.put()` 返回整数状态码（0=成功，非0=失败）
- **每次插入间等0.01s**：避免写入过快导致问题

---

## 日志观察方法

写入被拒绝时，Client 端会输出 WARNING 级别日志，无需额外配置即可看到：

```
W... client_service.cpp:...] Failed to start put operation for key=xxx
    due to insufficient space. Consider lowering eviction_high_watermark_ratio
    or mounting more segments.
```

Master 端的拒绝日志需要开启 verbose 才能看到。启动 Master 时加上 `--v=1`：

```bash
mooncake_master --v=1 ...
```

开启后可观察：

| Master 日志关键词 | 触发条件 | 含义 |
|---|---|---|
| `DDR overflow protection: rejecting allocation, ratio=X.XX` | 全局DDR使用率 > `eviction_high_watermark_ratio` | DDR准入控制生效（验证3） |
| `Failed to allocate replicas for key=xxx, error: NO_AVAILABLE_HANDLE` | 所有segment被排除（SSD满） | SSD水位拒绝（验证2、4） |

---

## 验证 1：SSD负载均衡（应首先运行）

两个Client使用不同的SSD容量：
- Client 1（写入端）：DDR=4GB, SSD=8GB
- Client 2：DDR=4GB, SSD=16GB

写入约1200个4MB key（4.8GB），按SSD空闲比例分配到两个Client。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_lb \
python mooncake-wheel/tests/verify_ssd_balance.py --test load_balancing
```

### 预期观察

- Client 1写入约1200个key
- 等待60s offload后，两个SSD子目录均有文件：
  - Client 1: `/tmp/mooncake_ssd_balance_lb/client1`
  - Client 2: `/tmp/mooncake_ssd_balance_lb/client2`
- **Client 2的SSD数据量 > Client 1的SSD数据量**（按比例分配正常）

### 判断标准

```bash
du -sh /tmp/mooncake_ssd_balance_lb/client1 /tmp/mooncake_ssd_balance_lb/client2
```

- 两个子目录大小均 > 0
- client2 > client1（按SSD空闲比例分配策略有效）
- 两者使用率应接近（约 1:2 的数据量比，对应 8GB:16GB 容量比）

---

## 验证 2a：SSD高水位分配阻断

脚本内部使用较小SSD（DDR=4GB, SSD=5GB），使少量数据即可填满SSD到90%高水位。

**关键机制**：`ssd_used_bytes` 仅在 `NotifyOffloadSuccess` 回调时异步更新。写入阶段 DDR 缓冲数据（4GB），DDR eviction 不断释放空间，因此写入期间 SSD watermark 无法阻断分配。
正确验证方式：offload 全部完成后 `ssd_used_bytes` 反映真实 SSD 使用率 > 90%，此时新写入应被拒绝。

流程：写入初始数据 → offload落盘 → 写入压力数据填满SSD → 等待offload完成 → 验证新写入被拒绝 → 验证初始数据可读。

注意：Bucket存储后端默认 `bucket_size_limit=256MB`，数据量不足一个 bucket 时不会落盘。
脚本设置了 `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=40MB` 使初始 80MB 数据足以填满 2 个 bucket。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_hwb \
python mooncake-wheel/tests/verify_ssd_balance.py --test ssd_high_watermark_blocking
```

注意：SSD容量由脚本内部控制（5GB），无需设置 `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES`。

### 预期观察

- 写入20个初始key（80MB），等待20s确保offload落盘到SSD
- 分6批写入1200个压力key（4.8GB），每批200个，批间等待20s offload
  - **此阶段写入全部成功是正常的**（DDR缓冲 + ssd_used_bytes滞后）
  - 部分压力key可能被DDR eviction丢弃（预期行为）
- 等待40s让所有offload完成，metrics显示SSD使用率 > 90%
- offload完成后写入新key `hw_blocked_test`，此时应被拒绝：
  ```
  W... client_service.cpp:...] Failed to start put operation for key=hw_blocked_test
      due to insufficient space...
  ```
- 初始20个key全部可读（已offload到SSD）

### 判断标准

- offload完成后新写入被拒绝（`store.put()` 返回非0）
- 初始写入的20个key全部可读，0个丢失

### 注意：offload 可能提前停止

当 `eviction_policy=NONE`（默认）时，`BucketStorageBackend::IsEnableOffloading` 检查：
```
total_size + bucket_size_limit <= total_size_limit
```
要求预留一个完整 bucket 的空间（40MB）。当 `total_size > 4.96GB`（5GB - 40MB）时，offload 被阻断。

更关键的是：一旦 `BatchOffload` 返回 `KEYS_ULTRA_LIMIT`，`file_storage.cpp:471` 将 `enable_offloading_` 永久设为 `false`，没有代码会重置它。后续所有心跳都发送 `enable_offloading_=false`，Master 不再返回 offloading objects。

此外，DDR eviction（DDR > 95% 时触发）会在 offload 之前删除 DDR 副本。被驱逐的 key 无法再被 offload，因为其数据已从 DDR 消失。

---

## 验证 2b：SSD驱逐保护

验证存储后端驱逐保护机制：启用 FIFO 驱逐策略 + `disable_ssd_eviction=true`，
写入压力数据触发容量检查，验证初始数据不被驱逐。

脚本内部设置：
- `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=fifo`（启用驱逐策略）
- `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=256MB`（容量上限）
- `MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION=true`（强制禁止驱逐）

**Terminal 1** — 启动Master（同 2a）

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_evict \
python mooncake-wheel/tests/verify_ssd_balance.py --test ssd_eviction_protection
```

### 预期观察

- 写入20个初始key（80MB），等待20s确保offload落盘到SSD
- 写入200个压力key（800MB），远超 `max_total_size=256MB`
- 如果 `disable_ssd_eviction` 不生效，初始 bucket 会按 FIFO 被驱逐
- 初始20个key全部可读（`PrepareEviction` 因 `disable_ssd_eviction` 跳过驱逐）

### 判断标准

- 初始写入的20个key全部可读，0个丢失

---

## 验证 3：DDR准入控制

写入 16MB key 逐步填满 DDR（4GB），观察 `ddr_admission_watermark_ratio=0.90`
触发后的写入阻断行为。

DDR 检查在 `SsdBalanceAllocationStrategy::Allocate` 中通过
`isDdrHighWatermark()` 逐 segment 判断（`allocation_strategy.h:748`）。
指标来自 `get_segment_mem_used_ratio`（异步采样），因此实际 DDR
可能略高于水位线（例：设 90% 实际到 93%），属正常行为。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --ddr_admission_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_ddr \
python mooncake-wheel/tests/verify_ssd_balance.py --test ddr_admission
```

### 预期观察

- 持续写入 16MB key 直至被拒绝
- DDR 水位附近出现 `NO_AVAILABLE_HANDLE` 或 `DDR_ADMISSION_REJECTED`，Client 日志：
  ```
  W... Failed to start put operation for key=ddr_fill_xxx
      due to insufficient space...
  ```

### 判断标准

- 至少一次写入被拒绝（`store.put()` 返回非0）

---

## 验证 4：全节点SSD满

写入大量数据填满所有节点SSD，验证全局拒绝后释放可恢复。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=17179869184 \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_allfull \
python mooncake-wheel/tests/verify_ssd_balance.py --test all_ssd_full
```

### 预期观察

- 写入大量16MB对象，等待offload排空DDR
- SSD满后新写入失败，Client 日志出现：
  ```
  W... Failed to start put operation for key=allfull_test
      due to insufficient space...
  ```
- 删除部分数据后写入恢复

### 判断标准

- SSD满时写入被阻止（`store.put()` 返回非0）
- 释放空间后写入恢复（`store.put()` 返回0）

---

## 关键环境变量

| 变量 | 值 | 说明 |
|------|----|------|
| `MC_METADATA_SERVER` | `http://127.0.0.1:8880/metadata` | HTTP元数据服务器地址 |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `17179869184` | SSD容量16GB（验证4使用，验证2由脚本内部控制） |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `1` | **必须设为1**，默认10s太慢 |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | 测试专用目录 | 每次测试前清空 |
