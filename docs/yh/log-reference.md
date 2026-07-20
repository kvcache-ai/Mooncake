# Mooncake Store 日志参考手册

本文档描述 `get` / `get_batch` / `get_into` / `batch_get_into` / `put` / `put_batch` 六个操作的全链路日志输出，以及传输任务层、URMA 建连层、Client 创建等新增日志。

---

## 0. 日志系统概览

### 0.1 两套日志系统

当前代码中存在两套日志系统：

| 系统 | 使用文件 | 宏 | trace_id 前缀 | 输出方式 |
|------|---------|-----|--------------|---------|
| **MC_LOG 系统** | `real_client.cpp`、`client_service.cpp`、`transfer_task.cpp` | `MC_LOG` / `MC_VLOG` | 是 | 异步队列 |
| **原生 glog** | `store_py.cpp`（batch 操作）、`urma_endpoint.cpp` | `LOG` / `VLOG` | 否 | 同步直接输出 |

MC_LOG 系统的每条日志自动带有 `trace_id[xxx] ` 前缀（无 trace 上下文时为 `trace_id[none] `）。原生 glog 日志无此前缀。

**注意**：当两层日志混合输出时（如 `get_batch`），原生 glog 日志与 MC_LOG 日志可能因异步队列导致顺序不完全一致。

### 0.2 MC_LOG 异步队列

MC_LOG 通过 `AsyncLogMessage` 临时对象在析构时将日志条目入队，后台单线程消费并调用 glog 输出。队列容量 8192 条，满时阻塞写入线程。FATAL 级别绕过队列直接同步输出。进程退出时通过 `atexit` 处理器刷出剩余日志。

实现文件：`mooncake-common/include/mooncake_logging.h`、`mooncake-common/src/mooncake_logging.cpp`。

### 0.3 TraceId 系统

- **生成**：`NewTraceId()` 使用 `(PID << 48) ^ (steady_clock_ns & 0x0000FFFFFFFF0000) ^ atomic_counter++` 生成全局唯一 ID
- **线程传递**：`ScopedTraceId` 通过 `thread_local` 保存/恢复当前线程的 trace_id
- **同步调用链传递**：`RealClient` 入口创建 `ScopedTraceId(NewTraceId())` 后，同线程下游函数通过 `CurrentTraceId()` 读取同一个 trace_id
- **异步任务传播**：提交线程用 `CurrentTraceId()` 捕获当前 trace_id，并写入 `MemcpyTask`、`FilereadTask` 或线程池 lambda；工作线程执行时通过 `ScopedTraceId` 恢复，确保异步路径日志可追踪
- **上下文恢复**：`ScopedTraceId` 析构时恢复旧值，避免线程池复用、嵌套调用或提前返回后把上一个请求的 trace_id 带到后续日志

---

## 1. `get` 日志链路

> Python 绑定层（`store_py.cpp`）的单 key `get` 生命周期日志（`get start/complete/slow`）已移除，改为由 `real_client.cpp` 输出慢操作告警。

正常路径日志按调用顺序：

```
real_client::get_buffer
  ├ real_client::get_buffer_internal
  │   ├ query_success
  │   ├ replica_selected
  │   ├ [SSD 路径] ssd_read_detail
  │   ├ get_breakdown
  │   └ [慢操作] get_buffer_slow
  ├ client_service::Get
  │   └ transfer_read_completed
  ├ client_service::TransferData
  │   └ transfer_data op[READ]
  └ [慢操作] get_buffer_slow（在 real_client::get_buffer 层）
```

### 1.1 核心逻辑层 — `real_client.cpp::get_buffer_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `query_success` | INFO | `query_success key[{key}] replicas[{n}]` | Master 查询成功，返回 n 个副本 |
| `replica_selected` | INFO | `replica_selected key[{key}] type[{type}] endpoint[{ip:port}] size[{bytes}]` | Memory/LocalDisk 副本选中，含 endpoint |
| `replica_selected` | INFO | `replica_selected key[{key}] type[{type}] file_path[{path}] size[{bytes}]` | Disk 副本选中，含文件路径 |
| `get_breakdown` | INFO | `get_breakdown key[{key}] query_us[{t1}] select_us[{t2}] alloc_us[{t3}] read_us[{t4}] total_us[{total}] type[{type}] status[{status}]` | 分阶段耗时汇总 |

**`get_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | Master 查询耗时（微秒） |
| `select_us` | 副本选择耗时 |
| `alloc_us` | 缓冲区分配耗时 |
| `read_us` | 数据读取耗时（RDMA/文件IO/SSD RPC） |
| `total_us` | 总耗时 |
| `type` | 副本类型：`memory_local` / `memory_remote` / `local_disk_local` / `local_disk_remote` / `disk` |
| `status` | 结果：`read_ok` / `read_fail` / `ssd_ok` / `ssd_fail` |

### 1.2 传输服务层 — `client_service.cpp::Get`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_read_completed` | INFO | `transfer_read_completed key[{key}] elapsed_us[{us}] data_size[{bytes}] cache_hit[{0/1}]` | RDMA/文件传输完成 |
| `transfer_read_failed` | ERROR | `transfer_read_failed key={key}` | 传输失败 |
| `lease_expired_before_data_transfer_completed` | WARNING | `lease_expired_before_data_transfer_completed key={key}` | 租约过期 |

**`transfer_read_completed` 字段说明：**

| 字段 | 含义 |
|------|------|
| `elapsed_us` | 传输总耗时（微秒） |
| `data_size` | 传输数据大小（字节） |
| `cache_hit` | 是否命中热缓存：`1` 命中，`0` 未命中 |

### 1.3 传输引擎层 — `client_service.cpp::TransferData`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_data` | INFO | `transfer_data first_transfer_data[{0/1}] op[{READ/WRITE}] strategy[{int}] submit_us[{t1}] wait_us[{t2}] result[{code}]` | 传输耗时拆分 |

**字段说明：**

| 字段 | 含义 |
|------|------|
| `first_transfer_data` | 进程首次传输数据输出 `1`，后续输出 `0`（用于区分首次建连开销） |
| `op` | 操作类型：`READ` 或 `WRITE` |
| `strategy` | `TransferFuture` 传输策略整数值（对应传输引擎内部策略枚举） |
| `submit_us` | 提交传输请求耗时（微秒） |
| `wait_us` | 等待传输完成耗时（微秒） |
| `result` | 传输结果，`OK` 表示成功 |

### 1.4 SSD Offload 路径 — `real_client.cpp::batch_get_into_offload_object_internal`

仅当副本类型为 `local_disk`（远端 SSD）时触发。

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `ssd_read_detail` | INFO | `ssd_read_detail endpoint[{ip:port}] num_keys[{n}] total_size[{bytes}] elapsed_ms[{ms}] batch_id[{id}]` | SSD RPC 读取详情 |

**`ssd_read_detail` 字段说明：**

| 字段 | 含义 |
|------|------|
| `endpoint` | SSD offload RPC 服务端地址 |
| `num_keys` | 本批次读取的 key 数量 |
| `total_size` | 本批次读取的总字节数 |
| `elapsed_ms` | 整批 RPC 耗时（毫秒） |
| `batch_id` | 批次 ID |

---

## 2. `get_batch` 日志链路

> **注意**：Python 绑定层（`store_py.cpp`）的 batch 操作仍使用原生 `LOG()`，无 `trace_id` 前缀。下层（real_client/client_service）使用 `MC_LOG`，带 `trace_id` 前缀。两层日志混合输出时可能因异步队列导致顺序不完全一致。

```
store_py::get_batch
  ├ get_batch start
  ├ real_client::batch_get_buffer_internal
  │   ├ batch_query_result
  │   ├ [逐 key] replica_selected (无此日志，batch 不逐 key 输出)
  │   ├ [SSD 路径] ssd_read_detail
  │   ├ batch_get_breakdown
  │   └ [慢操作] batch_get_buffer_slow
  ├ client_service::BatchGet
  │   └ batch_get_transfer_complete
  ├ client_service::TransferData (多次)
  │   └ transfer_data op[READ]
  └ get_batch complete
```

### 2.1 Python 绑定层 — `store_py.cpp::get_batch`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `get_batch start` | INFO | `get_batch start num_keys[{n}]` | 操作开始 |
| `get_batch complete` | INFO | `get_batch complete num_keys[{n}] success[{s}] rc[0] elapsed_us[{us}]` | 操作成功完成 |
| `get_batch complete` | INFO | `get_batch complete num_keys[{n}] rc[-1] elapsed_us[{us}]` | 操作失败 |
| `get_batch_slow` | WARNING | `get_batch_slow num_keys[{n}] elapsed_us[{us}]` | 耗时超过 3ms 触发慢操作告警 |

### 2.2 核心逻辑层 — `real_client.cpp::batch_get_buffer_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_query_result` | INFO | `batch_query_result num_keys[{n}] num_found[{f}]` | 批量查询结果，f 为找到的 key 数 |
| `batch_get_breakdown` | INFO | `batch_get_breakdown num_keys[{n}] query_us[{t1}] prep_us[{t2}] read_us[{t3}] total_us[{total}] batch_get_ops[{m}] ssd_offload_ops[{s}] success[{ok}]` | 分阶段耗时汇总 |

**`batch_get_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | 批量 Master 查询耗时 |
| `prep_us` | 准备阶段耗时（副本选择 + 缓冲区分配，逐 key 循环） |
| `read_us` | 数据读取耗时（BatchGet + SSD RPC） |
| `total_us` | 总耗时 |
| `batch_get_ops` | 走 BatchGet 的 key 数（MEMORY + DISK 副本） |
| `ssd_offload_ops` | 走 SSD RPC 的 key 数（LOCAL_DISK 副本） |
| `success` | 成功读取的 key 数 |

### 2.3 传输服务层 — `client_service.cpp::BatchGet`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_get_transfer_complete` | INFO | `batch_get_transfer_complete num_keys[{n}] success[{s}] elapsed_us[{us}] pending_count[{c}]` | 批量传输完成 |

**字段说明：**

| 字段 | 含义 |
|------|------|
| `num_keys` | 批量传输的 key 总数 |
| `success` | 成功传输的 key 数 |
| `elapsed_us` | 传输总耗时（微秒） |
| `pending_count` | 总传输任务数（提交的 TransferFuture 数量） |

### 2.4 传输引擎层 — 同第 1 节的 `transfer_data`

### 2.5 SSD Offload 路径 — 同第 1 节的 `ssd_read_detail`

---

## 3. `get_into` 日志链路

`get_into` 将对象直接读入调用方提供的 buffer，核心日志来自 `real_client.cpp`。

```
real_client::get_into
  ├ real_client::resolve_ranged_read_metadata
  │   ├ query_success
  │   └ replica_selected
  ├ real_client::execute_ranged_read
  │   ├ [MEMORY/DISK] client_service::Get
  │   ├ [LOCAL_DISK] ssd_read_detail
  │   └ [失败] SSD/DISK/scatter/Get error
  ├ get_into_breakdown
  └ [慢操作] get_into_slow
```

### 3.1 核心逻辑层 — `real_client.cpp::get_into_range_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `query_success` | INFO | `query_success key[{key}] replicas[{n}]` | Master 查询成功，返回 n 个副本 |
| `replica_selected` | INFO | `replica_selected key[{key}] type[{type}] endpoint[{ip:port}] size[{bytes}]` | Memory/LocalDisk 副本选中，含 endpoint |
| `replica_selected` | INFO | `replica_selected key[{key}] type[{type}] file_path[{path}] size[{bytes}]` | Disk 副本选中，含文件路径 |
| `get_into_breakdown` | INFO | `get_into_breakdown key[{key}] query_us[{t1}] select_us[{t2}] read_us[{t3}] total_us[{total}] type[{type}] mode[{mode}] status[{status}]` | 分阶段耗时汇总 |
| `get_into_breakdown` | INFO | `get_into_breakdown key[{key}] query_us[0] select_us[0] read_us[0] total_us[{total}] type[unknown] mode[unknown] status[{error}]` | metadata 查询/选副本失败时的汇总 |

**`get_into_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | Master 查询耗时（微秒） |
| `select_us` | 副本选择耗时 |
| `read_us` | 数据读取耗时（RDMA/文件IO/SSD RPC/scatter） |
| `total_us` | 总耗时 |
| `type` | 副本类型：`memory_local` / `memory_remote` / `local_disk_local` / `local_disk_remote` / `disk` / `unknown` |
| `mode` | 读取模式：`full` 完整对象读取，`range` 部分读取，`unknown` 表示 metadata 阶段失败 |
| `status` | 结果：`read_ok` / `mem_fail` / `disk_fail` / `ssd_fail` / 错误码字符串 |

### 3.2 读取失败日志 — `real_client.cpp::execute_ranged_read`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `SSD read failed` | ERROR | `SSD read failed for key '{key}': {error}` | LOCAL_DISK 完整读取失败 |
| `Ranged SSD read failed` | ERROR | `Ranged SSD read failed for key '{key}': {error}` | LOCAL_DISK 范围读取失败 |
| `DISK Get failed` | ERROR | `DISK Get failed for key: {key} with error: {error}` | DISK 文件读取失败 |
| `DISK full read scatter failed` | ERROR | `DISK full read scatter failed for key '{key}': {error}` | DISK 完整读取后 scatter 到用户 buffer 失败 |
| `Ranged disk read scatter failed` | ERROR | `Ranged disk read scatter failed for key '{key}': {error}` | DISK/LOCAL_DISK 范围读取后 scatter 失败 |
| `Ranged Get failed` | ERROR | `Ranged Get failed for key: {key} with error: {error}` | MEMORY 范围读取失败 |

### 3.3 下层日志

- MEMORY / DISK 通过 `Client::Get` 时，会继续产生 `transfer_read_completed` / `transfer_data op[READ]`。
- LOCAL_DISK 通过 SSD offload 时，会继续产生 `ssd_read_detail`。

---

## 4. `batch_get_into` 日志链路

`batch_get_into` 将多个对象分别读入调用方提供的 buffers，批量成功项不逐 key 打 INFO，失败项仍逐 key 打 ERROR。

```
real_client::batch_get_into
  ├ batch_get_into_query_result
  ├ [逐 key 失败] Query/Select/Buffer/DISK error
  ├ [MEMORY] client_service::BatchGet
  ├ [DISK] client_service::BatchGet + scatter
  ├ [LOCAL_DISK] ssd_read_detail
  ├ batch_get_into_breakdown
  └ [慢操作] batch_get_into_slow
```

### 4.1 核心逻辑层 — `real_client.cpp::batch_get_into_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_get_into_query_result` | INFO | `batch_get_into_query_result num_keys[{n}] num_found[{f}]` | 批量查询结果，f 为找到的 key 数 |
| `batch_get_into_breakdown` | INFO | `batch_get_into_breakdown num_keys[{n}] query_us[{t1}] prep_us[{t2}] read_us[{t3}] total_us[{total}] mem_ops[{m}] disk_ops[{d}] ssd_offload_ops[{s}] success[{ok}]` | 分阶段耗时汇总 |

**`batch_get_into_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | 批量 Master 查询耗时 |
| `prep_us` | 准备阶段耗时（逐 key 副本选择、容量校验、分类、slice 准备） |
| `read_us` | 数据读取耗时（MEMORY BatchGet + DISK BatchGet/scatter + SSD RPC） |
| `total_us` | 总耗时 |
| `mem_ops` | 走 MEMORY `BatchGet` 的 key 数 |
| `disk_ops` | 走 DISK 临时 buffer + `BatchGet` + scatter 的 key 数 |
| `ssd_offload_ops` | 实际提交到 SSD offload 的 key 数（LOCAL_DISK） |
| `success` | 成功读取且返回正数字节数的 key 数 |

### 4.2 逐 key / endpoint 失败日志

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `Query failed` | ERROR | `Query failed for key '{key}': {error}` | 单个 key 查询失败（非 NOT_FOUND/NOT_READY） |
| `Empty replica list` | ERROR | `Empty replica list for key: {key}` | 查询结果没有 replica |
| `No usable replica` | ERROR | `No usable replica for key: {key}` | 无可用 COMPLETE 副本 |
| `Buffer too small` | ERROR | `Buffer too small for key '{key}': required={r}, available={a}` | 用户 buffer 容量不足 |
| `BatchGet failed` | ERROR | `BatchGet failed for key '{key}': {error}` | MEMORY BatchGet 失败 |
| `DISK BatchGet failed` | ERROR | `DISK BatchGet failed for key '{key}': {error}` | DISK BatchGet 失败 |
| `DISK scatter failed` | ERROR | `DISK scatter failed for key '{key}': {error}` | DISK 临时 buffer scatter 到用户 buffer 失败 |
| `Batch get store object failed` | ERROR | `Batch get store object failed endpoint[{endpoint}] objects[{n}] error[{error}]` | LOCAL_DISK endpoint 级 offload 失败 |

### 4.3 下层日志

- MEMORY / DISK 批量读取会继续产生 `batch_get_transfer_complete` / `transfer_data op[READ]`。
- LOCAL_DISK offload 会继续产生 `ssd_read_detail`。

---

## 5. `put` 日志链路

> Python 绑定层（`store_py.cpp`）的单 key `put` 生命周期日志（`put start/complete/slow`）已移除，改为由 `real_client.cpp` 输出慢操作告警。

```
real_client::put_internal
  ├ put_result
  └ [慢操作] put_slow
client_service::Put
  ├ put_start_success (或 OBJECT_ALREADY_EXISTS)
  └ put_end_success
client_service::TransferData
  └ transfer_data op[WRITE]
```

### 5.1 核心逻辑层 — `real_client.cpp::put_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_result` | INFO | `put_result key[{key}] rc[0] size[{bytes}]` | Put 成功 |
| `put_result` | INFO | `put_result key[{key}] rc[{code}] size[{bytes}]` | Put 失败，code 为错误码 |

### 5.2 传输服务层 — `client_service.cpp::Put`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_start` | INFO | `put_start key[{key}] rc[OBJECT_ALREADY_EXISTS]` | 对象已存在，直接返回成功 |
| `put_start_success` | INFO | `put_start_success key[{key}] replicas[{n}]` | Master 分配 replica 成功 |
| `put_end_success` | INFO | `put_end_success key[{key}] transfer_us[{us}] data_size[{bytes}]` | Put 完成，数据写入成功 |

**`put_end_success` 字段说明：**

| 字段 | 含义 |
|------|------|
| `transfer_us` | 传输阶段总耗时（含磁盘写入 + RDMA 传输） |
| `data_size` | 写入数据大小 |

### 5.3 传输引擎层 — `client_service.cpp::TransferData`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_data` | INFO | `transfer_data first_transfer_data[{0/1}] op[WRITE] strategy[{int}] submit_us[{t1}] wait_us[{t2}] result[{code}]` | 传输耗时拆分 |

字段含义同第 1.3 节 `transfer_data`。

---

## 6. `put_batch` 日志链路

> **注意**：Python 绑定层（`store_py.cpp`）的 batch 操作仍使用原生 `LOG()`，无 `trace_id` 前缀。下层使用 `MC_LOG`，带 `trace_id` 前缀。

```
store_py::put_batch
  ├ put_batch start
  ├ real_client::put_batch_internal
  │   └ batch_put_result
  ├ client_service::BatchPut
  │   ├ batch_put start
  │   └ batch_put complete
  ├ client_service::TransferData (多次)
  │   └ transfer_data op[WRITE]
  └ put_batch complete
```

### 6.1 Python 绑定层 — `store_py.cpp::put_batch`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_batch start` | INFO | `put_batch start num_keys[{n}] total_size[{bytes}]` | 操作开始 |
| `put_batch complete` | INFO | `put_batch complete num_keys[{n}] rc[{ret}] elapsed_us[{us}]` | 操作完成，rc=0 成功 |
| `put_batch_slow` | WARNING | `put_batch_slow num_keys[{n}] elapsed_us[{us}]` | 耗时超过 10ms 触发慢操作告警 |

### 6.2 核心逻辑层 — `real_client.cpp::put_batch_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_put_result` | INFO | `batch_put_result num_keys[{n}] num_failed[{f}]` | 批量 Put 结果 |

### 6.3 传输服务层 — `client_service.cpp::BatchPut`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_put start` | INFO | `batch_put start num_keys[{n}]` | 批量 Put 传输开始 |
| `batch_put complete` | INFO | `batch_put complete num_keys[{n}] num_failed[{f}] transfer_us[{us}] total_size[{bytes}]` | 批量 Put 完成（正常路径） |
| `batch_put complete` | INFO | `batch_put complete num_keys[{n}] num_failed[{f}] total_size[{bytes}]` | 批量 Put 完成（prefer_same_node 路径，无 transfer_us） |

**`batch_put complete` 字段说明：**

| 字段 | 含义 |
|------|------|
| `num_keys` | 批量写入的 key 数量 |
| `num_failed` | 失败的 key 数量 |
| `transfer_us` | 传输阶段总耗时（微秒，prefer_same_node 路径无此字段） |
| `total_size` | 写入的总数据大小（字节） |

### 6.4 传输引擎层 — 同第 5 节的 `transfer_data`

---

## 7. 附录：PerfPoint 打点与日志对照表

PerfPoint 定义在 `mooncake-integration/store/mooncake_perf_points.def`。
使用 `ubdiag show` 可查看实时性能数据，配合日志进行交叉分析。

### 7.0 `get_into` / `batch_get_into` 与 `get_buffer` / `batch_get_buffer` 是否一致

结论：**单 key 的 `get_into` 与 `get_buffer` 基本一致，批量的 `batch_get_into` 与 `batch_get_buffer` 还不完全一致。**

单 key 路径中，`get_into_breakdown.type` 已经和 `get_breakdown.type` 一样细分为 `memory_local` / `memory_remote` / `local_disk_local` / `local_disk_remote` / `disk`。如果只看到 `memory` / `local_disk` / `disk` 三类，那是旧文档口径，不是当前源码口径。

| 接口 | 入口日志 | 入口 PerfPoint | 汇总日志 | 子步骤对齐情况 |
|------|----------|----------------|----------|----------------|
| `get_buffer` | `get_buffer_start` | `GET_BUFFER_INTERNAL_FULL` | `get_breakdown` | `type` 细分 local/remote；有 `alloc_us` |
| `get_into` | `get_into_start` | `GET_INTO_INTERNAL` | `get_into_breakdown` | `type` 细分 local/remote；没有独立 `alloc_us`，DISK/范围读临时 buffer 分配计入 `read_us` |
| `batch_get_buffer` | `batch_get_buffer_start` | `GET_BATCH_BUFFER_INTERNAL_FULL` | `batch_get_breakdown` | `batch_get_ops` 合并 MEMORY + DISK，`ssd_offload_ops` 表示 LOCAL_DISK |
| `batch_get_into` | `batch_get_into_start` | `GET_BATCH_INTO_INTERNAL` | `batch_get_into_breakdown` | `mem_ops` / `disk_ops` / `ssd_offload_ops` 拆开统计，比 `batch_get_buffer` 更细 |

差异点：

- `get_into` 的 replica type 现在与 `get_buffer` 一致，都是 local/remote 细分；文档已同步修正。
- `get_into_breakdown` 没有 `alloc_us` 字段，因为它通常直接写入调用方 buffer；只有 DISK 或 range 读需要临时 CPU buffer，这部分耗时归入 `read_us`。
- `batch_get_into_breakdown` 比 `batch_get_breakdown` 多拆了 `mem_ops` 和 `disk_ops`；而 `batch_get_breakdown` 用 `batch_get_ops` 合并 MEMORY + DISK。所以这两者目前不是完全一致口径。

### GET 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_STORE_PY_GET` | store_py.cpp::get | Get | —（已移除） |
| `GET_BUFFER_INTERNAL` | store_py.cpp::get | GetBuffer | `get_breakdown` |
| `GET_BUFFER_INTERNAL_FULL` | real_client.cpp::get_buffer | GetBufferInternal | `get_breakdown` |
| `GET_INTERNAL_QUERY` | real_client.cpp::get_buffer_internal | Query | `query_success` |
| `GET_INTERNAL_SELECT_REPLICA` | real_client.cpp::get_buffer_internal | SelectReplica | `replica_selected` |
| `GET_INTERNAL_ALLOC_BUFFER` | real_client.cpp::get_buffer_internal | AllocBuffer | `get_breakdown` alloc_us |
| `GET_INTERNAL_SSD_READ` | real_client.cpp::get_buffer_internal | SSDRead | `ssd_read_detail` |
| `GET_INTERNAL_MEM_READ` | real_client.cpp::get_buffer_internal | MemRead | `transfer_read_completed` |
| `GET_INTERNAL_DISK_READ` | real_client.cpp::get_buffer_internal | DiskRead | `transfer_read_completed` |
| `GET_SSD_OFFLOAD_RPC` | real_client.cpp::batch_get_into_offload_object_internal | OffloadRpc | `ssd_read_detail` |
| `GET_SSD_TRANSFER_DATA` | real_client.cpp::batch_get_into_offload_object_internal | TransferData | `ssd_read_detail` |
| `GET_SSD_RELEASE_BUFFER` | real_client.cpp::batch_get_into_offload_object_internal | ReleaseBuffer | — |
| `GET_SINGLE_FIND_REPLICA` | client_service.cpp::Get | FindReplica | `transfer_read_completed` |
| `GET_SINGLE_HOT_CACHE` | client_service.cpp::Get | HotCache | `transfer_read_completed` cache_hit |
| `GET_SINGLE_TRANSFER_READ` | client_service.cpp::Get | TransferRead | `transfer_read_completed` |
| `GET_SINGLE_RELEASE_CACHE` | client_service.cpp::Get | ReleaseCache | — |
| `GET_SINGLE_ASYNC_CACHE` | client_service.cpp::Get | AsyncCache | — |
| `GET_SINGLE_TRANSFER_FULL` | client_service.cpp::TransferData | TransferData | `transfer_data op[READ]` |
| `GET_SINGLE_TRANSFER_SUBMIT` | client_service.cpp::TransferData | Submit | `transfer_data` submit_us |
| `GET_SINGLE_TRANSFER_WAIT` | client_service.cpp::TransferData | Wait | `transfer_data` wait_us |

### GET INTO 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_INTO_INTERNAL` | real_client.cpp::get_into | GetIntoInternal | `get_into_breakdown` |
| `GET_INTO_INTERNAL_QUERY` | real_client.cpp::get_into_internal | Query | `query_success` |
| `GET_INTO_INTERNAL_SELECT_REPLICA` | real_client.cpp::get_into_internal | SelectReplica | `replica_selected` |
| `GET_INTO_INTERNAL_ALLOC_BUFFER` | real_client.cpp::get_into_internal | AllocBuffer | `get_into_breakdown` read_us |
| `GET_INTO_INTERNAL_SSD_READ` | real_client.cpp::get_into_internal | SSDRead | `ssd_read_detail` / `SSD read failed` |
| `GET_INTO_INTERNAL_MEM_READ` | real_client.cpp::get_into_internal | MemRead | `transfer_read_completed` / `get_into_breakdown` |
| `GET_INTO_INTERNAL_DISK_READ` | real_client.cpp::get_into_internal | DiskRead | `transfer_read_completed` / `get_into_breakdown` |

### GET BATCH 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_STORE_PY_GET_BATCH` | store_py.cpp::get_batch | GetBatch | `get_batch start` / `get_batch complete` |
| `GET_BATCH_BUFFER_INTERNAL` | store_py.cpp::get_batch | BatchGetBuffer | `batch_get_breakdown` |
| `GET_BATCH_BUFFER_INTERNAL_FULL` | real_client.cpp::batch_get_buffer | BatchGetBufferInternal | `batch_get_breakdown` |
| `GET_BATCH_INTERNAL_QUERY` | real_client.cpp::batch_get_buffer_internal | BatchQuery | `batch_query_result` |
| `GET_BATCH_INTERNAL_PREPARATION` | real_client.cpp::batch_get_buffer_internal | Preparation | —（仅定义，当前未在源码中使用） |
| `GET_BATCH_INTERNAL_SELECT_REPLICA` | real_client.cpp::batch_get_buffer_internal | SelectReplica | `batch_get_breakdown` prep_us（逐 key 汇总） |
| `GET_BATCH_INTERNAL_ALLOC_BUFFER` | real_client.cpp::batch_get_buffer_internal | AllocBuffer | `batch_get_breakdown` prep_us（逐 key 汇总） |
| `GET_BATCH_INTERNAL_SSD_READ` | real_client.cpp::batch_get_buffer_internal | SSDRead | `ssd_read_detail` |
| `GET_BATCH_INTERNAL_MEMDISH_READ` | real_client.cpp::batch_get_buffer_internal | MemDiskRead | `batch_get_transfer_complete` |
| `GET_BATCH_FULL` | client_service.cpp::BatchGet | TransferBatchGet | `batch_get_transfer_complete` |
| `GET_BATCH_FIND_REPLICA` | client_service.cpp::BatchGet | FindReplica | — |
| `GET_BATCH_HOT_CACHE` | client_service.cpp::BatchGet | HotCache | — |
| `GET_BATCH_SUBMIT` | client_service.cpp::BatchGet | Submit | — |
| `GET_BATCH_WAIT` | client_service.cpp::BatchGet | Wait | — |
| `GET_BATCH_RELEASE_CACHE` | client_service.cpp::BatchGet | ReleaseCache | — |
| `GET_BATCH_ASYNC_CACHE` | client_service.cpp::BatchGet | AsyncCache | — |

### BATCH GET INTO 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_BATCH_INTO_INTERNAL` | real_client.cpp::batch_get_into | BatchGetIntoInternal | `batch_get_into_breakdown` |
| `GET_BATCH_INTO_INTERNAL_QUERY` | real_client.cpp::batch_get_into_internal | BatchQuery | `batch_get_into_query_result` |
| `GET_BATCH_INTO_INTERNAL_SELECT_REPLICA` | real_client.cpp::batch_get_into_internal | SelectReplica | `batch_get_into_breakdown` prep_us（逐 key 汇总） |
| `GET_BATCH_INTO_INTERNAL_ALLOC_BUFFER` | real_client.cpp::batch_get_into_internal | AllocBuffer | `batch_get_into_breakdown` read_us（仅 DISK 临时 buffer） |
| `GET_BATCH_INTO_INTERNAL_MEM_READ` | real_client.cpp::batch_get_into_internal | MemRead | `batch_get_transfer_complete` / `batch_get_into_breakdown` |
| `GET_BATCH_INTO_INTERNAL_DISK_READ` | real_client.cpp::batch_get_into_internal | DiskRead | `DISK BatchGet failed` / `DISK scatter failed` |
| `GET_BATCH_INTO_INTERNAL_SSD_READ` | real_client.cpp::batch_get_into_internal | SSDRead | `ssd_read_detail` / `Batch get store object failed` |

### PUT 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `PUT_STORE_PY_PUT` | store_py.cpp::put | Put | —（已移除） |
| `PUT_INTERNAL_FULL` | store_py.cpp::put | PutBuffer | `put_result` |
| `PUT_INTERNAL_ALLOC_BUFFER` | real_client.cpp::put_internal | AllocBuffer | — |
| `PUT_INTERNAL_MEM_COPY` | real_client.cpp::put_internal | MemCopy | — |
| `PUT_INTERNAL_SPLIT_SLICES` | real_client.cpp::put_internal | SplitSlices | — |
| `PUT_SINGLE_FULL` | client_service.cpp::Put | TransferPut | `put_end_success` |
| `PUT_SINGLE_PUT_START` | client_service.cpp::Put | PutStart | `put_start_success` |
| `PUT_SINGLE_DISK_WRITE` | client_service.cpp::Put | DiskWrite | `put_end_success` |
| `PUT_SINGLE_TRANSFER_WRITE` | client_service.cpp::Put | TransferWrite | `put_end_success` |
| `PUT_SINGLE_PUT_END` | client_service.cpp::Put | PutEnd | `put_end_success` |
| `PUT_SINGLE_PUT_REVOKE` | client_service.cpp::Put | PutRevoke | — |
| `PUT_SINGLE_TRANSFER_FULL` | client_service.cpp::TransferData | TransferData | `transfer_data op[WRITE]` |
| `PUT_SINGLE_TRANSFER_SUBMIT` | client_service.cpp::TransferData | Submit | `transfer_data` submit_us |
| `PUT_SINGLE_TRANSFER_WAIT` | client_service.cpp::TransferData | Wait | `transfer_data` wait_us |

### PUT BATCH 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `PUT_STORE_PY_PUT_BATCH` | store_py.cpp::put_batch | PutBatch | `put_batch start` / `put_batch complete` |
| `PUT_BATCH_INTERNAL_FULL` | store_py.cpp::put_batch | BatchPutBuffer | `batch_put_result` |
| `PUT_BATCH_INTERNAL_ALLOC_BUFFER` | real_client.cpp::put_batch_internal | AllocBuffer | — |
| `PUT_BATCH_INTERNAL_MEM_COPY` | real_client.cpp::put_batch_internal | MemCopy | — |
| `PUT_BATCH_INTERNAL_SPLIT_SLICES` | real_client.cpp::put_batch_internal | SplitSlices | — |
| `PUT_BATCH_FULL` | client_service.cpp::BatchPut | TransferBatchPut | `batch_put complete` |
| `PUT_BATCH_CREATE_OPS` | client_service.cpp::BatchPut | CreateOps | — |
| `PUT_BATCH_PUT_START` | client_service.cpp::StartBatchPut | PutStart | — |
| `PUT_BATCH_SUBMIT` | client_service.cpp::SubmitTransfers | Submit | — |
| `PUT_BATCH_DISK_WRITE` | client_service.cpp::SubmitTransfers | DiskWrite | — |
| `PUT_BATCH_WAIT` | client_service.cpp::WaitForTransfers | Wait | — |
| `PUT_BATCH_PUT_END` | client_service.cpp::FinalizeBatchPut | PutEnd | — |
| `PUT_BATCH_PUT_REVOKE` | client_service.cpp::FinalizeBatchPut | PutRevoke | — |
| `PUT_BATCH_COLLECT_RESULTS` | client_service.cpp::BatchPut | CollectResults | — |

### UB/URMA 建连侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `UB_HANDSHAKE_ENCODE` | transfer_metadata.cpp::encode | Encode | — |
| `UB_HANDSHAKE_DECODE` | transfer_metadata.cpp::decode | Decode | — |
| `UB_ENDPOINT_CONSTRUCT` | urma_endpoint.cpp::construct | Construct | `urma_endpoint_construct_breakdown` |
| `UB_ENDPOINT_CREATE_JETTY` | urma_endpoint.cpp::construct | CreateJetty | `urma_create_jetty_breakdown` |
| `UB_ENDPOINT_ACTIVE_SETUP` | urma_endpoint.cpp::setupConnectionsByActive | ActiveSetup | `urma_active_setup_breakdown` |
| `UB_ENDPOINT_ACTIVE_HANDSHAKE` | urma_endpoint.cpp::setupConnectionsByActive | SendHandshake | — |
| `UB_ENDPOINT_PASSIVE_SETUP` | urma_endpoint.cpp::setupConnectionsByPassive | PassiveSetup | `urma_passive_setup_breakdown` |
| `UB_ENDPOINT_DO_SETUP_ALL` | urma_endpoint.cpp::doSetupConnection | DoSetupAll | `urma_do_setup_all_breakdown` |
| `UB_ENDPOINT_IMPORT_JETTY` | urma_endpoint.cpp::doSetupConnection | ImportJetty | `urma_import_jetty_breakdown` |
| `UB_ENDPOINT_BIND_JETTY` | urma_endpoint.cpp::doSetupConnection | BindJetty | `urma_bind_jetty_breakdown` |

---

## 8. 日志配置

Mooncake 使用 glog 作为日志库，通过环境变量控制日志级别、输出位置和开关。

### 8.1 环境变量

| 变量 | 默认值 | 说明 |
|------|-------|------|
| `MC_LOG_LEVEL` | `INFO` | 日志输出级别 |
| `MC_LOG_DIR` | 空（stderr） | 日志文件输出目录 |
| `MC_LOG_ENABLE` | 禁用 | 日志总开关 |
| `MC_HIFREQ_LOG_SAMPLE_RATE` | `0.1` | 高频 breakdown 日志采样率，详见 §8.6 |

代码来源：`mooncake-transfer-engine/src/config.cpp`（MC_LOG_LEVEL）、`mooncake-common/src/mooncake_logging.cpp`（MC_LOG_ENABLE、MC_HIFREQ_LOG_SAMPLE_RATE）

### 8.2 设置日志级别 — `MC_LOG_LEVEL`

可选值：

| 值 | 效果 |
|----|------|
| `TRACE` | 最详细，输出 INFO/WARNING/ERROR，额外启用 trace 标志 |
| `INFO` | 默认，输出 INFO/WARNING/ERROR |
| `WARNING` | 只输出 WARNING/ERROR |
| `ERROR` | 只输出 ERROR |

**操作方法：**

```bash
# 在启动 Mooncake 服务前设置
export MC_LOG_LEVEL=WARNING
./mooncake_master

# 或在 Python 端（import mooncake 前）
import os
os.environ['MC_LOG_LEVEL'] = 'WARNING'
import mooncake
```

**注意：** 设置日志级别只影响日志输出，不影响时间记录代码（`steady_clock::now()` 调用仍会执行）。

### 8.3 设置日志输出位置 — `MC_LOG_DIR`

| 情况 | 行为 |
|------|------|
| 未设置或为空 | 日志输出到 stderr（终端） |
| 目录不存在 | 输出 WARNING，回退到 stderr |
| 目录不可写 | 输出 WARNING，回退到 stderr |
| 目录存在且可写 | 日志写入该目录下的文件 |

**操作方法：**

```bash
# 输出到指定目录
export MC_LOG_DIR=/var/log/mooncake
./mooncake_master

# 需要先确保目录存在且可写
mkdir -p /var/log/mooncake
chmod 755 /var/log/mooncake
```

### 8.4 设置日志总开关 — `MC_LOG_ENABLE`

控制 Mooncake 日志输出开关。未显式设置时默认关闭；显式启用后，`MC_LOG`/`MC_VLOG` 按此开关输出，Transfer Engine 初始化时也会按此开关设置 glog 的最低输出级别。

| 值 | 效果 |
|----|------|
| 未设置 | 默认禁用 |
| `off` / `0` / `false` / `no` | 关闭日志（不区分大小写） |
| 其他值 | 启用 |

**注意：**
- FATAL 级别日志不受此开关影响，始终输出
- 此开关与 `MC_LOG_LEVEL` 独立：两者都允许时日志才输出
- 仅使用原生 `LOG()` 且未经过 Transfer Engine 配置初始化的进程，仍可能受 glog 自身 flag 控制

**操作方法：**

```bash
# 启用日志
export MC_LOG_ENABLE=on
./mooncake_master
```

### 8.5 常用配置场景

| 场景 | 配置 |
|------|------|
| 生产环境 | `export MC_LOG_ENABLE=on && export MC_LOG_LEVEL=WARNING && export MC_LOG_DIR=/var/log/mooncake` |
| 调试排查 | `export MC_LOG_ENABLE=on && export MC_LOG_LEVEL=INFO`（输出到 stderr，除非设置 `MC_LOG_DIR`） |
| 性能测试（减少日志） | `export MC_LOG_ENABLE=on && export MC_LOG_LEVEL=ERROR` |
| 启用日志输出 | `export MC_LOG_ENABLE=on` |

### 8.6 高频 breakdown 日志采样 — `MC_HIFREQ_LOG_SAMPLE_RATE`

`get_buffer` / `batch_get_buffer` / `get_into` / `batch_get_into` 四条链路精简后，每个请求只剩一条
`*_breakdown` 汇总日志（原生 `LOG(INFO)`）。由于 get 类操作调用极频繁，这条"每请求一条"的日志本身即为高频
日志，可用本变量按概率采样。

| 取值 | 效果 |
|------|------|
| `1.0` | 每个请求都输出其 breakdown（100%） |
| `0.1`（默认） | 每个请求 10% 概率输出 breakdown |
| `0` | 完全不输出 breakdown（全部静默） |
| 非法/越界 | 非数值回退 `0.1`；`<0` 截断为 `0`；`>1` 截断为 `1` |

**实现要点：**
- 每个请求只掷一次骰子（`mooncake::logging::ShouldSampleHiFreqLog()`，线程本地无锁 RNG）；命中才**既输出
  日志又记录其 steady/system clock 计时**，未命中则跳过计时与输出，开销接近零。
- 仅作用于 breakdown 这一条文本日志。**UbDiag `PerfPoint` 打点始终记录，不受采样影响**；`ERROR`/`WARNING`
  也照常每次输出。
- breakdown 是原生 `LOG(INFO)`，可见性由本变量控制，**与 `MC_LOG_ENABLE` 无关**。
- 解析与缓存：`mooncake-common/src/mooncake_logging.cpp::ParseHiFreqLogSampleRate`（进程内只解析一次）。

```bash
# 全量输出 breakdown（排查/对账时用）
export MC_HIFREQ_LOG_SAMPLE_RATE=1.0
# 默认 10% 采样，无需设置；完全关闭：
export MC_HIFREQ_LOG_SAMPLE_RATE=0
```

---

## 9. 异步日志与 TraceId

### 9.1 异步日志架构

实现文件：`mooncake-common/include/mooncake_logging.h`、`mooncake-common/src/mooncake_logging.cpp`

**流程：**

```
调用 MC_LOG(severity) << "message"
  → 创建 AsyncLogMessage 临时对象
  → 捕获当前 trace_id（CurrentTraceId()）
  → 构造日志消息到 ostringstream
  → 析构时判断：
      - FATAL：直接同步调用 glog 输出（带 trace_id 前缀）
      - 其他：构造 LogEntry{file, line, severity, trace_id, message} 入队
  → AsyncLogQueue 后台线程消费
  → WriteSync() 调用 google::LogMessage 输出（自动带 trace_id 前缀）
```

**队列参数：**

| 参数 | 值 |
|------|-----|
| 最大队列长度 | 8192 条 |
| 队列满策略 | 阻塞写入线程（condition_variable wait） |
| 后台线程数 | 1 |
| 退出处理 | `atexit` 注册 `Stop()`，刷出剩余日志 |

### 9.2 TraceId 系统

**ID 生成算法：**

```
process_seed = (PID << 48) ^ (steady_clock_ns & 0x0000FFFFFFFF0000)
trace_id = process_seed ^ atomic_counter++
```

PID 保证跨进程唯一，steady_clock_ns 保证同进程每次启动不同，atomic_counter 保证同进程内递增唯一。

**线程传递机制：**

- `thread_local uint64_t current_trace_id` 存储当前线程的 trace_id
- `ScopedTraceId` 在构造时保存旧值、设置新值，析构时恢复旧值
- `AsyncLogMessage` 构造时读取 `CurrentTraceId()`，并把该值固化到日志条目中；日志之后即使由后台线程异步落盘，也不会丢失原始 trace_id
- RAII 模式，支持嵌套、提前返回和异常退出

恢复旧值的原因是 `current_trace_id` 绑定在线程上，而 Mooncake 中大量使用线程池和后台 worker。线程处理完一个请求或任务后会继续处理其他工作；如果不恢复，后续不属于该请求的日志可能仍然带着旧 trace_id，导致排查时误以为它们属于同一条调用链。

**函数间传递链路：**

1. 入口函数生成 trace：`real_client.cpp` 中 `get_buffer`、`get_into`、`batch_get_into`、`put`、`put_batch` 等公开入口创建 `ScopedTraceId trace(NewTraceId())`，从这里开始一次用户操作拥有独立 trace_id。
2. 同步函数调用自动继承：入口函数继续调用 `*_internal`、`client_service`、`TransferSubmitter` 等下游函数时，只要仍在同一线程执行，下游 `MC_LOG` 会通过 `CurrentTraceId()` 读到同一个 thread-local trace_id，不需要把 trace_id 作为函数参数层层传递。
3. MC_LOG 捕获当前 trace：每条 `MC_LOG` / `MC_VLOG` 在构造 `AsyncLogMessage` 时立即捕获当前 trace_id，并随 `LogEntry` 放入异步日志队列。后台日志线程只负责输出已经捕获好的 trace_id。
4. 跨线程提交前显式捕获：当执行流要进入线程池或 worker 队列时，提交线程先调用 `CurrentTraceId()` 取出当前 trace_id，并把它放进任务对象或 lambda 捕获列表。
5. 工作线程恢复上下文：worker 取出任务后创建 `ScopedTraceId trace(task.trace_id)` 或 `ScopedTraceId trace(trace_id)`，让该任务执行期间的所有 `MC_LOG` 都继续带原始请求的 trace_id。
6. 任务结束自动恢复：worker 任务作用域结束后 `ScopedTraceId` 析构，恢复该线程之前的 trace_id，通常恢复为 `0`，后续空闲、清理或下一任务日志不会串到刚完成的请求上。

典型同步链路：

```
RealClient::get_buffer
  -> ScopedTraceId(NewTraceId())
  -> RealClient::get_buffer_internal
  -> Client::Get / TransferSubmitter
  -> MC_LOG 捕获 CurrentTraceId()
```

典型异步链路：

```
RealClient::put
  -> ScopedTraceId(NewTraceId())
  -> client_service 提交异步任务前 CurrentTraceId()
  -> write_thread_pool_.enqueue(..., trace_id)
  -> worker lambda 内 ScopedTraceId(trace_id)
  -> StoreObject / PutEnd 相关 MC_LOG 继续使用同一 trace_id
```

典型传输任务链路：

```
TransferSubmitter::submitTransfer
  -> MemcpyTask(..., CurrentTraceId())
  -> MemcpyWorkerPool::workerThread
  -> ScopedTraceId(task.trace_id)
  -> memcpy / GPU copy 相关 MC_LOG 使用原始 trace_id

TransferSubmitter::submitFileReadOperation
  -> FilereadTask(..., CurrentTraceId())
  -> FilereadWorkerPool::workerThread
  -> ScopedTraceId(task.trace_id)
  -> LoadObject 相关 MC_LOG 使用原始 trace_id
```

**异步任务传播：**

- `MemcpyTask` 和 `FilereadTask` 携带 `trace_id` 字段
- `client_service.cpp` 中异步 `StoreObject + PutEnd` 的线程池 lambda 通过捕获列表携带 `trace_id`
- 工作线程取出任务后通过 `ScopedTraceId trace(task.trace_id)` 或 `ScopedTraceId trace(trace_id)` 恢复上下文
- 确保异步路径的日志可关联到原始操作

**日志格式：**

```
I0527 14:30:00.123456 12345 real_client.cpp:2682] trace_id[123456789abcdef0] get_breakdown key[k1] ...
I0527 14:30:00.123789 12345 real_client.cpp:3600] trace_id[none] Cleaning up ...
```

### 9.3 FlushAsyncLogs

调用 `mooncake::logging::FlushAsyncLogs()` 可手动刷出异步队列中所有待输出日志，内部会等待队列为空且无活跃写入后调用 `google::FlushLogFiles(google::INFO)`。

---

## 10. `client_create_breakdown` 日志

来源：`client_service.cpp::Client::Create`

记录 Client 对象创建过程各阶段耗时。有两种变体：

### 10.1 RPC-only 模式（`protocol == "rpc_only"`）

```
client_create_breakdown protocol[rpc_only] connect_master_us[{t1}] storage_config_us[{t2}] init_transfer_engine_us[0] init_transfer_submitter_us[0] total_us[{t5}]
```

### 10.2 完整模式

```
client_create_breakdown protocol[{p}] connect_master_us[{t1}] storage_config_us[{t2}] init_transfer_engine_us[{t3}] init_transfer_submitter_us[{t4}] total_us[{t5}]
```

### 字段说明

| 字段 | 含义 |
|------|------|
| `protocol` | 传输协议：`rdma` / `tcp` / `ub` / `ascend` / `cxl` / `rpc_only` 等 |
| `connect_master_us` | 连接 Master 服务耗时（微秒），含 HA 视图读取（如启用） |
| `storage_config_us` | 获取存储配置耗时（微秒），含 GetStorageConfig 或回退 GetFsdir |
| `init_transfer_engine_us` | 初始化传输引擎耗时（微秒），rpc_only 模式为 0 |
| `init_transfer_submitter_us` | 初始化传输提交器耗时（微秒），rpc_only 模式为 0 |
| `total_us` | `Client::Create` 总耗时（微秒），从函数入口到返回 |

---

## 11. 传输任务层日志

来源：`mooncake-store/src/transfer_task.cpp`

记录传输任务执行过程中的关键步骤耗时。

### 11.1 `transfer_future_wait`

```
transfer_future_wait first_wait[{0/1}] ready_before_wait[{0/1}] strategy[{int}] wait_us[{us}] result[{code}]
```

记录等待传输 Future 完成的过程。

| 字段 | 含义 |
|------|------|
| `first_wait` | 是否为进程首次调用 wait：`1` 首次，`0` 后续（用于区分首次建连开销） |
| `ready_before_wait` | wait 前传输是否已完成：`1` 已完成（无需等待），`0` 需要等待 |
| `strategy` | `TransferFuture` 的传输策略整数值 |
| `wait_us` | 等待耗时（微秒） |
| `result` | 传输结果：`OK` 表示成功，其他为错误码 |

### 11.2 `open_segment_breakdown`

```
open_segment_breakdown endpoint[{addr}] open_segment_us[{us}] status[{0/-1}]
```

记录打开远端 Segment（建立传输连接）的耗时。

| 字段 | 含义 |
|------|------|
| `endpoint` | 传输引擎端点地址字符串 |
| `open_segment_us` | `openSegment()` 调用耗时（微秒） |
| `status` | 结果：`0` 成功，`-1` 失败（`ERR_INVALID_ARGUMENT`） |

### 11.3 `submit_transfer_breakdown`

```
submit_transfer_breakdown first_transfer[{0/1}] batch_id[{id}] request_count[{n}] alloc_batch_id_us[{us}] submit_transfer_us[{us}] status[{0/err}]
```

记录提交批量传输请求的耗时拆分。

| 字段 | 含义 |
|------|------|
| `first_transfer` | 是否为首次提交传输：`1` 首次，`0` 后续 |
| `batch_id` | 批量传输 ID |
| `request_count` | 本次提交的传输请求数量 |
| `alloc_batch_id_us` | 分配 batch ID 耗时（微秒） |
| `submit_transfer_us` | 提交传输请求耗时（微秒） |
| `status` | 提交结果：`0` 成功，其他为 gRPC 状态码 |

---

## 12. URMA 端建连日志（第一次建立连接）

来源：`mooncake-transfer-engine/src/transport/kunpeng_transport/urma/urma_endpoint.cpp`

> **注意**：本节的 `*_breakdown` 日志用的是 `MC_LOG(INFO)`，**会带 `trace_id` 前缀**——主动端在握手时通过 `local_desc.trace_id = CurrentTraceId()` 把 trace_id 传给对端，所以一次建连的主动端和被动端日志可以用同一个 `trace_id` 串起来。少量纯状态日志（如 `"Connection has been established"`）仍是原生 `LOG()`，无 trace_id。

记录 UB/URMA 传输端点的构建和建连过程各步骤耗时。

### 12.0 先搞清楚：连接是什么时候建的、日志按什么顺序打

**何时触发**：URMA 连接是**懒建立**的——`openSegment()` 只拿元数据，并不建连；直到第一次真正往某个 `peer_nic_path`（远端节点+网卡）提交传输时，worker 线程发现这个 endpoint 还没 connected，才触发建连。这也是为什么进程第一次传输某个对端时会有一段额外开销（对应 `first_transfer` / `first_wait` 字段的 `1`）。

**建连分主动端（发起方）和被动端（响应方）两侧**，各自打不同的 breakdown 日志。把它们按发生顺序串起来，就是下面这张图——读真实日志时照着对号入座即可：

```
主动端 Node A                                   被动端 Node B
（worker 发现 endpoint 未连）
setupConnectionsByActive()  ← 计时起点 t0
  │
  ├─[情况①: peer_nic == 本机自己] 同节点直连，跳过握手，直接 doSetupConnection
  │     → urma_active_setup_breakdown ... local_peer[1] handshake_us[0] ...   (§12.3 变体1)
  │
  └─[情况②: 跨节点] 需要握手
       sendHandshake(local_desc{nic,jetty_num,trace_id})  ──RPC──▶  onSetupConnections()
                                                                      setupConnectionsByPassive()
                                                                        doSetupConnection()   ← 被动端也建
                                                                          每个 jetty: import + bind
                                                                          → urma_import_jetty_breakdown  (§12.6)
                                                                          → urma_bind_jetty_breakdown    (§12.7)
                                                                        → urma_do_setup_all_breakdown    (§12.5)
                                                      ◀──返回 peer_desc{eid,jetty_num}──
                                                                      → urma_passive_setup_breakdown     (§12.4)
       ├─ 握手失败 → urma_active_setup_breakdown ... handshake_us[..] do_setup_us[0] status[err]  (§12.3 变体2)
       └─ 握手成功 → doSetupConnection(peer_eid, peer_jetty_num)   ← 主动端建
                       每个 jetty: import + bind
                       → urma_import_jetty_breakdown   (§12.6)
                       → urma_bind_jetty_breakdown     (§12.7)
                     → urma_do_setup_all_breakdown     (§12.5)
                   → urma_active_setup_breakdown ... local_peer[0] handshake_us[..] do_setup_us[..] status[0]  (§12.3 变体3)
```

**各日志在排查里的分工**（拿到一条慢建连日志时这样定位）：

- `urma_active_setup_breakdown` 是**主动端的总账**：先看 `total_us` 判断这次建连慢不慢，再看是 `handshake_us`（握手 RPC 慢，多半是网络/对端响应慢）还是 `do_setup_us`（本地 import+bind 慢）占大头。
- `urma_passive_setup_breakdown` 是**被动端的总账**：和主动端用同一 `trace_id` 配对，看对端响应那一侧花了多久。
- `urma_do_setup_all_breakdown` 把一次 `doSetupConnection` 里**所有 jetty 的 import+bind 循环**汇总；想进一步拆到单个 jetty，再看 `urma_import_jetty_breakdown` / `urma_bind_jetty_breakdown`。
- `urma_endpoint_construct_breakdown` / `urma_create_jetty_breakdown` 属于**更早的端点构建期**（创建 jetty 资源，发生在 init/首次用到该 context 时），不在每次建连路径上，但首次开销分析要一并看。

> 想从代码角度理解这套握手/Jetty 绑定流程（`setupConnectionsByActive` → `sendHandshake` → `doSetupConnection` → `import/bind jetty`），见 `transfer-engine-deep-dive.md` 第 8 站与 `urma-transfer-engine-flow.md` 第 5 站。对应的 PerfPoint 打点见本手册 §545「UB/URMA 建连侧」（`UB_ENDPOINT_ACTIVE_SETUP` / `ACTIVE_HANDSHAKE` / `PASSIVE_SETUP` 等）。

下面是每条日志的逐字段参考。

### 12.1 `urma_endpoint_construct_breakdown`

```
urma_endpoint_construct_breakdown local_nic[{nic}] jetty_count[{n}] construct_us[{us}] status[0]
```

记录 UrmaEndpoint 构建过程总耗时。

| 字段 | 含义 |
|------|------|
| `local_nic` | 本端 NIC 路径标识 |
| `jetty_count` | 创建的 jetty 数量（即 `num_jetty_per_ep` 配置值） |
| `construct_us` | 整个 construct 过程耗时（微秒），含所有 jetty 创建 |
| `status` | 结果：`0` 成功 |

### 12.2 `urma_create_jetty_breakdown`

**成功时：**
```
urma_create_jetty_breakdown index[{i}] jetty_id[{id}] jfc_id[{id}] create_us[{us}] status[0]
```

**失败时：**
```
urma_create_jetty_breakdown index[{i}] create_us[{us}] status[-1]
```

记录单个 jetty 创建耗时。

| 字段 | 含义 |
|------|------|
| `index` | jetty 在 jetty_list 中的索引 |
| `jetty_id` | 创建成功后的 jetty ID |
| `jfc_id` | jetty 关联的 JFC（Jetty Flow Control）ID |
| `create_us` | `urma_create_jetty()` 调用耗时（微秒） |
| `status` | 结果：`0` 成功，`-1` 失败 |

### 12.3 `urma_active_setup_breakdown`

记录主动建连（active side）过程耗时。有三种变体：

**本端对端（local_peer=1，同节点通信）：**
```
urma_active_setup_breakdown local_nic[{nic}] peer_nic[{nic}] local_peer[1] handshake_us[0] do_setup_us[{us}] total_us[{us}] status[{rc}]
```

**握手失败：**
```
urma_active_setup_breakdown local_nic[{nic}] peer_nic[{nic}] local_peer[0] handshake_us[{us}] do_setup_us[0] total_us[{us}] status[{rc}]
```

**成功路径：**
```
urma_active_setup_breakdown local_nic[{nic}] peer_nic[{nic}] local_peer[0] handshake_us[{us}] do_setup_us[{us}] total_us[{us}] status[{rc}]
```

| 字段 | 含义 |
|------|------|
| `local_nic` | 本端 NIC 路径标识 |
| `peer_nic` | 对端 NIC 路径标识 |
| `local_peer` | 是否为本节点内通信：`1` 是（无需握手，直接 doSetup），`0` 否（需跨节点握手） |
| `handshake_us` | 握手耗时（微秒），本端对端时为 `0` |
| `do_setup_us` | `doSetupConnection()` 耗时（微秒），握手失败时为 `0` |
| `total_us` | 主动建连总耗时（微秒） |
| `status` | 结果：`0` 成功，其他为错误码（如 `ERR_DEVICE_NOT_FOUND`、`ERR_REJECT_HANDSHAKE`） |

### 12.4 `urma_passive_setup_breakdown`

```
urma_passive_setup_breakdown local_nic[{nic}] peer_nic[{nic}] total_us[{us}] status[{rc}]
```

记录被动建连（passive side，响应握手请求）过程耗时。

| 字段 | 含义 |
|------|------|
| `local_nic` | 本端 NIC 路径标识 |
| `peer_nic` | 对端 NIC 路径标识 |
| `total_us` | 被动建连总耗时（微秒），含 `doSetupConnection()` |
| `status` | 结果：`0` 成功，其他为错误码 |

### 12.5 `urma_do_setup_all_breakdown`

```
urma_do_setup_all_breakdown local_nic[{nic}] peer_nic[{nic}] jetty_count[{n}] total_us[{us}] status[0]
```

记录 `doSetupConnection()` 整体耗时（包含所有 jetty 的 import + bind 循环）。

| 字段 | 含义 |
|------|------|
| `local_nic` | 本端 NIC 路径标识 |
| `peer_nic` | 对端 NIC 路径标识 |
| `jetty_count` | 设置的 jetty 数量（本端与对端匹配） |
| `total_us` | 全部 jetty 设置总耗时（微秒） |
| `status` | 结果：`0` 成功 |

### 12.6 `urma_import_jetty_breakdown`

```
urma_import_jetty_breakdown index[{i}] peer_jetty_id[{id}] import_us[{us}] status[{0/-1}]
```

记录单个 jetty 的 `urma_import_jetty()` 调用耗时。

| 字段 | 含义 |
|------|------|
| `index` | jetty 在 jetty_list 中的索引 |
| `peer_jetty_id` | 对端 jetty ID |
| `import_us` | `urma_import_jetty()` 调用耗时（微秒） |
| `status` | 结果：`0` 成功，`-1` 失败 |

### 12.7 `urma_bind_jetty_breakdown`

```
urma_bind_jetty_breakdown index[{i}] local_jetty_id[{id}] peer_jetty_id[{id}] bind_us[{us}] status[0]
```

记录单个 jetty 的 `urma_bind_jetty()` 调用耗时。

| 字段 | 含义 |
|------|------|
| `index` | jetty 在 jetty_list 中的索引 |
| `local_jetty_id` | 本端 jetty ID |
| `peer_jetty_id` | 对端 jetty ID（即 imported jetty） |
| `bind_us` | `urma_bind_jetty()` 调用耗时（微秒） |
| `status` | 结果：`0` 成功 |

---

## 13. 慢操作告警汇总

来源：`mooncake-store/src/real_client.cpp`

慢操作告警统一由 `real_client.cpp` 输出，阈值为 **3000us（3ms）**。仅当操作耗时超过阈值时才输出 WARNING 级别日志。

> **注意**：这些告警日志由 MC_LOG 输出，自动带 `trace_id` 前缀。

### 13.1 单 key 操作

```
trace_id[xxx] {op}_slow key[{key}] size[{size}] elapsed_us[{us}] rc[{rc}]
```

| 字段 | 含义 |
|------|------|
| `{op}` | 操作名：`get_buffer` / `get_into` / `put` / `put_parts` / `put_from` |
| `key` | 对象 key |
| `size` | 数据大小（字节） |
| `elapsed_us` | 操作总耗时（微秒） |
| `rc` | 返回码：`0` 成功（但慢），`-1` 失败 |

### 13.2 批量操作（带 success 计数）

```
trace_id[xxx] {op}_slow num_keys[{n}] size[{size}] elapsed_us[{us}] success[{s}]
```

| 字段 | 含义 |
|------|------|
| `{op}` | 操作名：`batch_get_buffer` / `batch_get_into` / `batch_put_from` / `batch_put_from_multi_buffers` |
| `num_keys` | 批量操作的 key 数量 |
| `size` | 批量操作的总数据大小（字节） |
| `elapsed_us` | 操作总耗时（微秒） |
| `success` | 成功的 key 数量 |

### 13.3 批量操作（带 rc 返回码）

```
trace_id[xxx] {op}_slow num_keys[{n}] size[{size}] elapsed_us[{us}] rc[{rc}]
```

| 字段 | 含义 |
|------|------|
| `{op}` | 操作名：`put_batch` |
| `num_keys` | 批量操作的 key 数量 |
| `size` | 批量操作的总数据大小（字节） |
| `elapsed_us` | 操作总耗时（微秒） |
| `rc` | 返回码：`0` 成功（但慢），其他为错误码 |

### 13.4 与旧版 Python 层慢日志的关系

旧版 `store_py.cpp` 的 `get_slow` / `put_slow` 已移除。旧版 `get_batch_slow` / `put_batch_slow` 仍在 `store_py.cpp` 中保留（使用原生 `LOG()`，无 trace_id）。

当操作超过 3ms 时：
- 单 key `get`/`put`：仅 real_client.cpp 输出慢日志
- 批量 `get_batch`/`put_batch`：可能同时出现 `store_py.cpp` 的慢日志（无 trace_id）和 `real_client.cpp` 的慢日志（有 trace_id）
