# 任务拥塞状态查询

Classic TE 和 TENT 为已提交的逻辑传输任务提供两套独立接口：极简查询返回分类，详细查询返回支持分类的异常证据。它们不改变现有完成状态，也不会轮询 CQ、提交 WR、推进重试或读取网卡控制接口。调用方先按原有完成接口等待任务，再在释放 batch 前查询需要诊断的 task。

## 分类与边界

| 状态 | 含义 |
| --- | --- |
| `NORMAL` | 当前 RDMA task 没有未解决的拥塞证据，或已成功完成。 |
| `CONGESTED` | 至少一个当前 slice 因窗口、接收端压力等短时原因等待。 |
| `LONG_UNAVAILABLE` | 当前候选路径持续隔离、探测失败或暂时没有可用路径。它不证明远端或网卡永久失效。 |
| `UNKNOWN` | 控制器关闭、非 RDMA attempt，或任务没有可用的拥塞控制证据。不要将它当成正常。 |

多 slice 任务以尚未解决的最严重情况为准：`LONG_UNAVAILABLE` 高于 `CONGESTED`。详细结果的 `resolved` 表示所保留异常已经解决；其窗口、在途字节和控制器数据是异常发生时的 best-effort 记录，不是查询时刻的实时读数。未观测字段在 C++ 中有 `observed=false`，在 C 中没有对应 bit，在 Python 中为 `None`。首版只记录 RDMA 拥塞证据，接口名称和类型不限定后续 transport。

无效、未提交或已释放的 batch/task 不能查询。`submitTransfer` 在创建 task 前失败时没有可供查询的 `task_id`，调用方继续使用提交错误。查询不延长 batch 生命周期；不要与释放同一个 batch 并发使用。

## C++

Classic `mooncake::TransferEngine` 和 TENT `mooncake::tent::TransferEngine` 都有同名只读方法：

```cpp
mooncake::TaskCongestionState state;
mooncake::TaskCongestionDetail detail;
auto simple = engine.getTaskCongestionState(batch_id, task_id, state);
auto detailed = engine.getTaskCongestionDetail(batch_id, task_id, detail);
if (simple.ok() && detailed.ok()) {
    // Use state for retry policy; inspect observed detail fields for diagnosis.
}
// Release the batch only after the required status queries.
```

Classic facade 选择 TENT backend 时也转发这两套方法。`getTransferStatus` 仍回答任务是否完成；新接口只回答当前拥塞分类。有关启用方式与模式，见 [自适应 RDMA 拥塞控制](adaptive-rdma-control.md)。

## C

Classic C API 使用 `getTaskCongestionState` / `getTaskCongestionDetail`；TENT C API 使用 `tent_task_congestion_state` / `tent_task_congestion_detail`。两个详细函数使用相同的 `task_congestion_detail_t` 与 `TASK_CONGESTION_HAS_*` 位。Classic 返回现有 `Status::Code` 数值，TENT 返回 `0` 或 `-1`；短缓冲区按各自的无效参数返回值处理。

先用 `path_buf=NULL, path_capacity=0` 获取 `required_path_length`。路径已观测时，长度包含结尾 NUL；未观测时为 0。若长度非零，分配至少该大小的缓冲区再查询。缓冲区不足不会截断或写入路径。两次查询之间状态可能变化，调用方应以第二次返回的长度和字段为准，必要时重试。

```c
int state = TASK_CONGESTION_UNKNOWN;
int rc = getTaskCongestionState(engine, batch_id, task_id, &state);
task_congestion_detail_t detail = {0};
size_t required = 0;
if (rc == 0) {
    rc = getTaskCongestionDetail(engine, batch_id, task_id, &detail,
                                 NULL, 0, &required);
}
```

## TENT Python

现有 `tent.TransferEngine` binding 暴露 `get_task_congestion_state(batch_id, task_id)` 和 `get_task_congestion_detail(batch_id, task_id)`。极简结果是 `tent.TaskCongestionState` 枚举；详细结果是字典，包含 `state`、`attempt_kind`、`resolved` 以及可选的 `reason`、`failure_scope`、`affected_path`、`observed_at_ns`、`controller_mode`、`controller_generation`、`window_bytes`、`inflight_bytes`、`retry_after_ns` 等。无效 handle 抛出 `tent.InvalidArgumentError`。

```python
state = engine.get_task_congestion_state(batch_id, task_id)
detail = engine.get_task_congestion_detail(batch_id, task_id)
if state == tent.TaskCongestionState.LONG_UNAVAILABLE:
    # Do not treat this as proof of permanent peer failure.
    affected_path = detail["affected_path"]
```

Mooncake-pro 的 PUT 失败处理将来可以在 `free_batch` 前读取分类，再决定重试或上报；本轮没有修改 PUT 接口或错误类型。
