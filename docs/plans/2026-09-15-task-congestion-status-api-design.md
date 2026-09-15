# TE/TENT 任务拥塞状态双接口设计

日期：2026-09-15；状态：实现中，待验收。

## 目标与边界

调用方以 `(batch_id, task_id)` 查询同一条逻辑传输任务。极简接口回答它当前是正常、短时拥塞，还是长时不可用；详细接口给出支持判断的证据。Classic TE、TENT 的 C++/C 入口和现有 TENT Python binding 均覆盖。此轮只改 Mooncake TE/TENT；Mooncake-pro 的 PUT 消费层留作后续任务。

当前控制器只管理 RDMA。接口不把单次 WC、慢请求或隔离状态说成永久硬件失效，也不替代原有任务完成状态。Pro 将来可在 PUT 释放 batch 前查询失败 task，再将短时/长时/未知作为结构化错误信息返回；其现有 `put -> Result<ObjectRoute, StoreError>` 成功契约无需改变。

## 选择

- 从当前 slice/attempt 即时推导：健康主路径无写入，但 TENT failover 可销毁 attempt，无法可靠保留失败证据，且并发读取指针风险高。
- 在逻辑 task 上保存有界异常证据（采用）：只在 defer、avoid、错误、恢复等异常转换时更新；任务在 batch 存活期间保留证据，查询不触发传输进度。
- 扩充原有 `TransferStatus`：调用简单，但混合“是否完成”和“为什么受阻”，同时扩大现有 ABI 与所有 transport 的修改范围。

## API 契约

```cpp
Status getTaskCongestionState(BatchID batch_id, size_t task_id,
                              TaskCongestionState& state) const;
Status getTaskCongestionDetail(BatchID batch_id, size_t task_id,
                               TaskCongestionDetail& detail) const;
```

`TaskCongestionState` 为 `Normal / Congested / LongUnavailable / Unknown`。`Unknown` 表示控制器关闭、非 RDMA attempt、尚无可用证据，或任务因非 拥塞控制 原因终止；不能将它伪装成正常。无效 task ID 返回原有风格的错误。和现有 `getTransferStatus` 一样，调用方必须在 `freeBatch` 前查询；释放后的 batch handle 不可使用。

判定针对任务内尚未解决的 slice/attempt：

1. 存在因持续隔离、恢复探测失败或所有当前候选路径不可用而被阻住的部分，则为 `LongUnavailable`。
2. 否则存在受 byte window、接收端压力等短时原因延迟的部分，则为 `Congested`。
3. 否则进行中的 RDMA 任务为 `Normal`；成功终止也为 `Normal`。失败终止且没有 拥塞控制 证据为 `Unknown`。

重新 admission、可用路径 failover、探测成功或新 attempt 会消除旧 attempt 的当前受阻状态；详细接口可保留最后一条已解决异常供诊断，但标明 `resolved`。旧 generation/attempt 的反馈不能覆盖新任务状态。提交前整批 admission 拒绝没有已提交的 task；该情况仍使用 `submitTransfer` 的可重试返回值。

详细结果包括：极简状态、当前已知的 attempt 类型、最近有效原因与错误范围、受影响路径标识、观测时间、是否已解决，以及异常发生时可取得的控制器 mode/generation、窗口与在途字节。剩余隔离冷却时间只有在适配层实际观测到时才返回；首版未读取该值，保持“未观测”。详细查询只复制逻辑 task 保存的异常证据，不读取当前控制器状态，也不调用 verbs、metadata RPC、tick 或主动探测。各字段是异常发生时的 best-effort 快照，不表示查询时刻的状态。

C ABI 的极简函数输出枚举；详细函数输出固定数值字段，并用调用方长度参数两次读取变长路径文本，不截断路径。Python 返回同语义的枚举/字典。Classic `TransferEngine` 选择 TENT backend 时转发并转换同一语义。

## 生命周期与性能

异常记录属于逻辑 task，直到 batch 释放；不能引用可回收的 slice/RdmaTask 裸指针。TENT failover 为每个 attempt 标记代数，避免旧数据污染新 attempt。多 slice 只更新有变化的异常项，极简查询读取有界摘要，详细查询按需取更丰富证据。

编译关闭或 runtime off 不创建 拥塞控制 记录、不改变既有传输行为；公开查询返回 `Unknown`。健康提交/成功 completion 不增加新的 per-slice 原子写入、锁、RPC 或 verbs 调用。只在显式查询和异常转换路径增加工作。性能只能通过相同初态的 congestion-control-on/off 基准证实，不预先承诺零百分比回归。

## 验收

- Classic/TENT 的真实公开调用路径：一个 task 被 defer、长时 avoid、恢复，分别读到 `Congested / LongUnavailable / Normal`；多 slice 最严重未解决项优先。
- 失败 task 在 `freeBatch` 前仍可取到证据；非 RDMA、off、无证据失败返回 `Unknown`；invalid/free 后 handle 遵守现有生命周期合同。
- failover 后旧 attempt 回报、generation 更新、并发查询与释放边界不造成污染或悬挂指针。
- C++、C、TENT Python 的结果一致；C 路径文本两阶段容量协议不截断；查询不推动 progress。
- exact-head congestion-control-on/off 构建和回归通过，独立代码 review；记录真实两机 HCA 拥塞、故障分类与性能仍未验证的边界。

## 后续 Pro 接入

Mooncake-pro 当前 remote PUT 在 wait 后、处理失败前释放 batch。后续接入只需在两个 remote PUT 失败分支释放前查询失败 task，通过 Rust transport FFI/trait 取极简分类，并在 `StoreError` 的失败通道携带类型；保留现有 PUT 参数和成功返回。Python 若需程序化判断，可映射成带分类属性的异常。此轮不改 Pro 仓库。
