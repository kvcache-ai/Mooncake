# Classic TE RDMA 双边实现：代码功能划分

本文对照当前实现，按**功能模块**梳理新增/改动代码，并说明主要类型与函数职责。
设计语义见 [rdma-two-sided-control-plane.md](rdma-two-sided-control-plane.md)。

源码根目录：`mooncake-transfer-engine/`。

## 1. 总览

```text
应用 / TransferEngine
        │
        ├─ allocateManagedBuffer / releaseManagedBuffer
        └─ submitTransfer(Task) ──► RdmaTransport::submitTransferTask
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
           shouldUseTwoSided?                         单边路径
                    │                              submitOneSidedTasks
                    ▼                              (原 WorkerPool WRITE/READ)
           submitTwoSidedTasks
                    │
        ┌───────────┼──────────────┐
        ▼           ▼              ▼
  SenderCredit   MsgChannel     CtrlChannel
  (admission)    (数据 SEND)    (控制帧 / ACK / GRANT)
        │           │              │
        │           ▼              ▼
        │      BouncePool     typed CtrlFrame
        │      MsgHeader
        └───────────┴──────────────┘
```

| 模块 | 主要文件 | 职责 |
|------|----------|------|
| 控制通道 | `ctrl_channel.{h,cpp}` | per-peer notify QP，控制面 SEND/RECV |
| 控制帧编解码 | `ctrl_frame.{h,cpp}` | 类型化帧与 payload |
| Credit 账本 | `sender_credit.{h,cpp}` | 发送侧累计 grant / tryReserve |
| 消息通道 | `msg_channel.{h,cpp}` | per-peer 数据 msg QP |
| Bounce 池 | `bounce_pool.{h,cpp}` | TE 托管 send/recv 槽 |
| 消息头 | `msg_header.h` | DATA_WRITE / READ_REQ / READ_RESP |
| 双边编排 | `two_sided.cpp` + `rdma_transport.*` | managed buf、分流、ACK、排队 |
| 对外 API | `transfer_engine*.h/cpp` | `allocateManagedBuffer` 等 |
| 握手/元数据 | `transfer_metadata.*`、handshake 字段 | `notify_*` / `msg_*` / `two_sided` |
| 配置 | `config.h` / `config.cpp` | `MC_RDMA_NOTIFY_*` / `CREDIT_*` / `MSG_*` |
| 测试 | `tests/{ctrl_frame,sender_credit,rdma_notify,rdma_twosided}_test.cpp` | 单测 / e2e |

---

## 2. 控制面：CtrlChannel

**路径**：`include/transport/rdma_transport/ctrl_channel.h`
`src/transport/rdma_transport/ctrl_channel.cpp`

Per-peer 独立 RC QP + CQ，承载类型化控制帧（credit、DATA_ACK、SESSION、兼容 notify）。不进入 EndpointStore。

### 生命周期 / 建联

| 函数 | 功能 |
|------|------|
| `CtrlChannel(...)` | 绑定 `RdmaTransport`、本地 `RdmaContext`、对端 server 名 |
| `~CtrlChannel` | 销毁 QP/CQ/MR |
| `construct()` | 分配资源，QP 到 INIT；**不**完成建联 |
| `createResources()` | 创建 CQ、QP、send/recv MR 与缓冲 |
| `destroyResources()` | 释放上述资源 |
| `connectActive()` | 主动侧：OOB handshake 交换 `notify_qp_num` 等，再 `connectQp` |
| `acceptPassive(...)` | 被动侧：根据 peer handshake 完成建联并回填 `local_desc` |
| `connectQp(...)` | QP 状态机 INIT→RTR→RTS（含 GID/LID/对端 qp_num） |
| `fillLocalDesc(...)` | 向 handshake 写入本端 `notify_qp_num` / `notify_rq_depth` / `ctrl_channel` |
| `connected()` / `notifyQpNum()` / `notifyRqDepth()` | 连接状态与能力查询 |
| `disconnect()` | 标记断开并清理 |

### 收发与完成

| 函数 | 功能 |
|------|------|
| `postRecv(idx)` | 预贴单个 recv 槽 |
| `repostAllRecvs()` | 建联后批量预贴 RQ |
| `sendCtrlFrame(frame)` | 编码并 `IBV_WR_SEND` 类型帧；受 pending SEND 上限流控 |
| `sendNotify(notify)` | 兼容旧 `NotifyDesc`：封成 `NOTIFY_COMPAT` 再发送 |
| `postSessionOpen()` | 建联后发送 `SESSION_OPEN`（bounce 能力声明） |
| `pollCompletions(...)` | 轮询 CQ：RECV 交给 `dispatchRecvPayload`，SEND 交给 `handleSendComplete` |
| `pollSendCompletions(...)` | 只排空 SEND CQE，避免占槽死等 |
| `dispatchRecvPayload(...)` | 识别 magic→`decodeCtrlFrame` 回调 transport；否则走 legacy notify |
| `handleSendComplete()` | 减少 pending SEND，唤醒等待发送的线程 |

---

## 3. 控制帧：CtrlFrame

**路径**：`include/transport/rdma_transport/ctrl_frame.h`
`src/transport/rdma_transport/ctrl_frame.cpp`

线格式（host 侧结构 + 编解码）：

```text
| magic | ver | type | flags | session | epoch | seq | ack_seq | payload_len | payload... |
```

### 类型与结构

| 符号 / 类型 | 功能 |
|-------------|------|
| `CtrlFrameType` | `CREDIT_GRANT` / `DATA_ACK` / `SESSION_OPEN` / `NOTIFY_COMPAT` 等 |
| `CreditResource` | `DataBytes` / `RequestSlots` / `BounceBytes` / `BounceSlots` |
| `CreditAmount` / `DataAckEntry` | grant 与 ACK payload 元素 |
| `CtrlFrame` | 内存中的完整帧 |

### 编解码函数

| 函数 | 功能 |
|------|------|
| `isCtrlFrameMagic` | 快速判断是否类型化帧 |
| `encodeCtrlFrame` / `decodeCtrlFrame` | 整帧编解码 |
| `encodeNotifyCompatPayload` / `decodeNotifyCompatPayload` | 旧 notify 名+消息 |
| `encodeCreditGrantPayload` / `decodeCreditGrantPayload` | 累计 credit grant 列表 |
| `encodeDataAckPayload` / `decodeDataAckPayload` | `(task_id, acked_bytes)` 列表 |
| `encodeSessionOpenPayload` / `decodeSessionOpenPayload` | bounce 槽数与槽大小 |

---

## 4. Credit：SenderCreditLedger

**路径**：`include/transport/rdma_transport/sender_credit.h`
`src/transport/rdma_transport/sender_credit.cpp`

发送方本地账本：key = `(peer, session)`，累计 `grant_total`，`consumed` 随 `tryReserve` 增加。

| 函数 | 功能 |
|------|------|
| `activate(peer, session, epoch)` | 打开/刷新会话条目 |
| `deactivate(...)` | 删除会话条目 |
| `applyGrant(..., seq, grants, disposition)` | 应用累计 `CREDIT_GRANT`；`disposition`：0 新、1 重复/旧、2 缺口 |
| `tryReserve(peer, session, charge)` | 预留额度；不足返回 `ERR_TOO_MANY_REQUESTS` |
| `rollbackReservation(...)` | 提交失败时退回额度 |
| `available(...)` / `availableForPeer(...)` | 查询剩余 |
| `hasPeer(...)` | 是否已有该 peer 的会话 |
| `resourceIndex` / `normalize`（私有） | 资源枚举 ↔ 数组下标、charge 归一化 |

双边数据路径在 `dispatchTwoSidedTask` 里对 `BounceSlots` / `BounceBytes` 做 `tryReserve`。

---

## 5. 数据面：MsgChannel + BouncePool + MsgHeader

### 5.1 MsgHeader

**路径**：`include/transport/rdma_transport/msg_header.h`

| 符号 / 函数 | 功能 |
|-------------|------|
| `MsgType` | `DATA_WRITE` / `READ_REQ` / `READ_RESP` |
| `MsgHeader` | `task_id`、`slice_seq`、`dest_addr`、`length` 等 |
| `encodeMsgHeader` / `decodeMsgHeader` | 固定 32 字节头编解码 |

### 5.2 BouncePool

**路径**：`include/transport/rdma_transport/bounce_pool.h`
`src/transport/rdma_transport/bounce_pool.cpp`

| 函数 | 功能 |
|------|------|
| `construct(pd, slot_size, slot_count)` | 分配 send/recv 槽并 `ibv_reg_mr` |
| `destroy()` | 注销 MR、释放内存 |
| `acquireSendSlot` / `releaseSendSlot` | 发送槽租用 / 归还（SEND CQE 后） |
| `slotPtr` / `slotMr` / `slotLkey` | 发送槽地址与 MR |
| `recvSlotPtr` / `recvSlotMr` | 接收槽（常驻 RQ） |
| `expand(extra)` | 按需扩槽（先扩池再抬 grant，设计预留） |
| `freeCount` / `slotSize` / `slotCount` | 池状态查询 |

> **TODO**
>
> - 实现 **`shrink`**（迟滞 + 冷却；先压 `CREDIT_GRANT` → RETIRING → dereg；不低于
>   `rdma_msg_pool_base`）。
> - 增加 **bounce 池管理线程**（或等价后台周期任务）：根据空闲水位、WAITING 队列、
>   inflight 与冷却时间决定 **何时扩张 / 何时收缩**，以及 **扩张、收缩步长**
>   （上限 `rdma_msg_pool_max`）；扩张后抬 grant，收缩前压 grant，并与 RQ repost /
>   `redispatchWaitingTasks` 闭环。
> - 现状：`expand` 已实现但 **无人调用**；无收缩；池大小固定为 construct 时的 base。

### 5.3 MsgChannel

**路径**：`include/transport/rdma_transport/msg_channel.h`
`src/transport/rdma_transport/msg_channel.cpp`

Per-peer 数据 RC QP；payload 经 bounce：`managed/src → send bounce → 网络 → recv bounce → managed/dst`。

| 函数 | 功能 |
|------|------|
| `construct` / `createResources` / `destroyResources` | QP/CQ/BouncePool 生命周期 |
| `connectActive` / `acceptPassive` / `connectQp` | 与 Ctrl 类似的 msg QP 握手（`msg_qp_num` 等） |
| `fillLocalDesc` | 填 `msg_qp_num` / `msg_rq_depth` / `msg_channel` |
| `sendDataWrite(...)` | 组 `DATA_WRITE` 头 + 拷贝 payload → `postSend` |
| `sendReadReq(...)` | 仅头发 `READ_REQ`（无 payload） |
| `sendReadResp(...)` | 响应方：源数据进 bounce 后发 `READ_RESP` |
| `postSend(hdr, payload, length)` | 取 send 槽、拼包、`IBV_WR_SEND` |
| `postRecv` / `repostAllRecvs` | RQ 预贴 |
| `pollCompletions` | 处理 SEND/RECV CQE |
| `dispatchRecv(idx, byte_len)` | 解头 → `RdmaTransport::onMsgReceived` → repost |
| `handleSendComplete(wr_id)` | 释放对应 send bounce 槽 |
| `disconnect` | 断开并销毁 |

> **TODO / 演进：多轨 Msg + poll 对齐 WorkerPool**
>
> - **现状**：每 peer 单条 `MsgChannel`；Msg/Ctrl CQ 均由 `rdma_transport.cpp` 里
>   **一条** `ctrlWorkerLoop` 轮询（非 per-NIC `WorkerPool`）。双边大块流量可与单边
>   同量级，此模型撑不住线速。
> - **目标**：按 rail / nic-path 建多条 msg QP；chunk 多轨喷洒；**Msg CQ 交由对应
>   context 的 WorkerPool（或同级 poller）**；Ctrl 仍可 per-peer 单通道。
> - Handshake 交换每轨能力；credit/bounce 与多轨计量、不超卖一并设计。

---

## 6. 双边编排：RdmaTransport（two_sided + transport）

**路径**：

- `src/transport/rdma_transport/two_sided.cpp`（managed / 分流 / msg 收发逻辑）
- `src/transport/rdma_transport/rdma_transport.cpp`（ctrl worker、credit/session、submit 分叉）
- `include/transport/rdma_transport/rdma_transport.h`

> **TODO：编排层文件拆分（现状混乱）**
>
> **现状**
>
> - 有独立 `two_sided.cpp`，**没有**对称的 `one_sided.cpp`；单边仍堆在
>   `rdma_transport.cpp` 的 `submitOneSidedTasks`（历史主路径）。
> - 即便有 `two_sided.cpp`，双边相关实现也 **拆成两半**：
>   - 已在 `two_sided.cpp`：managed API、`shouldUseTwoSided`、Msg setup/ensure、
>     `submit/dispatchTwoSided*`、WAITING 重投、`onMsgReceived` / `sendDataAck` /
>     `completeTwoSidedAck`。
>   - 仍在 `rdma_transport.cpp`：`submitTransferTask` 分叉、`submitOneSidedTasks`、
>     Ctrl setup/ensure、`onCtrlFrameReceived`、SESSION/GRANT/ACK 处理、
>     `ctrlWorkerLoop`、以及大量与双边无关的原有逻辑。
> - 成员仍全是 `RdmaTransport::`，文件边界只是编译单元切割，**没有**清晰模块边界。
>
> **建议整理（择一或渐进）**
>
> 1. **对称拆分**：`one_sided.cpp`（`submitOneSidedTasks` + 仅单边用的辅助）↔
>    `two_sided.cpp`（现有双边 submit/msg/managed）；`rdma_transport.cpp` 只留
>    install / register / `submitTransferTask` 分流胶水。
> 2. **按子系统再拆**：`ctrl_plane.cpp`（Ctrl ensure/setup、frame 分发、SESSION/GRANT、
>    ctrl worker）、`two_sided.cpp`（msg + admission + ACK 完成）、可选
>    `ctrl_worker.cpp`；单边继续 `one_sided.cpp` 或暂留 transport。
> 3. **原则**：Ctrl 接线与 Msg 数据编排不要继续混增在 `rdma_transport.cpp`；
>    新功能优先落到对应 `.cpp`，避免第三个「半截 two_sided」。
>
> 此项为结构债，可与「Msg 多轨 / WorkerPool poll」重构一起做，或先做纯搬迁、行为不变。

### 6.1 Managed buffer API

| 函数 | 功能 |
|------|------|
| `allocateManagedBuffer(length)` | `posix_memalign` + 注册为 `two_sided` buffer，记入 `managed_buffers_` |
| `releaseManagedBuffer(addr)` | 反注册并 `free`（须在 transfer `COMPLETED` 之后） |
| `isLocalManaged(addr, length)` | 本地地址是否落在 managed 段内 |
| `isRemoteTwoSided(target_id, offset, length)` | 远端 metadata 中对应 buffer 是否 `two_sided` |
| `validateLocalManagedDest(...)` | 接收落位前校验目的地址合法 |

### 6.2 提交分流

| 函数 | 功能 |
|------|------|
| `submitTransferTask(...)` | 按任务拆成双边 / 单边两组 |
| `shouldUseTwoSided(req)` | `msg` 开启且默认、本地 managed、远端 `two_sided` → 双边 |
| `submitTwoSidedTasks(...)` | 建跟踪 slice；`dispatch`；credit 不足则进 `waiting_tasks_`（WAITING） |
| `submitOneSidedTasks(...)` | 原单边 WRITE/READ 热路径（基本不改语义） |
| `dispatchTwoSidedTask(task)` | 确保 Ctrl/Msg 通道 → `tryReserve` → 切分 chunk 发 WRITE 或 READ_REQ |
| `redispatchWaitingTasks()` | 收到新 GRANT 后重试排队任务 |

### 6.3 完成与 ACK

| 函数 | 功能 |
|------|------|
| `onMsgReceived(peer, hdr, payload)` | 处理 `DATA_WRITE`（落位+发 ACK）、`READ_REQ`（回 READ_RESP）、`READ_RESP`（落位并完成本地 task） |
| `sendDataAck(peer, task_id, acked_bytes)` | CtrlChannel 发累计 `DATA_ACK` |
| `completeTwoSidedAck(task_id, acked_bytes)` | 发送方收到 ACK：累计字节达总量则 `slice->markSuccess()` → `COMPLETED` |

### 6.4 Ctrl 接线（transport 内）

| 函数 | 功能 |
|------|------|
| `onSetupCtrlChannel` / `onSetupMsgChannel` | handshake 回调：被动建立 Ctrl/Msg |
| `ensureCtrlChannel` / `ensureMsgChannel` | 主动侧懒创建并 `connectActive` |
| `onSetupRdmaConnections` | 在原有 data endpoint 握手中挂上 ctrl/msg 能力字段 |
| `sendRdmaNotify` | 优先 CtrlChannel，失败可 OOB fallback |
| `onCtrlNotifyReceived` | 兼容 notify 入队 |
| `onCtrlFrameReceived` | 按 `CtrlFrameType` 分发 |
| `handleSessionOpen` | 解析对端 bounce 能力，`activate` ledger，延迟发初始 GRANT |
| `sendInitialCreditGrant` / `sendCreditGrant` | 向对端发送 `CREDIT_GRANT` |
| `handleCreditGrant` | `applyGrant` + `redispatchWaitingTasks` |
| `handleDataAck` | 转 `completeTwoSidedAck` |
| `startCtrlWorker` / `stopCtrlWorker` / `ctrlWorkerLoop` | 后台轮询所有 Ctrl/Msg CQ（MVP；Msg 多轨后应下沉到 per-NIC WorkerPool，见 §5.3 TODO） |

---

## 7. 对外 API 与元数据

### 7.1 TransferEngine

**路径**：`include/transfer_engine.h`、`transfer_engine_impl.h`、对应 `.cpp`

| 函数 | 功能 |
|------|------|
| `allocateManagedBuffer` | 转发到 RDMA transport；失败返回 `nullptr` |
| `releaseManagedBuffer` | 转发释放 |

用法：`alloc → submit → 等 COMPLETED → release`。单边仍用 `registerLocalMemory`。

### 7.2 Metadata / Handshake

**路径**：`include/transfer_metadata.h` 及编解码实现

| 字段 / API | 功能 |
|------------|------|
| `HandShakeDesc::notify_qp_num` / `notify_rq_depth` / `ctrl_channel` | Ctrl 能力交换 |
| `HandShakeDesc::msg_qp_num` / `msg_rq_depth` / `msg_channel` | Msg 能力交换 |
| `BufferDesc::two_sided` | 标记 managed/双边落位段，供对端选路径 |
| `TransferMetadata::pushNotify`（及相关） | Ctrl 收到的兼容 notify 入队 |

---

## 8. 配置项

**路径**：`include/config.h`、`src/config.cpp`（环境变量）

| 环境变量（概念） | 配置字段 | 作用 |
|------------------|----------|------|
| `MC_RDMA_NOTIFY_ENABLED` | `rdma_notify_enabled` | 开关 CtrlChannel |
| `MC_RDMA_NOTIFY_RECV_COUNT` / `BUFFER_SIZE` / `MAX_PENDING_SENDS` | 对应 size 字段 | Ctrl 池深度与槽大小 |
| `MC_RDMA_NOTIFY_OOB_FALLBACK` | `rdma_notify_oob_fallback` | RDMA notify 失败回退 RPC |
| `MC_RDMA_CREDIT_ENABLED` | `rdma_credit_enabled` | credit admission |
| `MC_RDMA_CREDIT_*` / `CTRL_FAIL_OPEN` | window / timeout / fail-open | 单边滑窗与策略 |
| `MC_RDMA_MSG_ENABLED` / `MSG_DEFAULT` | `rdma_msg_*` | 双边数据面总开关与默认优先 |
| `MC_RDMA_MSG_SLOT_SIZE` / `POOL_BASE` / `POOL_MAX` | bounce 槽参数 | 池大小与单槽容量 |

---

## 9. 测试对照

| 测试 | 覆盖功能 |
|------|----------|
| `ctrl_frame_test` | 帧 / payload 编解码 |
| `sender_credit_test` | activate / grant / reserve / rollback |
| `rdma_notify_test` | CtrlChannel 建联、notify、Session+Grant |
| `rdma_twosided_test` | managed WRITE/READ e2e 与轻量吞吐 |

---

## 10. 调用链速查

### WRITE（双边）

```text
allocateManagedBuffer (两端)
submitTransferTask
  → shouldUseTwoSided
  → submitTwoSidedTasks → dispatchTwoSidedTask
      → tryReserve(Bounce*)
      → MsgChannel::sendDataWrite  (src → bounce → SEND)
对端:
  MsgChannel RECV → onMsgReceived(DATA_WRITE)
      → memcpy 到 managed dest → sendDataAck
本端:
  onCtrlFrameReceived(DATA_ACK) → completeTwoSidedAck → COMPLETED
releaseManagedBuffer
```

### READ（双边）

```text
dispatchTwoSidedTask → MsgChannel::sendReadReq
对端: onMsgReceived(READ_REQ) → sendReadResp (本地 managed src → bounce)
本端: onMsgReceived(READ_RESP) → memcpy 到 local source → markSuccess
```

### 控制面建联

```text
ensureCtrlChannel / onSetupCtrlChannel
  → construct → connectActive|acceptPassive
  → postSessionOpen
  → (延迟) sendInitialCreditGrant
ctrlWorkerLoop 持续 poll CtrlChannel + MsgChannel
```

---

## 11. 与单边路径的边界

| 项目 | 单边 | 双边（本文） |
|------|------|----------------|
| 缓冲 | 用户 `registerLocalMemory` | `allocateManagedBuffer` |
| 数据 QP | EndpointStore data QP | 独立 `MsgChannel` |
| 完成语义 | 本地 WRITE/READ CQE | 发送方以 `DATA_ACK` 为准（READ 以本地收齐 RESP 为准） |
| Credit | 可选滑窗 / fail-open | bounce 资源；不足 WAITING |
| 热路径改动 | `WorkerPool` / `submitPostSend` 基本不变 | `submitTransferTask` 分叉 + 新模块 |

单边建联与回程复用同一 RC endpoint 的行为不变；Ctrl/Msg 是**额外**的 per-peer 通道，不是「回程再开一条单边 data 连接」。
