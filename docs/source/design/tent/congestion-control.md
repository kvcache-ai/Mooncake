# 拥塞控制插件（Congestion Control Plugin）

## 概述

拥塞控制插件是 TENT Runtime 在数据传输热路径上引入的可插拔扩展点，用于在请求提交、完成回调以及拥塞信号触发时执行自定义的准入控制（admission control）与速率反馈（rate feedback）逻辑。该机制的目标，是在不修改 Transport、不破坏既有 QoS 模型的前提下，让上层运维或调度组件能够针对不同的部署形态（如 HPN 7.0、M2N 模型、跨机架 RDMA 等）注入差异化的拥塞规避策略。

默认实现为 NoOp 插件——所有请求一律准入，不施加任何速率限制，也不消费完成事件，因而对生产路径完全无副作用。

## 背景与问题

在大规模 LLM 推理与训练集群的部署实测中，Transfer Engine 暴露出若干网络层面的尾延迟与丢包风险，需要一种轻量级、可热插拔的机制来缓解：

| 编号 | 问题 | 现象 | 当前状态 |
|------|------|------|----------|
| 3.1 | 网卡跨 NUMA 访问引发 PFC | 远端 NUMA 的 NIC DMA 触发反压 PFC，影响共享交换机其他流量 | 已通过 NUMA 亲和的 DeviceSelector 解决 |
| 3.3 | M2N 模型下 ASW Buffer 耗尽 | 多对 N 的同步聚合（多个 prefill → N 个 decode）瞬时打满接入交换机的共享 buffer，触发丢包重传 | 待解决，需准入限流 |
| 3.4 | HPN 7.0 上行链路拥塞 | 跨机架（leaf→spine）带宽过订购（oversubscription），上行链路成为瓶颈，长尾延迟显著上升 | 待解决，需拓扑感知调度 |
| 3.5 | L20D PCIe Gen5 带宽瓶颈 | GPU↔NIC 走 PCIe Gen5 时单方向理论带宽不足，影响小消息聚合 | 已有规避方案（rebar / 拷贝路径切换） |

其中 3.3 与 3.4 是本插件接口最直接的目标场景。

### Transfer Engine 现有缺口

在引入本插件之前，Transfer Engine 在端到端路径上具备以下能力：
- 多 Transport 后端（RDMA / TCP / NVLink / EFA / Ascend 等）；
- 基于 NUMA 与 EWMA 带宽的 DeviceSelector；
- 三优先级的 QoS 队列与全局时间片协调；
- 失败重试与故障切换。

但缺失以下能力：
- **请求级速率控制**：无法在提交侧根据全局/单设备 inflight 流量主动延后或拒绝请求；
- **拥塞反馈闭环**：完成事件没有被聚合为可用于决策的信号，超时/错误也没有反向通知到准入逻辑；
- **拓扑感知准入**：缺少在准入阶段根据目标段位置、链路占用情况覆盖设备选择的扩展点。

拥塞控制插件接口正是为了填补这一缺口。

## 设计目标

1. **可插拔（Pluggable）**：以抽象基类暴露接口，允许在不同部署中加载不同的算法实现（AIMD、BBR-like、拓扑感知、强化学习等），运行时通过 `setCongestionControlPlugin()` 注入。
2. **零开销默认（Zero overhead when disabled）**：默认 NoOp 实现保证在未启用拥塞控制的部署中，热路径不产生额外内存分配、锁竞争或显著的指令开销。
3. **热路径友好（Hot-path friendly）**：单次 `onAdmit` 调用预算 < 100 ns，禁止阻塞、系统调用或重型分配；状态读写应使用无锁数据结构或线程本地缓存。
4. **Transport 无关（Transport-agnostic）**：接口定义在 Runtime 层，对 RDMA、TCP、NVLink 等所有 Transport 一视同仁；上下文中不暴露任何 Transport 私有字段。
5. **最小侵入（Minimal intrusion）**：仅在三个明确定义的位置插桩——提交准入、完成反馈、拥塞信号——不污染 Transport 内部循环。
6. **可观测（Observable）**：插件名称与状态可被外部查询，便于与 TentMetrics 集成。

## 架构设计

### 整体架构

拥塞控制插件位于 Application 与 Transport 之间，与 DeviceSelector、Worker 形成反馈闭环：

```
        ┌────────────────────────────────────────────────────────────┐
        │                     Application                             │
        │              (Mooncake Store / EP / 用户代码)                │
        └────────────────────────┬───────────────────────────────────┘
                                 │ submitTransfer(batch_id, requests)
                                 ▼
        ┌────────────────────────────────────────────────────────────┐
        │              TransferEngineImpl::submitTransfer            │
        │                                                            │
        │   ┌──────────────────────────────────────────────────┐    │
        │   │  Admission Hook                                   │◀───┼──┐
        │   │   ctx = build AdmitContext(req, inflight, dev)    │    │  │
        │   │   decision = plugin->onAdmit(ctx)                 │    │  │
        │   │   if (!decision.admit) defer/reject               │    │  │
        │   │   if (decision.device_mask_override) override     │    │  │
        │   └──────────────────────────────────────────────────┘    │  │
        │                          │                                 │  │
        │                          ▼                                 │  │
        │                  Transport Selector                        │  │
        │                          │                                 │  │
        │                          ▼                                 │  │
        │                   Device Selector                          │  │
        │                          │                                 │  │
        └──────────────────────────┼─────────────────────────────────┘  │
                                   ▼                                    │
                          ┌────────────────┐                            │
                          │   Transport    │                            │
                          │  (RDMA / TCP)  │                            │
                          └───────┬────────┘                            │
                                  │ post WR / send                      │
                                  ▼                                     │
                          ┌────────────────┐                            │
                          │      QP /      │                            │
                          │     Socket     │                            │
                          └───────┬────────┘                            │
                                  │ CQE / ack                           │
                                  ▼                                     │
        ┌────────────────────────────────────────────────────────────┐  │
        │                   Workers::asyncPollCq                     │  │
        │                                                            │  │
        │   ┌────────────────────────────┐  ┌──────────────────┐    │  │
        │   │ DeviceSelector::release    │──│ onCompletion     │────┼──┤
        │   │   (success path)           │  │ (latency, bytes) │    │  │
        │   └────────────────────────────┘  └──────────────────┘    │  │
        │                                                            │  │
        │   ┌────────────────────────────┐  ┌──────────────────┐    │  │
        │   │ timeout / error detection  │──│onCongestionSignal│────┼──┘
        │   └────────────────────────────┘  └──────────────────┘    │
        └────────────────────────────────────────────────────────────┘
```

闭环说明：完成事件与拥塞信号被推送回插件，使后续 `onAdmit` 调用能够基于最新的链路状态做出决策。

### 插件接口

核心抽象基类 `CongestionControlPlugin` 定义在头文件 `tent/include/tent/runtime/congestion_control.h` 中：

```cpp
class CongestionControlPlugin {
   public:
    virtual ~CongestionControlPlugin() = default;

    /// 提交准入：在请求被分派至 Transport 之前调用。
    /// 必须快速返回（典型 < 100 ns），处于热路径。
    virtual AdmitDecision onAdmit(const AdmitContext& ctx) = 0;

    /// 完成反馈：在传输成功或失败完成之后调用。
    /// 用于更新 EWMA、拥塞窗口等内部状态。
    virtual void onCompletion(const CompletionEvent& event) = 0;

    /// 拥塞信号：当探测到超时、QP 错误或 ECN 标记时调用。
    virtual void onCongestionSignal(const CongestionSignal& signal) = 0;

    /// 查询指定设备当前速率上限（bytes/sec）。0 表示不限速。
    virtual uint64_t getDeviceRateLimit(int device_id) const = 0;

    /// 插件名，用于日志与诊断。
    virtual const char* getName() const = 0;
};

/// 工厂函数：返回默认的 NoOp 插件。
std::shared_ptr<CongestionControlPlugin> createDefaultCongestionControlPlugin();
```

| 方法 | 调用时机 | 调用频率 | 性能预算 | 可否阻塞 |
|------|----------|----------|----------|----------|
| `onAdmit` | 每个请求提交前 | 每请求一次 | < 100 ns | 否 |
| `onCompletion` | 每次传输完成（成功/失败） | 每请求一次 | < 1 µs | 否 |
| `onCongestionSignal` | 超时/QP 错误/ECN | 偶发 | < 10 µs | 否 |
| `getDeviceRateLimit` | DeviceSelector 查询 | 频繁但非每请求 | < 50 ns | 否 |
| `getName` | 日志/指标导出 | 低频 | 无 | 否 |

### 上下文结构

#### AdmitContext

提交准入时的输入上下文：

| 字段 | 类型 | 说明 |
|------|------|------|
| `request` | `const Request&` | 当前提交的请求引用，含 opcode、长度、目标段等 |
| `global_inflight_bytes` | `uint64_t` | 当前 Engine 全局在途字节数 |
| `device_inflight_bytes` | `uint64_t` | 当前候选设备的在途字节数 |
| `device_id` | `int` | DeviceSelector 给出的初步候选设备 ID |
| `priority` | `int` | 请求优先级（与 QoS 一致：0=HIGH, 1=MEDIUM, 2=LOW） |

#### AdmitDecision

准入回调的返回值：

| 字段 | 类型 | 说明 |
|------|------|------|
| `admit` | `bool` | `true` 表示放行，`false` 表示推迟（Engine 将根据 `defer_us` 入队等待） |
| `defer_us` | `float` | 建议推迟的微秒数；仅在 `admit == false` 时生效 |
| `device_mask_override` | `uint64_t` | 设备位掩码覆盖。`0` 表示沿用 DeviceSelector 的选择；非零时只允许从该掩码内的设备中选取 |

#### CompletionEvent

传输完成事件：

| 字段 | 类型 | 说明 |
|------|------|------|
| `device_id` | `int` | 实际执行该请求的设备 ID |
| `bytes` | `uint64_t` | 该请求成功/失败传输的字节数 |
| `latency_us` | `double` | 端到端延迟（从提交到完成） |
| `status` | `TransferStatusEnum` | 状态码（COMPLETED / FAILED / TIMEOUT 等） |
| `priority` | `int` | 请求优先级 |

#### CongestionSignal

异常信号：

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | `enum Type` | 信号类型，见下表 |
| `device_id` | `int` | 触发信号的设备 ID |
| `timestamp_ns` | `uint64_t` | 信号产生时刻（单调时钟，纳秒） |
| `severity` | `double` | 严重度，范围 `[0.0, 1.0]`，用于区分轻微抖动与持续拥塞 |

支持的信号类型：

| 取值 | 含义 | 触发位置 |
|------|------|----------|
| `TIMEOUT_SPIKE` | 超时尖刺 | `Workers::asyncPollCq` 检测到 WR 超时 |
| `QP_ERROR` | QP 错误（含 RNR/Retry 耗尽） | RDMA Transport 完成回调 |
| `LATENCY_SPIKE` | 延迟突增 | 完成路径中观察到 EWMA 大幅偏离 |
| `ECN_MARK` | 收到 ECN 标记 | 预留，待硬件信号集成后启用 |

## 集成点

### 提交路径（Admission Hook）

钩子被插入在 `TransferEngineImpl::submitTransfer` 中，介于请求构造与 Transport 分派之间：

```cpp
// 伪代码：实际位置见 transfer_engine_impl.cpp::submitTransfer
for (auto& req : requests) {
    int dev = device_selector_->pick(req);
    AdmitContext ctx{
        req,
        global_inflight_bytes_.load(),
        device_selector_->inflight(dev),
        dev,
        req.priority,
    };
    AdmitDecision d = cc_plugin_->onAdmit(ctx);
    if (!d.admit) {
        deferQueue_.enqueue(req, d.defer_us);
        continue;
    }
    if (d.device_mask_override != 0) {
        dev = device_selector_->pickFromMask(d.device_mask_override);
    }
    transport_->post(req, dev);
}
```

`device_mask_override` 的语义是**收紧**而非扩展：插件无法选择不在原候选集内的设备，但可以将候选集限制为某个子集（例如剔除当前已观测到拥塞的上行端口），从而实现拓扑感知的规避。

### 完成反馈（Completion Feedback）

完成回调挂载在 `DeviceSelector::release()` 中——这是所有 Transport 在请求完成时的统一汇聚点：

```cpp
void DeviceSelector::release(int device_id, uint64_t bytes,
                              double latency_us,
                              TransferStatusEnum status,
                              int priority) {
    // 既有逻辑：归还在途计数、更新 EWMA 带宽
    inflight_[device_id] -= bytes;
    bandwidth_ewma_[device_id].update(bytes, latency_us);

    // 新增：通知拥塞控制插件
    if (cc_plugin_) {
        CompletionEvent ev{device_id, bytes, latency_us, status, priority};
        cc_plugin_->onCompletion(ev);
    }
}
```

可用指标说明：

- `bytes` 与 `latency_us` 可直接用于估计瞬时吞吐与 RTT；
- `status != COMPLETED` 时表示链路异常或超时，可与 `CongestionSignal` 联合判断；
- 多设备场景下，插件可累计 per-device EWMA，用于驱动 `getDeviceRateLimit` 的返回值。

### 拥塞信号（Congestion Signal）

异常路径来自 `Workers::asyncPollCq`，在以下情形下触发：

```cpp
// 超时检测
if (now_ns - wr.submit_ns > kWrTimeoutNs) {
    CongestionSignal sig{
        CongestionSignal::TIMEOUT_SPIKE,
        wr.device_id,
        now_ns,
        std::min(1.0, (now_ns - wr.submit_ns) / (double)kSeverityScale),
    };
    cc_plugin_->onCongestionSignal(sig);
}

// QP 错误（RDMA Transport 在收到 IBV_WC_*_ERR 时）
if (wc.status != IBV_WC_SUCCESS) {
    CongestionSignal sig{
        CongestionSignal::QP_ERROR,
        wr.device_id,
        now_ns,
        1.0,  // QP 错误一律视为最高严重度
    };
    cc_plugin_->onCongestionSignal(sig);
}
```

严重度（`severity`）的含义：

| 范围 | 语义 | 建议反应 |
|------|------|----------|
| `[0.0, 0.3)` | 轻微抖动，可能为正常方差 | 可忽略或仅做计数 |
| `[0.3, 0.7)` | 中等拥塞 | 收缩窗口、降低准入速率 |
| `[0.7, 1.0]` | 严重拥塞或硬件错误 | 快速回退、考虑 device 摘除 |

## 使用方式

### 注册插件

`TransferEngineImpl` 暴露 `setCongestionControlPlugin()` 用于注入实例。注入应当发生在 Engine 启动之后、首次 `submitTransfer` 之前；运行时替换需保证旧插件的引用计数被正确处理（实现方使用 `std::shared_ptr`）。

```cpp
#include "tent/runtime/congestion_control.h"
#include "tent/transfer_engine.h"

using namespace mooncake::tent;

// 1. 创建 Engine
auto engine = std::make_shared<TransferEngine>();
engine->init(/* metadata server, local hostname, ... */);

// 2. 安装自定义插件
auto plugin = std::make_shared<MyAimdPlugin>(/* config */);
engine->impl()->setCongestionControlPlugin(plugin);

// 3. 正常提交请求；插件将参与每次准入与完成反馈
engine->submitTransfer(batch_id, requests);
```

如未调用 `setCongestionControlPlugin`，Engine 内部默认持有由 `createDefaultCongestionControlPlugin()` 构造的 NoOp 实例，行为与未启用拥塞控制完全等价。

### 实现自定义插件

下面给出一个最小骨架——基于 AIMD（Additive Increase, Multiplicative Decrease）思想的设备级窗口控制：

```cpp
#include "tent/runtime/congestion_control.h"
#include <atomic>
#include <array>

namespace mooncake::tent {

class AimdCongestionControlPlugin : public CongestionControlPlugin {
   public:
    static constexpr int kMaxDevices = 32;

    AdmitDecision onAdmit(const AdmitContext& ctx) override {
        const uint64_t window = window_bytes_[ctx.device_id].load(
            std::memory_order_relaxed);
        if (ctx.device_inflight_bytes + ctx.request.length > window) {
            // 超过窗口，建议推迟
            return {false, /*defer_us=*/50.0f, /*device_mask_override=*/0};
        }
        return {true, 0.0f, 0};
    }

    void onCompletion(const CompletionEvent& ev) override {
        if (ev.status != TransferStatusEnum::COMPLETED) return;
        // AI: 加性增窗（每次成功增加 1 个 MTU）
        auto& w = window_bytes_[ev.device_id];
        uint64_t cur = w.load(std::memory_order_relaxed);
        w.store(std::min(cur + kMtu, kWindowCap),
                std::memory_order_relaxed);
    }

    void onCongestionSignal(const CongestionSignal& sig) override {
        // MD: 乘性减窗
        auto& w = window_bytes_[sig.device_id];
        uint64_t cur = w.load(std::memory_order_relaxed);
        uint64_t reduced = static_cast<uint64_t>(cur * (1.0 - 0.5 * sig.severity));
        w.store(std::max(reduced, kWindowFloor), std::memory_order_relaxed);
    }

    uint64_t getDeviceRateLimit(int device_id) const override {
        // 由窗口推导粗略速率上限，简化实现返回 0（不限速）
        (void)device_id;
        return 0;
    }

    const char* getName() const override { return "aimd"; }

   private:
    static constexpr uint64_t kMtu = 4 * 1024;
    static constexpr uint64_t kWindowCap = 256 * 1024 * 1024;
    static constexpr uint64_t kWindowFloor = 64 * 1024;
    std::array<std::atomic<uint64_t>, kMaxDevices> window_bytes_{};
};

}  // namespace mooncake::tent
```

实现要点：
- **无锁优先**：状态用 `std::atomic`，避免在热路径上加锁；
- **per-device 隔离**：以 `device_id` 索引，避免一台拥塞设备影响全局；
- **失败兜底**：当依赖的状态尚未初始化时，应返回 `{true, 0, 0}`，绝不可让插件成为新的故障源。

## 未来规划

1. **具体算法实现**：基于本接口提供 AIMD、BBR-like、拓扑感知（rack/spine 维度）等内置实现，按场景选择。
2. **per-destination 窗口**：当前粒度为 per-device；规划扩展到 (device, target_segment) 二元组，以解决 M2N 场景下单 N 端聚合点的 ASW Buffer 问题。
3. **ECN 硬件信号集成**：在支持 ECN 的 RDMA 网卡上读取 CQE 标志位，将 `ECN_MARK` 信号接通，实现真正的网内反馈控制环路。
4. **TentMetrics 指标导出**：将插件状态（窗口、推迟次数、拥塞事件计数）作为标准指标暴露至 Prometheus，支持运维侧观测与调参。
5. **JSON 配置模式**：为内置插件定义统一的配置 schema，可通过 Engine 的 JSON 配置直接加载并参数化，而无需用户编写 C++ 代码。示例草案：

   ```json
   {
     "runtime": {
       "congestion_control": {
         "plugin": "aimd",
         "params": {
           "initial_window_bytes": 1048576,
           "window_cap_bytes": 268435456,
           "md_factor": 0.5,
           "ai_increment_bytes": 4096,
           "defer_us_on_throttle": 50.0
         }
       }
     }
   }
   ```

6. **跨进程协同**：与 QoS 的全局时间片机制类似，通过共享内存让多进程的拥塞状态共享，从而在共置部署中也能形成统一的反压视图。

## 相关文档

- [TENT 概览](overview.md)
- [TENT QoS 设计](qos.md)
- [Slice Spraying](slice-spraying.md)
- [Transport Selector](transport-selector.md)
- [TENT Metrics](metrics.md)
- [TENT C++ API](cpp-api.md)
