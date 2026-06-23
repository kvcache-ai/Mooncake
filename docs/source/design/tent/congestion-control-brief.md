# TENT 拥塞控制插件 — 设计简述

## 要解决的问题

| 编号 | 风险 | 根因 | 状态 |
|------|------|------|------|
| 3.3 | M2N 下 ASW Buffer 耗尽 | 多发送端同时聚合到少数接收端，瞬时打满交换机缓冲 | **待解决** |
| 3.4 | HPN 7.0 上行链路拥塞 | 跨机架带宽过订购，leaf→spine 上行成瓶颈 | **待解决** |

**核心缺口**：Transfer Engine 当前没有应用层的速率控制和拥塞反馈机制——发送端不知道下游是否拥塞。

## 方案：可插拔的拥塞控制插件

在 `TransferEngineImpl` 的提交路径中插入一个插件扩展点，形成 **"感知—决策—执行"闭环**：

```
                 ┌──────────────┐
                 │  Application │
                 └──────┬───────┘
                        │ submitTransfer
                        ▼
              ┌─────────────────────┐
              │  ① Admission Hook   │◄──────────────┐
              │  plugin->onAdmit()  │               │
              └─────────┬───────────┘               │
                        ▼                           │
              Transport Selector                    │
                        ▼                           │ 反馈
              Device Selector                       │
                        ▼                           │
              ┌─────────────────────┐               │
              │  Transport (RDMA…)  │               │
              └─────────┬───────────┘               │
                        ▼                           │
              ┌─────────────────────┐               │
              │  ② onCompletion     │───────────────┤
              │  ③ onCongestionSignal│──────────────┘
              └─────────────────────┘
```

- **① 准入**：每个请求提交前询问插件，插件可放行、推迟、或收窄设备选择范围
- **② 完成反馈**：传输完成后将延迟/吞吐数据回传插件，更新内部状态
- **③ 拥塞信号**：超时、QP 错误等异常事件通知插件，触发降速

## 插件接口（5 个方法）

```cpp
class CongestionControlPlugin {
    // 准入决策：放行 or 推迟？可覆盖设备选择
    virtual AdmitDecision onAdmit(const AdmitContext& ctx) = 0;

    // 完成反馈：学习延迟与吞吐
    virtual void onCompletion(const CompletionEvent& event) = 0;

    // 拥塞信号：超时 / QP 错误 / ECN
    virtual void onCongestionSignal(const CongestionSignal& signal) = 0;

    // 设备级速率上限（0 = 不限速）
    virtual uint64_t getDeviceRateLimit(int device_id) const = 0;

    // 插件名称
    virtual const char* getName() const = 0;
};
```

## 设计要点

- **零开销默认**：未启用时使用 NoOp 插件，对现有行为无任何影响
- **热路径友好**：`onAdmit` 预算 < 100 ns，无锁、无分配、无系统调用
- **Transport 无关**：接口在 Runtime 层，RDMA / TCP / NVLink 通用
- **最小侵入**：仅 3 个插桩点，不修改任何 Transport 实现

## 如何解决 3.3 和 3.4

| 问题 | 插件策略 | 机制 |
|------|----------|------|
| 3.3 ASW Buffer 耗尽 | per-destination 窗口控制 | `onAdmit` 跟踪到每个目标的 inflight，超窗则推迟 |
| 3.4 上行链路拥塞 | 拓扑感知速率限制 | `device_mask_override` 避开拥塞上行口；`getDeviceRateLimit` 限制跨机架总速率 |

## 后续计划

1. 内置 AIMD / BBR 算法实现
2. per-destination 窗口（解决 M2N 聚合）
3. ECN 硬件信号接入
4. Prometheus 指标导出
5. JSON 配置化，无需写 C++ 即可启用

## 相关文档

- [完整设计文档](congestion-control.md)
- [TENT 概览](overview.md)
- [TENT QoS 设计](qos.md)
