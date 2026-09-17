---
orphan: true
---

# 自适应拥塞控制：启动参数

本模块提供 Classic TE 和 TENT RDMA adapter 共用的环境变量解析与校验。后续 adapter 接入后，worker 会在构造时读取配置；运行中修改环境变量不会热更新。`MC_ADAPTIVE_CC_MODE=off` 或构建时关闭 `MOONCAKE_ENABLE_ADAPTIVE_CC` 可恢复原有路径。

| 参数 | 默认值 | 用途 |
| --- | ---: | --- |
| `MC_ADAPTIVE_CC_HIGH_PRESSURE_EPOCHS` | 2 | 连续高压力轮数达到后收缩窗口。 |
| `MC_ADAPTIVE_CC_LOW_PRESSURE_EPOCHS` | 3 | 连续低压力轮数达到后扩大窗口。 |
| `MC_ADAPTIVE_CC_HARD_ERROR_THRESHOLD` | 3 | 硬错误达到后隔离路径。 |
| `MC_ADAPTIVE_CC_COOLDOWN_MS` | 30000 | 隔离后等待的毫秒数。 |
| `MC_ADAPTIVE_CC_PROBE_WINDOW_BYTES` | 65536 | 恢复探测的字节窗口。 |

这五项只接受正十进制整数；轮数和错误阈值须在 32 位无符号整数范围内，冷却时间换算成纳秒后须在 64 位无符号整数范围内。显式设置的探测窗口不得超过 `MC_ADAPTIVE_CC_MIN_WINDOW_BYTES`。非法配置会让共享加载器返回无效结果和默认关闭的配置；adapter 接入后应记录初始化错误并保留原有传输行为。
