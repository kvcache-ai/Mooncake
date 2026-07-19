# EPD 真实 Benchmark 协议

该协议把 EPD 的“请求成功”与“性能结论”分开。所有正式结果都必须由
`run_epd_benchmark_campaign.py` 汇总；单个 JSON 摘要、loopback、mock、或仅声明
protocol 的日志不能作为赛题性能证据。

## 不可妥协的证据要求

每个真实 serving 场景必须保存：

1. 原始请求和响应 JSONL；
2. Prefill、Decode、Encoder（如有）、proxy、metadata/master 日志清单；
3. metrics 快照、模型/runner 配置、GPU 拓扑与已脱敏环境；
4. 实际 native transport 证据：TCP、RDMA、NVLink、CUDA IPC 或 POSIX SHM；
5. `raw_artifacts/hardware.json`：采集时的 GPU UUID/型号/显存、驱动、拓扑与
   NVLink 状态；缺失硬件清单的 serving 场景默认不通过 campaign；
6. 无 fallback、无 transfer failure、正确 routing 的可失败 gate；
7. 结果 summary 的 SHA-256、命令和工作目录。

RDMA 场景还必须包含可验证的 NIC、GID 和远端主机信息。没有第二节点时应报告
`blocked`，绝不能把本机 loopback 标成双节点。

## 必测矩阵

模板：`epd_benchmark_campaign.template.json`。每个 `r1/r2/r3` 是独立服务重启后的
重复，而不是同一进程内的三次请求。

| 层级 | 场景 | 目的 | 关键门禁 |
| --- | --- | --- | --- |
| S0 | 硬件/环境清单 | 固化模型、vLLM、Mooncake、GPU 拓扑、驱动和 transport capability | 无秘密；NVLink/RDMA capability 与实际日志一致 |
| S1 | 单机 Qwen3-VL | short/medium/long context，C1/C4/C8 | 作为正确性和容量参考，不能与更多 GPU 的 EPD 直接作效率宣称 |
| S2 | P→D TCP | 相同 P/D GPU、prompt、采样、batch、worker、affinity | `PD` routing、direct backend、TCP 实际日志、零 fallback |
| S3 | P→D NVLink/CUDA IPC | 与 S2 仅改变真实 transport | capability fail-closed；两端 worker 的实际 native marker |
| S4 | 真实多模态 E→P→D | 冷缓存与 exact cache 热路径 | `EPD` routing、FeatureHandle consumed、Prefill vision skip、P→D direct |
| S4b | 全链路容量 | 1P1D 与 2P2D 的 C4/C8 full EPD | 每个 worker 有 dispatch，资源差异只可报告 capacity，不能称同资源效率；同图并发必须披露 singleflight/cache coalescing |
| S5 | Agent State Clone | 4/8/16 branches、16/64/256 pages、CoW write | branch clone 零 KV payload、refcount、CoW isolation、release 后无 orphan |
| S6 | Hidden-state cache | exact hit、asset/model/processor mismatch、非法 partial-prefix 负例 | 命中只在 exact key；负例不得复用 |
| S7 | Agent PD scheduler | round_robin、least_loaded、static_type_route、agent_aware | 相同 trace 下 deterministic route、SLO/queue/TTFT/TPOT 分解 |
| S8 | 故障与 soak | worker restart、stale manifest、transfer timeout、release/GC、10k request soak | fail-closed、无泄漏、无 silent fallback |
| S9 | Omni pipeline | AR→Generation→Diffusion 的真实进程/节点 stage edge | transport 名称与 payload 实际路径一致，不能把 `.to()` 叫 SHM |
| S10 | 双节点 TCP/RDMA | 真实远端 host、P→D 和 Agent materialize | TCP 兼容性或 RDMA native evidence；不可用时明确 blocked |

## 性能比较规则

### 同资源 transport A/B

TCP 与 NVLink 的比较必须保持以下值相同：模型与 revision、请求 fingerprint、温度、
max tokens、warmup、concurrency、max model length、batch limits、P/D GPU 数、GPU
memory utilization、layer grouping、transfer worker 数和 affinity。唯一允许变化是
实际 transport。

对 `nvlink_intra` 场景，还必须在 scenario 中声明实际 P→D GPU 对
`required_nvlink_pairs`，并由同一 artifact 的 `nvidia-smi topo -m` 快照验证该边为
`NV*`。仅有 CUDA P2P、PIX/PHB/SYS 或一个请求协议字符串均不足以声称 NVLink。

每侧至少 3 次独立重启运行。强性能结论还要求：

- 资源拓扑完全相同；
- 完全一致的输出（或单独通过的模型语义 oracle）；
- 95% bootstrap 区间下界仍大于 0；
- 原始证据、实际 backend 和 no-fallback gate 全通过。

否则报告为“诊断/容量观察”，而不是“证明的性能提升”。GPU 数不同的 EPD 与单机
比较只能报告 scale-out capacity，禁止表述为同资源效率优势。

### 调优 A/B 的配置差分

`tuning_ab` 不接受隐式的“配置不同也可以比较”。比较必须把唯一允许变化的字段写进
`allowed_config_differences`；当前只允许经过审核的内部旋钮（包括
`epd.scheduler_policy`、layer-group/transfer-worker 参数和 vLLM batch 上限）。模型、
请求 fingerprint、采样、缓存状态和 GPU 拓扑仍必须相同。报告会保留这份差分；未声明
或未支持的差分会 fail-closed，不能借此掩盖工作负载或资源变化。

缓存 A/B 使用 `cache_ab`：它必须同时门禁缓存配置和实际 hit/miss 计数，但即使
TTFT 降低也只报告为 cache-specific diagnostic；它不证明冷流量或不同前缀上的效率。

### Direct FeatureBuffer 缓存的 lease 规则

默认 `--release-direct-feature-buffers-after-prefill` 会在 Prefill 消费完成后归还
每请求的 direct-buffer lease。此模式下 proxy 不能安全复用旧 `FeatureHandle`，因此
runner 会把请求的 `direct_proxy_handle_cache` 记录为 `requested`，并把实际启用状态
记录为 `effective=false`；不会继续写入一个必然无法命中的 proxy cache。

默认热路径是 Prefill 端的 `direct_persistent_cache`：每次请求通过 Prefill lookup
重新获取有效 lease/descriptor，随后仍由 Mooncake 直接读取已注册的 peer buffer。只有
显式 `--no-release-direct-feature-buffers-after-prefill` 时，proxy handle cache 才能
成为有效实验变量；该模式必须额外证明 release/GC 后无悬挂引用，不能混入默认低泄漏
TTFT 基准。

### 指标

至少报告平均/P50/P95/P99 TTFT、TPOT、request RPS、output token goodput、成功率、
finish reason、token 数分布、GPU 数/显存、KV cache 使用、P→D bytes/batches、写入
时延/带宽、Decode first-token 时延和 proxy 分段时延。

## 执行

先复制模板到 artifact workdir，并把每个 `blocked_reason` 替换为真实 `summary` 或
受控 `command`：

```bash
PYTHONPATH=. "$VENV/bin/python" mooncake_epd/scripts/run_epd_benchmark_campaign.py \
  --manifest /abs/path/campaign.json \
  --output /abs/path/campaign-report.json \
  --execute-commands
```

仅审计已有 artifact 时不要传 `--execute-commands`。完成门禁：

```bash
PYTHONPATH=. "$VENV/bin/python" mooncake_epd/scripts/run_epd_benchmark_campaign.py \
  --manifest /abs/path/campaign.json \
  --output /abs/path/campaign-report.json \
  --enforce-complete
```

报告中的 `bottleneck_rankings` 是诊断指标，明确标记为非可加和；它用于定位下一轮
优化（例如 Decode first-token、P→D receive、peer-buffer write、proxy handoff），
而不能代替端到端测量。

对多 worker 容量场景，模板还要求 `require_all_stage_workers_dispatched=true` 与
`min_dispatch_entropy=0.9`。其中 entropy 在 0 到 1 之间，1 表示均衡分发；计数会包含
零请求 worker。未实际使用所有配置的 Prefill/Decode 副本，或分发明显偏斜时，该场景
不能作为 scale-out capacity 证据。

调度消融运行 `run_agent_pd_scheduler_eval.py` 时，固定同一 `tasks.jsonl` 和 seed，分别
传入 `--scheduler-policy round_robin|least_loaded|static_type_route|agent_aware`。非
`agent_aware` 臂必须显式传 `--min-route-correct-rate 0`，因为较低路由正确率正是消融
要观察的结果；这类比较是 control-plane diagnostic，不得表述为真实 GPU 吞吐提升。
真实多 worker serving 消融还必须把相同 `--scheduler-policy` 传给
`run_vllm_online_direct_e2e.py`，并记录每个 Prefill/Decode worker 的 dispatch；策略差异
只能通过上述 `tuning_ab` 白名单字段进行 A/B 比较。
