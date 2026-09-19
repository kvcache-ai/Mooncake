# SGLang Encoder Disaggregation 与 Mooncake EPD 对比分析

> 分析日期：2026-09-01（America/Los_Angeles）  
> SGLang 上游目录：[`python/sglang/srt/disaggregation/encoder`](https://github.com/sgl-project/sglang/tree/main/python/sglang/srt/disaggregation/encoder)  
> 固定提交：[`403a15c163b16836da7ca2ca07308159b626c2f6`](https://github.com/sgl-project/sglang/commit/403a15c163b16836da7ca2ca07308159b626c2f6)  
> 本地快照：`artifacts/sglang_encoder_analysis_20260901/upstream/encoder`  
> 完整性证据：`commit.txt`、`commit_metadata.txt`、`files.txt`、`sha256sums.txt`

## 1. 结论

两套实现不是简单的“谁更先进”：

- **SGLang Encoder 热路径更成熟**：CPU 预处理边界清晰，支持跨请求 processor/ViT 合批、
  只计算 cache miss、ViT 前发布 shape、DP least-pending、功能性健康检查和预注册接收池。
- **Mooncake EPD 系统语义更完整**：保留 Qwen3-VL DeepStack 多 tensor FeatureBundle，具有
  model/processor fingerprint、checksum、deadline/admission、incarnation fencing、WAL、P→D
  分层传输、pre-rendered request 与严格 no-fallback 门禁。
- 最合理路线不是替换当前协议，而是把 SGLang 的 **Encoder 热路径工程能力嵌入当前严格语义**。
- 当前真实部署仍为 **TCP**。SGLang 快照只证明 Mooncake Transfer Engine 调用，不能仅根据
  `WaitingRDMARequest`、`MooncakeDelivery` 或注释宣称 RDMA；当前项目的 `peer_buffer_direct`
  backend 名称同样不等于 RDMA。

优先吸收顺序：

1. pending direct GPU bytes/entries 的硬上限和过载拒绝；
2. content fingerprint 先行、cache hit 跳过 PIL/HF processor，cache miss 再融合 CPU 预处理；
3. 预处理后、ViT 前发布精确预测 descriptor，使 Prefill allocate/register 与 ViT 并行；
4. 真实 queue/patch/GPU telemetry 和功能性健康；
5. 公平的 size-class/FIFO 预注册接收池；
6. 最后再考虑多 Encoder DP、分布式 embedding cache、视频/音频和 GPUDirect。

只读架构复核把候选按“收益 × 可验证性 × (6−风险)”进一步排序：有界 CPU
I/O/processor 隔离 100、ViT 前 descriptor 与分配重叠 60、patch/byte 感知准入 50、
zero-millisecond same-turn batching 40、EngineCore landing pool 24、L2/global cache 16；当前
单 Encoder 下 DP dispatcher 为 0。该分数是实施优先级推断，不是性能实测；所有收益仍需当前
GPU1/2/5/6 的真实 benchmark 证明。

## 2. 分析边界

本次执行了真实 `git clone --depth=1 --filter=blob:none --sparse`，分析的是固定 SHA 的七个文件，
不是网页摘要或模型记忆。该目录引用的 Mooncake engine、global cache backend、模型适配和 ZMQ
公共工具位于目录外，因此：

- 能确认调用边界、请求协议、生命周期和并发设计；
- 不能从本快照确认实际 NIC、RoCE/IB、GPUDirect、网络拓扑或性能百分比；
- 代码路径只能形成优化假设，任何 TTFT/吞吐收益必须由当前机器真实 Qwen3-VL/W0/GPU1/2/5/6
  benchmark 证明。

当前 `mooncake_epd` 没有可用 Git 元数据，因此对比以当前文件内容和 RFC 为权威，不虚构 SHA。

## 3. SGLang 逐文件机制

下文 `SG/` 表示 `artifacts/sglang_encoder_analysis_20260901/upstream/encoder/`。

### 3.1 `SG/preprocessor.py`

- `1-7`：明确 CPU-bound media I/O/HF processor 与 GPU MMEncoder 的边界。
- `91-152`：媒体 I/O 与 processor 使用独立 executor，并保留 GPU image preprocessing 入口。
- `287-397`：并发加载 image/video/audio，并校验调用方 content hash。
- `437-478`：把多个请求展平后一次调用 processor，返回 mm inputs、grid 和 token count。
- `494-617`：图像、视频、音频分别适配，避免把 image batching 语义直接套给 video/audio。
- `643-713,805-826`：按模型准确计算 patch/token 数，并处理一项扩展成多 grid 的情况。

**对当前项目的直接启示**：`scripts/epd_encoder_service.py:_encode_records` 仍顺序加载图像，Qwen3
路径逐图调用 processor，当前 microbatch 只融合 ViT，不融合前处理。

### 3.2 `SG/server.py`

- `106-216`：`EncoderMetaRegistry` 提供 shape/size rendezvous、等待、send-count 释放和 stale sweep。
- `252-266,616-710`：`ReqState` 用 encode/send 引用保护 embedding 生命周期。
- `319-377`：`EncoderDelivery` 分离 Mooncake/ZMQ transport 与模型执行。
- `440-608`：`MMEncoder` 统一持有模型、preprocessor、local/global cache 和 delivery backend。
- `828-854,1035-1348`：只把 global-cache miss 项送进 ViT，再与 hit 拼回原顺序。
- `1494-1522`：**processor 完成后、ViT forward 前发布 embedding shape/bytes**。
- `1524-1580`：Mooncake 路径 register/transfer/deregister；具体底层网络不在快照内。
- `1583-1653`：ZMQ embedding/control socket 明确走 TCP。
- `1686-1755`：多请求 batch 分片 clone，避免小请求长期 pin 整个 batch；发送前同步 CUDA stream。
- `1782-1846`：单请求也是统一 batch pipeline 的 batch-of-one。

**对当前项目的直接启示**：当前 `/describe` 必须等 ViT 完成后才返回 descriptor，Prefill allocation
和 E-stage compute 串行；但当前 FeatureBundle 是 main + DeepStack + grid 多 tensor，预测 descriptor
必须严格推导每层 shape，不能退化成 SGLang 的单 embedding shape。

### 3.3 `SG/runtime.py`

- `75-91`：IMAGE/AUDIO 可 batch，VIDEO 因 per-request kwargs 不合批。
- `101-159`：`EncoderScheduler` 有请求 timeout，但输入 `asyncio.Queue()` 无容量上限。
- `161-349`：同 event-loop turn 聚合请求，按 modality 分组并保持 collective dispatch 顺序。
- `376-445,532-585`：DP 使用 least-pending，平局用 round-robin。
- `833-875`：worker 退出后失败 pending 并摘除，但不自动 respawn。
- `876-1013`：结果监听有错误阈值和 stale request mapping sweep。
- `1075-1196`：统一 scheduler、encode、metadata publish、delivery pipeline。
- `1199-1208`：空闲时执行真实小图 ViT 的功能性 health probe。
- `1375-1475`：worker 接收前获取 semaphore，形成 dispatcher→worker 的实际背压。
- `1575-1652`：DP 模式为多 GPU 独立进程，限制 `dp>1,tp=1`。

**需要批判性吸收**：least-pending 值得参考，但无界 queue 与只按 pending request 数计费不适合
异构图像。当前应使用 `pending visual patches/tokens + queue age + GPU busy`，并保持 RFC 的有界队列。

### 3.4 `SG/http_server.py` 与 `SG/grpc_server.py`

- HTTP 文件 `1-9`：HTTP 只是 adapter，GPU/调度在 runtime/MMEncoder。
- HTTP `104-173`：异步注册 bootstrap，不阻塞主服务启动。
- HTTP `263-504`：`/encode`、`/send`、metadata、receive URL 构成显式控制协议。
- HTTP `507-594`：DP health 要求 rank 存活，并在空闲时做真实 encode。
- gRPC `44-72`：health 仅 serving flag，不是功能性 encode。
- gRPC `75-205`：只支持 image；receiver `2523-2581` 明确禁止 Mooncake+gRPC。

**不应照搬**：gRPC 路径能力更窄、每次 helper 建 channel，且未统一复用 HTTP DP runtime。

### 3.5 `SG/receiver.py`

- `73-342`：动态维护 Encoder URL；连续三次失败摘除，恢复后重新加入。
- `405-706`：part/grid/shape/dtype/aux metadata 聚合协议。
- `768-1164`：ZMQ/TCP frame 接收并支持 CPU→GPU pool staging。
- `1214-1450`：`WaitingRDMARequest` 调 Mooncake session/pointer；名字不能证明底层 RDMA。
- `1501-1704`：一次预注册的大 GPU pool，256-byte 对齐，提供 zero-copy views。
- `1582-1583`：上游自己注明 first-fit 可能导致大请求饥饿和 thundering herd。
- `1707-2206`：等待表、TP 同步、timeout、abort。
- `2274-2318`：多 Encoder 是随机打乱后按 item 数均分，不是实时负载均衡。
- `2429-2520`：HTTP 请求只发控制信息，embedding 走独立数据面。

**对当前项目的直接启示**：预注册池可减少 allocation/register 开销，但必须用 size class + FIFO
reservation，不能复制 first-fit/notify-all；多 Encoder 必须按 patch 成本路由，不能按图片数量随机均分。

## 4. 当前 Mooncake EPD 的差异化优势

### 4.1 RFC 和严格语义

- `RFC-EPD-2026-FINAL.md:80-114`：四层架构和 Encoder/Prefill/Decode DeepStack 边界。
- `549-623`：失败模型、幂等、rollback、孤儿资源回收、有界队列和 backpressure。
- `952-1041`：TTFT/goodput/cache/一致性指标。
- `12,51-56,1340-1342`：阶段一 TCP/SHM；RoCE/IB+GPUDirect 是后续阶段，不能提前宣称。

### 4.2 FeatureBundle 与缓存

- `core/state/feature_store.py:15-265`：descriptor 明确携带 main、DeepStack intermediates、grid、
  shape/dtype/nbytes/checksum 和 model/processor fingerprint。
- `346-520`：cache 有 byte/entry hard cap、refcount、lease、TTL。
- `618-775`：`reuse_density` 以 `frequency × recompute-cost / bytes` 做 admission/eviction。
- `core/state/vllm_mm_hidden_cache.py:350-603`：按 image item 命中，只拼接 miss patches 调 vision tower。

这些 correctness 与 value-aware 策略比 SGLang 单 embedding 协议更适合 Qwen3-VL DeepStack，不应删除。

### 4.3 Direct E→P 与 P→D

- `scripts/vllm_disagg_proxy.py:3669-3739`：`/describe → Prefill /allocate → /publish_direct`。
- `core/state/direct_feature_buffer.py:53-224`：目标包含 session/pointer/nbytes/incarnation，并校验 plan。
- `core/control/vllm_mooncake_connector.py:768-1285,2384-2834`：P→D layer-group、chunk、retry、
  backend label 和严格 fallback 语义。
- `core/transfer/engine.py:310-334`：TCP 配置会强制 `MC_FORCE_TCP=1`。

SGLang Encoder 快照没有当前 P→D 分层协议、WAL/incarnation fencing 或 pre-render capability，不能
用其 Encoder transport 替换整个系统。

### 4.4 当前明确短板

- `scripts/epd_encoder_service.py:_encode_records`：顺序媒体加载、逐图 processor。
- `/describe`：ViT 完成后才返回 descriptor，无法隐藏 Prefill allocation/register RTT。
- `pending_direct_bundles`：已有 TTL，但尚无 retained bytes/entries admission hard cap。
- `/health`：状态和 metric 丰富，但不执行真实 processor/ViT probe。
- `serving_controller.update_worker_load()`：接口存在，生产路径没有持续注入真实 GPU/queue telemetry。
- per-feature direct allocation：缺少通用、公平、预注册 GPU pool。

## 5. 对照矩阵

| 维度 | SGLang | Mooncake EPD | 结论 |
|---|---|---|---|
| Encoder runtime | preprocessor/scheduler/MMEncoder/delivery 清晰分层 | online 路径散布于 service/proxy/connector | 吸收 runtime 边界，不替换系统协议 |
| MM 协议 | part-based 单 embedding | DeepStack 多 tensor descriptor | 当前语义更强 |
| CPU preprocess | 并发 I/O + 一次 processor batch | 顺序 load + per-image processor | 当前 P0 性能差距 |
| ViT batch | processor/ViT 融合 | ViT microbatch 已有但默认关闭 | 先补 processor 和精确准入 |
| cache | local/global，miss-only compute | value-aware bundle + per-item partial hit + handle/render cache | 互补；当前 admission 更先进 |
| 多 Encoder | DP least-pending；receiver 随机均分 | Encoder 通常单 URL | 未来按 patch/token cost 路由 |
| E→P | ZMQ/TCP 或 Mooncake；预注册池 | strict peer-buffer direct；per-feature allocation | 保留 strict plan，引入公平池 |
| P→D | 非本目录重点 | layer-group/chunk/retry/fencing | 当前明显更完整 |
| 背压 | semaphore，但 scheduler queue 无界 | 多处有界；pending tickets 仅 TTL | 先补 retained GPU hard cap |
| 健康 | HTTP 功能性小图；动态摘除恢复 | incarnation 强，health 较浅 | 吸收 idle functional probe |
| 可观测性 | encoder metrics/profiler | stage/connector/cache/WAL metrics | 补 CUDA Event 和真实 worker telemetry |
| RDMA 证据 | 本快照不足 | 当前生产明确 TCP | 两边都不能凭名称宣称 RDMA |

## 6. 开发优先级与专业 benchmark

### P0-A：pending direct retained GPU hard cap

实施：

- 配置 `max_pending_tickets`、`max_pending_bytes`；
- 在 `/describe` 计算完成、持有 ticket 前做原子 admission；过载返回 503 + `Retry-After`；
- 拒绝路径先同步 producer，再释放 bundle；
- health 增加 high-watermark、admission rejections、expired/released bytes。

验证：

- 软件：并发 oversize/TTL/publish/discard 竞态，不得超过 hard cap；
- 真机：GPU5 真实 Encoder，故意让客户端只 describe 不 publish；验证 P99、503、显存和恢复；
- 全链路：严格 EPD C8/C16，正常路径不能产生 fallback 或明显 TTFT 回归。

### P0-B：content-first、decode/processor-on-miss 与融合 CPU 预处理

实施：

- 先读取受限 raw bytes、计算 SHA256；cache hit 不执行 PIL decode/HF processor；
- miss 才在线程池 decode；同一兼容 batch 的 misses 一次 processor；
- 输出必须保持 source identity、content identity、grid、DeepStack split 和顺序不变；
- data URL/local-root 安全边界保持，HTTP 继续 fail closed。

验证：

- balanced crossover：eager control vs deferred/fused treatment；
- real W0 单图 cache hit、混合 hit/miss、多图请求，C1/C4/C8；
- 报告 preprocess、ViT、describe、TTFT p50/p95、throughput、strict goodput；
- exact descriptor/checksum parity，pending/fallback/inflight 必须为 0。

### P0-C：预测 descriptor 与 allocate/ViT overlap

实施：

- processor 后从 grid、hidden size、DeepStack 层数推导预测 descriptor；
- Proxy 并行发 Prefill allocate 与 Encoder ViT；
- publish 前将真实 descriptor 与预测逐 tensor 比较；任何差异 fail closed 并 discard；
- ticket/target 纳入 deadline 和 incarnation fencing。

验证：

- 先做 Qwen3-VL 单图、多图、不同 resolution 的 shape proof；
- 控制与 treatment 使用相同预处理结果，至少 5 对交替 C1/C4/C8；
- 单独报告被隐藏的 allocation/register RTT 和总 TTFT；
- 注入错误预测、Prefill 重启、client cancel，必须无 stale pointer/泄漏/fallback。

### P1：公平预注册池、真实 telemetry、功能性 health

- pool：size class + FIFO reservation，避免 first-fit 大请求饥饿；
- telemetry：queue age、pending patches/tokens、batch occupancy、GPU busy、transfer latency；
- route：least predicted finish time，而不是 request count；
- health：只在空闲执行小图真实 forward，连续失败摘除，恢复后加入；正常副本继续服务。

### P2：DP/global cache/video-audio/GPUDirect

只有 P0/P1 真机门禁稳定后再做。每项都需独立 capability detection、生命周期和 benchmark；不得用
代码存在或设备名称替代真实传输证据。

## 7. 明确不照搬

1. 不照搬无界 scheduler queue/waiting list。
2. 不照搬 receiver 按图片数量随机均分。
3. 不把 `WaitingRDMARequest`、Mooncake、direct backend 名称当作 RDMA 证明。
4. 不把 DeepStack FeatureBundle 降级为单 embedding。
5. 不照搬 image-only、非功能性 health 的 gRPC 路径。
6. 不照搬 first-fit + notify-all GPU pool。
7. 不采用固定 60 s global-cache 等待；必须受请求剩余 deadline 约束。
8. 不把 ZMQ `MessageTracker` 当远端消费 ACK。
9. 不用 process-local dict 取代 workflow WAL/incarnation 权威状态。
10. 不把 RFC 的 SHM/RDMA/GPUDirect 目标写成当前已实现或已测收益。

## 8. 最终裁决

SGLang 证明了 Encoder EPD 可以形成“预处理—调度—miss-only ViT—提前 shape—delivery—receiver
pool—功能性 health”的闭环。当前 Mooncake EPD 不应重写，而应在保留 DeepStack、deadline、
incarnation、WAL、strict no-fallback 和 TCP 事实的前提下，依次补齐：

`bounded retained GPU admission → content-first/fused preprocess → predicted-descriptor overlap →
fair registered pool → telemetry-driven routing`。

所有候选收益目前均为代码路径推断；只有真实 Qwen3-VL、真实数据、GPU1/2/5/6、严格 EPD、
零 fallback/泄漏的 balanced benchmark 才允许晋级为生产默认或写入性能百分比。
