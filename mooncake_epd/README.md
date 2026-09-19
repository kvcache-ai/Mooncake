# Mooncake EPD Disaggregation Framework

## 赛题四：Agent 与多模态推理场景下的 KVCache 分离与协同调度

基于 [Mooncake](https://github.com/kvcache-ai/Mooncake) 的 EPD 三阶段分离与 Agent 状态协同调度框架，面向下一代 AI 多模态推理工作负载。

## 真实验证现状

本仓库当前已经完成一轮**真模型 / 真 GPU / 真 Mooncake / 真 vLLM** 的落地验证，核心结论：

- `Qwen3-VL-8B-Instruct` 已完成 EPD 真机测试
- Mooncake store-backed 远程传输测试已通过
- vLLM `MooncakeConnector` 分离式 prefill/decode 已真实启动成功
- proxy 的 `POST /v1/chat/completions` 已真实返回 200
- `scripts/vllm_disagg_proxy.py` 已真实修复 proxy 首跳缺失 `transfer_id` 的告警
- GPU3-6 soak 已完成，结果见 `artifacts/real_soak_report.json`
- 本地 `KV Directory / owner-shard` 接入后已完成二次真机回归，结果见 `artifacts/real_soak_report_post_kvdir.json`

推荐先看：

- `coding_plan/真实测试经验.md`
- `artifacts/phase6_metrics.json`
- `artifacts/real_soak_report.json`
- `artifacts/real_soak_report_post_kvdir.json`
- `artifacts/rfc_eval_report.md`

## 项目概述

本框架将多模态大模型推理拆分为三个独立阶段（EPD），通过 Mooncake Transfer Engine 实现跨节点高性能数据传输：

```
Image → [Encoder GPU] → Hidden States → [Transfer E→P] →
        [Prefill GPU] → KV Cache → [Transfer P→D] →
        [Decode GPU] → Text
```

同时实现了 Agent 状态克隆、PD 调度策略和 Hidden State 前缀缓存等进阶功能。

## 环境要求

| 项目 | 要求 |
|------|------|
| GPU | RTX A6000 48GB x 8 (或同等) |
| CPU | 64 cores |
| RAM | 503 GB |
| CUDA | 12.9 |
| Python | 3.10+ |
| Mooncake | 0.3.11+ |
| vLLM | latest (V1 backend) |

### 硬件说明

当前环境为 RTX A6000 工作站，已探测到 ACTIVE 的 Intel `irdma0`
RNIC（`eno2=192.168.100.1`）。它的 verbs transport 是 **iWARP**，不能
使用 Mooncake legacy IB/RoCE 手工 QP 路径。仓库现已增加两级适配：

1. `core/transfer/rdma.py` 区分 IB、RoCE、iWARP、`rdma_cm` 与
   GPUDirect，不再用“存在 verbs 设备”误判 GPUDirect。
2. `core/transfer/rdmacm.py` 提供 `rdma_cm/rsocket` 硬件数据面；对
   Intel X722 使用 GPU→Host staging→iWARP→Host staging→GPU，并对远端
   CUDA 指针做注册范围校验。

本机真实 `rbind/rlisten` 已通过，artifact：
`artifacts/rdmacm_iwarp_diagnostics_20260723_173449.json`。诊断结论为
本地 endpoint `ready=true`、`data_plane_validated=false`、
`gpudirect_ready=false`。由于 iWARP 连接必须到另一台
RDMA 主机，单机 EPD 进程不能把流量绕回本机 RNIC；单机自动选择
CUDA P2P/SHM，跨机配置远端 RDMA IPv4 后选择 `rdmacm_staged`。Mooncake
继续作为 TCP 控制面与内存区域元数据面，RDMA 仍不作为 correctness
依赖。

本机 RDMA 诊断 artifacts：

- `artifacts/ibv_devinfo_after_install_20260709.txt`
- `artifacts/ibv_devices_after_install_20260709.txt`
- `artifacts/mooncake_rdma_diagnostics_after_ibverbs_20260709.json`

复测入口：

```bash
PYTHONPATH=/home/songbinbin/Proj/Proj_LWX \
WITH_NVIDIA_PEERMEM=1 \
MC_RDMA_BIND_ADDRESS=192.168.100.1 \
MC_GID_INDEX=0 \
MC_MTU=1024 \
MC_NUM_QP_PER_EP=1 \
MC_MAX_INLINE=0 \
MC_MAX_SGE=1 \
MC_MAX_WR=16 \
python scripts/diagnose_mooncake_rdma.py \
  --device-name irdma0 \
  --local-hostname 192.168.100.1 \
  --cuda-device cuda:0
```

生成跨机 iWARP EPD 配置：

```bash
PYTHONPATH=/data/songbinbin/Proj/Proj_LWX \
/data/songbinbin/Proj/Proj_LWX/venv_mooncake/bin/python \
  demo/vllm_integration.py \
  --output-dir config-rdmacm \
  --protocol rdmacm \
  --rdmacm-bind-address 192.168.100.1 \
  --rdmacm-remote-address <decode节点RDMA_IP>
```

生成结果中 Mooncake protocol 仍为 `tcp`，KV 数据面 backend 为
`rdmacm_staged`；这是 Intel iWARP 的预期组合，不是降级伪装。

## 快速开始

### 1. 环境安装

```bash
# 创建虚拟环境
python3.10 -m venv venv_mooncake
source venv_mooncake/bin/activate

# 安装 Mooncake
pip install mooncake-transfer-engine

# 安装 vLLM
pip install vllm

# 安装其他依赖
pip install torch numpy Pillow pyyaml requests aiohttp
```

### 2. 运行 EPD Demo

```bash
cd mooncake_epd
python demo/run_qwenvl_epd.py
```

### 3. 运行性能基准测试

```bash
python benchmarks/benchmark.py
```

### 4. 启动 Mooncake 基础服务

```bash
bash scripts/start_mooncake.sh
```

### 5. 启动 vLLM EPD 分离推理

```bash
bash scripts/start_vllm_disagg.sh
```

## 项目结构

```
mooncake_epd/
├── __init__.py
├── config/
│   ├── config.yaml              # 总配置文件
│   └── mooncake.json            # Mooncake Transfer Engine 配置
├── core/
│   ├── __init__.py
│   ├── transfer_engine.py       # Mooncake Transfer Engine 封装
│   ├── encoder_worker.py        # Vision Encoder Worker
│   ├── prefill_worker.py        # Prefill Worker
│   ├── decode_worker.py         # Decode Worker
│   └── epd_pipeline.py          # EPD 流水线编排
├── agent/
│   ├── __init__.py
│   ├── state_clone.py           # Agent KVCache 零拷贝克隆
│   ├── scheduler.py             # Agent PD 调度策略
│   └── prefix_cache.py          # Hidden State 前缀缓存
├── demo/
│   ├── run_qwenvl_epd.py        # Qwen3-VL EPD 端到端 Demo
│   └── vllm_integration.py      # vLLM 集成配置生成
├── benchmarks/
│   └── benchmark.py             # 性能基准测试
├── scripts/
│   ├── start_mooncake.sh        # 启动 Mooncake 服务
│   ├── start_vllm_disagg.sh     # 启动 vLLM EPD 分离
│   └── setup_mooncake.py        # Mooncake 环境管理
└── requirements.txt
```

## 基础任务实现

### 1. EPD 三阶段分离原型

**文件**: `core/encoder_worker.py`, `core/prefill_worker.py`, `core/decode_worker.py`, `core/epd_pipeline.py`

**实现要点**:
- Vision Encoder (ViT) 在独立 GPU 上运行，输出 Hidden States
- Prefill Worker 接收视觉特征 + 文本 token，生成 KV Cache
- Decode Worker 接收 KV Cache，执行自回归解码
- 通过 Mooncake Transfer Engine 实现 E→P (Hidden States) 和 P→D (KV Cache) 传输
- 支持 TCP 和 RDMA 协议

### 2. Agent State Cloning

**文件**: `agent/state_clone.py`

**实现要点**:
- 零拷贝克隆：通过引用计数共享 KV Cache 物理内存
- 写时复制 (CoW)：仅在修改时才分配新内存
- 生命周期管理：引用计数为 0 时自动回收
- 支持 Tree-of-Thought 剪枝（保留 top-k 分支）

**性能数据**:
- 2 分支克隆: 0.098 ms (0.049 ms/branch)
- 4 分支克隆: 0.142 ms (0.036 ms/branch)
- 8 分支克隆: 0.248 ms (0.031 ms/branch)
- 16 分支克隆: 0.471 ms (0.029 ms/branch)

### 3. Qwen-VL 端到端 Demo

**文件**: `demo/run_qwenvl_epd.py`

四个 Demo 模块：
1. **Basic EPD**: 多模态输入经过 E→P→D 三阶段处理
2. **Agent Cloning**: Tree-of-Thought 思考分支 fork 与剪枝
3. **Prefix Caching**: 图像编码结果缓存，避免重复计算
4. **PD Scheduling**: 思考型/交互型 Agent 动态路由

## 进阶任务实现

### 1. Agent PD Disaggregation 调度策略

**文件**: `agent/scheduler.py`

- 思考型 Agent → 高算力 Prefill Worker（选择 GPU utilization 最低的）
- 交互型 Agent → 低延迟 Decode Worker（选择 avg_latency 最低的）
- 支持优先级调度和动态负载均衡

### 2. Hidden State Prefix Caching

**文件**: `agent/prefix_cache.py`

- 基于 SHA-256 图像 hash 的精确匹配
- LRU 淘汰策略
- 可配置缓存大小 (默认 4GB) 和 TTL (默认 1 小时)
- 相同图像命中率 100%

### 3. vLLM MooncakeConnector 集成

**文件**: `demo/vllm_integration.py`, `scripts/start_vllm_disagg.sh`

- 使用 vLLM V1 后端的 `MooncakeConnector`
- Prefill (kv_producer) 和 Decode (kv_consumer) 分离部署
- Proxy Server 路由请求

## 性能数据

### Benchmark 结果 (Mock 模型, A6000, TCP)

| 指标 | 数值 |
|------|------|
| **EPD Pipeline** | |
| Avg Latency | 167.31 ms |
| P50 Latency | 160.26 ms |
| P99 Latency | 207.98 ms |
| Avg TTFT | 2.54 ms |
| Throughput | 204.59 tokens/s |
| **Transfer Bandwidth (Local CUDA)** | |
| 4KB Tensor | 1.284 Gbps |
| 40KB Tensor | 13.417 Gbps |
| 400KB Tensor | 120.452 Gbps |
| 4MB Tensor | 1240.088 Gbps |
| **Agent Cloning** | |
| 2 branches | 0.049 ms/branch |
| 16 branches | 0.029 ms/branch |
| **Prefix Caching** | |
| Cache Hit Rate | 100% |

> 注：以上数据基于 Mock 模型的演示性测试。实际模型（如 Qwen3-VL-8B）的数据会有所不同。

## vLLM 集成指南

### 使用 MooncakeConnector 实现 PD 分离

1. **配置 mooncake.json**:
```json
{
  "prefill_url": "127.0.0.1:9100",
  "decode_url": "127.0.0.1:9200",
  "metadata_server": "http://127.0.0.1:8080/metadata",
  "protocol": "tcp",
  "device_name": ""
}
```

2. **启动 Prefill**:
```bash
MOONCAKE_CONFIG_PATH=mooncake.json \
vllm serve Qwen/Qwen2.5-VL-7B-Instruct \
  --port 8100 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
```

3. **启动 Decode**:
```bash
MOONCAKE_CONFIG_PATH=mooncake.json \
vllm serve Qwen/Qwen2.5-VL-7B-Instruct \
  --port 8200 \
  --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'
```

4. **启动 Proxy**:
```bash
python mooncake/vllm_v1_proxy_server.py \
  --prefiller-host 127.0.0.1 --prefiller-port 8100 \
  --decoder-host 127.0.0.1 --decoder-port 8200 \
  --port 8000
```

## 已知限制

1. **RDMA 硬件口径**: 当前工作站有 Intel `irdma0` iWARP RNIC，但 Mooncake RDMA direct path 的 IB/RoCE RC QP smoke 未通过；真实可用路径是 TCP direct 与 SHM。若需要真 RDMA/GPU Direct，请使用 Mellanox/兼容 RoCEv2 或 IB HCA，或先让 `scripts/diagnose_mooncake_rdma.py` 返回 `ready=true`。
2. **Mock 模型**: Demo 使用模拟模型验证架构，实际 Qwen3-VL 需要 vLLM 集成
3. **跨节点**: 当前仅验证单节点多 GPU 场景，跨节点需要网络配置
4. **MooncakeConnector Proxy**: 当前使用 vLLM 自带的 toy_proxy_server，生产环境需要更健壮的方案

## 模型依赖

| 模型 | 用途 | VRAM 需求 |
|------|------|-----------|
| Qwen2.5-VL-7B-Instruct | 多模态推理 | ~16 GB (FP16) |
| Qwen2.5-VL-32B-Instruct | 高性能推理 | ~64 GB (FP16) |
| Qwen3-VL-8B (预期) | 最新模型 | ~18 GB (FP16) |

## 框架版本

| 组件 | 版本 |
|------|------|
| Mooncake | 0.3.11.post1 |
| vLLM | latest (V1) |
| PyTorch | 2.x |
| CUDA | 12.9 |
| Python | 3.10 |

## 部署拓扑

### 单机三 GPU EPD 分离
```
GPU 0: Vision Encoder (E)
GPU 1: LLM Prefill (P)
GPU 2: LLM Decode (D)
```

### 多机 EPD 分离
```
Node 1 (GPU 0,1): Encoder + Prefill
Node 2 (GPU 2,3): Decode
Transfer: Mooncake (TCP/RDMA)
```

### 生产环境建议
```
Node 1-2: Vision Encoder Pool
Node 3-6: Prefill Pool (high compute)
Node 7-8: Decode Pool (low latency)
Load Balancer: Agent PD Scheduler
```

## Omni Pipeline AR → Generation → Diffusion SHM 验证状态（2026-07-09）

`scripts/run_omni_stage_transfer_e2e.py` 现在提供三种可复现路径：

- `--stage-impl tensor --runtime thread`：真实 Qwen2.5-Omni Thinker image hidden-state + 轻量 tensor Generation/Diffusion，用于快速验证 CUDA stage 间 SHM/同机搬运。
- `--stage-impl semantic --runtime thread`：完整语义级 Qwen2.5-Omni 拆分，`Thinker/AR -> Talker speech-code Generation -> Token2Wav diffusion/vocoder`，模型按 `thinker/talker/token2wav` 分布到三张 GPU。
- `--stage-impl dataset_tensor --runtime process`：真实数据集图片派生 CPU tensor，三段分别运行在独立 OS process，通过 POSIX SHM 验证跨进程 stage 搬运与聚合统计。

已验证 artifacts：

```text
artifacts/qwen25_omni_stage_transfer_dataset_e2e_shm_semantic_20260709.json
artifacts/qwen25_omni_stage_transfer_dataset_e2e_shm_process_dataset_tensor_20260709.json
```

语义级真实模型命令：

```bash
source /home/songbinbin/Proj/Proj_LWX/venv_mooncake/bin/activate
cd /home/songbinbin/Proj/Proj_LWX/mooncake_epd
PYTHONPATH=/home/songbinbin/Proj/Proj_LWX python scripts/run_omni_stage_transfer_e2e.py \
  --model /home/songbinbin/Qwen2.5-Omni-7B \
  --stage-devices cuda:0 cuda:1 cuda:2 \
  --transport-backend shm \
  --protocol local \
  --dtype bf16 \
  --runtime thread \
  --stage-impl semantic \
  --dataset-root /home/songbinbin/Proj/Proj_LWX/mooncake_test_dataset \
  --dataset-jsonl chat_splits/dev-small.jsonl \
  --limit 1 \
  --thinker-max-new-tokens 8 \
  --talker-max-new-tokens 24 \
  --token2wav-num-steps 1 \
  --output artifacts/qwen25_omni_stage_transfer_dataset_e2e_shm_semantic_20260709.json
```

本机结果：`status=ok`，`AR->Generation` 与 `Generation->Diffusion` 均为 `backend_counts.shm`、`fallback_count=0`；语义质量断言包含 non-empty text、talker codes、non-zero waveform。

进程隔离 SHM 命令：

```bash
PYTHONPATH=/home/songbinbin/Proj/Proj_LWX python scripts/run_omni_stage_transfer_e2e.py \
  --runtime process \
  --process-start-method fork \
  --stage-impl dataset_tensor \
  --transport-backend shm \
  --protocol local \
  --dataset-root /home/songbinbin/Proj/Proj_LWX/mooncake_test_dataset \
  --dataset-jsonl chat_splits/dev-small.jsonl \
  --limit 3 \
  --output artifacts/qwen25_omni_stage_transfer_dataset_e2e_shm_process_dataset_tensor_20260709.json
```

进程模式使用 CPU SHM 作为可靠验证面。不要用 fork 继承已加载 CUDA 大模型做生产承诺；完整语义级 CUDA 模型路径当前使用单进程多 worker-thread + sharded modules，避免 CUDA fork 风险。
