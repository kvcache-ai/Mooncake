# End-to-End SGLang + HiCache + Mooncake Store Benchmark 部署指南

## 目标

在 A40 GPU 服务器上验证所有 Mooncake Store 优化在真实 LLM 推理负载下的效果。

## 硬件要求

- **推荐**: A40_8_node3 (6x A40 48GB, Port 20002)
- **最低**: 1x A40 + 32GB DRAM
- **网络**: TCP (单节点测试无需 RDMA)

## 步骤

### Step 1: 同步代码到 GPU 服务器

```bash
# 在本地执行
rsync -avz --exclude='.git' --exclude='build' --exclude='__pycache__' \
  -e "ssh -i /Users/yanchaomei/Downloads/ssh/id_rsa -p 20002" \
  ./ user@114.214.255.110:/home/user/chaomei/Mooncake_competition/
```

### Step 2: 在 GPU 服务器上构建 Mooncake

```bash
ssh -p 20002 user@114.214.255.110

cd /home/user/chaomei/Mooncake_competition

# 安装依赖
pip install sglang[all] --quiet

# 构建 Mooncake
git submodule update --init --recursive
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_CUDA=ON -DUSE_HTTP=ON
make -j$(nproc)

# 安装 Python wheel
cd .. && pip install -e mooncake-wheel/
```

### Step 3: 下载测试模型

```bash
# 小模型（快速验证，~3GB）
pip install huggingface_hub
huggingface-cli download Qwen/Qwen2.5-1.5B-Instruct \
  --local-dir /home/user/models/Qwen2.5-1.5B-Instruct

# 或者中等模型（完整验证，~15GB）
huggingface-cli download Qwen/Qwen2.5-7B-Instruct \
  --local-dir /home/user/models/Qwen2.5-7B-Instruct
```

### Step 4: 运行 Benchmark

```bash
cd /home/user/chaomei/Mooncake_competition

# 使用 tmux 防断连
tmux new -s bench

# 运行端到端 benchmark
./benchmarks/e2e_hicache_benchmark.sh \
  /home/user/models/Qwen2.5-7B-Instruct \
  benchmarks/e2e_results_$(date +%Y%m%d)
```

### Step 5: 回收结果到本地

```bash
# 在本地执行
rsync -avz -e "ssh -i /Users/yanchaomei/Downloads/ssh/id_rsa -p 20002" \
  user@114.214.255.110:/home/user/chaomei/Mooncake_competition/benchmarks/e2e_results_*/ \
  ./benchmarks/
```

## 预期输出

```
benchmarks/e2e_results_20260323/
├── REPORT.md                    # 综合报告
├── bench_multiturn.json         # 多轮对话 benchmark
├── bench_prefix_sharing.json    # 前缀共享 benchmark
├── bench_cache_miss.json        # Cache miss benchmark
├── mooncake_master.log          # Master 服务日志
├── sglang_server.log            # SGLang 服务日志
├── mooncake_metrics.txt         # Prometheus 指标
└── mooncake_metrics_summary.json # 指标摘要
```

## 对比方案

运行两次 benchmark：
1. **Baseline**: 使用未修改的 Mooncake Store（git checkout main）
2. **Optimized**: 使用优化后的版本（git checkout feat/optimize-mooncake-store）

对比关键指标：
- 多轮对话 avg_latency 和 throughput
- 前缀共享场景下的 cache hit rate
- Cache miss 场景下的 latency（验证 Bloom Filter 效果）
