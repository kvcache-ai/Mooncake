# End-to-End SGLang + HiCache + Mooncake Store Benchmark 部署指南

## 目标

在 A40 GPU 服务器上验证所有 Mooncake Store 优化在真实 LLM 推理负载下的效果。

## 硬件要求

- **推荐**: A40 GPU server (8x A40 48GB, custom SSH port)
- **最低**: 1x A40 + 32GB DRAM
- **网络**: TCP (单节点测试无需 RDMA)

## 步骤

### Step 1: 同步代码到 GPU 服务器

```bash
# 在本地执行
rsync -avz --exclude='.git' --exclude='build' --exclude='__pycache__' \
  -e "ssh -i ${SSH_KEY} -p ${REMOTE_PORT}" \
  ./ ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/
```

### Step 2: 准备隔离 Python 环境

```bash
ssh -p "${REMOTE_PORT}" ${REMOTE_USER}@${REMOTE_HOST}

cd ${REMOTE_DIR}

# 推荐从已有 torch/vLLM 环境克隆，避免污染原环境。
${CONDA_ROOT}/bin/conda create \
  --clone ${BASE_CONDA_ENV} \
  -p /tmp/mooncake-e2e-bfcl \
  -y

source ${CONDA_ROOT}/etc/profile.d/conda.sh
conda activate /tmp/mooncake-e2e-bfcl

export PIP_CACHE_DIR=/tmp/pip-cache-mooncake
pip install 'sglang[all]'
```

2026-07-10 实测环境：

```text
Python 3.10.20
SGLang 0.5.15
torch 2.11.0+cu130
```

### Step 3: 构建 Mooncake Python wheel

Mooncake 的 pybind 扩展必须和运行 E2E 的 Python ABI 对齐。不要用系统
Python 3.12 构建后安装到 Python 3.10 环境。

```bash
cd ${REMOTE_DIR}

git submodule update --init --recursive

cmake -S . -B build-py310-abi \
  -DCMAKE_BUILD_TYPE=Release \
  -DPython_EXECUTABLE=/tmp/mooncake-e2e-bfcl/bin/python \
  -DPython3_EXECUTABLE=/tmp/mooncake-e2e-bfcl/bin/python \
  -DPYBIND11_FINDPYTHON=ON

cmake --build build-py310-abi \
  --target engine store mooncake_master mooncake_client transfer_engine_bench \
  -j$(nproc)

BUILD_DIR=build-py310-abi ./scripts/build_wheel.sh 3.10 dist-py310-e2e

pip install --force-reinstall mooncake-wheel/dist-py310-e2e/*.whl

python - <<'PY'
import importlib.util
for mod in ["sglang", "torch", "mooncake.engine", "mooncake.store"]:
    print(mod, bool(importlib.util.find_spec(mod)))
PY
```

### Step 4: 下载并检查测试模型

```bash
# 小模型（快速验证，~3GB）
pip install huggingface_hub
huggingface-cli download Qwen/Qwen2.5-1.5B-Instruct \
  --local-dir ${MODEL_DIR}/Qwen2.5-1.5B-Instruct

# 或者中等模型（完整验证，~15GB）
huggingface-cli download Qwen/Qwen2.5-7B-Instruct \
  --local-dir ${MODEL_DIR}/Qwen2.5-7B-Instruct

# 必须确认目录里真的有权重文件；只有 tokenizer/config 不够。
find ${MODEL_DIR}/Qwen2.5-7B-Instruct \
  -maxdepth 2 -type f \( -name '*.safetensors' -o -name 'pytorch_model*.bin' \) \
  | head
```

### Step 5: C++ 正确性门禁

在跑 E2E 性能数据前，先确认 MasterService 关键回归全绿：

```bash
cd ${REMOTE_DIR}

cmake --build build --target master_service_test -j$(nproc)

GLOG_minloglevel=2 ./build/mooncake-store/tests/master_service_test \
  --gtest_filter='MasterServiceTest.GetReplicaList*:MasterServiceTest.RemoveLeasedObject:MasterServiceTest.BatchReplicaClearWithLeaseActive'
```

2026-07-08 的完整 C++ 远端验证记录见：

```text
benchmarks/REMOTE_VALIDATION_20260708.md
```

2026-07-10 的 SGLang + HiCache + Mooncake E2E smoke 记录见：

```text
benchmarks/E2E_VALIDATION_20260710.md
```

### Step 6: 运行 Benchmark

```bash
cd ${REMOTE_DIR}
source ${CONDA_ROOT}/etc/profile.d/conda.sh
conda activate /tmp/mooncake-e2e-bfcl

# 使用 tmux 防断连
tmux new -s bench

# 运行端到端 benchmark
export CUDA_VISIBLE_DEVICES=0
./benchmarks/e2e_hicache_benchmark.sh \
  ${MODEL_DIR}/Qwen2.5-7B-Instruct \
  benchmarks/e2e_results_$(date +%Y%m%d)
```

脚本默认会按下列顺序解析 Mooncake Master：

1. `MOONCAKE_MASTER_BIN` 指定的可执行文件。
2. `MOONCAKE_BUILD_DIR` 指定 build 目录下的 `mooncake_master`。
3. 常见本地 build 路径，例如 `build/mooncake-store/src/mooncake_master`。
4. Python wheel 安装目录或 `PATH` 中的 `mooncake_master`。

如果使用 Python ABI 专用 build 目录，可以显式传入：

```bash
export MOONCAKE_BUILD_DIR=build-py310-abi

# 或直接指定二进制，优先级最高。
export MOONCAKE_MASTER_BIN=${REMOTE_DIR}/build-py310-abi/mooncake-store/src/mooncake_master
```

2026-07-10 的 cleanup-fix rerun 使用直接二进制
`./build/mooncake-store/src/mooncake_master`。脚本会将 Mooncake Master 和
SGLang 分别放入独立进程组；退出、报错或中断时会回收整个进程组，避免
`mooncake_master` 或 SGLang 子进程残留占用 `50051`、`8080`、`9003`、
`30000` 等端口。

### Step 7: 回收结果到本地

```bash
# 在本地执行
rsync -avz -e "ssh -i ${SSH_KEY} -p ${REMOTE_PORT}" \
  ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/benchmarks/e2e_results_*/ \
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
