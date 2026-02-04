# Mooncake E2E 测试用例指南

## 概述

本目录包含 Mooncake 项目的端到端（E2E）测试用例，这些用例被用来做CI测试，会在 [T-One](https://tone.openanolis.cn) 平台上执行。

## 测试环境

- **当前支持**：2 台 A10 GPU 服务器
- **未来扩展**：可能支持更多机器配置

## 当前支持的测试用例

### 1. test_1p1d_erdma.sh

**用例说明**：Prefill-Decode 分离架构的分布式推理测试

### 2. test_hicache_storage_mooncake_backend.sh

**用例说明**：HiCache 存储系统与 Mooncake 后端集成测试

### 3. test_disaggregation_different_tp.sh

**用例说明**：不同 TP 配置的 Prefill-Decode 分离架构测试


## T-One/tone-cli 支持

[tone-cli](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test) 提供以下自动化功能：

1. **代码拉取**：自动从 GitHub 拉取 E2E 测试代码
2. **用例执行**：自动执行指定用例集
3. **结果解析**：读取 `test_results.json` 文件解析测试结果
4. **日志收集**：将测试日志复制到 T-One 平台可访问的路径

## E2E 测试用例编写规范

### 1. 脚本封装要求

- **必须使用 Shell 脚本封装**：无论使用 Python 还是其他语言编写测试，最终都需要用 Shell 脚本封装
- **脚本位置**：放置在 `scripts/` 目录下
- **命名规范**：以 `test_` 开头（例如：`test_1p1d_erdma.sh`）

### 2. 变量声明要求

必须在脚本开头声明以下变量：

```bash
# 测试用例名称（必需）
test_case_name="test_demo"

# 测试类型（必需）：single（单机）或 double（双机）
TEST_TYPE="single"  # 或 "double"
```

**重要说明**：
- `test_case_name`：必须与脚本文件名（不含 `.sh`）一致
- `TEST_TYPE`：用于 `run_test.sh` 判断环境准备类型
  - `single`：单机测试，仅在本地机器运行
  - `double`：双机测试，需要本地和远程两台机器协同

### 3. 路径使用规范

- **禁止使用相对路径**：避免使用 `./` 或 `../` 等相对路径
- **善用环境变量**：使用 `$BASE_DIR`、`$TEST_CASE_RESULT_DIR` 等环境变量
- **推荐做法**：
  ```bash
  BASE_DIR=${TONE_TESTS_DIR}
  TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
  ```

### 4. 目录结构要求

测试产生的所有文件必须保存在 `tone_tests/run/` 目录下：

```
tone_tests/run/
├── logs/                           # 测试日志根目录
│   ├── test_1p1d_erdma/           # 每个测试用例的日志目录
│   │   ├── test_results.json      # 测试结果（必需）
│   │   ├── Qwen__Qwen3-8B/        # 模型相关日志（如适用）
│   │   │   ├── sglang_server_local.log
│   │   │   ├── sglang_server_remote.log
│   │   │   ├── load_balancer.log
│   │   │   └── curl_response.log
│   │   └── ...
│   └── test_disaggregation_different_tp/
│       ├── test_results.json
│       └── test_disaggregation_different_tp.log
├── whls/                           # wheel 包
│   └── mooncake-*.whl
├── pids/                           # 进程 PID 记录（用于清理）
│   └── test_case_name/
│       ├── server_prefill.pid
│       └── proxy.pid
└── .shrc                           # 环境变量配置文件
```

**路径规范**：
- 日志路径使用变量：`${BASE_DIR}/run/logs/${test_case_name}/`
- 通过 `TEST_CASE_RESULT_PATH` 变量引用：`run/logs/${test_case_name}`
- 避免硬编码路径，使用环境变量确保可移植性

### 5. 测试自包含原则

测试用例脚本应遵循自包含原则，但环境准备由 `run_test.sh` 统一管理：

**由 `run_test.sh` 负责**：
- 环境初始化（创建目录、生成 `.shrc` 配置文件）
- Docker 容器管理（拉取镜像、启动/停止容器）
- Wheel 包下载和安装
- 单机/双机环境判断和准备
- ERDMA 驱动安装
- 远程机器同步和配置（双机测试）

**测试脚本需要实现**：
- `run_test()` 函数：执行具体的测试逻辑
- `parse()` 函数：解析测试结果并生成 `test_results.json`
- 可选的其他函数（如 `start_server`、`run_proxy` 等）

**公共函数复用**（来自 `common.sh`）：
  - `setup_directory` - 创建目录
  - `setup_log_directory` - 创建/清理日志目录
  - `get_whl` - 下载 wheel 包
  - `get_image` - 下载镜像
  - `docker_launch` - 启动 Docker 容器
  - `clean_container` - 清理容器
  - `stop_container` - 停止容器
  - `check_server_ready` - 检查服务器就绪状态
  - `check_proxy_ready` - 检查代理就绪状态
  - `save_test_result` - 保存测试结果到 JSON 文件
  - `launch_and_track_process` - 启动进程并记录 PID
  - `kill_process` - 根据 PID 文件停止进程

### 6. 测试结果格式要求

**必需文件**：`tone_tests/run/logs/${test_case_name}/test_results.json`

**JSON 格式**：

```json
{
  "test_case": "test_1p1d_erdma",
  "status": "Pass",
  "timestamp": "2025-12-08T12:50:20Z"
}
```

或失败时：

```json
{
  "test_case": "test_1p1d_erdma",
  "status": "Fail",
  "timestamp": "2025-12-08T12:50:20Z"
}
```

**字段说明**：
- `test_case`：测试用例名称（必需）
- `status`：测试状态，`Pass` 或 `Fail`（必需）
- `timestamp`：UTC 时间戳，ISO 8601 格式（可选）

### 7. 脚本结构示例

#### 测试脚本示例

```bash
#!/bin/bash

# 测试用例基本信息
test_case_name="test_demo"
TEST_TYPE="single"  # 单机测试

# 初始化路径和加载公共函数
BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_test()
{
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        cd /test_workspace && \
        python3 -m pytest test_demo.py -v -s --tb=long" | tee "$log_file"
    
    return ${PIPESTATUS[0]}
}

parse()
{
    local test_exit_code=$1

    echo "===== Parsing test results ====="
    if [ $test_exit_code -eq 0 ]; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✓ Test PASSED"
        return 0
    else
        save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✗ Test FAILED"
        return 1
    fi
}

# 脚本入口点
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    exit_code=0
    if ! run_test; then
        exit_code=1
    fi
    
    parse $exit_code
    exit $?
fi
```

## 使用方法

### 通过 run_test.sh 控制（推荐）

`run_test.sh` 是测试用例的统一入口，负责环境准备、测试执行和资源清理的完整生命周期管理。

#### 运行单个测试用例

```bash
cd scripts/tone_tests/scripts
./run_test.sh run-single test_hicache_storage_mooncake_backend.sh
```

执行流程：
1. 根据测试类型（`TEST_TYPE`）准备环境（单机或双机）
2. 创建和配置 Docker 容器
3. 下载和安装 Mooncake wheel 包
4. 执行测试脚本的 `run_test()` 函数
5. 调用 `parse()` 函数解析结果
6. 清理环境和容器

#### 运行所有 SGLANG 测试用例

```bash
cd scripts/tone_tests/scripts
./run_test.sh run-all SGLANG
```

将按顺序执行 `All_TEST_SCRIPTS_SGLANG` 数组中的所有测试：
- `test_hicache_storage_mooncake_backend.sh`
- `test_disaggregation_different_tp.sh`
- `test_1p1d_erdma.sh`

#### 查看帮助信息

```bash
./run_test.sh
```

输出：
```
Mooncake CI Controller
Usage: ./run_test.sh <command> [args]

Commands:
  run-single <test_name>             - Full lifecycle: setup -> run -> parse -> cleanup
  run-all [SGLANG|VLLM]              - Run all tests for specific framework
  run-all SGLANG                     - Run all SGLANG tests (using SGLANG image)
  run-all VLLM                       - Run all VLLM tests (using VLLM image)
```

## 注意事项

1. **环境变量使用**：使用 `$BASE_DIR`、`$TEST_CASE_RESULT_PATH` 等环境变量，避免硬编码路径
2. **日志完整性**：确保所有重要日志都保存到 `${BASE_DIR}/run/logs/${test_case_name}/` 目录
3. **资源清理**：测试脚本应通过 PID 文件管理进程，确保资源正确清理
4. **JSON 格式**：严格遵守 JSON 格式规范，必须包含 `test_case` 和 `status` 字段
5. **测试类型声明**：必须在脚本开头声明 `TEST_TYPE` 变量，供 `run_test.sh` 判断环境准备类型

## 相关链接

- [T-One 平台](https://tone.openanolis.cn)
- [tone-cli 代码仓库](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test)
