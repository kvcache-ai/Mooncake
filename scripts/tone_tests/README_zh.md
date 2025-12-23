# Mooncake E2E 测试用例指南

## 概述

本目录包含 Mooncake 项目的端到端（E2E）测试用例，这些用例被用来做CI测试，会在 [T-One](https://tone.openanolis.cn) 平台上执行。

## 测试环境

- **当前支持**：2 台 A10 GPU 服务器
- **未来扩展**：可能支持更多机器配置

## 当前支持的测试用例

### 1. test_1p1d_erdma.sh

**用例说明**：Prefill-Decode 分离架构的分布式推理测试

- **测试场景**：在两台机器上分别部署 Prefill 和 Decode 服务器，通过 ERDMA（弹性 RDMA）网络进行通信
- **测试内容**：
  - 本地机器运行 Prefill 模式的 SGLang 服务器
  - 远程机器运行 Decode 模式的 SGLang 服务器  
  - 通过负载均衡器（Load Balancer）代理请求
  - 发送测试请求验证分布式推理功能
- **支持的模型**：Qwen/Qwen3-8B，deepseek-ai/DeepSeek-V2-Lite
- **测试环境**：双机运行，每机 2 个 GPU（TP=2）

### 2. test_hicache_storage_mooncake_backend.sh

**用例说明**：HiCache 存储系统与 Mooncake 后端集成测试

- **测试场景**：验证 HiCache 存储系统使用 Mooncake 作为后端的功能正确性
- **测试内容**：
  - 启动 Mooncake metadata 和 master 服务
  - 在 Docker 容器中运行 pytest 测试
  - 测试不同的内存布局（page_first、page_first_direct 等）
  - 测试不同模型（标准模型、MLA 模型）
  - 验证缓存准确性
- **测试环境**：单机运行，使用 2 个 GPU（TP=2）

## T-One/tone-cli 支持

[tone-cli](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test) 提供以下自动化功能：

1. **代码拉取**：自动从 GitHub 拉取 E2E 测试代码
2. **变量替换**：扫描 `scripts/` 目录下以 `test_` 开头的脚本，自动替换以下变量：
   - `LOCAL_IP` - 本地机器 IP
   - `REMOTE_IP` - 远程机器 IP
   - `ARTIFACT_ID` - GitHub Actions artifact ID
   - `GIT_REPO` - 下载whl包的 Git 仓库地址
3. **用例执行**：自动执行符合规范的测试脚本
4. **结果解析**：读取 `test_results.json` 文件解析测试结果
5. **日志收集**：将测试日志复制到 T-One 平台可访问的路径

## E2E 测试用例编写规范

### 1. 脚本封装要求

- **必须使用 Shell 脚本封装**：无论使用 Python 还是其他语言编写测试，最终都需要用 Shell 脚本封装
- **脚本位置**：放置在 `scripts/` 目录下
- **命名规范**：以 `test_` 开头（例如：`test_1p1d_erdma.sh`）

### 2. 变量声明要求

必须在脚本开头声明以下变量：

```bash
# 双机测试
LOCAL_IP=
REMOTE_IP=

# 单机/多机测试都需要
ARTIFACT_ID=
GIT_REPO=
test_case_name=
```

### 3. 路径使用规范

- **禁止使用相对路径**：避免使用 `./` 或 `../` 等相对路径
- **善用环境变量**：使用 `$BASE_DIR`、`$TEST_CASE_RESULT_DIR` 等环境变量
- **推荐做法**：
  ```bash
  BASE_DIR=${TONE_TESTS_DIR}
  TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
  ```

### 4. 目录结构要求

测试产生的所有文件必须保存在 `tone_tests/run/${test_case_name}/` 目录下：

```
tone_tests/run/test_1p1d_erdma/
├── logs/                    # 测试日志
├── whls/                    # wheel 包
│   └── mooncake-*.whl
├── .shrc                    # 环境变量配置
└── test_results.json        # 测试结果（必需）
```

### 5. 测试自包含原则

- **容器管理**：脚本需自行管理 Docker 容器（拉取镜像、配置参数、启动/停止）
- **环境隔离**：单机和多机测试均由脚本独立完成
- **公共函数复用**：可使用 `common.sh` 中的公共函数：
  - `setup_test_result_directory` - 创建测试结果目录
  - `setup_log_directory` - 创建日志目录
  - `get_whl` - 下载 wheel 包
  - `get_image` - 拉取 Docker 镜像
  - `docker_launch` - 启动 Docker 容器
  - `clean_container` - 清理容器
  - `stop_container` - 停止容器
  - `check_server_ready` - 检查服务器就绪状态
  - `check_proxy_ready` - 检查代理就绪状态

### 6. 测试结果格式要求

**必需文件**：`tone_tests/run/${test_case_name}/test_results.json`

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

推荐的脚本结构：

```bash
#!/bin/bash

test_case_name="test_demo"
TONE_TESTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)

# 变量声明
LOCAL_IP=
REMOTE_IP=
ARTIFACT_ID=
GIT_REPO=

. ${TONE_TESTS_DIR}/scripts/common.sh

setup() {
    # 容器启动等环境配置
}

run_test() {
    # 执行测试
}

parse() {
    # 解析结果并生成 test_results.json
}

cleanup() {
    # 清理资源
}

main() {
    setup
    run_test
    parse
    cleanup
}

main
```

## 使用方法

### 执行完整测试

```bash
cd scripts && ./test_demo.sh
```

### 在 T-One 平台执行

测试用例会由 tone-cli 自动扫描和执行，无需手动干预。

## 注意事项

1. **环境变量优先级**：tone-cli 会替换脚本中的变量
2. **日志完整性**：确保所有重要日志都保存到指定目录，方便排查问题
3. **资源清理**：即使测试失败也要执行 cleanup 清理资源
4. **JSON 格式**：严格遵守 JSON 格式规范，避免语法错误

## 相关链接

- [T-One 平台](https://tone.openanolis.cn)
- [tone-cli 代码仓库](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test)
