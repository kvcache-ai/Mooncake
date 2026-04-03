# Mooncake store 集群压测脚本

## 运行环境与镜像要求

在 **nerdctl** 容器内执行 `real_client_stress_workload.py` 时，镜像需满足：

- 已安装 **Python 3**，且能正常执行 `import mooncake`（提供 `mooncake.store` / `MooncakeDistributedStore` 等接口；通常需安装与集群一致的 Mooncake Python 包或 wheel）。
- 镜像内存在本仓库中的 **`real_client_stress_workload.py`**（或你通过 **`MOONCAKE_STRESS_PY`** / **`--python-script`** 指定的路径）。`run_stress_cluster.py` 默认假定容器内路径为  
  `/vllm-workspace/Mooncake/mooncake-store/tests/real_client_stress_workload.py`。  
  若镜像未包含该文件，可使用 **`--update-script-path`** 将本机脚本同步到各节点主机路径，再 **`nerdctl cp`** 进容器。
- 分层存储配置：环境变量 **`MOONCAKE_TIERED_CONFIG`**（JSON 字符串），与 orchestrator 的 **`--tiered-backend-config`** / 工作负载的 **`--tiered_backend_config`** 一致。

## 脚本说明

| 脚本 | 作用 |
|------|------|
| **`real_client_stress_workload.py`** | 单机多线程压测：连接 `mooncake_master`，按 **`--workload_mode`** 执行不同负载，结束时输出 **`STATS_JSON:`** 行。 |
| **`run_stress_cluster.py`** | 在多台主机上通过 SSH + **nerdctl** 拉起 **`mooncake_master`** 与各 **`mc-client-node*`**，可选多机同步（`preload_then_read` 的 READ，或其它模式的 GO 屏障），合并各节点统计到 **`merged_stress_report.json`**。 |

子命令：

- **`run`**：部署并运行（见下文示例）。
- **`kill`**：在 **`--client-ips`** 所列主机上删除 **`mc-client-node*`**，在 **`--master-ip`** 上删除 **`mc-master`**。

## `workload_mode` 与各模式示例

以下示例中请替换 **`MASTER_IP`**、**`CLIENT_IPS`**（逗号分隔，顺序对应 node_id 1..N）、**`IMAGE`** 等为实际值。

### 1. `preload_then_read`

先 **`put`** 预加载本节点 key，再按 **`remote_read_ratio`** 做本地/跨节点读压测。

**单机直接跑（容器或本机，需已能连 master）：**

```bash
python3 real_client_stress_workload.py \
  --workload_mode=preload_then_read \
  --client_mode=centralized \
  --master_address=MASTER_IP:50053 \
  --metadata_connection_string=http://MASTER_IP:8080/metadata \
  --local_hostname=本机可达地址 \
  --num_nodes=1 --node_id=1 \
  --key_count=1000 --num_threads=4 --test_operation_nums=500
```

**多机 + orchestrator（先等全部 `preload_done`，再对各节点控制口发 `READ`）：**

```bash
python3 run_stress_cluster.py run \
  --mode centralized \
  --master-ip MASTER_IP \
  --client-ips NODE1,NODE2 \
  --image IMAGE \
  --workload-mode preload_then_read \
  --use-orchestrated
```

**多机、非 orchestrated、按时间对齐读阶段（`--start-delay-sec` 仅在该组合下由 orchestrator 写入 `start_timestamp_ms`）：**

```bash
python3 run_stress_cluster.py run \
  --master-ip MASTER_IP --client-ips NODE1,NODE2 \
  --workload-mode preload_then_read \
  --start-delay-sec 5
```

相关参数要点：**`--run_mode orchestrated`**（由 `--use-orchestrated` 注入）、**`--control_listen`**（控制端口，0 表示自动选端口）、**`--start_timestamp_ms`**（once 模式对齐读开始时间）。

### 2. `op_sequence`

混合 **`is_exist` / get / put`** 的序列操作，用于更贴近真实访问路径的压测。

```bash
python3 run_stress_cluster.py run \
  --master-ip MASTER_IP --client-ips NODE1,NODE2 \
  --workload-mode op_sequence \
  --op-sequence-max-rounds 8 \
  --remote-read-ratio 0.5
```

多节点默认在全部客户端就绪后由 orchestrator 发 **GO**；若不需要同步，可加 **`--no-cluster-barrier`**。

### 3. `concurrent_write_no_evict`

按全局段容量与 **`cw_target_fill_ratio`** 计算写入量，并发 **`put`**，并抽样 **`get`** 校验内容。

```bash
python3 run_stress_cluster.py run \
  --master-ip MASTER_IP --client-ips NODE1 \
  --workload-mode concurrent_write_no_evict \
  --global-segment-size 2147483648 \
  --cw-target-fill-ratio 0.15 \
  --cw-verify-sample-limit 100
```

### 4. `concurrent_write_with_evict`

写入量超过 **`cw_base_memory_bytes * cw_evict_data_ratio`**，触发淘汰；`run_stress_cluster` 会为 **`mooncake_master`** 打开与淘汰相关的参数。

```bash
python3 run_stress_cluster.py run \
  --master-ip MASTER_IP --client-ips NODE1 \
  --workload-mode concurrent_write_with_evict \
  --cw-base-memory-bytes 1073741824 \
  --cw-evict-data-ratio 3.0
```

## `run_stress_cluster.py run` 常用参数说明

| 参数 | 含义 |
|------|------|
| `--mode` | `p2p` 或 `centralized`（部署方式与 metadata 连接串）。 |
| `--master-ip` | 运行 **`mooncake_master`** 的主机。 |
| `--client-ips` | 客户端所在主机列表，顺序决定 **node_id**。 |
| `--image` | nerdctl 使用的镜像（默认可由环境变量 **`IMAGE`** 覆盖）。 |
| `--workload-mode` | 对应 **`real_client_stress_workload.py`** 的 **`--workload_mode`**。 |
| `--update-script-path` | 本机 **`real_client_stress_workload.py`** 路径；设置后会 scp 到远端再 **`nerdctl cp`** 进容器。 |
| `--python-script` | 容器内脚本路径（默认见 **`MOONCAKE_STRESS_PY`**）。 |
| `--use-orchestrated` | 仅 **`preload_then_read`**：等 **`preload_done`** 后对控制口发 **READ**。 |
| `--no-cluster-barrier` | 多节点非 **`preload_then_read`** 时不等待 **GO**。 |
| `--no-wait` | 不等待 **`=== Test Completed ===`** 即退出。 |
| `--no-cleanup` | 结束后不删除容器。 |
| `--latency-dump-dir` | 从各节点收集延迟 **`.npz`** 到该目录。 |
| `--merge-output` | 合并统计 JSON 输出路径（默认 **`merged_stress_report.json`**）。 |

更多环境变量默认值见脚本内 **`os.environ.get(...)`** 定义。

## Orchestrated `preload_then_read` 流程简述

1. 各客户端启动 **`--run_mode orchestrated`**，监听控制端口，并打印 **`CONTROL_ENDPOINT:host:port`** 与 **`=== Phase preload_done ... ===`**。
2. Orchestrator 等待全部 **`preload_done`** 后，向各节点控制连接发送以 **`READ`** 开头的行。
3. 读阶段结束后客户端打印 **`=== Test Completed ===`**；orchestrated 模式下进程会阻塞，需 **`run_stress_cluster.py kill`** 或手动停容器。