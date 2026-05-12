# Mooncake NVMF-SSD 池部署指南

## 1. 环境准备

### 1.1 获取源码
```bash
git clone https://gitcode.com/boostkit/mooncake.git
cd mooncake
```

### 1.2 切换分支
```bash
git checkout v0.3.8_add_ssd_cache
```

### 1.3 安装依赖
```bash
./dependencies.sh  # 自动安装编译 mooncake 所需的依赖库及 SPDK 库
```

## 2. 编译安装

### 2.1 编译
```bash
mkdir build
cd build
cmake .. -DUSE_NOF=ON  # -DUSE_NOF=ON 表示开启 NOF 池，默认开启可不指定
make -j
```

**说明**：
- `-DUSE_NOF=ON`：开启 NOF 池功能（默认开启）
- `-DUSE_NOF=OFF`：关闭 NOF 池功能

### 2.2 安装
```bash
make install
```

## 3. Mooncake 服务部署

### 3.1 节点拓扑结构
- **Mooncake 服务节点**：192.168.65.81（部署 master、metadata、store 三个组件）
- **SSD 池节点**：192.168.65.56 和 192.168.65.57（提供 SSD 存储资源）

### 3.2 部署 Master 服务
```bash
mooncake_master --rpc_address=192.168.65.81
```

### 3.3 部署 Metadata 服务
```bash
python3 -m mooncake.http_metadata_server --host=192.168.65.81 --port=8080
```
启动可能出现 aiohttp 相关错误，需安装 aiohttp 库：
```bash
pip3 install aiohttp
```

### 3.4 部署 Store 服务

#### 配置文件 `store_service.json`
在 `home` 目录下创建 `store_service.json` 配置文件：
```json
{
  "local_hostname": "localhost",
  "metadata_server": "http://192.168.65.81:8080/metadata",
  "master_server_address": "192.168.65.81:50051",
  "protocol": "rdma",
  "device_name": "mlx5_0",
  "global_segment_size": "50gb",
  "local_buffer_size": 0
}
```

**说明**：
- `device_name`：可通过 `ibv_devices` 命令查看 192.168.65.81 节点的网卡名称

#### 启动服务
store 服务启动会初始化 spdk 环境，需要在 store 服务节点（192.168.65.81）上配置大页内存：
```bash
echo 512 > /proc/sys/vm/nr_hugepages
```
说明：启动只需要少量大页内存，建议启动时配置 512 个大页内存即可。

启动 store 服务：
```bash
python3 -m mooncake.mooncake_store_service --config=/home/store_service.json --port=8081
```
提示：如启动出现 Timeout 错误，需检查 192.168.65.81 节点是否配置了代理。 如配置了代理，需取消代理配置。

## 4. NVMF-SSD 池部署

### 4.1 前置条件
1. 在 Mooncake 节点（192.168.65.81）配置到 SSD 池节点（192.168.65.56 和 192.168.65.57）的免密登录
参考 [配置免密登录](https://www.hikunpeng.com/document/detail/zh/kunpengsdss/ecosystemEnable/Ceph/kunpengcephblock_04_0017_0.html)
2. SSD 池节点需提前编译好 SPDK
参考 [SPDK 编译安装](https://github.com/spdk/spdk/blob/master/README.md#build)

### 4.2 安装 SSH 依赖
```bash
python3 -m pip install "paramiko>=3.4.0"
```

### 4.3 一键部署 SSD 池

#### 部署命令
```bash
python3 -m mooncake.spdk_tgt_create \
    --spdk_target_info="ip:192.168.65.56 path:/home/spdk pci:0000:01:00.0,0000:02:00.0" \
    --spdk_target_info="ip:192.168.65.57 path:/home/spdk pci:0000:01:00.0"
```

#### 参数说明
| 参数 | 说明 |
|------|------|
| `ip` | target 节点的 IP 地址 |
| `path` | target 节点上 SPDK 的安装路径 |
| `pci` | 需要注册到 target 的 SSD 盘 PCI 号（多个用逗号分隔） |

**提示**：可在 target 节点通过 `/spdkpath/scripts/setup.sh status` 命令查看可用的 PCI 号

## 5. NVMF-SSD 池注册

### 5.1 一键注册所有 SSD 盘
```bash
python3 -m mooncake.mooncake_ssd_register \
    --master_server_address=192.168.65.81:50051 \
    --spdk_target_info="ip:192.168.65.56 path:/home/spdk" \
    --spdk_target_info="ip:192.168.65.57 path:/root/spdk"
```

#### 参数说明
| 参数 | 说明 |
|------|------|
| `--master_server_address` | master 服务节点的 IP 和端口号（默认端口：50051） |
| `--spdk_target_info` | target 节点信息：ip（节点 IP）、path（SPDK 安装路径） |

## 6. NVMF-SSD 池解注册

### 6.1 解注册指定 SSD 盘
```bash
python3 -m mooncake.mooncake_ssd_unregister \
    --master_server_address=192.168.65.81:50051 \
    --spdk_target_info="ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1"
```

#### 参数说明
| 参数 | 说明 |
|------|------|
| `--master_server_address` | master 服务节点的 IP 和端口号（默认端口：50051） |
| `--spdk_target_info` | 解注册盘信息：ip（节点 IP）、ns（namespace 号）、nqn（所属 nqn） |

### 6.2 获取 Target 端盘信息

进入 target 节点的 SPDK 目录，执行以下命令：

1. 查看子系统信息（nqn 和 namespace 号）：
```bash
./scripts/rpc.py nvmf_get_subsystems
```

2. 查看盘的详细信息（块大小、PCI 号等）：
```bash
./scripts/rpc.py bdev_get_bdevs
```

## 7. 性能测试

### 7.1 使用内置压测工具

```bash
./build/mooncake-store/benchmarks/nof_worker_pool_bench \
    --endpoints='traddr:192.168.65.56 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1 trtype:RDMA adrfam:IPv4 ns:1, traddr:192.168.65.56 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1 trtype:RDMA adrfam:IPv4 ns:2' \
    --op=read \
    --io_size=1048576 \
    --iodepth=8 \
    --warmup_sec=3 \
    --duration_sec=30
```

#### 参数说明
| 参数 | 说明 |
|------|------|
| `--endpoints` | 测试盘信息 |
| `--op` | 读写操作类型（read/write） |
| `--io_size` | 块大小（字节） |
| `--iodepth` | 读写队列深度 |
| `--warmup_sec` | 预热时间（秒） |
| `--duration_sec` | 测试时长（秒） |

### 7.2 VLLM+LMCache+Mooncake 端到端测试
在 192.168.65.81 节点部署 VLLM 服务，并配置 LMCache 插件。
192.168.65.81 作为推理节点，需要配置显卡资源(本文档以 1 张 Nvidia A100 显卡为例)。
#### 安装 Nvidia 驱动
下载并安装显卡对应的 CUDA 驱动，根据 GPU 型号选择合适的驱动版本。参考 [Nvidia 驱动安装](https://www.nvidia.com/Download/index.aspx) A100 显卡对应的 CUDA 驱动我们选择版本为 12.9.0

下载 CUDA 12.9.0 驱动：
```bash
wget https://developer.download.nvidia.com/compute/cuda/12.9.0/local_installers/cuda_12.9.0_575.51.03_linux_sbsa.run
```
安装 CUDA 12.9.0 驱动：
```bash
sudo sh cuda_12.9.9_575.51.03_linux_sbsa.run
```

#### 安装 pytorch 及 torch 相关库
用一个干净的python环境安装pytorch及torch相关库,使用 conda 构建一个新python 3.11的环境
下载conda安装脚本：
```bash
wget https://repo.anaconda.com/archive/Anaconda3-2025.05-Linux-aarch64.sh
```
安装conda：
```bash
bash Anaconda3-2025.05-Linux-aarch64.sh
```
重启shell会话，使conda生效：
```bash
source /root/anaconda3/etc/profile.d/conda.sh
```
创建新的python 3.11环境：
```bash
conda create -n vllm python=3.11
conda activate vllm
```
安装pytorch及torch相关库：
```bash
pip install torch==2.9.0 -f https://mirrors.aliyun.com/pytorch-wheels/cu129 --trust-host mirrors.aliyun.com
```

#### 安装 LMCache
用 conda 环境安装 LMCache ，设置 LMCache 环境变量：
```bash
export CUDA_HOME=/usr/local/cuda/
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH
export TORCH_CUDA_ARCH_LIST="8.0"
```
下载 LMCache 代码并安装：
```bash
git clone https://gitcode.com/boostkit/LMCache.git
cd LMCache
git checkout v0.3.13_support_hugepage_memory
pip install -e . --no-build-isolation
```

#### 安装 VLLM 及相关库
用 conda 环境安装 VLLM 及相关库：
```bash
git clone https://gitcode.com/vllm-project/vllm.git
cd vllm
git checkout v0.15.2rc0
python use_existing_torch.py   # 指向已经安装的pytorch
pip install -r requirements/build.txt
taskset -c 0-31 pip install -e . --no-build-isolation # 编译 vllm 非常吃内存，建议用 taskset -c 限制下编译的核心数
```

#### 启动 VLLM 服务

1. 设置环境变量：
```bash
export LMCACHE_CONFIG_FILE="/path/vllm-lmcache-mooncake-config.yaml"
export MC_NOF_WORKERS=4
export MC_NOF_SUBMIT_CHUNK_BYTES=$((1 << 17))  # 128KB
export MC_NOF_INFLIGHT_BYTES_LIMIT=$((1 << 25))  # 32MB
```

2. 启动服务：
```bash
vllm serve --port 7070 \
           --tensor-parallel-size 1 --gpu-memory-utilization 0.8 --trust-remote-code \
           --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_both"}' \
           --model /home/Qwen3-8B
```
参数说明：
| 参数 | 说明 |
|------|------|
| `--port` | VLLM 服务端口号 |
| `--tensor-parallel-size` | 张量并行度（与 GPU 数量一致） |
| `--gpu-memory-utilization` | GPU 内存利用率（0.8 表示 80%） |
| `--trust-remote-code` | 信任远程代码执行 |
| `--kv-transfer-config` | KV 缓存传输配置（LMCache 插件） |
| `--model` | 模型路径（Qwen3-8B） |


#### LMCache 配置文件

```yaml
chunk_size: 256
remote_url: "mooncakestore://192.168.65.81:50051/"
remote_serde: "native"
local_cpu: True
max_local_cpu_size: 8
enable_mooncake_nof_pool: True

extra_config:
  local_hostname: "localhost"
  metadata_server: "http://192.168.65.81:8080/metadata"
  master_server_address: "192.168.65.81:50051"
  global_segment_size: 0
  local_buffer_size: 0
  protocol: "rdma"
  device_name: "mlx5_0"
```

**说明**：
- `extra_config` 中的参数与 `store_service.json` 一致
- `enable_mooncake_nof_pool=True`：启用 NOF 池化功能（使用 SPDK 申请大页内存）
- 若设为 `False`，则申请普通内存，KVCache 无法写入 SSD 池

#### 环境变量说明
| 环境变量 | 说明 | 默认值 |
|----------|------|--------|
| `MC_NOF_WORKERS` | 处理 SPDK NoF IO 操作的工作线程数量 | 4 |
| `MC_NOF_SUBMIT_CHUNK_BYTES` | 每次向 SPDK 提交的 IO 操作大小 | 128KB |
| `MC_NOF_INFLIGHT_BYTES_LIMIT` | 系统中允许的最大未完成 IO 字节数 | 32MB |

**注意**：这三个参数共同构成了 SPDK NoF IO 的 QoS 控制机制

#### 执行多轮对话推理测试

```bash
vllm bench serve \
--backend openai \
--model /home/Qwen3-8B \
--dataset-path /home/ShareGPT.json \
--burstiness 1 \
--request-rate 10 \
--max-concurrency 4 \
--save-result \
--result-dir ./results \
--host localhost \
--port 7070
```
参数说明：
| 参数 | 说明 |
|------|------|
| `--backend` | 后端服务类型（openai） |
| `--model` | 模型路径（Qwen3-8B） |
| `--dataset-path` | 测试数据集路径（ShareGPT.json） |
| `--burstiness` | 最大请求并发数 |
| `--input-len` | 输入序列长度 |
| `--output-len` | 输出序列长度 |
| `--request-rate` | 请求速率（QPS） |
| `--max-concurrency` | 最大并发数 |
| `--save-result` | 保存测试结果 |
| `--result-dir` | 测试结果保存目录 |
| `--host` | 服务主机名 |
| `--port` | 服务端口号 |
| `--num-clients` | 客户端数量 |
| `--num-rounds` | 测试轮数 |
| `--save-detailed` | 保存详细测试结果 |
