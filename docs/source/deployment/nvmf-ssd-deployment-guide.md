# Mooncake NVMe-oF SSD Pool Deployment Guide

## Overview

This guide shows how to attach an NVMe-oF SSD pool to Mooncake Store. The
deployment has two main phases:

- Start Mooncake services built with NoF support enabled.
- Create SPDK NVMe-oF targets on SSD pool nodes and register their namespaces
  with the Mooncake master.

After registration, the master reports the registered NVMe-oF namespaces as a
remote SSD pool in its metrics, and clients can place NoF replicas through
Mooncake Store.

## 1. Build Mooncake with NoF Support

Follow the "Build with NVMe-oF SSD Pool" section in the
[Build Guide](../getting_started/build.md) to install SPDK dependencies and
build Mooncake with `-DUSE_NOF=ON`.

## 2. Deploy Mooncake Services

### 2.1 Node Topology

- **Mooncake service node**: 192.168.65.81. This node runs the master, metadata, and store services.
- **SSD pool nodes**: 192.168.65.56 and 192.168.65.57. These nodes provide SSD storage resources.

### 2.2 Deploy the Master Service

```bash
mooncake_master --rpc_address=192.168.65.81
```

### 2.3 Deploy the Metadata Service

```bash
python3 -m mooncake.http_metadata_server --host=192.168.65.81 --port=8080
```

If an aiohttp-related error occurs during startup, install aiohttp:

```bash
pip3 install aiohttp
```

### 2.4 Deploy the Store Service

#### Configure `store_service.json`

Create `store_service.json` under `/home`:

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

**Notes**:

- `device_name`: Run `ibv_devices` on node 192.168.65.81 to check the RDMA device name.

#### Start the Service

The store service initializes the SPDK environment during startup. Configure hugepages on the store service node, 192.168.65.81:

```bash
echo 512 > /proc/sys/vm/nr_hugepages
```

Only a small number of hugepages is required during startup. In most cases, 512 hugepages are sufficient.

Start the store service:

```bash
python3 -m mooncake.mooncake_store_service --config=/home/store_service.json --port=8081
```

If a timeout error occurs during startup, check whether a proxy is configured on node 192.168.65.81. If a proxy is configured, unset the proxy configuration and try again.

## 3. Deploy the NVMe-oF SSD Pool

### 3.1 Prerequisites

1. Configure passwordless SSH login from the Mooncake node, 192.168.65.81, to the SSD pool nodes, 192.168.65.56 and 192.168.65.57.
   See [OpenSSH key-based authentication](https://help.ubuntu.com/community/SSH/OpenSSH/Keys).
2. Build SPDK on each SSD pool node in advance.
   See [SPDK build instructions](https://github.com/spdk/spdk/blob/master/README.md#build).

### 3.2 Install SSH Dependencies

```bash
python3 -m pip install "paramiko>=3.4.0"
```

### 3.3 Deploy the SSD Pool

#### Deployment Command

```bash
python3 -m mooncake.spdk_tgt_create \
    --spdk_target_info="ip:192.168.65.56 path:/home/spdk pci:0000:01:00.0,0000:02:00.0" \
    --spdk_target_info="ip:192.168.65.57 path:/home/spdk" \
    --core-mask=0xff \
    --transport-type=RDMA \
    --max-queue-depth=128 \
    --max-io-qpairs-per-ctrlr=127 \
    --max-io-size=4096 \
    --in-capsule-data-size=131072 \
    --io-unit-size=131072 \
    --max-aq-depth=128 \
    --num-shared-buffers=4096 \
    --buf-cache-size=32
```

#### Parameters

| Parameter | Description |
|-----------|-------------|
| `ip` | IP address of the target node. |
| `path` | SPDK installation path on the target node. |
| `pci` | PCI addresses of SSDs to register with the target. Use commas to separate multiple PCI addresses. If this field is omitted, SPDK-ready or unmounted NVMe devices on the target node are registered. |
| `--core-mask` | CPU core mask used to start `nvmf_tgt` with `-m`. The default value is `0xff`. |

**Tip**: Run `/path/scripts/setup.sh status` on a target node to list available PCI addresses.

#### Transport Options

The transport options are passed to the SPDK `nvmf_create_transport` RPC. If an option is not specified, the tool uses the default value listed below.

| Option | Default | Description |
|--------|---------|-------------|
| `--transport-type` | `RDMA` | NVMe-oF transport type. |
| `--max-queue-depth` | `128` | Maximum number of outstanding I/O operations per queue. |
| `--max-io-qpairs-per-ctrlr` | `127` | Maximum number of I/O queue pairs per controller. |
| `--max-io-size` | `4096` | Maximum I/O size, in bytes. |
| `--in-capsule-data-size` | `131072` | Maximum in-capsule data size, in bytes. |
| `--io-unit-size` | `131072` | I/O unit size, in bytes. |
| `--max-aq-depth` | `128` | Maximum number of admin commands per admin queue. |
| `--num-shared-buffers` | `4096` | Number of pooled data buffers available to the transport. |
| `--buf-cache-size` | `32` | Number of shared buffers reserved for each poll group. |

## 4. Register the NVMe-oF SSD Pool

### 4.1 Register All SSDs

```bash
python3 -m mooncake.mooncake_ssd_register \
    --master_server_address=192.168.65.81:50051 \
    --spdk_target_info="ip:192.168.65.56 path:/home/spdk" \
    --spdk_target_info="ip:192.168.65.57 path:/root/spdk"
```

#### Parameters

| Parameter | Description |
|-----------|-------------|
| `--master_server_address` | IP address and port of the master service node. The default port is 50051. |
| `--spdk_target_info` | Target node information, including `ip`, the node IP address, and `path`, the SPDK installation path. |
| `--username` | SSH username used to connect to target nodes. The default value is `root`. |
| `--port` | SSH port used to connect to target nodes. The default value is `22`. |
| `--password` | SSH password used to connect to target nodes. |
| `--key-file` | SSH private key file used to connect to target nodes. |

## 5. Unregister the NVMe-oF SSD Pool

### 5.1 Unregister a Specific SSD

```bash
python3 -m mooncake.mooncake_ssd_unregister \
    --master_server_address=192.168.65.81:50051 \
    --spdk_target_info="ip:192.168.65.56 ns:1 nqn:nqn.2016-06.io.spdk:cnode1"
```

#### Parameters

| Parameter | Description |
|-----------|-------------|
| `--master_server_address` | IP address and port of the master service node. The default port is 50051. |
| `--spdk_target_info` | Disk information to unregister, including `ip`, the node IP address, `ns`, the namespace ID, and `nqn`, the subsystem NQN. |
| `--username` | SSH username used to connect to target nodes. The default value is `root`. |
| `--port` | SSH port used to connect to target nodes. The default value is `22`. |
| `--password` | SSH password used to connect to target nodes. |
| `--key-file` | SSH private key file used to connect to target nodes. |

### 5.2 Get Target Disk Information

Enter the SPDK directory on the target node and run the following commands.

1. Show subsystem information, including NQN and namespace IDs:

```bash
./scripts/rpc.py nvmf_get_subsystems
```

2. Show disk details, including block size and PCI address:

```bash
./scripts/rpc.py bdev_get_bdevs
```

## 6. Performance Tests

### 6.1 Use the Built-in Benchmark Tool

```bash
./build/mooncake-store/benchmarks/nof_worker_pool_bench \
    --endpoints='traddr:192.168.65.56 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1 trtype:RDMA adrfam:IPv4 ns:1, traddr:192.168.65.56 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1 trtype:RDMA adrfam:IPv4 ns:2' \
    --op=read \
    --io_size=1048576 \
    --iodepth=8 \
    --warmup_sec=3 \
    --duration_sec=30
```

#### Parameters

| Parameter | Description |
|-----------|-------------|
| `--endpoints` | Disk information used for the test. |
| `--op` | I/O operation type, either `read` or `write`. |
| `--io_size` | Block size in bytes. |
| `--iodepth` | Read/write queue depth. |
| `--warmup_sec` | Warmup duration in seconds. |
| `--duration_sec` | Test duration in seconds. |

### 6.2 Use NoF with vLLM + LMCache

For the general VLLM + LMCache + Mooncake deployment flow, see
[vLLM V1 Disaggregated Serving with Mooncake Store and LMCache](../getting_started/examples/vllm-integration/vllmv1-lmcache-integration.md).
After the NVMe-oF SSD pool is registered with Mooncake, add the NoF-specific
Mooncake configuration below.

#### NoF Environment Variables

```bash
export LMCACHE_CONFIG_FILE="/path/vllm-lmcache-mooncake-config.yaml"
export MC_NOF_WORKERS=4
export MC_NOF_SUBMIT_CHUNK_BYTES=$((1 << 17))  # 128KB
export MC_NOF_INFLIGHT_BYTES_LIMIT=$((1 << 25))  # 32MB
```

#### NoF LMCache Configuration

```yaml
chunk_size: 256
remote_url: "mooncakestore://192.168.65.81:50051/"
remote_serde: "naive"
local_cpu: True
max_local_cpu_size: 8
enable_mooncake_nof_pool: True

extra_config:
  local_hostname: "localhost"
  metadata_server: "http://192.168.65.81:8080/metadata"
  master_server_address: "192.168.65.81:50051"
  global_segment_size: 0
  local_buffer_size: 1073741824
  protocol: "rdma"
  device_name: "mlx5_0"
```

**Notes**:

- `enable_mooncake_nof_pool=True` enables writing KV cache objects to the
  registered NoF pool.
- `global_segment_size: 0` means the inference process does not contribute a
  memory segment to the Mooncake cluster.
- Keep `local_buffer_size` non-zero because the client still needs local
  staging buffers for Mooncake transfers.
- The parameters in `extra_config` should use the same Mooncake master,
  metadata server, protocol, and RDMA device as the store service.

| Environment Variable | Description | Default |
|----------------------|-------------|---------|
| `MC_NOF_WORKERS` | Number of worker threads used to process SPDK NoF I/O operations. | 4 |
| `MC_NOF_SUBMIT_CHUNK_BYTES` | Size of each I/O operation submitted to SPDK. | 128KB |
| `MC_NOF_INFLIGHT_BYTES_LIMIT` | Maximum number of in-flight I/O bytes allowed in the system. | 32MB |

These three parameters together provide QoS control for SPDK NoF I/O.
