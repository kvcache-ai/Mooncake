# Mooncake Store Deployment Guide

This guide shows how to bring up a minimal Mooncake Store deployment and where
to tune production settings.

## Architecture Overview


![architecture](../image/mooncake-store-preview.png)

**Master Service** (`mooncake_master`): The central coordinator. It manages cluster membership, allocates object storage across client nodes, and enforces eviction/placement policies. Runs as a standalone process.

**Client Node**: Each node contributes DRAM (and optionally VRAM/SSD) to form the distributed cache pool. Clients communicate with the master over RPC for control operations (`Put`/`Get`/`Remove`), but transfer actual data directly between each other via the Transfer Engine — the master is never in the data path.

**Metadata Discovery**: The Transfer Engine needs peer-discovery metadata before clients can exchange data. The recommended starting point is `P2PHANDSHAKE`, which stores handshake metadata locally and does not require an external metadata service.

For a detailed design discussion, see the [Mooncake Store Design](../design/mooncake-store.md).

---

## Quick Start

Deploy a minimal single-node TCP Mooncake Store with the P2P metadata handshake.
No etcd, Redis, or HTTP metadata service is required.

```{figure} ../image/mooncake-store-quickstart-flow.svg
:alt: Mooncake Store Quick Start runtime map
:width: 100%

Runtime map for the Quick Start path: the master handles control RPC while clients discover peers through the selected metadata handshake and exchange data directly.
```

### 1. Start the Master Service

```bash
mooncake_master
```

On success the log shows:

```
Starting Mooncake Master Service
Port: 50051
Max threads: 4
Master service listening on 0.0.0.0:50051
```

The master's default RPC port is `50051`. This Quick Start does not enable the
embedded HTTP metadata server because clients use `P2PHANDSHAKE`.

### 2. Start a Store Client

Use the Python sample program to bring up a client that contributes 3.2 GB of DRAM to the cluster:

```python
# quickstart_store_client.py
import os
from distributed_object_store import DistributedObjectStore

store = DistributedObjectStore()
store.setup(
    local_hostname=os.getenv("LOCAL_HOSTNAME", "localhost"),
    metadata_server=os.getenv("METADATA_ADDR", "P2PHANDSHAKE"),
    global_segment_size=3200 * 1024 * 1024,  # DRAM contributed to the cluster
    local_buffer_size=512 * 1024 * 1024,     # Transfer Engine buffer
    protocol=os.getenv("PROTOCOL", "tcp"),
    device_name=os.getenv("DEVICE_NAME", ""),
    master_server_address=os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
)
```

```bash
python3 quickstart_store_client.py
```

**What just happened:**

1. The client registered itself with the master via RPC.
2. The master allocated a 3.2 GB segment on this node and added it to the cluster's memory pool.
3. The client is now ready to serve `Put`/`Get`/`Remove` requests.

### 3. Verify

```bash
# Health check — master metrics endpoint
curl -s http://localhost:9003/metrics/summary
```

The client should remain running without registration or transfer-engine setup
errors.

## Stress Benchmark

Mooncake Store includes sample programs for validating C++ and Python
integrations. The [stress benchmark script](gh-file:mooncake-store/tests/stress_cluster_benchmark.py)
can be used to verify a two-role prefill/decode setup after the master is
running.

Configure the script for your network:

- `local_hostname`: the local machine's reachable IP address or hostname.
- `metadata_server`: the Transfer Engine metadata service, for example `P2PHANDSHAKE`, `http://127.0.0.1:8080/metadata`, or an etcd address.
- `master_server_address`: the Mooncake Store master address. Use `IP:Port` in default mode, or `etcd://IP:Port;IP:Port;...;IP:Port` in etcd-backed HA mode.

Then start the roles:

```bash
ROLE=prefill python3 mooncake-store/tests/stress_cluster_benchmark.py
ROLE=decode python3 mooncake-store/tests/stress_cluster_benchmark.py
```

For RDMA, topology auto-discovery and NIC filters can be passed through
environment variables:

```bash
ROLE=prefill MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 mooncake-store/tests/stress_cluster_benchmark.py
ROLE=decode MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 mooncake-store/tests/stress_cluster_benchmark.py
```

The absence of errors indicates successful data transfer.

## Metrics Endpoints

The master exposes Prometheus-style metrics on `--metrics_port`:

```bash
# Prometheus format
curl -s http://<master_host>:9003/metrics

# Human-readable summary
curl -s http://<master_host>:9003/metrics/summary
```

---

## Next Steps

### Compose Commands and Runtime Modes

Use the [Configuration Reference](mooncake-store-configuration-reference) for the
master command builder, standalone Real Client deployment modes, startup flags,
and client/engine environment variables. Common first adjustments are
`--rpc_thread_num`, eviction watermarks, and storage-related flags.

### Add Storage Backends

Choose a storage guide when the in-memory quick start is not enough:

- [SSD Offload](ssd-offload): local SSD capacity, eviction policy, and io_uring settings.
- [NVMe-oF SSD Pool](nvmf-ssd-deployment-guide): remote NVMe-oF SSD pool deployment.
- [3FS USRBIO Plugin](../getting_started/plugin-usage/3FS-USRBIO-Plugin): experimental 3FS-backed persistent storage.

### Validate and Observe

- Use [Stress Benchmark](#stress-benchmark) for two-role prefill/decode validation.
- [Observability](../getting_started/observability): metrics collection and production monitoring.

:::{toctree}
:maxdepth: 1
:hidden:

Mooncake Store Configuration Reference<mooncake-store-configuration-reference>
ssd-offload
NvMe-Of SSD Pool<nvmf-ssd-deployment-guide>
HF3FS Plugin (Experimental)<../getting_started/plugin-usage/3FS-USRBIO-Plugin>
../getting_started/observability
:::
