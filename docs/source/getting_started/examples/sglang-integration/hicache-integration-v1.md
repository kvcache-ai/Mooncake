# SGLang HiCache with Mooncake Backend

This document describes how to use Mooncake as the storage backend for SGLang HiCache.

## Introduction

### About Mooncake

Mooncake aims to enhance the inference efficiency of large language models (LLMs), especially in slow object storage environments, by constructing a multi-level caching pool on high-speed interconnected DRAM/SSD resources. Compared to traditional caching systems, Mooncake utilizes (GPUDirect) RDMA technology to transfer data directly in a zero-copy manner, while maximizing the use of multi-NIC resources on a single machine.

For more details about Mooncake, please refer to [Mooncake project](https://github.com/kvcache-ai/Mooncake) and [Mooncake documents](https://kvcache-ai.github.io/Mooncake/).

### About SGLang HiCache

SGLang HiCache is a hierarchical KV caching system that extends SGLang's RadixAttention with advanced multi-tier memory management capabilities. It implements a scalable hierarchical storage architecture that spans GPU memory, CPU memory, and external storage layers, delivering significant performance improvements for large language model inference.

HiCache introduces a **HiRadixTree** that acts as a page table for referencing KV caches across different memory tiers: **GPU Memory (L1)**, **CPU Memory (L2)**, **Mooncake and other Storage Backends (L3)**.

The system includes an intelligent cache controller that automatically manages data movement between tiers, implementing optimized prefetching strategies and multiple write policies (write-through, write-through-selective, and write-back).

For more details about SGLang HiCache, please refer to [SGLang project](https://github.com/sgl-project/sglang) and [this blog](https://lmsys.org/blog/2025-09-10-sglang-hicache/).

### Mooncake & SGLang HiCache

Mooncake serves as a high-performance L3 storage backend for SGLang HiCache, enabling distributed KV cache storage across multiple servers with RDMA-accelerated data transfer. This integration addresses the capacity limitations of traditional GPU-only or GPU+CPU caching by providing virtually unlimited cache storage through a distributed memory pool.

When a cache miss occurs in L1 and L2, HiCache automatically fetches the required KV cache from Mooncake's distributed memory pool. The system uses intelligent prefetching strategies to minimize latency, and utilize RDMA technology and zero-copy technique to ensure high-bandwidth, low-latency data transfer between SGLang instances and Mooncake storage nodes.

**Key Advantages:**

- **Scalable Capacity**: Aggregate memory across entire clusters into large distributed pools.
- **Cache Sharing**: KV caches can be shared by all SGLang instances in the cluster.
- **RDMA Acceleration**: Direct memory access eliminates CPU overhead and reduces latency.
- **Zero Copy**: Direct data transfer between L2 and Mooncake without intermediate copying, maximizing throughput.
- **Fault Tolerance**: Distributed architecture provides resilience against individual node failures.

This integration is particularly valuable for production deployments involving long-context models, multi-turn conversations, and high-throughput serving scenarios where traditional caching approaches become capacity-constrained.

## Installation

### Install SGLang

1. Clone SGLang from official repo

```bash
git clone git@github.com:sgl-project/sglang.git
```

2. Build

```bash
cd sglang
pip install --upgrade pip
pip install -e "python[all]"
```

### Install Mooncake

**Method 1: with pip**

```bash
pip install mooncake-transfer-engine
```

**Method 2: from source**

Clone Mooncake project:

```bash
git clone https://github.com/kvcache-ai/Mooncake --recursive
```

Install dependencies:

```bash
cd Mooncake
bash dependencies.sh
```

Build the project. For additional build options, please refer to [the official guide](https://kvcache-ai.github.io/Mooncake/getting_started/build.html).

```bash
mkdir build
cd build
cmake ..
make -j
```

Install Mooncake:

```bash
sudo make install
```

## Deployment

**Mooncake** is a distributed system that efficiently aggregates memory resources across multiple servers. It can also be deployed on a single server for simpler setups.

When integrated with **SGLang**, the system conceptually consists of four key components: `the master service`, `metadata service`, `store service`, and the `SGLang server`. Among them, the `master service` and `metadata service` are responsible for object and metadata maintenance. The `store service` manages a contiguous memory segment that contributes to the distributed KV cache, making its memory accessible to both local and remote `SGLang servers`. Data transfer occurs directly between the `store service` and `SGLang servers`, bypassing the `master service`.

### Single Server Deployment

**Launch Mooncake `metadata service`:**

```bash
python -m mooncake.http_metadata_server
```

**Launch Mooncake `master service`:**

```bash
mooncake_master --eviction_high_watermark_ratio=0.95
```

**Understanding `eviction_high_watermark_ratio`:**

When a `PutStart` request fails due to insufficient memory, or when the eviction thread detects that space usage has reached the configured high watermark ratio, an eviction task is triggered to free up space by evicting a portion of objects.

Due to memory fragmentation, allocation failures may occur even when memory usage has not yet reached 100%. The actual threshold depends on the workload. This [benchmark document](https://kvcache-ai.github.io/Mooncake/performance/allocator-benchmark-result.html)
 provides memory allocation efficiency results under different scenarios. if excessive allocation failures are observed, consider lowering this parameter accordingly.

**Launch Mooncake `store service` (Optional):**

First, create and save a configuration file in JSON format. For example:

```json
{
    "local_hostname": "localhost",
    "metadata_server": "http://localhost:8080/metadata",
    "master_server_address": "localhost:50051",
    "protocol": "rdma",
    "device_name": "mlx5_0,mlx5_1",
    "global_segment_size": 2684354560,
    "local_buffer_size": 0
}
```

Parameter Explanation:

* `local_hostname`: The hostname of the `store service`.
* `metadata_server`: The network address of the `metadata service`. The default port is 8080.
* `master_server_address`: The network address of the `master service`. The default port is 50051.
* `protocol`: The protocol used by the Mooncake. Supported values are `"rdma"` or `"tcp"`. For optimal performance, `"rdma"` is recommended.
* `device_name`: The RDMA devices used by Mooncake. This parameter is required only when the protocol is set to `"rdma"`. Available devices can be listed using the `ibv_devices` command.
* `global_segment_size`: The amount of memory (in bytes) contributed to the global memory pool. A larger value allows Mooncake to cache more KV tensors.
* `local_buffer_size`: Local buffer is used to do request operations such as `Get` or `Put`. In this case, it is set to 0 because the instance functions solely as a storage server, contributing memory to the global pool without issuing any request operations.

Then start the `store service`:

```bash
python -m mooncake.mooncake_store_service --config=[config_path]
```

Note: If `MOONCAKE_GLOBAL_SEGMENT_SIZE` is set to a non-zero value when starting the `SGLang server`, launching the `store service` can be skipped. In this case, the `SGLang server` also takes on the role of the `store service`, which simplifies deployment but couples the two components together. Users can choose the deployment approach that best fits their needs.

**Start the `SGLang server` with Mooncake enabled:**

Mooncake configuration can be provided via environment variables. Note that, for optimal performance, the Mooncake backend currently supports only the `page_first` layout (which optimizes memory access patterns for KV cache operations).

There are two ways to configure Mooncake: 1. Using environment variables; 2. Using extra-config of sglang arguments.

**Using env variables to configure Mooncake**

```bash
MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:8080/metadata" \
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_PROTOCOL="rdma" \
MOONCAKE_DEVICE="mlx5_0,mlx5_1" \
MOONCAKE_GLOBAL_SEGMENT_SIZE=4294967296 \
python -m sglang.launch_server \
    --enable-hierarchical-cache \
    --hicache-storage-backend mooncake\
    --model-path [model_path]
```

Parameter Explanation:

* `MOONCAKE_TE_META_DATA_SERVER`: The network address of the `metadata service`. The default port is 8080.
* `MOONCAKE_MASTER`: The network address of the `master service`. The default port is 50051.
* `MOONCAKE_PROTOCOL`: The protocol used by Mooncake. Supported values are `"rdma"` or `"tcp"`. For optimal performance, `"rdma"` is recommended.
* `MOONCAKE_DEVICE`: The RDMA devices used by Mooncake. This parameter is required only when the protocol is set to `"rdma"`. Available devices can be listed using the `ibv_devices` command.
* `MOONCAKE_GLOBAL_SEGMENT_SIZE`: The amount of memory (in bytes) contributed to the global memory pool. If at least one `store service` is launched, then this value could be set to `0`. In this case, the `SGLang server` will not contribute any memory to the system. Note that KV tensors cached in the contributed memory will be lost once this process terminates; however, this will not cause any system errors.

**Using extra-config of sglang arguments to configure Mooncake**

```bash
python -m sglang.launch_server \
    --enable-hierarchical-cache \
    --hicache-storage-backend mooncake \
    --model-path [model_path] \
    --hicache-storage-backend-extra-config '{"master_server_address": "127.0.0.1:50051", "local_hostname": "localhost", "metadata_server": "http://127.0.0.1:8080/metadata", "global_segment_size": 4294967296, "local_buffer_size": 16777216, "protocol": "rdma", "device_name": "mlx5_0,mlx5_1"}'
```

**Important: Understanding Global Segment Size**

`global_segment_size` for `store service` and `MOONCAKE_GLOBAL_SEGMENT_SIZE` for `SGLang service`: This parameter specifies the amount of memory each instance contributes to the distributed memory pool. The total memory available for KV cache storage across the cluster is the sum of the memory contributed by all instances.

Adjust this value according to system’s available memory and expected cache requirements.

**HiCache Related Parameters for SGLang Server**

The following parameters control SGLang HiCache behavior when using Mooncake as the storage backend:

- **`--enable-hierarchical-cache`**: Enable hierarchical cache functionality. This is required to use HiCache with Mooncake.

- **`--hicache-ratio HICACHE_RATIO`**: The ratio of the size of host KV cache memory pool to the size of device pool. For example, setting this to 2 means the host memory pool will be twice the size of the device memory pool.

- **`--hicache-size HICACHE_SIZE`**: The size of host KV cache memory pool in gigabytes. This parameter overrides `hicache-ratio` if set. For example, `--hicache-size 30` allocates 30GB for the host memory pool.

- **`--hicache-write-policy {write_back,write_through,write_through_selective}`**: Controls how data is written from faster to slower memory tiers:
  - `write_through`: Immediately writes data to all tiers (strongest caching benefits)
  - `write_through_selective`: Uses hit-count tracking to back up only frequently accessed data
  - `write_back`: Writes data back to slower tiers only when eviction is needed (reduces I/O load)

- **`--hicache-io-backend {direct,kernel}`**: Choose the I/O backend for KV cache transfer between CPU and GPU:
  - `direct`: Standard CUDA memory copy operations
  - `kernel`: GPU-assisted I/O kernels (recommended for better performance)

- **`--hicache-mem-layout {layer_first,page_first}`**: Memory layout for the host memory pool:
  - `layer_first`: Compatible with GPU computation kernels (default for GPU memory)
  - `page_first`: Optimized for I/O efficiency (required if use Mooncake backend)

- **`--hicache-storage-backend {file,mooncake,hf3fs,nixl}`**: Choose the storage backend for the L3 tier

- **`--hicache-storage-prefetch-policy {best_effort,wait_complete,timeout}`**: Control when prefetching from storage should stop:
  - `best_effort`: Prefetch as much as possible without blocking
  - `wait_complete`: Wait for prefetch to complete before proceeding
  - `timeout`: Use timeout-based prefetching (recommended for Mooncake)

- **`--hicache-storage-backend-extra-config HICACHE_STORAGE_BACKEND_EXTRA_CONFIG`**: JSON string containing extra configuration for the storage backend. For Mooncake, this can include parameters like `master_server_address`, `local_hostname`, `metadata_server`, etc.

### Distributed Deployment

Distributed deployment of Mooncake is straightforward. Similar to the single-node setup, start one `metadata service` and one `master service` for this cluster. Then start a `store service` on each server.

Mooncake also supports high availability mode. This mode enhances fault tolerance by running the `master service` as a cluster of multiple master nodes coordinated through an `etcd` cluster. The master nodes use `etcd` to elect a leader, which is responsible for handling client requests. For more details about how to deploy in this mode, please refer to our [documents](https://kvcache-ai.github.io/Mooncake/).

## Troubleshooting

**RDMA Registration Failure**:

* In some environments, RDMA registration may require root privileges. In this case, try running the program as root.
* In certain environments (e.g., eRDMA), there is an upper limit on the total amount of RDMA memory that can be registered. Once this limit is exceeded, registration will fail. To resolve this, you can lower the value of `MOONCAKE_GLOBAL_SEGMENT_SIZE`, or reduce the host memory allocated to HiCache in the `SGLang server` (since this memory is fully registered with RDMA to enable zero-copy).

## Test Mooncake Store

This test is intended for developers to quickly verify that the MooncakeStore class interfaces are functioning correctly.

First, start the `metadata service` and `master service`. Then run the `test_mooncake_store.py`. 16MB global segments size is enough to run this test.

```bash
MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:8080/metadata" \
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_PROTOCOL="rdma" \
MOONCAKE_DEVICE="mlx5_0,mlx5_1" \
MOONCAKE_GLOBAL_SEGMENT_SIZE=16777216 \
python3 [path of test_mooncake_store.py]
```

If all tests pass, the message "✅ All tests passed" will be printed at the end.