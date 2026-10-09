# Quick Start

Get up and running with Mooncake in minutes.

This guide walks you through the entire flow of getting started with Mooncake:

1. **Install** Mooncake
2. **Start** the Store master and **Send a request** with the Python Store API

Serving-framework users can then connect SGLang, vLLM or other systems.

## Prerequisites

- **Python**: 3.10 or later; a virtual environment is recommended.
- **RDMA**: an RDMA driver and SDK (for example, Mellanox OFED), if you plan to use RDMA for data transfer. On ScaleFabric SHCA systems, install `shca-tools` and build with `-DUSE_SHCA=ON`.
- **CUDA**: 12.1 or later. For most CUDA-enabled use cases, such as RDMA-based KV cache transfer between GPUs or between GPU and DRAM, NVIDIA GPUDirect support is also required. You may install CUDA from [the NVIDIA downloads page](https://developer.nvidia.com/cuda-downloads).

```{note}
The default pip, build and Docker paths target NVIDIA CUDA. For other
platforms, see [Other Platforms](#other-platforms) below.
```


## Installation

The same package provides:

- Mooncake Store Python bindings for vLLM and SGLang HiCache integrations.
- Transfer Engine Python bindings and runtime components for direct
  `mooncake.engine.TransferEngine` usage.

::::{tab-set}

:::{tab-item} pip / uv
We recommend using **uv** for faster installation:

```bash
pip install --upgrade pip
pip install uv
uv pip install mooncake-transfer-engine
```

Plain `pip` also works:

```bash
pip install mooncake-transfer-engine
```

```{tip}
The default wheel targets CUDA 12.1–12.9 and includes Mooncake-EP and GPU
topology detection. For CUDA 13.0/13.1, install
`mooncake-transfer-engine-cuda13` instead.
```
:::

:::{tab-item} From Source
Clone the repository and build the default configuration:

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh

mkdir build
cd build
cmake ..
make -j
sudo make install
```

For CUDA, VRAM segments, NVMe-oF, and other backend flags, see the
[Build Guide](build.md).
:::

:::{tab-item} Docker
Published images are available on Docker Hub at
[kvcacheai/mooncake](https://hub.docker.com/r/kvcacheai/mooncake).

```bash
docker run --net=host \
    --ipc=host \
    --ulimit memlock=-1 \
    kvcacheai/mooncake:latest \
    mooncake_master
```

For details, see
[Use Mooncake in Docker Containers](build.md#use-mooncake-in-docker-containers).
:::

::::

```{note}
If users encounter problems such as missing `lib*.so`, first install the
corresponding system runtime libraries. If the issue persists, uninstall the
package and [build the binaries manually](build.md).
```

## Other Platforms

The default path above targets NVIDIA CUDA. Use the matching wheel or source
build for other platforms. Install only one variant in an environment.

::::{tab-set}

:::{tab-item} Non-CUDA
**Prerequisites**

- Python 3.10 or later.
- Ubuntu runtime libraries: `libcurl4`, `libibverbs1`, `rdma-core`,
  `librdmacm1`, `libnuma1`, and `liburing2`.

**Installation**

```bash
sudo apt-get update && sudo apt-get install -y \
  libcurl4 libibverbs1 rdma-core librdmacm1 libnuma1 liburing2
pip install mooncake-transfer-engine-non-cuda
```
:::

:::{tab-item} Ascend NPU
**Prerequisites**

- Python 3.10 or later.
- Ascend CANN Toolkit. Source `/usr/local/Ascend/cann/set_env.sh` before
  running Mooncake. Ascend Direct (ADXL/HIXL) is the recommended path.

**Installation**

```bash
pip install mooncake-transfer-engine-npu
source /usr/local/Ascend/cann/set_env.sh
```

See [Ascend Direct Transport](../design/transfer-engine/transport/ascend_direct_transport.md)
for the recommended path. The legacy backend is documented in
[Ascend Transport](../design/transfer-engine/transport/ascend_transport.md).
For mixed GPU/NPU transfers, see
[Heterogeneous Ascend Transport](../design/transfer-engine/transport/heterogeneous_ascend.md).
There are also two detailed Chinese guides:
[Mooncake KVPool guide](https://gitcode.com/cann/hixl/wiki/Mooncake%20KVPool%E6%8C%87%E5%8D%97.md)
and
[Mooncake NPU guide](https://gitcode.com/cann/hixl/wiki/Mooncake%EF%BC%88NPU%20%E7%89%88%EF%BC%89%E5%AE%8C%E6%95%B4%E6%8C%87%E5%8D%97.md).
:::

:::{tab-item} AMD ROCm
**Prerequisites**

- Python 3.10 or later.
- ROCm / HIP SDK, with `hipcc` and runtime libraries on `PATH` (for example
  `/opt/rocm`).

**Installation**

```bash
pip install mooncake-transfer-engine-rocm
```
:::

:::{tab-item} Moore Threads MUSA
**Prerequisites**

- Python 3.10 or later.
- MUSA SDK. Add `/usr/local/musa/lib` to `LIBRARY_PATH` and `LD_LIBRARY_PATH`.
- `mthreads-peermem` for GPUDirect RDMA.

**Installation**

```bash
pip install mooncake-transfer-engine-musa
```
:::

:::{tab-item} AWS EFA
**Prerequisites**

- An AWS instance with EFA (for example p5 or p6).
- AWS EFA driver and libfabric. Verify with `fi_info -p efa`, and keep
  `/opt/amazon/efa/lib` on `LD_LIBRARY_PATH`.
- CUDA 12.1–12.9 or CUDA 13 if you use the GPU-aware EFA wheels.

**Installation**

```bash
# GPU memory transfers with CUDA 12
pip install mooncake-transfer-engine-efa

# GPU memory transfers with CUDA 13
pip install mooncake-transfer-engine-efa-cuda13

# CPU/DRAM-only transfers
pip install mooncake-transfer-engine-efa-non-cuda
```

See the [EFA transport guide](../design/transfer-engine/transport/efa_transport.md)
for prerequisites and configuration.
:::

:::{tab-item} Cambricon MLU
**Prerequisites**

- Python 3.10 or later.
- Cambricon Neuware SDK. Set `NEUWARE_HOME`, or use the default
  `/usr/local/neuware`. There is no dedicated prebuilt MLU wheel yet.

**Installation**

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh
mkdir build && cd build
cmake .. -DUSE_MLU=ON
make -j
sudo make install
```
:::

:::{tab-item} MetaX MACA
**Prerequisites**

- Python 3.10 or later.
- MACA SDK. Set `MACA_HOME`, or use the default `/opt/maca`.

**Installation**

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh
mkdir build && cd build
cmake .. -DUSE_MACA=ON
make -j
sudo make install
```
:::

:::{tab-item} Hygon DCU
**Prerequisites**

- Python 3.10 or later.
- Hygon DTK SDK. Set `DTK_HOME`, or use the default `/opt/dtk`.

**Installation**

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh
mkdir build && cd build
cmake .. -DUSE_HYGON=ON
make -j
sudo make install
```
:::

:::{tab-item} Iluvatar CoreX
**Prerequisites**

- Python 3.10 or later.
- Iluvatar CoreX SDK. Set `COREX_HOME`, or use the default `/usr/local/corex`.

**Installation**

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh
mkdir build && cd build
cmake .. -DUSE_COREX=ON
make -j
sudo make install
```
:::

:::{tab-item} Biren GPU
**Prerequisites**

- Python 3.10 or later.
- Biren SUPA SDK. Set `BIREN_HOME` to the SDK root containing `supa/include`
  and `supa/lib`, or use the default `/usr/local/birensupa/all/latest`.

**Installation**

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo bash dependencies.sh
mkdir build && cd build
cmake .. -DUSE_SUPA=ON -DBIREN_HOME=/usr/local/birensupa/all/latest
make -j
sudo make install
```
:::

::::

## Start Mooncake Store

If you installed with pip or from source, start the master service:

```bash
mooncake_master
```

Wait until you see a line like this in the logs:

```
Master service started on port 50051, max_threads=4, ...
```

The default RPC port is `50051`. Skip this step if the Docker command above is
already running `mooncake_master`.

## Send Your First Request

Run this single-node `put`/`get` example after `mooncake_master` is running. This example uses `P2PHANDSHAKE`, so no separate Transfer Engine metadata service is required.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="localhost",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=512 * 1024 * 1024,
    local_buffer_size=128 * 1024 * 1024,
    protocol="tcp",
    rdma_devices="",
    master_server_addr="127.0.0.1:50051",
)

store.put("hello_key", b"Hello, Mooncake Store!")

data = store.get("hello_key")
print(data.decode())  # Output: Hello, Mooncake Store!

store.close()
```

## Connect vLLM or SGLang

Choose the integration path that matches your serving deployment.

### PD Disaggregation

PD disaggregation paths use Mooncake Transfer Engine for direct KV transfer
between prefill and decode workers. Configure these paths through the serving
framework guides, not by calling Transfer Engine APIs directly:

- [SGLang Disaggregated Serving with MooncakeTransferEngine](../deployment/integrations/sglang/pd-disaggregation.md)
- [Disaggregated Prefill-Decode with MooncakeConnector](../deployment/integrations/vllm/disagg-prefill-decode.md)

### Distributed KV Cache Pooling

Mooncake Store provides distributed KV cache storage for vLLM and SGLang
HiCache:

| Framework | Use case | Setup guide |
|-----------|----------|-------------|
| SGLang | HiCache L3 storage backend with Mooncake Store | [SGLang HiCache Quick Start](../deployment/integrations/sglang/hicache-quick-start.md) |
| vLLM | KV cache storage and sharing with `MooncakeStoreConnector` | [vLLM KV Cache Storage & Sharing](../deployment/integrations/vllm/kv-cache-storage.md) |

## AI Coding Assistant Skills

If you use Claude Code or another coding assistant that supports reusable
skills, Mooncake provides built-in playbooks for common development tasks:

| Skill | Use it for |
|-------|------------|
| `/mooncake-troubleshoot` | Diagnose services, RDMA, environment variables, and runtime logs. |
| `/mooncake-ci-local` | Run pre-PR local validation with Mooncake's CI script. |
| `/mooncake-api` | Work with Mooncake Store, Transfer Engine, and EP/Backend Python APIs. |

Install them from the Claude Code plugin marketplace without cloning the full
repository:

```text
/plugin marketplace add kvcache-ai/Mooncake --sparse .claude-plugin
/plugin install mooncake-troubleshoot@mooncake
/plugin install mooncake-ci-local@mooncake
/plugin install mooncake-api@mooncake
```

## Next Steps

For production deployment, standalone store services, high availability,
allocation strategies, SSD offload, and runtime tuning, continue to the
[Mooncake Store Deployment & Tuning Guide](../deployment/mooncake-store-deployment-guide.md).

For API details, see the [Mooncake Store Python API](../api-reference/python/mooncake-store.md)
and [Mooncake Store design](../design/store/mooncake-store.md).
