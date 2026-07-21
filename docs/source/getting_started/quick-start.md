# Quick Start

This document describes how to quickly start using Mooncake Transfer Engine and Mooncake Store.

## Before using Mooncake

Install the following prerequisites before running any Mooncake component:
- Python 3.10 or later; a virtual environment is recommended.
- RDMA driver and SDK (for example, Mellanox OFED), if you plan to use RDMA for data transfer.
- CUDA 12.1 or later, if the package is built with `-DUSE_CUDA` (disabled by default). For most CUDA-enabled use cases, such as RDMA-based KV cache transfer between GPUs or between GPU and DRAM, NVIDIA GPUDirect support is also required. *You may install them from [here](https://developer.nvidia.com/cuda-downloads)*.
- Cambricon Neuware, if the package is built with `-DUSE_MLU`. By default Mooncake looks for Neuware under `NEUWARE_HOME` or `/usr/local/neuware`.
- Hygon DTK SDK, if the package is built with `-DUSE_HYGON`. By default Mooncake looks for DTK under `DTK_HOME` or `/opt/dtk`.
- Iluvatar CoreX SDK, if the package is built with `-DUSE_COREX`. By default Mooncake looks for CoreX under `COREX_HOME` or `/usr/local/corex`.

## Installation

Install the Mooncake package from PyPI. The same package provides:

- Mooncake Store Python bindings for vLLM and SGLang HiCache integrations.
- Transfer Engine Python bindings and runtime components for direct
  `mooncake.engine.TransferEngine` usage.

**For CUDA-enabled systems:**

- CUDA < 13.0
```bash
pip install mooncake-transfer-engine
```

- CUDA >= 13.0
```bash
pip install mooncake-transfer-engine-cuda13
```

**For non-CUDA systems:**
```bash
pip install mooncake-transfer-engine-non-cuda
```

**For NPU systems:**
```bash
pip install mooncake-transfer-engine-npu
```

> **Important**:
> - The CUDA version (`mooncake-transfer-engine`) includes Mooncake-EP and GPU topology detection, requiring CUDA 12.1+.
> - The non-CUDA version (`mooncake-transfer-engine-non-cuda`) is for environments without CUDA dependencies, but it still needs system runtime libraries such as `libcurl4`, `libibverbs1`, `rdma-core`, `librdmacm1`, `libnuma1`, and `liburing2` on Ubuntu. In a fresh environment, run `sudo apt-get update` before installing them:
>   ```bash
>   sudo apt-get update && sudo apt-get install -y libcurl4 libibverbs1 rdma-core librdmacm1 libnuma1 liburing2
>   ```
> - MLU support is currently available through source builds with `-DUSE_MLU=ON`; there is no dedicated prebuilt MLU wheel yet.
> - If users encounter problems such as missing `lib*.so`, first install the corresponding system runtime libraries. If the issue persists, uninstall the package and build the binaries manually.

## Connect vLLM or SGLang

Choose the integration path that matches your serving deployment.

### PD Disaggregation

PD disaggregation paths use Mooncake Transfer Engine for direct KV transfer
between prefill and decode workers. Configure these paths through the serving
framework guides, not by calling Transfer Engine APIs directly:

- [SGLang Integration Overview](../integrations/sglang/index.md)
- [vLLM Integration Overview](../integrations/vllm/index.md)

### Mooncake Store

Mooncake Store provides distributed KV cache storage for vLLM and SGLang
HiCache:

| Framework | Use case | Setup guide |
|-----------|----------|-------------|
| SGLang | HiCache L3 storage backend with Mooncake Store | [SGLang HiCache Quick Start](../integrations/sglang/hicache-quick-start.md) |
| vLLM | KV cache storage and sharing with `MooncakeStoreConnector` | [vLLM KV Cache Storage & Sharing](../integrations/vllm/kv-cache-storage.md) |

The serving framework guides include the required Mooncake Store service
startup and connector configuration for each path.

## Optional Python Smoke Test

If you want to verify the Store Python API without a serving framework, run this
single-node `put`/`get` example after starting `mooncake_master`. It uses
`P2PHANDSHAKE`, so no separate Transfer Engine metadata service is required.

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
and [Mooncake Store design](../design/mooncake-store.md).
