# Mooncake Store Quick Start

Mooncake Store is the user-facing distributed KV cache backend for vLLM and
SGLang HiCache. Most users only need to install Mooncake, start
`mooncake_master`, and configure their serving framework to use Mooncake Store.

Transfer Engine is the lower-level transport layer used by these integrations.
If you are developing directly against the Transfer Engine API, see the
[Transfer Engine Python quick start](../design/transfer-engine/index.md#using-transfer-engine-in-your-projects).

## Installation

Install the Mooncake package from PyPI. The same package provides:

- Mooncake Store Python bindings for vLLM and SGLang HiCache integrations.
- Transfer Engine Python bindings and runtime components for direct
  `mooncake.engine.TransferEngine` usage.

**For CUDA-enabled systems:**
```bash
pip install mooncake-transfer-engine
```
📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine/](https://pypi.org/project/mooncake-transfer-engine/)

**For non-CUDA systems:**
```bash
pip install mooncake-transfer-engine-non-cuda
```

📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine-non-cuda/](https://pypi.org/project/mooncake-transfer-engine-non-cuda/)

> **Note**: The CUDA version includes Mooncake-EP and GPU topology detection, requiring CUDA 12.1+. The non-CUDA version is for environments without CUDA dependencies, but it still requires the system runtime libraries used by the transfer stack. On Ubuntu, install them with:
> ```bash
> sudo apt-get update && sudo apt-get install -y libcurl4 libibverbs1 rdma-core librdmacm1 libnuma1 liburing2
> ```

## Start Mooncake Store

For vLLM and SGLang HiCache, start the Mooncake master service first. Many
serving-framework integrations use Mooncake's HTTP metadata endpoint, so the
safest starter command is:

```shell
mooncake_master --enable_http_metadata_server=true
```

The master's default RPC port is `50051`.

If your deployment uses `P2PHANDSHAKE` or an external etcd/Redis metadata
service instead of the embedded HTTP metadata server, the minimal master command
is:

```shell
mooncake_master
```

If the master runs in a container and its IP is dynamic, set
`--rpc_interface=<ifname>` such as `--rpc_interface=eth0`. Mooncake Master will
resolve the current IPv4 address from that interface at startup instead of
relying on a fixed `--rpc_address`.

## Connect vLLM or SGLang

Choose the serving framework path that matches your deployment:

| Framework | Mooncake path | Start here |
|-----------|---------------|------------|
| SGLang | HiCache L3 storage backend with Mooncake Store | [SGLang HiCache Quick Start](examples/sglang-integration/hicache-quick-start.md) |
| vLLM | KV cache storage and sharing with `MooncakeStoreConnector` | [vLLM KV Cache Storage & Sharing](examples/vllm-integration/kv-cache-storage.md) |

SGLang and vLLM also support PD disaggregation paths that use Mooncake Transfer
Engine for direct KV transfer between prefill and decode workers. Those paths
are configured through the serving framework guides, not by calling Transfer
Engine APIs directly:

- [SGLang Integration Overview](examples/sglang-integration/index.md)
- [vLLM Integration Overview](examples/vllm-integration/index.md)

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

See [AI Coding Assistant Skills](../community/ai-coding-assistant-skills.md) for
installation commands and details.

## Next Steps

For production deployment, standalone store services, high availability,
allocation strategies, SSD offload, and runtime tuning, continue to the
[Mooncake Store Deployment & Tuning Guide](../deployment/mooncake-store-deployment-guide.md).

For API details, see the [Mooncake Store Python API](../python-api-reference/mooncake-store.md)
and [Mooncake Store design](../design/mooncake-store.md).
