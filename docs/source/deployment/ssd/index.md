# SSD Storage

Mooncake Store supports both node-local SSD offload and shared NVMe-over-Fabrics
storage pools. Choose the guide that matches the storage tier in your deployment.

| Storage option | Description |
|----------------|-------------|
| [SSD Offload](ssd-offload) | Configure local SSD offload, eviction, and I/O behavior. |
| [NVMe KV Local-Disk Backend](nvme-kv) | Configure a local NVMe KV namespace for SSD offload. |
| [NVMe-oF SSD Pool](nvmf-ssd-deployment-guide) | Configure a shared NVMe-over-Fabrics storage tier. |

:::{toctree}
:maxdepth: 1
:hidden:

ssd-offload
nvme-kv
nvmf-ssd-deployment-guide
:::
