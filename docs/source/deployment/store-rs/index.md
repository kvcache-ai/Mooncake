# Store-RS Deployment and Operations

Use these guides to build the optional Store-RS component, configure a runtime,
operate tenant policy and routes, and validate deployment scenarios. The
[source-checkout quickstart](../../getting_started/store-rs.md) starts with a
root CMake build and the unified Python wheel.

| Guide | Use it to |
|-------|-----------|
| [Deployment](deployment) | Prepare hosts and assign storage, writer, and reader roles. |
| [Configuration](configuration) | Set client, Python, transport, routing, metadata, and observability options. |
| [Python operations](python) | Run the standalone client and admin commands from the installed wheel. |
| [Multi-tenant isolation](multi-tenant) | Author tenant policy and inspect or repair tenant state. |
| [Route migration](route-migration) | Submit and observe explicit key-level route migration tasks. |
| [Quota validation](quota-validation) | Exercise strict tenant quota admission, refund, and repair behavior. |
| [Rolling upgrades](rolling-upgrade) | Hand off runtime ownership while preserving live data. |
| [NoF operations](nof) | Configure and validate a NoF-backed deployment. |
| [Cold-tier validation](cold-tier) | Review cold-tier validation status and scenario coverage. |
| [Testing and validation](testing) | Run local, integration, and hardware-specific scenarios. |

:::{toctree}
:maxdepth: 1

deployment
configuration
python
multi-tenant
route-migration
quota-validation
rolling-upgrade
nof
cold-tier
testing
:::
