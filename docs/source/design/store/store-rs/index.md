# Store-RS Design

These pages describe the Rust-native Store architecture, its components, and
design records. Operational procedures live in the
[Store-RS deployment guides](../../../deployment/store-rs/index.md), while
public language APIs live under the Rust and Python API references.

| Topic | Document |
|-------|----------|
| Runtime architecture and request paths | [Architecture](architecture) |
| Crates and module boundaries | [Components](components) |
| Hot and cold storage design | [Cold tier](cold-tier-design) |
| NoF integration boundaries | [NoF](nof) |
| Tenant control plane | [Multi-tenant admin design](multi-tenant-admin-control-plane) |
| Strict quota accounting | [Tenant quota consistency](tenant-quota-consistency) |

:::{toctree}
:maxdepth: 1

architecture
components
cold-tier-design
nof
multi-tenant-admin-control-plane
tenant-quota-consistency
:::
