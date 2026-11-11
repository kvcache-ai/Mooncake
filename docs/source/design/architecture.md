# Mooncake Architecture

Mooncake is an open-source, high-performance communication and storage infrastructure designed for large-scale, disaggregated LLM inference. As modern LLM workloads evolve toward long-context reasoning, multimodal processing, and complex agentic applications, traditional monolithic inference architectures face increasing challenges in resource utilization, scalability, and efficiency. Mooncake addresses these challenges by providing a unified infrastructure that enables the decoupling of computation, communication, and storage, supporting flexible resource allocation and independent scaling of different inference stages.

Mooncake operates as a foundational infrastructure layer beneath upper-level inference engines such as vLLM and SGLang. Rather than replacing their model execution and scheduling capabilities, Mooncake complements these engines by providing efficient cross-node data communication, distributed KVCache management, and resource pooling. This architecture supports a wide range of disaggregated inference paradigms, such as Prefill-Decode (PD) disaggregation, Encoder-Prefill-Decode (EPD) disaggregation, and large-scale expert parallelism, allowing inference engines to focus on model computation while leveraging Mooncake for high-performance data movement and shared storage.

Mooncake provides broad support for heterogeneous hardware and deployment environments, including NVIDIA, AMD, Ascend, and various other AI accelerators. It supports multiple high-performance networking technologies, such as RDMA and AWS EFA, and offers flexible deployment options through prebuilt packages, source builds, and Docker containers. This cross-platform compatibility enables Mooncake to serve as a portable and scalable communication and storage infrastructure for large-scale LLM inference across diverse hardware ecosystems.

## Design principles

- **Move data efficiently and ‌reliably.** Mooncake is designed for high-performance, scalable, and reliable data transfer. Leveraging RDMA zero-copy, multi-NIC bandwidth aggregation, and topology-aware routing, it minimizes data movement overhead and maximizes network utilization. Multiple transport protocols and automatic failover further ensure robust communication at scale.
- **Separate data from control.** Large tensors move directly between the processes that own the source and destination buffers. Control services exchange locations, leases, membership, and cache-index metadata.
- **Treat storage and compute as elastic pools.** Store segments can join or leave independently of inference workers, while PG reserves stable rank slots and changes the active membership without renumbering ranks.
- **Compose only the layers a workload needs.** Applications can use Transfer Engine directly, add Mooncake Store for shared persistence, use PG/EP for distributed execution, or combine these capabilities.

## Core components

### Transfer Engine: high-performance data transferring

[Mooncake Transfer Engine](transfer-engine/index.md) is a library embedded in the process that owns or consumes data. It exposes two central abstractions:

- A **Segment** names a remotely accessible address space. A process registers individual buffers within its RAM segment; buffers may reside in DRAM or accelerator memory. NVMe-oF storage is exposed as a persistent segment.
- A **BatchTransfer** groups asynchronous reads or writes across one or more local and remote buffer ranges.

Transfer Engine publishes segment, buffer, and endpoint metadata through a metadata service such as etcd, Redis, or HTTP, or exchanges it peer to peer during connection setup with a P2P handshake (`P2PHANDSHAKE`), which needs no separate metadata service. Peers use that metadata to open a segment, but neither the metadata service nor the handshake carries the transferred payload. The engine selects a transport according to the available build, memory location, and topology. Its transport implementations provide capabilities such as multi-NIC striping, locality-aware path selection, endpoint pooling, and retry over an alternate path.

This layer is usable on its own for prefill/decode KV-cache transfer, multimodal pipeline traffic, hidden-state exchange, and model-weight distribution. It is also the data plane used by Store and PG.

### Mooncake Store: shared KVCache storage

[Mooncake Store](store/mooncake-store.md) turns memory and storage contributed by a cluster of Store clients into a distributed object pool. The pool separates control from data. The Master Service decides object placement and returns replica descriptors, and it does not relay object bytes. Clients issue object operations and transfer slices directly between registered buffers through Transfer Engine. Large objects can be striped across segments and replicated, and slower tiers can extend the pool with local SSD/NVMe or configured distributed/object-storage backends.

- The **Master Service** is the control plane. It owns the object directory, allocation and eviction policy, replica state, leases, tenant quota, and segment membership. It allocates offsets and returns replica descriptors, and it does not carry object payloads. A default deployment has one Master; an HA deployment uses leader election and standby recovery.

- A **Client** issues object operations and moves the corresponding slices directly between registered buffers through Transfer Engine. It can also contribute a registered DRAM or accelerator-memory segment to the pool. A client may be embedded in an inference process, split into dummy and real clients for multi-rank deployments, or run as a standalone Store service.


### Mooncake PG and EP: elastic distributed execution

[Mooncake PG](mooncake-backend-pg.md) is a communication runtime and `torch.distributed` backend. Its data plane performs collective and P2P transfers over Transfer Engine. Its control plane consists of per-process Agents and a Coordinator on global rank 0. The Coordinator tracks process health, connectivity, stable rank slots, and the active membership of each group; workers report failures without independently changing membership.

[Mooncake EP](mooncake-ep.md) provides DeepEP-style dispatch and combine for Mixture-of-Experts inference. It normally bootstraps from a Mooncake PG process group, uses PG state for active-rank awareness, and selects an inter-node RDMA or intra-node device fast path when available. A Python collective fallback is available for unsupported environments. PG recovery and EP routing must be coordinated: after membership changes, the EP buffer refreshes peer metadata before recovered ranks re-enter dispatch and combine.

### Conductor: cache-aware routing

[Mooncake Conductor](conductor/index.md) maintains a global prefix-cache index for gateways and routers. It consumes KV-cache store/remove event streams from inference engines or storage backends, normalizes engine-specific hashes, and answers HTTP queries about the longest reusable prefix on each registered instance, medium, and data-parallel rank.

### Reshard: logical placement planning

[Mooncake Reshard](mooncake-reshard/index.md) is a framework-neutral planning and binding library for moving resources between parallel placements. Its current model-weight contracts describe TP, PP, EP, and DP placement without embedding runtime addresses. Planning produces logical N-dimensional transfer regions; a later binding step attests those regions against immutable runtime buffer snapshots.

## Representative application scenarios

The following are a few representative scenarios:

| Scenario | Typical Mooncake components |
| --- | --- |
| Prefill/decode (PD) disaggregation, encoder-prefill-decode (EPD) disaggregation | Transfer Engine |
| Cross-instance, hierarchical KV-cache reuse | Mooncake Store + Transfer Engine; optionally Conductor for cache-aware routing |
| Draft-model training and inference disaggregation | Transfer Engine + Mooncake Store |
| Elastic MoE inference | Mooncake PG + Mooncake EP + Transfer Engine |
| Model-weight cache | Mooncake Reshard + Transfer Engine + Mooncake Store |

## Repository map

| Directory | Responsibility |
| --- | --- |
| `mooncake-transfer-engine/` | Core C++ transfer engine, transports, topology, metadata plugins, TENT, and language bindings |
| `mooncake-store/` | Store Master, clients, allocators, metadata/HA, storage tiers, and object lifecycle |
| `mooncake-conductor/` | Prefix index, event subscribers, and router-facing HTTP service |
| `mooncake-pg/` | Process-group control plane, collectives, P2P protocol, and PyTorch backend |
| `mooncake-ep/` | Expert-parallel dispatch/combine runtime and device kernels |
| `mooncake-reshard/` | Placement contracts, logical planner, runtime binding, and executors |
| `mooncake-p2p-store/` | Go peer-assisted distribution service built on Transfer Engine |
| `mooncake-integration/` | Python bindings for Transfer Engine, Store, and Conductor |
| `python/mooncake/` | Python APIs, configuration, launchers, async Store, EP wrappers, and utilities |
| `mooncake-wheel/` | Packaged connectors and services, including vLLM integration and the standalone Store service |
| `mooncake-common/` | Shared configuration, RPC, metadata-backend, and utility code |
