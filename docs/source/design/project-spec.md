# Project Specification: Mooncake

<!--
  Structured sections use XML for machine-readability and self-description.
  Code samples use fenced code blocks. Simple lookups use Markdown tables.
-->

---

## Chain of Thought — Task Understanding

<cot>

  <understanding>
    Mooncake is a KVCache-centric disaggregated LLM inference platform. Its primary goal is
    to separate the prefill and decode phases of transformer inference across different nodes,
    while managing multi-tier memory (GPU VRAM → host DRAM → NVMe SSD → CXL) through a
    unified high-performance transfer engine. The codebase is a C++20 / Python monorepo with
    13+ pluggable network transports, a distributed KVCache store (master/replica with optional
    etcd HA), CUDA/HIP extensions for Expert and Pipeline parallelism, and official vLLM v1
    integration. The spec must accurately reflect this as ground truth for onboarding,
    tooling, and AI-assisted development.
  </understanding>

  <conflicts>
    <conflict id="C1">
      <description>
        Dual Transfer Engine: the repository contains both the original Transfer Engine
        (mooncake-transfer-engine, the stable public API) and a successor "TENT"
        (mooncake-transfer-engine/tent, enabled via USE_TENT=OFF by default). The Python
        package and documentation present a single TransferEngine class, but internally
        two implementations coexist. Users integrating at the C++ level must be aware of
        which implementation is active.
      </description>
      <resolution>
        This spec documents the stable, default TransferEngine (USE_TENT=OFF). TENT is
        noted as an emerging alternative but its API is not guaranteed stable.
      </resolution>
    </conflict>
    <conflict id="C2">
      <description>
        PyPI package name mismatch: the Python package published to PyPI is named
        "mooncake-transfer-engine" (v0.3.10) but it ships CLI tools and modules for the
        store, Expert Parallelism (EP), Pipeline Gradient (PG), and metadata server —
        far beyond just the transfer engine.
      </description>
      <resolution>
        The PyPI name is a legacy artifact. Internally, the package is imported as
        "mooncake" and covers the full platform surface.
      </resolution>
    </conflict>
    <conflict id="C3">
      <description>
        Two access paths to the Store: MasterClient uses coro_rpc (yalantinglibs, port
        50051 by default) for high-performance C++ callers, while mc_store_rest_server
        exposes the same store over HTTP for non-C++ callers. The REST server's port and
        authentication surface are not covered by the same HA/etcd guarantees.
      </description>
      <resolution>
        Both paths are valid and documented. HA/etcd only protects the coro_rpc path.
        REST callers should treat the REST server as a stateless proxy.
      </resolution>
    </conflict>
  </conflicts>

  <analysis>
    <step n="1">Identify languages and versioned dependencies from pyproject.toml,
      CMakeLists.txt, dependencies.sh, and requirements-dev.txt.</step>
    <step n="2">Map the layered monorepo structure and determine build dependencies
      between mooncake-* subprojects.</step>
    <step n="3">Trace the two primary data flows: (a) disaggregated prefill/decode via
      vLLM and (b) explicit KVCache Put/Get via MasterClient.</step>
    <step n="4">Extract public API signatures from header files and the Python connector,
      annotating inputs, outputs, and error conventions.</step>
    <step n="5">Enumerate enforced coding style rules from .clang-format, ruff config,
      and pre-commit hooks.</step>
  </analysis>

</cot>

---

## 1. Core Technology Stack

### 1.1 Programming Languages

| Language | Role | Standard / Version |
|----------|------|--------------------|
| **C++** | Core engine, transports, storage, RPC | C++20 |
| **Python** | Bindings, CLI tools, vLLM connectors, REST APIs | ≥ 3.8 |
| **CUDA / HIP** | GPU memory kernels (EP/PG extensions) | CUDA ≤ 12.9 (primary), 13.x (variant) |
| **Go** | etcd wrapper library | 1.23.8 |
| **CMake** | Build system | ≥ 3.16 |

---

### 1.2 Key Libraries & Versions

<dependencies>

  <group name="C++ / System">
    <dep name="yalantinglibs"   version="0.5.7"    source="dependencies.sh"
         role="Async RPC (coro_rpc), coroutine I/O (coro_io), JSON reflection"/>
    <dep name="pybind11"        version="bundled"  source="extern/pybind11"
         role="C++ ↔ Python bindings"/>
    <dep name="ASIO"            version="bundled"  source="mooncake-asio"
         flags="ASIO_SEPARATE_COMPILATION ASIO_DYN_LINK"
         role="Async I/O, used by yalantinglibs socket layer"/>
    <dep name="JsonCpp"         version="system"   source="mooncake-common/FindJsonCpp.cmake"
         role="JSON configuration parsing"/>
    <dep name="GLOG"            version="system"   source="mooncake-common/FindGLOG.cmake"
         role="Structured logging (LOG macros)"/>
    <dep name="libibverbs"      version="system"   source="apt"
         role="RDMA verbs (InfiniBand, RoCEv2, eRDMA, GPUDirect)"/>
    <dep name="libmlx5"         version="system"   source="apt"
         role="Mellanox/Nvidia RDMA provider"/>
    <dep name="libnuma"         version="system"   source="apt"
         role="NUMA-aware CPU and memory binding"/>
    <dep name="Boost"           version="system"   source="apt"
         role="boost::functional/hash used in MasterClient connection pools"/>
    <dep name="etcd"            version="v3.6.1"   source="CI / optional runtime"
         role="HA metadata backend for mooncake-store"/>
    <dep name="CacheLib"        version="bundled"  source="mooncake-store/include/cachelib_memory_allocator"
         role="Custom slab/buddy allocator for KVCache segments"/>
    <dep name="tl::expected"    version="bundled"  source="mooncake-store/include"
         role="Functional error-return type (Store API return convention)"/>
  </group>

  <group name="Python">
    <dep name="aiohttp"         version="any"   source="pyproject.toml"
         role="Async HTTP client/server for metadata and REST APIs"/>
    <dep name="requests"        version="any"   source="pyproject.toml"
         role="Synchronous HTTP client for metadata operations"/>
    <dep name="setuptools"      version=">=61.0.0" source="pyproject.toml"
         role="Package build backend"/>
    <dep name="wheel"           version=">=0.37.0" source="pyproject.toml"
         role="Wheel packaging"/>
    <dep name="torch"           version="optional" source="mooncake-ep/setup.py mooncake-pg/setup.py"
         role="Required only when building EP/PG CUDA extensions"/>
  </group>

  <group name="Development Tools">
    <dep name="pre-commit"      version="3.7.1"  source="requirements-dev.txt"
         role="Git hook runner"/>
    <dep name="ruff"            version="0.6.9"  source="requirements-dev.txt"
         role="Python linting and formatting (replaces black + isort)"/>
    <dep name="codespell"       version="2.2.6"  source="requirements-dev.txt"
         role="Spell checking across all source files"/>
    <dep name="cmake-format"    version="0.6.13" source="requirements-dev.txt"
         role="CMake file formatting"/>
    <dep name="clang-format"    version="system" source="apt"
         role="C++ code formatting (Google style, 4-space indent)"/>
  </group>

</dependencies>

---

## 2. Architecture Patterns

### 2.1 Monorepo with Layered Components

The repository is a **monorepo**. Each `mooncake-*` directory is an independently buildable
CMake sub-project. The components form a clean layered architecture where higher layers depend
on lower ones but not vice-versa.

<architecture>

  <layer name="Integration" description="External framework connectors and Python API surface">
    <component id="mooncake-integration" role="vLLM (v0.1/v1), LMCache, SGLang connectors"/>
    <component id="mooncake-wheel"       role="Python package v0.3.10 — CLI entry points and pybind11 bindings"/>
  </layer>

  <layer name="Compute Parallelism" description="GPU-accelerated parallelism extensions">
    <component id="mooncake-ep" role="Expert Parallelism CUDA extension (MoE via IBGDA/MLX5 GDA)"/>
    <component id="mooncake-pg" role="Pipeline Gradient parallelism CUDA extension"/>
    <component id="mooncake-rl" role="Reinforcement-learning training support utilities"/>
  </layer>

  <layer name="Storage" description="Distributed KVCache object stores">
    <component id="mooncake-store"     role="Distributed KVCache store — master/replica, eviction, allocation, optional etcd HA (v2.0.0)"/>
    <component id="mooncake-p2p-store" role="Lightweight peer-to-peer object store (no master node)"/>
  </layer>

  <layer name="Transfer" description="Unified multi-protocol data movement">
    <component id="mooncake-transfer-engine" role="Stable Transfer Engine API (13+ transport plugins)"/>
    <component id="mooncake-transfer-engine/tent" role="TENT — next-generation Transfer Engine (USE_TENT=OFF by default; API not yet stable)"/>
  </layer>

  <layer name="Foundation" description="Shared utilities, async I/O, CMake helpers">
    <component id="mooncake-common" role="Shared CMake helpers (FindJsonCpp, FindGLOG), etcd wrapper"/>
    <component id="mooncake-asio"   role="Compiled ASIO shared library"/>
    <component id="extern/pybind11" role="Bundled pybind11 for Python C++ bindings"/>
  </layer>

</architecture>

---

### 2.2 Transport Plugin Architecture

The Transfer Engine applies the **strategy/plugin pattern**. Each network technology is
a `Transport` subclass registered at runtime via `installTransport(proto, args)`.

<transports>
  <transport id="tcp"                    protocols="TCP/IP"                         hardware="Any NIC"/>
  <transport id="rdma"                   protocols="IB, RoCEv2, eRDMA, GPUDirect"  hardware="InfiniBand / Mellanox / Nvidia"/>
  <transport id="nvlink"                 protocols="NVLink (inter-node)"            hardware="Nvidia NVLink"/>
  <transport id="intranode_nvlink"       protocols="NVLink (intra-node)"            hardware="Nvidia NVLink"/>
  <transport id="hip"                    protocols="HIP"                            hardware="AMD ROCm GPU"/>
  <transport id="cxl"                    protocols="CXL"                            hardware="CXL memory expanders"/>
  <transport id="nvmeof"                 protocols="NVMe-oF"                        hardware="NVMe-over-Fabrics targets"/>
  <transport id="efa"                    protocols="AWS EFA"                        hardware="AWS Elastic Fabric Adapter"/>
  <transport id="barex"                  protocols="Direct DRAM"                    hardware="Host DRAM"/>
  <transport id="ascend"                 protocols="Ascend device transfers"        hardware="Huawei Ascend NPU"/>
  <transport id="hccl"                   protocols="HCCL AllReduce"                 hardware="Huawei Ascend NPU"/>
  <transport id="ascend_direct"          protocols="Ascend direct P2P"              hardware="Huawei Ascend NPU"/>
  <transport id="ubshmem"               protocols="Shared memory"                  hardware="Huawei NPU / host"/>
  <transport id="heterogeneous_rdma"    protocols="Heterogeneous RDMA"             hardware="Mixed CPU+Ascend RDMA"/>
</transports>

---

### 2.3 Distributed Store: Master / Replica

`mooncake-store` follows a **master–replica** model:

- A single **MasterService** manages object metadata, placement, allocation strategy,
  eviction policy, and HA via optional **etcd** backend.
- **MasterClient** connects to the master over `coro_rpc` (yalantinglibs, default port 50051)
  to perform Put / Get / Delete / Exist operations.
- Replicas hold actual KVCache data buffers in DRAM or on NVMe.
- A **REST API** (`mc_store_rest_server`) exposes the store to non-C++ callers as a
  stateless HTTP proxy (not covered by etcd HA guarantees — see [conflict C3](#conflicts)).

---

## 3. Data Flow

### 3.1 Disaggregated Inference (Prefill → Decode)

<dataflow name="Disaggregated Prefill / Decode">
  <step n="1" actor="User"                 output="Inference request"/>
  <step n="2" actor="vLLM / SGLang Scheduler"
              input="Inference request"    output="KVConnectorMetadata"/>
  <step n="3" actor="MooncakeConnector"
              input="KVConnectorMetadata"  output="TransferRequest batches"
              call="build_connector_meta() → submitTransfer()"/>
  <step n="4" actor="TransferEngine"
              input="TransferRequest batches"
              output="Selected Transport + DMA operations"
              call="submitTransfer(batch_id, entries)"/>
  <step n="5" actor="Transport Plugin"
              input="DMA operations"
              output="KV tensor bytes moved"
              note="Parallel: RDMA/NVLink/TCP chosen per topology"/>
  <step n="6" from="Prefill node VRAM/DRAM" to="Decode node VRAM/DRAM"
              mechanism="DMA / network transfer"/>
  <step n="7" actor="MooncakeConnector"
              call="finish_async_transfer()"
              output="Signal to scheduler that KV tensors are ready"/>
  <step n="8" actor="Decode worker"
              input="KV tensors in local VRAM"
              output="Generated tokens"/>
</dataflow>

---

### 3.2 KVCache Store (Put / Get)

<dataflow name="KVCache Store Put">
  <step n="1" actor="Client (Python / C++)"   call="Put(object_key, value, replica_list)"/>
  <step n="2" actor="MasterClient"             call="RPC → MasterService"/>
  <step n="3" actor="MasterService"            action="AllocationStrategy selects segments; EvictionStrategy frees space if needed"/>
  <step n="4" actor="ReplicaManager"           call="TransferEngine.submitTransfer()"/>
  <step n="5" actor="Segment buffers"          note="DRAM / NVMe / CXL target"/>
</dataflow>

<dataflow name="KVCache Store Get">
  <step n="1" actor="Client (Python / C++)"   call="Get(object_key)"/>
  <step n="2" actor="MasterClient"             call="RPC → MasterService"/>
  <step n="3" actor="MasterService"            action="Resolves replica locations"/>
  <step n="4" actor="TransferEngine"           call="submitTransfer() — pull data to caller's buffer"/>
</dataflow>

---

### 3.3 Python Binding Data Path

<dataflow name="Python Binding">
  <step n="1" actor="Python caller"/>
  <step n="2" actor="mooncake Python package (mooncake-wheel)"
              note="Pure Python modules + _mooncake.so (pybind11 extension)"/>
  <step n="3" actor="C++ TransferEngine / MasterClient"/>
  <step n="4" actor="Transport Plugins / Store backend"/>
</dataflow>

---

## 4. API Contracts / Key Function Signatures

### 4.1 Transfer Engine (C++) — `mooncake-transfer-engine/include/transfer_engine.h`

<api class="TransferEngine" namespace="mooncake"
     file="mooncake-transfer-engine/include/transfer_engine.h"
     note="Stable public API. TENT variant is separate (USE_TENT=OFF by default).">

  <type-aliases>
    <alias name="TransferRequest"    from="Transport::TransferRequest"/>
    <alias name="TransferStatus"     from="Transport::TransferStatus"/>
    <alias name="TransferStatusEnum" from="Transport::TransferStatusEnum"/>
    <alias name="SegmentHandle"      from="Transport::SegmentHandle"/>
    <alias name="SegmentID"          from="Transport::SegmentID"/>
    <alias name="BatchID"            from="Transport::BatchID"
           note="uint64_t; INVALID_BATCH_ID = UINT64_MAX"/>
    <alias name="BufferEntry"        from="Transport::BufferEntry"/>
  </type-aliases>

</api>

```cpp
namespace mooncake {

class TransferEngine {
public:
    // Construction — auto_discover probes hardware and installs matching transports
    explicit TransferEngine(bool auto_discover = false);
    TransferEngine(bool auto_discover, const std::vector<std::string>& filter);
    ~TransferEngine();

    // Initialization — must be called before any other method
    int init(const std::string& metadata_conn_string,   // e.g. "etcd://127.0.0.1:2379"
             const std::string& local_server_name,       // unique node identifier
             const std::string& ip_or_host_name = "",   // advertised address
             uint64_t rpc_port = 12345);                 // RPC listener port
    int freeEngine();

    // Transport management
    Transport* installTransport(const std::string& proto, void** args);
    int        uninstallTransport(const std::string& proto);

    // Segment (remote memory region) management
    SegmentHandle openSegment(const std::string& segment_name);
    int           closeSegment(SegmentHandle handle);
    int           removeLocalSegment(const std::string& segment_name);
    Status        CheckSegmentStatus(SegmentID sid);

    // Local memory registration (must precede DMA use)
    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location = kWildcardLocation,
                            bool remote_accessible = true,
                            bool update_metadata  = true);
    int unregisterLocalMemory(void* addr, bool update_metadata = true);
    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location);
    int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list);

    // Batched transfer operations
    BatchID allocateBatchID(size_t batch_size);
    Status  freeBatchID(BatchID batch_id);
    Status  submitTransfer(BatchID batch_id,
                           const std::vector<TransferRequest>& entries);
    Status  submitTransferWithNotify(
                BatchID batch_id,
                const std::vector<TransferRequest>& entries,
                TransferMetadata::NotifyDesc notify_msg);
    Status  getTransferStatus(BatchID batch_id, size_t task_id,
                              TransferStatus& status);

    // Peer notification (used for prefill→decode signalling)
    int getNotifies(std::vector<TransferMetadata::NotifyDesc>& notifies);
    int sendNotifyByID(SegmentID target_id,
                       TransferMetadata::NotifyDesc notify_msg);
    int sendNotifyByName(std::string remote_agent,
                         TransferMetadata::NotifyDesc notify_msg);

    // Network introspection
    std::string getLocalIpAndPort();
    int         getRpcPort();
};

} // namespace mooncake
```

---

### 4.2 Mooncake Store — Master Client (C++) — `mooncake-store/include/master_client.h`

<api class="MasterClient" namespace="mooncake"
     file="mooncake-store/include/master_client.h"
     default-addr="localhost:50051"
     error-convention="tl::expected[T, ErrorCode] or bare ErrorCode; [[nodiscard]] enforced"
     thread-safety="Non-copyable; one MasterClient instance per logical client context">
</api>

```cpp
namespace mooncake {

class MasterClient {
public:
    explicit MasterClient(const UUID& client_id,
                          MasterClientMetric* metrics = nullptr);
    ~MasterClient();
    MasterClient(const MasterClient&)            = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    // Connect to the master service (call once before any operation)
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = "localhost:50051");

    // Key existence checks
    [[nodiscard]] tl::expected<bool, ErrorCode>
        ExistKey(const std::string& object_key);
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>>
        BatchExistKey(const std::vector<std::string>& object_keys);

    // Put / Get / Delete — all return tl::expected<T, ErrorCode> or ErrorCode
    // (see full header for complete signatures)
};

} // namespace mooncake
```

---

### 4.3 Python vLLM v1 Connector — `mooncake-wheel/mooncake/mooncake_connector_v1.py`

<api class="MooncakeConnector" lang="python"
     base="KVConnectorBase_V1"
     file="mooncake-wheel/mooncake/mooncake_connector_v1.py">

  <method-group name="Scheduler-side">
    <method name="get_num_new_matched_tokens"
            returns="tuple[int, bool]"
            params="request: SchedulerRequest, num_computed_tokens: int"/>
    <method name="update_state_after_alloc"
            returns="None"
            params="request: SchedulerRequest, blocks: KVConnectorBlocks, num_external_tokens: int"/>
    <method name="build_connector_meta"
            returns="KVConnectorMetadata"
            params="scheduler_output: SchedulerOutput"/>
  </method-group>

  <method-group name="Worker-side">
    <method name="prepare_for_forward"
            returns="None"
            params="num_requests: int, scheduler_output: SchedulerOutput"/>
    <method name="finish_async_transfer"
            returns="None" params=""/>
  </method-group>

  <method-group name="Lifecycle">
    <method name="request_finished" returns="None" params="request_id: str"/>
    <method name="release_all_seqs" returns="None" params=""/>
  </method-group>

</api>

```python
class MooncakeConnector(KVConnectorBase_V1):

    # Scheduler-side
    def get_num_new_matched_tokens(self, request, num_computed_tokens: int) -> tuple[int, bool]: ...
    def update_state_after_alloc(self, request, blocks, num_external_tokens: int) -> None: ...
    def build_connector_meta(self, scheduler_output) -> "KVConnectorMetadata": ...

    # Worker-side
    def prepare_for_forward(self, num_requests: int, scheduler_output) -> None: ...
    def finish_async_transfer(self) -> None: ...

    # Lifecycle
    def request_finished(self, request_id: str) -> None: ...
    def release_all_seqs(self) -> None: ...
```

---

### 4.4 CLI Entry Points

<api name="CLI Entry Points" lang="python" package="mooncake" version="0.3.10"
    file="mooncake-wheel/pyproject.toml">
  <command name="mooncake_master"               entry="mooncake.cli:main"
           purpose="Start master coro_rpc service"/>
  <command name="mooncake_client"               entry="mooncake.cli_client:main"
           purpose="CLI client for Put / Get / Delete"/>
  <command name="transfer_engine_bench"         entry="mooncake.cli_bench:main"
           purpose="Transfer bandwidth benchmark"/>
  <command name="mooncake_http_metadata_server" entry="mooncake.http_metadata_server:main"
           purpose="HTTP metadata service"/>
  <command name="mc_store_rest_server"          entry="mooncake.mooncake_store_service:main"
           purpose="Store REST API proxy"/>
  <command name="transfer_engine_topology_dump" entry="mooncake.transfer_engine_topology_dump:main"
           purpose="Network topology introspection"/>
</api>

---

### 4.5 Handshake Protocol

```cpp
enum class HandShakeRequestType : uint8_t {
    Connection  = 0,
    Metadata    = 1,
    Notify      = 2,
    OldProtocol = 0xff,  // backward compatibility
};
```

---

## 5. Coding Style & Constraints

### 5.1 Enforced Style Rules

<style>

  <ruleset lang="cpp" enforcer=".clang-format" style="Google">
    <rule name="IndentWidth"    value="4 spaces"  note="tabs never used"/>
    <rule name="ColumnLimit"    value="80"/>
    <rule name="SortIncludes"   value="false"     note="maintain manual grouping"/>
    <rule name="Standard"       value="C++20"/>
    <rule name="NamingTypes"    value="PascalCase"  examples="TransferEngine, MasterClient"/>
    <rule name="NamingFunctions" value="camelCase" examples="submitTransfer, openSegment"/>
    <rule name="NamingMembers"  value="snake_case_" examples="client_id_"/>
    <rule name="NamingConstants" value="UPPER_SNAKE_CASE"/>
    <rule name="Namespace"      value="namespace mooncake { }" note="all production code"/>
    <rule name="ErrorHandling"  value="int (0=ok, negative=error) or tl::expected[T,ErrorCode]"/>
    <rule name="Ownership"      value="RAII — std::unique_ptr / std::shared_ptr; raw pointers only when non-owning"/>
    <rule name="Concurrency"    value="coro_rpc / async_simple for async I/O; std::mutex for shared state"/>
    <rule name="Logging"        value="LOG(INFO), LOG(WARNING), LOG(ERROR) from GLOG"/>
    <rule name="Nodiscard"      value="[[nodiscard]] on every function whose return value must be checked"/>
    <rule name="CopyrightHeader" value="Apache 2.0 block required at top of every source file"/>
  </ruleset>

  <ruleset lang="python" enforcer="ruff==0.6.9">
    <rule name="LineLength"     value="88 characters"/>
    <rule name="NamingVars"     value="snake_case"/>
    <rule name="NamingClasses"  value="PascalCase"/>
    <rule name="TypeHints"      value="required on public APIs; encouraged everywhere"/>
    <rule name="Async"          value="asyncio / aiohttp for I/O-bound operations"/>
    <rule name="ErrorHandling"  value="specific exception types; no bare except:"/>
    <rule name="Imports"        value="grouped stdlib → third-party → local; sorted by ruff"/>
  </ruleset>

  <ruleset lang="cmake" enforcer="cmake-format==0.6.13">
    <rule name="OptionNames"    value="UPPER_SNAKE_CASE" examples="WITH_STORE, USE_ETCD"/>
    <rule name="Linking"        value="target_link_libraries(target PRIVATE|PUBLIC dep)"/>
    <rule name="Standalone"     value="Each mooncake-* sub-project builds independently"/>
  </ruleset>

</style>

---

### 5.2 General Constraints

<constraints>
  <constraint id="ERR-1" severity="mandatory">
    Error handling is required in every function that can fail. Silent failures are not acceptable.
  </constraint>
  <constraint id="ERR-2" severity="mandatory">
    No C++ exceptions across module boundaries. Python C extensions and IPC layers must use
    return codes (int or tl::expected).
  </constraint>
  <constraint id="THR-1" severity="mandatory">
    Shared data structures accessed by multiple threads must be protected by mutexes or
    lock-free primitives. Thread-safety guarantees must be documented in header comments.
  </constraint>
  <constraint id="MEM-1" severity="mandatory">
    All user-space buffers used by the Transfer Engine must be registered with
    registerLocalMemory() before DMA and unregistered afterwards.
    Region sizes are clamped to globalConfig().max_mr_size.
  </constraint>
  <constraint id="MEM-2" severity="mandatory">
    Transports that modify local segment metadata (transport_attrs, devices) must call
    synchronizeLocal() to publish changes to the metadata store.
  </constraint>
  <constraint id="BUILD-1" severity="informational">
    Production builds: -O3 -g0. ASAN builds: ENABLE_ASAN=ON adds -fsanitize=address.
  </constraint>
  <constraint id="QA-1" severity="mandatory">
    All source files, comments, and documentation are spell-checked by codespell v2.2.6.
    Project-specific exceptions are listed in .typos.toml.
  </constraint>
  <constraint id="QA-2" severity="mandatory">
    Large files above the pre-commit threshold are blocked from being committed
    (check-added-large-files hook).
  </constraint>
</constraints>

---

## 6. Build & Test Quick Reference

### Build (CMake)

```bash
# Install system dependencies
sudo ./dependencies.sh

# Configure (enable all major components + tests)
cmake -B build \
  -DWITH_TE=ON \
  -DWITH_STORE=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=ON \
  -DSTORE_USE_ETCD=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j$(nproc)
```

<build-options>
  <option name="WITH_TE"          default="ON"  description="Build Transfer Engine"/>
  <option name="WITH_STORE"       default="ON"  description="Build Mooncake Store"/>
  <option name="WITH_P2P_STORE"   default="OFF" description="Build P2P Store"/>
  <option name="WITH_EP"          default="OFF" description="Build EP/PG CUDA extensions"/>
  <option name="USE_ETCD"         default="OFF" description="Enable etcd metadata backend"/>
  <option name="STORE_USE_ETCD"   default="OFF" description="Enable etcd HA for Store"/>
  <option name="STORE_USE_JEMALLOC" default="OFF" description="Use jemalloc in master"/>
  <option name="USE_TENT"         default="OFF" description="Enable TENT (next-gen Transfer Engine — API unstable)"/>
  <option name="USE_ASCEND"       default="OFF" description="Huawei Ascend NPU support"/>
  <option name="USE_MNNVL"        default="OFF" description="Multi-node NVLink"/>
  <option name="BUILD_UNIT_TESTS" default="OFF" description="Compile and register unit tests"/>
  <option name="BUILD_EXAMPLES"   default="OFF" description="Build example programs"/>
  <option name="ENABLE_ASAN"      default="OFF" description="AddressSanitizer"/>
  <option name="ENABLE_SCCACHE"   default="OFF" description="Use sccache for faster rebuilds"/>
</build-options>

### Python Package

```bash
pip install -e mooncake-wheel/
```

### Run Unit Tests

```bash
cd build && ctest --output-on-failure
```

### Lint

```bash
pip install -r requirements-dev.txt
pre-commit run --all-files
```

### CI Matrix

<ci file=".github/workflows/ci.yml">
  <dimension name="Python"  values="3.10, 3.12"/>
  <dimension name="CUDA"    values="12.8.1 (primary), 13.x (variant)"/>
  <dimension name="etcd"    values="v3.6.1"/>
  <dimension name="OS"      values="Ubuntu Linux x86-64"/>
</ci>

---

## 7. Repository Structure (Top Level)

```
Mooncake/
├── CMakeLists.txt               # Top-level build (requires CMake ≥ 3.16)
├── dependencies.sh              # One-shot dependency installer
├── requirements-dev.txt         # Python dev tools (pre-commit, ruff, …)
├── requirements_docs.txt        # Sphinx docs dependencies
├── extern/pybind11/             # Bundled pybind11
├── mooncake-asio/               # Bundled ASIO shared library
├── mooncake-common/             # Shared CMake helpers, etcd wrapper
├── mooncake-transfer-engine/    # Transfer Engine (C++) — stable API
│   └── tent/                   # TENT — next-gen engine (USE_TENT=OFF)
├── mooncake-store/              # Distributed KVCache Store (C++) — v2.0.0
├── mooncake-p2p-store/          # Peer-to-peer Store (C++)
├── mooncake-ep/                 # Expert Parallelism CUDA extension
├── mooncake-pg/                 # Pipeline Gradient CUDA extension
├── mooncake-rl/                 # Reinforcement learning utilities
├── mooncake-integration/        # vLLM / LMCache / SGLang connectors
├── mooncake-wheel/              # Python package v0.3.10
│   ├── mooncake/                # Python modules
│   └── pyproject.toml
├── benchmarks/                  # Performance benchmarks
├── monitoring/                  # Prometheus + Grafana stack
├── docker/                      # Dockerfiles
├── docs/                        # Sphinx documentation source
├── scripts/                     # Utility scripts
└── .github/workflows/           # CI/CD pipelines
```

---

*This document was generated from the repository at commit HEAD on 2026-03-19.
Keep it updated when new components, transports, or APIs are introduced.*
