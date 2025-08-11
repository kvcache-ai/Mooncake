# Mooncake Store Preview

## 概述

Mooncake Store 是一款专为LLM推理场景设计的高性能**分布式键值 KV Cache 存储引擎**。

与 Redis 或 Memcached 等传统缓存系统不同，Mooncake Store 的核心定位是**KV Cache 的存储引擎而非完整的缓存系统**。它们之间的最大区别是，对于后者，key 是由 value 通过哈希计算得到的，因此不再需要 `update()` 操作，也没有版本管理方面的需求。

Mooncake Store 提供了底层的对象存储和管理能力，包括可配置的缓存与淘汰策略，具有高内存利用率，专为加速LLM推理性能而设计。

Mooncake Store 的主要特性包括：

*   **对象级存储操作**：提供简单易用的对象级 API，包括 Put、Get 和 Remove 等操作，方便用户进行数据管理。
*   **多副本支持**：支持为同一对象保存多个数据副本，有效缓解热点访问压力。
*   **强一致性**：保证 `Get` 操作读取到完整且正确的数据，并且数据写入成功后，后续的 `Get` 操作一定能读取到最新写入的值。
*   **零拷贝、高带宽利用**：由 Transfer Engine 提供支持, 数据链路零拷贝，对大型对象进行条带化和并行 I/O 传输，充分利用多网卡聚合带宽，实现高速数据读写。
*   **动态资源伸缩**：支持动态添加和删除节点，灵活应对系统负载变化，实现资源的弹性管理。
*   **容错能力**: 任意数量的 master 节点和 client 节点故障都不会导致读取到错误数据。只要至少有一个 master 和一个 client 处于正常运行状态，Mooncake Store 就能继续正常工作并对外提供服务。
*   **分级缓存**: 支持将 RAM 中缓存数据卸载到 SSD 中，以进一步实现成本与性能间的平衡，提升存储系统的效率。

## 架构

![architecture](../../image/mooncake-store-preview.png)

如上图所示，Mooncake Store 中有两个关键组件：**Master Service** 和 **Client**。

**Master Service**：`Master Service` 负责管理整个集群的逻辑存储空间池，并处理节点的加入与退出事件。它负责对象空间的分配以及元数据的维护。其内存分配与替换策略经过专门设计和优化，以满足大语言模型推理任务的需求。


`Master Service` 作为一个独立进程运行，并向外部组件提供 RPC 服务。需要注意的是，`Transfer Engine` 所依赖的 `metadata service`（可通过 etcd、Redis 或 HTTP 等方式实现）不包含在 `Master Service` 中，需要单独部署。

**Client**：在 Mooncake Store 中，`Client` 类是唯一用于表示客户端逻辑的类，但它承担着**两种不同的角色**：

1. 作为 **客户端**，由上层应用调用，用于发起 `Put`、`Get` 等请求。
2. 作为 **存储服务器**，它托管一段连续的内存区域，作为分布式 KV 缓存的一部分，使其内存可以被其他 `Client` 实例访问。数据传输实际上是发生在不同 `Client` 实例之间，绕过了 `Master Service`。

可以通过配置使 `Client` 实例仅执行上述两种角色中的一种：

* 如果将 `global_segment_size` 设置为 0，则该实例作为 **纯客户端**，只发起请求，但不贡献内存给系统。
* 如果将 `local_buffer_size` 设置为 0，则该实例作为 **纯服务器**，仅提供内存用于存储。在这种情况下，该实例不允许发起 `Get` 或 `Put` 等请求操作。

`Client` 有两种运行模式：

1. **嵌入式模式**：作为共享库被导入，与 LLM 推理程序（例如 vLLM 实例）运行在同一进程中。
2. **独立模式**：作为一个独立进程运行。

Mooncake Store 支持两种部署方式，以满足不同的可用性需求：
1. **默认模式**：在该模式下，master service 由单个 master 节点组成，部署方式较为简单，但存在单点故障的问题。如果 master 崩溃或无法访问，系统将无法继续提供服务，直到 master 恢复为止。
2. **高可用模式（不稳定）**：在该模式下 master service 以多个 master 节点组成集群，并借助 etcd 集群进行协调，从而提升系统的容错能力。多个 master 节点使用 etcd 进行 leader 选举，由 leader 负责处理客户端请求。
如果当前的 leader 崩溃或发生网络故障，其余 master 节点将自动进行新的 leader 选举，以确保服务的持续可用性。
leader 通过定期心跳监控所有 client 节点的健康状态。如果某个 client 崩溃或无法访问，leader 能迅速检测到故障并采取相应措施。当 client 恢复或重新连接后，会自动重新加入集群，无需人工干预。

## Client C++ API

### 构造及初始化函数Init

```C++
ErrorCode Init(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               void** protocol_args,
               const std::string& master_server_entry);
```

初始化 Mooncake Store 客户端。其中各参数含义如下：
- `local_hostname` 表示本机的 IP:Port 或者可访问的域名（若不含端口号则使用默认值）
- `metadata_connstring` 表示 Transfer Engine 初始化所需的 etcd/redis 等元数据服务的地址
- `protocol` 为 Transfer Engine 所支持的协议，包括rdma、tcp
- `protocol_args` 是Transfer Engine 需要的协议参数
- `master_server_entry` 表示 Master 的地址信息（默认模式为 `IP:Port`，高可用模式为 `etcd://IP:Port;IP:Port;...;IP:Port`）


### Get 接口

```C++
tl::expected<void, ErrorCode> Get(const std::string& object_key, 
                                  std::vector<Slice>& slices);
```

![mooncake-store-simple-get](../../image/mooncake-store-simple-get.png)


用于获取 `object_key` 对应的值。该接口保证读取到的数据是完整且正确的。读取到的值将通过 TransferEngine 存储到 `slices` 所指向的内存区域中，可以是用户提前通过 `registerLocalMemory(addr, len)` 注册的本地 DRAM/VRAM 内存空间，注意非 Mooncake Store 内部管理的逻辑存储空间池（Logical Memory Pool）。（当启用了持久化功能时，若内存的 `Query` 请求失败，会尝试从SSD中寻找并载入对应的数据）

> 在目前的实现中，Get 接口可选 TTL 功能。当首次获取 `object_key` 对应的值后一段时间（默认为 1s），相应的条目会被自动删除。

### Put 接口

```C++
tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                  std::vector<Slice>& slices,
                                  const ReplicateConfig& config);
```

![mooncake-store-simple-put](../../image/mooncake-store-simple-put.png)

用于存储 `key` 对应的值。可通过 `config` 参数设置所需的副本数量。（当启用了持久化功能时，在内存的 `Put` 请求成功后，会异步发起一次数据向SSD的持久化操作）
其中`ReplicateConfig` 的数据结构细节如下：

```C++
struct ReplicateConfig {
    size_t replica_num{1};                    // 对象的总副本数
    bool with_soft_pin{false};               // 是否为该对象启用软固定机制
    std::string preferred_segment{};         // 首选的分配段
};
```

### Remove 接口

```C++
tl::expected<void, ErrorCode> Remove(const ObjectKey& key);
```

用于删除指定 key 对应的对象。该接口标记存储引擎中与 key 关联的所有数据副本已被删除，不需要与对应存储节点(Client)通信。

### Master Service

将集群中所有可用的资源看做一个巨大的资源池，由一个中心化的 Master 进程进行空间分配，并指导实现数据复制（**注意 Master Service 不接管任何的数据流，只是提供对应的元数据信息**）。

#### Master Service 接口

Master与Client的通信协议如下：

```protobuf
message BufHandle {
  required uint64 segment_name = 1;  // 存储段名称（可简单理解为存储节点的名称）
  required uint64 size = 2;          // 分配空间的大小
  required uint64 buffer = 3;        // 指向分配空间的指针

  enum BufStatus {
    INIT = 0;          // 初始状态，空间已被预留但尚未使用
    COMPLETE = 1;      // 使用完成，空间存储了有效数据
    FAILED = 2;        // 使用失败，上游应将 handle 状态更新为此值
    UNREGISTERED = 3;  // 空间已被注销，元数据已删除
  }
  required BufStatus status = 4 [default = INIT]; // 空间状态
};

message ReplicaInfo {
  repeated BufHandle handles = 1; // 存储对象数据的具体位置

  enum ReplicaStatus {
    UNDEFINED = 0;   // 未初始化
    INITIALIZED = 1; // 空间已分配，等待写入
    PROCESSING = 2;  // 正在写入数据
    COMPLETE = 3;    // 写入完成，副本可用
    REMOVED = 4;     // 副本已被移除
    FAILED = 5;      // 副本写入失败，可考虑重新分配
  }
  required ReplicaStatus status = 2 [default = UNDEFINED]; // 副本状态
};

service MasterService {
  // 获取对象的副本列表
  rpc GetReplicaList(GetReplicaListRequest) returns (GetReplicaListResponse);

  // 开始 Put 操作，分配存储空间
  rpc PutStart(PutStartRequest) returns (PutStartResponse);

  // 结束 Put 操作，标记对象写入完成
  rpc PutEnd(PutEndRequest) returns (PutEndResponse);

  // 删除对象的所有副本
  rpc Remove(RemoveRequest) returns (RemoveResponse);

  // 存储节点(Client)注册存储段
  rpc MountSegment(MountSegmentRequest) returns (MountSegmentResponse);

  // 存储节点(Client)注销存储段
  rpc UnmountSegment(UnmountSegmentRequest) returns (UnmountSegmentResponse);
}
```

1. GetReplicaList

```protobuf
message GetReplicaListRequest {
  required string key = 1; 
};

message GetReplicaListResponse {
  required int32 status_code = 1;
  repeated ReplicaInfo replica_list = 2; // 副本信息列表
};
```

* 请求: GetReplicaListRequest，包含需要查询的 key。
* 响应: GetReplicaListResponse，包含状态码 status_code 和 副本信息列表 replica_list。

说明: 用于获取指定 key 的所有可用副本的信息。Client 可以根据这些信息选择合适的副本进行读取。

2. PutStart

```protobuf
message PutStartRequest {
  required string key = 1;             // 对象键值
  required int64 value_length = 2;     // 待写入的数据总长度
  required ReplicateConfig config = 3; // 副本配置信息
  repeated uint64 slice_lengths = 4;   // 记录每个数据分片的长度
};

message PutStartResponse {
  required int32 status_code = 1; 
  repeated ReplicaInfo replica_list = 2;  // Master Service 分配好的副本信息
};
```

* 请求: PutStartRequest，包含 key、数据长度和副本配置config。
* 响应: PutStartResponse，包含状态码 status_code 和分配好的副本信息 replica_list。

说明: Client 在写入对象前，需要先调用 PutStart 向 `Master Service` 申请存储空间。`Master Service` 会根据 config 分配空间，并将分配结果（replica_list）返回给 Client。Client 随后将数据写入到分配副本所在的存储节点。 之所以需要 start 和 end 两步，是为确保其他Client不会读到正在写的值，进而造成脏读。

3. PutEnd

```protobuf
message PutEndRequest {
  required string key = 1; 
};

message PutEndResponse {
  required int32 status_code = 1;
};
```

* 请求: PutEndRequest，包含 key。
* 响应: PutEndResponse，包含状态码 status_code

Client 完成数据写入后，调用 PutEnd 通知 `Master Service`。`Master Service` 将更新对象的元数据信息，将副本状态标记为 COMPLETE，表示该对象可以被读取。

4. Remove

```protobuf
message RemoveRequest {
  required string key = 1; 
};

message RemoveResponse {
  required int32 status_code = 1;
};
```

* 请求: RemoveRequest，包含需要删除对象的key
* 响应: RemoveResponse，包含状态码 status_code 

用于删除指定 key 对应的对象及其所有副本。Master Service 将对应对象的所有副本状态标记为删除。

5. MountSegment

```protobuf
message MountSegmentRequest {
  required uint64 buffer = 1;       // 空间的起始地址
  required uint64 size = 2;         // 空间的大小
  required string segment_name = 3; // 存储段名称
}

message MountSegmentResponse {
  required int32 status_code = 1;
};
```

存储节点(Client)自己分配一段内存，然后在调用`TransferEngine::registerLoalMemory` 完成本地挂载后，调用该接口，将分配好的一段连续的地址空间挂载到`Master Service`用于分配。

6. UnmountSegment

```protobuf
message UnmountSegmentRequest {
  required string segment_name = 1;  // 挂载时的存储段名称
}

message UnMountSegmentResponse {
  required int32 status_code = 1;
};
```

空间需要释放时，通过该接口在`Master Service` 中把之前挂载的资源移除。

#### 对象信息维护
`Master Service` 需要维护与 buffer allocator 相关的映射关系和对象元数据等信息，在多副本场景下实现对内存资源的高效管理和副本状态的精确控制。此外，`Master Service` 使用读写锁保护关键数据结构，从而在多线程环境下保证数据的一致性与安全性。
以下为`Master Service` 维护存储空间信息的接口：

- MountSegment

```C++
tl::expected<void, ErrorCode> MountSegment(uint64_t buffer,
                                          uint64_t size,
                                          const std::string& segment_name);
```

存储节点(Client)向`Master Service`注册存储段空间。

- UnmountSegment

```C++
tl::expected<void, ErrorCode> UnmountSegment(const std::string& segment_name);
```

存储节点(Client)向`Master Service`注销存储段空间。

`Master Service` 处理对象相关的接口如下：

- Put

```C++
    ErrorCode PutStart(const std::string& key,
                       uint64_t value_length,
                       const std::vector<uint64_t>& slice_lengths,
                       const ReplicateConfig& config,
                       std::vector<ReplicaInfo>& replica_list);

    ErrorCode PutEnd(const std::string& key);
```

`Client` 在写入对象前，先调用 PutStart 向 `Master Service`申请分配存储空间。
`Client` 在完成数据写入后，调用 PutEnd 通知 `Master Service`标记对象写入完成。

- GetReplicaList

```C++
ErrorCode GetReplicaList(const std::string& key,
                         std::vector<ReplicaInfo>& replica_list);
```

`Client`向`Master Service`请求获取指定key的副本列表，`Client`可以根据这些信息选择合适的副本进行读取。

- Remove

```C++
tl::expected<void, ErrorCode> Remove(const std::string& key);
```

`Client`向`Master Service`请求删除指定key的所有副本。

### Buffer Allocator

在 Mooncake Store 系统中，Buffer Allocator 是一个底层的内存管理组件，主要负责高效的内存分配与释放。它基于底层内存分配器实现其功能。

需要注意的是，Buffer Allocator 管理的内存并不属于 `Master Service` 本身。它实际操作的是由 `Clients` 注册的内存段。当 `Master Service` 收到一个 `MountSegment` 请求来注册一段连续的内存区域时，会通过 `AddSegment` 接口创建一个对应的 Buffer Allocator。

Mooncake Store 提供了 `BufferAllocatorBase` 的两个具体实现：

**CachelibBufferAllocator**：该分配器基于 Facebook 的 [CacheLib](https://github.com/facebook/CacheLib)，采用基于 slab 的分配策略进行内存管理，具有良好的碎片控制能力，适用于高性能场景。

**OffsetBufferAllocator**：该分配器源自 [OffsetAllocator](https://github.com/sebbbi/OffsetAllocator)，采用自定义的基于 bin 的分配策略，支持实时性要求较高的 `O(1)` 级偏移分配，并能最大限度地减少内存碎片。

Mooncake Store 针对 LLM 推理任务的内存使用特性对这两种分配器进行了优化，从而提升了在大模型场景下的内存利用率。用户可以根据具体的性能需求和内存使用模式选择不同的分配器，具体通过 `master_service` 的启动参数 `--memory-allocator` 进行配置。

这两种分配器都实现了 `BufferAllocatorBase` 接口。`BufferAllocatorBase` 类的主要接口如下：

```C++
class BufferAllocatorBase {
    virtual ~BufferAllocatorBase() = default;
    virtual std::unique_ptr<AllocatedBuffer> allocate(size_t size) = 0;
    virtual void deallocate(AllocatedBuffer* handle) = 0;
};
```

1. **构造函数**：创建 `BufferAllocator` 实例时，上层组件必须提供要管理的内存区域的基地址和大小。该信息用于初始化内部分配器，实现统一的内存管理。

2. **`allocate` 函数**：当上层组件发起读写请求时，需要一段可操作的内存区域。`allocate` 函数调用内部分配器分配内存块，并返回内存起始地址和大小等元信息。新分配的内存状态被初始化为 `BufStatus::INIT`。

3. **`deallocate` 函数**：该函数由 `BufHandle` 析构函数自动触发，内部调用分配器释放对应内存，并将句柄状态更新为 `BufStatus::UNREGISTERED`。

### AllocationStrategy

`AllocationStrategy` 与 Master Service 和底层 Buffer Allocator 协同工作：

* **Master Service**：通过 `AllocationStrategy` 决定副本分配的目标位置。
* **Buffer Allocator**：执行实际的内存分配与释放操作。

#### 接口定义

`Allocate`：从可用的存储资源中查找合适的存储段，以分配指定大小的空间。

```C++
virtual std::unique_ptr<AllocatedBuffer> Allocate(
        const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
        const std::unordered_map<std::string, std::vector<std::shared_ptr<BufferAllocatorBase>>>& allocators_by_name,
        size_t objectSize, const ReplicateConfig& config) = 0;
```

* **输入参数**：
  * `allocators`：所有已挂载的 buffer allocator 的列表
  * `allocators_by_name`：按 segment 名称组织的 allocator 映射，用于优先分配到指定 segment
  * `objectSize`：待分配对象的大小
  * `config`：副本配置，包含首选 segment 及其他分配偏好

* **输出结果**：如果分配成功，返回一个指向 `AllocatedBuffer` 的智能指针；若找不到合适的 allocator，则返回 `nullptr`

#### 实现策略

`RandomAllocationStrategy` 是 `AllocationStrategy` 的一个子类，提供如下智能分配特性：

1. **支持首选 Segment**：若 `ReplicateConfig` 中指定了首选 segment，策略将优先尝试从该 segment 分配；如果失败，再退回到随机分配。

2. **带重试逻辑的随机分配**：当存在多个可用 allocator 时，采用带最多 10 次重试机制的随机选择策略以寻找合适的 allocator。

3. **确定性随机性**：使用 Mersenne Twister 随机数生成器，并通过合理种子确保分配行为的一致性。

该策略能够自动处理首选 segment 不存在、空间不足或不可用等情况，并优雅地回退到所有可用 segment 中进行随机分配。

### 替换策略

当挂载的空间已满，即由于内存不足而导致 `PutStart` 请求失败时，系统将启动替换任务以释放空间。与 `Remove` 操作类似，被换出的对象仅被标记为已删除，无需进行数据传输。

目前采用的是一种近似的 LRU 策略，即尽可能优先换出最近最少被访问的对象。为了避免数据竞争和数据损坏，正在被客户端读取或写入的对象不会被换出。因此，拥有租约或尚未被 `PutEnd` 请求标记为 complete 的对象不会被换出。

每次替换任务被触发时，会尝试换出大约 10% 的对象，这个比例可通过 `master_service` 的启动参数进行配置。

为了尽力避免 Put 失败，还可以通过 `master_service` 的启动参数 `-eviction_high_watermark_ratio=<RATIO>`(默认为 1) 来设定 eviction 的高水位触发条件。当清理线程发现当前空间使用量达到了设定的高水位，
则开始进行清理工作，清理的目标在高水位基础上再多清理 `-eviction_ratio` 指定的清理比例，从而达到空间低水位。

### 租约机制

为避免数据冲突，每当 `ExistKey` 请求或 `GetReplicaListRequest` 请求成功时，系统会为对应对象授予一个租约。在租约过期前，该对象将受到保护，不会被 `Remove`、`RemoveAll` 或替换任务删除。对有租约的对象执行 `Remove` 请求会失败；`RemoveAll` 请求则只会删除没有租约的对象。

默认的租约时间为 5 秒，并可通过 `master_service` 的启动参数进行配置。

### 软固定机制

对于重要且频繁使用的对象，例如 system prompt，Mooncake Store 提供了软固定（soft pin）机制。在执行 `Put` 操作时，可以选择为特定的对象开启软固定机制。在执行替换任务时，系统会优先替换未被软固定的对象。仅当内存不足且没有其他对象可以被替换时，才会替换被软固定的对象。

如果某个软固定的对象长时间未被访问，其软固定状态将被解除。而后当该对象再次被访问时，它将自动重新进入软固定状态。

`master_service` 中有两个与软固定机制相关的启动参数：

* `default_kv_soft_pin_ttl`：表示一个被软固定的对象在多长时间（毫秒）未被访问后会自动解除软固定状态。默认值为`30 分钟`。

* `allow_evict_soft_pinned_objects`：是否允许替换已被软固定的对象。默认值为 `true`。

被软固定的对象仍然可以通过 `Remove`、`RemoveAll` 等 API 主动删除。

### 首选段分配

Mooncake Store 提供了**首选段分配**功能，允许用户为对象分配指定首选的存储段（节点）。此功能特别适用于优化数据局部性和减少分布式场景中的网络开销。

#### 工作原理

首选段分配功能通过 `AllocationStrategy` 系统实现，并通过 `ReplicateConfig` 结构中的 `preferred_segment` 字段进行控制：

```cpp
struct ReplicateConfig {
    size_t replica_num{1};                    // 对象的总副本数
    bool with_soft_pin{false};               // 是否为该对象启用软固定机制
    std::string preferred_segment{};         // 首选的分配段
};
```

当使用非空的 `preferred_segment` 值启动 `Put` 操作时，分配策略遵循以下流程：

1. **首选分配尝试**：系统首先尝试从指定的首选段分配空间。如果首选段有足够的可用空间，分配立即成功。

2. **回退到随机分配**：如果首选段不可用、已满或不存在，系统会自动回退到所有可用段之间的标准随机分配策略。

3. **重试逻辑**：分配策略包含内置的重试机制，最多尝试 10 次在不同段中寻找合适的存储空间。

- **数据局部性**：通过首选本地段，应用程序可以减少网络流量并提高频繁使用数据的访问性能。
- **负载均衡**：应用程序可以将数据分布到特定节点，实现更好的负载分布。

### 分级缓存支持

本系统提供了分级缓存架构的支持，通过内存缓存与持久化存储相结合的方式实现高效数据访问。数据首先存储在内存缓存中，并会异步写入分布式文件系统（DFS，Distributed File System）作为备份，形成"内存-SSD持久化存储"的分级缓存结构。

#### 持久化功能启用方法

当用户在启动client时指定了`MOONCAKE_STORAGE_ROOT_DIR`的环境变量，且该路径为一个已存在的有效路径时，则client端的数据持久化功能就会开始工作。同时在client启动时，会向master请求一个`cluster_id`，该id可以在初始化master进行指定，若未指定则会使用默认值`mooncake_cluster`，之后client执行持久化的根目录即为`<MOONCAKE_STORAGE_ROOT_DIR>/<cluster_id>`。注意在使用DFS时，需要在各client上分别指定DFS对应的挂载目录，以实现SSD上数据之间的共享。

#### 数据访问机制
在目前的实现版本中，kvcache object的读\写\查询等操作都是完全在client端完成的，master对其无感知。在文件系统中key -> kvcache object的索引信息是由固定的索引机制来维护的，每个文件对应一个kvcache object（文件名即为对应的key名称）。 

启用持久化功能后，对于每次成功写入memory的 `Put`或`BatchPut` 操作，都会异步地发起一次持久化操作，写入DFS当中。之后执行 `Get`或 `BatchGet` 时，如果在memory pool中没有找到对应的kvcache，则会尝试从DFS中读取该文件数据，并返回给用户。

## Mooncake Store Python API

**完整的 Python API 文档**: [https://kvcache-ai.github.io/Mooncake/mooncake-store-api/python-binding.html](https://kvcache-ai.github.io/Mooncake/mooncake-store-api/python-binding.html)


## 编译及使用方法
Mooncake Store 与其它相关组件（Transfer Engine等）一同编译。

默认模式:
```
mkdir build && cd build
cmake .. # 默认模式
make
sudo make install # 安装 Python 接口支持包
```

高可用模式:
```
mkdir build && cd build
cmake .. -DSTORE_USE_ETCD # 编译 etcd 客户端接口封装模块，依赖 go
make
sudo make install # 安装 Python 接口支持包
```

### 启动 Transfer Engine 的 Metadata 服务
Mooncake Store 使用 Transfer Engine 作为核心传输引擎，因此需要启动元数据服务（etcd/redis/http），`metadata` 服务的启动与配置可以参考[Transfer Engine](./transfer-engine.md)的有关章节。特别注意：对于 etcd 服务，默认仅为本地进程提供服务，需要修改监听选项（IP 为 0.0.0.0，而不是默认的 127.0.0.1）。可使用 curl 等指令验证正确性。

### 启动 Master Service

Master Service 独立作为一个进程，对外提供 gRPC 接口，负责Mooncake Store 的元数据管理（注意Master Service 并不复用Transfer Engine 的Metadata服务），默认监听端口为 `50051`。在编译完成后，可直接运行位于 `build/mooncake-store/src/` 目录下的 `mooncake_master`。启动后，Master Service 会在日志中输出下列内容：
```
Starting Mooncake Master Service
Port: 50051
Max threads: 4
Master service listening on 0.0.0.0:50051
```

**高可用模式**:

高可用模式依赖于 etcd 服务进行协调。如果 Transfer Engine 也使用 etcd 作为其元数据服务，那么 Mooncake Store 使用的 etcd 集群可以与 Transfer Engine 使用的集群共用，也可以是独立的。

高可用模式支持部署多个 master 实例，以消除单点故障。每个 master 实例启动时必须带上以下参数：
```
--enable-ha：启用高可用模式
--etcd-endpoints：指定 etcd 服务的多个入口，使用分号 ';' 分隔
--rpc-address：该实例的 RPC 地址
```

例如:
```
./build/mooncake-store/src/mooncake_master \
    --enable-ha=true \
    --etcd-endpoints="0.0.0.0:2379;0.0.0.0:2479;0.0.0.0:2579" \
    --rpc-address=10.0.0.1
```

### 启动验证程序
Mooncake Store 提供了多种验证程序，包括基于 C++ 和 Python 等接口形态。下面以 `stress_cluster_benchmark` 为例介绍一下如何运行。

1. 打开 `stress_cluster_benchmark.py`，结合网络情况修改初始化代码，重点是 local_hostname（对应本机 IP 地址）、metadata_server（对应 Transfer Engine 元数据服务）、master_server_address（对应 Master Service 地址及端口）等：

打开 `stress_cluster_benchmark.py`，根据你的网络环境更新初始化设置。特别注意以下字段：
```
local_hostname：本机的 IP 地址
metadata_server：Transfer Engine 元数据服务的地址
master_server_address：Master 服务的地址
```
**注意**：`master_server_address` 的格式取决于部署模式。在默认模式下，使用格式 `IP:Port`，表示单个 master 节点的地址。在高可用模式下，使用格式 `etcd://IP:Port;IP:Port;...;IP:Port`，表示 etcd 集群各节点的地址。
例如：
```python
import os
import time

from distributed_object_store import DistributedObjectStore

store = DistributedObjectStore()
# Transfer engine 使用的协议，可选值为 "rdma" 或 "tcp"
protocol = os.getenv("PROTOCOL", "tcp")
# Transfer engine 使用的设备名称
device_name = os.getenv("DEVICE_NAME", "ibp6s0")
# 本节点在集群中的 hostname,端口号从（12300-14300）中随机选择
local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
# Transfer Engine 的 Metadata 服务地址，这里使用 etcd 作为元数据服务
metadata_server = os.getenv("METADATA_ADDR", "127.0.0.1:2379")
# 每个节点向集群中挂载的 Segment 大小，挂载之后由Master Service 进行分配，单位为字节
global_segment_size = 3200 * 1024 * 1024
# 注册Transfer Engine 的本地缓冲区大小，单位为字节
local_buffer_size = 512 * 1024 * 1024
# Mooncake Store 的 Master Service 地址
master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
# 每次 put() 的数据长度
value_length = 1 * 1024 * 1024
# 总共发送请求数量
max_requests = 1000
# 初始化 Mooncake Store Client
retcode = store.setup(
    local_hostname,
    metadata_server,
    global_segment_size,
    local_buffer_size,
    protocol,
    device_name,
    master_server_address,
)
```

2. 在一台机器上运行 `ROLE=prefill python3 ./stress_cluster_benchmark.py`，启动 Prefill 节点。
   对于 rdma 协议, 你可以开启自动探索 topology 和设置网卡白名单, e.g., `ROLE=prefill MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 ./stress_cluster_benchmark.py`。如需启用持久化功能，可运行`ROLE=prefill MOONCAKE_STORAGE_ROOT_DIR=/path/to/dir  python3 ./stress_cluster_benchmark.py`。
3. 在另一台机器上运行 `ROLE=decode python3 ./stress_cluster_benchmark.py`，启动 Decode 节点。
   对于 rdma 协议, 你可以开启自动探索 topology 和设置网卡白名单, e.g., `ROLE=decode MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 ./stress_cluster_benchmark.py`。如需启用持久化功能，可运行`ROLE=decode MOONCAKE_STORAGE_ROOT_DIR=/path/to/dir  python3 ./stress_cluster_benchmark.py`。

无报错信息表示数据传输成功。

### 以独立进程方式启动 Client

使用 `mooncake-wheel/mooncake/mooncake_store_service.py` 可以以独立进程的形式启动 `Client`。

首先，创建并保存一个 JSON 格式的配置文件。例如：

```
{
    "local_hostname": "localhost",
    "metadata_server": "http://localhost:8080/metadata",
    "global_segment_size": 268435456,
    "local_buffer_size": 268435456,
    "protocol": "tcp",
    "device_name": "",
    "master_server_address": "localhost:50051"
}
```

然后运行 `mooncake_store_service.py`。该程序在启动 `Client` 的同时，会启动一个 HTTP 服务器。用户可以通过该服务器手动执行 `Get`、`Put` 等操作，方便调试。

程序的主要启动参数包括：

* `config`：配置文件路径
* `port`：HTTP 服务的端口号

假设 `mooncake_transfer_engine` 的 wheel 包已经安装，通过下列命令可以启动程序：
```bash
python -m mooncake.mooncake_store_service --config=[config_path] --port=8081
```

## 范例代码

#### Python 使用示例
我们提供一个参考样例 `distributed_object_store_provider.py`，其位于 `mooncake-store/tests` 目录下。为了检测相关组件是否正常安装，可在相同的服务器上后台运行 etcd、Master Service（`mooncake_master`）等两个服务，然后在前台执行该 Python 程序，此时应输出测试成功的结果。

#### C++ 使用示例

Mooncake Store 的 C++ API 提供了更底层的控制能力。我们提供一个参考样例 `client_integration_test`，其位于 `mooncake-store/tests` 目录下。为了检测相关组件是否正常安装，可在相同的服务器上运行 etcd、Master Service（`mooncake_master`），并执行该 C++ 程序（位于 `build/mooncake-store/tests` 目录下），应输出测试成功的结果。
