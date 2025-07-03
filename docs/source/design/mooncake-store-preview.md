# Mooncake Store Preview

## Introduction

Mooncake Store is a high-performance **distributed key-value (KV) cache storage engine** designed specifically for LLM inference scenarios.

Unlike traditional caching systems such as Redis or Memcached, Mooncake Store is positioned as **a distributed KV cache rather than a generic caching system**. The key difference is that in the former, the key is derived from the value through hashing, so value is immutable after inserting (although the key/value pair may be garbage collected).

Mooncake Store provides low-level object storage and management capabilities, while specific caching strategies (e.g., eviction policies) are left to upper-layer frameworks (like vLLM) or users for implementation, offering higher flexibility and customizability.

Key features of Mooncake Store include:
- **Object-level storage operations**: Mooncake Store provides simple and easy-to-use object-level APIs, including `Put`, `Get`, and `Remove` operations.
- **Multi-replica support**: Mooncake Store supports storing multiple data replicas for the same object, effectively alleviating hotspots in access pressure.
- **Eventual consistency**: Mooncake Store ensures that `Get` operations read complete and correct data, but does not guarantee the latest written data. This eventual consistency model ensures high performance while simplifying system design.
- **High bandwidth utilization**: Mooncake Store supports striping and parallel I/O transfer of large objects, fully utilizing multi-NIC aggregated bandwidth for high-speed data reads and writes.
- **Dynamic resource scaling**: Mooncake Store supports dynamically adding and removing nodes to flexibly handle changes in system load, achieving elastic resource management.
- **Fault tolerance**: Mooncake store is designed with robust fault tolerance. Failures of any number of master and client nodes will not result in incorrect data being read. As long as at least one master and one client remain operational, Mooncake Store continues to function correctly and serve requests.

## Architecture

Mooncake Store is managed by a global **Master Service** responsible for allocating storage space pools. The specific allocation details are implemented by the `BufferAllocator` class, coordinated by the `AllocationStrategy` strategy class.

![architecture](../image/mooncake-store-preview.png)

As shown in the figure above, there are two key components in Mooncake Store:
1. **Master Service**: Manages the logical storage space pool of the entire cluster and maintains node entry and exit. This is an independently running process that provides RPC services externally. Note that the metadata service required by the Transfer Engine (via etcd, Redis, or HTTP, etc.) is not included in the Master Service and needs to be deployed separately.
2. **Client**: Shares a process with the vLLM instance (at least in the current version). Each Client is allocated a certain size of DRAM space, serving as part of the logical storage space pool. Therefore, data transfer is actually from one Client to another, bypassing the Master.

Mooncake store supports two deployment methods to accommodate different availability requirements:
1. **Default mode**: In this mode, the master service consists of a single master node, which simplifies deployment but introduces a single point of failure. If the master crashes or becomes unreachable, the system cannot continue to serve requests until it is restored.
2. **High availability mode (unstable)**: This mode enhances fault tolerance by running the master service as a cluster of multiple master nodes coordinated through an etcd cluster. The master nodes use etcd to elect a leader, which is responsible for handling client requests.
If the current leader fails or becomes partitioned from the network, the remaining master nodes automatically perform a new leader election, ensuring continuous availability.
The leader monitors the health of all client nodes through periodic heartbeats. If a client crashes or becomes unreachable, the leader quickly detects the failure and takes appropriate action. When a client node recovers or reconnects, it can automatically rejoin the cluster without manual intervention.

## Client C++ API

### Constructor and Initialization `Init`

```C++
ErrorCode Init(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               void** protocol_args,
               const std::string& master_server_entry);
```

Initializes the Mooncake Store client. The parameters are as follows:
- `local_hostname`: The `IP:Port` of the local machine or an accessible domain name (default value used if port is not included)
- `metadata_connstring`: The address of the metadata service (e.g., etcd/Redis) required for Transfer Engine initialization
- `protocol`: The protocol supported by the Transfer Engine, including RDMA and TCP
- `protocol_args`: Protocol parameters required by the Transfer Engine
- `master_server_entry`: The address information of the Master (`IP:Port` for default mode and `etcd://IP:Port;IP:Port;...;IP:Port` for high availability mode)

### Get

```C++
ErrorCode Get(const std::string& object_key, 
              std::vector<Slice>& slices);
```

![mooncake-store-simple-get](../image/mooncake-store-simple-get.png)


Used to retrieve the value corresponding to `object_key`. The retrieved data is guaranteed to be complete and correct. The retrieved value is stored in the memory region pointed to by `slices` via the Transfer Engine, which can be local DRAM/VRAM memory space registered in advance by the user through `registerLocalMemory(addr, len)`. Note that this is not the logical storage space pool (Logical Memory Pool) managed internally by Mooncake Store.

> In the current implementation, the Get interface has an optional TTL feature. When the value corresponding to `object_key` is fetched for the first time, the corresponding entry is automatically deleted after a certain period of time (1s by default).

### Put

```C++
ErrorCode Put(const ObjectKey& key,
              std::vector<Slice>& slices,
              const ReplicateConfig& config);
```

![mooncake-store-simple-put](../image/mooncake-store-simple-put.png)

Used to store the value corresponding to `key`. The required number of replicas can be set via the `config` parameter. The data structure details of `ReplicateConfig` are as follows:

```C++
enum class MediaType {
    VRAM,
    RAM,
    SSD
};

struct Location {
    std::string segment_name;   // Segment Name within the Transfer Engine, usually the local_hostname field filled when the target server starts
};

struct ReplicateConfig {
    uint64_t replica_num;       // Total number of replicas for the object
    std::map<MediaType, int> media_replica_num; // Number of replicas allocated on a specific medium, with the higher value taken if the sum exceeds replica_num
    std::vector<Location> locations; // Specific storage locations (machine and medium) for a replica
};
```

### Remove

```C++
ErrorCode Remove(const ObjectKey& key);
```

Used to delete the object corresponding to the specified key. This interface marks all data replicas associated with the key in the storage engine as deleted, without needing to communicate with the corresponding storage node (Client).

### Master Service

The cluster's available resources are viewed as a large resource pool, managed centrally by a Master process for space allocation and guiding data replication 

**Note: The Master Service does not take over any data flow, only providing corresponding metadata information.**

#### Master Service APIs

The protobuf definition between Master and Client is as follows:

```protobuf
message BufHandle {
  required uint64 segment_name = 1;  // Storage segment name (can be simply understood as the name of the storage node)
  required uint64 size = 2;          // Size of the allocated space
  required uint64 buffer = 3;        // Pointer to the allocated space

  enum BufStatus {
    INIT = 0;          // Initial state, space reserved but not used
    COMPLETE = 1;      // Completed usage, space contains valid data
    FAILED = 2;        // Usage failed, upstream should update the handle state to this value
    UNREGISTERED = 3;  // Space has been unregistered, metadata deleted
  }
  required BufStatus status = 4 [default = INIT]; // Space status
};

message ReplicaInfo {
  repeated BufHandle handles = 1; // Specific locations of the stored object data

  enum ReplicaStatus {
    UNDEFINED = 0;   // Uninitialized
    INITIALIZED = 1; // Space allocated, waiting for write
    PROCESSING = 2;  // Writing data in progress
    COMPLETE = 3;    // Write completed, replica available
    REMOVED = 4;     // Replica has been removed
    FAILED = 5;      // Replica write failed, consider reallocation
  }
  required ReplicaStatus status = 2 [default = UNDEFINED]; // Replica status
};

service MasterService {
  // Get the list of replicas for an object
  rpc GetReplicaList(GetReplicaListRequest) returns (GetReplicaListResponse);

  // Start Put operation, allocate storage space
  rpc PutStart(PutStartRequest) returns (PutStartResponse);

  // End Put operation, mark object write completion
  rpc PutEnd(PutEndRequest) returns (PutEndResponse);

  // Delete all replicas of an object
  rpc Remove(RemoveRequest) returns (RemoveResponse);

  // Storage node (Client) registers a storage segment
  rpc MountSegment(MountSegmentRequest) returns (MountSegmentResponse);

  // Storage node (Client) unregisters a storage segment
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
  repeated ReplicaInfo replica_list = 2; // List of replica information
};
```

- **Request**: `GetReplicaListRequest` containing the key to query.
- **Response**: `GetReplicaListResponse` containing the status code status_code and the list of replica information `replica_list`.
- **Description**: Used to retrieve information about all available replicas for a specified key. The Client can select an appropriate replica for reading based on this information.

2. PutStart

```protobuf
message PutStartRequest {
  required string key = 1;             // Object key
  required int64 value_length = 2;     // Total length of data to be written
  required ReplicateConfig config = 3; // Replica configuration information
  repeated uint64 slice_lengths = 4;   // Lengths of each data slice
};

message PutStartResponse {
  required int32 status_code = 1; 
  repeated ReplicaInfo replica_list = 2;  // Replica information allocated by the Master Service
};
```

- **Request**: `PutStartRequest` containing the key, data length, and replica configuration config.
- **Response**: `PutStartResponse` containing the status code status_code and the allocated replica information replica_list.
- **Description**: Before writing an object, the Client must call PutStart to request storage space from the Master Service. The Master Service allocates space based on the config and returns the allocation results (`replica_list`) to the Client. The Client then writes data to the storage nodes where the allocated replicas are located. The need for both start and end steps ensures that other Clients do not read partially written values, preventing dirty reads.

3. PutEnd

```protobuf
message PutEndRequest {
  required string key = 1; 
};

message PutEndResponse {
  required int32 status_code = 1;
};
```

- **Request**: `PutEndRequest` containing the key.
- **Response**: `PutEndResponse` containing the status code status_code.
- **Description**: After the Client completes data writing, it calls `PutEnd` to notify the Master Service. The Master Service updates the object's metadata, marking the replica status as `COMPLETE`, indicating that the object is readable.

4. Remove

```protobuf
message RemoveRequest {
  required string key = 1; 
};

message RemoveResponse {
  required int32 status_code = 1;
};
```

- **Request**: `RemoveRequest` containing the key of the object to be deleted.
- **Response**: `RemoveResponse` containing the status code `status_code`.
- **Description**: Used to delete the object and all its replicas corresponding to the specified key. The Master Service marks all replicas of the corresponding object as deleted.

5. MountSegment

```protobuf
message MountSegmentRequest {
  required uint64 buffer = 1;       // Starting address of the space
  required uint64 size = 2;         // Size of the space
  required string segment_name = 3; // Storage segment name
}

message MountSegmentResponse {
  required int32 status_code = 1;
};
```

The storage node (Client) allocates a segment of memory and, after calling `TransferEngine::registerLocalMemory` to complete local mounting, calls this interface to mount the allocated continuous address space to the Master Service for allocation.

6. UnmountSegment

```protobuf
message UnmountSegmentRequest {
  required string segment_name = 1;  // Storage segment name used during mounting
}

message UnMountSegmentResponse {
  required int32 status_code = 1;
};
```

When the space needs to be released, this interface is used to remove the previously mounted resources from the Master Service.

#### Object Information Maintenance
The Master Service needs to maintain mappings related to `BufferAllocator` and object metadata to efficiently manage memory resources and precisely control replica states in multi-replica scenarios. Additionally, the Master Service uses read-write locks to protect critical data structures, ensuring data consistency and security in multi-threaded environments. The following are the interfaces maintained by the Master Service for storage space information:

- MountSegment

```C++
ErrorCode MountSegment(uint64_t buffer,
                       uint64_t size,
                       const std::string& segment_name);
```

The storage node (Client) registers the storage segment space with the Master Service.

- UnmountSegment

```C++
ErrorCode UnmountSegment(const std::string& segment_name);
```

The storage node (Client) unregisters the storage segment space with the Master Service.

The Master Service handles object-related interfaces as follows:

- Put

```C++
    ErrorCode PutStart(const std::string& key,
                       uint64_t value_length,
                       const std::vector<uint64_t>& slice_lengths,
                       const ReplicateConfig& config,
                       std::vector<ReplicaInfo>& replica_list);

    ErrorCode PutEnd(const std::string& key);
```

Before writing an object, the Client calls PutStart to request storage space allocation from the Master Service. After completing data writing, the Client calls PutEnd to notify the Master Service to mark the object write as completed.

- GetReplicaList

```C++
ErrorCode GetReplicaList(const std::string& key,
                         std::vector<ReplicaInfo>& replica_list);
```

The Client requests the Master Service to retrieve the replica list for a specified key, allowing the Client to select an appropriate replica for reading based on this information.

- Remove

```C++
ErrorCode Remove(const std::string& key);
```

The Client requests the Master Service to delete all replicas corresponding to the specified key.

### Buffer Allocator

The BufferAllocator is a low-level space management class in the Mooncake Store system, primarily responsible for efficiently allocating and releasing memory. It leverages Facebook's CacheLib `MemoryAllocator` to manage underlying memory. When the Master Service receives a `MountSegment` request to register underlying space, it creates a `BufferAllocator` object via `AddSegment`. The main interfaces in the `BufferAllocator` class are as follows:

```C++
class BufferAllocator {
    BufferAllocator(SegmentName segment_name,
                    size_t base,
                    size_t size);
    ~BufferAllocator();

    std::shared_ptr<BufHandle> allocate(size_t size);
    void deallocate(BufHandle* handle);
 };
```

1. Constructor: When creating a `BufferAllocator` instance, the upstream needs to pass the address and size of the instance space. Based on this information, the internal cachelib interface is called for unified management.
2. `allocate` function: When the upstream has read/write requests, it needs to specify a region for the upstream to use. The `allocate` function calls the internal cachelib allocator to allocate memory and provides information such as the address and size of the space.
3. `deallocate` function: Automatically called by the `BufHandle` destructor, which internally calls the cachelib allocator to release memory and sets the handle state to `BufStatus::UNREGISTERED`.

### AllocationStrategy
AllocationStrategy is a strategy class for efficiently managing memory resource allocation and replica storage location selection in a distributed environment. It is mainly used in the following scenarios:
- Determining the allocation locations for object storage replicas.
- Selecting suitable read/write paths among multiple replicas.
- Providing decision support for resource load balancing between nodes in distributed storage.

AllocationStrategy is used in conjunction with the Master Service and the underlying module BufferAllocator:
- Master Service: Determines the target locations for replica allocation via `AllocationStrategy`.
- `BufferAllocator`: Executes the actual memory allocation and release tasks.

#### APIs

1. `Allocate`: Finds a suitable storage segment from available storage resources to allocate space of a specified size.

```C++
virtual std::shared_ptr<BufHandle> Allocate(
        const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>& allocators,
        size_t objectSize) = 0;
```

- Input: The list of available storage segments and the size of the space to be allocated.
- Output: Returns a successful allocation handle BufHandle.

#### Implementation Strategies

`RandomAllocationStrategy` is a subclass implementing `AllocationStrategy`, using a randomized approach to select a target from available storage segments. It supports setting a random seed to ensure deterministic allocation. In addition to random allocation, strategies can be defined based on actual needs, such as:
- Load-based allocation strategy: Prioritizes low-load segments based on current load information of storage segments.
- Topology-aware strategy: Prioritizes data segments that are physically closer to reduce network overhead.

### Eviction Policy

When the mounted segments are full, i.e., when a `PutStart` request fails due to insufficient memory, an eviction task will be launched to free up space by evicting some objects. Just like `Remove`, evicted objects are simply marked as deleted. No data transfer is needed.

Currently, an approximate LRU policy is adopted, where the least recently used objects are preferred for eviction. To avoid data races and corruption, objects currently being read or written by clients should not be evicted. For this reason, objects that have leases or have not been marked as complete by `PutEnd` requests will be ignored by the eviction task.

Each time the eviction task is triggered, in default it will try to evict about 10% of objects. This ratio is configurable via a startup parameter of `master_service`.

To minimize put failures, you can set the eviction high watermark via the `master_service` startup parameter `-eviction_high_watermark_ratio=<RATIO>`(Default to 1). When the eviction thread detects that current space usage reaches the configured high watermark,
it initiates evict operations. The eviction target is to clean an additional `-eviction_ratio` specified proportion beyond the high watermark, thereby reaching the space low watermark.

### Lease

To avoid data conflicts, a per-object lease will be granted whenever an `ExistKey` request or a `GetReplicaListRequest` request succeeds. An object is guaranteed to be protected from `Remove` request, `RemoveAll` request and `Eviction` task until its lease expires. A `Remove` request on a leased object will fail. A `RemoveAll` request will only remove objects without a lease.

The default lease TTL is 200 ms and is configurable via a startup parameter of `master_service`.

## Mooncake Store Python API

### setup
```python
def setup(
    self,
    local_hostname: str,
    metadata_server: str,
    global_segment_size: int = 16777216,  # 16MB
    local_buffer_size: int = 16777216,    # 16MB
    protocol: str = "tcp",
    rdma_devices: str = "",
    master_server_addr: str = "127.0.0.1:50051"
) -> int
```
Initializes storage configuration and network parameters.

**Parameters**  
- `local_hostname`: Hostname/IP of local node
- `metadata_server`: Address of metadata coordination server
- `global_segment_size`: Shared memory segment size (default 16MB)
- `local_buffer_size`: Local buffer allocation size (default 16MB)
- `protocol`: Communication protocol ("tcp" or "rdma")
- `rdma_devices`: RDMA device spec (format depends on implementation)
- `master_server_addr`: The address information of the Master (format: `IP:Port` for default mode and `etcd://IP:Port;IP:Port;...;IP:Port` for high availability mode)

**Returns**  
- `int`: Status code (0 = success, non-zero = error)

---

### initAll
```python
def initAll(
    self,
    protocol: str,
    device_name: str,
    mount_segment_size: int = 16777216  # 16MB
) -> int
```
Initializes distributed resources and establishes connections.

**Parameters**  
- `protocol`: Network protocol to use ("tcp"/"rdma")
- `device_name`: Hardware device identifier
- `mount_segment_size`: Memory segment size for mounting (default 16MB)

**Returns**  
- `int`: Status code (0 = success, non-zero = error)

---

### put
```python
def put(self, key: str, value: bytes) -> int
```
Stores a binary object in the distributed storage.

**Parameters**  
- `key`: Unique object identifier (string)
- `value`: Binary data to store (bytes-like object)

**Returns**  
- `int`: Status code (0 = success, non-zero = error)

---

### get
```python
def get(self, key: str) -> bytes
```
Retrieves an object from distributed storage.

**Parameters**  
- `key`: Object identifier to retrieve

**Returns**  
- `bytes`: Retrieved binary data

**Raises**  
- `KeyError`: If specified key doesn't exist

---

### remove
```python
def remove(self, key: str) -> int
```
Deletes an object from the storage system.

**Parameters**  
- `key`: Object identifier to remove

**Returns**  
- `int`: Status code (0 = success, non-zero = error)

---

### isExist
```python
def isExist(self, key: str) -> int
```
Checks object existence in the storage system.

**Parameters**  
- `key`: Object identifier to check

**Returns**  
- `int`: 
  - `1`: Object exists
  - `0`: Object doesn't exist
  - `-1`: Error occurred

---

### close
```python
def close(self) -> int
```
Cleans up all resources and terminates connections. Corresponds to `tearDownAll()`.

**Returns**  
- `int`: Status code (0 = success, non-zero = error)

### Usage Example
```python

from mooncake.store import MooncakeDistributedStore
store = MooncakeDistributedStore()
store.setup(
    local_hostname="IP:port",
    metadata_server="etcd://metadata server IP:port",
    protocol="rdma",
    ...
)
store.initAll(protocol="rdma", device_name="mlx5_0")

# Store configuration
store.put("kvcache1",  pickle.dumps(torch.Tensor(data)))

# Retrieve data
value = store.get("kvcache1")

store.close()
```

## Compilation and Usage
Mooncake Store is compiled together with other related components (such as the Transfer Engine).

For default mode:
```
mkdir build && cd build
cmake .. # default mode
make
sudo make install # Install Python interface support package
```

High availability mode:
```
mkdir build && cd build
cmake .. -DSTORE_USE_ETCD # compile etcd wrapper that depends on go
make
sudo make install # Install Python interface support package
```

### Starting the Transfer Engine's Metadata Service
Mooncake Store uses the Transfer Engine as its core transfer engine, so it is necessary to start the metadata service (etcd/redis/http). The startup and configuration of the `metadata` service can be referred to in the relevant sections of [Transfer Engine](./transfer-engine.md). **Special Note**: For the etcd service, by default, it only provides services for local processes. You need to modify the listening options (IP to 0.0.0.0 instead of the default 127.0.0.1). You can use commands like curl to verify correctness.

### Starting the Master Service
The Master Service runs as an independent process, provides gRPC interfaces externally, and is responsible for the metadata management of Mooncake Store (note that the Master Service does not reuse the metadata service of the Transfer Engine). The default listening port is `50051`. After compilation, you can directly run `mooncake_master` located in the `build/mooncake-store/src/` directory. After starting, the Master Service will output the following content in the log:
```
Starting Mooncake Master Service
Port: 50051
Max threads: 4
Master service listening on 0.0.0.0:50051
```

**High availability mode**:

HA mode relies on an etcd service for coordination. If Transfer Engine also uses etcd as its metadata service, the etcd cluster used by Mooncake Store can either be shared with or separate from the one used by Transfer Engine.

HA mode allows deployment of multiple master instances to eliminate the single point of failure. Each master instance must be started with the following parameters:
```
--enable-ha: enables high availability mode
--etcd-endpoints: specifies endpoints for etcd service, separated by ';'
--rpc-address: the RPC address of this instance
```

For example:
```
./build/mooncake-store/src/mooncake_master \
    --enable-ha=true \
    --etcd-endpoints="0.0.0.0:2379;0.0.0.0:2479;0.0.0.0:2579" \
    --rpc-address=10.0.0.1
```

### Starting the Sample Program
Mooncake Store provides various sample programs, including interface forms based on C++ and Python. Below is an example of how to run using `stress_cluster_benchmark`.

1. Open `stress_cluster_benchmark.py` and update the initialization settings based on your network environment. Pay particular attention to the following fields:
`local_hostname`: the IP address of the local machine
`metadata_server`: the address of the Transfer Engine metadata service
`master_server_address`: the address of the Master Service
**Note**: The format of `master_server_address` depends on the deployment mode. In default mode, use the format `IP:Port`, specifying the address of a single master node. In HA mode, use the format `etcd://IP:Port;IP:Port;...;IP:Port`, specifying the addresses of the etcd cluster endpoints.
For example: 
```python
import os
import time

from distributed_object_store import DistributedObjectStore

store = DistributedObjectStore()
# Protocol used by the transfer engine, optional values are "rdma" or "tcp"
protocol = os.getenv("PROTOCOL", "tcp")
# Device name used by the transfer engine
device_name = os.getenv("DEVICE_NAME", "ibp6s0")
# Hostname of this node in the cluster, port number is randomly selected from (12300-14300)
local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
# Metadata service address of the Transfer Engine, here etcd is used as the metadata service
metadata_server = os.getenv("METADATA_ADDR", "127.0.0.1:2379")
# The size of the Segment mounted by each node to the cluster, allocated by the Master Service after mounting, in bytes
global_segment_size = 3200 * 1024 * 1024
# Local buffer size registered with the Transfer Engine, in bytes
local_buffer_size = 512 * 1024 * 1024
# Address of the Master Service of Mooncake Store
master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
# Data length for each put()
value_length = 1 * 1024 * 1024
# Total number of requests sent
max_requests = 1000
# Initialize Mooncake Store Client
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

2. Run `ROLE=prefill python3 ./stress_cluster_benchmark.py` on one machine to start the Prefill node.
   For "rdma" protocol, you can also enable topology auto discovery and filters, e.g., `ROLE=prefill MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 ./stress_cluster_benchmark.py`

3. Run `ROLE=decode python3 ./stress_cluster_benchmark.py` on another machine to start the Decode node.
   For "rdma" protocol, you can also enable topology auto discovery and filters, e.g., `ROLE=decode MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 ./stress_cluster_benchmark.py`

The absence of error messages indicates successful data transfer.

## Example Code

#### Python Usage Example
We provide a reference example `distributed_object_store_provider.py`, located in the `mooncake-store/tests` directory. To check if the related components are properly installed, you can run etcd and Master Service (`mooncake_master`) in the background on the same server, and then execute this Python program in the foreground. It should output a successful test result.

#### C++ Usage Example
The C++ API of Mooncake Store provides more low-level control capabilities. We provide a reference example `client_integration_test`, located in the `mooncake-store/tests` directory. To check if the related components are properly installed, you can run etcd and Master Service (`mooncake_master`) on the same server, and then execute this C++ program (located in the `build/mooncake-store/tests` directory). It should output a successful test result.
