# Mooncake Store C++ API Reference

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
tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                  std::vector<Slice>& slices);
```

`Get` retrieves the value of `object_key` into the provided `slices`. The returned data is guaranteed to be complete and correct. Each slice must reference local DRAM/VRAM memory that has been pre-registered with `registerLocalMemory(addr, len)` (not the global segments that contribute to the distributed memory pool). When persistence is enabled and the requested data is not found in the distributed memory pool, `Get` will fall back to loading the data from SSD.

### Put

```C++
tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                  std::vector<Slice>& slices,
                                  const ReplicateConfig& config);
```

`Put` stores the value associated with `key` in the distributed memory pool. The `config` parameter allows specifying the required number of replicas as well as the preferred segment for storing the value. When persistence is enabled, `Put` also asynchronously triggers a persistence operation to SSD.

**Replication Guarantees and Best Effort Behavior:**
- Each slice of an object is guaranteed to be replicated to different segments, ensuring distribution across separate storage nodes
- Different slices from different objects may be placed in the same segment
- Replication operates on a best-effort basis: if insufficient space is available for all requested replicas, the object will still be written with as many replicas as possible

The data structure details of `ReplicateConfig` are as follows:

```C++
struct ReplicateConfig {
    size_t replica_num{1};                    // Total number of replicas for the object
    bool with_soft_pin{false};               // Whether to enable soft pin mechanism for this object
    bool with_hard_pin{false};               // Whether to enable hard pin (never evicted)
    std::string preferred_segment{};         // Preferred segment for allocation
};
```

### Upsert

```C++
tl::expected<void, ErrorCode> Upsert(const ObjectKey& key,
                                     std::vector<Slice>& slices,
                                     const ReplicateConfig& config);

std::vector<tl::expected<void, ErrorCode>> BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config);
```

`Upsert` inserts `key` if it does not exist and updates the existing object if
it does. It uses the same replication configuration model as `Put`, while
allowing the store to reuse existing placement for in-place updates when the
current layout permits it. `BatchUpsert` performs the same operation for
multiple keys using a shared replication configuration.

### Remove

```C++
tl::expected<void, ErrorCode> Remove(const ObjectKey& key);
```

Used to delete the object corresponding to the specified key. This interface marks all data replicas associated with the key in the storage engine as deleted, without needing to communicate with the corresponding storage node (Client).

### CreateCopyTask

```C++
tl::expected<UUID, ErrorCode> CreateCopyTask(
    const std::string& key,
    const std::vector<std::string>& targets);
```

`CreateCopyTask` creates an asynchronous copy task that will be executed by the client's task execution system. This is useful when you want to submit multiple copy operations without waiting for each one to complete. The task is submitted to the master service, assigned a unique task ID, and executed asynchronously by an available client. The task status can be queried using `QueryTask`.

**Task Execution and Result Reporting:**
1. **Task Assignment**: The master service assigns the task to an available client during the client's periodic ping operation
2. **Task Execution**: The assigned client executes the copy operation asynchronously in a background thread pool
3. **Result Reporting**: Upon completion (success or failure), the client automatically reports the result to the master service via `MarkTaskToComplete`:
   - On success: `status = SUCCESS`, `message = "Task completed successfully"`
   - On failure: `status = FAILED`, `message = <error description>`
4. **Status Query**: You can query the task status at any time using `QueryTask` to monitor progress

### CreateMoveTask

```C++
tl::expected<UUID, ErrorCode> CreateMoveTask(
    const std::string& key,
    const std::string& source,
    const std::string& target);
```

`CreateMoveTask` creates an asynchronous move task that will be executed by the client's task execution system. This is useful when you want to submit multiple move operations without waiting for each one to complete. The task is submitted to the master service, assigned a unique task ID, and executed asynchronously by an available client. The task status can be queried using `QueryTask`.

**Task Execution and Result Reporting:**
1. **Task Assignment**: The master service assigns the task to an available client during the client's periodic ping operation
2. **Task Execution**: The assigned client executes the move operation asynchronously in a background thread pool
3. **Result Reporting**: Upon completion (success or failure), the client automatically reports the result to the master service via `MarkTaskToComplete`:
   - On success: `status = SUCCESS`, `message = "Task completed successfully"`
   - On failure: `status = FAILED`, `message = <error description>`
4. **Status Query**: You can query the task status at any time using `QueryTask` to monitor progress

### QueryTask

```C++
tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);
```

`QueryTask` queries the status of an asynchronous task (copy or move). This allows you to monitor the progress of task-based operations. The response includes task status, type, creation time, last update time, assigned client, and status message.

The data structure details of `QueryTaskResponse` are as follows:

```C++
struct QueryTaskResponse {
    UUID id;                                    // Task UUID
    TaskType type;                              // Task type (REPLICA_COPY or REPLICA_MOVE)
    TaskStatus status;                          // Task status (PENDING, PROCESSING, SUCCESS, or FAILED)
    int64_t created_at_ms_epoch;                // Task creation timestamp in milliseconds
    int64_t last_updated_at_ms_epoch;           // Last update timestamp in milliseconds
    UUID assigned_client;                       // UUID of the client assigned to execute the task
    std::string message;                        // Status message or error description
};
```

### BatchQueryIp

```C++
tl::expected<std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>, ErrorCode>
BatchQueryIp(const std::vector<UUID>& client_ids);
```

Used to batch query the IP addresses for multiple client IDs. For each client ID in the input list, this interface retrieves the unique IP addresses from all segments mounted by that client. The operation is performed on the Master Service and returns a map from client ID to their IP address lists. Only client IDs that have successfully mounted segments are included in the result map. This is useful for discovering the network locations of storage nodes in the cluster.

### BatchReplicaClear

```C++
tl::expected<std::vector<std::string>, ErrorCode>
BatchReplicaClear(const std::vector<std::string>& object_keys,
                  const UUID& client_id,
                  const std::string& segment_name);
```

Used to batch clear replicas for multiple object keys belonging to a specific client ID. This interface allows clearing replicas either on a specific segment or across all segments. If segment_name is empty, all replicas of the specified objects are cleared (the objects are deleted entirely). If segment_name is provided, only replicas located on that specific segment are cleared. The operation is performed on the Master Service and returns a list of object keys that were successfully cleared. Only objects that belong to the specified `client_id`, have expired leases, and meet the clearing criteria are processed. This is useful for managing storage resources and cleaning up data on specific storage nodes.

### QueryByRegex

```C++
tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>, ErrorCode>
QueryByRegex(const std::string& str);
```

Used to query the replica information for all objects whose keys match the given regular expression. This is useful for batch operations or for retrieving a group of related objects. The operation is performed on the Master and returns a map of keys to their replica lists.

### RemoveByRegex

```C++
tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str);
```

Used to delete all objects from the store whose keys match the specified regular expression. This provides a powerful way to perform bulk deletions. The command returns the number of objects that were successfully removed.
