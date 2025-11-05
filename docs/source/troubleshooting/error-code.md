# Error Code Explanation

## Mooncake TransferEngine

TransferEngine may generate various types of errors during execution. For most APIs, the return value indicates the error reason. For details, refer to `mooncake-transfer-engine/include/error.h`.

| Group      | Return Value                 | Description                                                                                               |
|------------|-----------------------------|-----------------------------------------------------------------------------------------------------------|
| Normal     | 0                           | Normal execution                                                                                           |
| Error Args | ERR_INVALID_ARGUMENT        | Input parameters are incorrect (and cannot be detailed into other items in this group)                  |
|            | ERR_TOO_MANY_REQUESTS       | The number of requests passed by the user when calling the `SubmitTransfer` interface exceeds the maximum value specified by the allocated `BatchID` |
|            | ERR_ADDRESS_NOT_REGISTERED  | The source address and/or target address in the request initiated by the user are not registered (including the situation where it has been registered locally but not uploaded to the metadata server) |
|            | ERR_BATCH_BUSY              | Reclaim a `BatchID` that is currently executing a request                                    |
|            | ERR_DEVICE_NOT_FOUND        | No available RDMA device to execute the user's request                                                    |
|            | ERR_ADDRESS_OVERLAPPED      | Registering overlapping memory areas multiple times                                                        |
| Handshake  | ERR_DNS                     | Local server name is not a valid DNS hostname or IP address, preventing other nodes from handshaking with this node |
|            | ERR_SOCKET                  | Errors related to TCP Socket during the handshake process                                             |
|            | ERR_MALFORMED_JSON          | Data format error during handshake exchange                                                                |
|            | ERR_REJECT_HANDSHAKE        | Peer rejects the handshake due to peer's errors                             |
| Other      | ERR_METADATA                | Failed to communicate with metadata server                                                           |
|            | ERR_ENDPOINT                | Exceptions during the creation and use of `RdmaEndPoint` objects                                          |
|            | ERR_NUMA                    | The system does not support numa interface                                                                 |
|            | ERR_CLOCK                   | The system does not support `clock_gettime` interface                                                         |
|            | ERR_MEMORY                  | Out of memory                                                                            |

## Mooncake Store

Mooncake Store may generate various types of errors during execution. For most APIs, the return value indicates the error reason. For details, refer to `mooncake-store/include/types.h`.

| Group                    | Return Value                    | Description                                                                                               |
|--------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------|
| Normal                   | 0                              | Operation successful                                                                                       |
| Internal                 | INTERNAL_ERROR (-1)            | Internal error occurred                                                                                    |
| Buffer Allocation        | BUFFER_OVERFLOW (-10)          | Insufficient buffer space                                                                                  |
| Segment Selection        | SHARD_INDEX_OUT_OF_RANGE (-100)| Shard index is out of bounds                                                                              |
|                          | SEGMENT_NOT_FOUND (-101)       | No available segments found                                                                               |
|                          | SEGMENT_ALREADY_EXISTS (-102)  | Segment already exists                                                                                    |
| Handle Selection         | NO_AVAILABLE_HANDLE (-200)     | Memory allocation failed due to insufficient space                                                        |
| Version                  | INVALID_VERSION (-300)         | Invalid version                                                                                           |
| Key                      | INVALID_KEY (-400)             | Invalid key                                                                                              |
| Engine                   | WRITE_FAIL (-500)              | Write operation failed                                                                                    |
| Parameter                | INVALID_PARAMS (-600)          | Invalid parameters                                                                                        |
| Engine Operation         | INVALID_WRITE (-700)           | Invalid write operation                                                                                   |
|                          | INVALID_READ (-701)            | Invalid read operation                                                                                    |
|                          | INVALID_REPLICA (-702)         | Invalid replica operation                                                                                 |
| Object                   | REPLICA_IS_NOT_READY (-703)    | Replica is not ready                                                                                      |
|                          | OBJECT_NOT_FOUND (-704)        | Object not found                                                                                          |
|                          | OBJECT_ALREADY_EXISTS (-705)   | Object already exists                                                                                     |
|                          | OBJECT_HAS_LEASE (-706)        | Object has lease                                                                                          |
|                          | LEASE_EXPIRED (-707)           | Lease expired before data transfer completed                                                              |
| Transfer                 | TRANSFER_FAIL (-800)           | Transfer operation failed                                                                                 |
| RPC                      | RPC_FAIL (-900)                | RPC operation failed                                                                                      |
| High Availability        | ETCD_OPERATION_ERROR (-1000)   | etcd operation failed                                                                                     |
|                          | ETCD_KEY_NOT_EXIST (-1001)     | Key not found in etcd                                                                                    |
|                          | ETCD_TRANSACTION_FAIL (-1002)  | etcd transaction failed                                                                                   |
|                          | ETCD_CTX_CANCELLED (-1003)     | etcd context cancelled                                                                                    |
|                          | UNAVAILABLE_IN_CURRENT_STATUS (-1010) | Request cannot be done in current status                                                      |
|                          | UNAVAILABLE_IN_CURRENT_MODE (-1011)   | Request cannot be done in current mode                                                           |
| File                     | FILE_NOT_FOUND (-1100)         | File not found                                                                                            |
|                          | FILE_OPEN_FAIL (-1101)         | Error opening file or writing to an existing file                                                        |
|                          | FILE_READ_FAIL (-1102)         | Error reading file                                                                                        |
|                          | FILE_WRITE_FAIL (-1103)        | Error writing file                                                                                        |
|                          | FILE_INVALID_BUFFER (-1104)    | File buffer is wrong                                                                                      |
|                          | FILE_LOCK_FAIL (-1105)         | File lock operation failed                                                                                |
|                          | FILE_INVALID_HANDLE (-1106)    | Invalid file handle                                                                                       |