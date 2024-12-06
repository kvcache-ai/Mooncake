# Error Code Explanation

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
