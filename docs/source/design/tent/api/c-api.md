# TENT C API Reference

This document provides a complete reference for the TENT C API. The C API is defined in [`mooncake-transfer-engine/tent/include/tent/transfer_engine.h`](../../../../../mooncake-transfer-engine/tent/include/tent/transfer_engine.h) and provides a C-compatible interface for applications that cannot or prefer not to use C++.

For conceptual background, see [TENT Overview](overview.md). For C++ API documentation, see [C++ API Reference](cpp-api.md).

## Header File

```c
#include "tent/transfer_engine.h"
```

## Types and Constants

### Engine Handle

```c
typedef void* tent_engine_t;
```

An opaque handle representing a TENT engine instance.

### Identifiers

```c
#define tent_batch_id_t uint64_t
#define tent_segment_id_t uint64_t
```

- `tent_batch_id_t`: Batch identifier for tracking transfer requests
- `tent_segment_id_t`: Segment identifier for data locations

### Local Segment ID

```c
#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif
```

Constant representing the local segment. Use this when referring to your own segment.

### Operation Codes

```c
#define OPCODE_READ (0)
#define OPCODE_WRITE (1)
```

Transfer operation types:
- `OPCODE_READ`: Read data from remote segment
- `OPCODE_WRITE`: Write data to remote segment

### Status Codes

```c
#define STATUS_WAITING    (0)
#define STATUS_PENDING    (1)
#define STATUS_INVALID    (2)
#define STATUS_CANCELED   (3)
#define STATUS_COMPLETED (4)
#define STATUS_TIMEOUT    (5)
#define STATUS_FAILED     (6)
```

Transfer status values:
- `STATUS_WAITING`: Batch is allocated but no requests submitted
- `STATUS_PENDING`: Transfer is in progress
- `STATUS_INVALID`: Invalid batch or task ID
- `STATUS_CANCELED`: Transfer was canceled
- `STATUS_COMPLETED`: Transfer completed successfully
- `STATUS_TIMEOUT`: Transfer timed out
- `STATUS_FAILED`: Transfer failed

### Memory Types

```c
#define TYPE_MEMORY (0)
#define TYPE_FILE  (1)
```

Segment types:
- `TYPE_MEMORY`: Memory segment (DRAM, VRAM)
- `TYPE_FILE`: File-backed segment (NVMe, local file)

### Permissions

```c
#define PERM_LOCAL_READ_WRITE  (0)
#define PERM_GLOBAL_READ_ONLY  (1)
#define PERM_GLOBAL_READ_WRITE (2)
```

Memory access permissions:
- `PERM_LOCAL_READ_WRITE`: Local access only (not accessible remotely)
- `PERM_GLOBAL_READ_ONLY`: Read-only access for remote peers
- `PERM_GLOBAL_READ_WRITE`: Full read/write access for remote peers

### Transport Types

```c
#define TRANSPORT_RDMA          (0)
#define TRANSPORT_MNNVL         (1)
#define TRANSPORT_SHM           (2)
#define TRANSPORT_NVLINK        (3)
#define TRANSPORT_GDS           (4)
#define TRANSPORT_IOURING       (5)
#define TRANSPORT_TCP           (6)
#define TRANSPORT_ASCEND_DIRECT (7)
#define TRANSPORT_UNSPEC        (8)
```

Transport backend types (used in `tent_memory_options_t`):
- `TRANSPORT_RDMA`: RDMA/InfiniBand/RoCE
- `TRANSPORT_MNNVL`: Moore Threads NVLink
- `TRANSPORT_SHM`: Shared memory
- `TRANSPORT_NVLINK`: NVIDIA NVLink
- `TRANSPORT_GDS`: GPUDirect Storage
- `TRANSPORT_IOURING`: Linux io_uring
- `TRANSPORT_TCP`: TCP/IP
- `TRANSPORT_ASCEND_DIRECT`: Ascend NPU direct transport
- `TRANSPORT_UNSPEC`: Unspecified/automatic

## Data Structures

### tent_request_t

```c
struct tent_request {
    int opcode;              /* OPCODE_READ or OPCODE_WRITE */
    void* source;            /* Local source/destination address */
    tent_segment_id_t target_id; /* Remote segment ID */
    uint64_t target_offset;  /* Offset in remote segment */
    uint64_t length;         /* Number of bytes to transfer */
    int priority;            /* 0=HIGH, 1=MEDIUM, 2=LOW */
};
```

Describes a single transfer request.

**Fields:**
- `opcode`: Operation type (read or write)
- `source`: Local memory address for data
- `target_id`: Remote segment identifier (from `tent_open_segment`)
- `target_offset`: Byte offset in the remote segment
- `length`: Number of bytes to transfer
- `priority`: Request priority level

### tent_status_t

```c
struct tent_status {
    int status;              /* STATUS_* code */
    uint64_t transferred_bytes; /* Bytes actually transferred */
};
```

Transfer completion status.

**Fields:**
- `status`: Status code (one of `STATUS_*` values)
- `transferred_bytes`: Actual number of bytes transferred

### tent_buffer_info_t

```c
struct tent_buffer_info {
    uint64_t base;           /* Base address of buffer */
    uint64_t length;         /* Length of buffer in bytes */
    char location[64];       /* Location string (e.g., "cpu:0", "cuda:0") */
};
```

Information about a single buffer within a segment.

### tent_segment_info_t

```c
struct tent_segment_info {
    int type;                /* TYPE_MEMORY or TYPE_FILE */
    int num_buffers;         /* Number of buffers in segment */
    struct tent_buffer_info* buffers; /* Array of buffer info */
};
```

Information about a segment.

**Fields:**
- `type`: Segment type (memory or file)
- `num_buffers`: Number of buffers in the segment
- `buffers`: Array of `tent_buffer_info` structures

**Note:** Call `tent_free_segment_info()` to free the `buffers` array when done.

### tent_notifi_record_t

```c
struct tent_notifi_record {
    tent_segment_id_t handle; /* Sender segment ID */
    char name[256];           /* Notification name */
    char msg[4096];           /* Notification message */
};
```

A single notification record.

### tent_notifi_info

```c
struct tent_notifi_info {
    int num_records;          /* Number of notifications */
    struct tent_notifi_record* records; /* Array of notifications */
};
```

Collection of notifications received from remote peers.

**Note:** Call `tent_free_notifs()` to free the `records` array when done.

### tent_memory_options_t

```c
struct tent_memory_options {
    char location[64];        /* Memory location (e.g., "cuda:0") */
    int permission;           /* PERM_* value */
    int transport_type;       /* TRANSPORT_* value */
    char shm_path[256];      /* SHM path for SHM transport */
    size_t shm_offset;       /* Offset in SHM region */
    int internal;            /* 0 = false, non-zero = true */
};
```

Advanced memory registration options.

**Fields:**
- `location`: Memory location identifier
- `permission`: Access permission
- `transport_type`: Preferred transport type
- `shm_path`: Path for shared memory transport
- `shm_offset`: Offset within shared memory region
- `internal`: Internal flag (usually 0)

## Configuration Functions

### tent_load_config_from_file

```c
void tent_load_config_from_file(const char* path);
```

Load TENT configuration from a JSON file. Must be called before `tent_create_engine()`.

**Parameters:**
- `path`: Path to configuration JSON file

**Example:**
```c
tent_load_config_from_file("/etc/tent/config.json");
tent_engine_t engine = tent_create_engine();
```

### tent_set_config

```c
void tent_set_config(const char* key, const char* value);
```

Set a configuration value programmatically. Overrides values from the configuration file.

**Parameters:**
- `key`: Configuration key (supports nested keys with `/` separator)
- `value`: Configuration value (string, numeric, or boolean)

**Example:**
```c
tent_set_config("metadata_type", "etcd");
tent_set_config("metadata_servers", "192.168.1.10:2379");
tent_set_config("transports/rdma/enable", "true");
```

## Engine Lifecycle

### tent_create_engine

```c
tent_engine_t tent_create_engine(void);
```

Create a new TENT engine instance.

**Returns:**
- Engine handle on success
- `NULL` on failure

**Example:**
```c
tent_engine_t engine = tent_create_engine();
if (engine == NULL) {
    fprintf(stderr, "Failed to create engine\n");
    return -1;
}
```

### tent_destroy_engine

```c
void tent_destroy_engine(tent_engine_t engine);
```

Destroy a TENT engine instance and release all resources.

**Parameters:**
- `engine`: Engine handle to destroy

**Example:**
```c
tent_destroy_engine(engine);
engine = NULL;
```

### tent_available

```c
int tent_available(tent_engine_t engine);
```

Check if the engine is available and ready for operations.

**Parameters:**
- `engine`: Engine handle

**Returns:**
- `1` if engine is available
- `0` if engine is not available

## Engine Information

### tent_segment_name

```c
int tent_segment_name(tent_engine_t engine, char* buf, size_t buf_len);
```

Get the local segment name.

**Parameters:**
- `engine`: Engine handle
- `buf`: Buffer to receive the segment name
- `buf_len`: Length of the buffer

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
char name[256];
if (tent_segment_name(engine, name, sizeof(name)) == 0) {
    printf("Local segment name: %s\n", name);
}
```

### tent_rpc_server_addr_port

```c
int tent_rpc_server_addr_port(tent_engine_t engine,
                              char* addr_buf,
                              size_t buf_len,
                              uint16_t* port);
```

Get the RPC server address and port.

**Parameters:**
- `engine`: Engine handle
- `addr_buf`: Buffer to receive the address
- `buf_len`: Length of the address buffer
- `port`: Pointer to receive the port number

**Returns:**
- `0` on success
- Negative value on failure

## Segment Management

### tent_open_segment

```c
int tent_open_segment(tent_engine_t engine,
                      tent_segment_id_t* handle,
                      const char* segment_name);
```

Open a segment for data transfer.

**Parameters:**
- `engine`: Engine handle
- `handle`: Pointer to receive the segment ID
- `segment_name`: Name of the segment to open

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
tent_segment_id_t remote_seg;
if (tent_open_segment(engine, &remote_seg, "peer_host:12345") != 0) {
    fprintf(stderr, "Failed to open segment\n");
    return -1;
}
```

### tent_close_segment

```c
int tent_close_segment(tent_engine_t engine, tent_segment_id_t handle);
```

Close a segment and release associated resources.

**Parameters:**
- `engine`: Engine handle
- `handle`: Segment ID to close

**Returns:**
- `0` on success
- Negative value on failure

### tent_get_segment_info

```c
int tent_get_segment_info(tent_engine_t engine,
                          tent_segment_id_t handle,
                          tent_segment_info_t* info);
```

Get information about a segment.

**Parameters:**
- `engine`: Engine handle
- `handle`: Segment ID
- `info`: Pointer to receive segment information

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
tent_segment_info_t info;
if (tent_get_segment_info(engine, remote_seg, &info) == 0) {
    printf("Segment type: %d, buffers: %d\n", info.type, info.num_buffers);
    for (int i = 0; i < info.num_buffers; i++) {
        printf("  Buffer %d: base=%lu, length=%lu, location=%s\n",
               i, info.buffers[i].base, info.buffers[i].length,
               info.buffers[i].location);
    }
    tent_free_segment_info(&info);
}
```

### tent_free_segment_info

```c
void tent_free_segment_info(tent_segment_info_t* info);
```

Free resources allocated by `tent_get_segment_info`.

**Parameters:**
- `info`: Segment information to free

## Memory Management

### tent_allocate_memory

```c
int tent_allocate_memory(tent_engine_t engine,
                         void** addr,
                         size_t size,
                         const char* location);
```

Allocate memory with automatic registration.

**Parameters:**
- `engine`: Engine handle
- `addr`: Pointer to receive the allocated address
- `size`: Size in bytes to allocate
- `location`: Memory location (e.g., "cpu:0", "cuda:0")

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
void* buffer = NULL;
size_t size = 1024 * 1024; // 1MB
if (tent_allocate_memory(engine, &buffer, size, "cpu:0") != 0) {
    fprintf(stderr, "Failed to allocate memory\n");
    return -1;
}
```

### tent_free_memory

```c
int tent_free_memory(tent_engine_t engine, void* addr);
```

Free memory allocated with `tent_allocate_memory`.

**Parameters:**
- `engine`: Engine handle
- `addr`: Address to free

**Returns:**
- `0` on success
- Negative value on failure

### tent_register_memory

```c
int tent_register_memory(tent_engine_t engine,
                          void* addr,
                          size_t size);
```

Register a memory region for data transfer.

**Parameters:**
- `engine`: Engine handle
- `addr`: Memory address
- `size`: Size in bytes

**Returns:**
- `0` on success
- Negative value on failure

**Note:** Memory must be registered before it can be used in transfer requests.

### tent_unregister_memory

```c
int tent_unregister_memory(tent_engine_t engine,
                            void* addr,
                            size_t size);
```

Unregister a memory region.

**Parameters:**
- `engine`: Engine handle
- `addr`: Memory address
- `size`: Size in bytes (can be 0 if size was provided during registration)

**Returns:**
- `0` on success
- Negative value on failure

### tent_register_memory_with_perm

```c
int tent_register_memory_with_perm(tent_engine_t engine,
                                   void* addr,
                                   size_t size,
                                   int permission);
```

Register memory with specific access permissions.

**Parameters:**
- `engine`: Engine handle
- `addr`: Memory address
- `size`: Size in bytes
- `permission`: One of `PERM_*` values

**Returns:**
- `0` on success
- Negative value on failure

### tent_register_memory_batch

```c
int tent_register_memory_batch(tent_engine_t engine,
                               void** addrs,
                               size_t* sizes,
                               size_t count,
                               int permission);
```

Register multiple memory regions in a single call.

**Parameters:**
- `engine`: Engine handle
- `addrs`: Array of memory addresses
- `sizes`: Array of sizes in bytes
- `count`: Number of memory regions
- `permission`: Access permission for all regions

**Returns:**
- `0` on success
- Negative value on failure

### tent_unregister_memory_batch

```c
int tent_unregister_memory_batch(tent_engine_t engine,
                                 void** addrs,
                                 size_t* sizes,
                                 size_t count);
```

Unregister multiple memory regions in a single call.

**Parameters:**
- `engine`: Engine handle
- `addrs`: Array of memory addresses
- `sizes`: Array of sizes in bytes (can be NULL)
- `count`: Number of memory regions

**Returns:**
- `0` on success
- Negative value on failure

### tent_allocate_memory_ex

```c
int tent_allocate_memory_ex(tent_engine_t engine,
                             void** addr,
                             size_t size,
                             tent_memory_options_t* opts);
```

Allocate memory with advanced options.

**Parameters:**
- `engine`: Engine handle
- `addr`: Pointer to receive the allocated address
- `size`: Size in bytes to allocate
- `opts`: Memory options

**Returns:**
- `0` on success
- Negative value on failure

### tent_register_memory_ex

```c
int tent_register_memory_ex(tent_engine_t engine,
                             void* addr,
                             size_t size,
                             tent_memory_options_t* opts);
```

Register memory with advanced options.

**Parameters:**
- `engine`: Engine handle
- `addr`: Memory address
- `size`: Size in bytes
- `opts`: Memory options

**Returns:**
- `0` on success
- Negative value on failure

### tent_register_memory_batch_ex

```c
int tent_register_memory_batch_ex(tent_engine_t engine,
                                  void** addrs,
                                  size_t* sizes,
                                  size_t count,
                                  tent_memory_options_t* opts);
```

Register multiple memory regions with advanced options.

**Parameters:**
- `engine`: Engine handle
- `addrs`: Array of memory addresses
- `sizes`: Array of sizes in bytes
- `count`: Number of memory regions
- `opts`: Memory options (applied to all regions)

**Returns:**
- `0` on success
- Negative value on failure

## Transfer Operations

### tent_allocate_batch

```c
tent_batch_id_t tent_allocate_batch(tent_engine_t engine,
                                    size_t batch_size);
```

Allocate a batch for submitting transfer requests.

**Parameters:**
- `engine`: Engine handle
- `batch_size`: Maximum number of requests in the batch

**Returns:**
- Batch ID on success
- `0` on failure

**Example:**
```c
tent_batch_id_t batch = tent_allocate_batch(engine, 100);
if (batch == 0) {
    fprintf(stderr, "Failed to allocate batch\n");
    return -1;
}
```

### tent_free_batch

```c
int tent_free_batch(tent_engine_t engine,
                    tent_batch_id_t batch_id);
```

Free a batch and release associated resources.

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID to free

**Returns:**
- `0` on success
- Negative value on failure

### tent_submit

```c
int tent_submit(tent_engine_t engine,
                tent_batch_id_t batch_id,
                tent_request_t* entries,
                size_t count);
```

Submit transfer requests to a batch.

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID
- `entries`: Array of transfer requests
- `count`: Number of requests

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
tent_request_t req;
req.opcode = OPCODE_WRITE;
req.source = local_buffer;
req.target_id = remote_segment;
req.target_offset = 0;
req.length = buffer_size;
req.priority = 1; // MEDIUM

if (tent_submit(engine, batch, &req, 1) != 0) {
    fprintf(stderr, "Failed to submit request\n");
    return -1;
}
```

### tent_submit_notif

```c
int tent_submit_notif(tent_engine_t engine,
                      tent_batch_id_t batch_id,
                      tent_request_t* entries,
                      size_t count,
                      const char* name,
                      const char* message);
```

Submit transfer requests with a notification to be sent upon completion.

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID
- `entries`: Array of transfer requests
- `count`: Number of requests
- `name`: Notification name
- `message`: Notification message

**Returns:**
- `0` on success
- Negative value on failure

### tent_task_status

```c
int tent_task_status(tent_engine_t engine,
                      tent_batch_id_t batch_id,
                      size_t task_id,
                      tent_status_t* status);
```

Get the status of a single task in a batch.

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID
- `task_id`: Task index within the batch (0-based)
- `status`: Pointer to receive the status

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
tent_status_t status;
if (tent_task_status(engine, batch, 0, &status) == 0) {
    printf("Task status: %d, bytes: %lu\n",
           status.status, status.transferred_bytes);
}
```

### tent_overall_status

```c
int tent_overall_status(tent_engine_t engine,
                        tent_batch_id_t batch_id,
                        tent_status_t* status);
```

Get the overall status of a batch (aggregates all tasks).

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID
- `status`: Pointer to receive the overall status

**Returns:**
- `0` on success
- Negative value on failure

### tent_task_status_list

```c
int tent_task_status_list(tent_engine_t engine,
                          tent_batch_id_t batch_id,
                          tent_status_t* statuses,
                          size_t* count);
```

Get the status of all tasks in a batch.

**Parameters:**
- `engine`: Engine handle
- `batch_id`: Batch ID
- `statuses`: Array to receive the statuses
- `count`: Input: size of array; Output: number of statuses written

**Returns:**
- `0` on success
- Negative value on failure

## Notification Operations

### tent_send_notifs

```c
int tent_send_notifs(tent_engine_t engine,
                      tent_segment_id_t handle,
                      const char* name,
                      const char* message);
```

Send a notification to a remote segment.

**Parameters:**
- `engine`: Engine handle
- `handle`: Target segment ID
- `name`: Notification name
- `message`: Notification message

**Returns:**
- `0` on success
- Negative value on failure

### tent_recv_notifs

```c
int tent_recv_notifs(tent_engine_t engine,
                      tent_notifi_info* info);
```

Receive pending notifications.

**Parameters:**
- `engine`: Engine handle
- `info`: Pointer to receive the notifications

**Returns:**
- `0` on success
- Negative value on failure

**Example:**
```c
tent_notifi_info notifications;
if (tent_recv_notifs(engine, &notifications) == 0) {
    for (int i = 0; i < notifications.num_records; i++) {
        printf("Notification from %lu: %s - %s\n",
               notifications.records[i].handle,
               notifications.records[i].name,
               notifications.records[i].msg);
    }
    tent_free_notifs(&notifications);
}
```

### tent_free_notifs

```c
void tent_free_notifs(tent_notifi_info* info);
```

Free resources allocated by `tent_recv_notifs`.

**Parameters:**
- `info`: Notification information to free

## Complete Example

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tent/transfer_engine.h"

int main(int argc, char** argv) {
    // Load configuration
    tent_load_config_from_file("./transfer-engine.json");

    // Create engine
    tent_engine_t engine = tent_create_engine();
    if (engine == NULL) {
        fprintf(stderr, "Failed to create engine\n");
        return -1;
    }

    // Check availability
    if (!tent_available(engine)) {
        fprintf(stderr, "Engine not available\n");
        tent_destroy_engine(engine);
        return -1;
    }

    // Get local segment name
    char local_name[256];
    if (tent_segment_name(engine, local_name, sizeof(local_name)) == 0) {
        printf("Local segment: %s\n", local_name);
    }

    // Allocate and register memory
    void* buffer = NULL;
    size_t buffer_size = 1024 * 1024; // 1MB
    if (tent_allocate_memory(engine, &buffer, buffer_size, "cpu:0") != 0) {
        fprintf(stderr, "Failed to allocate memory\n");
        tent_destroy_engine(engine);
        return -1;
    }

    // Initialize buffer
    memset(buffer, 0xAB, buffer_size);

    // Open remote segment
    tent_segment_id_t remote_seg;
    const char* remote_name = (argc > 1) ? argv[1] : "peer:12345";
    if (tent_open_segment(engine, &remote_seg, remote_name) != 0) {
        fprintf(stderr, "Failed to open remote segment: %s\n", remote_name);
        tent_free_memory(engine, buffer);
        tent_destroy_engine(engine);
        return -1;
    }

    // Allocate batch
    tent_batch_id_t batch = tent_allocate_batch(engine, 10);
    if (batch == 0) {
        fprintf(stderr, "Failed to allocate batch\n");
        tent_close_segment(engine, remote_seg);
        tent_free_memory(engine, buffer);
        tent_destroy_engine(engine);
        return -1;
    }

    // Prepare transfer request
    tent_request_t req;
    req.opcode = OPCODE_WRITE;
    req.source = buffer;
    req.target_id = remote_seg;
    req.target_offset = 0;
    req.length = buffer_size;
    req.priority = 1; // MEDIUM

    // Submit request
    if (tent_submit(engine, batch, &req, 1) != 0) {
        fprintf(stderr, "Failed to submit request\n");
        tent_free_batch(engine, batch);
        tent_close_segment(engine, remote_seg);
        tent_free_memory(engine, buffer);
        tent_destroy_engine(engine);
        return -1;
    }

    // Wait for completion
    tent_status_t status;
    while (1) {
        if (tent_task_status(engine, batch, 0, &status) != 0) {
            fprintf(stderr, "Failed to get status\n");
            break;
        }

        if (status.status == STATUS_COMPLETED) {
            printf("Transfer completed: %lu bytes\n", status.transferred_bytes);
            break;
        } else if (status.status == STATUS_FAILED) {
            fprintf(stderr, "Transfer failed\n");
            break;
        }

        // Sleep a bit before polling again
        usleep(1000);
    }

    // Cleanup
    tent_free_batch(engine, batch);
    tent_close_segment(engine, remote_seg);
    tent_free_memory(engine, buffer);
    tent_destroy_engine(engine);

    return 0;
}
```

## Return Value Convention

All functions that return `int` follow this convention:

- **>= 0**: Success (0 is most common, some functions may return positive values)
- **< 0**: Error (negative values indicate various error conditions)

## Thread Safety

- **Engine handle**: Thread-safe for all operations
- **Batch operations**: Thread-safe within the same batch
- **Memory registration**: Thread-safe
- **Segment operations**: Thread-safe

Multiple threads can safely use the same engine handle concurrently.

## Error Handling

When a function returns a negative value:
1. Check engine availability with `tent_available()`
2. Verify configuration is correct
3. Ensure memory is properly registered
4. Check network connectivity for remote operations

Common error scenarios:
- `tent_create_engine()` returns `NULL`: Configuration error or metadata service unavailable
- `tent_open_segment()` fails: Remote segment not found or unreachable
- `tent_submit()` fails: Invalid request parameters or insufficient resources
- `tent_task_status()` returns `STATUS_FAILED`: Transfer failed (check failover configuration)

## See Also

- [TENT Overview](../overview.md) - Conceptual introduction to TENT
- [C++ API Reference](cpp-api.md) - C++ API documentation
- [Configuration Reference](../configuration/configuration.md) - Configuration file reference
- [Transport Selector](../features/transport-selector.md) - Transport selection policies
- [Failover](../features/failover.md) - Failure handling and recovery
