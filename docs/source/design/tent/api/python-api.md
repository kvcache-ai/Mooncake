# TENT Python API Reference

The TENT Python API provides a Pythonic interface to the Transfer Engine NEXT runtime. It is different from the classic Transfer Engine Python API and uses the `tent` module.

## Installation

```bash
pip install mooncake-transfer-engine
```

## Module Import

```python
import tent
```

## Exceptions

TENT uses a custom exception hierarchy that inherits from `TentException`:

| Exception | Base | When Thrown |
|-----------|------|-------------|
| `TentException` | Exception | Base exception for all TENT errors |
| `InvalidArgumentError` | TentException | Invalid function arguments |
| `AddressNotRegisteredError` | TentException | Memory address not registered |
| `DeviceNotFoundError` | TentException | Requested device not found |
| `InvalidEntryError` | TentException | Invalid entry (batch, task, segment) |
| `RdmaError` | TentException | RDMA operation failed |
| `CudaError` | TentException | CUDA operation failed |
| `MetadataError` | TentException | Metadata operation failed |
| `RpcServiceError` | TentException | RPC service error |
| `InternalError` | TentException | Internal error |
| `NotImplementedError` | TentException | Feature not implemented |

**Example:**
```python
try:
    segment_id = engine.open_segment("remote:12345")
except MetadataError as e:
    print(f"Metadata error: {e}")
except RdmaError as e:
    print(f"RDMA error: {e}")
```

## Constants

### Segment Constants

```python
tent.LOCAL_SEGMENT_ID  # 0 - Local segment identifier
```

### Location Constants

```python
tent.kWildcardLocation  # "*" - Wildcard for any location
```

### Priority Constants

```python
tent.PRIO_HIGH    # 0 - High priority
tent.PRIO_MEDIUM  # 1 - Medium priority (default)
tent.PRIO_LOW     # 2 - Low priority
```

## Enums

### OpCode

Transfer operation types:

```python
tent.OpCode.READ   # Read operation
tent.OpCode.WRITE  # Write operation
```

### TransferStatusEnum

Transfer status values:

```python
tent.TransferStatusEnum.INITIAL    # Not yet submitted
tent.TransferStatusEnum.PENDING    # In progress
tent.TransferStatusEnum.INVALID    # Invalid entry
tent.TransferStatusEnum.CANCELED   # Canceled
tent.TransferStatusEnum.COMPLETED  # Completed successfully
tent.TransferStatusEnum.TIMEOUT    # Timed out
tent.TransferStatusEnum.FAILED     # Failed
```

### Permission

Memory access permissions:

```python
tent.Permission.LocalReadWrite     # Local access only
tent.Permission.GlobalReadOnly     # Remote read-only
tent.Permission.GlobalReadWrite    # Full remote access (default)
```

### TransportType

Transport backend types:

```python
tent.TransportType.RDMA           # RDMA/InfiniBand/RoCE
tent.TransportType.MNNVL          # Moore Threads NVLink
tent.TransportType.SHM            # Shared memory
tent.TransportType.NVLINK         # NVIDIA NVLink
tent.TransportType.GDS            # GPUDirect Storage
tent.TransportType.IOURING        # Linux io_uring
tent.TransportType.TCP            # TCP/IP
tent.TransportType.AscendDirect   # Ascend NPU direct
tent.TransportType.UNSPEC         # Unspecified/automatic
```

### SegmentInfoType

Segment types:

```python
tent.SegmentInfoType.Memory  # Memory segment
tent.SegmentInfoType.File    # File-backed segment
```

## Classes

### TransferEngine

Main engine class for data transfer operations.

#### Constructor

```python
engine = tent.TransferEngine()
# or with config path
engine = tent.TransferEngine("/path/to/config.json")
```

#### Information Methods

##### available()

```python
is_available = engine.available()
```

Check if the engine is available and ready.

**Returns:** `bool` - `True` if available

##### get_segment_name()

```python
segment_name = engine.get_segment_name()
```

Get the local segment name.

**Returns:** `str` - Local segment name

##### get_rpc_server_address()

```python
address = engine.get_rpc_server_address()
```

Get the RPC server address.

**Returns:** `str` - RPC server address

##### get_rpc_server_port()

```python
port = engine.get_rpc_server_port()
```

Get the RPC server port.

**Returns:** `int` - RPC server port

#### Segment Operations

##### export_local_segment()

```python
shared_handle = engine.export_local_segment()
```

Export the local segment as a shared handle.

**Returns:** `str` - Shared handle string

##### import_remote_segment()

```python
segment_id = engine.import_remote_segment(shared_handle)
```

Import a remote segment using its shared handle.

**Parameters:**
- `shared_handle` (str): Shared handle string

**Returns:** `int` - Segment ID

##### open_segment()

```python
segment_id = engine.open_segment(segment_name)
```

Open a remote segment by name.

**Parameters:**
- `segment_name` (str): Segment name in "hostname:port" format

**Returns:** `int` - Segment ID

**Raises:** `MetadataError` if segment not found

##### close_segment()

```python
engine.close_segment(segment_id)
```

Close a segment.

**Parameters:**
- `segment_id` (int): Segment ID to close

##### get_segment_info()

```python
info = engine.get_segment_info(segment_id)
```

Get information about a segment.

**Parameters:**
- `segment_id` (int): Segment ID

**Returns:** `SegmentInfo` object

#### Memory Allocation

##### allocate_local_memory()

```python
address = engine.allocate_local_memory(size, location="*")
```

Allocate memory with automatic registration.

**Parameters:**
- `size` (int): Size in bytes
- `location` (str, optional): Memory location (default: "*")

**Returns:** `int` - Memory address as integer

##### allocate_local_memory_ex()

```python
address = engine.allocate_local_memory_ex(size, options)
```

Allocate memory with advanced options.

**Parameters:**
- `size` (int): Size in bytes
- `options` (MemoryOptions): Memory options

**Returns:** `int` - Memory address as integer

##### free_local_memory()

```python
engine.free_local_memory(address)
```

Free previously allocated memory.

**Parameters:**
- `address` (int): Memory address

#### Memory Registration

##### register_local_memory()

```python
engine.register_local_memory(address, size, permission=tent.Permission.GlobalReadWrite)
```

Register memory for data transfer.

**Parameters:**
- `address` (int): Memory address
- `size` (int): Size in bytes
- `permission` (Permission, optional): Access permission

##### unregister_local_memory()

```python
engine.unregister_local_memory(address, size=0)
```

Unregister memory.

**Parameters:**
- `address` (int): Memory address
- `size` (int, optional): Size in bytes (0 = use original size)

##### register_local_memory_batch()

```python
engine.register_local_memory_batch(address_list, size_list, permission)
```

Register multiple memory regions.

**Parameters:**
- `address_list` (List[int]): List of addresses
- `size_list` (List[int]): List of sizes
- `permission` (Permission, optional): Access permission

##### unregister_local_memory_batch()

```python
engine.unregister_local_memory_batch(address_list, size_list)
```

Unregister multiple memory regions.

**Parameters:**
- `address_list` (List[int]): List of addresses
- `size_list` (List[int], optional): List of sizes

##### register_local_memory_ex()

```python
engine.register_local_memory_ex(address, size, options)
```

Register memory with advanced options.

**Parameters:**
- `address` (int): Memory address
- `size` (int): Size in bytes
- `options` (MemoryOptions): Memory options

##### register_local_memory_batch_ex()

```python
engine.register_local_memory_batch_ex(address_list, size_list, options)
```

Register multiple memory regions with advanced options.

**Parameters:**
- `address_list` (List[int]): List of addresses
- `size_list` (List[int]): List of sizes
- `options` (MemoryOptions): Memory options

#### RAII Memory Management

##### allocate_memory_guard()

```python
with engine.allocate_memory_guard(size, location="*") as address:
    # Use address for transfers
    pass
# Memory automatically freed
```

Allocate memory with automatic cleanup (context manager).

**Parameters:**
- `size` (int): Size in bytes
- `location` (str, optional): Memory location

**Returns:** `MemoryGuard` - Context manager

##### allocate_memory_guard_ex()

```python
with engine.allocate_memory_guard_ex(size, options) as address:
    # Use address for transfers
    pass
# Memory automatically freed
```

Allocate memory with options and automatic cleanup.

**Parameters:**
- `size` (int): Size in bytes
- `options` (MemoryOptions): Memory options

**Returns:** `MemoryGuard` - Context manager

#### Batch Operations

##### allocate_transfer_batch()

```python
batch_id = engine.allocate_transfer_batch(batch_size)
```

Allocate a transfer batch.

**Parameters:**
- `batch_size` (int): Maximum number of requests

**Returns:** `int` - Batch ID

##### free_transfer_batch()

```python
engine.free_transfer_batch(batch_id)
```

Free a transfer batch.

**Parameters:**
- `batch_id` (int): Batch ID to free

##### allocate_batch_guard()

```python
with engine.allocate_batch_guard(100) as batch_id:
    # Submit transfers
    pass
# Batch automatically freed
```

Allocate batch with automatic cleanup (context manager).

**Parameters:**
- `batch_size` (int): Maximum number of requests

**Returns:** `BatchGuard` - Context manager

#### Transfer Submission

##### submit_transfer()

```python
engine.submit_transfer(batch_id, request_list)
# or with notification
engine.submit_transfer(batch_id, request_list, notification)
```

Submit transfer requests.

**Parameters:**
- `batch_id` (int): Batch ID
- `request_list` (List[Request]): List of transfer requests
- `notification` (Notification, optional): Notification to send

##### submit_transfer_notif()

```python
engine.submit_transfer_notif(batch_id, request_list, name, message)
```

Submit transfers with a string notification.

**Parameters:**
- `batch_id` (int): Batch ID
- `request_list` (List[Request]): List of transfer requests
- `name` (str): Notification name
- `message` (str): Notification message

#### Notification Operations

##### send_notifi()

```python
engine.send_notifi(target_segment_id, notification)
```

Send a notification to a remote segment.

**Parameters:**
- `target_segment_id` (int): Target segment ID
- `notification` (Notification): Notification object

##### recv_notifi()

```python
notifications = engine.recv_notifi()
```

Receive pending notifications.

**Returns:** `List[Notification]` - List of received notifications

#### Status Queries

##### get_transfer_status()

```python
status = engine.get_transfer_status(batch_id, task_id)
```

Get status of a specific task.

**Parameters:**
- `batch_id` (int): Batch ID
- `task_id` (int): Task index (0-based)

**Returns:** `TransferStatus` object

##### get_transfer_status_list()

```python
status_list = engine.get_transfer_status_list(batch_id)
```

Get status of all tasks in a batch.

**Parameters:**
- `batch_id` (int): Batch ID

**Returns:** `List[TransferStatus]` - List of status objects

##### get_transfer_status_overall()

```python
status = engine.get_transfer_status_overall(batch_id)
```

Get overall batch status.

**Parameters:**
- `batch_id` (int): Batch ID

**Returns:** `TransferStatus` object

## Data Structures

### Notification

Represents a notification message.

```python
notif = tent.Notification()
# or with constructor
notif = tent.Notification("name", "message")

# Properties
notif.name  # Notification name
notif.msg   # Notification message
```

### Request

Represents a transfer request.

```python
req = tent.Request()
# or with constructor
req = tent.Request(
    opcode,      # OpCode.READ or WRITE
    source,      # Source address (int)
    target_id,   # Target segment ID (int)
    target_offset,  # Offset in target segment (int)
    length,      # Number of bytes (int)
    priority     # Priority (int, default=PRIO_HIGH)
)

# Properties
req.opcode        # OpCode enum
req.source        # Source address (int)
req.target_id     # Target segment ID
req.target_offset # Target offset
req.length        # Transfer length
req.priority      # Priority level
```

### TransferStatus

Represents transfer completion status.

```python
status.state  # TransferStatusEnum value
status.bytes  # Bytes transferred
```

### SegmentInfo

Segment information.

```python
info.type     # SegmentInfoType enum
info.buffers  # List of SegmentInfoBuffer
```

### SegmentInfoBuffer

Buffer information within a segment.

```python
buffer.base     # Base address (int)
buffer.length   # Buffer length (int)
buffer.location # Location string
```

### MemoryOptions

Advanced memory registration options.

```python
options = tent.MemoryOptions()
options.location      # Location string
options.perm          # Permission enum
options.type          # TransportType enum
options.shm_path      # SHM path (str)
options.shm_offset    # SHM offset (int)
options.internal      # Internal flag (int)
```

## RAII Helper Classes

### MemoryGuard

Context manager for automatic memory cleanup.

```python
with engine.allocate_memory_guard(size) as addr:
    # Use addr
    pass
# Automatically freed
```

### BatchGuard

Context manager for automatic batch cleanup.

```python
with engine.allocate_batch_guard(100) as batch_id:
    # Use batch_id
    pass
# Automatically freed
```

## Complete Example

```python
import tent
import numpy as np

def main():
    # Create engine
    engine = tent.TransferEngine("./transfer-engine.json")
    
    if not engine.available():
        print("Engine not available")
        return
    
    # Get local info
    print(f"Segment: {engine.get_segment_name()}")
    print(f"RPC: {engine.get_rpc_server_address()}:{engine.get_rpc_server_port()}")
    
    # Allocate memory with RAII
    with engine.allocate_memory_guard(1024*1024, "cpu:0") as buffer:
        # Initialize buffer
        data = bytearray(b'\xAB' * (1024*1024))
        
        # Open remote segment
        try:
            remote_id = engine.open_segment("peer-host:12345")
        except tent.MetadataError as e:
            print(f"Failed to open segment: {e}")
            return
        
        # Allocate batch with RAII
        with engine.allocate_batch_guard(1) as batch_id:
            # Create request
            req = tent.Request(
                tent.OpCode.WRITE,
                buffer,           # Source
                remote_id,        # Target segment
                0,                # Target offset
                1024*1024,        # Length
                tent.PRIO_MEDIUM   # Priority
            )
            
            # Submit
            try:
                engine.submit_transfer(batch_id, [req])
            except tent.RdmaError as e:
                print(f"RDMA error: {e}")
                return
            
            # Wait for completion
            while True:
                status = engine.get_transfer_status(batch_id, 0)
                if status.state == tent.TransferStatusEnum.COMPLETED:
                    print(f"Transferred {status.bytes} bytes")
                    break
                elif status.state == tent.TransferStatusEnum.FAILED:
                    print("Transfer failed")
                    break
        
        # Close segment
        engine.close_segment(remote_id)

if __name__ == "__main__":
    main()
```

## Batch Transfer Example

```python
import tent

engine = tent.TransferEngine()

# Allocate multiple buffers
buffers = []
with engine.allocate_batch_guard(100) as batch_id:
    for i in range(10):
        with engine.allocate_memory_guard(1024*1024) as buf:
            buffers.append(buf)
    
    # Register all at once
    engine.register_local_memory_batch(
        buffers,
        [1024*1024] * 10,
        tent.Permission.GlobalReadWrite
    )
    
    # Create requests
    requests = []
    for i, buf in enumerate(buffers):
        req = tent.Request(
            tent.OpCode.WRITE,
            buf,
            remote_id,
            i * 1024*1024,
            1024*1024,
            tent.PRIO_LOW
        )
        requests.append(req)
    
    # Submit all
    engine.submit_transfer(batch_id, requests)
    
    # Get all statuses
    statuses = engine.get_transfer_status_list(batch_id)
    for i, status in enumerate(statuses):
        print(f"Task {i}: {status.state}, {status.bytes} bytes")
```

## Notification Example

```python
import tent

engine = tent.TransferEngine()

# Send notification
notif = tent.Notification("checkpoint_done", "model_v1")
engine.send_notifi(remote_id, notif)

# Receive notifications
notifications = engine.recv_notifi()
for n in notifications:
    print(f"From {n.name}: {n.msg}")
```

## Memory Options Example

```python
import tent

engine = tent.TransferEngine()

# Advanced GPU memory allocation
options = tent.MemoryOptions()
options.location = "cuda:0"
options.perm = tent.Permission.GlobalReadWrite
options.type = tent.TransportType.RDMA

with engine.allocate_memory_guard_ex(1024*1024, options) as gpu_buffer:
    # Use GPU memory
    pass
```

## Difference from Classic TE Python API

| Feature | TENT Python API | Classic TE Python API |
|---------|-----------------|----------------------|
| **Module name** | `tent` | `mooncake.engine` |
| **Error handling** | Exception-based | Integer return codes |
| **Memory management** | RAII context managers | Manual cleanup |
| **API style** | Pythonic | C-style |
| **GIL handling** | Automatic release | Manual release needed |

## See Also

- [Quick Start](../getting-started/quick-start.md) - Getting started with TENT
- [C++ API Reference](cpp-api.md) - C++ API documentation
- [Configuration Reference](../configuration/configuration.md) - Configuration options
