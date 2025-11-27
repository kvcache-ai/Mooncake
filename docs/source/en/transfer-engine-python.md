# Transfer Engine Python API Documentation

## Overview

The Transfer Engine Python API provides a high-level interface for efficient data transfer between distributed systems using RDMA (Remote Direct Memory Access) and other transport protocols. It enables fast, low-latency data movement between nodes in a cluster.

## Installation

```python
from mooncake.engine import TransferEngine
```

## Class: TransferEngine

The main class that provides all transfer engine functionality.

### Constructor

```python
TransferEngine()
```

Creates a new TransferEngine instance with default settings.

### Initialization Methods

#### initialize()

```python
initialize(local_hostname, metadata_server, protocol, device_name)
```

Initializes the transfer engine with basic configuration.

**Parameters:**
- `local_hostname` (str): The hostname and port of the local server (e.g., "127.0.0.1:12345")
- `metadata_server` (str): The metadata server connection string (e.g., "127.0.0.1:2379" or "etcd://127.0.0.1:2379")
- `protocol` (str): The transport protocol to use ("rdma", "tcp", etc.)
- `device_name` (str): Comma-separated list of device names to filter, or empty string for all devices

**Returns:**
- `int`: 0 on success, negative value on failure

#### initialize_ext()

```python
initialize_ext(local_hostname, metadata_server, protocol, device_name, metadata_type)
```

Initializes the transfer engine with extended configuration including metadata type specification.

**Parameters:**
- `local_hostname` (str): The hostname and port of the local server
- `metadata_server` (str): The metadata server connection string
- `protocol` (str): The transport protocol to use
- `device_name` (str): Comma-separated list of device names to filter
- `metadata_type` (str): The type of metadata server ("etcd", "p2p", etc.)

**Returns:**
- `int`: 0 on success, negative value on failure

### Network Information

#### get_rpc_port()

```python
get_rpc_port()
```

Gets the RPC port that the transfer engine is listening on.

**Returns:**
- `int`: The RPC port number

### Buffer Management

#### allocate_managed_buffer()

```python
allocate_managed_buffer(length)
```

Allocates a managed buffer of the specified size using a buddy allocation system for efficient memory management.

**Parameters:**
- `length` (int): The size of the buffer to allocate in bytes

**Returns:**
- `int`: The memory address of the allocated buffer as an integer, or 0 on failure

#### free_managed_buffer()

```python
free_managed_buffer(buffer_addr, length)
```

Frees a previously allocated managed buffer.

**Parameters:**
- `buffer_addr` (int): The memory address of the buffer to free
- `length` (int): The size of the buffer in bytes

**Returns:**
- `int`: 0 on success, negative value on failure

#### get_first_buffer_address()

```python
get_first_buffer_address(segment_name)
```

Gets the address of the first buffer in a specified segment.

**Parameters:**
- `segment_name` (str): The name of the segment

**Returns:**
- `int`: The memory address of the first buffer in the segment

### Data Transfer Operations

#### transfer_sync_write()

```python
transfer_sync_write(target_hostname, buffer, peer_buffer_address, length)
```

Performs a synchronous write operation to transfer data from local buffer to remote buffer.

**Parameters:**
- `target_hostname` (str): The hostname of the target server
- `buffer` (int): The local buffer address
- `peer_buffer_address` (int): The remote buffer address
- `length` (int): The number of bytes to transfer

**Returns:**
- `int`: 0 on success, negative value on failure

#### transfer_sync_read()

```python
transfer_sync_read(target_hostname, buffer, peer_buffer_address, length)
```

Performs a synchronous read operation to transfer data from remote buffer to local buffer.

**Parameters:**
- `target_hostname` (str): The hostname of the target server
- `buffer` (int): The local buffer address
- `peer_buffer_address` (int): The remote buffer address
- `length` (int): The number of bytes to transfer

**Returns:**
- `int`: 0 on success, negative value on failure

#### transfer_sync()

```python
transfer_sync(target_hostname, buffer, peer_buffer_address, length, opcode)
```

Performs a synchronous transfer operation with specified opcode.

**Parameters:**
- `target_hostname` (str): The hostname of the target server
- `buffer` (int): The local buffer address
- `peer_buffer_address` (int): The remote buffer address
- `length` (int): The number of bytes to transfer
- `opcode` (TransferOpcode): The transfer operation type (READ or WRITE)

**Returns:**
- `int`: 0 on success, negative value on failure

#### transfer_submit_write()

```python
transfer_submit_write(target_hostname, buffer, peer_buffer_address, length)
```

Submits an asynchronous write operation and returns immediately.

**Parameters:**
- `target_hostname` (str): The hostname of the target server
- `buffer` (int): The local buffer address
- `peer_buffer_address` (int): The remote buffer address
- `length` (int): The number of bytes to transfer

**Returns:**
- `int`: Batch ID for tracking the operation, or negative value on failure

#### transfer_check_status()

```python
transfer_check_status(batch_id)
```

Checks the status of an asynchronous transfer operation.

**Parameters:**
- `batch_id` (int): The batch ID returned from transfer_submit_write()

**Returns:**
- `int`: 
  - 1: Transfer completed successfully
  - 0: Transfer still in progress
  - -1: Transfer failed
  - -2: Transfer timed out

### Buffer I/O Operations

#### write_bytes_to_buffer()

```python
write_bytes_to_buffer(dest_address, src_ptr, length)
```

Writes bytes from a Python bytes object to a buffer at the specified address.

**Parameters:**
- `dest_address` (int): The destination buffer address
- `src_ptr` (bytes): The source bytes to write
- `length` (int): The number of bytes to write

**Returns:**
- `int`: 0 on success, negative value on failure

#### read_bytes_from_buffer()

```python
read_bytes_from_buffer(source_address, length)
```

Reads bytes from a buffer at the specified address and returns them as a Python bytes object.

**Parameters:**
- `source_address` (int): The source buffer address
- `length` (int): The number of bytes to read

**Returns:**
- `bytes`: The bytes read from the buffer

### Memory Registration (Experimental)

#### register_memory()

```python
register_memory(buffer_addr, capacity)
```

Registers a memory region for RDMA access (experimental feature).

**Parameters:**
- `buffer_addr` (int): The memory address to register
- `capacity` (int): The size of the memory region in bytes

**Returns:**
- `int`: 0 on success, negative value on failure

#### unregister_memory()

```python
unregister_memory(buffer_addr)
```

Unregisters a previously registered memory region.

**Parameters:**
- `buffer_addr` (int): The memory address to unregister

**Returns:**
- `int`: 0 on success, negative value on failure

### Enums

#### TransferOpcode

```python
TransferOpcode.READ   # Read operation
TransferOpcode.WRITE  # Write operation
```

## Environment Variables

The Transfer Engine respects the following environment variables:

- `MC_TRANSFER_TIMEOUT`: Sets the transfer timeout in seconds (default: 30)
- `MC_METADATA_SERVER`: Default metadata server address
- `MC_LEGACY_RPC_PORT_BINDING`: Enables legacy RPC port binding behavior
- `MC_TCP_BIND_ADDRESS`: Specifies the TCP bind address
- `MC_CUSTOM_TOPO_JSON`: Path to custom topology JSON file
- `MC_TE_METRIC`: Enables metrics reporting (set to "1", "true", "yes", or "on")
- `MC_TE_METRIC_INTERVAL_SECONDS`: Sets metrics reporting interval in seconds

## Usage Examples

### Basic Setup and Data Transfer

```python
from mooncake.engine import TransferEngine
import os

# Create transfer engine instance
engine = TransferEngine()

# Initialize with basic configuration
ret = engine.initialize(
    local_hostname="127.0.0.1:12345",
    metadata_server="127.0.0.1:2379",
    protocol="rdma",
    device_name=""
)

if ret != 0:
    raise RuntimeError(f"Initialization failed with code {ret}")

# Allocate and initialize client buffer (1MB)
client_buffer = np.ones(1024 * 1024, dtype=np.uint8)  # Fill with ones
buffer_data = client_buffer.ctypes.data
buffer_data_len = client_buffer.nbytes

# Prepare data
data = b"Hello, Transfer Engine!"
data_len = len(data)

engine.register(buffer_data, buffer_data_len)

# Get Remote Addr from ZMQ or upper-layer inference framework
remote_addr = ??

# Transfer data to remote node
ret = engine.transfer_sync_write(
    target_hostname="127.0.0.1:12346",
    buffer=data,
    peer_buffer_address=remote_addr,
    length=data_len
)

if ret == 0:
    print("Data transfer completed successfully")
else:
    print(f"Data transfer failed with code {ret}")

engine.deregister(data)
```

### Asynchronous Transfer

```python
# Submit asynchronous write
batch_id = engine.transfer_submit_write(
    target_hostname="127.0.0.1:12346",
    buffer=local_addr,
    peer_buffer_address=remote_addr,
    length=data_len
)

if batch_id < 0:
    print(f"Failed to submit transfer with code {batch_id}")
else:
    # Poll for completion
    while True:
        status = engine.transfer_check_status(batch_id)
        if status == 1:
            print("Transfer completed successfully")
            break
        elif status == -1:
            print("Transfer failed")
            break
        elif status == -2:
            print("Transfer timed out")
            break
        # Transfer still in progress, continue polling
        import time
        time.sleep(0.001)  # Small delay to avoid busy waiting
```

### Managed Buffer Allocation

```python
# Allocate managed buffer
buffer_size = 1024 * 1024  # 1MB
buffer_addr = engine.allocate_managed_buffer(buffer_size)

if buffer_addr == 0:
    print("Failed to allocate buffer")
else:
    # Use the buffer
    test_data = b"Test data for managed buffer"
    engine.write_bytes_to_buffer(buffer_addr, test_data, len(test_data))
    
    # Read back
    read_data = engine.read_bytes_from_buffer(buffer_addr, len(test_data))
    print(f"Read data: {read_data}")
    
    # Free the buffer when done
    engine.free_managed_buffer(buffer_addr, buffer_size)
```

## Error Handling

All methods return integer status codes:
- `0`: Success
- Negative values: Error codes indicating various failure conditions

Common error scenarios:
- Network connectivity issues
- Invalid buffer addresses
- Memory allocation failures
- Transfer timeouts
- Metadata server connection problems

## Performance Considerations

1. **Buffer Reuse**: Reuse allocated buffers when possible to avoid frequent allocation/deallocation overhead
2. **Batch Operations**: Use `transfer_submit_write()` and `transfer_check_status()` for better throughput when multiple transfers are needed
3. **Memory Alignment**: Ensure buffers are properly aligned for optimal RDMA performance
4. **Timeout Configuration**: Adjust `MC_TRANSFER_TIMEOUT` based on your network characteristics and data sizes

## Thread Safety

The Transfer Engine Python API is thread-safe for most operations. However, it's recommended to:
- Use separate TransferEngine instances for different threads when possible
- Avoid concurrent modifications to the same buffer addresses
- Use proper synchronization when sharing buffer addresses between threads

## Troubleshooting

1. **Initialization Failures**: Check metadata server connectivity and network configuration
2. **Transfer Failures**: Verify target hostname is correct and network connectivity is established
3. **Memory Issues**: Ensure sufficient system memory and proper buffer alignment
4. **Performance Issues**: Check RDMA device configuration and network topology