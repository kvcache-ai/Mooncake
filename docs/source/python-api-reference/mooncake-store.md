# Mooncake Store Python API

## Installation

### PyPI Package
Install the Mooncake Transfer Engine package from PyPI, which includes both Mooncake Transfer Engine and Mooncake Store Python bindings:

```bash
pip install mooncake-transfer-engine
```

📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine/](https://pypi.org/project/mooncake-transfer-engine/)

### Required Service
Only one service is required now:

- `mooncake_master` — Master service which now embeds the HTTP metadata server

## Quick Start

### Start Master (with HTTP enabled)

Enable the built-in HTTP metadata server when starting the master:

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```
This exposes the metadata endpoint at `http://<host>:<port>/metadata`.

### Hello World Example

```python
from mooncake.store import MooncakeDistributedStore

# 1. Create store instance
store = MooncakeDistributedStore()

# 2. Setup with all required parameters
store.setup(
    "localhost",           # Your node's address
    "http://localhost:8080/metadata",    # HTTP metadata server
    512*1024*1024,          # 512MB segment size
    128*1024*1024,          # 128MB local buffer
    "tcp",                             # Use TCP (RDMA for high performance)
    "",                            # Leave empty; Mooncake auto-picks RDMA devices when needed
    "localhost:50051"        # Master service
)

# 3. Store data
store.put("hello_key", b"Hello, Mooncake Store!")

# 4. Retrieve data
data = store.get("hello_key")
print(data.decode())  # Output: Hello, Mooncake Store!

# 5. Clean up
store.close()
```

**RDMA device selection**: Leave `rdma_devices` as `""` to auto-select RDMA NICs. Provide a comma-separated list (e.g. `"mlx5_0,mlx5_1"`) to pin to specific hardware.

Mooncake selects available ports internally at `setup() `, so you do not need to fix specific port numbers in these examples. Internally, ports are chosen from a dynamic range (currently 12300–14300).

#### P2P Hello World (preview)

The following setup uses the new P2P handshake and does not require an HTTP metadata server. This feature is not released yet; use only if you’re testing the latest code.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    "localhost",           # Your node's ip address
    "P2PHANDSHAKE",              # P2P handshake (no HTTP metadata)
    512*1024*1024,                # 512MB segment size
    128*1024*1024,                # 128MB local buffer
    "tcp",                       # Use TCP (RDMA for high performance)
    "",                          # Leave empty; Mooncake auto-picks RDMA devices when needed
    "localhost:50051"           # Master service
)

store.put("hello_key", b"Hello, Mooncake Store!")
print(store.get("hello_key").decode())
store.close()
```

## Basic API Usage

### Simple Get/Put Operations

<details>
<summary>Click to expand: Complete Get/Put example with NumPy arrays</summary>

```python
import numpy as np
import json
from mooncake.store import MooncakeDistributedStore

# 1. Initialize
store = MooncakeDistributedStore()
store.setup("localhost",
            "http://localhost:8080/metadata",
            512*1024*1024,
            128*1024*1024,
            "tcp",
            "",
            "localhost:50051")
print("Store ready.")

# 2. Store data
store.put("config", b'{"model": "llama-7b", "temperature": 0.7}')
model_weights = np.random.randn(1000, 1000).astype(np.float32)
store.put("weights", model_weights.tobytes())
store.put("cache", b"some serialized cache data")

# 3. Retrieve and verify data
config = json.loads(store.get("config").decode())
weights = np.frombuffer(store.get("weights"), dtype=np.float32).reshape(1000, 1000)

print("Config OK:", config["model"])
print("Weights OK, mean =", round(float(weights.mean()), 4))
print("Cache exists?", bool(store.is_exist("cache")))

# 4. Close
store.close()
```

</details>

## Zero-Copy API (Advanced Performance)

For maximum performance, especially with RDMA networks, use the zero-copy API. This allows direct memory access without intermediate copies.

### Memory Registration

⚠️ **Important**: `register_buffer` is required for zero-copy RDMA operations. Without proper buffer registration, undefined behavior and memory corruption may occur.

Zero-copy operations require registering memory buffers with the store:

#### register_buffer()
Register a memory buffer for direct RDMA access.

#### unregister_buffer()
Unregister a previously registered buffer.

<details>
<summary>Click to expand: Buffer registration example</summary>

```python
import numpy as np
from mooncake.store import MooncakeDistributedStore

# Initialize store
store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Create a large buffer
buffer = np.zeros(100 * 1024 * 1024, dtype=np.uint8)  # 100MB buffer

# Register the buffer for zero-copy operations
buffer_ptr = buffer.ctypes.data
result = store.register_buffer(buffer_ptr, buffer.nbytes)
if result != 0:
    print(f"Failed to register buffer: {result}")
    raise RuntimeError(f"Failed to register buffer: {result}")
print("Buffer registered successfully.")
store.unregister_buffer(buffer_ptr)
```

</details>

---

### Zero-Copy Operations

#### Complete Zero-Copy Workflow

⚠️ **Critical**: Always register buffers before zero-copy operations. Failure to register buffers will cause undefined behavior and potential memory corruption.

Here's a complete example showing the full zero-copy workflow with proper buffer management:

<details>
<summary>Click to expand: Complete zero-copy workflow example</summary>

```python
import numpy as np
from mooncake.store import MooncakeDistributedStore

# Initialize store with RDMA protocol for maximum performance
store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 16*1024*1024, "tcp", "", "localhost:50051")

# Create data to store
original_data = np.random.randn(1000, 1000).astype(np.float32)
buffer_ptr = original_data.ctypes.data
size = original_data.nbytes

# Step 1: Register the buffer
result = store.register_buffer(buffer_ptr, size)
if result != 0:
    raise RuntimeError(f"Failed to register buffer: {result}")

# Step 2: Zero-copy store
result = store.put_from("large_tensor", buffer_ptr, size)
if result == 0:
    print(f"Successfully stored {size} bytes with zero-copy")
else:
    raise RuntimeError(f"Store failed with code: {result}")

# Step 3: Pre-allocate buffer for retrieval
retrieved_data = np.empty((1000, 1000), dtype=np.float32)
recv_buffer_ptr = retrieved_data.ctypes.data
recv_size = retrieved_data.nbytes

# Step 4: Register receive buffer
result = store.register_buffer(recv_buffer_ptr, recv_size)
if result != 0:
    raise RuntimeError(f"Failed to register receive buffer: {result}")

# Step 5: Zero-copy retrieval
bytes_read = store.get_into("large_tensor", recv_buffer_ptr, recv_size)
if bytes_read > 0:
    print(f"Successfully retrieved {bytes_read} bytes with zero-copy")
    # Verify the data
    print(f"Data matches: {np.array_equal(original_data, retrieved_data)}")
else:
    raise RuntimeError(f"Retrieval failed with code: {bytes_read}")

# Step 6: Clean up - unregister both buffers
store.unregister_buffer(buffer_ptr)
store.unregister_buffer(recv_buffer_ptr)
store.close()
```

</details>

#### put_from()
Store data directly from a registered buffer (zero-copy).

```python
def put_from(self, key: str, buffer_ptr: int, size: int, config=None) -> int
```

**Parameters:**
- `key`: Object identifier
- `buffer_ptr`: Memory address (from ctypes.data or similar)
- `size`: Number of bytes to store
- `config`: Optional replication configuration

#### get_into()
Retrieve data directly into a registered buffer (zero-copy).

```python
def get_into(self, key: str, buffer_ptr: int, size: int) -> int
```

**Parameters:**
- `key`: Object identifier to retrieve
- `buffer_ptr`: Memory address of pre-allocated buffer
- `size`: Size of the buffer (must be >= object size)

**Returns:** Number of bytes read, or negative on error

---

## ReplicateConfig Configuration

The `ReplicateConfig` class allows you to control data replication behavior when storing objects in Mooncake Store. This configuration is essential for ensuring data reliability, performance optimization, and storage placement control.

### Class Definition

```python
from mooncake.store import ReplicateConfig

# Create a configuration instance
config = ReplicateConfig()
```

### Properties

#### replica_num
**Type:** `int`
**Default:** `1`
**Description:** Specifies the total number of replicas to create for the stored object.

```python
config = ReplicateConfig()
config.replica_num = 3  # Store 3 copies of the data
```

#### with_soft_pin
**Type:** `bool`
**Default:** `False`
**Description:** Enables soft pinning for the stored object. Soft pinned objects are prioritized to remain in memory during eviction - they are only evicted when memory is insufficient and no other objects are eligible for eviction. This is useful for frequently accessed or important objects like system prompts.

```python
config = ReplicateConfig()
config.with_soft_pin = True  # Keep this object in memory longer
```

#### preferred_segment
**Type:** `str`
**Default:** `""` (empty string)
**Description:** Specifies a preferred segment (node) for data allocation. This is typically the hostname:port of a target server.

```python
config = ReplicateConfig()

# Preferred replica location ("host:port")
config.preferred_segment = "localhost:12345"     # pin to a specific machine

# Alternatively, pin to the local host
config.preferred_segment = self.get_hostname()

# Optional: speed up local transfers
#   export MC_STORE_MEMCPY=1
```
---

## Non-Zero-Copy API (Simple Usage)

For simpler use cases, use the standard API without memory registration:

### Basic Operations

<details>
<summary>Click to expand: Non-zero-copy API examples</summary>

```python
from mooncake.store import MooncakeDistributedStore

# Initialize (same as zero-copy)
store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Simple put/get (automatic memory management)
data = b"Hello, World!" * 1000  # ~13KB
store.put("message", data)

retrieved = store.get("message")
print(retrieved == data)  # True

# Batch operations
keys = ["key1", "key2", "key3"]
values = [b"value1", b"value2", b"value3"]

store.put_batch(keys, values)
retrieved = store.get_batch(keys)
print("Retrieved all keys successfully:", retrieved == values)
```

</details>

### Performance Notes

**Choose the appropriate API based on your use case:**

**Zero-copy API is beneficial when:**
- Working with large data transfers
- RDMA network infrastructure is available and configured
- Direct memory access patterns fit your application design

**Non-zero-copy API is suitable for:**
- Development and prototyping phases
- Applications without specific performance requirements

**Batch operations can improve throughput for:**
- Multiple related operations performed together
- Scenarios where network round-trip reduction is beneficial

---

## Topology & Devices

- Auto-discovery: Disabled by default. For `protocol="rdma"`, you must specify RDMA devices.
- Enable auto-discovery (optional):
  - `MC_MS_AUTO_DISC=1` enables auto-discovery; then `rdma_devices` is not required.
  - Optionally restrict candidates with `MC_MS_FILTERS`, a comma-separated whitelist of NIC names, e.g. `MC_MS_FILTERS=mlx5_0,mlx5_2`.
  - If `MC_MS_AUTO_DISC` is not set or set to `0`, auto-discovery remains disabled and `rdma_devices` is required for RDMA.

Examples:

```bash
# Auto-select with default settings
python - <<'PY'
from mooncake.store import MooncakeDistributedStore as S
s = S()
s.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "rdma", "", "localhost:50051")
PY

# Manual device list 
unset MC_MS_AUTO_DISC
python - <<'PY'
from mooncake.store import MooncakeDistributedStore as S
s = S()
s.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "rdma", "mlx5_0,mlx5_1", "localhost:50051")
PY

# Auto-select with filters
export MC_MS_AUTO_DISC=1
export MC_MS_FILTERS=mlx5_0,mlx5_2
python - <<'PY'
from mooncake.store import MooncakeDistributedStore as S
s = S()
s.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "rdma", "", "localhost:50051")
PY
```

## get_buffer Buffer Protocol

The `get_buffer` method returns a `BufferHandle` object that implements the Python buffer protocol:

<details>
<summary>Click to expand: Buffer protocol usage example</summary>

```python
# Get buffer with buffer protocol support
buffer = store.get_buffer("large_object")
if buffer:
    # Access as numpy array without copy
    import numpy as np
    arr = np.array(buffer, copy=False)

    # Direct memory access
    ptr = buffer.ptr()  # Memory address
    size = buffer.size()  # Buffer size in bytes

    # Use with other libraries that accept buffer protocol
    print(f"Buffer size: {len(buffer)} bytes")

    # The buffer is automatically freed
```

</details>
---

## Full API Reference

### Class: MooncakeDistributedStore

The main class for interacting with Mooncake Store.

#### Constructor
```python
store = MooncakeDistributedStore()
```
Creates a new store instance. No parameters required.

---


#### setup()
Initialize distributed resources and establish network connections.

```python
def setup(
    self,
    local_hostname: str,
    metadata_server: str,
    global_segment_size: int = 16777216,
    local_buffer_size: int = 1073741824,
    protocol: str = "tcp",
    rdma_devices: str = "",
    master_server_addr: str,
) -> int
```

**Parameters:**
- `local_hostname` (str): **Required**. Local hostname and port (e.g., "localhost" or "localhost:12345")
- `metadata_server` (str): **Required**. Metadata server address (e.g., "http://localhost:8080/metadata")
- `global_segment_size` (int): Memory segment size in bytes for mounting (default: 16MB = 16777216)
- `local_buffer_size` (int): Local buffer size in bytes (default: 1GB = 1073741824)
- `protocol` (str): Network protocol - "tcp" or "rdma" (default: "tcp")
- `rdma_devices` (str): RDMA device name(s), e.g. `"mlx5_0"` or `"mlx5_0,mlx5_1"`. Leave empty to auto-select NICs. Provide device names to pin the NICs. Always empty for TCP.
- `master_server_addr` (str): **Required**. Master server address (e.g., "localhost:50051")

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**

<details>
<summary>Click to expand: Setup examples for TCP and RDMA</summary>

```python
# TCP initialization
store.setup("localhost", "http://localhost:8080/metadata", 1024*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# RDMA auto-detect 
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "rdma", "", "localhost:50051")

# RDMA with explicit device list
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "rdma", "mlx5_0,mlx5_1", "localhost:50051")
```

</details>

---

#### put()
Store binary data in the distributed storage.

```python
def put(self, key: str, value: bytes, config: ReplicateConfig = None) -> int
```

**Parameters:**
- `key` (str): Unique object identifier
- `value` (bytes): Binary data to store
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**

<details>
<summary>Click to expand: Put operation examples</summary>

```python
# Simple put
store.put("my_key", b"Hello, World!")

# Put with replication config
config = ReplicateConfig()
config.replica_num = 2
store.put("important_data", b"Critical information", config)
```

</details>

---

#### get()
Retrieve binary data from distributed storage.

```python
def get(self, key: str) -> bytes
```

**Parameters:**
- `key` (str): Object identifier to retrieve

**Returns:**
- `bytes`: Retrieved binary data

**Raises:**
- Returns empty bytes if key doesn't exist

**Example:**

<details>
<summary>Click to expand: Get operation example</summary>

```python
data = store.get("my_key")
if data:
    print(f"Retrieved: {data.decode()}")
else:
    print("Key not found")
```

</details>

---

#### put_batch()
Store multiple objects in a single batch operation.

```python
def put_batch(self, keys: List[str], values: List[bytes], config: ReplicateConfig = None) -> int
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `values` (List[bytes]): List of binary data to store
- `config` (ReplicateConfig, optional): Replication configuration for all objects

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**

<details>
<summary>Click to expand: Batch put example</summary>

```python
keys = ["key1", "key2", "key3"]
values = [b"value1", b"value2", b"value3"]
result = store.put_batch(keys, values)
```

</details>

---

#### get_batch()
Retrieve multiple objects in a single batch operation.

```python
def get_batch(self, keys: List[str]) -> List[bytes]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers to retrieve

**Returns:**
- `List[bytes]`: List of retrieved binary data

**Example:**

<details>
<summary>Click to expand: Batch get example</summary>

```python
keys = ["key1", "key2", "key3"]
values = store.get_batch(keys)
for key, value in zip(keys, values):
    print(f"{key}: {len(value)} bytes")
```

</details>

---

#### remove()
Delete an object from the storage system.

```python
def remove(self, key: str) -> int
```

**Parameters:**
- `key` (str): Object identifier to remove

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**
```python
result = store.remove("my_key")
if result == 0:
    print("Successfully removed")
```

---

#### remove_by_regex()
Remove objects from the storage system whose keys match a regular expression.

```python
def remove_by_regex(self, regex: str) -> int
```

**Parameters:**
- `regex` (str): The regular expression to match against object keys.

**Returns:**
- `int`: The number of objects removed, or a negative value on error.

**Example:**
```python
# Remove all keys starting with "user_session_"
count = store.remove_by_regex("^user_session_.*")
if count >= 0:
    print(f"Removed {count} objects")
```

---

#### remove_all()
Remove all objects from the storage system.

```python
def remove_all(self) -> int
```

**Returns:**
- `int`: Number of objects removed, or -1 on error

**Example:**
```python
count = store.remove_all()
print(f"Removed {count} objects")
```

---

#### is_exist()
Check if an object exists in the storage system.

```python
def is_exist(self, key: str) -> int
```

**Parameters:**
- `key` (str): Object identifier to check

**Returns:**
- `int`:
  - `1`: Object exists
  - `0`: Object doesn't exist
  - `-1`: Error occurred

**Example:**
```python
exists = store.is_exist("my_key")
if exists == 1:
    print("Object exists")
elif exists == 0:
    print("Object not found")
else:
    print("Error checking existence")
```

---

#### batch_is_exist()
Check existence of multiple objects in a single batch operation.

```python
def batch_is_exist(self, keys: List[str]) -> List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers to check

**Returns:**
- `List[int]`: List of existence results (1=exists, 0=not exists, -1=error)

**Example:**
```python
keys = ["key1", "key2", "key3"]
results = store.batch_is_exist(keys)
for key, exists in zip(keys, results):
    status = "exists" if exists == 1 else "not found" if exists == 0 else "error"
    print(f"{key}: {status}")
```

---

#### get_size()
Get the size of a stored object in bytes.

```python
def get_size(self, key: str) -> int
```

**Parameters:**
- `key` (str): Object identifier

**Returns:**
- `int`: Size in bytes, or negative value on error

**Example:**
```python
size = store.get_size("my_key")
if size >= 0:
    print(f"Object size: {size} bytes")
else:
    print("Error getting size or object not found")
```

---

#### get_buffer()
Get object data as a buffer that implements Python's buffer protocol.

```python
def get_buffer(self, key: str) -> BufferHandle
```

**Parameters:**
- `key` (str): Object identifier

**Returns:**
- `BufferHandle`: Buffer object or None if not found

**Example:**
```python
buffer = store.get_buffer("large_object")
if buffer:
    print(f"Buffer size: {buffer.size()} bytes")
    # Use with numpy without copying
    import numpy as np
    arr = np.array(buffer, copy=False)
```

---

#### put_parts()
Store data from multiple buffer parts as a single object.

```python
def put_parts(self, key: str, *parts, config: ReplicateConfig = None) -> int
```

**Parameters:**
- `key` (str): Object identifier
- `*parts`: Variable number of bytes-like objects to concatenate
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**
```python
part1 = b"Hello, "
part2 = b"World!"
part3 = b" From Mooncake"
result = store.put_parts("greeting", part1, part2, part3)
```

---

#### get_hostname()
Get the hostname of the current store instance.

```python
def get_hostname(self) -> str
```

**Returns:**
- `str`: Hostname and port of this store instance

**Example:**
```python
hostname = store.get_hostname()
print(f"Store running on: {hostname}")
```

---

#### close()
Clean up all resources and terminate connections.

```python
def close(self) -> int
```

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**
```python
store.close()
```

---

### Batch Zero-Copy Operations

#### batch_put_from()
Store multiple objects from pre-registered buffers (zero-copy).

```python
def batch_put_from(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int], config: ReplicateConfig = None) -> List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `buffer_ptrs` (List[int]): List of memory addresses
- `sizes` (List[int]): List of buffer sizes
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `List[int]`: List of status codes for each operation (0 = success, negative = error)

---

#### batch_get_into()
Retrieve multiple objects into pre-registered buffers (zero-copy).

```python
def batch_get_into(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int]) -> List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `buffer_ptrs` (List[int]): List of memory addresses
- `sizes` (List[int]): List of buffer sizes

**Returns:**
- `List[int]`: List of bytes read for each operation (positive = success, negative = error)

⚠️ **Buffer Registration Required**: All buffers must be registered before batch zero-copy operations.

**Example:**

<details>
<summary>Click to expand: Batch zero-copy retrieval example</summary>

```python
# Prepare buffers
keys = ["tensor1", "tensor2", "tensor3"]
buffer_size = 1024 * 1024  # 1MB each
buffers = []
buffer_ptrs = []

for i in range(len(keys)):
    buffer = np.empty(buffer_size, dtype=np.uint8)
    buffers.append(buffer)
    buffer_ptrs.append(buffer.ctypes.data)
    store.register_buffer(buffer.ctypes.data, buffer_size)

# Batch retrieve
sizes = [buffer_size] * len(keys)
results = store.batch_get_into(keys, buffer_ptrs, sizes)

# Check results
for key, result in zip(keys, results):
    if result > 0:
        print(f"Retrieved {key}: {result} bytes")
    else:
        print(f"Failed to retrieve {key}: error {result}")

# Cleanup
for ptr in buffer_ptrs:
    store.unregister_buffer(ptr)
```

</details>

---

## Error Handling

Most methods return integer status codes:
- `0`: Success
- Negative values: Error codes (for methods that can return data size)

For methods that return data (`get`, `get_batch`, `get_buffer`, `get_tensor`):
- Return the requested data on success
- Return empty/None on failure or key not found

📋 **Complete Error Codes Reference**: See [Error Code Explanation](../troubleshooting/error-code.md) for detailed descriptions of all error codes and their meanings.

---

## Performance Tips

1. **Use batch operations** when working with multiple objects to reduce network overhead
2. **Use zero-copy APIs** (`put_from`, `get_into`) for large data transfers
3. **Register buffers** once and reuse them for multiple operations
4. **Configure replication** appropriately - more replicas provide better availability but use more storage
5. **Use soft pinning** for frequently accessed objects to keep them in memory
6. **Choose RDMA protocol** when available for maximum performance

---
