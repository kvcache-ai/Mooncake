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

#### prefer_alloc_in_same_node
**Type:** `str`
**Default:** `""` (empty string)
**Description:** Enables the preference for allocating data on the same node. Currently, this only supports `batch_put_from_multi_buffers`. Additionally, it does not support disk segments, and the `replica_num` can only be set to 1.

```python
config = ReplicateConfig()
config.prefer_alloc_in_same_node = "True
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
#### setup_dummy()
Initialize the store with a dummy client for testing purposes.

```python
def setup_dummy(self, mem_pool_size: int, local_buffer_size: int, server_address: str) -> int
```

**Parameters:**
- `mem_pool_size` (int): Memory pool size in bytes
- `local_buffer_size` (int): Local buffer size in bytes
- `server_address` (str): Server address in format "hostname:port"

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**
```python
# Initialize with dummy client
store.setup_dummy(1024*1024*256, 1024*1024*64, "localhost:8080")
```

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
#### batch_get_buffer()
Get multiple objects as buffers that implement Python's buffer protocol.

```python
def batch_get_buffer(self, keys: List[str]) -> List[BufferHandle]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers to retrieve

**Returns:**
- `List[BufferHandle]`: List of buffer objects, with None for keys not found

**Note:** This function is not supported for dummy client.

**Example:**
```python
buffers = store.batch_get_buffer(["key1", "key2", "key3"])
for i, buffer in enumerate(buffers):
    if buffer:
        print(f"Buffer {i} size: {buffer.size()} bytes")
```

---
#### alloc_from_mem_pool()
Allocate memory from the memory pool.

```python
def alloc_from_mem_pool(self, size: int) -> int
```

**Parameters:**
- `size` (int): Size of memory to allocate in bytes

**Returns:**
- `int`: Memory address as integer, or 0 on failure

---
#### init_all()
Initialize all resources with specified protocol and device.

```python
def init_all(self, protocol: str, device_name: str, mount_segment_size: int = 16777216) -> int
```

**Parameters:**
- `protocol` (str): Network protocol - "tcp" or "rdma"
- `device_name` (str): Device name for the protocol
- `mount_segment_size` (int): Memory segment size in bytes for mounting (default: 16MB = 16777216)

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

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

#### get_replica_desc()
Get descriptors of replicas for a key.

```python
def get_replica_desc(self, key: str) -> List[Replica::Descriptor]
```

**Parameters:**
- `key` (str): mooncake store key

**Returns:**
- `List[Replica::Descriptor]`: List of replica descriptors

**Example:**
```python
descriptors = store.get_replica_desc("mooncake_key")
for desc in descriptors:
    print("Status:", desc.status)
    if desc.is_memory_replica():
        mem_desc = desc.get_memory_descriptor()
        print("Memory buffer desc:", mem_desc.buffer_descriptor)
    elif desc.is_disk_replica():
        disk_desc = desc.get_disk_descriptor()
        print("Disk path:", disk_desc.file_path, "Size:", disk_desc.object_size)
```

---

#### batch_get_replica_desc()
Get descriptors of replicas for a tuple of keys.

```python
def batch_get_replica_desc(self, keys: List[str]) -> Dict[str, List[Replica::Descriptor]]
```

**Parameters:**
- `keys` (List[str]): List of mooncake store keys

**Returns:**
- `Dict[str, List[Replica::Descriptor]]`: Dictionary mapping keys to their list of replica descriptors

**Example:**
```python
descriptors_map = store.batch_get_replica_desc(["key1", "key2"])
for key, desc_list in descriptors_map.items():
    print(f"Replicas for key: {key}")
    for desc in desc_list:
        if desc.is_memory_replica():
            mem_desc = desc.get_memory_descriptor()
            print("Memory buffer desc:", mem_desc.buffer_descriptor)
        elif desc.is_disk_replica():
            disk_desc = desc.get_disk_descriptor()
            print("Disk path:", disk_desc.file_path, "Size:", disk_desc.object_size)
```

---

#### create_copy_task()

Creates an asynchronous copy task to replicate an object to target segments.

```python
def create_copy_task(self, key: str, targets: List[str]) -> Tuple[UUID, int]
```

**Parameters:**
- `key` (str): Object key to copy
- `targets` (List[str]): List of target segment names where replicas should be created

**Returns:**
- `Tuple[UUID, int]`: (task UUID, error code)
  - If successful: (task UUID, 0)
  - If failed: (UUID{0, 0}, error code)

**Example:**
```python
# Create an asynchronous copy task
task_id, error_code = store.create_copy_task("my_key", ["segment1", "segment2"])
if error_code == 0:
    print(f"Copy task created with ID: {task_id}")
    # Query task status later
    response, status = store.query_task(task_id)
    if status == 0:
        print(f"Task status: {response.status}")
else:
    print(f"Failed to create copy task: {error_code}")
```

---

#### create_move_task()

Creates an asynchronous move task to move an object from source segment to target segment.

```python
def create_move_task(self, key: str, source: str, target: str) -> Tuple[UUID, int]
```

**Parameters:**
- `key` (str): Object key to move
- `source` (str): Source segment name where the replica currently exists
- `target` (str): Target segment name where the replica should be moved to

**Returns:**
- `Tuple[UUID, int]`: (task UUID, error code)
  - If successful: (task UUID, 0)
  - If failed: (UUID{0, 0}, error code)

**Example:**
```python
# Create an asynchronous move task
task_id, error_code = store.create_move_task("my_key", "old_segment", "new_segment")
if error_code == 0:
    print(f"Move task created with ID: {task_id}")
    # Query task status later
    response, status = store.query_task(task_id)
    if status == 0:
        print(f"Task status: {response.status}")
else:
    print(f"Failed to create move task: {error_code}")
```

---

#### query_task()

Queries the status of an asynchronous task (copy or move).

```python
def query_task(self, task_id: UUID) -> Tuple[QueryTaskResponse | None, int]
```

**Parameters:**
- `task_id` (UUID): UUID of the task to query

**Returns:**
- `Tuple[QueryTaskResponse | None, int]`: (QueryTaskResponse if success, error code)
  - If successful: (QueryTaskResponse, 0)
  - If failed: (None, error code)

**Example:**
```python
from mooncake.store import MooncakeDistributedStore, TaskStatus
import time

# Initialize store
store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata",
            512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Submit multiple copy tasks
tasks = []
for key in ["key1", "key2", "key3"]:
    task_id, error = store.create_copy_task(key, ["segment1", "segment2"])
    if error == 0:
        tasks.append(task_id)
        print(f"Created copy task {task_id} for {key}")

# Monitor task progress
while tasks:
    completed = []
    for task_id in tasks:
        response, status = store.query_task(task_id)
        if status == 0 and response:
            if response.status == TaskStatus.COMPLETED:
                print(f"Task {task_id} completed")
                completed.append(task_id)
            elif response.status == TaskStatus.FAILED:
                print(f"Task {task_id} failed: {response.message}")
                completed.append(task_id)

    # Remove completed tasks
    tasks = [t for t in tasks if t not in completed]

    if tasks:
        time.sleep(1)  # Wait before next check

print("All tasks completed")
store.close()
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
#### put_from_with_metadata()
Store data directly from a registered buffer with metadata (zero-copy).

```python
def put_from_with_metadata(self, key: str, buffer_ptr: int, metadata_buffer_ptr: int, size: int, metadata_size: int, config: ReplicateConfig = None) -> int
```

**Parameters:**
- `key` (str): Object identifier
- `buffer_ptr` (int): Memory address of the main data buffer (from ctypes.data or similar)
- `metadata_buffer_ptr` (int): Memory address of the metadata buffer
- `size` (int): Number of bytes for the main data
- `metadata_size` (int): Number of bytes for the metadata
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Note:** This function is not supported for dummy client.

**Example:**
```python
import numpy as np

# Create data and metadata
data = np.random.randn(1000).astype(np.float32)
metadata = np.array([42, 100], dtype=np.int32)  # example metadata

# Register buffers
data_ptr = data.ctypes.data
metadata_ptr = metadata.ctypes.data
store.register_buffer(data_ptr, data.nbytes)
store.register_buffer(metadata_ptr, metadata.nbytes)

# Store with metadata
result = store.put_from_with_metadata("data_with_metadata", data_ptr, metadata_ptr,
                                     data.nbytes, metadata.nbytes)
if result == 0:
    print("Data with metadata stored successfully")

# Cleanup
store.unregister_buffer(data_ptr)
store.unregister_buffer(metadata_ptr)
```

---
#### pub_tensor()
Publish a PyTorch tensor with configurable replication settings.

```python
def pub_tensor(self, key: str, tensor: torch.Tensor, config: ReplicateConfig = None) -> int
```

**Parameters:**
- `key` (str): Unique object identifier
- `tensor` (torch.Tensor): PyTorch tensor to store
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Note:** This function requires `torch` to be installed and available in the environment.

**Example:**
```python
import torch
from mooncake.store import ReplicateConfig

# Create a tensor
tensor = torch.randn(100, 100)

# Create replication config
config = ReplicateConfig()
config.replica_num = 3
config.with_soft_pin = True

# Publish tensor with replication settings
result = store.pub_tensor("my_tensor", tensor, config)
if result == 0:
    print("Tensor published successfully")
```

---

### PyTorch Tensor Operations (Tensor Parallelism)

These methods provide direct support for storing and retrieving PyTorch tensors. They automatically handle serialization and metadata, and include built-in support for **Tensor Parallelism (TP)** by automatically splitting and reconstructing tensor shards.

⚠️ **Note**: These methods require `torch` to be installed and available in the environment.

#### put_tensor_with_tp()

Put a PyTorch tensor into the store, optionally splitting it into shards for tensor parallelism.
The tensor is chunked immediately and stored as separate keys (e.g., `key_tp_0`, `key_tp_1`...).

```python
def put_tensor_with_tp(self, key: str, tensor: torch.Tensor, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> int
```

**Parameters:**

  - `key` (str): Base identifier for the tensor.
  - `tensor` (torch.Tensor): The PyTorch tensor to store.
  - `tp_rank` (int): Current tensor parallel rank (default: 0). *Note: The method splits and stores all chunks for all ranks regardless of this value.*
  - `tp_size` (int): Total tensor parallel size (default: 1). If \> 1, the tensor is split into `tp_size` chunks.
  - `split_dim` (int): The dimension to split the tensor along (default: 0).

**Returns:**

  - `int`: Status code (0 = success, non-zero = error code).

#### pub_tensor_with_tp()

Publish a PyTorch tensor into the store with configurable replication settings, optionally splitting it into shards for tensor parallelism.
The tensor is chunked immediately and stored as separate keys (e.g., `key_tp_0`, `key_tp_1`...).

```python
def pub_tensor_with_tp(self, key: str, tensor: torch.Tensor, config: ReplicateConfig, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> int
```

**Parameters:**

  - `key` (str): Base identifier for the tensor.
  - `tensor` (torch.Tensor): The PyTorch tensor to store.
  - `config` (ReplicateConfig): Optional replication configuration.
  - `tp_rank` (int): Current tensor parallel rank (default: 0). *Note: The method splits and stores all chunks for all ranks regardless of this value.*
  - `tp_size` (int): Total tensor parallel size (default: 1). If \> 1, the tensor is split into `tp_size` chunks.
  - `split_dim` (int): The dimension to split the tensor along (default: 0).

**Returns:**

  - `int`: Status code (0 = success, non-zero = error code).

#### get_tensor_with_tp()

Get a PyTorch tensor from the store, specifically retrieving the shard corresponding to the given Tensor Parallel rank.

```python
def get_tensor_with_tp(self, key: str, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> torch.Tensor
```

**Parameters:**

  - `key` (str): Base identifier of the tensor.
  - `tp_rank` (int): The tensor parallel rank to retrieve (default: 0). Fetches key `key_tp_{rank}` if `tp_size > 1`.
  - `tp_size` (int): Total tensor parallel size (default: 1).
  - `split_dim` (int): The dimension used during splitting (default: 0).

**Returns:**

  - `torch.Tensor`: The retrieved tensor (or shard). Returns `None` if not found.

#### batch_put_tensor_with_tp()

Put a batch of PyTorch tensors into the store, splitting each into shards for tensor parallelism.

```python
def batch_put_tensor_with_tp(self, base_keys: List[str], tensors_list: List[torch.Tensor], tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> List[int]
```

**Parameters:**

  - `base_keys` (List[str]): List of base identifiers.
  - `tensors_list` (List[torch.Tensor]): List of tensors to store.
  - `tp_rank` (int): Current rank (default: 0).
  - `tp_size` (int): Total TP size (default: 1).
  - `split_dim` (int): Split dimension (default: 0).

**Returns:**

  - `List[int]`: List of status codes for each tensor operation.

#### batch_pub_tensor_with_tp()

Publish a batch of PyTorch tensors into the store with configurable replication settings, splitting each into shards for tensor parallelism.

```python
def batch_pub_tensor_with_tp(self, base_keys: List[str], tensors_list: List[torch.Tensor], config: ReplicateConfig, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> List[int]
```

**Parameters:**

  - `base_keys` (List[str]): List of base identifiers.
  - `tensors_list` (List[torch.Tensor]): List of tensors to store.
  - `config` (ReplicateConfig): Optional replication configuration.
  - `tp_rank` (int): Current rank (default: 0).
  - `tp_size` (int): Total tp size (default: 1).
  - `split_dim` (int): Split dimension (default: 0).

**Returns:**

  - `List[int]`: List of status codes for each tensor operation.

#### batch_get_tensor_with_tp()

Get a batch of PyTorch tensor shards from the store for a given Tensor Parallel rank.

```python
def batch_get_tensor_with_tp(self, base_keys: List[str], tp_rank: int = 0, tp_size: int = 1) -> List[torch.Tensor]
```

**Parameters:**

  - `base_keys` (List[str]): List of base identifiers.
  - `tp_rank` (int): The tensor parallel rank to retrieve (default: 0).
  - `tp_size` (int): Total tensor parallel size (default: 1).

**Returns:**

  - `List[torch.Tensor]`: List of retrieved tensors (or shards). Contains `None` for missing keys.

---
#### put_tensor()

Put a PyTorch tensor into the store.

```python
def put_tensor(self, key: str, tensor: torch.Tensor) -> int
```

**Parameters:**
- `key` (str): Object identifier
- `tensor` (torch.Tensor): The PyTorch tensor to store

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Note:** This function requires `torch` to be installed and available in the environment.

**Example:**
```python
import torch
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Store a tensor
tensor = torch.randn(100, 100)
result = store.put_tensor("my_tensor", tensor)
if result == 0:
    print("Tensor stored successfully")
```

---

#### get_tensor()

Get a PyTorch tensor from the store.

```python
def get_tensor(self, key: str) -> torch.Tensor
```

**Parameters:**
- `key` (str): Object identifier to retrieve

**Returns:**
- `torch.Tensor`: The retrieved tensor. Returns `None` if not found.

**Note:** This function requires `torch` to be installed and available in the environment.

**Example:**
```python
import torch
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Store a tensor
tensor = torch.randn(100, 100)
store.put_tensor("my_tensor", tensor)

# Retrieve the tensor
retrieved_tensor = store.get_tensor("my_tensor")
if retrieved_tensor is not None:
    print(f"Retrieved tensor with shape: {retrieved_tensor.shape}")
```

---

#### batch_get_tensor()

Get a batch of PyTorch tensors from the store.

```python
def batch_get_tensor(self, keys: List[str]) -> List[torch.Tensor]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers to retrieve

**Returns:**
- `List[torch.Tensor]`: List of retrieved tensors. Contains `None` for missing keys.

**Note:** This function requires `torch` to be installed and available in the environment.

**Example:**
```python
import torch
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Store tensors
tensor1 = torch.randn(100, 100)
tensor2 = torch.randn(50, 50)
store.put_tensor("tensor1", tensor1)
store.put_tensor("tensor2", tensor2)

# Retrieve multiple tensors
tensors = store.batch_get_tensor(["tensor1", "tensor2", "nonexistent"])
for i, tensor in enumerate(tensors):
    if tensor is not None:
        print(f"Tensor {i} shape: {tensor.shape}")
    else:
        print(f"Tensor {i} not found")
```

---

#### batch_put_tensor()

Put a batch of PyTorch tensors into the store.

```python
def batch_put_tensor(self, keys: List[str], tensors_list: List[torch.Tensor]) -> List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `tensors_list` (List[torch.Tensor]): List of tensors to store

**Returns:**
- `List[int]`: List of status codes for each tensor operation.

**Note:** This function requires `torch` to be installed and available in the environment.

**Example:**
```python
import torch
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata", 512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Create tensors
tensors = [torch.randn(100, 100), torch.randn(50, 50), torch.randn(25, 25)]
keys = ["tensor1", "tensor2", "tensor3"]

# Store multiple tensors
results = store.batch_put_tensor(keys, tensors)
for i, result in enumerate(results):
    if result == 0:
        print(f"Tensor {i} stored successfully")
    else:
        print(f"Tensor {i} failed to store with code: {result}")
```

#### batch_pub_tensor()

Pub a batch of PyTorch tensors into the store with configurable replication settings.

```python
def batch_pub_tensor(self, keys: List[str], tensors_list: List[torch.Tensor], config: ReplicateConfig) -> List[int]
```

**Parameters:**
  - `keys` (List[str]): List of object identifiers
  - `tensors_list` (List[torch.Tensor]): List of tensors to store
  - `config` (ReplicateConfig): Optional replication configuration.

**Returns:**
- `List[int]`: List of status codes for each tensor operation.

**Note:** This function requires `torch` to be installed and available in the environment.

---

### PyTorch Tensor Operations (Zero Copy)

These methods provide direct support for storing and retrieving PyTorch tensors. They automatically handle serialization and metadata, and include built-in support for **Tensor Parallelism (TP)** by automatically splitting and reconstructing tensor shards.

⚠️ **Note**: These methods require `torch` to be installed and available in the environment.

#### get_tensor_into()

Get a PyTorch tensor from the store directly into a pre-allocated buffer.

```python
def get_tensor_into(self, key: str, buffer_ptr: int, size: int) -> torch.Tensor
```

**Parameters:**

  - `key` (str): Base identifier of the tensor.
  - `buffer_ptr` (int): The buffer pointer pre-allocated for tensor, and the buffer should be registered.
  - `size` (int): The size of buffer.

**Returns:**

  - `torch.Tensor`: The retrieved tensor (or shard). Returns `None` if not found.

#### batch_get_tensor()

Get a batch of PyTorch tensor from the store directly into a pre-allocated buffer.

```python
def batch_get_tensor_into(self, base_keys: List[str], buffer_ptrs: List[int], sizes: List[int]) -> List[torch.Tensor]
```

**Parameters:**

  - `base_keys` (List[str]): List of base identifiers.
  - `buffer_ptrs` (List[int]): List of the buffers pointer pre-allocated for tensor, and the buffers should be registered.
  - `sizes` (List[int]): List of the size of buffers.

**Returns:**

  - `List[torch.Tensor]`: List of retrieved tensors (or shards). Contains `None` for missing keys.

#### get_tensor_with_tp_into()

Get a PyTorch tensor from the store, specifically retrieving the shard corresponding to the given Tensor Parallel rank, directly into the pre-allocated buffer.

```python
def get_tensor_with_tp_into(self, key: str, buffer_ptr: int, size: int, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0) -> torch.Tensor
```

**Parameters:**

  - `key` (str): Base identifier of the tensor.
  - `buffer_ptr` (int): The buffer pointer pre-allocated for tensor, and the buffer should be registered.
  - `size` (int): The size of buffer.
  - `tp_rank` (int): The tensor parallel rank to retrieve (default: 0). Fetches key `key_tp_{rank}` if `tp_size > 1`.
  - `tp_size` (int): Total tensor parallel size (default: 1).
  - `split_dim` (int): The dimension used during splitting (default: 0).

**Returns:**

  - `torch.Tensor`: The retrieved tensor (or shard). Returns `None` if not found.

#### batch_get_tensor_with_tp_into()

Get a batch of PyTorch tensor shards from the store for a given Tensor Parallel rank, directly into the pre-allocated buffer.

```python
def batch_get_tensor_with_tp_into(self, base_keys: List[str], buffer_ptrs: List[int], sizes: List[int], tp_rank: int = 0, tp_size: int = 1) -> List[torch.Tensor]
```

**Parameters:**

  - `base_keys` (List[str]): List of base identifiers.
  - `buffer_ptrs` (List[int]): List of the buffers pointer pre-allocated for tensor, and the buffers should be registered.
  - `sizes` (List[int]): List of the size of buffers.
  - `tp_rank` (int): The tensor parallel rank to retrieve (default: 0).
  - `tp_size` (int): Total tensor parallel size (default: 1).

**Returns:**

  - `List[torch.Tensor]`: List of retrieved tensors (or shards). Contains `None` for missing keys.

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

#### batch_put_from_multi_buffers()
Store multiple objects from multiple pre-registered buffers (zero-copy).

```python
def batch_put_from_multi_buffers(self, keys: List[str], all_buffer_ptrs: List[List[int]], all_sizes: List[List[int]],
                                 config: ReplicateConfig = None) -> List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `all_buffer_ptrs` (List[int]): all List of memory addresses
- `sizes` (List[int]): all List of buffer sizes
- `config` (ReplicateConfig, optional): Replication configuration

**Returns:**
- `List[int]`: List of status codes for each operation (0 = success, negative = error)

---

#### batch_get_into_multi_buffers()

Retrieve multiple objects into multiple pre-registered buffers (zero-copy).

```python
def batch_get_into_multi_buffers(self, keys: List[str], all_buffer_ptrs: List[List[int], all_sizes: List[List[int]) ->
List[int]
```

**Parameters:**
- `keys` (List[str]): List of object identifiers
- `all_buffer_ptrs` (List[int]): List of memory addresses
- `all_sizes` (List[int]): List of buffer sizes

**Returns:**
- `List[int]`: List of bytes read for each operation (positive = success, negative = error)

⚠️ **Buffer Registration Required**: All buffers must be registered before batch zero-copy operations.

**Example:**

<details>
<summary>Click to expand: Batch zero-copy put and get for multiple buffers example</summary>

```python
tensor = torch.ones(10, 61, 128*1024, dtype=torch.int8)
data_ptr = tensor.data_ptr()
store.register_buffer(data_ptr, 10*61*128*1024)

target_tensor = torch.zeros(10, 61, 128*1024, dtype=torch.int8)
target_data_ptr = target_tensor.data_ptr()
store.register_buffer(target_data_ptr, 10*61*128*1024)

all_local_addrs = []
all_remote_addrs = []
all_sizes = []
keys = []
for block_i in range(10):
  local_addrs = []
  remote_addrs = []
  sizes = []
  for _ in range(61):
    local_addrs.append(data_ptr)
    remote_addrs.append(target_data_ptr)
    sizes.append(128*1024)
    data_ptr += 128*1024
    target_data_ptr += 128*1024
  all_local_addrs.append(local_addrs)
  all_remote_addrs.append(remote_addrs)
  all_sizes.append(sizes)
  keys.append(f"kv_{rank}_{block_i}")

config = ReplicateConfig()
config.prefer_alloc_in_same_node = True
store.batch_put_from_multi_buffers(keys, all_local_addrs, all_sizes, config)
store.batch_get_into_multi_buffers(keys, all_remote_addrs, all_sizes, True)

store.unregister_buffer(tensor.data_ptr())
store.unregister_buffer(target_tensor.data_ptr())
```
</details>

## MooncakeHostMemAllocator Class

The `MooncakeHostMemAllocator` class provides host memory allocation capabilities for Mooncake Store operations.

### Class Definition

```python
from mooncake.store import MooncakeHostMemAllocator

# Create an allocator instance
allocator = MooncakeHostMemAllocator()
```

### Methods

#### alloc()
Allocate memory from the host memory pool.

```python
def alloc(self, size: int) -> int
```

**Parameters:**
- `size` (int): Size of memory to allocate in bytes

**Returns:**
- `int`: Memory address as integer, or 0 on failure

**Example:**
```python
allocator = MooncakeHostMemAllocator()
ptr = allocator.alloc(1024 * 1024)  # Allocate 1MB
if ptr != 0:
    print(f"Allocated memory at address: {ptr}")
```

#### free()
Free previously allocated memory.

```python
def free(self, ptr: int) -> int
```

**Parameters:**
- `ptr` (int): Memory address to free

**Returns:**
- `int`: Status code (0 = success, non-zero = error code)

**Example:**
```python
result = allocator.free(ptr)
if result == 0:
    print("Memory freed successfully")
```

---

## bind_to_numa_node Function

The `bind_to_numa_node` function binds the current thread and memory allocation preference to a specified NUMA node.

### Function Definition

```python
from mooncake.store import bind_to_numa_node

# Bind to NUMA node
bind_to_numa_node(node: int)
```

**Parameters:**
- `node` (int): NUMA node number to bind to

**Example:**
```python
from mooncake.store import bind_to_numa_node

# Bind current thread to NUMA node 0
bind_to_numa_node(0)
```

---

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
