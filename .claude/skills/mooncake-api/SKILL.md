<!-- Let's perfect this skill together. -->

# Mooncake Python API Skill

Use this skill to help users work with Mooncake Python APIs for distributed storage and high-performance data transfer.

## When to Use This Skill

Use this skill when users ask about:
- Using Mooncake Store for distributed KV cache storage
- Using Transfer Engine for RDMA/TCP data transfers
- Setting up Mooncake services (master, metadata server)
- Working with PyTorch tensors in Mooncake Store
- Zero-copy operations and buffer management
- Batch operations and replication configuration
- Mooncake EP (Expert Parallelism) and Mooncake Backend
- Troubleshooting Mooncake Python API issues

## Core Components

### 1. Mooncake Store (Distributed KV Cache)

**Import:**
```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig
```

**Basic Setup:**
```python
store = MooncakeDistributedStore()
store.setup(
    "localhost",                          # local_hostname
    "http://localhost:8080/metadata",     # metadata_server
    512*1024*1024,                        # global_segment_size (512MB)
    128*1024*1024,                        # local_buffer_size (128MB)
    "tcp",                                # protocol ("tcp" or "rdma")
    "",                                   # rdma_devices (empty for auto-select)
    "localhost:50051"                     # master_server_address
)
```

**Common Operations:**
```python
# Put/Get
store.put("key", b"value")
data = store.get("key")

# Batch operations
store.put_batch(["key1", "key2"], [b"val1", b"val2"])
values = store.get_batch(["key1", "key2"])

# Check existence
exists = store.is_exist("key")  # Returns 1 (exists), 0 (not exists), -1 (error)

# Remove
store.remove("key")
store.remove_by_regex("^prefix_.*")
store.remove_all()

# Cleanup
store.close()
```

**Zero-Copy Operations (Advanced):**
```python
import numpy as np

# Create and register buffer
buffer = np.zeros(100*1024*1024, dtype=np.uint8)
buffer_ptr = buffer.ctypes.data
store.register_buffer(buffer_ptr, buffer.nbytes)

# Zero-copy put
store.put_from("key", buffer_ptr, buffer.nbytes)

# Zero-copy get
recv_buffer = np.empty(100*1024*1024, dtype=np.uint8)
recv_ptr = recv_buffer.ctypes.data
store.register_buffer(recv_ptr, recv_buffer.nbytes)
bytes_read = store.get_into("key", recv_ptr, recv_buffer.nbytes)

# Cleanup
store.unregister_buffer(buffer_ptr)
store.unregister_buffer(recv_ptr)
```

**PyTorch Tensor Operations:**
```python
import torch

# Simple tensor operations
tensor = torch.randn(100, 100)
store.put_tensor("my_tensor", tensor)
retrieved = store.get_tensor("my_tensor")

# Batch tensor operations
tensors = [torch.randn(100, 100) for _ in range(3)]
store.batch_put_tensor(["t1", "t2", "t3"], tensors)
retrieved_tensors = store.batch_get_tensor(["t1", "t2", "t3"])

# Tensor Parallelism (TP) support
store.put_tensor_with_tp("model_weights", tensor, tp_rank=0, tp_size=4, split_dim=0)
shard = store.get_tensor_with_tp("model_weights", tp_rank=0, tp_size=4)
```

**Replication Configuration:**
```python
config = ReplicateConfig()
config.replica_num = 3              # Number of replicas
config.with_soft_pin = True         # Keep in memory longer
config.preferred_segment = "host:port"  # Preferred location

store.put("key", b"value", config)
```

### 2. Transfer Engine (High-Performance Data Transfer)

**Import:**
```python
from mooncake.engine import TransferEngine, TransferOpcode, TransferNotify
```

**Basic Setup:**
```python
engine = TransferEngine()
engine.initialize(
    "127.0.0.1:12345",      # local_hostname
    "127.0.0.1:2379",       # metadata_server (or "etcd://...")
    "tcp",                  # protocol ("tcp" or "rdma")
    ""                      # device_name (empty for all devices)
)
```

**Buffer Management:**
```python
# Allocate managed buffer
buffer_size = 1024 * 1024  # 1MB
buffer_addr = engine.allocate_managed_buffer(buffer_size)

# Write/read bytes
data = b"Hello, Transfer Engine!"
engine.write_bytes_to_buffer(buffer_addr, data, len(data))
read_data = engine.read_bytes_from_buffer(buffer_addr, len(data))

# Free buffer
engine.free_managed_buffer(buffer_addr, buffer_size)
```

**Data Transfer Operations:**
```python
# Synchronous write
result = engine.transfer_sync_write(
    "target_host:port",     # target_hostname
    local_buffer_addr,      # buffer
    remote_buffer_addr,     # peer_buffer_address
    data_length             # length
)

# Synchronous read
result = engine.transfer_sync_read(
    "target_host:port",
    local_buffer_addr,
    remote_buffer_addr,
    data_length
)

# Asynchronous write
batch_id = engine.transfer_submit_write(
    "target_host:port",
    local_buffer_addr,
    remote_buffer_addr,
    data_length
)

# Check status
status = engine.transfer_check_status(batch_id)
# Returns: 1 (completed), 0 (in progress), -1 (failed), -2 (timeout)
```

**Batch Transfer Operations:**
```python
# Batch synchronous write
local_addrs = [addr1, addr2, addr3]
remote_addrs = [remote1, remote2, remote3]
lengths = [len1, len2, len3]

result = engine.batch_transfer_sync_write(
    "target_host:port",
    local_addrs,
    remote_addrs,
    lengths
)

# Batch asynchronous operations
batch_id = engine.batch_transfer_async_write(
    "target_host:port",
    local_addrs,
    remote_addrs,
    lengths
)

# Wait for completion
result = engine.get_batch_transfer_status([batch_id])
```

**Memory Registration (for RDMA):**
```python
import numpy as np

buffer = np.ones(1024*1024, dtype=np.uint8)
buffer_ptr = buffer.ctypes.data
buffer_size = buffer.nbytes

# Register memory
engine.register_memory(buffer_ptr, buffer_size)

# Use buffer for transfers...

# Unregister when done
engine.unregister_memory(buffer_ptr)
```

### 3. Mooncake EP & Backend (Expert Parallelism)

**Mooncake Backend (Fault-Tolerant Collectives):**
```python
import torch
import torch.distributed as dist
from mooncake import pg

# Initialize with fault tolerance
active_ranks = torch.ones((world_size,), dtype=torch.int32, device="cuda")
dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=world_size,
    pg_options=pg.MooncakeBackendOptions(active_ranks),
)

# Use standard PyTorch distributed APIs
dist.all_gather(...)
dist.all_reduce(...)

# Check for failures
assert active_ranks.all()  # Verify no ranks are broken
```

**Mooncake EP (Expert Parallelism):**
```python
from mooncake.mooncake_ep_buffer import Buffer
import torch.distributed as dist

# Calculate buffer size
num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
    num_max_dispatch_tokens_per_rank=1024,
    hidden=4096,
    num_ranks=8,
    num_experts=64
)

# Create buffer (must be Mooncake Backend process group)
buffer = Buffer(group=dist.group.WORLD, num_ep_buffer_bytes=num_ep_buffer_bytes)

# Dispatch/combine operations
active_ranks = torch.ones((num_ranks,), dtype=torch.int32, device="cuda")
buffer.dispatch(..., active_ranks=active_ranks, timeout_us=1000000)
buffer.combine(..., active_ranks=active_ranks, timeout_us=1000000)
```

## Starting Services

### Start Master Service (with HTTP metadata server)
```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --default_kv_lease_ttl=5000
```

### Using External etcd (Production)
```bash
# Start etcd
etcd --listen-client-urls http://0.0.0.0:2379 \
     --advertise-client-urls http://0.0.0.0:2379

# Start master
mooncake_master --default_kv_lease_ttl=5000
```

## Environment Variables

### Transfer Engine
- `MC_METADATA_SERVER`: Metadata server URL
- `MC_FORCE_TCP`: Force TCP transport (set to "true")
- `MC_LOG_LEVEL`: Logging level (0=INFO, 1=WARNING, 2=ERROR)
- `MC_MS_AUTO_DISC`: Enable RDMA device auto-discovery (set to "1")
- `MC_MS_FILTERS`: Filter RDMA devices (e.g., "mlx5_0,mlx5_2")
- `MC_TRANSFER_TIMEOUT`: Transfer timeout in seconds (default: 30)

### Mooncake Store
- `MC_STORE_CLUSTER_ID`: Cluster identifier (default: "mooncake")
- `MC_STORE_USE_HUGEPAGE`: Enable hugepage support
- `MC_STORE_MEMCPY`: Enable local memcpy optimization (set to "1")
- `MC_STORE_CLIENT_METRIC`: Enable client metrics (enabled by default)
- `MC_YLT_LOG_LEVEL`: Log level (trace/debug/info/warn/error/critical)

## Common Patterns

### Pattern 1: Simple KV Store
```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata",
            512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# Store and retrieve
store.put("config", b'{"model": "llama-7b"}')
config = store.get("config")

store.close()
```

### Pattern 2: High-Performance Tensor Storage
```python
import torch
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata",
            512*1024*1024, 128*1024*1024, "rdma", "mlx5_0", "localhost:50051")

# Configure replication
config = ReplicateConfig()
config.replica_num = 2
config.with_soft_pin = True

# Store tensor with replication
tensor = torch.randn(1000, 1000)
store.put_tensor("weights", tensor, config)

# Retrieve
retrieved = store.get_tensor("weights")

store.close()
```

### Pattern 3: Zero-Copy Batch Operations
```python
import numpy as np
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata",
            512*1024*1024, 16*1024*1024, "rdma", "", "localhost:50051")

# Prepare buffers
num_buffers = 10
buffers = [np.random.randn(1024*1024).astype(np.float32) for _ in range(num_buffers)]
buffer_ptrs = [buf.ctypes.data for buf in buffers]
sizes = [buf.nbytes for buf in buffers]

# Register all buffers
for ptr, size in zip(buffer_ptrs, sizes):
    store.register_buffer(ptr, size)

# Batch put
keys = [f"tensor_{i}" for i in range(num_buffers)]
results = store.batch_put_from(keys, buffer_ptrs, sizes)

# Batch get
recv_buffers = [np.empty(1024*1024, dtype=np.float32) for _ in range(num_buffers)]
recv_ptrs = [buf.ctypes.data for buf in recv_buffers]

for ptr, size in zip(recv_ptrs, sizes):
    store.register_buffer(ptr, size)

results = store.batch_get_into(keys, recv_ptrs, sizes)

# Cleanup
for ptr in buffer_ptrs + recv_ptrs:
    store.unregister_buffer(ptr)

store.close()
```

### Pattern 4: Transfer Engine Direct Transfer
```python
from mooncake.engine import TransferEngine
import numpy as np

# Setup engines on both nodes
engine = TransferEngine()
engine.initialize("127.0.0.1:12345", "127.0.0.1:2379", "tcp", "")

# Allocate and register buffer
buffer = np.ones(1024*1024, dtype=np.uint8)
buffer_ptr = buffer.ctypes.data
engine.register_memory(buffer_ptr, buffer.nbytes)

# Get remote buffer address (from peer via metadata exchange)
remote_addr = engine.get_first_buffer_address("target_host:port")

# Transfer data
data = b"Hello from Transfer Engine!"
engine.write_bytes_to_buffer(buffer_ptr, data, len(data))
result = engine.transfer_sync_write("target_host:port", buffer_ptr, remote_addr, len(data))

# Cleanup
engine.unregister_memory(buffer_ptr)
```

## Error Handling

All methods return status codes:
- `0`: Success
- Negative values: Error codes

Common checks:
```python
# Store operations
result = store.put("key", b"value")
if result != 0:
    print(f"Put failed with error code: {result}")

# Existence check
exists = store.is_exist("key")
if exists == 1:
    print("Key exists")
elif exists == 0:
    print("Key not found")
else:
    print("Error checking existence")

# Transfer operations
result = engine.transfer_sync_write(...)
if result == 0:
    print("Transfer successful")
else:
    print(f"Transfer failed with code: {result}")
```

## Troubleshooting

### Connection Issues
```python
# Force TCP for testing without RDMA
import os
os.environ["MC_FORCE_TCP"] = "true"

# Enable verbose logging
os.environ["MC_LOG_LEVEL"] = "0"
os.environ["MC_YLT_LOG_LEVEL"] = "debug"
```

### Memory Issues
```python
# Check buffer registration before zero-copy ops
result = store.register_buffer(buffer_ptr, size)
if result != 0:
    raise RuntimeError(f"Failed to register buffer: {result}")
```

### Service Connectivity
```bash
# Check master is running
curl http://localhost:50051

# Check metadata server
curl http://localhost:8080/metadata
```

## Best Practices

1. **Always close stores**: Call `store.close()` when done
2. **Register buffers for zero-copy**: Required for RDMA operations
3. **Use batch operations**: Better throughput for multiple operations
4. **Configure replication**: Use `ReplicateConfig` for important data
5. **Use soft pinning**: For frequently accessed objects
6. **Choose protocol wisely**: TCP for dev/test, RDMA for production
7. **Monitor leases**: Objects have TTL, renew if needed
8. **Handle errors**: Check return codes and handle failures

## Quick Reference

### Mooncake Store Methods
- `setup()`: Initialize store
- `put()`, `get()`: Basic operations
- `put_batch()`, `get_batch()`: Batch operations
- `put_from()`, `get_into()`: Zero-copy operations
- `put_tensor()`, `get_tensor()`: PyTorch tensors
- `register_buffer()`, `unregister_buffer()`: Buffer management
- `is_exist()`, `remove()`: Metadata operations
- `close()`: Cleanup

### Transfer Engine Methods
- `initialize()`: Setup engine
- `allocate_managed_buffer()`, `free_managed_buffer()`: Buffer allocation
- `transfer_sync_write()`, `transfer_sync_read()`: Synchronous transfers
- `transfer_submit_write()`, `transfer_check_status()`: Async transfers
- `batch_transfer_sync_write()`, `batch_transfer_sync_read()`: Batch sync
- `batch_transfer_async_write()`, `get_batch_transfer_status()`: Batch async
- `register_memory()`, `unregister_memory()`: Memory registration
- `write_bytes_to_buffer()`, `read_bytes_from_buffer()`: Buffer I/O

## Documentation Links

- Full API Reference: https://kvcache-ai.github.io/Mooncake/
- Mooncake Store: docs/source/python-api-reference/mooncake-store.md
- Transfer Engine: docs/source/python-api-reference/transfer-engine.md
- EP Backend: docs/source/python-api-reference/ep-backend.md
