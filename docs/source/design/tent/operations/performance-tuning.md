# TENT Performance Tuning Guide

Performance optimization techniques for TENT.

## Quick Configuration

### Enable High-Performance Transports

```json
{
  "transports": {
    "rdma": { "enable": true },
    "tcp": { "enable": true }
  },
  "policy": [{
    "name": "default",
    "segment_type": "memory",
    "transports": ["rdma", "tcp"]
  }]
}
```

### Multi-Rail RDMA

```json
{
  "topology": {
    "rdma_whitelist": ["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"]
  }
}
```

## RDMA Parameters

### Completion Queue Size

```json
{
  "transports": {
    "rdma": {
      "device": {
        "max_cqe": 16384
      }
    }
  }
}
```

Larger values allow more outstanding operations.

### Block Size

```json
{
  "transports": {
    "rdma": {
      "workers": {
        "block_size": 131072  // 128KB
      }
    }
  }
}
```

Adjust based on transfer size:
- Small transfers (<1MB): 32KB - 64KB
- Medium transfers (1-100MB): 128KB - 256KB
- Large transfers (>100MB): 256KB - 512KB

## Memory Optimization

### Batch Memory Registration

```cpp
// Efficient: register multiple buffers at once
std::vector<void*> addrs = {buf1, buf2, buf3};
std::vector<size_t> sizes = {size1, size2, size3};
engine.registerLocalMemory(addrs, sizes);
```

### NUMA-Aware Allocation

```bash
# Pin process to NUMA node
numactl --cpunodebind=0 --membind=0 ./your_app
```

## Batch Optimization

### Throughput-Oriented

```cpp
// Larger batches for bulk transfer
BatchID batch = engine.allocateBatch(1000);
```

### Latency-Oriented

```cpp
// Smaller batches for low latency
BatchID batch = engine.allocateBatch(10);
```

## Transport Selection

### Priority-Based Selection

```json
{
  "policy": [
    {
      "name": "low_latency",
      "priority": "high",
      "transports": ["rdma"]
    },
    {
      "name": "bulk",
      "priority": "low",
      "transports": ["rdma", "tcp"]
    }
  ]
}
```

### Size-Based Selection

```json
{
  "policy": [
    {
      "name": "small",
      "max_size": 65536,
      "transports": ["shm", "rdma"]
    },
    {
      "name": "large",
      "min_size": 65536,
      "transports": ["rdma", "tcp"]
    }
  ]
}
```

## Monitoring

### Check Metrics

```bash
# Transfer metrics
curl http://localhost:9100/metrics | grep tent_transfer

# Latency metrics
curl http://localhost:9100/metrics | grep tent_latency

# Queue depth
curl http://localhost:9100/metrics | grep tent_queue
```

### Enable Debug Logging

```json
{
  "log_level": "debug"
}
```

## Common Issues

### Poor Throughput

Check:
- RDMA MTU settings (should be 2048+)
- NUMA alignment
- Rail health in metrics

### High Latency

Check:
- Network baseline latency
- Queue depths
- Retry behavior in logs

## See Also

- [Configuration Reference](../configuration/configuration.md) - Detailed configuration options
- [QoS](../features/qos.md) - Quality of Service configuration
- [Metrics](../features/metrics.md) - Performance monitoring
