# TENT Troubleshooting Guide

Common issues and solutions when using TENT.

## Engine Initialization

### "Failed to create engine" / `available()` returns false

**Check configuration file:**
```bash
# Validate JSON syntax
cat transfer-engine.json | jq .

# Check file exists and is readable
ls -la transfer-engine.json
```

**Check metadata server connectivity:**
```bash
# For etcd
etcdctl endpoint health

# Test P2P connectivity
telnet peer-hostname port
```

### RDMA devices not accessible

```bash
# List RDMA devices
ibv_devinfo

# Check device permissions
ls -la /dev/infiniband/

# Add user to infiniband group if needed
sudo usermod -a -G infiniband $USER
```

## Segment Operations

### "Failed to open segment"

**Verify segment name format:**
```cpp
// Correct: "hostname:port"
std::string segment_name = "server-host:12345";
```

**Check if remote is running:**
```bash
# Verify remote process
ps aux | grep your_app

# Check RPC port
netstat -tlnp | grep rpc_port
```

## Memory Registration

### "Failed to register memory"

**For GPU memory:**
```bash
# Check GPU status
nvidia-smi
# or for ROCm
rocm-smi
```

**Verify memory was allocated:**
```cpp
void* buffer = nullptr;
auto status = engine.allocateLocalMemory(&buffer, size);
if (!status.ok()) {
    // Handle allocation failure
}
```

## Transfer Failures

### Transfers stuck in PENDING state

**Check network connectivity:**
```bash
ping peer-host
```

**Enable progress worker for automatic failover:**
```json
{
  "enable_progress_worker": true
}
```

### Transfer timeouts

**Increase timeout in configuration:**
```json
{
  "transports": {
    "rdma": {
      "max_timeout_ns": 30000000000  // 30 seconds
    }
  }
}
```

## Performance Issues

For detailed performance optimization, see [Performance Tuning Guide](performance-tuning.md).

**Quick checks:**
```bash
# Verify RDMA MTU (should be 2048+)
ip link show dev rdma_device | grep mtu

# Check NUMA topology
numactl -H

# Monitor metrics
curl http://localhost:9100/metrics | grep tent
```

## Diagnostic Tools

**Enable debug logging:**
```json
{
  "log_level": "debug"
}
```

**Check metrics:**
```bash
# All TENT metrics
curl http://localhost:9100/metrics | grep tent

# RDMA metrics
curl http://localhost:9100/metrics | grep rdma
```

**Network diagnostics:**
```bash
# Test RDMA connectivity
ib_write_test -d <device> <peer_ip>
ib_read_lat -d <device> <peer_ip>

# Check bandwidth
ib_write_bw -d <device> <peer_ip>
```

## Getting Help

If issues persist:

1. Check [GitHub Issues](https://github.com/kvcache-ai/Mooncake/issues)
2. Enable debug logging and collect:
   - Configuration file
   - Error messages
   - System info (OS, kernel, hardware)
3. File a bug with minimal reproduction case

## See Also

- [Performance Tuning Guide](performance-tuning.md) - Performance optimization
- [Configuration Reference](../configuration/configuration.md) - Configuration options
