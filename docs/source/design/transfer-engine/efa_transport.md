# EFA Transport

The source code path for EFA Transport is `Mooncake/mooncake-transfer-engine/src/transport/efa_transport`, which includes headers, source files, and build configuration.

## Overview

EFA Transport is a high-performance network transport layer designed for AWS Elastic Fabric Adapter (EFA). EFA is a network interface for Amazon EC2 instances that enables customers to run applications requiring high levels of inter-node communications at scale on AWS. To compile and use the EFA Transport library, please set the `USE_EFA` flag to `ON` in the `mooncake-common/common.cmake` file.

EFA Transport provides:
- Low-latency inter-node communication on AWS
- Higher throughput compared to traditional TCP
- OS-bypass capabilities for efficient data transfer
- Seamless integration with Mooncake Transfer Engine

## Features

- **AWS Native Integration**: Built specifically for AWS EFA devices
- **High Performance**: Leverages EFA's low-latency, high-bandwidth capabilities
- **Zero-Copy Operations**: Efficient memory operations without unnecessary data copying
- **Scalable**: Designed for distributed workloads across multiple EC2 instances
- **Compatible API**: Follows the same Transport interface as other Mooncake transports

## Prerequisites

### Hardware Requirements

- AWS EC2 instances with EFA support (e.g., c5n.18xlarge, p3dn.24xlarge, p4d.24xlarge)
- EFA-enabled network interfaces

### Software Dependencies

In addition to the dependencies already required by Mooncake, EFA Transport needs:

**EFA Kernel Module and Libraries**
```bash
# On Amazon Linux 2 or AL2023
sudo yum install efa -y

# On Ubuntu 20.04/22.04
wget https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
tar -xf aws-efa-installer-latest.tar.gz
cd aws-efa-installer
sudo ./efa_installer.sh -y
```

**libibverbs** (usually included with EFA installer)
```bash
# Verify installation
ibv_devices
```

## Compilation

To compile Mooncake with EFA Transport support:

1. Enable EFA in the CMake configuration:
   ```bash
   cd mooncake-common
   # Edit common.cmake and set USE_EFA to ON
   # Or use CMake command line:
   cmake -DUSE_EFA=ON ..
   ```

2. Build Mooncake:
   ```bash
   mkdir -p build && cd build
   cmake -DUSE_EFA=ON ..
   make -j$(nproc)
   ```

## Configuration

### Environment Setup

Before using EFA Transport, ensure that:

1. EFA devices are properly configured:
   ```bash
   # List available EFA devices
   ibv_devices
   # Should show devices like "rdmap0s2", "rdmap1s3", etc.
   ```

2. Security groups allow EFA traffic (no specific ports needed, uses OS-bypass)

3. Instances are in the same placement group for optimal performance

### Using EFA Transport in Your Application

#### C++ API

```cpp
#include "transfer_engine.h"

using namespace mooncake;

// Initialize Transfer Engine
auto engine = std::make_unique<TransferEngine>(false);
engine->init(metadata_server, local_server_name, hostname, port);

// Install EFA transport
Transport *efa_transport = engine->installTransport("efa", nullptr);

// Register memory for RDMA operations
void *buffer = /* allocate memory */;
size_t buffer_size = /* size */;
engine->registerLocalMemory(buffer, buffer_size, "cpu:0");

// Create and submit transfer requests
auto batch_id = engine->allocateBatchID(1);
TransferRequest request;
request.opcode = TransferRequest::WRITE;
request.source = buffer;
request.target_id = remote_segment_id;
request.target_offset = remote_offset;
request.length = data_length;

Status status = engine->submitTransfer(batch_id, {request});

// Wait for completion
TransferStatus transfer_status;
while (true) {
    engine->getTransferStatus(batch_id, 0, transfer_status);
    if (transfer_status.s == TransferStatusEnum::COMPLETED) break;
}

engine->freeBatchID(batch_id);
```

#### Python API

```python
from mooncake_transfer_engine import TransferEngine

# Initialize engine
engine = TransferEngine()
engine.init(metadata_server, local_server_name)

# Install EFA transport
efa_transport = engine.install_transport("efa")

# Register memory and perform transfers
# (Python API follows similar patterns to C++)
```

## Performance Tuning

### EFA-Specific Optimizations

1. **MTU Configuration**: EFA supports jumbo frames (up to 9000 bytes)
   ```bash
   # Set MTU for EFA interface
   sudo ip link set dev efa0 mtu 9000
   ```

2. **CPU Affinity**: Pin worker threads to specific CPU cores
   ```cpp
   // Set CPU affinity for best performance
   // Implementation depends on your workload
   ```

3. **Batch Transfers**: Use batch operations for multiple small transfers
   ```cpp
   std::vector<TransferRequest> requests;
   // Add multiple requests
   engine->submitTransfer(batch_id, requests);
   ```

### Monitoring and Diagnostics

Check EFA statistics:
```bash
# View EFA device statistics
cat /sys/class/infiniband/rdmap0s2/hw_counters/*

# Monitor with ethtool (if available)
ethtool -S efa0
```

## Important Notes

1. **Network Requirements**:
   - EFA requires instances to be in the same VPC and subnet
   - Security groups must allow all traffic between instances
   - Placement groups recommended for lowest latency

2. **Memory Registration**:
   - Memory must be pinned for RDMA operations
   - Large memory registrations may take time
   - Consider pre-registering frequently used buffers

3. **Error Handling**:
   - EFA connections may timeout if remote node is unreachable
   - Implement retry logic for transient failures
   - Monitor connection health proactively

4. **Compatibility**:
   - EFA Transport is compatible with other Mooncake transports
   - Can be used alongside RDMA, TCP, or other transport layers
   - Protocol selection is automatic based on segment metadata

## Troubleshooting

### Common Issues

**Issue**: `Failed to get IB device list`
```bash
# Solution: Verify EFA driver installation
sudo modprobe ib_uverbs
lsmod | grep ib_
```

**Issue**: `No EFA devices found in topology`
```bash
# Solution: Check if EFA devices are visible
ibv_devices
# Ensure devices are named with "rdmap" prefix
```

**Issue**: Connection timeouts
```bash
# Solution: Verify network connectivity
# 1. Check security group rules
# 2. Ensure instances are in same placement group
# 3. Test basic connectivity with fi_pingpong
```

## Example Applications

See the `mooncake-transfer-engine/example/` directory for complete examples:

- `transfer_engine_bench.cpp`: Basic benchmark using EFA
- `transfer_engine_validator.cpp`: Validation and testing tool

## References

- [AWS EFA Documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html)
- [EFA Performance Optimization Guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html)
- [Mooncake Transfer Engine Overview](../../../README.md)

## Support

For issues specific to EFA Transport, please:
1. Check AWS EFA status and configuration
2. Review Mooncake logs for error messages
3. Open an issue on the Mooncake GitHub repository
