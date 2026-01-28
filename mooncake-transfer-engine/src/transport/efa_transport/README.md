# AWS EFA Transport

This directory contains the implementation of the AWS Elastic Fabric Adapter (EFA) transport for Mooncake Transfer Engine.

## Overview

EFA Transport enables high-performance, low-latency communication between EC2 instances in AWS using Elastic Fabric Adapter. It provides RDMA-like capabilities with OS-bypass for efficient data transfer.

## Directory Structure

```
efa_transport/
├── efa_transport.h       # Main transport interface
├── efa_transport.cpp     # Transport implementation
├── efa_context.h         # EFA context management
├── efa_context.cpp       # Context implementation  
├── efa_endpoint.h        # Endpoint/connection management
├── efa_endpoint.cpp      # Endpoint implementation
└── CMakeLists.txt        # Build configuration
```

## Key Components

### EfaTransport
Main transport class that implements the Mooncake Transport interface. Handles:
- Device initialization and management
- Memory registration and deregistration
- Transfer request submission and status tracking
- Connection establishment and handshaking

### EfaContext
Manages resources for each EFA device:
- Protection domains
- Completion queues
- Memory region registration
- Worker pools for async operations

### EfaEndPoint
Manages connections between local and remote EFA devices:
- Queue pair (QP) creation and management
- Connection state management (INIT, RTR, RTS)
- Data transfer operations

## Building

To build with EFA support:

```bash
cmake -DUSE_EFA=ON ..
make
```

## Testing

Run the EFA transport tests:

```bash
# Ensure EFA is available
ibv_devices

# Run tests
ctest -R efa_transport_test
```

## Usage Example

```cpp
#include "transfer_engine.h"

auto engine = std::make_unique<TransferEngine>(false);
engine->init(metadata_server, local_server_name);

// Install EFA transport
Transport *efa = engine->installTransport("efa", nullptr);

// Register memory
void *buffer = /* your buffer */;
engine->registerLocalMemory(buffer, size, "cpu:0");

// Perform transfers
auto batch_id = engine->allocateBatchID(1);
TransferRequest req = { /* ... */ };
engine->submitTransfer(batch_id, {req});
```

## Implementation Notes

- EFA devices are identified by names starting with "rdmap" (e.g., rdmap0s2)
- Memory registration uses ibverbs API
- Supports both blocking and non-blocking operations
- Compatible with existing Mooncake transport infrastructure

## Dependencies

- libibverbs (provided by AWS EFA installer)
- EFA kernel module
- RDMA core libraries

## Performance Considerations

1. **Memory Registration**: Pre-register large buffers for better performance
2. **Batch Operations**: Use batch transfers for multiple small messages
3. **CPU Affinity**: Pin threads to cores for consistent performance
4. **MTU**: Configure jumbo frames (MTU 9000) on EFA interfaces

## Limitations

- EFA requires AWS EC2 instances with EFA support
- Instances must be in the same VPC and subnet
- Security groups must allow all traffic between instances
- Best performance achieved with placement groups

## Future Enhancements

- [ ] Support for GPUDirect RDMA with EFA
- [ ] Enhanced error recovery and retry logic
- [ ] Performance monitoring and metrics
- [ ] Dynamic QP management
- [ ] Multi-path support

## Contributing

When contributing to EFA transport:
1. Follow existing code style
2. Add tests for new features
3. Update documentation
4. Ensure compatibility with other transports

## References

- [AWS EFA Documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html)
- [Mooncake Documentation](../../../../README.md)
- [Transfer Engine Design](../../design/transfer-engine/)
