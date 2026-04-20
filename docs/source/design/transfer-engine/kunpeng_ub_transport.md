# Kunpeng UB Transport for Mooncake

This document describes how to build and use Mooncake with Kunpeng UB (Unified Bus) transport support using URMA (Unified Remote Memory Access).

## Overview

UB (Unified Bus) is a transport protocol at the same abstraction layer as RDMA, CXL, NVLink, and TCP, providing a flexible transport solution that can be selected at the application layer. Currently, UB protocol has two open-source implementations:

- **URMA (Unified Remote Memory Access)**: Provides a unified programming abstraction and core semantic layer for upper-layer applications. It offers unified APIs and semantic interfaces for remote shared memory access and operations, leveraging the low-latency, high-bandwidth characteristics of the UB protocol.
  - URMA open-source repository: https://atomgit.com/openeuler/umdk

- **OBMM (Ownership Based Memory Management)**: A kernel memory management system for supernode environments, supporting cross-node physical memory sharing. It provides efficient remote memory access capabilities through a kernel module (obmm.ko) and a user-space library (libobmm.so).
  - OBMM open-source repository: https://atomgit.com/openeuler/obmm

## Prerequisites

### 1. Hardware and Operating System

- **Hardware Platform**: Kunpeng 950 CPU with native UB interconnect architecture
- **OS Version**: openEuler 24.03 (LTS-SP3) [Download link](https://www.openeuler.openatom.cn/zh/download/#openEuler%2024.03%20LTS%20SP3)

### 2. URMA Dependencies

Install UMDK (URMA development package):

```bash
# Install via yum
yum install umdk-urma-devel

# Or build from source
git clone https://atomgit.com/openeuler/umdk.git
cd umdk
mkdir build && cd build
cmake ..
make -j$(nproc)
sudo make install
```

### 3. Build Dependencies

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libgflags-dev \
    libgoogle-glog-dev \
    libjsoncpp-dev \
    libnuma-dev \
    libibverbs-dev \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libgtest-dev \
    libmsgpack-dev \
    libxxhash-dev \
    libyaml-cpp-dev \
    pybind11-dev \
    python3-dev

# Install yalantinglibs (required)
cd /tmp
git clone https://github.com/alibaba/yalantinglibs.git
cd yalantinglibs
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local
make -j$(nproc)
sudo make install
```

## Building Mooncake with UB Support

### 1. Clone the Repository

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
git submodule update --init --recursive
```

### 2. Build with UB Enabled

```bash
mkdir build && cd build

cmake .. \
    -DUSE_UB=ON \
    -DURMA_INCLUDE_DIR=/usr/include \
    -DURMA_LIBRARY=/usr/lib64/liburma.so \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo

make -j$(nproc)
```

### 3. Install Python Package

```bash
# Copy built modules to wheel directory
cp mooncake-integration/engine.cpython-*.so ../mooncake-wheel/mooncake/
cp mooncake-integration/store.cpython-*.so ../mooncake-wheel/mooncake/
cp mooncake-asio/libasio.so ../mooncake-wheel/mooncake/

# Install with pip
pip install -e ../mooncake-wheel --no-build-isolation
```

## Verification

### Check UB Transport Registration

```bash
# Check if UB transport is registered
./mooncake_server --list-transports
# Expected output: rdma, tcp, nvlink, ub
```

### Test UB Transport Initialization

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
result = te.initialize('127.0.0.1', 'P2PHANDSHAKE', 'ub', '')
print(f'Initialize result: {result}')  # Should be 0

# You should see logs like:
# URMA module init success
# found 1 devices.
# device_name : urma0 EID : 01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10
```

## Usage

### Single Node Benchmark Test

```bash
# Terminal 1: Target (receiver)
./transfer_engine_bench \
    --mode=target \
    --protocol=ub \
    --device_name=urma0 \
    --local_server_name=127.0.0.1 \
    --metadata_server=P2PHANDSHAKE

# Terminal 2: Initiator (sender)
./transfer_engine_bench \
    --mode=initiator \
    --protocol=ub \
    --device_name=urma0 \
    --metadata_server=P2PHANDSHAKE \
    --segment_size=8388608 \
    --batch_size=1 \
    --segment_id=127.0.0.1:$PORT
```

### Multi-device Benchmark Test

```bash
# Auto-discovery of multiple URMA devices
./transfer_engine_bench \
    --protocol=ub \
    --device_name=urma0,urma1,urma2,urma3
```

## Unit Tests

Run the UB transport unit tests:

```bash
./build/mooncake-transfer-engine/tests/ub_transport_test
```

The test suite includes:

| Test | Description |
|------|-------------|
| `MultiWrite` | Multiple write operations |
| `MultipleRead` | Multiple read operations with data integrity check |

You can also run all unit tests via CTest:

```bash
cd build && ctest --output-on-failure
```

Environment variables for test configuration:

```bash
export MC_METADATA_SERVER=P2PHANDSHAKE     # default
export MC_LOCAL_SERVER_NAME=127.0.0.1:12345  # default
```

## Technical Details

### UB Transport Architecture

```
┌─────────────────────────────────────────────────────┐
│                   UbTransport                       │
├─────────────────────────────────────────────────────┤
│  UrmaContext (per device)                          │
│  ├── urma_device   (URMA device handle)            │
│  ├── urma_context  (URMA context)                 │
│  ├── urma_jfce     (URMA jetty factory create)     │
│  ├── urma_jfc      (URMA jetty factory send)       │
│  └── urma_jfr      (URMA jetty factory receive)    │
├─────────────────────────────────────────────────────┤
│  UrmaEndpoint (per connection)                     │
│  ├── urma_jetty    (URMA jetty for communication)  │
│  ├── local_jetty   (local jetty ID)                │
│  └── remote_jetty  (remote jetty ID)               │
└─────────────────────────────────────────────────────┘
```

### Key Components

1. **UbTransport**: The main transport class that manages URMA resources and endpoints
2. **UrmaContext**: Represents a URMA device context, handling device initialization and resource management
3. **UrmaEndpoint**: Represents a connection to a remote peer, handling data transfer operations
4. **mock_urma_api.cpp**: Mock implementation of URMA API for testing without real URMA hardware

### Protocol Advantages

- **Optimized for Kunpeng**: URMA is specifically optimized for Kunpeng chip on-chip interconnect
- **RDMA-like Semantics**: Provides similar memory semantics to RDMA
- **High Performance**: Leverages UB's low-latency, high-bandwidth characteristics
- **Unified Abstraction**: Offers a unified programming model for remote memory access

## Troubleshooting

### No URMA devices found

```
UbTransport: No URMA devices found
```

Solution: Verify URMA is properly installed and devices are available:
```bash
# Check URMA installation
ls /usr/lib64/liburma.so
ls /usr/include/ub/umdk/urma/urma_api.h

# Check for URMA devices
urma_admin -l
```

### URMA initialization failed

```
URMA module init failed
```

Solution: Ensure the URMA kernel module is loaded and the device is properly configured:
```bash
# Load URMA module
sudo modprobe urma

# Check module status
sudo lsmod | grep urma

# Check device status
urma_admin -l
```

### Device port inactive

```
Device urma0 port not active
```

Solution: Ensure the UB port is properly configured and active:
```bash
# Check port status
urma_admin -p urma0
```

### Missing liburma.so

```
cannot find -lurma
```

Solution: Verify URMA library is installed and in the library path:
```bash
export LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH
```

## Conclusion

Kunpeng UB Transport provides a high-performance, optimized transport solution for Mooncake on Kunpeng 950 CPU platforms. By leveraging the UB protocol's low-latency and high-bandwidth characteristics, it offers comparable performance to RDMA while being specifically tailored for Kunpeng chip architectures.

With proper configuration and tuning, UB Transport can significantly improve the performance of distributed AI workloads, particularly for scenarios involving large-scale parameter transfers and distributed training.
