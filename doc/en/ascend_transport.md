# Ascend Transport

The source code path for Ascend Transport is `Mooncake/mooncake-transfer-engine/src/transport/ascend_transport`, which also includes automated build scripts and the README file.

## Overview

Ascend Transport is a high-performance zero-copy NPU data transfer library with one-sided semantics, directly compatible with Mooncake Transfer Engine. To compile and use the Ascend Transport library, please set the `USE_ASCEND` flag to `"ON"` in the `mooncake-common/common.cmake` file.

Ascend Transport supports inter-NPU data transfer using one-sided semantics (currently supports Device to Device; other modes are under development). Users only need to specify the node and memory information at both ends through the Mooncake Transfer Engine interface to achieve high-performance point-to-point data transfer. Ascend Transport abstracts away internal complexities and automatically handles operations such as establishing connections, registering and exchanging memory, and checking transfer status.

### New Dependencies

In addition to the dependencies already required by Mooncake, Ascend Transport needs some HCCL-related dependencies:

**MPI**
```bash
yum install -y mpich mpich-devel
```

**Ascend Compute Architecture for Neural Networks**
Ascend Compute Architecture for Neural Networks 8.1.RC1 version

### One-Step Build Script

Ascend Transport provides a one-step build script located at `scripts/ascend/dependencies_ascend.sh`. Copy this script to the desired installation directory and run it. You can also pass an installation path as an argument; if not provided, it defaults to the current directory:

```bash
./dependencies_ascend.sh /path/to/install_directory
```

This script also supports environments where users cannot perform `git clone` directly. Users can place the source code for dependencies and Mooncake in the target directory, and the script will handle the compilation accordingly.

### One-Step Installation Script (Without Building Mooncake)

To avoid potential conflicts when running other processes during Mooncake compilation, Ascend Transport offers a solution that separates the build and runtime environments.

After completing the Mooncake build via dependencies_ascend.sh, you can run dependencies_ascend_installation.sh to install only the required dependencies. Place the generated Mooncake .whl package and libascend_transport_mem.so into the installation directory.

Copy the script to the installation directory and run:
```bash
./dependencies_ascend_installation.sh /path/to/install_directory
```

Before use, ensure that `libascend_transport_mem.so` has been copied to `/usr/local/Ascend/ascend-toolkit/latest/python/site-packages`, then execute:
```bash
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH
```

### Build Instructions

Once all dependencies are installed successfully, you can proceed with building Mooncake normally. If errors occur, try setting the following environment variable:
```bash
export CPLUS_INCLUDE_PATH=$(echo $CPLUS_INCLUDE_PATH | tr ':' '\n' | grep -v "/usr/local/Ascend" | paste -sd: -)
```

### Endpoint Management

Each Huawei NPU card has a dedicated parameter-plane NIC and should be managed by a single `TransferEngine` instance responsible for all its data transfers.

### Ranktable Management
Ascend Transport does not rely on global Ranktable information. It only needs to obtain the local Ranktable information of the current NPU card. During the initialization of Ascend Transport, it will automatically parse the /etc/hccn.conf file to acquire this information.

### Initialization

When using Ascend Transport, the `TransferEngine` must still call the `init` function after construction:

```cpp
TransferEngine();

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port)
```

The only difference is that the `local_server_name` parameter must now include the physical NPU card ID. The format changes from `ip:port` to `ip:port:npu_x`, e.g., `"0.0.0.0:12345:npu_2"`.

> **Note**: This extension of the `local_server_name` is used internally by Ascend Transport without modifying Mooncake's external API. The `segment_desc_name` in metadata remains in the original format (`ip:port`). Therefore, each NPU card must use a unique port that is not occupied.

### Metadata Service

Ascend Transport is compatible with all metadata services currently supported by Mooncake, including `etcd`, `redis`, `http`, and `p2phandshake`. Upon initialization, Ascend Transport registers key NPU card information such as `device_id`, `device_ip`, `rank_id`, and `server_ip`.

### Data Transfer

Ascend Transport supports write/read semantics and automatically determines whether cross-HCCS communication is needed, selecting either HCCS or ROCE as the underlying transport protocol. Users can use the standard Mooncake `getTransferStatus` API to monitor the progress of each transfer request.

### Fault Handling

Building upon HCCL’s built-in fault handling mechanisms, Ascend Transport implements comprehensive error recovery strategies across multiple stages, including initialization, connection setup, and data transfer. It incorporates retry logic and returns precise error codes based on HCCL collective communication standards when retries fail. For detailed logs, refer to `/root/Ascend/log/plog`.

### Test Cases

Ascend Transport provides two test files:
- Multi-scenario test: `mooncake-transfer-engine/example/transfer_engine_ascend_one_sided.cpp`
- Performance test: `mooncake-transfer-engine/example/transfer_engine_ascend_perf.cpp`

You can configure various scenarios (e.g., 1-to-1, 1-to-2, 2-to-1) and performance tests by passing valid parameters to these programs.

#### Example Commands for Scenario Testing

**Start Initiator Node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608
```

**Start Target Node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --block_size=8388608
```

#### Example Commands for Performance Testing

**Start Initiator Node:**
```bash
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608
```

**Start Target Node:**
```bash
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target
```

### Print Description
If you need to obtain information about whether each transport request is cross-hccs and its corresponding execution time, you can enable the related logs by setting the environment variable. Use the following command to turn on the logging:

```bash
export ASCEND_TRANSPORT_PRINT=1
```

### Notes
ascend_transport will establish a TCP connection on the host side. This connection uses port (10000 + deviceId). Please avoid using this port for other services to prevent conflicts.

ascend_transport has an automatic reconnection mechanism in place that triggers after a transfer is completed, in case the remote end goes offline and restarts. There is no need to manually restart the local service.

Note If the target end goes offline and restarts, the initiating end will attempt to re-establish the connection when it sends the next request. The target must complete its restart and become ready within 5 seconds after the initiator sends the request. If the target does not become available within this window, the connection will fail and an error will be returned.

### Timeout Configuration
Ascend Transport uses TCP-based out-of-band communication on the host side, with a receive timeout set to 120 seconds.

Connection timeout is controlled by the environment variable HCCL_CONNECT_TIMEOUT.
Execution timeout is configured via HCCL_EXEC_TIMEOUT.
If no communication occurs within this timeout, the hccl_socket connection will be terminated.

Point-to-point communication between endpoints involves a connection handshake with a timeout of 120 seconds.
