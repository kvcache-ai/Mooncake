# Ascend Transport

The source code path for Ascend Transport is `Mooncake/mooncake-transfer-engine/src/transport/ascend_transport`, which also includes automated build scripts and the README file.

## Overview

Ascend Transport is a high-performance zero-copy NPU data transfer library with one-sided semantics, directly compatible with Mooncake Transfer Engine. To compile and use the Ascend Transport library, please set the `USE_ASCEND` flag to `"ON"` in the `mooncake-common/common.cmake` file.

Ascend Transport supports inter-NPU data transfer using one-sided semantics (currently supports Device to Device; other modes are under development). Users only need to specify the node and memory information at both ends through the Mooncake Transfer Engine interface to achieve high-performance point-to-point data transfer. Ascend Transport abstracts away internal complexities and automatically handles operations such as establishing connections, registering and exchanging memory, and checking transfer status.

Ascend Transport supports the batch transfer interface batch_transfer_sync provided by Mooncake in transfer_engine_py.cpp, which enables batch transmission of multiple non-contiguous memory blocks targeting the same destination card. Test results demonstrate that when transferring small non-contiguous memory blocks (e.g., 128KB), using the batch transfer interface achieves over 100% bandwidth improvement compared to using the transferSync interface.

### New Dependencies

In addition to the dependencies already required by Mooncake, Ascend Transport needs some HCCL-related dependencies:

**MPI**
```bash
yum install -y mpich mpich-devel
```
or
```bash
apt-get install -y mpich libmpich-dev
```

**Ascend Compute Architecture for Neural Networks**
Updated to Ascend Compute Architecture for Neural Networks 8.2.RC1. pkg packages are no longer required.

## One-Step Compilation Script

Ascend Transport provides a one-step compilation script to simplify the setup process. The script is located at `scripts/ascend/dependencies_ascend.sh`. To execute the script, follow these steps:

```bash
sh scripts/ascend/dependencies_ascend.sh
```

The one-step compilation script also accommodates scenarios where users are unable to directly perform `git clone` operations in their environment. Users can place the source code of the dependencies that would normally be cloned via `git` and the Mooncake source code in the same directory. The script will automatically compile the dependencies and Mooncake.

The output artifacts of Mooncake include the `libascend_transport_mem.so` and `mooncake-wheel/dist/mooncake_transfer_engine*.whl`.

**Environment Setup**
Before using `ascend_transport`, ensure that the environment variables are set. You can use the following command:

```bash
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH
```

Alternatively, you can copy the `.so` file to another path referenced by `$LD_LIBRARY_PATH`.

## Important Notes (Mandatory)

1. **Initialization Parameter Change**:
   When calling `TransferEngine::initialize`, the `local_server_name` parameter must include the physical ID of the NPU on which it is running. The format of `local_server_name` has changed from `ip:port` to `ip:port:npu_x`. For example, `"0.0.0.0:12345:npu_2"`. Ascend Transport will internally parse and obtain the physical ID of the NPU. The format for `segment_name` stored in metadata and `target_hostname` used during transfer remains unchanged, still in the format of `ip:port`, for example, `"0.0.0.0:12345"`.

2. **TCP Connection and Port Usage**:
   Ascend Transport will establish a TCP connection on the host side, occupying a port number that is `10000` plus the physical ID of the NPU. If this port is already in use, it will automatically search for an available port.

3. **Memory Alignment Requirement**:
   Due to internal interface limitations, Ascend Transport currently requires that the registered memory be NPU memory and must be aligned to 2M.

4. **hccn.conf File Requirement**:
   Ensure that the file `hccn.conf` exists in the `/etc` directory. This is especially important when running inside a container. You should either mount `/etc/hccn.conf` or copy the file from the host machine to the container's `/etc` directory.

5. **potential MPI Conflicts**:
   Concurrently installing MPI (typically MPICH) and OpenMPI may cause conflicts. If you encounter MPI-related issues, try running the following commands:
```bash
   sudo apt install mpich libmpich-dev
   sudo apt purge openmpi-bin libopenmpi-dev
```

6. **IPV6 is not support**:
   IPv6 is not supported in this release; an IPv6-compatibility patch will be delivered shortly.

## One-Step Installation Script (Without Compiling Mooncake)

If you have already compiled Mooncake on one machine, you can skip the compilation step on other machines with the same system and only install the dependencies. You can directly use the output artifacts of Mooncake, `libascend_transport_mem.so` and `mooncake-wheel/dist/mooncake_transfer_engine*.whl`.

To do this, place `mooncake-wheel/dist/mooncake_transfer_engine*.whl`, `libascend_transport_mem.so`, and the script `scripts/ascend/dependencies_ascend_installation.sh` in the same directory. Then, execute the script as follows:

```bash
./dependencies_ascend_installation.sh
```

**Environment Setup**
Before using `ascend_transport`, ensure that the environment variables are set. You can use the following command:

```bash
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH
```

Alternatively, you can copy the `.so` file to another path referenced by `$LD_LIBRARY_PATH`.

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

### Metadata Service

Ascend Transport is compatible with all metadata services currently supported by Mooncake, including `etcd`, `redis`, `http`, and `p2phandshake`. Upon initialization, Ascend Transport registers key NPU card information such as `device_id`, `device_ip`, `rank_id`, and `server_ip`.

### Data Transfer

Ascend Transport supports write/read semantics and automatically determines whether cross-HCCS communication is needed, selecting either HCCS or ROCE as the underlying transport protocol. Users can use the standard Mooncake `getTransferStatus` API to monitor the progress of each transfer request.

### Fault Handling

Building upon HCCL’s built-in fault handling mechanisms, Ascend Transport implements comprehensive error recovery strategies across multiple stages, including initialization, connection setup, and data transfer. It incorporates retry logic and returns precise error codes based on HCCL collective communication standards when retries fail. For detailed logs, refer to `/root/Ascend/log/debug/plog`.

### Test Cases

Ascend Transport provides multi-scenario test files: `mooncake-transfer-engine/example/transfer_engine_ascend_one_sided.cpp`, which supports one-to-one, one-to-two, and two-to-one transfer tests, as well as a performance benchmark file `mooncake-transfer-engine/example/transfer_engine_ascend_perf.cpp`. After successfully compiling the Transfer Engine, the corresponding test programs can be found in the `build/mooncake-transfer-engine/example` directory.The test programs accept configuration parameters via command-line arguments. For the full list of configurable options, refer to the DEFINE_string definitions at the beginning of each test file.

When `metadata_server` is set to `P2PHANDSHAKE`, Mooncake randomly selects a port in the new RPC port-mapping to avoid conflicts.  
Therefore, in testing:

1. **Start the target node first**.  
   Watch the log produced by `mooncake-transfer-engine/src/transfer_engine.cpp`; you should see a line similar to  
   ```
   Transfer Engine RPC using <protocol> listening on <IP>:<actual-port>
   ```  
   Note the **actual port** the target node is listening on.

2. **Edit the initiator’s launch command**:  
   Change the value of `--segment_id` to `<IP>:<actual-port>` (i.e., the target node’s IP plus the port you just captured).

3. **Launch the initiator node** to complete the test.

Refer to the command format shown below:

#### Example Commands for Scenario Testing

**Start Initiator Node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608 --batch_size=32 
```

**Start Target Node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --block_size=8388608 --batch_size=32 
```

#### Example Commands for Performance Testing

**Start Initiator Node:**
```bash
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=16384 --batch_size=32 --block_iteration=10
```

**Start Target Node:**
```bash
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --batch_size=32 --block_iteration=10
```

**Note:** The `device_id` parameter mentioned above represents both the NPU logical ID and physical ID. If not all devices are mounted in your container, please specify the logical ID and physical ID separately when using the demo above. Replace `--device_id` with `--device_logicid` and `--device_phyid`.

For example, if only device 5 and device 7 are mounted in the container, with device 5 as the initiator and device 7 as the receiver, the commands for multi-scenario use cases should be modified as follows:

**Start the initiator node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_logicid=0 --device_phyid=5 --mode=initiator --block_size=8388608
```

**Start the target node:**
```bash
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_logicid=1 --device_phyid=7 --mode=target --block_size=8388608
```

### Print Description
If you need to obtain information about whether each transport request is cross-hccs and its corresponding execution time, you can enable the related logs by setting the environment variable. Use the following command to turn on the logging:

```bash
export ASCEND_TRANSPORT_PRINT=1
```

### Timeout Configuration
Ascend Transport based on TCP for out-of-band communication, has a connection timeout configured via the environment variable `Ascend_TCP_TIMEOUT`, with a default value of 30 seconds. On the host side, the `recv` timeout is set to 30 seconds, meaning that if no message is received from the peer within 30 seconds, an error will be reported.

The connection timeout for `hccl_socket` is configured through the environment variable `Ascend_HCCL_SOCKET_TIMEOUT`, with a default value of 30 seconds. If this timeout is exceeded, the current transmission will report an error and return.

`hccl_socket` has a keep-alive requirement, and the execution timeout is configured via the environment variable `HCCL_EXEC_TIMEOUT`. If no communication occurs within the `HCCL_EXEC_TIMEOUT` period, the `hccl_socket` connection will be disconnected.

In `transport_mem`, the point-to-point communication between endpoints involves a connection handshake process, and its timeout is configured via `Ascend_TRANSPORT_MEM_TIMEOUT`, with a default value of 120 seconds.

### Error Code
Ascend transport error codes reuse the HCCL collective communication transport error codes.
typedef enum {
    HCCL_SUCCESS = 0,       /* success */
    HCCL_E_PARA = 1,        /* parameter error */
    HCCL_E_PTR = 2,         /* empty pointer, checking if operations like notifypool->reset are performed multiple times leading to pointer clearing */
    HCCL_E_MEMORY = 3,      /* memory error */
    HCCL_E_INTERNAL = 4,    /* Internal error. Common causes include:
                            * - Memory not registered
                            * - Memory registration error
                            * - The starting address of the registered memory is not 2M aligned
                            * - Memory not successfully swapped
                            * Check whether the memory to be transferred is within the range of registered memory.
                            * In multi-machine scenarios, check whether the startup script is mixed used.
                            */
    HCCL_E_NOT_SUPPORT = 5, /* feature not supported */
    HCCL_E_NOT_FOUND = 6,   /* specific resource not found */
    HCCL_E_UNAVAIL = 7,     /* resource unavailable */
    HCCL_E_SYSCALL = 8,     /* error calling system interface, prioritize checking if initialization parameters are correctly passed */
    HCCL_E_TIMEOUT = 9,     /* timeout */
    HCCL_E_OPEN_FILE_FAILURE = 10, /* open file fail */
    HCCL_E_TCP_CONNECT = 11, /* tcp connect fail, connection failure, limited to checking ports on both ends and connectivity */
    HCCL_E_ROCE_CONNECT = 12, /* roce connect fail, connection failure, limited to checking ports on both ends and connectivity */
    HCCL_E_TCP_TRANSFER = 13, /* tcp transfer fail, check if the port is correct and if SOCKET is mixed */
    HCCL_E_ROCE_TRANSFER = 14, /* roce transfer fail */
    HCCL_E_RUNTIME = 15,      /* call runtime api fail, check the transportmem-related parameters filled in for transmission */
    HCCL_E_DRV = 16,          /* call driver api fail */
    HCCL_E_PROFILING = 17,    /* call profiling api fail */
    HCCL_E_CCE = 18,          /* call cce api fail */
    HCCL_E_NETWORK = 19,      /* call network api fail */
    HCCL_E_AGAIN = 20,        /* operation repeated, in some cases it is not an error. For example, registering the same memory multiple times will directly return the handle from the first registration. If it does not affect the functionality, it is considered successful */
    HCCL_E_REMOTE = 21,       /* error cqe */
    HCCL_E_SUSPENDING = 22,   /* error communicator suspending */
    HCCL_E_RESERVED           /* reserved */
} HcclResult;
