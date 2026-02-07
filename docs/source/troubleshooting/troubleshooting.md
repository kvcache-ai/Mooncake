# Troubleshooting

This document lists common errors that may occur when using Mooncake Store and provides troubleshooting and resolution measures.

> **CheckList**
> - [ ] `connectable_name` is not the local machine's LAN/WAN address, such as the loopback address (`127.0.0.1`/`localhost`) and address of other machines.
> - [ ] Incorrect MTU and GID configurations. Use the environment variables MC_MTU and MC_GID_INDEX.
> - [ ] Incorrect RDMA device name and connection status is not active.
> - [ ] etcd is not started normally and it is not bind with `0.0.0.0`.


## Metadata and Out-of-Band Communication
1. At startup, a `TransferMetadata` object is constructed according to the incoming `metadata_server` parameter. During program execution, this object is used to communicate with the etcd server to maintain internal data required for connection.
2. At startup, the current node is registered with the cluster according to the incoming `connectable_name` parameter and `rpc_port` parameter, and the TCP port specified by the `rpc_port` parameter is listened. Before other nodes send the first read/write request to the current node, they will use the above information, resolve DNS, and initiate a connection through socket's `connect()` method. 

Errors in this part usually indicate that the error occurred within the `mooncake-transfer-engine/src/transfer_metadata.cpp` file.

### Recommended Troubleshooting Directions
1. The incoming `metadata_server` parameter is not a valid and reachable etcd server address (or a group of addresses). In this case, an `Error from etcd client` error will be displayed. It is recommended to investigate from the following aspects:
    - After installing the etcd service, the default listening IP is 127.0.0.1, which other nodes cannot use. Therefore, the actual listening IP should be determined in conjunction with the network environment. In the experimental environment, 0.0.0.0 can be used. For example, the following command line can be used to start the required service:
      ```bash
      # This is 10.0.0.1
      etcd --listen-client-urls http://0.0.0.0:2379  --advertise-client-urls http://10.0.0.1:2379
      ```
      You can verify this on other nodes using `curl <metadata_server>`.
    - HTTP proxies need to be disabled before starting the program.
      ```bash
      unset http_proxy
      unset https_proxy
      ```

2. Other nodes cannot establish a socket out-of-band communication with the current node through the incoming `connectable_name` parameter and `rpc_port` parameter to implement connection establishment operations. The types of errors in this case may include:
    - A `bind address already in use` error is displayed when starting the process, usually because the port number corresponding to the `rpc_port` parameter is occupied. Try using another port number.
    - Another node displays a `connection refused` type of error when initiating a Batch Transfer to the current node, which requires a focus on checking the correctness of these two parameters when the current node is created.
      - `connectable_name` must be a non-Loopback IP address of the current node or a valid Hostname (with records in DNS or `/etc/hosts`), and other nodes in the cluster can use `connectable_name` and `rpc_port` parameters to connect to the current node.
      - There may be firewall mechanisms in some networks that need to be added to the whitelist in advance.
    - If the correspondence between `local_server_name` and `connectable_name`/`rpc_port` parameters changes and various errors occur, you can try clearing the etcd database and restarting the cluster.

## RDMA Resource Initialization

1. At startup, all RDMA network cards corresponding to the incoming `nic_priority_matrix` parameter are initialized, including device contexts and other internal objects.
2. Before other nodes send the first read/write request to the current node, they will exchange GID, LID, QP Num, etc., through out-of-band communication mechanisms and complete the establishment of RDMA reliable connection paths.

Errors in this part usually indicate that the error occurred within the `mooncake-transfer-engine/src/transport/rdma_transport/rdma_*.cpp` files.

### Recommended Troubleshooting Directions
1. If the error `No matched device found` is displayed, check if there are any network card names in the `nic_priority_matrix` parameter that do not exist on the machine. You can use the `ibv_devinfo` command to view the list of installed network cards on the machine.
2. If the error `Device XXX port not active` is displayed, it indicates that the default RDMA Port of the corresponding device (RDMA device port, to be distinguished from the `rpc_port` TCP port) is not in the ACTIVE state. This is usually due to RDMA cables not being installed properly or the driver not being configured correctly. You can use the `MC_IB_PORT` environment variable to change the default RDMA Port used.
3. If both the error `Worker: Cannot make connection for endpoint` and `Failed to exchange handshake description` are displayed, it indicates that the two parties cannot establish a reliable RDMA connection path. In most cases, it is usually due to incorrect configuration on one side or the inability of both parties to reach each other. First, use tools like `ib_send_bw` to confirm the reachability of the two nodes and pay attention to the output of GID, LID, MTU, and other parameter information. Then, analyze the possible error points based on the error message:
    1. After starting, the log output usually includes several lines of log information like `RDMA device: XXX, LID: XXX, GID: (X) XX:XX:XX:...`. If the displayed GID address is all 0 (the bracket indicates GID Index), you need to choose the correct GID Index according to the network environment and specify it at startup using the `MC_GID_INDEX` environment variable.
    2. If the error `Failed to modify QP to RTR, check mtu, gid, peer lid, peer qp num` is displayed, first determine which party the error occurred on. If there is no prefix `Handshake request rejected by peer endpoint: `, it indicates that the problem comes from the party displaying the error. According to the error message, you need to check the MTU length configuration (adjust using the `MC_MTU` environment variable), whether your own and the other party's GID addresses are valid, etc. At the same time, if the two nodes cannot achieve physical connection, it may also be hang/interrupted at this step, please pay attention.

4. If you encounter an error like `Failed to register memory 0x7efb94000000: Input/output error [5]`, this may indicate that RDMA memory registration has failed. This is usually caused by device limitations where some machines can only register a maximum of 64GB of memory. When the registration exceeds this limit, it will fail.

   **Diagnostic Commands:**
   - Use `ulimit -a` to check system limits, particularly the `max locked memory` value
   - Use `ibv_devinfo -v` to check RDMA device capabilities, especially the `max_mr_size` field
   - Use `dmesg -T` to view detailed failure logs, which may show messages like:
     ```
     [Wed Jul 30 11:49:18 2025] infiniband erdma_0: ERROR: Out of mr size: 0, max 68719476736
     ```

   **Solution:**
   Ensure that the total memory registration does not exceed the device's upper limit. You may need to reduce the amount of memory being registered or split large memory regions into smaller chunks that fit within the device's `max_mr_size` limit.

5. If you encounter errors indicating inability to allocate memory space when requesting large memory regions, this may be due to ulimit restrictions. When the total memory requirement (number of registered RDMA devices × requested space) exceeds the ulimit, the system will display errors about failing to allocate space.

   **Diagnostic Commands:**
   - Use `ulimit -a` to check current limits, particularly the `max locked memory` value
   - Calculate total memory requirement: number of RDMA devices × requested space per device
   - Verify if the total requirement exceeds the ulimit

   **Solutions:**
   - Switch to a higher privilege level (root) to bypass ulimit restrictions
   - Modify ulimit settings: use `ulimit -l unlimited` to remove locked memory limits (may require root privileges)
   - Start multiple store instances with smaller memory allocations that stay within ulimit constraints
   - Add permanent ulimit configuration in `/etc/security/limits.conf`:
     ```
     *    soft    memlock    unlimited
     *    hard    memlock    unlimited
     ```

6. If the error `Failed to create QP: Cannot allocate memory` is displayed, it is typically caused by too many QP have been created, reaching the driver limit. You can use `rdma resource` to trace how many QP is created. One possible way to resolve this issue:
   - Update Mooncake to version v0.3.5 or later
   - Set the environment variable `MC_ENABLE_DEST_DEVICE_AFFINITY=1` before starting the application

## RDMA Transfer Period
### Recommended Troubleshooting Directions

If the network state is unstable, some requests may not be delivered, displaying errors like `Worker: Process failed for slice`. Transfer Engine can avoid problems by reselecting paths, etc. In some complex cases, if a large number of such errors are output continuously, it is recommended to search for the cause of the problem according to the string prompt of the last field.

Note: In most cases, the errors output, except for the first occurrence, are `work request flushed error`. This is because when the first error occurs, the RDMA driver sets the connection to an unavailable state, so tasks in the submission queue are blocked from execution and subsequent errors are reported. Therefore, it is recommended to locate the first occurrence of the error and check it.

In addition, if the error `Failed to get description of XXX` is displayed, it indicates that the Segment name input by the user when calling the `openSegment` interface cannot be found in the etcd database. For memory read/write scenarios, the Segment name needs to strictly match the `local_hostname` field filled in by the other node during initialization.

## SGLang Common Questions

### Do I need RDMA to run SGLang and Mooncake?

When using Mooncake for KV cache transfer in SGLang PD disaggregation deployments, GPUDirect RDMA (GDR) is required.

When using Mooncake as a KV cache storage backend in SGLang HiCache, RDMA is recommended for better performance.
However, if RDMA NICs are not available, the TCP protocol is also supported.

### How to make sure GPUDirect RDMA (GDR) is supported

1. Verify the presence of an RDMA-capable NIC (e.g., Mellanox, ERDMA) and drivers.
```
ibv_devices
lspci | grep rdma
lsmod | grep -E 'ib_core|mlx4_core|mlx5_core|nvidia_peer_mem'
```
If no RDMA devices appear: (1) Confirm physical NIC presence via lspci
(2) Install vendor-specific drivers (e.g., Mellanox MLNX_OFED)

2. check GDR driver is ready, and peer_memory module (part of MLNX_OFED) should be installed
```
# Check peer_memory module (from MLNX_OFED)
lsmod | grep peer_mem

# Verify NVIDIA peer memory module
lsmod | grep nvidia_peer_mem
```

3. If you use container to run SGLang, please make sure RDMA and GDR driver are installed in the container and run container in privileged mode. Requirements: (1) privileged mode must be enabled. (2) RDMA devices/NVIDIA devices mounted into container

4. Check the connectivity
Benchmark end-to-end performance using ib_write_bw.
```
apt install perftest
# server side
ib_write_bw -d [rdma_device] -R -x gdr
# client side
ib_write_bw -d [rdma_device] -R -x gdr [server_ip]
```
Expected Output:
Successful bidirectional transfer with "BW peak" reported
Errors with -x gdr indicate GDR setup failures

### Low GPUDirect RDMA Bandwidth

If you observe significantly lower bandwidth (~15 GB/s) when transferring GPU memory compared to CPU memory (~47 GB/s), this is likely due to PCIe relaxed ordering being disabled.

**Symptoms:**
- GPU RDMA transfers show ~15 GB/s bandwidth
- CPU RDMA transfers show ~47 GB/s (near line rate)
- Same hardware configuration

**Solution:**
Enable PCIe relaxed ordering by setting the environment variable:
```bash
export MC_IB_PCI_RELAXED_ORDERING=1  # or 2 for auto
```

This flag enables `IBV_ACCESS_RELAXED_ORDERING` in RDMA memory registrations, which is critical for optimal GPUDirect RDMA performance. The flag is disabled by default (value 0) for backward compatibility, but should be enabled when using GPUDirect RDMA.

**Verification:**
After enabling relaxed ordering, re-run your benchmark or application. You should see bandwidth improvements from ~15 GB/s to near line rate (~47 GB/s for 400Gbps NICs).

**Note:**
- This optimization requires `ibv_reg_mr_iova2` support in your RDMA driver (available in modern MLNX_OFED versions)
- The setting applies to both the classic Transfer Engine and TENT backends
- When the flag is set to 1 or 2, the system will automatically detect if relaxed ordering is supported and enable it accordingly
