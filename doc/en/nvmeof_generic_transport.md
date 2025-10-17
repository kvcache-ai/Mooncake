# Generic NVMeoF Transport

## Overview

NVMeoFGenericTransport is a more complete NVMeoF protocol-based TransferEngine Transport, designed to eventually replace the existing NVMeoFTransport and provide TransferEngine with the ability to manage and access file Segments.

Compared to the legacy NVMeoFTransport, NVMeoFGenericTransport offers the following advantages:

- **More Complete:** Provides a full set of management interfaces consistent with memory Segments, including registering/unregistering local files, mounting/unmounting remote files, etc.
- **More Generic:** No longer depends on cuFile, and can be deployed and used in environments without CUDA support.
- **Higher Performance:** Supports multi-threaded I/O and Direct I/O, fully leveraging the performance potential of NICs and SSDs.
- **More Reliable:** Ensures that unavailability of a single file or storage device does not affect the availability of others, through a more flexible multi-file management scheme.

## Component Support

Both TransferEngine and Mooncake Store have added full support for NVMeoFGenericTransport. The relevant API interfaces are listed below:

### TransferEngine Support

`TransferEngine` now supports registering and reading/writing file segments. This mainly includes adding fields related to file management and access in `SegmentDesc` and `TransferRequest`, and introducing interfaces for registering and unregistering files.

#### SegmentDesc

To support file registration management, the `file_buffers` field has been added to `SegmentDesc`.

```cpp
using FileBufferID = uint32_t;
struct FileBufferDesc {
    FileBufferID id; // File ID, used to identify the file within a Segment
    std::string path; // File path on the owning node
    std::size_t size; // Available space size of the file
    std::size_t align;  // For future usage.
};

struct SegmentDesc {
    std::string name;
    std::string protocol;
    // Generic file buffers.
    std::vector<FileBufferDesc> file_buffers;

    // Other fields...
};
```

#### TransferRequest

To support multi-file registration and access, the `file_id` field has been added to `TransferRequest` to identify the file to be read from or written to.

```cpp
struct TransferRequest {
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void *source;
    SegmentID target_id;
    uint64_t target_offset; // When accessing a file, target_offset indicates the offset within the target file
    size_t length;
    int advise_retry_cnt = 0;
    FileBufferID file_id; // Target file ID, required only when accessing files, used with target_id to locate the target file
};
```

`file_id` is the ID assigned by the target `TransferEngine` when registering the target file, and can be obtained from the `SegmentDesc` of the target `Segment`.

#### installTransport

```cpp
Transport *installTransport(const std::string &proto, void **args)
```

The `TransferEngine::installTransport` interface now supports directly passing the `args` parameter to the `install` interface of the corresponding Transport, enabling Transport-specific initialization parameters.

For `NVMeoFGenericTransport`, if the current TransferEngine instance does not need to share local files, the `args` parameter can be `nullptr`. Otherwise, `args` should be a valid pointer array, where the first pointer points to a `char *` that references a string containing NVMeoF Target configuration parameters. For example:

```cpp
// NVMeoF Target configuration parameters
char *trid_str = "trtype=<tcp|rdma> adrfam=<ipv4|ipv6> traddr=<Listen address> trsvcid=<Listen port>";

// Arguments for installTransport
void **args = (void **)&trid_str;
```

#### registerLocalFile

```cpp
int registerLocalFile(const std::string &path, size_t size, FileBufferID &id);
```

Registers a local file into TransferEngine, enabling cross-node access. The file can be a regular file or a block device file. **Note: Using a block device file for registration may cause data corruption or complete loss on the device—use with caution!**

- `path`: File path, can be any regular file or block device file such as `/dev/nvmeXnY`;
- `size`: Available space size of the file, can be less than or equal to the physical size;
- `id`: ID assigned by `TransferEngine` to the file, used to distinguish each file when multiple files are registered;
- Return value: Returns 0 on success, otherwise returns a negative error code;

#### unregisterLocalFile

```cpp
int unregisterLocalFile(const std::string &path);
```

Unregisters a local file.

- `path`: File path, must match the path used during registration;

### Mooncake Store Support

Mooncake Store now supports using files as shared storage space for storing objects. This capability is based on two newly added interfaces:

#### MountFileSegment

```cpp
tl::expected<void, ErrorCode> MountFileSegment(const std::string& path);
```

Mounts the local file at `path` as part of the shared storage space.

#### UnmountFileSegment

```cpp
tl::expected<void, ErrorCode> UnmountFileSegment(const std::string& path);
```

Unmounts a previously mounted file.

### Mooncake Store Python API

The Mooncake Store Python API now supports specifying a set of local files as shared storage space.

#### setup_with_files

```python
def setup_with_files(
        local_hostname: str,
        metadata_server: str,
        files: List[str],
        local_buffer_size: int,
        protocol: str,
        protocol_arg: str,
        master_server_addr: str
    ):
    pass
```

Starts a Mooncake Store Client instance and registers the specified files as shared storage space.

## Running Tests

Users can test NVMeoFGenericTransport at both the TransferEngine and Mooncake Store levels.

### Environment Requirements

In addition to the original compilation and runtime environment of the Mooncake project, NVMeoFGenericTransport has additional requirements:

#### Kernel Version and Drivers

NVMeoFGenericTransport currently relies on the Linux kernel's nvme and nvmet driver suite, including the following kernel modules:

- NVMeoF RDMA: Requires Linux Kernel 4.8 or higher, install drivers:

```bash
# Initiator driver, required for accessing remote files
modprobe nvme_rdma

# Target driver, required for sharing local files
modprobe nvmet_rdma
```

- NVMeoF TCP: Requires Linux Kernel 5.0 or higher, install drivers:

```bash
# Initiator driver, required for accessing remote files
modprobe nvme_tcp

# Target driver, required for sharing local files
modprobe nvmet_tcp
```

#### Dependencies

NVMeoFGenericTransport depends on the following third-party libraries:

```bash
apt install -y libaio-dev libnvme-dev
```

### Build Options

To enable NVMeoFGenericTransport, the `USE_NVMEOF_GENERIC` build option must be turned on:

```bash
cmake .. -DUSE_NVMEOF_GENERIC=ON
```

### Runtime Options

NVMeoFGenericTransport supports configuring the following runtime options via environment variables:

- `MC_NVMEOF_GENERIC_DIRECT_IO`: Use Direct I/O when reading/writing NVMeoF SSDs. Disabled by default. Enabling this option can significantly improve performance, but requires that buffer addresses, SSD locations, and I/O lengths all meet alignment requirements (typically 512-byte alignment, 4 KiB alignment recommended).
- `MC_NVMEOF_GENERIC_NUM_WORKERS`: Number of threads used for reading/writing NVMeoF SSDs. Default is 8.

### TransferEngine Testing

After enabling the `USE_NVMEOF_GENERIC` option and completing the build, an executable named `transfer_engine_nvmeof_generic_bench` can be found under `build/mooncake-transfer-engine/example`. This program can be used to test the performance of NVMeoFGenericTransport.

#### Start Metadata Service

Same as the `transfer_engine_bench` test tool. Refer to [transfer-engine.md](../zh/transfer-engine.md#范例程序transfer-engine-bench) for details.

Assume the metadata service address is `http://127.0.0.1:8080/metadata` (using HTTP metadata service as an example).

#### Start Target

**Note: After file registration, existing data may be corrupted or completely lost—use with extreme caution!!**

```bash
./build/mooncake-transfer-engine/example/transfer_engine_nvmeof_generic_bench \
    --local_server_name=127.0.0.1:8081 \
    --metadata_server=http://127.0.0.1:8080/metadata \
    --mode=target \
    --trtype=tcp \
    --traddr=127.0.0.1 \
    --trsvcid=4420 \
    --files="/path/to/file0 /path/to/file1 ..."
```

#### Start Initiator

```bash
./build/mooncake-transfer-engine/example/transfer_engine_nvmeof_generic_bench \
    --local_server_name=127.0.0.1:8082 \
    --metadata_server=http://127.0.0.1:8080/metadata \
    --mode=initiator \
    --operation=read \
    --segment_id=127.0.0.1:8081 \
    --batch_size=4096 \
    --block_size=65536 \
    --duration=30 \
    --threads=1 \
    --report_unit=GB
```

#### Loopback Mode

For quick validation, loopback mode can also be used to test on a single machine:

```bash
./build/mooncake-transfer-engine/example/transfer_engine_nvmeof_generic_bench \
    --local_server_name=127.0.0.1:8081 \
    --metadata_server=http://127.0.0.1:8080/metadata \
    --mode=loopback \
    --operation=read \
    --segment_id=127.0.0.1:8081 \
    --batch_size=4096 \
    --block_size=65536 \
    --duration=30 \
    --threads=1 \
    --report_unit=GB \
    --trtype=tcp \
    --traddr=127.0.0.1 \
    --trsvcid=4420 \
    --files="/path/to/file0 /path/to/file1 ..."
```

#### Performance Tuning

- For workloads involving many files, increasing `MC_NVMEOF_GENERIC_NUM_WORKERS` appropriately usually improves performance.
- If `--block_size` meets `4 KiB` alignment, set environment variable `MC_NVMEOF_GENERIC_DIRECT_IO=on` to significantly boost performance on SSD devices.

### Mooncake Store Testing

Use `mooncake-store/tests/stress_cluster_benchmark.py` to test the performance of Mooncake Store based on NVMeoFGenericTransport.

#### Start Metadata Service

Follow the instructions in [transfer-engine.md](./transfer-engine.md#example-transfer-engine-bench) and [mooncake-store-preview.md](./mooncake-store-preview.md#starting-the-master-service) to start the metadata service and Master service respectively.

#### Start Prefill Instance

```bash
python3 mooncake-store/tests/stress_cluster_benchmark.py \
        --local-hostname=127.0.0.1:8081 \
        --role=prefill \
        --protocol=nvmeof_generic \
        --protocol-args="trtype=tcp adrfam=ipv4 traddr=127.0.0.1 trsvcid=4420" \
        --local-buffer-size=1024 \
        --files="/path/to/file0 /path/to/file1 ..."
```

#### Start Decode Instance

```bash
python3 mooncake-store/tests/stress_cluster_benchmark.py \
        --local-hostname=127.0.0.1:8082 \
        --role=decode \
        --protocol=nvmeof_generic \
        --protocol-args="" \
        --local-buffer-size=1024 \
        --files=""
```

#### Performance Tuning

- For workloads involving many files, increasing `MC_NVMEOF_GENERIC_NUM_WORKERS` appropriately usually improves performance.
- Mooncake Store currently cannot guarantee allocation of buffers that meet Direct I/O alignment requirements; therefore, Direct I/O is not currently supported.