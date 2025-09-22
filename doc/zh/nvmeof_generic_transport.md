# Generic NVMeoF Transport

## 概述

NVMeoFGenericTransport是一个更完善基于NVMeoF协议的TransferEngine Transport，旨在最终替代已有的NVMeoFTransport，为TransferEngine提供管理和访问文件Segment的能力。

相较于旧的NVMeoFTransport，NVMeoFGenericTransport具备以下优势：

- **更完善：** 提供了与内存Segment一致的全套管理接口，包括注册/取消注册本地文件，挂载/取消挂载远端文件等；
- **更通用：** 不再依赖于cuFile，可在没有cuda支持的环境部署和使用；
- **更高性能：** 支持了多线程I/O和Direct I/O，可充分挖掘网卡与SSD的性能潜力；
- **更可靠：** 通过更加灵活的多文件管理方案，可以保证单个文件或存储设备不可用不影响其他文件或存储设备的可用性；

## 组件支持

TransferEngine和Mooncake Store中均已增加了对NVMeoFGenericTransport完整支持，相关的API接口如下：

### TransferEngine支持

`TransferEngine`现在支持了注册和读写文件segment，主要包括在`SegmentDesc`和`TransferRequest`中加入了与文件管理和访问相关的字段，并增加了注册和取消注册文件的接口。

#### SegmentDesc

为了支持文件注册管理，`SegmentDesc`中增加了`file_buffers`字段。

```cpp
using FileBufferID = uint32_t;
struct FileBufferDesc {
    FileBufferID id; // 文件ID，用于在Segment中标识文件
    std::string path; // 文件在所属节点上的路径
    std::size_t size; // 文件的可用空间大小
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

为了支持多文件注册与访问，`TransferRequest`中增加了`file_id`字段，用于标识需要读写的文件。

```cpp
struct TransferRequest {
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void *source;
    SegmentID target_id;
    uint64_t target_offset; // 访问文件时，target_offset表示在目标文件中的偏移量
    size_t length;
    int advise_retry_cnt = 0;
    FileBufferID file_id; // 目标文件ID，只在访问文件时需要，与target_id一起定位目标文件
};
```

`file_id`是目标`TransferEngine`在注册目标文件时分配的ID，可从目标`Segment`的`SegmentDesc`中获得。

#### installTransport

```cpp
Transport *installTransport(const std::string &proto, void **args)
```

`TransferEngine::installTransport`接口现在支持将`args`参数直接传递给相应Transport的`install`接口，以支持Transport特有的初始化参数。

对于`NVMeoFGenericTransport`来说，如果当前TransferEngine实例不需要共享本地文件，则`args`参数可以为`nullptr`。否则，`args`参数应为一个有效的指针数组，其中的第一个指针为一个`char *`类型的指针，指向一个包含NVMeoF Target配置参数的的字符串。如下所示：

```cpp
// NVMeoF Target配置参数
char *trid_str = "trtype=<tcp|rdma> adrfam=<ipv4|ipv6> traddr=<Listen address> trsvcid=<Listen port>";

// 用于installTransport的参数
void **args = (void **)&trid_str;
```

#### registerLocalFile

```cpp
int registerLocalFile(const std::string &path, size_t size, FileBufferID &id);
```

将一个本地文件注册到TransferEngine，使其能够被跨节点访问。文件可以是普通文件或块设备文件。**注意：注册使用块设备文件会导致设备上原有的数据损坏或完全丢失，请谨慎使用！**

- `path`： 文件路径，可以是任意普通文件，或块设备文件，如`/dev/nvmeXnY`；
- `size`： 文件的可用空间大小，可以小于等于文件的物理空间大小；
- `id`：   `TransferEngine`为文件分配的ID，用于在注册了多个文件的情况下区分每个文件；
- 返回值：注册成功时返回0，否则返回负的错误码；

#### unregisterLocalFile

```cpp
int unregisterLocalFile(const std::string &path);
```

取消注册一个本地文件。

- `path`： 文件路径，需要与注册时使用的路径一致；

### Mooncake Store支持

Mooncake Store现在支持了使用文件作为共享存储空间存储对象。这一能力基于两个新增的接口：

#### MountFileSegment

```cpp
tl::expected<void, ErrorCode> MountFileSegment(const std::string& path);
```

挂载路径为`path`的本地文件作为共享存储空间的一部分。

#### UnmountFileSegment

```cpp
tl::expected<void, ErrorCode> UnmountFileSegment(const std::string& path);
```

取消挂载先前挂载了的文件。

### Mooncake Store Python API

Mooncake Store Python API现在支持指定一组本地文件作为共享存储空间。

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

启动Mooncake Store Client实例，并将指定的文件注册为共享存储空间。

## 运行测试

用户可以从TransferEngine和Mooncake Store两个层面对NVMeoFGenericTransport进行测试。

### 环境要求

在Mooncake项目原有编译运行环境的基础上，NVMeoFGenericTransport还有一些额外的要求：

#### 内核版本与驱动

NVMeoFGenericTransport当前依赖Linux内核的nvme和nvmet驱动组，包含以下内核模块：

- NVMeoF RDMA: 依赖 Linux Kernel 4.8 及以上版本，安装驱动：

```bash
# Initiator 驱动，访问远端文件需要
modprobe nvme_rdma

# Target 驱动，共享本地文件需要
modprobe nvmet_rdma
```

- NVMeoF TCP: 依赖 Linux Kernel 5.0 及以上版本，安装驱动：

```bash
# Initiator 驱动，访问远端文件需要
modprobe nvme_tcp

# Target 驱动，共享本地文件需要
modprobe nvmet_tcp
```

#### 依赖库

NVMeoFGenericTransport依赖于以下第三方库：

```bash
apt install -y libaio-dev libnvme-dev
```

### 编译选项

要启用NVMeoFGenericTransport，需要开启`USE_NVMEOF_GENERIC`编译选项：

```bash
cmake .. -DUSE_NVMEOF_GENERIC=ON
```

### 运行时选项

NVMeoFGenericTransport支持通过环境变量配置以下运行时选项：

- `MC_NVMEOF_GENERIC_DIRECT_IO` 在读写NVMeoF SSD时使用Direct I/O，默认关闭。开启这一选项可以大幅提升性能，但要求读写操作使用的buffer地址、读写的SSD位置以及读写长度全部满足对齐要求（通常是512字节对齐，建议4 KiB对齐）
- `MC_NVMEOF_GENERIC_NUM_WORKERS` 读写NVMeoF SSD时使用的线程数量，默认为8

### TransferEngine测试

开启`USE_NVMEOF_GENERIC`选项并完成编译后，在`build/mooncake-transfer-engine/example`下可以找到名为`transfer_engine_nvmeof_generic_bench`的可执行文件。此程序可用于测试NVMeoFGenericTransport的性能。

#### 启动元数据服务

与`transfer_engine_bench`测试工具相同，具体可参考 [transfer-engine.md](../zh/transfer-engine.md#范例程序transfer-engine-bench)

后续以HTTP元数据服务为例，假设元数据服务地址为`http://127.0.0.1:8080/metadata`

#### 启动target

**注意：文件注册使用后，其中原有的数据将损坏甚至全部丢失，请谨慎使用！！！**

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

#### 启动initiator

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

#### Loopback模式

为了快速验证，也可以使用loopback模式在单机上进行测试：

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

#### 性能调优

- 对于大量文件，适当调大`MC_NVMEOF_GENERIC_NUM_WORKERS`通常可以提升性能；
- 在`--block_size`满足`4 KiB`对齐的前提下，可以设置环境变量`MC_NVMEOF_GENERIC_DIRECT_IO=on`，对于SSD设备可以大幅提升性能；

### Mooncake Store测试

使用`mooncake-store/tests/stress_cluster_benchmark.py`可以测试基于NVMeoFGenericTransport的Mooncake Store的性能。

#### 启动元数据服务

按照 [transfer-engine.md](./transfer-engine.md#范例程序transfer-engine-bench) 和 [mooncake-store-preview.md](./mooncake-store-preview.md#启动-master-service) 的说明分别启动元数据服务和Master服务。

#### 启动prefill实例

```bash
MC_MS_AUTO_DISC=0 python3 ../mooncake-store/tests/stress_cluster_benchmark.py \
                          --local-hostname=127.0.0.1:8081 \
                          --role=prefill \
                          --protocol=nvmeof_generic \
                          --protocol-args="trtype=tcp adrfam=ipv4 traddr=127.0.0.1 trsvcid=4420" \
                          --local-buffer-size=1024 \
                          --files="/path/to/file0 /path/to/file1 ..."
```

#### 启动decode实例

```bash
MC_MS_AUTO_DISC=0 python3 ../mooncake-store/tests/stress_cluster_benchmark.py \
                          --local-hostname=127.0.0.1:8082 \
                          --role=decode \
                          --protocol=nvmeof_generic \
                          --protocol-args="" \
                          --local-buffer-size=1024 \
                          --files=""
```

#### 性能调优

- 对于大量文件，适当调大`MC_NVMEOF_GENERIC_NUM_WORKERS`通常可以提升性能；
- Mooncake Store目前无法保证分配满足Direct I/O对齐要求的Buffer，因此暂无法启用Direct I/O；
