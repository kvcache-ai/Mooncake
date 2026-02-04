# Transfer Engine

## 概述
Mooncake Transfer Engine 是一个围绕 Segment 和 BatchTransfer 两个核心抽象设计的高性能，零拷贝数据传输库。

- [**Segment**](#segment) 代表一段可被远程读写的连续地址空间，既可以是 DRAM 或 VRAM 提供的非持久化存储 **RAM Segment**，也可以是 NVMeof 提供的持久化存储 **NVMeof Segment**。

- [**BatchTransfer**](#batchtransfer) 封装了操作请求，具体负责将一个 Segment 中非连续的一组数据空间的数据和另外一组 Segment 的对应空间进行数据同步，支持 Read/Write 两种方向，因此类似一个异步且更灵活的的 AllScatter/AllGather。

![transfer_engine](../../image/transfer-engine.png)

如上图所示，每个特定的客户端对应一个 TransferEngine，其中不仅包含一个 RAM Segment，还集成了对于多线程多网卡高速传输的管理。RAM Segment 原则上就对应这个 TransferEngine 的全部虚拟地址空间，但实际上仅仅会注册其中的部分区域（被称为一个 Buffer）供外部 (GPUDirect) RDMA Read/Write。每一段 Buffer 可以分别设置权限（对应 RDMA rkey 等）和网卡亲和性（比如基于拓扑优先从哪张卡读写等）。

Mooncake Transfer Engine 通过 `TransferEngine` 类对外提供接口（位于 `mooncake-transfer-engine/include/transfer_engine.h`），其中对应不同后端的具体的数据传输功能在内部由 `Transport` 类实现，包括`TcpTransport`、`RdmaTransport` 和 `NVMeoFTransport`。

### Segment
Segment 表示 Transfer Engine 实现数据传输过程期间可使用的源地址范围及目标地址范围集合。也就是说，所有 BatchTransfer 请求中涉及的本地与远程地址都需要位于合法的 Segment 区间里。Transfer Engine 支持以下两种类型的 Segment。

#### 1. 位于内存地址空间（DRAM、VRAM）的 RAM Segment
每一个进程启动时， Transfer Engine 会自动创建一个以自身 `local_hostname` 为名称（见 TransferEngine 的初始化函数，需要全局唯一）的 Segment，该 Segment 在逻辑上覆盖了完整的内存地址空间，包括 DRAM/VRAM 等存储介质，Transfer Engine 在使用 BatchTransfer 接口进行传输任务时，会自动判断相应硬件信息，从而选择最佳传输方式。每个进程有且只有一个 Segment。其他进程通过调用 `openSegment` 接口并传递正确名称的方式，可引用 Segment 并完成读写操作。

在实际部署中，应用系统通常只使用部分内存地址空间完成数据传输，因此在 Transfer Engine 内部将 Segment 进一步划分成多个 Buffers。每个 Buffer 代表一段连续的、位于同一设备上的地址空间，用户使用 BatchTransfer 接口完成读写操作时，若引用 RAM Segment，则每次读写任务的范围必须在其中的某一个合法 Buffer 内。
一个 Segment 内的内存范围不需要是连续的，也就是说，可以通过分配多段DRAM/VRAM 地址空间并纳入相同的 Segment。

除此在外，Transfer Engine 也支持注册一些本地 DRAM 区域，这一部分区域仅仅是作为数据操作的本地侧存储空间，比如 vLLM 的 DRAM PageCache 区域。它也被视为当前进程中有效 RAM Segment 的一部分，但不能被其他进程通过调用 `openSegment` 接口引用。

#### 2. 位于挂载到 NVMeof 上文件的 **NVMeof Segment**
Transfer Engine 也借助 NVMeof 协议，支持从 NVMe 上直接将文件指定偏移的数据，通过 PCIe 直传方式直达 DRAM/VRAM，无需经过 CPU 且实现零拷贝。用户需要按照指引的说明将远程存储节点挂载到本地，并使用 `openSegment` 接口进行引用，从而完成数据读写操作。

### BatchTransfer

借助 Transfer Engine，Mooncake Store 可实现本地 DRAM/VRAM 通过(GPUDirect) RDMA、NVMe-of 协议等读写本地/远端的有效 Segment（即注册的 DRAM/VRAM 区间及 NVMe 文件）中的指定部分。

| 远程 ↓ 本地→ | DRAM | VRAM |
| ------------ | ---- | ---- |
| DRAM         | ✓    | ✓    |
| VRAM         | ✓    | ✓    |
| NVMe-of      | ✓    | ✓    |

- Local memcpy: 如果目标 Segment 其实就在本地 DRAM/VRAM 的话，直接使用 memcpy、cudaMemcpy 等数据复制接口。
- RDMA: 支持本地 DRAM/VRAM 与远程 DRAM 之间的数据传递。在实现上支持多网卡池化及重试等功能。
- cuFile (GPUDirect Storage): 实现本地 DRAM/VRAM 与 Local/Remote NVMeof 之间的数据传递。

BatchTransfer API 使用请求（Request）对象数组传入用户请求，需指定操作类型（READ 或 WRITE）、数据长度以及本地和远程内存的地址。传输操作适用于 DRAM 和 GPU VRAM，并在最佳情况下利用 GPU 直接 RDMA，前提是指定的内存区域已预先注册。这些操作的完成情况可通过 `getTransferStatus` API 来异步监控这些操作的完成情况。

### 拓扑感知路径选择（Topology Aware Path Selection）
现代推理服务器通常由多个CPU插槽、DRAM、GPU和RDMA NIC设备组成。尽管从技术上讲，使用任何RDMA NIC将数据从本地DRAM或VRAM传输到远程位置是可能的，但这些传输可能会受到Ultra Path Interconnect (UPI)或PCIe交换机带宽限制的制约。为了克服这些限制，Transfer Engine 实现了拓扑感知路径选择算法。在处理请求之前，每个服务器生成一个拓扑矩阵（Topology Matrix）并将其广播到整个集群。拓扑矩阵将网络接口卡（NIC）分类为各种类型的内存的“首选”和“次要”列表，这些类型在内存注册时指定。在正常情况下，选择首选列表中的NIC进行传输，便于在本地NUMA或仅通过本地PCIe交换机进行GPU Direct RDMA操作。在出现故障的情况下，两个列表中的所有NIC都可能被使用。上述过程包括根据内存地址识别适当的本地和目标NIC，建立连接，并执行数据传输。

![topology-matrix](../../image/topology-matrix.png)

例如，如图所示，要将数据从本地节点分配给 `cpu:0` 的缓冲区0传输到目标节点分配给`cpu:1`的缓冲区1，引擎首先使用本地服务器的拓扑矩阵识别`cpu:0`的首选NIC，并选择一个，如`mlx5_1`，作为本地NIC。同样，根据目标内存地址选择目标NIC，如`mlx5_3`。这种设置允许建立从`mlx5_1@本地`到`mlx5_3@目标`的RDMA连接，以执行RDMA读写操作。

为了进一步最大化带宽利用率，如果单个请求的传输长度超过64KB，则其内部被划分为多个切片。每个切片可能使用不同的路径，使所有RDMA NIC能够协同工作。

### 端点管理
Transfer Engine 使用一对端点来表示本地RDMA NIC和远程RDMA NIC之间的连接。实际上，每个端点包括一个或多个RDMA QP对象。
Transfer Engine 中的连接是按需建立的；端点在第一次请求之前保持未配对状态。
为了防止大量端点减慢请求处理速度，Transfer Engine 采用端点池，限制最大活动连接数。
Transfer Engine 使用SIEVE算法来管理端点的逐出。如果由于链路错误导致连接失败，它将从两端的端点池中移除，并在下一次数据传输尝试期间重新建立。

### 故障处理
在多NIC环境中，一个常见的故障场景是特定NIC的暂时不可用，而其他路由仍然可以连接两个节点。Transfer Engine 旨在有效地管理这种暂时性故障。如果识别到连接不可用，Transfer Engine 会自动识别一个替代的、可达的路径，并将请求重新提交给不同的RDMA NIC设备。此外，Transfer Engine 能够检测到其他RDMA资源的问题，包括RDMA上下文和完成队列。它会暂时避免使用这些资源，直到问题得到解决。

## 范例程序：Transfer Engine Bench
`mooncake-transfer-engine/example/transfer_engine_bench.cpp` 提供了一个范例程序，通过调用 Transfer Engine API 接口，发起节点从目标节点的 DRAM 处反复读取/写入数据块，以展示 Transfer Engine 的基本用法，并可用于测量读写吞吐率。目前 Transfer Engine Bench 工具支持 RDMA 及 TCP 协议。

编译 Transfer Engine 成功后，可在 `build/mooncake-transfer-engine/example` 目录下产生测试程序 `transfer_engine_bench`。

1. **启动 `metadata` 服务。** 该服务用于 Mooncake 各类元数据的集中高可用管理，包括 Transfer Engine 的内部连接状态等。需确保发起节点和目标节点都能顺利通达该 metadata 服务，因此需要注意：
   - metadata 服务的监听 IP 不应为 127.0.0.1，需结合网络环境确定。在实验环境中，可使用 0.0.0.0。
   - 在某些平台下，如果发起节点和目标节点设置了 `http_proxy` 或 `https_proxy` 环境变量，也会影响 Transfer Engine 与 metadata 服务的通信。

   Transfer Engine 支持多种 metadata 服务，包括 `etcd`, `redis` 和 `http`。下面以 `etcd` 和 `http` 为例说明如何启动 metadata 服务。

   1.1. **`etcd`**

   默认状态下不会使用etcd服务，要在transfer engine中使用etcd服务，需要在`mooncake-common/common.cmake`文件中，把`USE_ETCD`变量的值设为`ON`，就可以使用了。

   例如，可使用如下命令行启动 `etcd` 服务：
      ```bash
      etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379
      ```

   1.2. **启动 `http` 作为 `metadata` 服务**

   例如，可使用 `mooncake-transfer-engine/example/http-metadata-server` 示例中的 `http` 服务：
      ```bash
      cd mooncake-transfer-engine/example/http-metadata-server
      go run . --addr=:8080
      ```

2. **启动目标节点。**
    ```bash
    ./transfer_engine_bench --mode=target \
                            --metadata_server=etcd://10.0.0.1:2379 \
                            [--local_server_name=TARGET_NAME] \
                            [--device_name=erdma_0 | --auto-discovery]
    ```
   各个参数的含义如下：
   - `--mode=target` 表示启动目标节点。目标节点不发起读写请求，只是被动按发起节点的要求供给或写入数据。
      > [!NOTE]
      > 实际应用中可不区分目标节点和发起节点，每个节点可以向集群内其他节点自由发起读写请求。
   - `--metadata_server` 为元数据服务器地址，一般形式是 `[proto]://[hostname:port]`。例如，下列元数据服务器地址是合法的：
      - 使用 `etcd` 作为元数据存储服务：`"10.0.0.1:2379"` 或 `"etcd://10.0.0.1:2379"` 或 `"etcd://10.0.0.1:2379,10.0.0.2:2379"`
      - 使用 `redis` 作为元数据存储服务：`"redis://10.0.0.1:6379"`
      - 使用 `http` 作为元数据存储服务：`"http://10.0.0.1:8080/metadata"`
   - `--local_server_name` 表示本节点创建段（segment）名称，供发起节点引用。大多数情况下无需设置。如果不设置该选项，则该值等同于本机的主机名（即 `hostname(2)` ）。
   - `--device_name` 表示传输期间所用的网卡列表（使用逗号分割，不要添加空格）。也可使用`--auto_discovery` 检测安装的所有网卡列表并进行使用。
   - 在仅支持 TCP 的网络环境中，可使用 `--protocol=tcp` 参数，此时不需要指定 `--device_name` 参数。

1. **启动发起节点。**
    ```bash
    export MC_GID_INDEX=n
    ./transfer_engine_bench --metadata_server=etcd://10.0.0.1:2379 \
                            --segment_id=TARGET_NAME \
                            [--local_server_name=INITIATOR_NAME] \
                            [--device_name=erdma_1 | --auto-discovery]
    ```
   各个参数的含义如下（其余同前）：
   - `--segment_id` 可以简单理解为目标节点对应的段名称，需要和启动目标节点时 `--local_server_name` 传入的值（如果有）保持一致。

   正常情况下，发起节点将开始进行传输操作，等待 10s 后回显“Test completed”信息，表明测试完成。

   发起节点还可以配置下列测试参数：`--operation`（可为 `"read"` 或 `"write"`）、`batch_size`、`block_size`、`duration`、`threads` 等。

### 运行示例及结果判读

下面的视频显示了按上述操作正常运行的过程，其中右侧是 Target，左侧是 Initiator。测试结束后，Initiator 会报告测试时长（10 秒）、IOPS（379008 次请求/s）、吞吐率（19.87 GiB/s）等信息。这里的吞吐率超出了所用主机单卡支持的最大吞吐率。

![transfer-engine-running](../../image/transfer-engine-running.gif)

> [!NOTE]
> 如果在执行期间发生异常，大多数情况是参数设置不正确所致，建议参考[故障排除文档](troubleshooting.md)先行排查。

## C/C++ API
Transfer Engine 通过 `TransferEngine` 类统一对外提供接口（位于 `mooncake-transfer-engine/include/transfer_engine.h`），其中对应不同后端的具体的数据传输功能在内部由 `Transport` 类实现，目前支持 `TcpTransport`,`RdmaTransport` 和 `NVMeoFTransport`。

### 数据传输

#### TransferEngine::TransferRequest

Mooncake Transfer Engine 提供的最核心 API 是：通过 `submitTransfer()` 接口提交一组异步的由 `TransferRequest` 结构体表示的任务，并通过 `getTransferStatus()` 接口查询其状态。每个 `TransferRequest` 结构体规定从本地的起始地址 `source` 开始，读取或写入长度为 `length` 的连续数据空间，到 `target_id` 对应的段、从 `target_offset` 开始的位置。

`TransferRequest` 结构体定义如下：

```cpp
using SegmentID = int32_t;
struct TransferRequest
{
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void *source;
    SegmentID target_id; // 目标 segment 的 ID，可能对应本地或远程的 DRAM/VRAM/NVMeof，具体的选路逻辑被隐藏
    size_t target_offset;
    size_t length;
};
```

- `opcode` 取值为 `READ` 或 `WRITE`。`READ` 表示数据从 `<target_id, target_offset>` 表示的目标地址复制到本地的起始地址 `source`；`WRITE` 表示数据从 `source` 复制到 `<target_id, target_offset>` 表示的地址。
- `source` 表示当前 `TransferEngine` 管理的 DRAM/VRAM buffer，需提前已经被 `registerLocalMemory` 接口注册
- `target_id` 表示传输目标的 Segment ID。Segment ID 的获取需要用到 `openSegment` 接口。Segment 分为以下两种类型：
  - RAM 空间型，涵盖 DRAM/VRAM 两种形态。如前所述，同一进程（或者说是 `TransferEngine` 实例）下只有一个 Segment，这个 Segment 内含多种不同种类的 Buffer（DRAM/VRAM）。此时 `openSegment` 接口传入的 Segment 名称等同于服务器主机名。`target_offset` 为目标服务器的虚拟地址。
  - NVMeOF 空间型，每个文件对应一个 Segment。此时 `openSegment` 接口传入的 Segment 名称等同于文件的唯一标识符。`target_offset` 为目标文件的偏移量。
- `length` 表示传输的数据量。TransferEngine 在内部可能会进一步拆分成多个读写请求。

#### TransferEngine::allocateBatchID

```cpp
BatchID allocateBatchID(size_t batch_size);
```

分配 `BatchID`。同一 `BatchID` 下最多可提交 `batch_size` 个 `TransferRequest`。

- `batch_size`: 同一 `BatchID` 下最多可提交的 `TransferRequest` 数量；
- 返回值：若成功，返回 `BatchID`（非负）；否则返回负数值。

#### TransferEngine::submitTransfer

```cpp
int submitTransfer(BatchID batch_id, const std::vector<TransferRequest> &entries);
```

向 `batch_id` 追加提交新的 `TransferRequest` 任务。该任务被异步提交到后台线程池。同一 `batch_id` 下累计的 `entries` 数量不应超过创建时定义的 `batch_size`。

- `batch_id`: 所属的 `BatchID`；
- `entries`: `TransferRequest` 数组；
- 返回值：若成功，返回 0；否则返回负数值。

#### TransferEngine::getTransferStatus

```cpp
enum TaskStatus
{
  WAITING,   // 正在处于传输阶段
  PENDING,   // 暂不支持
  INVALID,   // 参数不合法
  CANCELED,  // 暂不支持
  COMPLETED, // 传输完毕
  TIMEOUT,   // 暂不支持
  FAILED     // 即使经过重试仍传输失败
};
struct TransferStatus {
  TaskStatus s;
  size_t transferred; // 已成功传输了多少数据（不一定是准确值，确保是 lower bound）
};
int getTransferStatus(BatchID batch_id, size_t task_id, TransferStatus &status)
```

获取 `batch_id` 中第 `task_id` 个 `TransferRequest` 的运行状态。

- `batch_id`: 所属的 `BatchID`；
- `task_id`: 要查询的 `TransferRequest` 序号；
- `status`: 输出 Transfer 状态；
- 返回值：若成功，返回 0；否则返回负数值。

#### TransferEngine::freeBatchID

```cpp
int freeBatchID(BatchID batch_id);
```

回收 `BatchID`，之后对此的 `submitTransfer` 及 `getTransferStatus` 操作均是未定义的。若 `BatchID` 内仍有 `TransferRequest` 未完成，则拒绝操作。

- `batch_id`: 所属的 `BatchID`；
- 返回值：若成功，返回 0；否则返回负数值。

### 多 Transport 管理

`TransferEngine` 类内部管理多后端的 `Transport` 类，并且会自动探查 CPU/CUDA 和 RDMA 网卡之间的拓扑关系（更多设备种类的支持正在开发中，如无法给出准确的硬件拓扑，欢迎您的反馈和改进建议)，以及自动安装合适的 `Transport`。

### 空间注册

对于 RDMA 的传输过程，作为源端指针的 `TransferRequest::source` 必须提前注册为 RDMA 可读写的 Memory Region 空间，即纳入当前进程中 RAM Segment 的一部分。因此需要用到如下函数：

#### TransferEngine::registerLocalMemory

```cpp
int registerLocalMemory(void *addr, size_t size, string location, bool remote_accessible);
```

在本地 DRAM/VRAM 上注册起始地址为 `addr`，长度为 `size` 的空间。

- `addr`: 注册空间起始地址；
- `size`：注册空间长度；
- `location`: 这一段内存对应的 `device`，比如 `cuda:0` 表示对应 GPU 设备，`cpu:0` 表示对应 CPU socket，通过和网卡优先级顺序表（见`installTransport`） 匹配，识别优选的网卡。 也可以使用 `*`，Transfer Engine 会尽量自动识别 `addr` 对应的 `device`，如果识别失败，将打印 `WARNING` 级别日志，并且使用全部的网卡，不再区分优选网卡。
- `remote_accessible`: 标识这一块内存能否被远端节点访问。
- 返回值：若成功，返回 0；否则返回负数值。

#### TransferEngine::unregisterLocalMemory

```cpp
int unregisterLocalMemory(void *addr);
```

解注册区域。

- addr: 注册空间起始地址；
- 返回值：若成功，返回 0；否则返回负数值。

### Segment 管理与元数据格式

TransferEngine 提供 `openSegment` 函数，该函数获取一个 `SegmentHandle`，用于后续 `Transport` 的传输。
```cpp
SegmentHandle openSegment(const std::string& segment_name);
```
- `segment_name`：segment 的唯一标志符。对于 RAM Segment，这需要与对端进程初始化 TransferEngine 对象时填写的 `server_name` 保持一致。
- 返回值：若成功，返回对应的 SegmentHandle；否则返回负数值。

```cpp
int closeSegment(SegmentHandle segment_id);
```
- `segment_id`：segment 的唯一标志符。
- 返回值：若成功，返回 0；否则返回负数值。

<details>
<summary><strong>元数据格式</strong></summary>

```
// 用于根据 server_name 查找可通信的地址以及暴露的 rpc 端口。
// 创建：调用 TransferEngine::init() 时。
// 删除：TransferEngine 被析构时。
Key = mooncake/rpc_meta/[server_name]
Value = {
    'ip_or_host_name': 'node01'
    'rpc_port': 12345
}

// 对于 segment，采用 mooncake/[proto]/[segment_name] 的 key 命名方式，segment name 可以采用 Server Name。
// Segment 对应机器，buffer 对应机器内的不同段内存或者不同的文件或者不同的盘。同一个 segment 的不同 buffer 处于同一个故障域。

// RAM Segment，用于 RDMA Transport 获取传输信息。
// 创建：命令行工具 register.py，此时 buffers 为空，仅填入可预先获知的信息。
// 修改：TransferEngine 在运行时通过 register / unregister 添加或删除 Buffer。
Key = mooncake/ram/[segment_name]
Value = {
    'server_name': server_name,
    'protocol': rdma,
    'devices': [
        { 'name': 'mlx5_2', 'lid': 17, 'gid': 'fe:00:...' },
        { 'name': 'mlx5_3', 'lid': 22, 'gid': 'fe:00:...' }
    ],
    'priority_matrix': {
        "cpu:0": [["mlx5_2"], ["mlx5_3"]],
        "cpu:1": [["mlx5_3"], ["mlx5_2"]],
        "cuda:0": [["mlx5_2"], ["mlx5_3"]],
    },
    'buffers': [
        {
            'name': 'cpu:0',
            'addr': 0x7fa16bdf5000,
            'length': 1073741824,
            'rkey': [1fe000, 1fdf00, ...], // 长度等同于 'devices' 字段的元素长度
        },
    ],
}

// 创建：命令行工具 register.py，确定可被挂载的文件路径。
// 修改：命令行工具 mount.py，向被挂载的 buffers.local_path_map 中添加挂载该文件的机器 -> 挂载机器上的文件路径的映射。
Key = mooncake/nvmeof/[segment_name]
Value = {
    'server_name': server_name,
    'protocol': nvmeof,
    'buffers':[
    {
        'length': 1073741824,
        'file_path': "/mnt/nvme0" // 本机器上的文件路径
        'local_path_map': {
            "node01": "/mnt/transfer_engine/node01/nvme0", // 挂载该文件的机器 -> 挂载机器上的文件路径
            ....
        },
     }，
     {
        'length': 1073741824,
        'file_path': "/mnt/nvme1",
        'local_path_map': {
            "node02": "/mnt/transfer_engine/node02/nvme1",
            ....
        },
     }
    ]
}
```
</details>

### HTTP 元数据服务

使用 HTTP 作为 metadata 元数据服务时，HTTP 服务端需要提供三个接口，以 metadata_server 配置为 `http://host:port/metadata` 举例：

1. `GET /metadata?key=$KEY`：获取 `$KEY` 对应的元数据。
2. `PUT /metadata?key=$KEY`：更新 `$KEY` 对应的元数据为请求 body 的值。
3. `DELETE /metadata?key=$KEY`：删除 `$KEY` 对应的元数据。

具体实现，可以参考 [mooncake-transfer-engine/example/http-metadata-server](../../mooncake-transfer-engine/example/http-metadata-server) 用 Golang 实现的 demo 服务。

### 构造函数与初始化
TransferEngine 在完成构造后需要调用 `init` 函数进行初始化：
```cpp
TransferEngine();

int init(const std::string &metadata_conn_string,
         const std::string &local_server_name);
```
- metadata_conn_string: 元数据存储服务连接字符串，表示 `etcd`/`redis` 的 IP 地址/主机名，或者 http 服务的 URI。一般形式是 `[proto]://[hostname:port]`。例如，下列元数据服务器地址是合法的：

    - 使用 `etcd` 作为元数据存储服务：`"10.0.0.1:2379"` 或 `"etcd://10.0.0.1:2379"`
    - 使用 `redis` 作为元数据存储服务：`"redis://10.0.0.1:6379"`
    - 使用 `http` 作为元数据存储服务：`"http://10.0.0.1:8080/metadata"`

- local_server_name: 本地的 server name，保证在集群内唯一。它同时作为其他节点引用当前实例所属 RAM Segment 的名称（即 Segment Name）
- 返回值：若成功，返回 0；若 TransferEngine 已被 init 过，返回 -1。

```cpp
~TransferEngine();
```

回收分配的所有类型资源，同时也会删除掉全局 meta data server 上的信息。

## 二次开发
### 使用 C/C++ 接口二次开发
在完成 Mooncake Store 编译后，可将编译好的静态库文件 `libtransfer_engine.a` 及 C 头文件 `transfer_engine_c.h`，移入到你自己的项目里。不需要引用 `src/transfer_engine` 下的其他文件。

### 使用 Golang 接口二次开发
为了支撑 P2P Store 的运行需求，Transfer Engine 提供了 Golang 接口的封装，详见 `mooncake-p2p-store/src/p2pstore/transfer_engine.go`。

编译项目时启用 `-DWITH_P2P_STORE=ON` 选项，则可以一并编译 P2P Store 样例程序。

### 使用 Rust 接口二次开发
在 `mooncake-transfer-engine/rust` 下给出了 TransferEngine 的 Rust 接口实现，并根据该接口实现了 Rust 版本的样例程序，逻辑类似于 [transfer_engine_bench.cpp](../../mooncake-transfer-engine/example/transfer_engine_bench.cpp)。若想编译 rust example，需安装 Rust SDK，并在 cmake 命令中添加 `-DWITH_RUST_EXAMPLE=ON`。

## 高级运行时选项
对于高级用户，TransferEngine 提供了如下所示的高级运行时选项，均可通过 **环境变量（environment variable）** 方式传入。

- `MC_NUM_CQ_PER_CTX` 每个设备实例创建的 CQ 数量，默认值 1
- `MC_NUM_COMP_CHANNELS_PER_CTX` 每个设备实例创建的 Completion Channel 数量，默认值 1
- `MC_IB_PORT` 每个设备实例使用的 IB 端口号，默认值 1
- `MC_IB_TC` 当使用`RDMA`通信协议时，在交换机和网卡默认配置不一致场景/需要流量规划场景下，可能需要修改 RDMA 网卡的 Traffic Class 配置，默认值 -1
- `MC_IB_PCI_RELAXED_ORDERING` 将网络适配器的PCIe顺序设置为放宽有时会带来更好的性能。可设置 1 以启用RO功能，默认值 0
- `MC_GID_INDEX` 每个设备实例使用的 GID 序号，默认值 3（或平台支持的最大值）
- `MC_MAX_CQE_PER_CTX` 每个设备实例中 CQ 缓冲区大小，默认值 4096
- `MC_MAX_EP_PER_CTX` 每个设备实例中活跃 EndPoint 数量上限，默认值 65536。**注意：** 小于 0.3.7.post1 的版本，这里默认值是 256，但是无法手动设置为 65536，最大值支持 65535！请注意
- `MC_NUM_QP_PER_EP` 每个 EndPoint 中 QP 数量，数量越多则细粒度 I/O 性能越好，默认值 2
- `MC_MAX_SGE` 每个 QP 最大可支持的 SGE 数量，默认值 4（或平台支持的最高值）
- `MC_MAX_WR` 每个 QP 最大可支持的 Work Request 数量，默认值 256（或平台支持的最高值）
- `MC_MAX_INLINE` 每个 QP 最大可支持的 Inline 写数据量（字节），默认值 64（或平台支持的最高值）
- `MC_MTU` 每个设备实例使用的 MTU 长度，可为 512、1024、2048、4096，默认值 4096（或平台支持的最大长度）
- `MC_WORKERS_PER_CTX` 每个设备实例对应的异步工作线程数量
- `MC_SLICE_SIZE` Transfer Engine 中用户请求的切分粒度
- `MC_RETRY_CNT` Transfer Engine 中最大重试次数
- `MC_LOG_LEVEL` 该选项可以设置成`TRACE`/`INFO`/`WARNING`/`ERROR`（详情见 [glog doc](https://github.com/google/glog/blob/master/docs/logging.md)），则在运行时会输出更详细的日志
- `MC_HANDSHAKE_LISTEN_BACKLOG` 监听握手连接的 backlog 大小, 默认值 128
- `MC_HANDSHAKE_MAX_LENGTH` P2P 模式下握手消息的最大长度（字节）。有效范围：1MB 到 128MB。默认值为 1MB (1048576 字节)。当单个 RDMA 实例注册大量内存缓冲区（>10,000）时，需要增大此值以避免握手失败。示例：设置为 10485760 表示 10MB
- `MC_LOG_DIR` 该选项指定存放日志重定向文件的目录路径。如果路径无效，glog将回退到向标准错误[stderr]输出日志。
- `MC_REDIS_PASSWORD` Redis 存储插件的密码，仅在指定 Redis 作为 metadata server 时生效。如果未设置，将不会尝试进行密码认证登录 Redis。
- `MC_REDIS_DB_INDEX` Redis 存储插件的数据库索引，必须为 0 到 255 之间的整数。仅在指定 Redis 作为 metadata server 时生效。如果未设置或无效，默认值为 0。
- `MC_FRAGMENT_RATIO` 在将RdmaTransport::submitTransferTask中切割传输任务为传输块时，当切割完成后最后一块数据大小小于等于切割块大小的1/MC_FRAGMENT_RATIO，最后一块数据将合并进前一块的切割块进行传输以减少开销，默认值为4。
- `MC_ENABLE_DEST_DEVICE_AFFINITY` 启用设备亲和性以优化 RDMA 性能。启用后，Transfer Engine 将优先选择和本地网卡同名的远端网卡进行通信，以减少 QP 数量并改善 Rail-optimized 拓扑中的网络性能。默认值为 false
- `MC_FORCE_HCA` 强制使用RDMA作为主要传输方式，如果没有探测到有效的RDMA网卡，返回失败
- `MC_FORCE_MNNVL` 强制使用 Multi-Node NVLink 作为主要传输方式，无论是否安装了有效的 RDMA 网卡
- `MC_INTRA_NVLINK` 指定使用Intra-Node NVLink 作为主要传输方式，同时注意该设置不能与MC_FORCE_MNNVL一起使用
- `MC_FORCE_TCP` 强制使用 TCP 作为主要传输方式，无论是否安装了有效的 RDMA 网卡
- `MC_MIN_PRC_PORT` 指定 RPC 服务使用的最小端口号。默认值为 15000。
- `MC_MAX_PRC_PORT` 指定 RPC 服务使用的最大端口号。默认值为 17000。
- `MC_PATH_ROUNDROBIN` 指定 RDMA 路径选择使用 Round Robin 模式，这对于传输大块数据可能有利。
- `MC_ENDPOINT_STORE_TYPE` 选择 FIFO Endpoint Store (`FIFO`) 或者 Sieve Endpoint Store (`SIEVE`)，模式是 `SIEVE`。
