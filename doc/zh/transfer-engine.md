# Transfer Engine

## 概述
Mooncake Transfer Engine 是一个围绕 Segment 和 BatchTransfer 两个核心抽象设计的高性能，零拷贝数据传输库。

- [**Segment**](#segment) 代表一段可被远程读写的连续地址空间，既可以是 DRAM 或 VRAM 提供的非持久化存储 **RAM Segment**，也可以是 NVMeof 提供的持久化存储 **NVMeof Segment**。

- [**BatchTransfer**](#batchtransfer) 封装了操作请求，具体负责将一个 Segment 中非连续的一组数据空间的数据和另外一组 Segment 的对应空间进行数据同步，支持 Read/Write 两种方向，因此类似一个异步且更灵活的的 AllScatter/AllGather。

![transfer_engine](../../image/transfer-engine.png)

如上图所示，每个特定的客户端对应一个 TransferEngine，其中不仅包含一个 RAM Segment，还集成了对于多线程多网卡高速传输的管理。RAM Segment 原则上就对应这个 TransferEngine 的全部虚拟地址空间，但实际上仅仅会注册其中的部分区域（被称为一个 Buffer）供外部 (GPUDirect) RDMA Read/Write。每一段 Buffer 可以分别设置权限（对应 RDMA rkey 等）和网卡亲和性（比如基于拓扑优先从哪张卡读写等）。

Mooncake Transfer Engine 通过 `TransferEngine` 类对外提供接口（位于 `mooncake-transfer-engine/include/transfer_engine.h`），其中对应不同后端的具体的数据传输功能由 `Transport` 类实现，目前支持 `TcpTransport`、`RdmaTransport` 和 `NVMeoFTransport`。

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
|----------|------|------|
| DRAM     | ✓    | ✓    |
| VRAM     | ✓    | ✓    |
| NVMe-of  | ✓    | ✓    |

- Local memcpy: 如果目标 Segment 其实就在本地 DRAM/VRAM 的话，直接使用 memcpy、cudaMemcpy 等数据复制接口。
- RDMA: 支持本地 DRAM/VRAM 与远程 DRAM 之间的数据传递。在实现上支持多网卡池化及重试等功能。
- cuFile (GPUDirect Storage): 实现本地 DRAM/VRAM 与 Local/Remote NVMeof 之间的数据传递。

BatchTransfer API 使用请求（Request）对象数组传入用户请求，需指定操作类型（READ 或 WRITE）、数据长度以及本地和远程内存的地址。传输操作适用于 DRAM 和 GPU VRAM，并在最佳情况下利用 GPU 直接 RDMA，前提是指定的内存区域已预先注册。这些操作的完成情况可通过 `getTransferStatus` API 来异步监控这些操作的完成情况。

### 拓扑感知路径选择（Topology Aware Path Selection）
现代推理服务器通常由多个CPU插槽、DRAM、GPU和RDMA NIC设备组成。尽管从技术上讲，使用任何RDMA NIC将数据从本地DRAM或VRAM传输到远程位置是可能的，但这些传输可能会受到Ultra Path Interconnect (UPI)或PCIe交换机带宽限制的制约。为了克服这些限制，Transfer Engine 实现了拓扑感知路径选择算法。在处理请求之前，每个服务器生成一个拓扑矩阵（Topology Matrix）并将其广播到整个集群。拓扑矩阵将网络接口卡（NIC）分类为各种类型的内存的“首选”和“次要”列表，这些类型在内存注册时指定。在正常情况下，选择首选列表中的NIC进行传输，便于在本地NUMA或仅通过本地PCIe交换机进行GPU Direct RDMA操作。在出现故障的情况下，两个列表中的所有NIC都可能被使用。上述过程包括根据内存地址识别适当的本地和目标NIC，建立连接，并执行数据传输。

![topology-matrix](../../image/topology-matrix.png)

例如，如图所示，要将数据从本地节点分配给 `cpu:0` 的缓冲区0传输到目标节点分配给`cpu:1`的缓冲区1，引擎首先使用本地服务器的拓扑矩阵识别`cpu:0`的首选NIC，并选择一个，如`mlx5_1`，作为本地NIC。同样，根据目标内存地址选择目标NIC，如`mlx5_3`。这种设置允许建立从`mlx5_1@本地`到`mlx5_3@目标`的RDMA连接，以执行RDMA读写操作。

为了进一步最大化带宽利用率，如果单个请求的传输长度超过16KB，则其内部被划分为多个切片。每个切片可能使用不同的路径，使所有RDMA NIC能够协同工作。

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

   例如，可使用如下命令行启动 `etcd` 服务：
      ```bash
      # This is 10.0.0.1
      etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379
      ```

   1.2. **启动 `http` 作为 `metadata` 服务**

   例如，可使用 `mooncake-transfer-engine/example/http-metadata-server` 示例中的 `http` 服务：
      ```bash
      # This is 10.0.0.1
      # cd mooncake-transfer-engine/example/http-metadata-server
      go run . --addr=:8080
      ```

2. **启动目标节点。**
    ```bash
    # This is 10.0.0.2
    export MC_GID_INDEX=n
    ./transfer_engine_bench --mode=target \
                            --metadata_server=10.0.0.1:2379 \
                            --local_server_name=10.0.0.2:12345 \
                            --device_name=erdma_0
    ```
   各个参数的含义如下：
   - 环境变量 `MC_GID_INDEX` 对应参数的默认值为 0，表示由 Transfer Engine 选取一个最可能连通的 GID。
     如果连接被挂起，用户仍需手工设置改环境变量的值。
   - `--mode=target` 表示启动目标节点。目标节点不发起读写请求，只是被动按发起节点的要求供给或写入数据。
      > 注意：实际应用中可不区分目标节点和发起节点，每个节点可以向集群内其他节点自由发起读写请求。
   - `--metadata_server` 为元数据服务器地址（etcd 服务的完整地址）。
      > 如果使用 `http` 作为 `metadata` 服务，需要将 `--metadata_server` 参数改为 `--metadata_server=http://10.0.0.1:8080/metadata`，并且指定 `--metadata_type=http`。
   - `--local_server_name` 表示本机器地址，大多数情况下无需设置。如果不设置该选项，则该值等同于本机的主机名（即 `hostname(2)` ）。集群内的其它节点会使用此地址尝试与该节点进行带外通信，从而建立 RDMA 连接。
      > 注意：若带外通信失败则连接无法建立。因此，若有必要需修改集群所有节点的 `/etc/hosts` 文件，使得可以通过主机名定位到正确的节点。
   - `--device_name` 表示传输过程使用的 RDMA 网卡名称。
      > 提示：高级用户还可通过 `--nic_priority_matrix` 传入网卡优先级矩阵 JSON 文件，详细参考 [Transfer Engine 的开发者手册](#transferengineinstallorgettransport)。
   - 在仅支持 TCP 的网络环境中，可使用 `--protocol=tcp` 参数，此时不需要指定 `--device_name` 参数。

1. **启动发起节点。**
    ```bash
    # This is 10.0.0.3
    export MC_GID_INDEX=n
    ./transfer_engine_bench --metadata_server=10.0.0.1:2379 \
                            --segment_id=10.0.0.2:12345 \
                            --local_server_name=10.0.0.3:12346 \
                            --device_name=erdma_1
    ```
   各个参数的含义如下（其余同前）：
   - `--segment_id` 可以简单理解为目标节点的主机名，需要和启动目标节点时 `--local_server_name` 传入的值（如果有）保持一致。
   
   正常情况下，发起节点将开始进行传输操作，等待 10s 后回显“Test completed”信息，表明测试完成。

   发起节点还可以配置下列测试参数：`--operation`（可为 `"read"` 或 `"write"`）、`batch_size`、`block_size`、`duration`、`threads` 等。

### 运行示例及结果判读

下面的视频显示了按上述操作正常运行的过程，其中右侧是 Target，左侧是 Initiator。测试结束后，Initiator 会报告测试时长（10 秒）、IOPS（379008 次请求/s）、吞吐率（19.87 GiB/s）等信息。这里的吞吐率超出了所用主机单卡支持的最大吞吐率。

![transfer-engine-running](../../image/transfer-engine-running.gif)

> 如果在执行期间发生异常，大多数情况是参数设置不正确所致，建议参考[故障排除文档](troubleshooting.md)先行排查。

## C/C++ API
Transfer Engine 通过 `TransferEngine` 类对外提供接口（位于 `mooncake-transfer-engine/include/transfer_engine.h`），其中对应不同后端的具体的数据传输功能由 `Transport` 类实现，目前支持 `TcpTransport`,`RdmaTransport` 和 `NVMeoFTransport`。

### 数据传输

#### Transport::TransferRequest

Mooncake Transfer Engine 提供的最核心 API 是：通过 `Transport::submitTransfer` 接口提交一组异步的 `Transport::TransferRequest` 任务，并通过 `Transport::getTransferStatus` 接口查询其状态。每个 `Transport::TransferRequest` 规定从本地的起始地址 `source` 开始，读取或写入长度为 `length` 的连续数据空间，到 `target_id` 对应的段、从 `target_offset` 开始的位置。

`Transport::TransferRequest` 结构体定义如下：

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

#### Transport::allocateBatchID

```cpp
BatchID allocateBatchID(size_t batch_size);
```

分配 `BatchID`。同一 `BatchID` 下最多可提交 `batch_size` 个 `TransferRequest`。

- `batch_size`: 同一 `BatchID` 下最多可提交的 `TransferRequest` 数量；
- 返回值：若成功，返回 `BatchID`（非负）；否则返回负数值。

#### Transport::submitTransfer

```cpp
int submitTransfer(BatchID batch_id, const std::vector<TransferRequest> &entries);
```

向 `batch_id` 追加提交新的 `TransferRequest` 任务。该任务被异步提交到后台线程池。同一 `batch_id` 下累计的 `entries` 数量不应超过创建时定义的 `batch_size`。

- `batch_id`: 所属的 `BatchID`；
- `entries`: `TransferRequest` 数组；
- 返回值：若成功，返回 0；否则返回负数值。

#### Transport::getTransferStatus

```cpp
enum TaskStatus
{
  WAITING,   // 正在处于传输阶段
  PENDING,   // 暂不支持
  INVALID,   // 参数不合法
  CANNELED,  // 暂不支持
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

#### Transport::freeBatchID

```cpp
int freeBatchID(BatchID batch_id);
```

回收 `BatchID`，之后对此的 `submitTransfer` 及 `getTransferStatus` 操作均是未定义的。若 `BatchID` 内仍有 `TransferRequest` 未完成，则拒绝操作。

- `batch_id`: 所属的 `BatchID`；
- 返回值：若成功，返回 0；否则返回负数值。

### 多 Transport 管理
`TransferEngine` 类内部管理多后端的 `Transport` 类，用户可向 `TransferEngine` 中装载或卸载对不同后端进行传输的 `Transport`。

#### TransferEngine::installOrGetTransport
```cpp
Transport* installOrGetTransport(const std::string& proto, void** args);
```
在 `TransferEngine` 中注册 `Transport`。如果某个协议对应的 `Transport` 已存在，则返回该 `Transport`。

- `proto`: `Transport` 使用的传输协议名称，目前支持 `tcp`, `rdma`, `nvmeof`。
- `args`：以变长数组形式呈现的 `Transport` 初始化需要的其他参数，数组内最后一个成员应当是 `nullptr`。
- 返回值：若 `proto` 在确定范围内，返回对应 `proto` 的 `Transport`；否则返回空指针。

**TCP 传输模式：**
对于 TCP 传输模式，注册 `Transport` 期间不需要传入 `args` 对象。
```cpp
engine->installOrGetTransport("tcp", nullptr);
```

**RDMA 传输模式：**
对于 RDMA 传输模式，注册 `Transport` 期间需通过 `args` 指定网卡优先级顺序。
```cpp
void** args = (void**) malloc(2 * sizeof(void*));
args[0] = /* topology matrix */;
args[1] = nullptr;
engine->installOrGetTransport("rdma", args);
```
网卡优先级顺序是一个 JSON 字符串，表示使用的存储介质名称及优先使用的网卡列表，样例如下：
```json
{
    "cpu:0": [["mlx0", "mlx1"], ["mlx2", "mlx3"]],
    "cuda:0": [["mlx1", "mlx0"]],
    ...
}
```
其中每个 `key` 代表一个 CPU socket 或者一个 GPU device 对应的设备名称
每个 `value` 为一个 (`preferred_nic_list`, `accessable_nic_list`) 的二元组，每一项都是一个 NIC 名称的列表（list）。
- `preferred_nic_list` 表示优先选择的 NIC，比如对于 CPU 可以是当前直连而非跨 NUMA 的 NIC，对于 GPU 可以是挂在同一个 PCIe Switch 下的 NIC；
- `accessable_nic_list` 表示虽然不优选但是理论上可以连接上的 NIC，用于故障重试场景。

**NVMeOF 传输模式：** 对于 NVMeOF 传输模式，注册 `Transport` 期间需通过 `args` 指定文件路径。
```cpp
void** args = (void**) malloc(2 * sizeof(void*));
args[0] = /* topology matrix */;
args[1] = nullptr;
engine->installOrGetTransport("nvmeof", args);
```

#### TransferEngine::uinstallTransport
```cpp
int uninstallTransport(const std::string& proto);
```
从 `TransferEngine` 中卸载 `Transport`。
- `proto`: `Transport` 使用的传输协议名称，目前支持 `rdma`, `nvmeof`。
- 返回值：若成功，返回 0；否则返回负数值。

### 空间注册

对于 RDMA 的传输过程，作为源端指针的 `TransferRequest::source` 必须提前注册为 RDMA 可读写的 Memory Region 空间，即纳入当前进程中 RAM Segment 的一部分。因此需要用到如下函数：

#### TransferEngine::registerLocalMemory

```cpp
int registerLocalMemory(void *addr, size_t size, string location, bool remote_accessible);
```

在本地 DRAM/VRAM 上注册起始地址为 `addr`，长度为 `size` 的空间。

- `addr`: 注册空间起始地址；
- `size`：注册空间长度；
- `location`: 这一段内存对应的 `device`，比如 `cuda:0` 表示对应 GPU 设备，`cpu:0` 表示对应 CPU socket，通过和网卡优先级顺序表（见`installOrGetTransport`） 匹配，识别优选的网卡。
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

TranferEngine 提供 `openSegment` 函数，该函数获取一个 `SegmentHandle`，用于后续 `Transport` 的传输。
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

```cpp
TransferEngine(std::unique_ptr<TransferMetadata> metadata_client);
TransferMetadata(const std::string &metadata_server, const std::string &protocol = "etcd");
```

- TransferMetadata 对象指针，该对象将 TransferEngine 框架与元数据服务器等带外通信逻辑抽取出来，以方便用户将其部署到不同的环境中。
  目前支持 `etcd`，`redis` 和 `http` 三种元数据服务。`metadata_server` 表示 `etcd`/`redis` 的 IP 地址/主机名，或者 http 服务的 URI。

为了便于异常处理，TransferEngine 在完成构造后需要调用init函数进行二次构造：
```cpp
int init(std::string& server_name, std::string& connectable_name, uint64_t rpc_port = 12345);
```
- server_name: 本地的 server name，保证在集群内唯一。它同时作为其他节点引用当前实例所属 RAM Segment 的名称（即 Segment Name）
- connectable_name：用于被其它 client 连接的 name，可为 hostname 或 ip 地址。
- rpc_port：用于与其它 client 交互的 rpc 端口。- 
- 返回值：若成功，返回 0；若 TransferEngine 已被 init 过，返回 -1。

```cpp
  ~TransferEngine();
```

回收分配的所有类型资源，同时也会删除掉全局 meta data server 上的信息。

## 二次开发
### 使用 C/C++ 接口二次开发
在完成 Mooncake Store 编译后，可将编译好的静态库文件 `libtransfer_engine.a` 及 C 头文件 `transfer_engine_c.h`，移入到你自己的项目里。不需要引用 `src/transfer_engine` 下的其他文件。

在项目构建阶段，需要为你的应用配置如下选项：
```
-I/path/to/include
-L/path/to/lib -ltransfer_engine
-lnuma -lglog -libverbs -ljsoncpp -letcd-cpp-api -lprotobuf -lgrpc++ -lgrpc
```

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
- `MC_GID_INDEX` 每个设备实例使用的 GID 序号，默认值 3（或平台支持的最大值）
- `MC_MAX_CQE_PER_CTX` 每个设备实例中 CQ 缓冲区大小，默认值 4096
- `MC_MAX_EP_PER_CTX` 每个设备实例中活跃 EndPoint 数量上限，默认值 256
- `MC_NUM_QP_PER_EP` 每个 EndPoint 中 QP 数量，数量越多则细粒度 I/O 性能越好，默认值 2
- `MC_MAX_SGE` 每个 QP 最大可支持的 SGE 数量，默认值 4（或平台支持的最高值）
- `MC_MAX_WR` 每个 QP 最大可支持的 Work Request 数量，默认值 256（或平台支持的最高值）
- `MC_MAX_INLINE` 每个 QP 最大可支持的 Inline 写数据量（字节），默认值 64（或平台支持的最高值）
- `MC_MTU` 每个设备实例使用的 MTU 长度，可为 512、1024、2048、4096，默认值 4096（或平台支持的最大长度）
- `MC_WORKERS_PER_CTX` 每个设备实例对应的异步工作线程数量
- `MC_SLICE_SIZE` Transfer Engine 中用户请求的切分粒度
- `MC_RETRY_CNT` Transfer Engine 中最大重试次数
- `MC_VERBOSE` 若设置此选项，则在运行时会输出更详细的日志

