# Ascend Transport
Ascend Transport源代码路径为Mooncake/mooncake-transfer-engine/src/transport/ascend_transport，该路径下还包含自动化编译脚本、README文件。
## 概述
Ascend Transport是一个单边语义的高性能零拷贝NPU数据传输库，直接兼容Mooncake Transfer Engine。要编译使用Ascend Transport库，请在mooncake-common\common.cmake文件中将USE_ASCEND开关置于"ON"。

Ascend Transport支持使用单边语义进行NPU间数据传输（当前版本只支持DEVICE TO DEVICE，其它进行中），用户只需通过Mooncake Transfer Engine接口指定两端传输的节点及内存信息，即可完成点对点高性能传输。Ascend Transport为用户隐去繁琐的内部实现，自动完成一系列如建链、注册和交换内存、检查传输状态等操作。

Ascend Transport支持Mooncake在transfer_engine_py.cpp中提供的批量传输接口batch_transfer_sync，支持批量传输一组目标端为同一张卡且不连续的内存块，经过测试，在传输小块不连续内存时(如128KB)，使用批量传输接口带宽较使用transferSync接口，带宽提升达到了100%+。

### 新增依赖
Ascend Transport在Mooncake本身依赖的基础上，新增了一部分HCCL的依赖：
**MPI**
yum install -y mpich mpich-devel
或者
apt-get install -y mpich libmpich-dev

**昇腾Compute Architecture for Neural Networks**
更新到昇腾Compute Architecture for Neural Networks 8.2.RC1版本，不再需要pkg包。

### 一键式编译脚本
Ascend Transport提供一键式编译脚本，脚本位置为scripts/ascend/dependencies_ascend.sh,执行命令如下：
sh scripts/ascend/dependencies_ascend.sh
一键式编译脚本同样考虑到用户无法直接在环境上git clone的情况，用户可以把需要git clone的依赖项源码和Mooncake的源码放在同一目录中，然后执行脚本，脚本会自动编译依赖项和Mooncake。Mooncake的输出件是libascend_transport_mem.so和mooncake-wheel/dist/mooncake_transfer_engine*.whl包。

使用ascend_transport前，记得设置环境变量，命令如下：
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH
或者把so包复制到$LD_LIBRARY_PATH指向的其它路径下。

### 注意事项（必看）
1.调用TransferEngine initialize时需要在传入的local_server_name参数中包含运行的NPU物理Id，local_server_name参数从ip:port改为ip:port:npu_x，例如"0.0.0.0:12345:npu_2"，Ascend Transport会在内部解析获取本卡的NPU物理Id。存储在metadata的segment_name以及传输时传入的target_hostname格式不变，仍为ip:port，例如："0.0.0.0:12345“。

2.ascend_transport会建立一个host侧的tcp连接，占用端口为10000加上npu的物理Id,若该端口已被占用会自动寻找一个可用端口；

3.由于内部接口限制，ascend_transport要求注册的内存是NPU内存，且必须2M对齐；

4.请确保/etc路径下存在文件hccn.conf，特别是在容器内，请挂载/etc/hccn.conf或者把宿主机的文件复制到容器的/etc路径下。

5.潜在的 MPI 冲突,同时安装 MPI 和 OpenMPI 可能导致冲突。如果遇到与 MPI 相关的问题，请尝试按顺序执行以下命令进行解决：

sudo apt install mpich libmpich-dev        # 安装 MPICH
sudo apt purge openmpi-bin libopenmpi-dev  # 卸载 OpenMPI

6.当前版本不支持IPV6，我们会很快完成针对IPV6的适配。

### 一键式安装脚本(不编译Mooncake)
用户如果在一台机器上编译过Mooncake，在其它相同系统的机器上可以不编译Mooncake只安装依赖项，直接使用Mooncake的输出件libascend_transport_mem.so和mooncake whl包。
用户将whl包、so包和scripts/ascend/dependencies_ascend_installation.sh都放置到同一目录下，执行脚本即可，命令如下：
./dependencies_ascend_installation.sh

使用ascend_transport前，记得执行环境变量，命令如下：
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH
或者把so包复制到$LD_LIBRARY_PATH指向的其它路径下。

### 编译说明
在成功安装所有依赖后，正常编译Mooncake即可，如果报错，可尝试设置环境变量：
export CPLUS_INCLUDE_PATH=$(echo $CPLUS_INCLUDE_PATH | tr ':' '\n' | grep -v "/usr/local/Ascend" | paste -sd: -)

### 端点管理
每张华为NPU卡拥有一张参数面网卡，对应使用一个TransferEngine管理该NPU卡上所有的传输操作。

### Ranktable管理
Ascend Transport不依赖全局的Ranktable信息，只需要获取当前NPU卡的本地Ranktable信息，在Ascend Transport初始化时会解析/etc/hccn.conf文件自动获取。

### 初始化
使用Ascend Transport时，TransferEngine 在完成构造后同样需要调用 `init` 函数进行初始化：
```cpp
TransferEngine();

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port)
```
唯一的区别在于，在TransferEngine init时需要在local_server_name参数中包含TransferEngine所在的NPU物理卡号，local_server_name参数从ip:port改为ip:port:npu_x，例如"0.0.0.0:12345:npu_2"。

```注意```：Ascend Transport只是在不修改Mooncake对外接口的形势下，传入所在的NPU物理卡号，只为Transport内部使用，metadata的segment_desc_name仍然还是原来的形式，即ip:port，因此，每张npu卡的port需要保证互不相同，且端口未被占用。

### metadata服务
Ascend Transport兼容所有Mooncake当前支持的metadata服务，包括 etcd, redis 和 http，以及p2phandshake。Ascend Transport在初始化时会注册当前NPU卡的一系列信息，包括device_id\device_ip\rank_id\server_ip等。

### 数据传输
Ascend Transport支持write/read语义，且会自动判断是否跨HCCS通信，选择HCCS/ROCE的底层通信协议，用户只需采用Mooncake的getTransferStatus接口即可获取每个请求的传输情况。

### 故障处理
Ascend Transport在HCCL本身的故障处理基础上，设计了完善的故障处理机制。针对初始化、建链、数据传输等多个阶段可能出现的故障，新增或沿用了失败重试机制。在重试仍然失败后，沿用了HCCL集合通信相关操作错误码，给出精准的报错信息。为获取更详细的错误信息，也可以查询/root/Ascend/log/debug目录下的plog日志。

### 测试用例
Ascend Transport提供多场景测试文件mooncake-transfer-engine/example/transfer_engine_ascend_one_sided.cpp，可以完成一对一、一对二、二对一多种场景的传输测试，以及性能测试文件mooncake-transfer-engine/example/transfer_engine_ascend_perf.cpp。编译 Transfer Engine 成功后，可在 `build/mooncake-transfer-engine/example` 目录下找到对应的测试程序。执行命令可通过传入参数配置，具体的可配置参数请查看测试文件开头DEFINE_string的参数列表。

当 metadata_server 配置为 P2PHANDSHAKE 时，Mooncake 会在新的 RPC 端口映射中随机选择监听端口，以避免端口冲突。因此，测试时需要按以下步骤操作：
1.先启动目标节点，观察其在 mooncake-transfer-engine/src/transfer_engine.cpp 中打印的日志，找到如下格式的语句：
Transfer Engine RPC using <协议> listening on <IP>:<实际端口>，记录目标节点实际监听的端口号。
2.修改发起节点的启动命令：
将 --segment_id 参数的值改为目标节点的 IP + 实际监听的端口号（格式为 <IP>:<端口>）。
3.启动发起节点，完成连接测试。

完整命令格式见下文：

多场景用例执行命令如：
```启动发起节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608 --batch_size=32
```启动目标节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --block_size=8388608 --batch_size=32

性能用例执行命令如：
```启动发起节点：```
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=16384 --batch_size=32 --block_iteration=10
```启动目标节点：```
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --block_size=16384 --batch_size=32 --block_iteration=10

需要注意的是，上面传入的device_id既代表NPU逻辑id也代表物理id，如果您在容器里没有挂载所有的卡，使用上面的demo时请分别传入逻辑id和物理id，即--device_id改为--device_logicid和--device_phyid，如容器内只挂载了5卡和7卡，5卡为发起端，7卡为接收端，多场景用例执行命令改为：
```启动发起节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_logicid=0 --device_phyid=5 --mode=initiator --block_size=8388608
```启动目标节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_logicid=1 --device_phyid=7 --mode=target --block_size=8388608

### 打印说明
如果需要得到每个传输request请求是否跨hccs和耗时情况，可以通过设置环境变量打开相关打印，命令如下：
export ASCEND_TRANSPORT_PRINT=1

### 超时时间配置
Ascend Transport基于TCP的带外通信，连接的超时时间通过环境变量Ascend_TCP_TIMEOUT配置，默认为30秒，在主机侧recv接收超时设置为30秒，即recv阻塞超过30s未收到对端的消息会报错。

hccl_socket的连接超时时间通过环境变量Ascend_HCCL_SOCKET_TIMEOUT配置，默认为30秒，超时则本次传输会报错并返回。

hccl_socket有保活要求，执行超时通过环境变量HCCL_EXEC_TIMEOUT配置，超过HCCL_EXEC_TIMEOUT未进行通信，会断开hccl_socket连接。

在transport_mem中，端到端之间的点对点通信涉及连接握手过程，其超时时间通过Ascend_TRANSPORT_MEM_TIMEOUT配置，默认为120秒。

### 错误码
Ascend传输错误码沿用HCCL集合通信传输错误码。
typedef enum {
    HCCL_SUCCESS = 0,       /* 成功 */
    HCCL_E_PARA = 1,        /* 参数错误 */
    HCCL_E_PTR = 2,         /* 空指针，检查是否多次执行了如 notifypool->reset 之类的操作，导致指针被清除 */
    HCCL_E_MEMORY = 3,      /* 内存错误 */
    HCCL_E_INTERNAL = 4,    /* 内部错误，常见原因：内存未注册、内存注册错误、内存注册首地址未2M对齐、内存未成功交换。检查传输的内存是否在已注册内存范围内。
                            在多机场景下，检查启动脚本是否混用 */
    HCCL_E_NOT_SUPPORT = 5, /* 不支持该特性 */
    HCCL_E_NOT_FOUND = 6,   /* 特定资源未找到 */
    HCCL_E_UNAVAIL = 7,     /* 资源不可用 */
    HCCL_E_SYSCALL = 8,     /* 调用系统接口出错，优先检查初始化参数是否正确传递 */
    HCCL_E_TIMEOUT = 9,     /* 超时 */
    HCCL_E_OPEN_FILE_FAILURE = 10, /* 打开文件失败 */
    HCCL_E_TCP_CONNECT = 11, /* tcp 连接失败，连接失败，仅限于检查两端的端口以及连通性 */
    HCCL_E_ROCE_CONNECT = 12, /* roce 连接失败，连接失败，仅限于检查两端的端口以及连通性 */
    HCCL_E_TCP_TRANSFER = 13, /* tcp 传输失败，检查端口是否正确以及 SOCKET 是否混用 */
    HCCL_E_ROCE_TRANSFER = 14, /* roce 传输失败 */
    HCCL_E_RUNTIME = 15,      /* 调用运行时 api 失败，检查传输时传入的 transportmem 相关参数 */
    HCCL_E_DRV = 16,          /* 调用驱动 api 失败 */
    HCCL_E_PROFILING = 17,    /* 调用 profiling api 失败 */
    HCCL_E_CCE = 18,          /* 调用 cce api 失败 */
    HCCL_E_NETWORK = 19,      /* 调用网络 api 失败 */
    HCCL_E_AGAIN = 20,        /* 操作重复，在某些情况下不是错误。例如，多次注册同一内存时，会直接返回第一次注册的句柄。如果不影响功能，则视为成功 */
    HCCL_E_REMOTE = 21,       /* 错误的 cqe */
    HCCL_E_SUSPENDING = 22,   /* 错误的通信挂起 */
    HCCL_E_RESERVED           /* 保留 */
} HcclResult;