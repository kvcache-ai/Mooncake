# Ascend Transport
Ascend Transport源代码路径为Mooncake/mooncake-transfer-engine/src/transport/ascend_transport，该路径下还包含自动化编译脚本、README文件。
## 概述
Ascend Transport是一个单边语义的高性能零拷贝NPU数据传输库，直接兼容Mooncake Transfer Engine。要编译使用Ascend Transport库，请在mooncake-common\common.cmake文件中将USE_ASCEND开关置于"ON"。

Ascend Transport支持使用单边语义进行NPU间数据传输（当前版本只支持DEVICE TO DEVICE，其它进行中），用户只需通过Mooncake Transfer Engine接口指定两端传输的节点及内存信息，即可完成点对点高性能传输。Ascend Transport为用户隐去繁琐的内部实现，自动完成一系列如建链、注册和交换内存、检查传输状态等操作。

### 新增依赖
Ascend Transport在Mooncake本身依赖的基础上，新增了一部分HCCL的依赖：
**MPI**
yum install -y mpich mpich-devel

**昇腾Compute Architecture for Neural Networks**
昇腾Compute Architecture for Neural Networks 8.1.RC1版本

### 一键式编译脚本
Ascend Transport提供一键式编译脚本，脚本位置为scripts/ascend/dependencies_ascend.sh，将脚本复制到想要安装依赖和Mooncake的目录下执行脚本即可，也支持传入参数指定安装路径，不传入时默认为脚本所在目录，命令如下：
./dependencies_ascend.sh /path/to/install_directory
一键式编译脚本同样考虑到用户无法直接在环境上git clone的情况，用户可以把给依赖和Mooncake的源码放在安装目录下并指定，脚本会自动编译依赖和Mooncake。

### 一键式安装脚本(不编译Mooncake)
为了避免用户出现在编译Mooncake的环境上，执行其它进程有冲突的可能问题，Ascend Transport给出编译和执行Mooncake分离的一种方案。
在执行dependencies_ascend.sh完成Mooncake编译后，用户可执行scripts/ascend/dependencies_ascend_installation.sh，仅安装依赖。将一键式编译脚本生成的mooncake whl包和libascend_transport_mem.so放在安装目录下

将脚本复制到安装目录下执行脚本即可，命令如下：
./dependencies_ascend_installation.sh /path/to/install_directory

在使用前，确保编译生成的libascend_transport_mem.so文件已经复制到/usr/local/Ascend/ascend-toolkit/latest/python/site-packages下，然后执行命令：
export LD_LIBRARY_PATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:$LD_LIBRARY_PATH

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
Ascend Transport在HCCL本身的故障处理基础上，设计了完善的故障处理机制。针对初始化、建链、数据传输等多个阶段可能出现的故障，新增或沿用了失败重试机制。在重试仍然失败后，沿用了HCCL集合通信相关操作错误码，给出精准的报错信息。为获取更详细的错误信息，也可以查询/root/Ascend/log目录下的plog日志。

### 测试用例
Ascend Transport提供多场景测试mooncake-transfer-engine/example/transfer_engine_ascend_one_sided.cpp和性能测试mooncake-transfer-engine/example/transfer_engine_ascend_perf.cpp两个测试文件，根据测试头部设置的可传入参数传入合法参数，
可以完成一对一、一对二、二对一多种场景和性能测试。

多场景用例执行命令如：
```启动发起节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608
```启动目标节点：```
./transfer_engine_ascend_one_sided --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target --block_size=8388608

性能用例执行命令如：
```启动发起节点：```
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12345 --protocol=hccl --operation=write --segment_id=10.0.0.0:12346 --device_id=0 --mode=initiator --block_size=8388608
```启动目标节点：```
./transfer_engine_ascend_perf --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.0:12346 --protocol=hccl --operation=write --device_id=1 --mode=target

### 打印说明
如果需要得到每个传输request请求是否跨hccs和耗时情况，可以通过设置环境变量打开相关打印，命令如下：
export ASCEND_TRANSPORT_PRINT=1

### 注意事项
1.ascend_transport会建立一个host侧的tcp连接，占用端口为10000+deviceId,请注意避开此端口，勿重复占用
2.ascend_transport 在一次传输结束后，若对端（remote end）发生掉线并重启，系统已设计有自动重试建链机制，无需手动重启本端服务。
注意：若目标端发生掉线并重启，发起端在下次发起请求时会尝试重新建立连接。目标端需确保在发起端发起请求后的 5 秒内完成重启并进入就绪状态。若超过该时间窗口仍未恢复，连接将失败并返回错误。

### 超时时间配置
Ascend Transport基于TCP的带外通信，在主机侧接收超时设置为 120 秒。

在hccl_socket中，连接超时时间由环境变量HCCL_CONNECT_TIMEOUT配置，执行超时通过环境变量HCCL_EXEC_TIMEOUT配置，超过HCCL_EXEC_TIMEOUT未进行通信，会断开hccl_socket连接。

在transport_mem中，端到端之间的点对点通信涉及连接握手过程，其超时时间为 120 秒。