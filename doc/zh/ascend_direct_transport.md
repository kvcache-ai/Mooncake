# Ascend Direct Transport
Ascend Direct Transport(以下简称Transport)源代码路径为Mooncake/mooncake-transfer-engine/src/transport/ascend_transport/ascend_direct_transport。

## 概述
Transport是一个基于CANN提供的ADXL能力构建的传输适配层，直接兼容Mooncake Transfer Engine。
用户使用Transport可以实现在Ascend环境进行Host To Device, Device To Host, Device To Device传输，并且可使用多种通信协议，如HCCS、RDMA，未来还可实现Ascend和GPU异构环境的数据传输。
要编译使用Transport库，请参考build.md所述流程，并开启USE_ASCEND_DIRECT选项。

### 新增依赖
**昇腾Compute Architecture for Neural Networks**
请安装昇腾Compute Architecture for Neural Networks最新版本。

### 测试用例
Transport提供性能测试文件mooncake-transfer-engine/example/transfer_engine_ascend_direct_perf.cpp。

当 metadata_server 配置为 P2PHANDSHAKE 时，Mooncake 会在新的 RPC 端口映射中随机选择监听端口，以避免端口冲突。因此，测试时需要按以下步骤操作：
1. 先启动目标节点，观察其在 mooncake-transfer-engine/src/transfer_engine.cpp 中打印的日志，找到如下格式的语句：
Transfer Engine RPC using <协议> listening on <IP>:<实际端口>，记录目标节点实际监听的端口号。
2. 修改发起节点的启动命令： 将 --segment_id 参数的值改为目标节点的 IP + 实际监听的端口号（格式为 <IP>:<端口>）。
3. 启动发起节点，完成连接测试。

完整命令格式见下文：

启动目标节点:
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12345 --operation=write --device_logicid=0 --mode=target --block_size=16384 --batch_size=32 --block_iteration=10
```
启动发起节点:
```shell
./transfer_engine_ascend_direct_perf --metadata_server=P2PHANDSHAKE --local_server_name=127.0.0.1:12346 --operation=write --device_logicid=1 --mode=initiator --block_size=16384 --batch_size=32 --block_iteration=10  --segment_id=127.0.0.1:real_port
```

### 注意事项（必看）
1. 调用TransferEngine initialize前需要set device, 比如`torch.npu.set_device(0)`。

2. Transport内部会建立一个host侧的tcp连接，会自动寻找可使用的端口，没有可用的时候会报错。

3. 当使用`HCCS`通信协议时，注册的内存地址需要和页表对齐(host内存是4KB对齐，device内存是2MB对齐)。

4. 请确保`/etc/hccn.conf`存在，特别是在容器内，请挂载`/etc/hccn.conf`或者把宿主机的文件复制到容器的/etc路径下。

5. 通过`ASCEND_CONNECT_TIMEOUT`环境变量控制链路建链超时时间，默认超时时间为3s, 通过`ASCEND_TRANSFER_TIMEOUT`环境变量控制数据传输超时时间，默认超时时间为3s。

6. 通过`HCCL_RDMA_TIMEOUT` 用于配置RDMA网卡数据包重传超时时间系数，真实的数据包重传超时时间为`4.096us * 2 ^ $HCCL_RDMA_TIMEOUT`，通过`HCCL_RDMA_RETRY_CNT`来配置RDMA网卡的重传次数，建议配置`ASCEND_TRANSFER_TIMEOUT`略大于`重传时间 * HCCL_RDMA_RETRY_CNT`。

7. A2 server内/A3超节点内默认通信协议为HCCS，可以通过设置`export HCCL_INTRA_ROCE_ENABLE=1`来指定走RDMA。通常在KV Cache传输场景，为避免与模型集合通信流量冲突，影响推理性能，建议走RDMA传输。

8. 当使用`RDMA`通信协议时，在交换机和网卡默认配置不一致场景/需要流量规划场景下，可能需要修改RDMA网卡的Traffic Class和Service Level配置，通过`ASCEND_RDMA_TC`环境变量来设置Traffic Class, 通过`ASCEND_RDMA_SL`环境变量来设置Service Level。

