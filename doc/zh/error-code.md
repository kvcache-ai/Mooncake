# 错误码说明

TransferEngine 在执行期间可能会产生各种类型的错误，对于绝大多数接口，都可以通过接口返回值进行判读。具体参阅 `mooncake-transfer-engine/include/error.h`

| 组别     | 返回值                     | 说明                                                                                               |
| -------- | -------------------------- | -------------------------------------------------------------------------------------------------- |
| 正常     | 0                          | 正常执行                                                                                           |
| 错误参数 | ERR_INVALID_ARGUMENT       | 用户输入参数不正确（且无法细化为该组错误中其余各项）                                               |
|          | ERR_TOO_MANY_REQUESTS      | 用户调用 SubmitTransfer 接口时，传入的 Request 数量超出分配 BatchID 指定的最大值                   |
|          | ERR_ADDRESS_NOT_REGISTERED | 用户发起的 Request 中源地址和/或目标地址未被注册（含已在本地注册但未上传 etcd 元数据服务器的情况） |
|          | ERR_BATCH_BUSY             | 用户尝试对一个正在执行 Request 的 BatchID 进行回收操作                                             |
|          | ERR_DEVICE_NOT_FOUND       | 没有一个可用的 RDMA 设备以执行用户请求的 Request                                                   |
|          | ERR_ADDRESS_OVERLAPPED     | 多次注册重叠的内存区域                                                                             |
| 握手错误 | ERR_DNS                    | 用户输入的 local server name 不是一个有效的 DNS 主机名或 IP 地址，导致其他节点无法与该设备握手     |
|          | ERR_SOCKET                 | 握手期间发生有关 TCP Socket 的错误                                                                 |
|          | ERR_MALFORMED_JSON         | 握手期间交换数据格式错误                                                                           |
|          | ERR_REJECT_HANDSHAKE       | 对方因配置等原因主动拒绝握手                                                                       |
| 其他错误 | ERR_METADATA               | 与 etcd 元数据服务器通信发生异常                                                                   |
|          | ERR_ENDPOINT               | 创建及使用 RdmaEndPoint 对象期间发生异常                                                           |
|          | ERR_NUMA                   | 系统不支持 numa 接口                                                                               |
|          | ERR_CLOCK                  | 系统不支持 clock_gettime 接口                                                                      |
|          | ERR_MEMORY                 | 分配内存已满                                                                                       |
