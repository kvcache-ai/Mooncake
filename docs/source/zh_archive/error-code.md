# 错误码说明

## Mooncake TransferEngine

TransferEngine 在执行期间可能会产生各种类型的错误，对于绝大多数接口，返回值会指示错误原因。详细信息请参见 `mooncake-transfer-engine/include/error.h`

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

## Mooncake Store

Mooncake Store 在执行期间可能会产生各种类型的错误，对于绝大多数接口，返回值会指示错误原因。详细信息请参见 `mooncake-store/include/types.h`。

| 分组    | 返回值                                      | 描述               |
| ----- | ---------------------------------------- | ---------------- |
| 正常    | 0                                        | 操作成功             |
| 内部错误  | INTERNAL\_ERROR (-1)                     | 发生内部错误           |
| 缓冲区分配 | BUFFER\_OVERFLOW (-10)                   | 缓冲区空间不足          |
| 分片选择  | SHARD\_INDEX\_OUT\_OF\_RANGE (-100)      | 分片索引超出范围         |
|       | SEGMENT\_NOT\_FOUND (-101)               | 未找到可用的分片         |
|       | SEGMENT\_ALREADY\_EXISTS (-102)          | 分片已存在            |
| 句柄选择  | NO\_AVAILABLE\_HANDLE (-200)             | 由于空间不足，内存分配失败      |
| 版本相关  | INVALID\_VERSION (-300)                  | 无效的版本            |
| 键相关   | INVALID\_KEY (-400)                      | 无效的键             |
| 引擎相关  | WRITE\_FAIL (-500)                       | 写入操作失败           |
| 参数错误  | INVALID\_PARAMS (-600)                   | 参数无效             |
| 引擎操作  | INVALID\_WRITE (-700)                    | 无效的写操作           |
|       | INVALID\_READ (-701)                     | 无效的读操作           |
|       | INVALID\_REPLICA (-702)                  | 无效的副本操作          |
| 对象相关  | REPLICA\_IS\_NOT\_READY (-703)           | 副本尚未就绪           |
|       | OBJECT\_NOT\_FOUND (-704)                | 未找到对象            |
|       | OBJECT\_ALREADY\_EXISTS (-705)           | 对象已存在            |
|       | OBJECT\_HAS\_LEASE (-706)                | 对象存在租约           |
|       | LEASE\_EXPIRED (-707)                    | 数据传输完成前租约已过期   |
| 数据传输  | TRANSFER\_FAIL (-800)                    | 数据传输失败           |
| RPC   | RPC\_FAIL (-900)                         | RPC 操作失败         |
| 高可用相关 | ETCD\_OPERATION\_ERROR (-1000)           | etcd 操作失败        |
|       | ETCD\_KEY\_NOT\_EXIST (-1001)            | etcd 中未找到键       |
|       | ETCD\_TRANSACTION\_FAIL (-1002)          | etcd 事务失败        |
|       | ETCD\_CTX\_CANCELLED (-1003)             | etcd 上下文被取消      |
|       | UNAVAILABLE\_IN\_CURRENT\_STATUS (-1010) | 当前状态下请求不可执行      |
|       | UNAVAILABLE\_IN\_CURRENT\_MODE (-1011)   | 当前模式下请求不可执行      |
| 文件相关  | FILE\_NOT\_FOUND (-1100)                 | 文件未找到            |
|       | FILE\_OPEN\_FAIL (-1101)                 | 打开文件或写入已有文件时发生错误 |
|       | FILE\_READ\_FAIL (-1102)                 | 文件读取失败           |
|       | FILE\_WRITE\_FAIL (-1103)                | 文件写入失败           |
|       | FILE\_INVALID\_BUFFER (-1104)            | 文件缓冲区错误          |
|       | FILE\_LOCK\_FAIL (-1105)                 | 文件加锁失败           |
|       | FILE\_INVALID\_HANDLE (-1106)            | 无效的文件句柄          |
