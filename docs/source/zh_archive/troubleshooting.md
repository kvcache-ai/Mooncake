# 疑难解答

本文档列举了使用 Mooncake Store 期间容易出现的错误及排查应对措施。

> **容易出错的地方**
> - [ ] `connectable_name` 填写的不是本机的 LAN/WAN 地址：错误示例如填写 Loopback 地址（127.0.0.1/localhost）、填写主机名、填写其他机器的 LAN/WAN 地址等
> - [ ] MTU 和 GID 配置：使用 MC_MTU 和 MC_GID_INDEX 环境变量
> - [ ] RDMA 模式下的设备名称、设备连接状态
> - [ ] etcd 是否正常启动，是否对外开放端口

## 元数据及带外通信
1. 启动时需要按照传入的 `metadata_server` 参数构造 `TransferMetadata` 对象，在程序执行期间，会使用该对象与 etcd 服务器通信，维护连接建立及维持所需的各种内部数据。
2. 启动时需要按照传入的 `connectable_name` 参数和 `rpc_port` 参数向集群注册当前节点，并开启本机 `rpc_port` 参数指定的 TCP 端口供交换元数据使用。其他节点第一次向当前节点发出读写请求前，会使用上述信息，经 DNS 解析后通过 TCP 协议的 `connect()` 方法向其发起连接，连接成功后才传递元数据。

这一部分的报错信息通常显示错误产生于 `mooncake-transfer-engine/src/transfer_metadata.cpp` 文件内。

### 建议排查方向
1. 传入的 `metadata_server` 参数不是一个（或一组）有效的可通达的 etcd 服务端地址。这种情况下会显示 `Error from etcd client` 错误。建议从以下几个方面排查：
    - 刚刚安装 etcd 服务后的默认监听 IP 为 127.0.0.1，这种情况下其他节点不能使用该服务。因此实际监听 IP 需结合网络环境确定。在实验环境中，可使用 0.0.0.0。例如，可使用下列命令行启动合要求的服务：
      ```bash
      # This is 10.0.0.1
      etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379
      ```
      可在其他节点上使用 `curl <metadata_server>` 进行验证。
    - 启动程序前需关闭 HTTP 代理。
      ```bash
      unset http_proxy
      unset https_proxy
      ```

2. 其他节点不能通过传入的 `connectable_name` 参数和 `rpc_port` 参数和当前节点发出 Socket 带外通信，从而实施连接建立操作。这种情况下的错误类型可能有：
    - 启动进程时显示 `bind address already in use` 错误，通常是因为 `rpc_port` 参数对应的端口号被占用，请尝试使用其他端口号；
    - 另一个节点向当前节点发起 Batch Transfer 期间显示 `connection refused` 等类型错误，则需重点检查当前节点创建时这两个参数的正确性。
      - `connectable_name` 必须是当前节点的非 Loopback IP 地址，或者有效的 Hostname（DNS 或 `/etc/hosts` 上有记录），集群内其它节点可使用 `connectable_name` 和 `rpc_port` 等参数连接当前节点。
      - 某些网络中有防火墙机制，需要提前加入白名单。
    - 如果 `local_server_name` 和  `connectable_name`/`rpc_port` 参数的对应关系发生改变并发生各类错误，可尝试清空 etcd 数据库后重启集群。

## RDMA 资源初始化

1. 启动时需要按照传入的 `nic_priority_matrix` 参数，初始化列表中所有 RDMA 网卡对应的设备上下文和其它内部对象；
2. 其他节点第一次向当前节点发出读写请求前，会借助带外通信机制交换 GID、LID、QP Num 等信息，并完成 RDMA 可靠连接通路的建立

这一部分的报错信息通常显示错误产生于 `mooncake-transfer-engine/src/transport/rdma_transport/rdma_*.cpp` 文件内。

### 建议排查方向
1. 显示 `No matched device found` 错误，检查 `nic_priority_matrix` 参数内是否有本机不存在的网卡名称。可使用 `ibv_devinfo` 命令查看本机已安装的网卡列表。
2. 显示 `Device XXX port not active` 错误，表明对应设备的默认 RDMA Port（RDMA 设备端口，需要和 `rpc_port` TCP 端口区分开）没有处于 ACTIVE 状态，这通常是因为 RDMA 网线没有安装好，或者驱动程序没有配置好所致。可使用 `MC_IB_PORT` 环境变量变更默认使用的 RDMA Port。
3. 显示 `Failed to exchange handshake description` 错误，表明通信双方无法建立 RDMA 可靠连接通路。在大多数情况下，通常是因为某一方配置有误，或者双方本身无法通达所致。首先使用 `ib_send_bw` 等工具确认两个节点的可通达性，并留意输出的 GID、LID、MTU 等参数信息，之后对照报错信息分析可能的出错点：
    1. 在启动后输出日志通常包含数行形如 `RDMA device: XXX, LID: XXX, GID: (X) XX:XX:XX:...` 的日志信息。如果显示的 GID 地址全部为 0（括号内表示 GID Index），则需要结合网络环境选择正确的 GID Index，启动时使用使用 `MC_GID_INDEX` 环境变量加以指定。
    2. 显示 `Failed to modify QP to RTR, check mtu, gid, peer lid, peer qp num` 错误，首先需要确定发生错误的是哪一方，没有显示 `Handshake request rejected by peer endpoint: ` 前缀的就表明问题来自显示错误的一方。按照错误提示指引，需要检查 MTU 长度配置（使用 `MC_MTU` 环境变量调节）、自己及对方的 GID 地址是否有效等。同时，如果两个节点无法实现物理连接，也可能会在这一步中断，敬请留意。
    3. 如果显示 `Failed to create QP: Cannot allocate memory` 错误，通常是由于创建的 QP 数量过多，达到了驱动限制。可以使用 `rdma resource` 命令追踪已创建的 QP 数量。解决此问题的一种可能方法：
       - 将 Mooncake 更新到 v0.3.5 或更高版本
       - 在启动应用程序前设置环境变量 `MC_ENABLE_DEST_DEVICE_AFFINITY=1`

## RDMA 传输期间
### 建议排查方向

若网络状态不稳定，部分请求会无法发送到位，显示 `Worker: Process failed for slice` 等错误。Transfer Engine 能通过重选路径等方式规避掉问题。在某些复杂情况下，如果连续输出大量此类错误，建议比照最后一个字段的字符串提示，搜索造成问题的原因。

注意：大多数情况下输出的错误除第一次出现之外，都是 `work request flushed error`。这是因为第一次产生错误时，RDMA 驱动会将连接设置为不可用状态，因此处于提交队列的任务都被阻止执行，并报告后续错误。因此建议定位到第一次发生错误的地方并进行检查。

此外，若出现 `Failed to get description of XXX` 错误，表明用户调用 `openSegment` 接口时输入的 Segment 名称无法在 etcd 数据库中找到。对于内存读写场景，Segment 名称需要严格匹配对方节点初始化时填写的 `local_hostname` 字段。
