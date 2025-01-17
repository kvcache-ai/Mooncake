# P2P Store

## 概述
P2P Store 基于 [Transfer Engine](transfer-engine.md) 构建，支持在集群中的对等节点之间临时共享对象，典型的场景包括 Checkpoint 分发等。P2P Store 是纯客户端架构，没有统一的 Master 节点，全局元数据由 etcd 服务维护。P2P Store 现已用于 Moonshot AI 的检查点传输服务。

P2P Store 提供的是类似 Register 和 GetReplica 的接口。Register 相当于 BT 中的做种将本地某个文件注册到全局元数据中去，此时并不会发生任何的数据传输，仅仅是登记一个元数据；GetReplica 接口会检索元数据，并从曾调用 Register 或 Get 的其他机器中克隆数据（除非显式调用 Unregister 或 DeleteReplica 停止从本地拉取文件），自身也可作为数据源提高其他节点传输数据的效率。这样做可以提高大规模数据分发效率，避免单机出口带宽饱和影响分发效率。

## P2P Store 演示程序
按照编译指南的说明，执行 `cmake .. -DWITH_P2P_STORE=ON && make -j` 编译 P2P Store 成功后，可在 `build/mooncake-p2p-store` 目录下产生测试程序 `p2p-store-example`。该工具演示了 P2P Store 的使用方法，模拟了训练节点完成训练任务后，将模型数据迁移到大量推理节点的过程。目前仅支持 RDMA 协议。

1. **启动 `etcd` 服务。** 这与 Transfer Engine Bench 所述的方法是一致的。
   
2. **启动模拟训练节点。** 该节点将创建模拟模型文件，并向集群内公开。
   ```bash
   # This is 10.0.0.2
   export MC_GID_INDEX=n # 应填入一个数字
   ./p2p-store-example --cmd=trainer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.2:12345 \
   ```

3. **启动模拟推理节点。** 该节点会从模拟训练节点或其他模拟推理节点拉取数据。
   ```bash
   # This is 10.0.0.3
   export MC_GID_INDEX=n
   ./p2p-store-example --cmd=inferencer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.3:12346 \
   ```
   测试完毕显示“ALL DONE”。

上述过程中，模拟推理节点检索数据来源由 P2P Store 内部逻辑实现，因此不需要用户提供训练节点的 IP。同样地，需要保证其他节点可使用本机主机名 `hostname(2)` 或创建节点期间填充的 `--local_server_name` 来访问这台机器。

### 运行示例及结果判读

下面的视频显示了按上述操作正常运行的过程，其中右侧是 Trainer，左侧是 Inferencer。传输数据量为 2 GiB。Trainer 首先使用 0.41s 完成 Register 操作，数据对集群所有节点可见；Inferencer 使用 0.64s 完成 GetReplica 操作，此时数据即可直接访问利用。

![p2p-store](../../image/p2p-store.gif)

> 如果在执行期间发生异常，大多数情况是参数设置不正确所致，建议参考[故障排除文档](troubleshooting.md)先行排查。

## P2P Store API

P2P Store 基于 [Transfer Engine](transfer-engine.md) 构建，支持在集群中的对等节点之间临时共享对象，典型的场景包括 Checkpoint 分发等。P2P Store 是纯客户端架构，没有统一的 Master 节点，全局元数据由 etcd 服务维护。P2P Store 现已用于 Moonshot AI 的检查点传输服务。

Mooncake P2P Store 目前基于 Golang 实现了下列接口：

```go
func NewP2PStore(metadataUri string, localSegmentName string) (*P2PStore, error)
```
创建 P2PStore 实例，该实例内部会启动一个 Transfer Engine 服务。
- `metadataUri`：元数据服务器/etcd服务所在主机名或 IP 地址。
- `localSegmentName`：本地的服务器名称（主机名/IP地址：端口号），保证在集群内唯一。
- 返回值：若成功则返回 `P2PStore` 实例指针，否则返回 `error`。

```go
func (store *P2PStore) Close() error
```
关闭 P2PStore 实例。

```go
type Buffer struct {
   addr uintptr
   size uint64
}

func (store *P2PStore) Register(ctx context.Context, name string, addrList []uintptr, sizeList []uint64, maxShardSize uint64, location string) error
```
注册本地某个文件到整个集群，使得整个集群可下载此文件。在调用 Unregister 之前需保证对应地址区间的数据不被修改或 unmap。
- `ctx`：Golang Context 引用。
- `name`：文件注册名，保证在集群内唯一。
- `addrList` 和 `sizeList`：这两个数组分别表示文件的内存范围，`addrList` 表示起始地址，`sizeList` 表示对应的长度。文件内容在逻辑上与数组中的顺序相对应。
- `maxShardSize`：内部数据切分粒度，推荐值 64MB。
- `location`：这一段内存对应的设备名称，与 `nicPriorityMatrix` 匹配。


```go
func (store *P2PStore) Unregister(ctx context.Context, name string) error
```
停止本地某个文件注册到整个集群。
- `ctx`：Golang Context 引用。
- `name`：文件注册名，保证在集群内唯一。

```go
type PayloadInfo struct {
   Name         string   // Checkpoint 文件的完整名称
   MaxShardSize uint64   // Register 传入的 maxShardSize
   TotalSize    uint64   // Register 传入的 sizeList 累加起来的总长度
   SizeList     []uint64 // Register 传入的 sizeList
}

func (store *P2PStore) List(ctx context.Context, namePrefix string) ([]PayloadInfo, error)
```
获取集群中已注册的文件列表，可使用文件名前缀进行过滤。
- `ctx`：Golang Context 引用。
- `namePrefix`：文件名前缀，若空则表示枚举所有文件。


```go
func (store *P2PStore) GetReplica(ctx context.Context, name string, addrList []uintptr, sizeList []uint64) error
```
拉取文件的一个副本到指定的本地内存区域，同时允许其他节点以此副本为来源拉取文件。在调用 DeleteReplica 之前需保证对应地址区间的数据不被修改或 unmap。一个文件在同一个 P2PStore 实例上只能拉取一次。
- `ctx`：Golang Context 引用。
- `name`：文件注册名，保证在集群内唯一。
- `addrList` 和 `sizeList`：这两个数组分别表示文件的内存范围，`addrList` 表示起始地址，`sizeList` 表示对应的长度。文件内容在逻辑上与数组中的顺序相对应。

```go
func (store *P2PStore) DeleteReplica(ctx context.Context, name string) error
```
停止其他节点从本地拉取文件。
- `ctx`：Golang Context 引用。
- `name`：文件注册名，保证在集群内唯一。
