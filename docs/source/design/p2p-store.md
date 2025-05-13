# P2P Store

## Overview
P2P Store is built on [Transfer Engine](transfer-engine.md) and supports the temporary sharing of objects between peer nodes in a cluster, with typical scenarios including checkpoint distribution, etc. P2P Store is a client-only architecture with no centralized master node; global metadata is maintained by the metadata service. P2P Store is now used in Moonshot AI's checkpoint transfer service.

P2P Store provides several interfaces like `Register` and `GetReplica`. A `Register` is equivalent to seeding in BitTorrent, where a local file is registered with the global metadata without any data transfer occurring; it merely registers metadata in etcd. A `GetReplica` searches metadata and clones data from other machines that have called Register or Get (unless explicitly calling `Unregister` or `DeleteReplica` to stop pulling files from the local machine), and it can also act as a data source to improve the efficiency of data transfer for other nodes. This approach can increase the efficiency of large-scale data distribution and avoid the outbound bandwidth saturation.

## P2P Store Sample Program
After compiling P2P Store successfully by following the compilation guide with `cmake .. -DWITH_P2P_STORE=ON && make -j`, a test program `p2p-store-example` will be produced in the `build/mooncake-p2p-store` directory. This tool demonstrates the usage of P2P Store, simulating the process of migrating model data from training nodes to a large number of inference nodes after the training task is completed. Currently, it only supports the RDMA protocol.

1. **Start the `etcd` service.** This is consistent with the method described in Transfer Engine Bench.

2. **Start the simulated training node.** This node will create a simulated model file and make it public within the cluster.
   ```bash
   # This is 10.0.0.2
   export MC_GID_INDEX=n    # NOTE that n is integer
   ./p2p-store-example --cmd=trainer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.2:12345 \
   ```

3. **Start the simulated inference node.** This node will pull data from the simulated training node or other simulated inference nodes.
   ```bash
   # This is 10.0.0.3
   export MC_GID_INDEX=n
   ./p2p-store-example --cmd=inferencer \
                       --metadata_server=10.0.0.1:2379 \
                       --local_server_name=10.0.0.3:12346 \
   ```
   The test is completed with the display of "ALL DONE".

In the above process, the simulated inference nodes search for data sources, which is done by P2P Store, so there is no need for users to provide the IP address of the training node. Similarly, it is necessary to ensure that other nodes can access this machine using the local machine's hostname or the `--local_server_name` filled in during the creation of the node.

## P2P Store API
Mooncake P2P Store currently implements the following interfaces in Golang:

```go
func NewP2PStore(metadataUri string, localSegmentName string) (*P2PStore, error)
```
Creates an instance of `P2PStore`, which internally starts a Transfer Engine service.
- `metadataUri`: The hostname or IP address of the metadata server/etcd service.
- `localSegmentName`: The local server name (hostname/IP address:port), ensuring uniqueness within the cluster.
- Return value: If successful, returns a pointer to the `P2PStore` instance, otherwise returns `error`.

```go
func (store *P2PStore) Close() error
```
Closes the P2PStore instance.

```go
type Buffer struct {
   addr uintptr
   size uint64
}

func (store *P2PStore) Register(ctx context.Context, name string, addrList []uintptr, sizeList []uint64, maxShardSize uint64, location string) error
```
Registers a local file to the cluster, making it downloadable by other peers. Ensure that the data in the specified address range is not modified or unmapped before calling `Unregister`.
- `ctx`: Golang Context reference.
- `name`: The file registration name, ensuring uniqueness within the cluster.
- `addrList` and `sizeList`: These two arrays represent the memory range of the file, with `addrList` indicating the starting address and `sizeList` indicating the corresponding length. The file content corresponds logically to the order in the arrays.
- `maxShardSize`: The internal data sharding granularity, with a recommended value of 64MB.
- `location`: The device name corresponding to this memory segment.

```go
func (store *P2PStore) Unregister(ctx context.Context, name string) error
```
Remove the registration of a local file to the entire cluster. After calling this function, it is safe to modify/delete the memory region reserved for this file.
- `ctx`: Golang Context reference.
- `name`: The file registration name, ensuring uniqueness within the cluster.

```go
type PayloadInfo struct {
   Name         string   // Full name of the Checkpoint file
   MaxShardSize uint64   // The maxShardSize passed into Register
   TotalSize    uint64   // The total length of the sizeList passed into Register
   SizeList     []uint64 // The sizeList passed into Register
}

func (store *P2PStore) List(ctx context.Context, namePrefix string) ([]PayloadInfo, error)
```
Obtains a list of files registered in the cluster, with the ability to filter by file name prefix.
- `ctx`: Golang Context reference.
- `namePrefix`: The file name prefix; if empty, it indicates enumeration of all files.

```go
func (store *P2PStore) GetReplica(ctx context.Context, name string, addrList []uintptr, sizeList []uint64) error
```
Pulls a copy of a file to a specified local memory area, while allowing other nodes to pull the file from this copy. Ensure that the data in the corresponding address range is not modified or unmapped before calling `DeleteReplica`. A file can only be pulled once on the same P2PStore instance.
- `ctx`: Golang Context reference.
- `name`: The file registration name, ensuring uniqueness within the cluster.
- `addrList` and `sizeList`: These two arrays represent the memory range of the file, with `addrList` indicating the starting address and `sizeList` indicating the corresponding length. The file content corresponds logically to the order in the arrays.

```go
func (store *P2PStore) DeleteReplica(ctx context.Context, name string) error
```
Stops other nodes from pulling the file from the local node. After calling this function, it is safe to modify/delete the memory region reserved for this file.
- `ctx`: Golang Context reference.
- `name`: The file registration name, ensuring uniqueness within the cluster.
