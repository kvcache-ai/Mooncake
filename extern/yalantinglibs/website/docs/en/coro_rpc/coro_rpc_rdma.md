coro_rpc, from yalantinglibs, is a high-performance RPC library based on C++20 coroutines. It provides a concise and easy-to-use interface, allowing users to implement RPC communication with just a few lines of code. In addition to TCP, coro_rpc now also supports RDMA communication (ibverbs). Let's use a simple example to get a feel for coro_rpc with RDMA communication.

# example
Start the RPC server:

```cpp
std::string_view echo(std::string str) { return str; }
coro_rpc_server server(/*thread_number*/ std::thread::hardware_concurrency(), /*port*/ 9000);
server.register_handler<echo>();
server.init_ibv(); // Initialize RDMA resources
server.start();
```

The client sends an RPC request:

```cpp
Lazy<void> async_request() {
  coro_rpc_client client{};
  client.init_ibv(); // Initialize RDMA resources
  co_await client.connect("127.0.0.1:9000");
  auto result = co_await client.call<echo>("hello rdma");
  assert(result.value() == "hello rdma");
}

int main() {
  syncAwait(async_request());
}
```

With just a few lines of code, you can set up an RPC server and client based on RDMA communication. If users need to configure more RDMA-related parameters, they can pass a `config` object when calling `init_ibv()`. Various ibverbs-related parameters can be set in this object. For details, see the [documentation](https://alibaba.github.io/yalantinglibs/en/coro_rpc/coro_rpc_client.html#rdma-socket-configuration).

How do you enable TCP communication? Simply don't call `init_ibv()`. TCP is the default communication protocol; RDMA communication is enabled only after `init_ibv()` is called.

# benchmark
We conducted some performance tests on coro_rpc between two hosts in a 180Gb RDMA (RoCE v2) environment. In high-concurrency, small-packet scenarios, the QPS can reach 1.5 million. When sending slightly larger packets (256KB and above), the bandwidth can be easily saturated with fewer than 10 concurrent connections.

| Request Data Size | Concurrency | Throughput (Gb/s) | P90 (us) | P99 (us) | QPS |
| --- | --- | --- | --- | --- | --- |
| 128B | 1 | 0.04 | 24 | 26 | 43,394 |
|  | 4 | 0.15 | 29 | 44 | 149,130 |
|  | 16 | 0.40 | 48 | 61 | 393,404 |
|  | 64 | 0.81 | 100 | 134 | 841,342 |
|  | 256 | 1.47 | 210 | 256 | 1,533,744 |
| 4K | 1 | 1.21 | 35 | 39 | 37,017 |
|  | 4 | 4.50 | 37 | 48 | 137,317 |
|  | 16 | 11.64 | 62 | 74 | 355,264 |
|  | 64 | 24.47 | 112 | 152 | 745,242 |
|  | 256 | 42.36 | 244 | 312 | 1,318,979 |
| 32K | 1 | 8.41 | 39 | 41 | 32,084 |
|  | 4 | 29.91 | 42 | 55 | 114,081 |
|  | 16 | 83.73 | 58 | 93 | 319,392 |
|  | 64 | 148.66 | 146 | 186 | 565,878 |
|  | 256 | 182.74 | 568 | 744 | 697,849 |
| 256K | 1 | 28.59 | 81 | 90 | 13,634 |
|  | 4 | 100.07 | 96 | 113 | 47,718 |
|  | 16 | 182.58 | 210 | 242 | 87,063 |
|  | 64 | 181.70 | 776 | 864 | 87,030 |
|  | 256 | 180.98 | 3072 | 3392 | 88,359 |
| 1M | 1 | 55.08 | 158 | 172 | 6,566 |
|  | 4 | 161.90 | 236 | 254 | 19,299 |
|  | 16 | 183.41 | 832 | 888 | 21,864 |
|  | 64 | 184.29 | 2976 | 3104 | 21,969 |
|  | 256 | 184.90 | 11648 | 11776 | 22,041 |
| 8M | 1 | 78.64 | 840 | 1488 | 1,171 |
|  | 4 | 180.88 | 1536 | 1840 | 2,695 |
|  | 16 | 185.01 | 5888 | 6010 | 2,756 |
|  | 64 | 185.01 | 23296 | 23552 | 2,756 |
|  | 256 | 183.47 | 93184 | 94208 | 2,733 |


The specific benchmark code can be found [here](https://github.com/alibaba/yalantinglibs/blob/main/src/coro_rpc/benchmark/bench.cpp).

## RDMA Performance Optimization

### RDMA Memory Pool

RDMA requests require pre-registering memory for sending and receiving data. In our tests, the overhead of registering RDMA memory is much higher than that of memory copying. A more reasonable approach, compared to registering memory for each send or receive operation, is to use a memory pool to cache already-registered RDMA memory. Each time a request is initiated, data is divided into multiple chunks for receiving/sending. The maximum length of each chunk corresponds to the size of the pre-registered memory blocks. A registered block is retrieved from the pool, and a memory copy is performed between this block and the actual data address.

### RNR and the Receive Buffer Queue
RDMA operates directly on remote memory. If the remote memory is not ready, it triggers an RNR (Receiver Not Ready) error. To handle an RNR error, one must either disconnect or sleep for a period. Clearly, avoiding RNR errors is key to improving RDMA transfer performance and stability.

coro_rpc uses the following strategy to address the RNR issue: For each connection, we prepare a receive buffer queue. This queue contains several memory blocks (e.g., 8 blocks of 256KB by default). Whenever a notification of a completed data transfer is received, a new memory block is immediately added to the buffer queue, and this new block is posted to RDMA's receive queue.

### Send Buffer Queue

In the send path, the most straightforward approach is to first copy data to an RDMA buffer, then post it to the RDMA send queue. After the data is written to the peer, repeat the process for the next block.

This process has two bottlenecks: first, how to parallelize memory copying and network transmission; second, the NIC is idle during the time between when it finishes sending one block and when the CPU posts the next, failing to maximize bandwidth utilization.

To improve sending performance, we introduce the concept of a send buffer. For each I/O operation, instead of waiting for the peer to complete the write, we complete the send operation immediately after posting the memory to the RDMA send queue. This allows the upper-level code to send the next request/data block until the number of outstanding sends reaches the send buffer queue's limit. Only then do we wait for a send request to complete before posting a new memory block to the RDMA send queue.

For large packets, this algorithm allows memory copying and network transmission to occur concurrently. Since multiple blocks are sent simultaneously, the NIC can send another pending block during the time it takes for the application layer to post a new one after the previous one is sent, thus maximizing bandwidth utilization.

### Small Packet Write Coalescing

RDMA throughput is relatively low when sending small packets. For small packet requests, a strategy that improves throughput without introducing extra latency is to coalesce multiple small packets into a larger one. If the application layer posts a send request when the send queue is full, the data is not immediately sent but is temporarily stored in a buffer. If the application then posts another request, we can merge the new data into the same buffer as the previous data, achieving data coalescing for sending.

### Inline Data

Some RDMA NICs can send small packets using the inline data method, which does not require registering RDMA memory and offers better transfer performance. coro_rpc uses this method for packets smaller than 256 bytes if the NIC supports inline data.

### Memory Usage Control

RDMA communication requires managing memory buffers manually. Currently, coro_rpc uses a default memory chunk size of 256KB. The initial size of the receive buffer is 8, and the send buffer limit is 2. Therefore, the memory usage per connection is 10 * 256KB, approximately 2.5MB. Users can control memory usage by adjusting the buffer queue sizes and the buffer block size.

Additionally, the RDMA memory pool also provides a watermark configuration to control the upper limit of memory usage. When the memory pool's watermark is high, a connection attempting to get a new memory block from the pool will fail and be closed.

### Using a Connection Pool

In high-concurrency scenarios, the connection pool provided by coro_rpc can be used to reuse connections, avoiding the overhead of repeated connection creation. Furthermore, since coro_rpc supports connection multiplexing, we can submit multiple small packet requests over a single connection. This enables pipelined sending and leverages the underlying small packet write coalescing to improve throughput.

```cpp
auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
    conf.url, pool_conf);
auto ret = co_await pool->send_request(
    [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
        return client.send_request<echo>("hello");
    });
if (ret.has_value()) {
    auto result = co_await std::move(ret.value());
    if (result.has_value()) {
        assert(result.value()=="hello"); 
    }
}
```