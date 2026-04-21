# Introduction to coro_rpc Client

## Base Usage

class `coro_rpc::coro_rpc_client` is the client side of coro_rpc, allowing users to send RPC requests to the server. 

Below, we will demonstrate the basic usage of rpc_client.

```cpp
using namespace async_simple;
using namespace coro_rpc;
int add(int a,int b);
Lazy<void> example() {
  coro_rpc_client client;
  coro_rpc::err_code ec = co_await client.connect("localhost:9001");
  if (ec) { /*check if connection error*/
    std::cout<<ec.message()<<std::endl;
    co_return;
  }
  rpc_result result = co_await client.call<add>(1,2);
  /*rpc_result is type of expected<T,rpc_error>, which T is rpc return type*/
  if (!result.has_value()) {
    /*call result.error() to get rpc error message*/
    std::cout<<"error code:"<<result.error().val()<< ", error message:"<<result.error().msg()<<std::endl;
    co_return;
  }
  assert(result.value()==3); /*call result.value() to  get rpc return type*/
}
```

You can use the `set_req_attachment` function to set the attachment for the current request. This is a piece of binary data that will be sent directly to the client without serialization. Similarly, you can also use `get_resp_attachment()` and `release_resp_attachment()` to retrieve the attachment returned by the RPC request.

```cpp
using namespace async_simple;
using namespace coro_rpc;
void attachment_echo() {
  auto ctx=coro_rpc::get_context();
  ctx->set_resp_attachment(ctx->get_req_attachment());
}
Lazy<std::string> example(coro_rpc_client& client, std::string_view attachment) {
  client.set_req_attachment(attachment);
  rpc_result result = co_await client.call<attachment_echo>();
  if (result.has_value()) {
    assert(result.get_resp_attachment()==attachment);
    co_return std::move(result.release_resp_attachment());
  }
  co_return "";
}
```

By default, the RPC client will wait for 5 seconds after sending a request/establishing a connection. If no response is received after 5 seconds, it will return a timeout error. Users can also customize the wait duration by calling the `call_for` function.

```cpp
client.connect("127.0.0.1:9001", std::chrono::seconds{10});
auto result = co_await client.call_for<add>(std::chrono::seconds{10},1,2);
assert(result.value() == 3);
```

The duration can be any `std::chrono::duration` type, common examples include `std::chrono::seconds` and `std::chrono::milliseconds`. Notably, if the duration is set to zero, it indicates that the function call will never time out.

## SSL support

coro_rpc supports using OpenSSL to encrypt connections. After installing OpenSSL and importing yalantinglibs into your project with CMake's `find_package` or `fetch_content`, you can enable SSL support by setting the CMake option YLT_ENABLE_SSL=ON. Alternatively, you might manually add the YLT_ENABLE_SSL macro and manually link to OpenSSL.

Once SSL support has been enabled, users can invoke the `init_ssl` function before establishing a connection to the server. This will create an encrypted link between the client and the server. It’s important to note that the coro_rpc server must also be compiled with SSL support enabled, and the `init_ssl` method must be called to enable SSL support before starting the server.

```cpp
client.init_ssl("./","server.crt");
```

The first string represents the base path where the SSL certificate is located, the second string represents the relative path of the SSL certificate relative to the base path.

We also support NTLS if you enable it by CMAKE OPTION `YLT_ENABLE_NTLS`.

## Conversion and compile-time checking of RPC parameters

coro_rpc will perform compile-time checks on the validity of arguments during invocation. For example, for the following rpc function:

```cpp
inline std::string echo(std::string str) { return str; }
```

Next, when the current client invokes the rpc function:

```cpp
client.call<echo>(42); // Parameter does not match, compilation error
client.call<echo>(); // Missing parameter, compilation error
client.call<echo>("", 0); // Extra parameters, compilation error
client.call<echo>("hello, coro_rpc"); // The string literal can be converted to std::string, compilation succeeds
```

## Connection Options

The `coro_rpc_client` provides an `init_config` function to configure connection options. The code below lists all configurable options, which are optional with default values.

```cpp
using namespace coro_rpc;
using namespace std::chrono;
void set_config(coro_rpc_client& client) {
  uint64_t client_id;
  std::chrono::milliseconds connect_timeout_duration;
  std::chrono::milliseconds request_timeout_duration;
  std::string host;
  std::string port;
  std::string local_ip;
  client.init_config(config{
    .connect_timeout_duration = 5s, // Connection timeout duration
    .request_timeout_duration = 5s, // Request timeout duration
    .host = "localhost", // Server hostname
    .port = "9001", // Server port
    .local_ip = "", // Local IP address used to specify the local communication interface
    .socket_config=std::variant<tcp_config,
                 tcp_with_ssl_config,
                 coro_io::ib_socket_t::config_t>{tcp_config{}}; // Specify transport protocol and its configuration. Supported protocols: TCP, SSL over TCP, RDMA
  });
}
```

### RDMA Socket Configuration

The configuration for IBVerbs socket protocol is shown below:

```cpp
struct ib_socket_t::config_t {
  uint32_t cq_size = 128; // Maximum length of event notification queue
  uint32_t recv_buffer_cnt = 8;                 // Number of buffers pre-submitted to receive queue. Each buffer defaults to 256KB, so a RDMA connection occupies 8MB memory immediately after establishment. More pending receive data will result in more buffers in the queue, up to max_recv_wr*buffer_size (where buffer_size is configured in buffer_pool). If upper layer doesn't consume data, sender will receive RNR (Receiver Not Ready) errors and retry continuously.
  uint32_t send_buffer_cnt = 4;                 // default send buffer queue max size,
  ibv_qp_type qp_type = IBV_QPT_RC;             // Default QP type
  ibv_qp_cap cap = {.max_send_wr = 32,           // Maximum send queue length
                    .max_recv_wr = 32,          // Maximum receive queue length
                    .max_send_sge = 3,          // Maximum send scatter/gather elements. Only 1 needed without inline data. Use 3 segments when using inline data (default supports 3 scattered addresses)
                    .max_recv_sge = 1,          // Maximum receive scatter/gather elements. 1 suffices with current buffer configuration
                    .max_inline_data = 256};    // If packet size < inline data threshold and NIC supports it, small packets bypass buffer copy and go directly to NIC
  std::shared_ptr<coro_io::ib_device_t> device; // Underlying IB device for RPC. Defaults to first device in list
};
```

Simple RDMA activation examples:

```cpp
  coro_rpc_client cli;
  cli.init_ibv(); // Use default configuration
  cli.init_ibv({.recv_buffer_cnt=8}); // Use custom configuration
```

RDMA activation through config:

```cpp
  coro_rpc_client cli;
  cli.init_config(config{.socket_config=ib_socket_t::config_t{}})
```

#### RDMA Device Configuration

The `ib_device_t` manages the connection context and buffers required during the ibverbs transmission process. By default, it uses the global device `coro_io::get_global_ib_device()`, but users can also specify their own device.

By modifying the configuration of `ib_device_t`, users can assign different network interfaces to RPC connections and use separate buffers.

1. Modify the default device configuration
```cpp
  // The configuration only takes effect on the first invocation
  coro_io::get_global_ib_device({ 
    .buffer_pool_config = {
      .buffer_size = 256 * 1024,  // Buffer size
      .max_memory_usage = 4 * 1024 * 1024, // Max memory usage (allocation fails beyond this limit)
      .memory_usage_recorder = nullptr; // nullopt means that memory usage across different devices will be counted together. If you want the memory pool to have independent memory usage tracking, you should assign a non-null std::shared_ptr<std::atomic<std::size_t>> as the recorder.
      .idle_timeout = 5s // Buffers unused for this duration will be reclaimed
    }
  }); 
  // ...
```

2. Specify the RDMA NIC to use when initializing the connection
```cpp
  coro_rpc_client cli;
  cli.init_ibv({
    .device = coro_io::get_global_ib_device({.dev_name = "my_rmda_network_device_name"});
  });
```

3. Create and use your own `ib_device_t`
```cpp
  auto dev = coro_io::ib_device_t::create({
      .dev_name = "",  // If dev_name is empty, the first device in the device list will be used.
      .port = 1,       // Manually specify the NIC port number.
      .use_best_gid_index = true,  // Automatically find the best GID index for this device.
      .gid_index = 0,   // Manually specify the GID index; takes effect when automatic lookup is disabled or fails.
      .buffer_pool_config = {
        // ...
      }
  });
```

4. Query all currently successfully registered RDMA global devices
```cpp
  // Get all devices
  auto devices = coro_io::g_ib_device_manager();
  for (auto &dev: devices.get_dev_list()) {
    std::cout << "name:" << dev.first;
    // dev.second is a global std::shared_ptr<ib_device_t>
  }
```

## Calling Model

Each `coro_rpc_client` is bound to a specific IO thread. By default, it selects a connection via round-robin from the global IO thread pool. Users can also manually bind it to a specific IO thread.

```cpp
auto executor=coro_io::get_global_executor();
coro_rpc_client client(executor),client2(executor);
// Both clients are bound to the same IO thread.
```

Each time a coroutine-based IO task is initiated (such as `connect`, `call`, `send_request`), the client internally submits the IO event to the operating system. When the IO event is completed, the coroutine is then resumed on the bound IO thread to continue execution. For example, in the following code, the task switches to the IO thread for execution after calling connect.

```cpp
/*run in thread 1*/
coro_rpc_client cli;
co_await cli.connect("localhost:9001");
/*run in thread 2*/
do_something();
```

## Connection Pool and Load Balancing

`coro_io` offers a connection pool `client_pool` and a load balancer `channel`. Users can manage `coro_rpc`/`coro_http` connections through the `client_pool`, and can use `channel` to achieve load balancing among multiple hosts. For more details, please refer to the documentation of `coro_io`.

## Connection Reuse

The `coro_rpc_client` can achieve connection reuse through the `send_request` function. This function is thread-safe, allowing multiple threads to call the `send_request` method on the same client concurrently. The return value of the function is `Lazy<Lazy<async_rpc_result<T>>>`. The first `co_await` waits for the request to be sent, and the second `co_await` waits for the rpc result to return.


Connection reuse allows us to reduce the number of connections under high concurrency, eliminating the need to create new connections. It also improves the throughput of each connection.

Here's a simple example code snippet:

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
std::string_view echo(std::string_view);
Lazy<void> example(coro_rpc_client& client) {
  // send request to the server 
  Lazy<async_rpc_result<std::string_view>> handler = co_await client.send_request<echo>("Hello");
  // then wait server response
  async_rpc_result<std::string_view> result = co_await std::move(handler);
  if (result) {
    assert(result->result() == "Hello");
  }
  else {
    // error handle
    std::cout<<result.error().msg()<<std::endl;
  }
}
```

We can call send_request multiple times to implement connection reuse:

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
std::string echo(std::string);
Lazy<void> example(coro_rpc_client& client) {
  std::vector<Lazy<async_rpc_result<std::string>>> handlers;
  // First, send 10 requests consecutively
  for (int i=0;i<10;++i) {
    handlers.push_back(co_await client.send_request<echo>(std::to_string(i)));
  }
  // Next, wait for all the requests to return
  std::vector<async_rpc_result<std::string>> results = co_await collectAll(std::move(handlers));
  for (int i=0;i<10;++i) {
    assert(results[i]->result() == std::to_string(i));
  }
  co_return;
}
```

When sending data using the connection pool, we can also reuse a connection. To do this, you need to add the `coro_io::client_reuse_hint` parameter to instruct the connection pool to enable connection reuse. For more details, see the connection pool documentation. (TODO)

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

### Attachment

When using the `send_request` method, since multiple requests might be sent simultaneously, we should not call the `set_req_attachment` method to send an attachment to the server, nor should we call the `get_resp_attachment` and `release_resp_attachment` methods to get the attachment returned by the server.

Instead, we can set the attachment when sending a request by calling the `send_request_with_attachment` function. Additionally, we can retrieve the attachment by calling the `->get_attachment()` and `->release_buffer()` methods of `async_rpc_result`.

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
int add(int a, int b);
Lazy<std::string> example(coro_rpc_client& client) {
  auto handler = co_await client.send_request_with_attachment<add>("Hello", 1, 2);
  async_rpc_result<std::string_view> result = co_await std::move(handler);
  assert(result->result() == 3);
  assert(result->get_attachment() == "Hello");
  co_return std::move(result->release_buffer().resp_attachment_buf_);
}
```


### Execution order

When the called rpc function is a coroutine rpc function or a callback rpc function, the rpc requests may not necessarily be executed in order. The server might execute multiple rpc requests simultaneously.
For example, suppose there is the following code:

```cpp
using namespace async_simple::coro;
Lazy<void> sleep(int seconds) {
  co_await coro_io::sleep(1s * seconds);  // Yield the coroutine here
  co_return;
}
```

Server registration and startup:
```cpp
using namespace coro_rpc;
void start() {
  coro_rpc_server server(/* thread = */1,/* port = */ 8801);
  server.register_handler<sleep>();
  server.start();
}
```

The client consecutively calls the sleep function twice on the same connection, sleeping for 2 seconds the first time and 1 second the second time.
```cpp
using namespace async_simple::coro;
using namespace coro_rpc;
Lazy<void> call() {
  coro_rpc_client cli,cli2;
  co_await cli.connect("localhost:8801");
  co_await cli2.connect("localhost:8801");
  auto handler1 = co_await cli.send_request<sleep>(2);
  auto handler2 = co_await cli.send_request<sleep>(1);
  auto handler3 = co_await cli2.send_request<sleep>(0);
  handler2.start([](auto&&){
    std::cout<<"handler2 return"<<std::endl;
  });
  handler3.start([](auto&&){
   d::cout<<"handler3 return"<<std::endl;
  });
  co_await handler1;
  std::cout<<"handler1 return"<<std::endl;
}
```
Under normal circumstances, handler3 will return first, followed by handler2, and finally handler1. Although the server only has one IO thread for executing rpc functions, the coroutine function will yield the coroutine when calling `coro_io::sleep`, thus ensuring that other connections will not be blocked.

### Socket Delayed Sending

When using connection reuse, you can try setting the option `enable_tcp_no_delay` to `false`. This allows the underlying implementation to batch multiple small requests together for sending, thereby increasing throughput, but it may lead to increased latency.

## Thread-safe

For multiple coro_rpc_client instances, they do not interfere with each other and can be safely called in different threads respectively.

When calling a single `coro_rpc_client` simultaneously in multiple threads, it is necessary to note that only some member functions are thread-safe, including `send_request()`, `close()`, `connect()`, `get_executor()`, `get_pipeline_size()`, `get_client_id()`, `get_config()`, etc. If the user has not called the `connect()` function again with an endpoint or hostname, then the `get_port()` and `get_host()` functions are also thread-safe.

It is important to note that the `call`, `get_resp_attachment`, `set_req_attachment`, `release_resp_attachment`, and `init_config` functions are not thread-safe and must not be called by multiple threads simultaneously. In this case, only `send_request` can be used for multiple threads to make concurrent requests over a single connection.

# coro_rpc Performance Testing
First, compile the coro_rpc benchmark and then execute the `bench` command-line tool. The command-line options are as follows:
```
options:
  -u, --url                   URL (string [=0.0.0.0:9000])
  -s, --send_data_len         Send data length (unsigned long [=8388608])
  -c, --client_concurrency    Total number of clients (unsigned int [=0])
  -m, --max_request_count     Max request count (unsigned long [=100000000])
  -p, --port                  Server port (unsigned short [=9000])
  -r, --resp_len              Response data length (unsigned long [=13])
  -b, --buffer_size           Buffer size (unsigned int [=262144])
  -o, --log_level             Log level. Severity::INFO=1 (default), WARN is 4, etc. (int [=1])
  -i, --enable_ib             Enable ibverbs (RDMA) (bool [=1])
  -d, --duration              Duration in seconds (unsigned int [=100000])
  -?, --help                  Print this message
```

Server-side command:
`./bench -p 9004 -r 13 -o 5`

Meaning: Start the server, set the response data length to 13, and set the log level to error (the log levels correspond to those in easylog).

Client-side command:
`./bench -u 0.0.0.0:9004 -c 100 -s 8388608 -o 5 -d 30`

Meaning: Start 100 clients for stress testing, sending 8MB of data per request, with the log level set to error, and a duration of 30 seconds.

If user install ibverbs environment, ibverbs is enabled default. To test only TCP, add the flag `-i 0`.

After the performance test completes, it will output QPS, throughput, and latency data. The test results will look similar to this:
```
# Benchmark result
avg qps 2757 and throughput:185.04 Gb/s in duration 30
# HELP Latency(us) of rpc call help
# TYPE Latency(us) of rpc call summary
rpc call latency(us){quantile="0.500000"} 3616.000000
rpc call latency(us){quantile="0.900000"} 3712.000000
rpc call latency(us){quantile="0.950000"} 3776.000000
rpc call latency(us){quantile="0.990000"} 3872.000000
rpc call latency(us)_sum 298707968.000000
rpc call latency(us)_count 82761
```