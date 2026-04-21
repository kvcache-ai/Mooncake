# coro_rpc客户端介绍

## 基本使用

类`coro_rpc::coro_rpc_client`是coro_rpc的客户端，用户可以通过它向服务器发送rpc请求。

下面我们展示rpc_client的基本使用方法。

```cpp
using namespace async_simple;
using namespace coro_rpc;
int add(int a,int b);
Lazy<void> example() {
  coro_rpc_client client;
  coro_rpc::err_code ec = co_await client.connect("localhost:9001");
  if (ec) { /*判断连接是否出错*/
    std::cout<<ec.message()<<std::endl;
    co_return;
  }
  rpc_result result = co_await client.call<add>(1,2);
  /*rpc_result是一个expected<T,rpc_error>类型，其中T为rpc返回值*/
  if (!result.has_value()) {
    /*调用result.error()获取rpc错误信息*/
    std::cout<<"error code:"<<result.error().val()<< ", error message:"<<result.error().msg()<<std::endl;
    co_return;
  }
  assert(result.value()==3); /*调用result.value()获取rpc返回值*/
}
```

我们可以通过`set_req_attachment`函数设置本次请求的attachment，这是一段不会经过序列化直接发送给客户端的二进制数据。同样的，我们也可以使用`get_resp_attachment()`和`release_resp_attachment()`来获取rpc请求返回的attachment。

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

默认情况下，rpc客户端发送请求/建立连接后会等待5秒，如果5秒后仍未收到响应，则会返回超时错误。
用户也可以通过调用`call_for`函数自定义等待的时长。

```cpp
client.connect("127.0.0.1:9001", std::chrono::seconds{10});
auto result = co_await client.call_for<add>(std::chrono::seconds{10},1,2);
assert(result.value() == 3);
```

时长可以是任一的`std::chrono::duration`类型，常见的如`std::chrono::seconds`,`std::chrono::millseconds`。
特别的，如果时长为0，代表该函数调用永远也不会超时。

## SSL支持

coro_rpc支持使用openssl对连接进行加密。在安装openssl并使用cmake find_package/fetch_content 将yalantinglibs导入到你的工程后，可以打开cmake选项`YLT_ENABLE_SSL=ON`启用ssl支持。或者，你也可以手动添加宏`YLT_ENABLE_SSL`并手动链接openssl。

当启用ssl支持后，用户可以调用`init_ssl`函数，然后再连接到服务器。这会使得客户端与服务器之间建立加密的链接。需要注意的是，coro_rpc服务端在编译时也必须启用ssl支持，并且在启动服务器之前也需要调用`init_ssl`方法来启用SSL支持。

```cpp
client.init_ssl("./","server.crt");
```

第一个字符串代表SSL证书所在的基本路径，第二个字符串代表SSL证书相对于基本路径的相对路径。

当建立连接时，客户端会使用该证书校验服务端发来的证书，以避免中间人攻击。因此，客户端必须持有服务端使用的证书或其根证书。

我们同样支持国密NTLS。你需要开启CMAKE选项`YLT_ENABLE_NTLS`。

## RPC参数的转换与编译期检查

coro_rpc会在调用的时候对参数的合法性做编译期检查，比如，对于如下rpc函数：

```cpp
inline std::string echo(std::string str) { return str; }
```

接下来，当前client调用rpc函数时：

```cpp
client.call<echo>(42);// The argument does not match, a compilation error occurs.
client.call<echo>();// Missing argument, compilation error occurs.
client.call<echo>("", 0);// There are too many arguments, a compilation error occurs.
client.call<echo>("hello, coro_rpc");// The string literal can be converted to std::string, compilation succeeds.
```

## 连接选项

coro_rpc_client提供了`init_config`函数，用于配置连接选项。下面这份代码列出了可配置的选项。这些选项默认均可以不填写。

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
    .connect_timeout_duration = 5s, // 连接的超时时间
    .request_timeout_duration = 5s, // 请求超时时间
    .host = "localhost", // 服务器域名
    .port = "9001", // 服务器端口
    .local_ip = "", // 本地ip，用于指定本地通信的ip地址。
    .socket_config=std::variant<tcp_config,
                 tcp_with_ssl_config,
                 coro_io::ib_socket_t::config_t>{tcp_config{}}; // 指定底层的协议及其底层配置，目前支持tcp, ssl over tcp, rdma三种协议。
  });
}
```


### rdma配置

ibverbs协议的配置如下：
```cpp
struct ib_socket_t::config_t {
  uint32_t cq_size = 128; // 事件通知队列的最大长度
  uint32_t recv_buffer_cnt = 8;                 // 默认提交到接受队列的缓冲数目，一个缓冲区默认256KB。积压的接收数据越多，队列中的缓冲区也会越多，最多可以缓冲max_recv_wr*buffer_size这么多的数据(buffer_size为buffer_pool配置的缓冲区大小），此后如果上层仍不消费数据，则发送端会收到rnr错误，不断重试并等待对端消费。
  uint32_t send_buffer_cnt = 4;                 // 默认的发送缓冲区队列长度上限。代表最多积压的发送缓冲区数目。
  ibv_qp_type qp_type = IBV_QPT_RC;             // 默认的qp类型。
  ibv_qp_cap cap = {.max_send_wr = 32,           // 发送队列的最大长度。
                    .max_recv_wr = 32,          // 接受队列的最大长度
                    .max_send_sge = 3,          // 发送的最大地址分段数，在不启用inline data时只需要1个。使用inline data时数据可能不经过拷贝直接从原始分段地址发送，因此设置为3段（默认支持3段分散地址）。
                    .max_recv_sge = 1,          // 接受的最大地址分段数，目前的缓冲区配置下只需要1个即可。
                    .max_inline_data = 256};    // 如果发送的数据包小于inline data，且底层网卡支持该设置，则小数据包不会被拷贝到缓冲中，而是直接交给网卡发送。
  std::shared_ptr<coro_io::ib_device_t> device; // rpc使用的底层ib网卡。默认选择设备列表第一个网卡。
};
```

可以通过下面的代码简单的启用rdma：

```cpp
  coro_rpc_client cli;
  cli.init_ibv(); //使用默认配置
  cli.init_ibv(ib_socket_t::config_t{}); //使用用户指定的配置
```

也可以在配置中启用rdma：

```cpp
  coro_rpc_client cli;
  cli.init_config(config{.socket_config=ib_socket_t::config_t{}})
```


#### ib_device_t

`ib_device_t`管理了ibverbs传输过程中需要使用到的连接上下文和缓冲区。默认使用全局设备`coro_io::get_global_ib_device()`，用户也可以指定使用自己的设备。

通过修改ib_device_t的配置，可以给rpc连接配置不同的网卡，使用独立的缓冲区。

1. 修改默认的设备配置
```cpp
  // 配置只有在第一次调用时才会生效
  coro_io::get_global_ib_device({ 
    .buffer_pool_config = {
      .buffer_size = 256 * 1024,  // 缓冲区大小
      .max_memory_usage = 4 * 1024 * 1024, // 最大内存使用量（超过此限制将分配失败）
      .memory_usage_recorder = nullptr; // nullopt 表示不同设备的内存占用会被一起统计，如果想要让内存池具有独立的内存占用记录，请分配一个非空的std::shared_ptr<std::atomic<std::size_t>>作为记录
      .idle_timeout = 5s // 空闲时间超过这个时长的缓冲区将被回收
    }
  }); 
  // ...
```

2. 初始化连接时，指定需要使用的 RDMA 网卡
```cpp
  coro_rpc_client cli;
  cli.init_ibv({
    .device = coro_io::get_global_ib_device({.dev_name = "my_rmda_network_device_name"});
  });
```

3. 创建并使用自己的 `ib_device_t`
```cpp
  auto dev = coro_io::ib_device_t::create({
    .dev_name = "",  // 如果 dev_name 为 空，则会使用设备列表中的第一个设备
    .port = 1, // 手动指定网卡port
    .use_best_gid_index = true, // 自动查找该设备最佳的gid_index
    .gid_index = 0, // 手动指定gid_index，当关闭自动查找或自动查找失败时生效
    .buffer_pool_config = {
      // ...
    },
  });
  coro_rpc_client cli;
  cli.init_ibv({
    .device = dev
  });
```

4. 查询当前所有成功注册的全局 RDMA 设备
```cpp
  // 获取所有设备
  auto devices = coro_io::g_ib_device_manager();
  for (auto &dev: devices.get_dev_list()) {
    std::cout<<"name:"<<dev.first;
    // dev.second 是一个全局的 std::shared_ptr<ib_device_t>
  }
```

## 调用模型

每一个`coro_rpc_client`都会绑定到某个IO线程上，默认通过轮转法从全局IO线程池中选择一个连接，用户也可以手动绑定到特定的IO线程上。

```cpp
auto executor=coro_io::get_global_executor();
coro_rpc_client client(executor),client2(executor);
// 两个客户端都被绑定到同一个io线程上
```

每次发起一个基于协程的IO任务（如`connect`,`call`,`send_request`），客户端内部会将IO事件提交给操作系统，当IO事件完成后，再将协程恢复到绑定的IO线程上继续执行。

例如以下代码，调用connect之后任务将切换到IO线程执行。

```cpp
/*run in thread 1*/
coro_rpc_client cli;
co_await cli.connect("localhost:9001");
/*run in thread 2*/
do_something();
```

## 连接池与负载均衡

`coro_io`提供了连接池`client_pool`与负载均衡器`channel`。用户可以通过连接池`client_pool`来管理`coro_rpc`/`coro_http`连接，可以使用`channel`实现多个host之间的负载均衡。具体请见`coro_io`的文档。

## 连接复用

`coro_rpc_client` 可以通过 `send_request`函数实现连接复用。该函数是线程安全的，允许多个线程同时调用同一个client的 `send_request`方法。该函数返回值为`Lazy<Lazy<async_rpc_result<T>>>`.


连接复用允许我们在高并发下减少连接的个数，无需创建新的连接。同时也能提高每个连接的吞吐量。

下面是一段简单的示例代码：

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
std::string_view echo(std::string_view);
Lazy<void> example(coro_rpc_client& client) {
  //向服务器发送请求
  Lazy<async_rpc_result<std::string_view>> handler = co_await client.send_request<echo>("Hello");
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

我们可以多次调用send_request实现连接复用：

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
std::string echo(std::string);
Lazy<void> example(coro_rpc_client& client) {
  std::vector<Lazy<async_rpc_result<std::string>>> handlers;
  // 准备发送10个请求
  for (int i=0;i<10;++i) {
    handlers.push_back(co_await client.send_request<echo>(std::to_string(i)));
  }
  //接下来等待所有的请求返回
  std::vector<async_rpc_result<std::string>> results = co_await collectAll(std::move(handlers));
  for (int i=0;i<10;++i) {
    assert(results[i]->result() == std::to_string(i));
  }
  co_return;
}
```

在使用连接池发送数据时，我们也可以复用连接，此时需要在添加`coro_io_client_reuse_hint`参数，提示连接池启用连接复用。详见连接池文档。（TODO）

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

使用`send_request`方法时，由于可能同时发送多个请求，因此我们不能调用`set_req_attachment`方法向服务器发送attachment，同样也不能调用`get_resp_attachment`和`release_resp_attachment`方法来获取服务器返回的attachment。

我们可以通过调用`send_request_with_attachment`函数，在发送请求时设置attachment。我们也可以通过调用async_rpc_result的`->get_attachment()`方法和`->release_buffer()`方法来获取attachment。

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
int add(int a, int b);
Lazy<std::string> example(coro_rpc_client& client) {
  async_rpc_result<std::string_view> result = co_await co_await client.send_request_with_attachment<add>("Hello", 1, 2);
  assert(result->result() == 3);
  assert(result->get_attachment() == "Hello");
  co_return std::move(result->release_buffer().resp_attachment_buf_);
}
```


### 执行顺序

当调用的rpc函数是协程rpc函数或回调rpc函数时，rpc请求不一定会按顺序执行，服务端可能会同时执行多个rpc请求。

例如，假如有以下代码：

```cpp
using namespace async_simple::coro;
Lazy<void> sleep(int seconds) {
  co_await coro_io::sleep(1s * seconds);  // 在此处让出协程
  co_return;
}
```

服务器注册并启动：
```cpp
using namespace coro_rpc;
void start() {
  coro_rpc_server server(/* thread = */1,/* port = */ 8801);
  server.register_handler<sleep>();
  server.start();
}
```

客户端连续在同一个连接上调用两次sleep函数，第一次sleep2秒，第二次sleep1秒。
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
正常情况下handler3会先返回，随后是handler2，最后是handler1。尽管服务器只有一个IO线程用于执行rpc函数，但协程函数会在调用`coro_io::sleep`时让出该协程，从而保证其他连接不会被阻塞。

### socket延迟发送

当使用连接复用时，可以尝试将选项中的`enable_tcp_no_delay`设为`false`,这允许底层实现将多个小请求打包后一起发送，从而提高吞吐量，但是可能会导致延迟上升。

## 线程安全

对于多个coro_rpc_client实例，它们之间互不干扰，可以分别在不同的线程中安全的调用。

单个coro_rpc_client在多个线程同时调用时需要注意，只有部分成员函数是线程安全的，包括`send_request()`,`close()`,`connect()`,`get_executor()`,`get_pipeline_size()`,`get_client_id()`,`get_config()`等。如果用户没有重新调用connect()函数并传入endpoint或hostname，那么`get_port()`,`get_host()`函数也是线程安全的。

需要注意，`call`,`get_resp_attachment`,`set_req_attachment`,`release_resp_attachment`和`init_config`函数均不是线程安全的，禁止多个线程同时调用。此时只能使用`send_request`实现多个线程并发请求同一个连接。

# coro_rpc 性能测试
先编译coro_rpc 的bench再执行bench命令行，命令行详情：
```
options:
  -u, --url                   url (string [=0.0.0.0:9000])
  -s, --send_data_len         send data length (unsigned long [=8388608])
  -c, --client_concurrency    total number of http clients (unsigned int [=0])
  -m, --max_request_count     max request count (unsigned long [=100000000])
  -p, --port                  server port (unsigned short [=9000])
  -r, --resp_len              response data length (unsigned long [=13])
  -b, --buffer_size           buffer size (unsigned int [=262144])
  -o, --log_level             Severity::INFO 1 as default, WARN is 4 (int [=1])
  -i, --enable_ib             enable ib (bool [=1])
  -d, --duration              duration seconds (unsigned int [=100000])
  -?, --help                  print this message
```

server侧命令行：
`./bench -p 9004 -r 13 -o 5`

含义：启动服务端，response的数据长度为13，日志级别为error(日志级别和easylog的日志级别对应)。

client测命令行：
`./bench -u 0.0.0.0:9004 -c 100 -s 8388608 -o 5 -d 30`

含义：启动100个client去压测，每次请求发送8MB数据，日志级别为error，持续时间30秒。

如果安装了ibverbs环境，则默认会启用ibverbs，如果希望只测试tcp则添加`-i 0`

性能测试完成之后将输出qps，吞吐和latency数据，测试结果类似于：
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