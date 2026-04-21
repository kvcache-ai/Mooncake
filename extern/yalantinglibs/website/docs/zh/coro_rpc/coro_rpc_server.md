# coro_rpc服务端介绍

## 服务器的注册与启动

### 函数注册
在启动rpc服务器之前，我们需要调用`register_handler<>`函数注册所有的rpc函数。注册不是线程安全的，不能在启动rpc服务器后再注册。

```cpp
void hello();
Lazy<std::string_view> echo(std::string_view);
int add(int a, int b);
int regist_rpc_funtion(coro_rpc_server& server) {
  server.register_handler<hello, echo, add>(); 
}
```

### 启动服务器

我们可以通过调用`.start()`方法，启动一个服务器，这会阻塞当前线程直到服务器退出为止。
```cpp
int start_server() {
  coro_rpc_server server;
  regist_rpc_funtion(server);
  coro_rpc::err_code ec = server.start(); 
  /*block util server down*/
}
```

如果不想阻塞当前线程，我们也允许通过`async_start()`异步启动一个服务器，该函数返回后，保证服务器已经开始监听端口（或发生错误）。用户可以通过检查`async_simple::Future<coro_rpc::error_code>::hasResult()`来判断服务器当前是否启动成功并正常运行。调用`async_simple::Future<coro_rpc::error_code>::get()`方法则可以等待服务器停止。

```cpp
int start_server() {
  coro_rpc_server server;
  regist_rpc_funtion(server);
  async_simple::Future<coro_rpc::err_code> ec = server.async_start();  /*won't block here */
  assert(!ec.hasResult()) /* check if server start success */
  auto err = ec.get(); /*block here util server down then return err code*/
}
```

### 在rpc端口添加http服务或其他服务
我们可以通过调用`add_subserver()`方法，添加一个子服务器。

例如，如果想要同时监听http server，可以调用如下代码。
```cpp
coro_rpc_server server(/*thread=*/std::thread::hardware_concurrency(),
                       /*port=*/8801);
auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
std::function dispatcher = [](coro_io::socket_wrapper_t&& soc,
                              std::string_view magic_number,
                              coro_http::coro_http_server& server) {
  server.transfer_connection(std::move(soc), magic_number);
};
server.add_subserver(std::move(dispatcher), std::move(http_server));
```

`magic_number`是客户端发来的第一个字节，当客户段建立连接发起首次请求时，rpc server会检查第一个字节是否是coro_rpc是魔数`21`,如果魔数不正确则会释放连接并调用用户注册的dispatcher方法。用户应当在dispatcher函数内根据魔数进行分发，将连接转发到正确的服务器。

特别的是，`add_subserver()`方法可以添加多个子服务器, 这允许我们在同一个端口启动多个服务器。

```cpp
coro_rpc_server server(/*thread=*/std::thread::hardware_concurrency(),
                       /*port=*/8801);
auto rest_server = std::make_unique<coro_rest_server>(0, 0);
auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
std::function dispatcher = [](coro_io::socket_wrapper_t&& soc,
                              std::string_view magic_number,
                              coro_http::coro_http_server& server, coro_rest_server& rest_server) {
  if (magic_number == (char)16) { // rest server
    server.transfer_connection(std::move(soc), magic_number);
  }
  else { //http
    rest_server.transfer_connection(std::move(soc), magic_number);
  }
};
server.add_subserver(std::move(dispatcher), std::move(http_server), std::move(rest_server));
```



coro_rpc支持注册并调用的rpc函数有三种：
1. 普通函数
2. 协程函数
3. 回调函数

## 普通rpc函数

如果一个函数既不是协程，同时函数的第一个参数也不是`coro_rpc::context<T>`类型，那么这个rpc函数就是一个普通函数。

例如，以下函数都是普通函数：

```cpp
int add(int a, int b);
std::string_view echo(std::string_view str);
struct dummy {
  std::string_view echo(std::string_view str) { return str; }
};
```

### 调用模型

普通函数一定是同步执行的。当某个连接发来一个普通函数请求时，服务器会在该连接绑定的IO线程上执行该函数，直到函数执行完毕，然后向客户端返回结果，随后才会处理该连接的下一个请求。例如，客户端按顺序发送了两个请求A和B，则我们保证B一定在A之后执行。

需要注意的是，如果在函数内执行长时间的耗时操作，不但会阻塞当前连接，还有可能会阻塞其他被绑定到该IO线程上的连接。因此，在对性能有较高要求的场景中，不应该注册过于耗时的普通函数。可以考虑使用协程函数或回调函数来代替。

### 获取上下文信息

当函数被coro_rpc_server调用时，可以用下面代码来获取连接的上下文信息。

```cpp
using namespace coro_rpc;
void test() {
  context_info_t* ctx = coro_rpc::get_context();
 if (ctx->has_closed()) { //检查连接是否被关闭
    throw std::runtime_error("connection is close!");
  }
  //获取连接ID和请求ID
  ELOGV(INFO, "call function echo_with_attachment, conn ID:%d, request ID:%d",
        ctx->get_connection_id(), ctx->get_request_id());
  //获取客户端的ip和端口与服务端的ip和端口
  ELOGI << "remote endpoint: " << ctx->get_remote_endpoint() << "local endpoint"
        << ctx->get_local_endpoint();
  //获取rpc函数名
  ELOGI << "rpc function name:" << ctx->get_rpc_function_name();
  //获取请求attachment
  std::string_view sv{ctx->get_request_attachment()};
  //释放请求attachment
  std::string str = ctx->release_request_attachment();
  //设置响应attachment
  ctx->set_response_attachment(std::move(str)); 
}
```

An attachment is an additional piece of data that comes with an RPC request. Coro_rpc does not serialize it, allowing users to obtain a view of the attachment that accompanies the request, or to release it from the context and move it separately. Similarly, users can also set the attachment to be sent back to the RPC client.

### 错误处理

我们允许通过抛出`coro_rpc::rpc_error`异常的方式来终止rpc调用，并将rpc错误码和错误信息返回给用户。

```cpp
void rpc() {
  throw coro_rpc::rpc_error{coro_rpc::errc::io_error}; // 返回自定义错误码
  throw coro_rpc::rpc_error{10404}; // 返回自定义错误码
  throw coro_rpc::rpc_error{10404,"404 Not Found"}; // 返回自定义错误码和错误消息
}
```

rpc错误码是一个16位的无符号整数。其中，0-255是保留给rpc框架使用的错误码，用户自定义的错误码可以是[256,65535]之间的任一整数。当rpc返回用户自定义错误码时，连接不会断开。如果返回的是rpc框架自带的错误码，则视为发生了严重的rpc错误，会导致rpc连接断开。


## 协程rpc函数

如果一个rpc函数，其返回值类型是`async_simple::coro::Lazy<T>`，则我们该函数是协程函数。相比普通函数，协程函数是异步的，它可以在等待事件完成的时候暂时让出IO线程，从而提高并发性能。

例如，下面这个rpc函数，通过协程，将重计算任务提交到全局线程池，从而避免阻塞当前I/O线程。

```cpp
using namespace async_simple::coro;
int heavy_calculate(int value);
Lazy<int> calculate(int value) {
  auto val = co_await coro_io::post([value](){return heavy_calculate(value);});  //将任务提交到全局线程池执行，让出当前IO线程，直到任务完成。
  co_return val;
}
```
用户也可以使用`async_simple::Promise<T>`将任务提交到自定义线程池：

```cpp
using namespace async_simple::coro;
void heavy_calculate(int value);
Lazy<int> calculate(int value) {
  async_simple::Promise<int> p;
  std::thread th([&p,value](){
    auto ret = heavy_calculate(value);
    p.setValue(ret); //任务已完成，唤醒rpc函数
  });
  th.detach();
  auto ret = co_await p.get_future(); //等待任务完成
  co_return ret;
}
```

### 调用模型

当某个连接发来一个协程函数请求时，服务器会在该连接绑定的IO线程上启动一个新的协程，在新的协程上执行该函数。当协程函数执行完毕后，根据其返回值将rpc结果返回给客户端。如果协程在执行的过程中暂停让出，则该IO线程就会继续执行其他的任务（如处理下一个请求，又例如处理其他绑定在该IO线程上的连接）。

例如，假定有以下代码：

```cpp
using namespace async_simple::coro;
Lazy<void> sleep(int seconds) {
  co_await coro_io::sleep(1s * seconds);  //在此处让出协程
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

### 获取上下文信息

当协程函数被coro_rpc_server调用时，可以调用`coro_io::get_context_in_coro()`来获取上下文信息。需要注意的是，此时调用`coro_io::get_context()`会获取到错误的上下文信息。

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
Lazy<void> test() {
  context_info_t* ctx = co_await coro_rpc::get_context_in_coro();
}
```

### 错误处理

和普通函数相同，我们可以通过抛出`coro_rpc::rpc_error`异常的方式返回rpc错误，允许自定义rpc错误码和错误信息。


## 回调rpc函数

我们同样支持更为传统的回调函数来实现异步RPC调用。回调函数的写法如下：
```cpp
void echo(coro_rpc::context</* return type = */ std::string_view>, std::string_view param);
```

如果一个函数的返回值是void类型，并且第一个参数是`coro_rpc::context<T>`类型,那么这个函数就是回调函数。`coro_rpc::context<T>`类似于一个智能指针，持有本次rpc调用的回调句柄和上下文信息。

例如，在下面的代码中，我们将`coro_rpc::context<std::string_view>`拷贝到另外一个线程，该线程睡眠30秒后再通过调用`coro_rpc::context<std::string_view>::response_msg()`将结果返回给rpc客户端。

```cpp
using namespace std::chrono;
void echo(coro_rpc::context<std::string_view> ctx, std::string_view param) {
  std::thread th([ctx, param](){
    std::this_thread::sleep_for(30s);
    ctx.response_msg(param);
  });
  return;
}
```

需要说明的是，rpc函数参数中的std::string_view，std::span等视图类型，其指向的内容，会在本次rpc调用的`coro_rpc::context<T>`对象的拷贝副本全部被析构后失效。

### 调用模型

当某个连接发来一个回调函数请求时，分配给该连接的IO线程会立即执行该函数，直到该函数执行完毕。随后处理其他请求。由于回调函数没有返回值，因此rpc函数执行完毕后服务器不会立即回复客户端。

当用户调用回调函数`coro_rpc::context<T>::response_msg()`或`coro_rpc::context<T>::response_error()`后，rpc服务器会接收到通知，此时才会将结果发送给客户端。因此用户必须保证在代码的某个位置主动调用回调函数。


### 获取上下文信息

在回调函数中，我们可以调用`coro_rpc::context<T>::get_context_info()`来获取协程的上下文信息。此外，在rpc函数返回之前也可以使用`coro_io::get_context()`获取上下文信息。但是当rpc函数返回以后，通过`coro_io::get_context()`指向的上下文信息可能会被修改或变得无效，因此我们还是建议使用`coro_rpc::context<T>::get_context_info()`来获取上下文信息。

```cpp
void echo(coro_rpc::context<void> ctx) {
  context_info_t* info = ctx.get_context_info();
  return;
}
```

### 错误处理

在回调函数中，不应该也不能通过抛出异常的形式来返回rpc错误，因为错误可能不会发生在rpc函数的调用栈中。

作为代替，我们可以调用`coro_rpc::context<T>::response_error()`函数来返回rpc错误。

```cpp
void echo(coro_rpc::context<void> ctx) {
  ctx.response_error(10015); //自定义rpc错误码
  ctx.response_error(10015, "my error msg"); //自定义rpc错误码和错误消息
  ctx.response_error(coro_rpc::errc::io_error); //使用rpc框架自带的错误码
  return;
}
```

rpc错误码是一个16位的无符号整数。其中，0-255是保留给rpc框架使用的错误码，用户自定义的错误码可以是[256,65535]之间的任一整数。当rpc返回用户自定义错误码时，连接不会断开。如果返回的是rpc框架自带的错误码，则视为发生了严重的rpc错误，会导致rpc连接断开。


## 完成回调

当服务器成功将rpc响应数据写入socket时，会调用完成回调函数。用户可以设置该回调函数，用于日志，统计。此外，当rpc函数的返回值具有`std::string_view`,`std::span`等类型时，也可以通过该回调函数在合适的时间点析构对象。

该回调函数有两个参数，第一个参数是`const std::error_code&`，代表写入`socket`的结果。第二个参数是`std::size_t` ,代表发送的字节数。需要注意，这里的成功只能代表服务器成功发送数据，不能代表客户端收到了数据。

```cpp
void foo() {
  auto ctx = coro_rpc::get_context();
  ctx->set_complete_handler([](const std::error_code& ec, std::size_t length){
    if (ec) {
      std::cout<<"error: "<<ec.message()<<std::endl;
    }
    else {
      std::cout<<"ok: write "<<length<<" bytes"<<std::endl;
    }
  });
  return;
}
```


## 连接与IO线程

服务器内部有一个IO线程池，其大小默认为cpu的逻辑线程数目。当服务器启动后，它会在某个IO线程上启动一个监听任务，接收客户端发来的连接。每次接收连接时，服务器会通过轮转法，选择一个IO线程将其绑定到连接上。随后，该连接上各请求收发数据，序列化，rpc路由等步骤都会在该IO线程上执行。rpc函数也同样会在该IO线程上执行。

这意味着，如果你的rpc函数会阻塞当前线程（例如线程sleep，同步读写文件），那么最好通过异步化来避免阻塞io线程，从而避免阻塞其他请求。例如，`async_simple::coro`提供了协程锁`Mutex`和`Spinlock`，提供了将异步任务包装为协程任务的`Promise`和`Future`组件。`coro_io`提供了基于协程的异步文件读写，socket的异步读写，`sleep`和定时器`period_timer`，还可通过`coro_io::post`将重CPU任务提交给全局的阻塞任务线程池。`coro_rpc`/`coro_http`提供了基于协程的异步rpc调用和http调用。`easylog`默认会将日志内容提交给后台线程写入，从而保证前台不阻塞。



## 参数与返回值类型

coro_rpc允许用户注册的rpc函数具有多个参数（最多255个），参数和返回值的类型可以是用户自定义的聚合结构体，也支持了各种c++标准库提供的数据结构和许多第三方库提供的数据结构。详见：[struct_pack的类型系统](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_type_system.html)

如果你的rpc参数或返回值类型不属于struct_pack的类型系统支持的类型，我们也允许用户注册自己的结构体或者自定义序列化算法，详见：[自定义功能支持](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_intro.html#%E8%87%AA%E5%AE%9A%E4%B9%89%E7%B1%BB%E5%9E%8B%E7%9A%84%E5%BA%8F%E5%88%97%E5%8C%96)

### 返回值包含视图类型

用户的返回值可能包含了`std::string_view`或`std::span`等视图类型，这些类型能够减少反序列化时的拷贝，从而提升rpc性能。然而，这要求其指向的对象必须在rpc请求成功发送后才能析构。

用户可以利用设置`complete_handler`来保证这一点：

```cpp
std::string_view hello() {
  auto ctx = coro_rpc::get_context();
  auto str=std::make_unique<std::string>("Hello");
  ctx->set_complete_handler([str=std::move(str)](const std::error_code& ec, std::size_t length){
    if (ec) {
      std::cout<<"error: "<<ec.message()<<std::endl;
    }
    else {
      std::cout<<"ok: write "<<length<<" bytes"<<std::endl;
    }
  });
  return *str; /*this is safe*/
}
```

特别的，当返回值的视图来自rpc函数中的参数时，rpc框架保证其一定是合法的，无需额外操作。

```cpp
std::string_view echo(std::string_view sv) {
  return sv;
}
```


### RPC返回值的构造与检查

此外，对于回调函数，coro_rpc会尝试通过参数列表构造返回值类型。如果无法构造则会导致编译失败。

```cpp
void echo(coro_rpc::context<std::string> ctx) {
  ctx.response_msg(); //无法构造std::string。编译失败。
  ctx.response_msg(42); //无法构造std::string。编译失败。
  ctx.response_msg(42,'A'); //可以构造std::string，编译通过。
  ctx.response_msg("Hello"); //可以构造std::string，编译通过。
  return;
}
```

## SSL支持

coro_rpc支持使用openssl对连接进行加密。在安装openssl并使用cmake find_package/fetch_content 将yalantinglibs导入到你的工程后，可以打开cmake选项`YLT_ENABLE_SSL=ON`启用ssl支持。或者，你也可以手动添加宏`YLT_ENABLE_SSL`并手动链接openssl。

当启用ssl支持后，用户可以调用`init_ssl`函数，然后再连接到服务器。这会使得客户端与服务器之间建立加密的链接。需要注意的是，coro_rpc服务端在编译时也必须启用ssl支持。

```cpp
coro_rpc_server server;
server.init_ssl({
  .base_path = "./",           // ssl文件的基本路径
  .cert_file = "server.crt",   // 证书相对于base_path的路径
  .key_file = "server.key"     // 私钥相对于base_path的路径
});
```

启用ssl支持后，服务器将拒绝一切非ssl连接。

我们同样支持国密NTLS。你需要开启CMAKE选项`YLT_ENABLE_NTLS`。

## rdma 支持

coro_rpc支持使用rdma：
```cpp
coro_rpc_server server;
server.init_ibverbs(ib_socket_t::config_t{});
```

启用rdma后，服务器仍然可以接受非rdma请求。

## 高级设置

我们提供了coro_rpc::config_t类，用户可以通过该类型设置server的细节：

```cpp
struct config_base {
  bool is_enable_tcp_no_delay = true; /*tcp请求是否立即响应*/
  uint16_t port = 9001; /*监听端口*/
  unsigned thread_num = std::thread::hardware_concurrency(); /*rpc server内部使用的连接数，默认为逻辑核数*/
  std::chrono::steady_clock::duration conn_timeout_duration = 
      std::chrono::seconds{0};  /*rpc请求的超时时间，0秒代表rpc请求不会自动超时*/
  std::string address="0.0.0.0"; /*监听地址*/
  /*下面设置只有启用SSL才有*/
  std::optional<ssl_configure> ssl_config = std::nullopt; // 配置是否启用ssl
  /*下面设置只有启用rdma才有*/
  std::optional<coro_io::ib_socket_t::config_t> ibv_config = std::nullopt; // 配置是否启用rdma
};
struct ssl_configure {
  std::string base_path;  // ssl文件的基本路径
  std::string cert_file;  // 证书相对于base_path的路径
  std::string key_file;   // 私钥相对于base_path的路径
  std::string dh_file;    // dh_file相对于base_path的路径(可选) 
}
int start() {
  coro_rpc::config_t config{};
  coro_rpc_server server(config);
  /*regist rpc function here... */
  server.start();
}
```


## 特殊rpc函数的注册与调用

### rpc abi变更与兼容性

coro_rpc 底层采用序列化库struct_pack，因此只要rpc参数和返回值的更改，满足struct_pack能向前/向后兼容的约束，那么新老版本的abi就可以相互兼容。你可以在参数和返回值的结构体中添加`struct_pack::compatible<T,VERSION_NUMBER>`字段。其中`struct_pack::compatible<T>`类似于`std::optional<T>`。当客户端的老版本没有该字段时，服务端将得到一个空值。

具体规则详见[struct_pack文档](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_intro.html#%E5%90%91%E5%89%8D-%E5%90%91%E5%90%8E%E5%85%BC%E5%AE%B9%E6%80%A7)。如果abi可能多次变更，建议每次变更都手动指定模板参数中的版本号。

如果参数或者返回值没有结构体？你可以添加新的参数和返回值！同样，请增加若干个`compatible<T>`字段。

例如：
// server 端
```server.cpp
int client_oldapi_server_newapi(int a, struct_pack::compatible<int> b) {
  return a + b.value_or(1);
}
int client_newapi_server_oldapi(int a) { return a; }

std::tuple<int,struct_pack::compatible<int>> client_oldapi_server_newapi_ret() {
    return {42,1};
}
int client_newapi_server_oldapi_ret() {
    return 42;
}

```
// client 端
```client.cpp
int client_oldapi_server_newapi(int a);
int client_newapi_server_oldapi(int a, struct_pack::compatible<int> b);
int client_oldapi_server_newapi_ret();
std::tuple<int,struct_pack::compatible<int>> client_newapi_server_oldapi_ret();
```

我们保证server和client之间可以调用这api变更后的函数正常通信。

特别的，返回值是`void`，我们同样支持，只需要将其升级为`std::tuple<std::monostate,...>` 即可。

例如：
// server 端
```server.cpp
std::tuple<std::monostate,struct_pack::compatible<int>> client_oldapi_server_newapi_ret_void() {
    return {std::monostate{},1};
}
void client_newapi_server_oldapi_ret_void() {
    return;
}

```
// client 端
```client.cpp
void client_oldapi_server_newapi_ret_void();
std::tuple<std::monostate,struct_pack::compatible<int>>  client_newapi_server_oldapi_ret_void();
```


### 成员函数的注册与调用 

coro_rpc支持注册并调用成员函数：

例如，假如有以下函数：

```cpp
struct dummy {
  std::string_view echo(std::string_view str) { return str; }
  Lazy<std::string_view> coroutine_echo(std::string_view str) {co_return str;}
  void callback_echo(coro_rpc::context</*return type = */ std::string_view> ctx, std::string_view str) {
    ctx.response_msg(str);
  }
};
```

则服务端可以这样注册这些函数。

```cpp
#include "rpc_service.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>
int main() {
  coro_rpc_server server;
  dummy d{};
  server.register_handler<&dummy::echo,&dummy::coroutine_echo,&dummy::callback_echo>(&d); // 注册成员函数
  server.start();
}
```

需要注意的时，必须注意被注册的dummy类型的生命周期，保证在服务器启动时dummy始终存活。否则调用行为是未定义的。

客户端可以这样调用这些函数：

```cpp
#include "rpc_service.h"
#include <coro_rpc/coro_rpc_client.hpp>

Lazy<void> test_client() {
  coro_rpc_client client;
  co_await client.connect("localhost", /*port =*/"9000");

  //RPC调用
  {
    auto result = co_await client.call<&dummy::echo>("hello");
    assert(result.value() == "hello");
  }
  {
    auto result = co_await client.call<&dummy::coroutine_echo>("hello");
    assert(result.value() == "hello");
  }
  {
    auto result = co_await client.call<&dummy::callback_echo>("hello");
    assert(result.value() == "hello");
  }
}
```

### 特化的模板函数

coro_rpc允许用户注册并调用特化的模板函数。

例如，假如有以下函数：

```cpp
template<typename T>
T echo(T param) { return param; }
```

则服务端可以这样注册这些函数。

```cpp
#include <ylt/coro_rpc/coro_rpc_server.hpp>
using namespace coro_rpc;
int main() {
  coro_rpc_server server;
  server.register_handler<echo<int>,echo<std::string>,echo<std::vector<int>>>(&d); // 注册特化的模板函数
  server.start();
}
```

客户端可以这样调用：
```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
Lazy<void> rpc_call(coro_rpc_client& cli) {
  assert(co_await cli.call<echo<int>>(42).value() == 42);
  assert(co_await cli.call<echo<std::string>>("Hello").value() == "Hello");
  assert(co_await cli.call<echo<std::vector<int>>>(std::vector{1,2,3}).value() == std::vector{1,2,3});
}
```