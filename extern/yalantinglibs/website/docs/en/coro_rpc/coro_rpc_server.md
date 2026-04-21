# coro_rpc Server Introduction

## Server registration and startup

### Function registration

Before starting the RPC server, we need to call the `register_handler<>` function to register all RPC functions. Registration is not thread-safe and cannot be done after the RPC server has started.

```cpp
void hello();
Lazy<std::string_view> echo(std::string_view);
int add(int a, int b);
int regist_rpc_funtion(coro_rpc_server& server) {
  server.register_handler<hello, echo, add>(); 
}
```

### Start the server

We can start a server by calling the `.start()` method, which will block the current thread until the server exits.

```cpp
int start_server() {
  coro_rpc_server server;
  regist_rpc_funtion(server);
  coro_rpc::err_code ec = server.start(); 
  /*block util server down*/
}
```

If you do not want to block the current thread, we also allow asynchronously starting a server by calling `async_start()`. After this function returns, it ensures that the server has already started listening on the port (or an error has occurred). Users can check `async_simple::Future<coro_rpc::error_code>::hasResult()` to determine whether the server is currently started successfully and running normally. Calling the `async_simple::Future<coro_rpc::error_code>::get()` method can then be used to wait for the server to stop.

```cpp
int start_server() {
  coro_rpc_server server;
  regist_rpc_funtion(server);
  async_simple::Future<coro_rpc::err_code> ec = server.async_start();  /*won't block here */
  assert(!ec.hasResult()) /* check if server start success */
  auto err = ec.get(); /*block here util server down then return err code*/
}
```

### Add HTTP Service or Other Services on the RPC Port
We can add a sub-server by calling the `add_subserver()` method.

For example, if you want to listen to an HTTP server at the same time, you can use the following code:
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

`magic_number` is the first byte sent by the client. When the client establishes a connection and initiates the first request, the RPC server checks whether the first byte matches the coro_rpc magic number `21`. If the magic number is incorrect, the connection will be released, and the user-registered dispatcher method will be invoked. The user should dispatch based on the magic number within the dispatcher function and forward the connection to the correct server.

Notably, the `add_subserver()` method allows adding multiple sub-servers, enabling us to start multiple servers on the same port.

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

coro_rpc supports the registration and calling of three types of RPC functions:

1. Ordinary RPC Functions
2. Coroutine RPC Functions
3. Callback RPC Functions

## Ordinary RPC Functions

If a function is neither a coroutine nor its first parameter is of type `coro_rpc::context<T>`, then this RPC function is an ordinary function.
For example, the following functions are ordinary functions:


```cpp
int add(int a, int b);
std::string_view echo(std::string_view str);
struct dummy {
  std::string_view echo(std::string_view str) { return str; }
};
```

### Calling model

Synchronous execution is a definite characteristic of ordinary functions. When a connection submits a request for an ordinary function, the server will execute that function on the I/O thread associated with that connection, and it will continue to do so until the function has been completed. Only then will the result be sent back to the client, and subsequent requests from that connection will be addressed. For instance, if a client sends two requests, A and B, in sequence, we guarantee that B will be executed only after A has finished.

It's important to note that performing time-consuming operations within a function can not only block the current connection but may also impede other connections that are bound to the same I/O thread. Therefore, in scenarios where performance is of high concern, one should not register ordinary functions that are too taxing. Instead, one might want to consider the use of coroutine functions or callback functions as an alternative.

### Retrieving Context Information

When a function is called by coro_rpc_server, the following code can be used to obtain context information about the connection.

```cpp
using namespace coro_rpc;
void test() {
  context_info_t* ctx = coro_rpc::get_context();
  if (ctx->has_closed()) { // Check if the connection has been closed
    throw std::runtime_error("Connection is closed!");
  }
  // Retrieve the connection ID and request ID
  ELOGV(INFO, "Call function echo_with_attachment, connection ID: %d, request ID: %d",
        ctx->get_connection_id(), ctx->get_request_id());
  // Obtain the client's IP and port as well as the server's IP and port
  ELOGI << "Remote endpoint: " << ctx->get_remote_endpoint() << ", local endpoint: "
        << ctx->get_local_endpoint();
  // Get the name of the RPC function
  ELOGI << "RPC function name: " << ctx->get_rpc_function_name();
  // Get the request attachment
  std::string_view sv{ctx->get_request_attachment()};
  // Release the request attachment
  std::string str = ctx->release_request_attachment();
  // Set the response attachment
  ctx->set_response_attachment(std::move(str)); 
}
```

An attachment is an additional piece of data that accompanies an RPC request. coro_rpc does not serialize it. Users can obtain a view of the request's accompanying attachment or release it from the context for separate manipulation. Similarly, users can also set the attachment to be sent back to the RPC client in the response.

### Error Handling

We allow the termination of an RPC call and the return of RPC error codes and error messages to the user by throwing a `coro_rpc::rpc_error` exception.

```cpp
void rpc() {
  throw coro_rpc::rpc_error{coro_rpc::errc::io_error}; // Return a custom error code
  throw coro_rpc::rpc_error{10404}; // Return a custom error code
  throw coro_rpc::rpc_error{10404, "404 Not Found"}; // Return a custom error code and error message
}
```

An RPC error code is a 16-bit unsigned integer. The range 0-255 is reserved for error codes used by the RPC framework itself, whereas user-defined error codes can be any integer within [256, 65535]. When an RPC returns a user-defined error code, the connection will not be closed. However, if the returned error code is one reserved by the RPC framework and indicates a severe RPC error, it will result in the disconnection of the RPC link.


## Coroutine RPC Functions

If an RPC function has a return type of `async_simple::coro::Lazy<T>`, then it's considered a coroutine function. Compared to ordinary functions, coroutine functions are asynchronous, which means they can yield the I/O thread while waiting for events to complete, thus improving concurrency performance.

For instance, the following RPC function uses a coroutine to submit a heavy computation task to the global thread pool, thereby avoiding blocking the current I/O thread.

```cpp
using namespace async_simple::coro;
int heavy_calculate(int value);
Lazy<int> calculate(int value) {
  auto val = co_await coro_io::post([value](){return heavy_calculate(value);});  //将任务提交到全局线程池执行，让出当前IO线程，直到任务完成。
  co_return val;
}
```

Users can also use `async_simple::Promise<T>` to submit tasks to a custom thread pool:

```cpp
using namespace async_simple::coro;
void heavy_calculate(int value);
Lazy<int> calculate(int value) {
  async_simple::Promise<int> p;
  std::thread th([&p,value](){
    auto ret = heavy_calculate(value);
    p.setValue(ret); // Task completed, wake up the RPC function
  });
  th.detach();
  auto ret = co_await p.get_future(); // Wait for the task to complete
  co_return ret;
}
```

### Calling Model

When a connection submits a coroutine function request, the server will start a new coroutine on the I/O thread that the connection is bound to and execute the function within this new coroutine. Once the coroutine function completes, the server will send the RPC result back to the client based on its return value. If the coroutine yields during execution, the I/O thread will continue to execute other tasks, such as handling the next request or managing other connections bound to the same I/O thread.

For example, consider the following code:

```cpp
using namespace async_simple::coro;
Lazy<void> sleep(int seconds) {
  co_await coro_io::sleep(1s * seconds);  // Yield the coroutine here
  co_return;
}
```

Then the server register and start:
```cpp
using namespace coro_rpc;
void start() {
  coro_rpc_server server(/* thread = */1,/* port = */ 8801);
  server.register_handler<sleep>();
  server.start();
}
```

The client invokes the sleep function twice consecutively on the same connection, sleeping for 2 seconds the first time and 1 second the second time.

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
Under normal circumstances, handler3 will return first, followed by handler2, and then handler1. Although the server has only one I/O thread for executing RPC functions, the coroutine function will yield the current coroutine when calling `coro_io::sleep`, thus ensuring other connections are not blocked.

### Obtaining Context Information

When a coroutine function is called by the coro_rpc_server, it can call `coro_io::get_context_in_coro()` to obtain context information. It is important to note that calling `coro_io::get_context()` at this time will result in incorrect context information being obtained.

```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
Lazy<void> test() {
  context_info_t* ctx = co_await coro_rpc::get_context_in_coro();
}
```

### Error Handling

Similar to regular functions, we can return RPC errors by throwing the `coro_rpc::rpc_error` exception, allowing for customized RPC error codes and messages.


## Callback RPC Functions

We also support the more traditional callback functions to implement asynchronous RPC calls. The syntax for a callback function is as follows:
```cpp
void echo(coro_rpc::context</* return type = */ std::string_view>, std::string_view param);
```

If a function's return type is `void` and the first parameter is of type `coro_rpc::context<T>`, then this function is a callback function. The `coro_rpc::context<T>` is similar to a smart pointer, holding the callback handle and context information for this RPC call.

For example, in the code below, we copy `coro_rpc::context<std::string_view>` to another thread, which then sleeps for 30 seconds before returning the result to the RPC client by calling `coro_rpc::context<std::string_view>::response_msg()`.

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

It should be noted that view types in the RPC function parameters, such as std::string_view and std::span, will have their underlying data become invalid after all copies of the `coro_rpc::context<T>` object for this RPC call are destructed.

### Calling Model

When a connection receives a request for a callback function, the I/O thread allocated to that connection will immediately execute the function until it is completed, after which other requests will be processed. Since callback functions do not have return values, the server does not immediately reply to the client after the RPC function is executed.

When the user calls `coro_rpc::context<T>::response_msg` or `coro_rpc::context<T>::response_error`, the RPC server will be notified, and only then will the result be sent to the client. Therefore, users must ensure that they actively call one of the callback functions at some point in their code.


### Obtaining Context Information

In callback functions, we can call `coro_rpc::context<T>::get_context_info()` to obtain the coroutine's context information. Additionally, `coro_io::get_context()` can be used to obtain context information before the RPC function returns. However, after the RPC function has returned, the context information pointed to by `coro_io::get_context()` may be modified or invalid. Therefore, it's recommended to use `coro_rpc::context<T>::get_context_info()` to obtain context information.

```cpp
void echo(coro_rpc::context<void> ctx) {
  context_info_t* info = ctx.get_context_info();
  return;
}
```

### Error Handling

In callback functions, one should not and cannot return RPC errors by throwing exceptions, because the error might not occur within the call stack of the RPC function. Instead, we can use the `coro_rpc::context<T>::response_error()` function to return RPC errors.

```cpp
void echo(coro_rpc::context<void> ctx) {
  ctx.response_error(10015); // Custom RPC error code
  ctx.response_error(10015, "my error msg"); // Custom RPC error code and error message
  ctx.response_error(coro_rpc::errc::io_error); // Using the built-in error code of the RPC framework
  return;
}
```

The RPC error code is a 16-bit unsigned integer. The range 0-255 is reserved for error codes used by the RPC framework, and user-defined error codes can be any integer between [256, 65535]. When an RPC returns a user-defined error code, the connection will not be terminated. However, if an error code from the RPC framework is returned, it is considered a serious RPC error, leading to the disconnection of the RPC link.

### Complete Handler

When the server successfully writes the RPC response data to the socket, the response callback function is invoked. Users can set this callback function for logging, statistics, etc. Moreover, when the return value of the RPC function includes types like `std::string_view` or `std::span`, this callback function can be used to destruct objects at an appropriate time.

The callback function takes two parameters: the first is `const std::error_code&`, which represents the result of writing to the socket. The second is `std::size_t`, which represents the number of bytes sent. Note that a success here only indicates that the server successfully sent the data; it does not guarantee that the client has received it.

```cpp
void foo() {
  auto ctx = coro_rpc::get_context();
  ctx->set_complete_handler([](const std::error_code& ec, std::size_t length) {
    if (ec) {
      std::cout << "error: " << ec.message() << std::endl;
    } else {
      std::cout << "ok: wrote " << length << " bytes" << std::endl;
    }
  });
  return;
}
```

## Connections and I/O Threads

The server internally has an I/O thread pool, the size of which defaults to the number of logical threads of the CPU. After the server starts, it launches a listening task on one of the I/O threads to accept connections from clients. Each time a connection is accepted, the server selects an I/O thread through round-robin to bind it to. Subsequently, all steps including data transmission, serialization, RPC routing, etc., of that connection are executed on this I/O thread. The RPC functions are also executed on the same I/O thread.

This means if your RPC functions will block the current thread (e.g., thread sleep, synchronous file read/write), it is better to make them asynchronous to avoid blocking the I/O thread, thereby preventing other requests from being blocked. For example, `async_simple` provides coroutine locks such as `Mutex` and `Spinlock`, and components such as `Promise` and `Future` that wrap asynchronous tasks as coroutine tasks. `coro_io` offers coroutine-based asynchronous file read/write, asynchronous read/write of sockets, sleep, and the `period_timer` timer. It also allows submitting high-CPU-load tasks to the global blocking task thread pool through `coro_io::post`. coro_rpc/coro_http offer coroutine-based asynchronous RPC and HTTP calls, respectively. easylog by default submits log content to a background thread for writing, ensuring the foreground does not block.


## Parameter and Return Value Types

coro_rpc allows users to register rpc functions with multiple parameters (up to 255), and the types of arguments and return values can be user-defined aggregate structures. They also support various data structures provided by the C++ standard library and many third-party libraries. For details, see: [struct_pack type system](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_type_system.html)

If your rpc argument or return value type is not supported by the struct_pack type system, we also allow users to register their own structures or custom serialization algorithms. For more details, see: [Custom feature](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_intro.html#custom-type)

### Return Value with view

The user's return value may contain view types such as `std::string_view` or `std::span`. These types can reduce copies during deserialization, thereby enhancing RPC performance. However, this requires that the objects they point to must not be destructed until after the RPC request has been successfully sent.

Users can ensure this by setting a `complete_handler`:

```cpp
std::string_view hello() {
  auto ctx = coro_rpc::get_context();
  auto str = std::make_unique<std::string>("Hello");
  ctx->set_complete_handler([str = std::move(str)](const std::error_code& ec, std::size_t length) {
    if (ec) {
      std::cout << "error: " << ec.message() << std::endl;
    } else {
      std::cout << "ok: write " << length << " bytes" << std::endl;
    }
  });
  return *str; /*this is safe*/
}
```

Especially, when the return value's view originates from the parameters within the RPC function, the RPC framework guarantees it to be valid without the need for extra operations.

```cpp
std::string_view echo(std::string_view sv) {
  return sv;
}
```

### RPC Return Value Construction and Checking

Furthermore, for callback functions, coro_rpc will try to construct the return value type from the parameter list. If it fails to construct, it will lead to a compilation failure.

```cpp
void echo(coro_rpc::context<std::string> ctx) {
  ctx.response_msg(); // Unable to construct std::string. Compilation fails.
  ctx.response_msg(42); // Unable to construct std::string. Compilation fails.
  ctx.response_msg(42,'A'); // Able to construct std::string, compilation passes.
  ctx.response_msg("Hello"); // Able to construct std::string, compilation passes.
  return;
}
```

## SSL Support

coro_rpc supports encrypting connections with OpenSSL. After installing OpenSSL and importing yalantinglibs into your project using cmake's `find_package/fetch_content`, you can enable SSL support by turning on the cmake option `YLT_ENABLE_SSL=ON`. Alternatively, you can manually add the macro `YLT_ENABLE_SSL` and manually link OpenSSL.

Once SSL support is enabled, users can call the `init_ssl` function before connecting to the server. This will establish an encrypted link between the client and the server. It should be noted that the coro_rpc server also must have SSL support enabled at compile time.

```cpp
coro_rpc_server server;
server.init_ssl({
  .base_path = "./",           // Base path of ssl files.
  .cert_file = "server.crt",   // Path of the certificate relative to base_path.
  .key_file = "server.key"     // Path of the private key relative to base_path.
});
```


After enabling SSL, the server will reject all non-ssl connections.

We also support NTLS if you enable it by CMAKE OPTION `YLT_ENABLE_NTLS`.

## RDMA Support


```cpp
coro_rpc_server server;
server.init_ibv(ib_socket_t::config_t{});
```

After enabling RDMA support, the server will reject all non-rdma connections.

## Advanced Settings

We provide the coro_rpc::config_t class, which allows users to set the details of the server:

```cpp
struct config_base {
  bool is_enable_tcp_no_delay = true; /* Whether to respond immediately to tcp requests */
  uint16_t port = 9001; /* Listening port */
  unsigned thread_num = std::thread::hardware_concurrency(); /* Number of connections used internally by rpc server, default is the number of logical cores */
  std::chrono::steady_clock::duration conn_timeout_duration =
      std::chrono::seconds{0}; /* Timeout duration for rpc requests, 0 seconds means rpc requests will not automatically timeout */
  std::string address="0.0.0.0"; /* Listening address */
  /* The following settings are only applicable if SSL is enabled */
  std::optional<ssl_configure> ssl_config = std::nullopt; // Configure whether to enable ssl
  /* The following settings are only applicable if rdma is enabled */
  std::optional<coro_io::ib_socket_t::config_t> ibv_config = std::nullopt; // Configure whether to enable rdma
};
struct ssl_configure {
  std::string base_path;  // Base path of ssl files.
  std::string cert_file;  // Path of the certificate relative to base_path.
  std::string key_file;   // Path of the private key relative to base_path.
  std::string dh_file;    // Path of the dh_file relative to base_path (optional).
}
int start() {
  coro_rpc::config_t config{};
  coro_rpc_server server(config);
  /* Register rpc function here... */
  server.start();
}

After enabling RDMA support, the server still allow non-rdma connections.

```


## Registration and Invocation of Special RPC Functions

### RPC ABI Changes and Compatibility

coro_rpc uses the `struct_pack` serialization library internally. As long as changes to RPC parameters and return values satisfy the forward/backward compatibility constraints of `struct_pack`, the ABIs of new and old versions can remain mutually compatible. You can add `struct_pack::compatible<T,VERSION_NUMBER>` fields to the structs used for parameters and return values. `struct_pack::compatible<T>` is similar to `std::optional<T>`. When an older client version does not contain this field, the server will receive an empty value.

For detailed rules, see the [struct_pack documentation](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_intro.html#forward-backward-compatibility). If the ABI is likely to change multiple times, it is recommended to manually specify the version number in the template parameter for each change.

What if the parameters or return values do not use structs? You can add new parameters and return values directly. Likewise, add several `compatible<T>` fields.

Example:
// server side
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
// client side
```client.cpp
int client_oldapi_server_newapi(int a);
int client_newapi_server_oldapi(int a, struct_pack::compatible<int> b);
int client_oldapi_server_newapi_ret();
std::tuple<int,struct_pack::compatible<int>> client_newapi_server_oldapi_ret();
```

We ensure that the server and client can communicate normally when calling these functions after the API changes.

In particular, we also support the case where the return value is `void`; you only need to upgrade it to `std::tuple<std::monostate,...>`.

Example:
// server side
```server.cpp
std::tuple<std::monostate,struct_pack::compatible<int>> client_oldapi_server_newapi_ret_void() {
    return {std::monostate{},1};
}
void client_newapi_server_oldapi_ret_void() {
    return;
}

```
// client side
```client.cpp
void client_oldapi_server_newapi_ret_void();
std::tuple<std::monostate,struct_pack::compatible<int>>  client_newapi_server_oldapi_ret_void();
```

### Registration and Invocation of Member Functions

coro_rpc supports registering and invoking member functions:

For example, consider the following function:

```cpp
struct dummy {
  std::string_view echo(std::string_view str) { return str; }
  Lazy<std::string_view> coroutine_echo(std::string_view str) {co_return str;}
  void callback_echo(coro_rpc::context</*return type = */ std::string_view> ctx, std::string_view str) {
    ctx.response_msg(str);
  }
};
```

The server can register these functions like this:

```cpp
#include "rpc_service.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>
int main() {
  coro_rpc_server server;
  dummy d{};
  server.register_handler<&dummy::echo,&dummy::coroutine_echo,&dummy::callback_echo>(&d); // regist member function
  server.start();
}
```

It's important to note that the lifecycle of the registered dummy type must be considered to ensure it remains alive while the server is running. Otherwise, the invoking behavior is undefined.

The client can call these functions like this:

```cpp
#include "rpc_service.h"
#include <coro_rpc/coro_rpc_client.hpp>

Lazy<void> test_client() {
  coro_rpc_client client;
  co_await client.connect("localhost", /*port =*/"9000");

  // calling RPC
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

### Specialized Template Functions

coro_rpc allows users to register and call specialized template functions.

For example, consider the following function:

```cpp
template<typename T>
T echo(T param) { return param; }
```

The server can register these functions like this:

```cpp
#include <ylt/coro_rpc/coro_rpc_server.hpp>
using namespace coro_rpc;
int main() {
  coro_rpc_server server;
  server.register_handler<echo<int>,echo<std::string>,echo<std::vector<int>>>(&d); // Register specialized template functions
  server.start();
}
```

The client can call like this:
```cpp
using namespace coro_rpc;
using namespace async_simple::coro;
Lazy<void> rpc_call(coro_rpc_client& cli) {
  assert(co_await cli.call<echo<int>>(42).value() == 42);
  assert(co_await cli.call<echo<std::string>>("Hello").value() == "Hello");
  assert(co_await cli.call<echo<std::vector<int>>>(std::vector{1,2,3}).value() == std::vector{1,2,3});
}
```