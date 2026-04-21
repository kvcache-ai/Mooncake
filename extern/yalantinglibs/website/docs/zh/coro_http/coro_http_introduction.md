# coro_http 简介

## 如何引入 coro_http_cient

coro_http_cient 是yalantinglibgs 的子库，yalantinglibs 是header only的，下载yalantinglibgs 库之后，在自己的工程中包含目录：

```cpp
  include_directories(include)
  include_directories(include/ylt/thirdparty)
```

如果是gcc 编译器还需要设置以启用C++20 协程：
```cpp
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
    #-ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
  endif()
```

最后在你的工程里引用coro_http_client 的头文件即可:

```cpp
#include <iostream>
#include "ylt/coro_http/coro_http_client.hpp"

int main() {
    coro_http::coro_http_client client{};
    std::string uri = "http://cn.bing.com";
    auto result = client.get(uri);
    if (result.net_err) {
      std::cout << result.net_err.message() << "\n";
    }
    std::cout << result.status << "\n";

    result = client.post(uri, "hello", coro_http::req_content_type::json);
    std::cout << result.status << "\n";    
}
```

## http 同步请求

### http 同步请求接口
```cpp
/// http header
/// \param name header 名称
/// \param value header 值
struct http_header {
  std::string_view name;
  std::string_view value;
};

/// http 响应的结构体
/// \param net_err 网络错误，默认为空
/// \param status http 响应的状态码，正常一般为200
/// \param resp_body http 响应body，类型为std::string_view，如果希望保存到后面延迟处理则需要将resp_body 拷贝走
/// \param resp_headers http 响应头，headers 都是string_view，生命周期和本次请求响应的生命周期一致，如果需要延迟使用则需要拷贝走
/// \param eof http 响应是否结束，一般请求eof 为true，eof对于文件下载才有意义，
///            下载的中间过程中eof 为false，最后一个包时eof才为true)
struct resp_data {
  std::error_code net_err;
  int status;
  std::string_view resp_body;
  std::span<http_header> resp_headers;
  bool eof;
};

/// \param uri http uri，如http://www.example.com
resp_data get(std::string uri);

enum class req_content_type {
  html,
  json,
  text,
  string,
  multipart,
  ranges,
  form_url_encode,
  octet_stream,
  xml,
  none
};

/// \param uri http uri，如http://www.example.com
/// \param content http 请求的body
/// \param content_type http 请求的content_type，如json、text等类型
resp_data post(std::string uri, std::string content,
                 req_content_type content_type);
```

### http 同步请求的用法
简单的请求一个网站一行代码即可:

```cpp
coro_http_client client{};
auto result = client.get("http://www.example.com");
if(result.net_err) {
  std::cout << net_err.message() << "\n";
  return;
}

if(result.status == 200) {
  std::cout << result.resp_body << "\n";
}
```
请求返回之后需要检查是否有网络错误和状态码，如果都正常则可以处理获取的响应body和响应头了。

```cpp
void test_sync_client() {
  {
    std::string uri = "http://www.baidu.com";
    coro_http_client client{};
    auto result = client.get(uri);
    assert(!result.net_err);
    print(result.resp_body);

    result = client.post(uri, "hello", req_content_type::json);
    print(result.resp_body);
  }

  {
    coro_http_client client{};
    std::string uri = "http://cn.bing.com";
    auto result = client.get(uri);
    assert(!result.net_err);
    print(result.resp_body);

    result = client.post(uri, "hello", req_content_type::json);
    print(result.resp_body);
  }
}
```

## http 异步请求接口

```cpp
async_simple::coro::Lazy<resp_data> async_get(std::string uri);

async_simple::coro::Lazy<resp_data> async_post(
    std::string uri, std::string content, req_content_type content_type);
```
async_get和get 接口参数一样，async_post 和 post 接口参数一样，只是返回类型不同，同步接口返回的是一个普通的resp_data，而异步接口返回的是一个Lazy 协程对象。事实上，同步接口内部就是调用对应的协程接口，用法上接近，多了一个co_await 操作。

事实上你可以把任意异步协程接口通过syncAwait 方法同步阻塞调用的方式转换成同步接口，以同步接口get 为例：
```cpp
resp_data get(std::string uri) {
  return async_simple::coro::syncAwait(async_get(std::move(uri)));
}
```

同步请求例子：
```cpp
async_simple::coro::Lazy<void> test_async_client() {
  std::string uri = "http://www.baidu.com";

  {
    coro_http_client client{};
    auto data = co_await client.async_get(uri);
    print(data.status);

    data = co_await client.async_get(uri);
    print(data.status);

    data = co_await client.async_post(uri, "hello", req_content_type::string);
    print(data.status);
  }
}  
```

## https 请求
发起https 请求之前确保已经安装了openssl，并开启YLT_ENABLE_SSL 预编译宏：
```
option(YLT_ENABLE_SSL "Enable ssl support" OFF)
```
client 只需要调用init_ssl 方法即可，之后便可以和之前一样发起https 请求了。

```cpp
const int verify_none = SSL_VERIFY_NONE;
const int verify_peer = SSL_VERIFY_PEER;
const int verify_fail_if_no_peer_cert = SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
const int verify_client_once = SSL_VERIFY_CLIENT_ONCE;

  /// 
  /// \param verify_mode 证书校验模式，默认校验
  /// \param full_path ssl 证书名称
  /// \param sni_hostname sni host 名称，默认为url的host
  /// \return ssl 初始化是否成功
  bool init_ssl(int verify_mode = asio::ssl::verify_peer,
                              std::string full_path = "",
                              const std::string &sni_hostname = "");
```

```cpp
#ifdef YLT_ENABLE_SSL
void test_coro_http_client() {
  coro_http_client client{};
  auto data = client.get("https://www.bing.com");
  std::cout << data.resp_body << "\n";
  data = client.get("https://www.bing.com");
  std::cout << data.resp_body << "\n";

  std::string uri2 = "https://www.baidu.com";
  coro_http_client client1{};
  client1.init_ssl("../../include/cinatra", "server.crt");
  data = co_await client1.async_get(uri2);
  print(data.status);

  data = co_await client1.async_get(uri2);
  print(data.status);  
}
#endif
```
根据需要，一般情况下init_ssl()可以不调用。

## 国密SSL（NTLS）支持

yaLanTingLibs 现已全面支持国密SSL（NTLS），通过集成 Tongsuo（铜锁）密码库，`coro_http` 和 `coro_rpc` 组件现在可以支持两种国密通信协议：

- **GB/T 38636-2020 TLCP**：双证书国密通信协议
- **RFC 8998**：TLS 1.3 + 国密单证书模式

详细使用说明请参考：[国密SSL（NTLS）支持文档](ntls_support.md)

## http 先连接再请求
前面介绍的get/post 接口传入uri，在函数内部会自动去连接服务器并发请求，一次性完成了连接和请求，如果希望将连接和请求分开程两个阶段，那么就可以先调用connect 接口再调用async_get 接口。

如果host 已经通过请求连接成功之后，后面发请求的时候只传入path 而不用传入完整的路径，这样可以获得更好的性能，coro_http_client 对于已经连接的host，当传入path 的时候不会再重复去解析已经解析过的uri。

```cpp
async_simple::coro::Lazy<void> test_async_client() {
  std::string uri = "http://www.baidu.com";

  {
    coro_http_client client{};
    // 先连接
    auto data = co_await client.connect(uri);
    print(data.status);

    // 后面再发送具体的请求
    data = co_await client.async_get(uri);
    print(data.status);

    // 对于已经连接的host，这里可以只传入path，不需要传入完整的uri
    data = co_await client.async_post("/", "hello", req_content_type::string);
    print(data.status);
  }
}
```

## http 重连
当http 请求失败之后，这个http client是不允许复用的，因为内部的socket 都已经关闭了，除非你调用reconnect 去重连host，这样就可以复用http client 了。

```cpp
  coro_http_client client1{};
  // 连接了一个非法的uri 会失败
  r = async_simple::coro::syncAwait(
      client1.async_http_connect("http//www.badurl.com"));
  CHECK(r.status != 200);

  // 通过重连复用client1
  r = async_simple::coro::syncAwait(client1.reconnect("http://cn.bing.com"));
  CHECK(client1.get_host() == "cn.bing.com");
  CHECK(client1.get_port() == "http");
  CHECK(r.status == 200);
```

# 其它http 接口
http_method
```cpp
enum class http_method {
  UNKNOW,
  DEL,
  GET,
  HEAD,
  POST,
  PUT,
  PATCH,
  CONNECT,
  OPTIONS,
  TRACE
};
```

coro_http_client 提供了这些http_method 对应的请求接口:
```cpp
async_simple::coro::Lazy<resp_data> async_delete(
    std::string uri, std::string content, req_content_type content_type);

async_simple::coro::Lazy<resp_data> async_get(std::string uri);

async_simple::coro::Lazy<resp_data> async_head(std::string uri);

async_simple::coro::Lazy<resp_data> async_post(
    std::string uri, std::string content, req_content_type content_type);

async_simple::coro::Lazy<resp_data> async_put(std::string uri,
                                              std::string content,
                                              req_content_type content_type);

async_simple::coro::Lazy<resp_data> async_patch(std::string uri);

async_simple::coro::Lazy<resp_data> async_http_connect(std::string uri);

async_simple::coro::Lazy<resp_data> async_options(std::string uri);

async_simple::coro::Lazy<resp_data> async_trace(std::string uri);
```
注意，async_http_connect 接口不是异步连接接口，它实际上是http_method::CONNECT 对应的接口，真正的异步连接接口connect 前面已经介绍过。

## 文件上传下载
除了http method 对应的接口之外，coro_http_client 还提供了常用文件上传和下载接口。

## chunked 格式上传
```cpp
template <typename S, typename File>
async_simple::coro::Lazy<resp_data> async_upload_chunked(
    S uri, http_method method, File file,
    req_content_type content_type = req_content_type::text,
    std::unordered_map<std::string, std::string> headers = {});
```
method 一般是POST 或者PUT，file 可以是带路径的文件名，也可以是一个iostream 流，content_type 文件的类型，headers 是请求头，这些参数填好之后，coro_http_client 会自动将文件分块上传到服务器，直到全部上传完成之后才co_return，中间上传出错也会返回。 

chunked 每块的大小默认为1MB，如果希望修改分块大小可以通过set_max_single_part_size 接口去设置大小，或者通过config 里面的max_single_part_size配置项去设置。

## multipart 格式上传
multipart 上传有两个接口，一个是一步实现上传，一个是分两步实现上传。

一步上传接口
```cpp
async_simple::coro::Lazy<resp_data> async_upload_multipart(
    std::string uri, std::string name, std::string filename);
```
name 是multipart 里面的name 参数，filename 需要上传的带路径的文件名。client 会自动将文件分片上传，分片大小的设置和之前介绍的max_single_part_size 一样，默认分片大小是1MB。

一步上传接口适合纯粹上传文件用，如果要上传多个文件，或者既有字符串也有文件的场景，那就需要两步上传的接口。

两步上传接口
```cpp
// 设置要上传的字符串key-value
bool add_str_part(std::string name, std::string content);
// 设置要上传的文件
bool add_file_part(std::string name, std::string filename);

// 上传
async_simple::coro::Lazy<resp_data> async_upload_multipart(std::string uri);
```
两步上传，第一步是准备要上传的字符串或者文件，第二步上传；

```cpp
  std::string uri = "http://127.0.0.1:8090/multipart";

  coro_http_client client{};
  client.add_str_part("hello", "world");
  client.add_str_part("key", "value");
  auto result = async_simple::coro::syncAwait(client.async_upload_multipart(uri));
```

## chunked 格式下载
```cpp
async_simple::coro::Lazy<resp_data> async_download(std::string uri,
                                                   std::string filename,
                                                   std::string range = "");
```
传入uri 和本地要保存的带路径的文件名即可，client 会自动下载并保存到文件中，直到全部下载完成。

## ranges 格式下载
ranges 下载接口和chunked 下载接口相同，需要填写ranges:
```cpp
  coro_http_client client{};
  std::string uri = "http://uniquegoodshiningmelody.neverssl.com/favicon.ico";

  std::string filename = "test.txt";
  std::error_code ec{};
  std::filesystem::remove(filename, ec);
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "1-10,11-16"));

  std::string filename1 = "test1.txt";
  std::error_code ec{};
  std::filesystem::remove(filename1, ec);
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename1, "1-10"));
```
ranges 按照"m-n,x-y,..." 的格式填写，下载的内容将会保存到文件里。

## chunked\ranges 格式下载到内存
如果下载的数据量比较小，不希望放到文件里，希望放到内存里，那么直接使用async_get、async_post 等接口即可，chunked\ranges 等下载数据将会保存到resp_data.resp_body 中。

# http client 配置项
client 配置项：
```cpp
  struct config {
    // 连接超时时间，默认8 秒
    std::optional<std::chrono::steady_clock::duration> conn_timeout_duration;
    // 请求超时时间，默认60 秒(包括连接时间和等待请求响应的时间)
    std::optional<std::chrono::steady_clock::duration> req_timeout_duration;
    // websocket 的安全key
    std::string sec_key;
    // chunked 下载/multipart 下载，chunked 上传/multipart上传时文件分片大小，默认1MB
    size_t max_single_part_size;
    // http 代理相关的设置
    std::string proxy_host;
    std::string proxy_port;
    std::string proxy_auth_username;
    std::string proxy_auth_passwd;
    std::string proxy_auth_token;
    // 是否启用tcp_no_delay
    bool enable_tcp_no_delay;
#ifdef CINATRA_ENABLE_SSL
    // 当请求的url中没有schema时，use_ssl为true时添加https，为false时添加http
    bool use_ssl = false;
    // ssl 证书路径
    std::string base_path;
    // ssl 证书名称
    std::string cert_file;
    // ssl 校验模式
    int verify_mode;
    // ssl 校验域名
    std::string domain;
#endif
  };
```

把config项设置之后，调用init_config 设置http client 的参数。
```cpp
coro_http_client client{};
coro_http_client::config conf{.req_timeout_duration = 60s};
client.init_config(conf);
auto r = async_simple::coro::syncAwait(
    client.async_http_connect("http://www.baidu.com"));
```
## websocket
websocket 的支持需要3步：
- 连接服务器；
- 发送websocket 数据；
- 读websocket 数据；

websocket 读数据接口:
```cpp
async_simple::coro::Lazy<resp_data> read_websocket();
```
websocket 连接服务器接口:
```cpp
async_simple::coro::Lazy<resp_data> connect(std::string uri);
```
websocket 发送数据接口：
```cpp
enum opcode : std::uint8_t {
  cont = 0,
  text = 1,
  binary = 2,
  rsv3 = 3,
  rsv4 = 4,
  rsv5 = 5,
  rsv6 = 6,
  rsv7 = 7,
  close = 8,
  ping = 9,
  pong = 10,
  crsvb = 11,
  crsvc = 12,
  crsvd = 13,
  crsve = 14,
  crsvf = 15
};

/// 发送websocket 数据
/// \param msg 要发送的websocket 数据
/// \param op opcode 一般为text、binary或 close 等类型
async_simple::coro::Lazy<resp_data> write_websocket(std::string msg,
                                                  opcode op = opcode::text);
```

websocket 例子:

```cpp
  coro_http_client client;
  // 连接websocket 服务器
  async_simple::coro::syncAwait(
      client.connect("ws://localhost:8090"));

  std::string send_str(len, 'a');

  // 发送websocket 数据
  async_simple::coro::syncAwait(client.write_websocket(std::string(send_str)));
  auto data = async_simple::coro::syncAwait(client.read_websocket());
  REQUIRE(data.resp_body.size() == send_str.size());
  CHECK(data.resp_body == send_str);
```

## 错误码
coro_http 错误码复用了std::error_code，具体错误的值为std::errc：https://en.cppreference.com/w/cpp/error/errc

coro_http 只对std::errc::timeout错误码做了扩展，发生连接和请求超时的时候不会返回有二义性的std::errc::timeout，而是返回扩展的http_errc：
```cpp
enum class http_errc { connect_timeout = 2025, request_timeout };
std::error_code make_error_code(http_errc e);
```
error message 分别对应：`Connect timeout` 和`Request timeout`

常见的返回错误及其含义有：

std::errc::protocol_error: url解析错误；response 解析错误；websocket 协议解析错误；chunked格式解析错误；gzip解析错误都归为协议错误；

http_errc::connect_timeout：连接超时，自定义错误码2025，错误消息`Connect timeout`

http_errc::request_timeout：请求超时，自定义错误码2026，错误消息`Request timeout`

std::errc::invalid_argument：请求参数非法，文件上传时文件不存在；文件上传时upload size非法；

std::errc::bad_file_descriptor：文件流上传时，文件打开失败；

std::errc::no_such_file_or_directory：文件流发送时文件不存在

网络io返回的错误来自于asio io请求返回的错误asio::error(std::errc 的子集)，如asio::error::eof, asio::error::broken_pipe，asio::connection_reset，asio::error::connection_refused等等；

## 线程模型
coro_http_client 默认情况下是共享一个全局“线程池”，这个“线程池”准确来说是一个io_context pool，coro_http_client 的线程模型是一个client一个io_context，
io_context 和 client 是一对多的关系。io_context pool 默认的线程数是机器的核数，如果希望控制pool 的线程数可以调用coro_io::get_global_executor(pool_size) 去设置
总的线程数。


client 不是线程安全的，要确保只有一个线程在调用client，如果希望并发请求服务端有两种方式：

方式一：

创建多个client 去请求服务端， 全局的“线程池”，会用轮询的方式为每个client 分配一个线程。

方式二：

通过多个协程去请求服务端, 每个协程都在内部线程池的某个线程中执行。去请求服务端

```cpp
std::vector<std::shared_ptr<coro_http_client>> clients;
std::vector<async_simple::coro::Lazy<resp_data>> futures;
for (int i = 0; i < 10; ++i) {
  auto client = std::make_shared<coro_http_client>();
  futures.push_back(client->async_get("http://www.baidu.com/"));
  clients.push_back(client);
}

auto out = co_await async_simple::coro::collectAll(std::move(futures));

for (auto &item : out) {
  auto result = item.value();
  assert(result.status == 200);
}
```

## 设置解析http response 的最大header 数量
默认情况下，最多可以解析100 个http header，如果希望解析更多http header 需要define一个宏CINATRA_MAX_HTTP_HEADER_FIELD_SIZE，通过它来设置解析的最大header 数, 在include client 头文件之前定义：
```cpp
#define CINATRA_MAX_HTTP_HEADER_FIELD_SIZE 200  // 将解析的最大header 数设置为200
```

## coro_http_server 基本用法

### coro_http指令集功能使用

coro_http支持通过指令集优化其内部逻辑，其通过宏来控制是否使用指令集。使用之前请确保cpu支持。

使用如下命令即可编译带simd优化的coro_http。注意只能开启一种simd指令集优化,开启多个会导致编译失败。

```shell
cmake -DENABLE_SIMD=SSE42 .. # 启用sse4.2指令集
cmake -DENABLE_SIMD=AVX2 .. # 启用avx2指令集
cmake -DENABLE_SIMD=AARCH64 .. # arm环境下,启用neon指令集
```

### 快速示例

### 示例1：一个简单的hello world
```cpp
  #include "ylt/coro_http/coro_http_client.hpp"
  #include "ylt/coro_http/coro_http_server.hpp"
	using namespace coro_http;
	
	int main() {
		int max_thread_num = std::thread::hardware_concurrency();
		coro_http_server server(max_thread_num, 8080);
		server.set_http_handler<GET, POST>("/", [](coro_http_request& req, coro_http_response& res) {
			res.set_status_and_content(status_type::ok, "hello world");
		});

		server.sync_start();
		return 0;
	}
```

5行代码就可以实现一个简单http服务器了，用户不需要关注多少细节，直接写业务逻辑就行了。

### 示例2：基本用法
```cpp
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"
using namespace coro_http;

struct person_t {
  void foo(coro_http_request &, coro_http_response &res) {
    res.set_status_and_content(status_type::ok, "ok");
  }
};

async_simple::coro::Lazy<void> basic_usage() {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/get", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/coro",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      });

  server.set_http_handler<GET>(
      "/in_thread_pool",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        // will respose in another thread.
        co_await coro_io::post([&] {
          // do your heavy work here when finished work, response.
          resp.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<POST, PUT>(
      "/post", [](coro_http_request &req, coro_http_response &resp) {
        auto req_body = req.get_body();
        resp.set_status_and_content(status_type::ok, std::string{req_body});
      });

  server.set_http_handler<GET>(
      "/headers", [](coro_http_request &req, coro_http_response &resp) {
        auto name = req.get_header_value("name");
        auto age = req.get_header_value("age");
        assert(name == "tom");
        assert(age == "20");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/query", [](coro_http_request &req, coro_http_response &resp) {
        auto name = req.get_query_value("name");
        auto age = req.get_query_value("age");
        assert(name == "tom");
        assert(age == "20");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<coro_http::GET, coro_http::POST>(
      "/users/:userid/subscriptions/:subid",
      [](coro_http_request &req, coro_http_response &response) {
        assert(req.params_["userid"] == "ultramarines");
        assert(req.params_["subid"] == "guilliman");
        response.set_status_and_content(status_type::ok, "ok");
      });

  person_t person{};
  server.set_http_handler<GET>("/person", &person_t::foo, person);

  server.async_start();
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto result = co_await client.async_get("http://127.0.0.1:9001/get");
  assert(result.status == 200);
  assert(result.resp_body == "ok");
  for (auto [key, val] : result.resp_headers) {
    std::cout << key << ": " << val << "\n";
  }

  result = co_await client.async_get("/coro");
  assert(result.status == 200);

  result = co_await client.async_get("/in_thread_pool");
  assert(result.status == 200);

  result = co_await client.async_post("/post", "post string",
                                      req_content_type::string);
  assert(result.status == 200);
  assert(result.resp_body == "post string");

  client.add_header("name", "tom");
  client.add_header("age", "20");
  result = co_await client.async_get("/headers");
  assert(result.status == 200);

  result = co_await client.async_get("/query?name=tom&age=20");
  assert(result.status == 200);

  result = co_await client.async_get(
      "http://127.0.0.1:9001/users/ultramarines/subscriptions/guilliman");
  assert(result.status == 200);

  // make sure you have installed openssl and enable YLT_ENABLE_SSL
#ifdef YLT_ENABLE_SSL
  coro_http_client client2{};
  result = co_await client2.async_get("https://baidu.com");
  assert(result.status == 200);
#endif
}

int main() {
  async_simple::coro::syncAwait(basic_usage());
}
```

### 示例3：面向切面的http服务器
```cpp
  #include "ylt/coro_http/coro_http_client.hpp"
  #include "ylt/coro_http/coro_http_server.hpp"
	using namespace coro_http;

	//日志切面
	struct log_t {
		bool before(coro_http_request& req, coro_http_response& res) {
			std::cout << "before log" << std::endl;
			return true;
		}
	
		bool after(coro_http_request& req, coro_http_response& res) {
			std::cout << "after log" << std::endl;
			return true;
		}
	};
	
	//校验的切面
	struct check {
		bool before(coro_http_request& req, coro_http_response& res) {
			std::cout << "before check" << std::endl;
			if (req.get_header_value("name").empty()) {
				res.set_status_and_content(status_type::bad_request);
				return false;
			}
			return true;
		}
	
		bool after(coro_http_request& req, coro_http_response& res) {
			std::cout << "after check" << std::endl;
			return true;
		}
	};

	//将信息从中间件传输到处理程序
	struct get_data {
		bool before(coro_http_request& req, coro_http_response& res) {
			req.set_aspect_data("hello world");
			return true;
		}
	}

	int main() {
		coro_http_server server(std::thread::hardware_concurrency(), 8080);
		server.set_http_handler<GET, POST>("/aspect", [](coro_http_request& req, coro_http_response& res) {
			res.set_status_and_content(status_type::ok, "hello world");
		}, check{}, log_t{});

		server.set_http_handler<GET,POST>("/aspect/data", [](coro_http_request& req, coro_http_response& res) {
			std::string hello = req.get_aspect_data()[0];
			res.set_status_and_content(status_type::ok, std::move(hello));
		}, get_data{});

		server.sync_start();
		return 0;
	}
```
本例中有两个切面，一个校验http请求的切面，一个是日志切面，这个切面用户可以根据需求任意增加。本例会先检查http请求的合法性，如果不合法就会返回bad request，合法就会进入下一个切面，即日志切面，日志切面会打印出一个before表示进入业务逻辑之前的处理，业务逻辑完成之后会打印after表示业务逻辑结束之后的处理。

### 示例4：文件上传、下载、websocket
见[example中的例子](example/main.cpp)

### 示例5：RESTful服务端路径参数设置
本代码演示如何使用RESTful路径参数。下面设置了两个RESTful API。第一个API当访问，比如访问这样的url`http://127.0.0.1:8080/numbers/1234/test/5678`时服务器可以获取到1234和5678这两个参数，第一个RESTful API的参数是`(\d+)`是一个正则表达式表明只能参数只能为数字。获取第一个参数的代码是`req.matches_[1]`。因为每一个req不同所以每一个匹配到的参数都放在`request`结构体中。

同时还支持任意字符的RESTful API，即示例的第二种RESTful API`"/string/:id/test/:name"`，要获取到对应的参数使用`req.get_query_value`函数即可，其参数只能为注册的变量(如果不为依然运行但是有报错)，例子中参数名是id和name，要获取id参数调用`req.get_query_value("id")`即可。示例代码运行后，当访问`http://127.0.0.1:8080/string/params_1/test/api_test`时，浏览器会返回`api_test`字符串。

```cpp
  #include "ylt/coro_http/coro_http_client.hpp"
  #include "ylt/coro_http/coro_http_server.hpp"
	using namespace coro_http;
	
	int main() {
		int max_thread_num = std::thread::hardware_concurrency();
		coro_http_server server(max_thread_num, 8080);

		server.set_http_handler<GET, POST>(
			R"(/numbers/(\d+)/test/(\d+))", [](request &req, response &res) {
				std::cout << " matches[1] is : " << req.matches_[1]
						<< " matches[2] is: " << req.matches_[2] << std::endl;

				res.set_status_and_content(status_type::ok, "hello world");
			});

		server.set_http_handler<GET, POST>(
			"/string/:id/test/:name", [](request &req, response &res) {
				std::string id = req.get_query_value("id");
				std::cout << "id value is: " << id << std::endl;
				std::cout << "name value is: " << std::string(req.get_query_value("name")) << std::endl;
				res.set_status_and_content(status_type::ok, std::string(req.get_query_value("name")));
			});

		server.sync_start();
		return 0;
	}
  ```

  ### 反向代理
  目前支持random, round robin 和 weight round robin三种负载均衡三种算法，设置代理服务器时指定算法类型即可。
  假设需要代理的服务器有三个，分别是"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"，coro_http_server设置路径、代理服务器列表和算法类型即可实现反向代理。

  ```cpp
void http_proxy() {
  coro_http::coro_http_server web_one(1, 9001);

  web_one.set_http_handler<coro_http::GET, coro_http::POST>(
      "/",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          response.set_status_and_content(status_type::ok, "web1");
        });
      });

  web_one.async_start();

  coro_http::coro_http_server web_two(1, 9002);

  web_two.set_http_handler<coro_http::GET, coro_http::POST>(
      "/",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          response.set_status_and_content(status_type::ok, "web2");
        });
      });

  web_two.async_start();

  coro_http::coro_http_server web_three(1, 9003);

  web_three.set_http_handler<coro_http::GET, coro_http::POST>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "web3");
      });

  web_three.async_start();

  std::this_thread::sleep_for(200ms);

  coro_http_server proxy_wrr(2, 8090);
  proxy_wrr.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::WRR, {10, 5, 5});

  coro_http_server proxy_rr(2, 8091);
  proxy_rr.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::RR);

  coro_http_server proxy_random(2, 8092);
  proxy_random.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"});

  coro_http_server proxy_all(2, 8093);
  proxy_all.set_http_proxy_handler(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"});

  proxy_wrr.async_start();
  proxy_rr.async_start();
  proxy_random.async_start();
  proxy_all.async_start();

  std::this_thread::sleep_for(200ms);

  coro_http_client client_rr;
  resp_data resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web2");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web3");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web2");
  resp_rr = client_rr.post("http://127.0.0.1:8091/", "test content",
                           req_content_type::text);
  assert(resp_rr.resp_body == "web3");

  coro_http_client client_wrr;
  resp_data resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web2");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web3");

  coro_http_client client_random;
  resp_data resp_random = client_random.get("http://127.0.0.1:8092/");
  std::cout << resp_random.resp_body << "\n";
  assert(!resp_random.resp_body.empty());

  coro_http_client client_all;
  resp_random = client_all.post("http://127.0.0.1:8093/", "test content",
                                req_content_type::text);
  std::cout << resp_random.resp_body << "\n";
  assert(!resp_random.resp_body.empty());
}
```
