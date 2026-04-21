<p align="center">
<h1 align="center">yaLanTingLibs</h1>
<h6 align="center">C++20基础工具库集合，包括struct_pack, struct_json, struct_xml, struct_pb, easylog, coro_rpc, coro_http, metric 和 async_simple </h6>
</p>
<p align="center">
<img alt="license" src="https://img.shields.io/github/license/alibaba/async_simple?style=flat-square">
<img alt="language" src="https://img.shields.io/github/languages/top/alibaba/yalantinglibs?style=flat-square">
<img alt="last commit" src="https://img.shields.io/github/last-commit/alibaba/async_simple?style=flat-square">
</p>

[English Version](../../en/guide/what_is_yalantinglibs.md)

yaLanTingLibs 是一个现代C++基础工具库的集合, 现在它包括 struct_pack, struct_json, struct_xml, struct_yaml, struct_pb, easylog, coro_rpc, coro_io, coro_http, metric, reflection 和 async_simple, 目前我们正在开发并添加更多的新功能。

yaLanTingLibs 的目标: 为C++开发者提供高性能，极度易用的现代C++基础工具库, 帮助用户构建高性能的现代C++应用。

| 测试平台 (编译器版本)                            | 状态                                                                                                   |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Ubuntu 22.04 (clang 14.0.0)                    | ![ubuntu-clang](https://github.com/alibaba/yalantinglibs/actions/workflows/ubuntu_clang.yml/badge.svg?branch=main) |
| Ubuntu 22.04 (gcc 11.2.0)                      | ![ubuntu-gcc](https://github.com/alibaba/yalantinglibs/actions/workflows/ubuntu_gcc.yml/badge.svg?branch=main)   |
| macOS Monterey 12 (AppleClang 14.0.0.14000029) | ![macos-clang](https://github.com/alibaba/yalantinglibs/actions/workflows/mac.yml/badge.svg?branch=main)         |
| Windows Server 2022 (MSVC 19.33.31630.0)       | ![win-msvc](https://github.com/alibaba/yalantinglibs/actions/workflows/windows.yml/badge.svg?branch=main)     |

[开发规范](../yalantinglibs-dev-guidelines.md)
# 快速开始

## 编译器要求

如果你的编译器只支持C++17，yalantinglibs 只会编译序列化库。(struct_*系列)

确保你的编译器版本不低于:
- clang6++ (libstdc++-8 以上)。
- g++9 或更高版本。
- msvc 14.20 或更高版本。



如果你的编译器支持C++20，yalantinglibs会编译全部库。

确保你的编译器版本不低于:
- clang11++ (libstdc++-8 以上)。
- g++10 或更高版本。
- msvc 14.29 或更高版本。

你也可以手动指定Cmake选项`-DENABLE_CPP_20=ON` 或 `-DENABLE_CPP_20=OFF`来控制。

## 安装&编译

### 通过HomeBrew安装

1. 安装 [homebrew](https://brew.sh/)
2. 运行 `brew install yalantinglibs`
3. 如果使用cmake，添加如下代码:
```cmake
find_package(yalantinglibs CONFIG REQUIRED)
target_link_libraries(main PRIVATE yalantinglibs::yalantinglibs)
```

### 通过vcpkg包管理器安装

1. 下载[vcpkg](https://github.com/microsoft/vcpkg)
2. 执行命令：`./vcpkg install yalantinglibs`
3. 如果你使用cmake，向脚本里添加如下内容：   
```cmake
find_package(yalantinglibs CONFIG REQUIRED)
target_link_libraries(main PRIVATE yalantinglibs::yalantinglibs)
```

### Cmake FetchContent

也可以使用Cmake FetchContent来安装yalantinglibs。

```cmake
cmake_minimum_required(VERSION 3.15)
project(ylt_test)

include(FetchContent)

FetchContent_Declare(
    yalantinglibs
    GIT_REPOSITORY https://github.com/alibaba/yalantinglibs.git
    GIT_TAG main
    GIT_SHALLOW 1 # optional ( --depth=1 )
)

FetchContent_MakeAvailable(yalantinglibs)
add_executable(main main.cpp)

target_link_libraries(main yalantinglibs::yalantinglibs)
target_compile_features(main PRIVATE cxx_std_20)
```

### Cake add_subdirectory

1. 下载yalantinglibs的源代码，将文件夹放到你的工程下。
2. 将yalantinglibs的根目录加入到你的工程中。

```cmake
cmake_minimum_required(VERSION 3.15)
project(ylt_test)

add_subdirectory(yalantinglibs)  # 你可能需要根据实际情况修改这里的相对路径
add_executable(main main.cpp)

target_link_libraries(main yalantinglibs::yalantinglibs)
target_compile_features(main PRIVATE cxx_std_20)
```

### 手动安装

Yalantinglibs 是一个head-only的库，这意味着你可以简单粗暴的直接将`./include/ylt`拷贝走。但是更推荐的做法还是用Cmake安装。

- 克隆仓库

```shell
git clone https://github.com/alibaba/yalantinglibs.git
```

- 构建，测试并安装

- 我们建议，最好在安装之前编译样例/压测程序并执行测试：

```shell
cmake ..
cmake --build . --config debug # 可以在末尾加上-j 选项, 通过并行编译加速
ctest . # 执行测试
```

测试/样例/压测的可执行文件存储在路径`./build/output/`下。

- 你也可以跳过编译:

```shell
# 可以通过这些选项来跳过编译样例/压测/测试程序
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF -DGENERATE_BENCHMARK_DATA=OFF
cmake --build .
```

3. 安装

默认情况下会安装到系统默认的include路径，你也可以通过选项来自定义安装路径。

```shell
cmake --install . # --prefix ./user_defined_install_path 
```

4. 开始编程

- 从example开始开发:

安装完成后，你可以直接拷贝并打开文件夹`src/*/examples`，然后执行以下命令：

```shell
mkdir build
cd build
cmake ..
cmake --build .
```

- 使用cmake，将ylt导入到你的项目:

```cmake
find_package(yalantinglibs CONFIG REQUIRED)
target_link_libraries(main PRIVATE yalantinglibs::yalantinglibs)
```


- 如果不使用cmake，需要手动导入依赖:

1. 将 `include/`加入到头文件包含路径中(如果已安装到系统默认路径，可跳过该步骤)
2. 将 `include/ylt/thirdparty` 加入到头文件包含路径中(如果已通过cmake安装了yalantinglibs，可跳过该步骤)
3. 将 `include/ylt/standalone` 加入到头文件包含路径中(如果已通过cmake安装了yalantinglibs，可跳过该步骤)
4. 通过选项`-std=c++20`(g++/clang++) or `/std:c++20`(msvc)启用C++20标准。（序列化库和日志库至少需要c++17，网络库与协程至少需要C++20）
5. 使用g++时需要添加选项 `-pthread`,`-ldl` . 在`g++10`编译器下使用协程需要添加选项 `-fcoroutines`。

- 更多细节:
如需查看更多细节, 除了`example/cmakelist.txt`，你还可以参考 [here](https://github.com/alibaba/yalantinglibs/tree/main/CmakeLists.txt) and [there](https://github.com/alibaba/yalantinglibs/tree/main/cmake).

# 简介

## coro_rpc

coro是一个高度易用, head-only，基于协程的C++20高性能rpc框架库。在pipeline echo模式下单机每核心qps可达40万+。

你可以在几分钟之内就构建一个高性能的rpc服务端和客户端！

[简介](https://alibaba.github.io/yalantinglibs/zh/coro_rpc/coro_rpc_introduction.html)  

[API](https://alibaba.github.io/yalantinglibs/doxygen_cn/html/group__coro__rpc.html)

[Purecpp 演讲稿](https://alibaba.github.io/yalantinglibs/resource/coro_rpc_introduction_purecpp_talk.pdf).

[Purecpp 会议视频](https://live.csdn.net/room/csdnlive1/bKFbKP7T)，从04:55:08开始。

### 快速开始

1. 定义一个普通函数作为rpc函数。

```cpp
// rpc_service.hpp
inline std::string_view echo(std::string_view str) { return str; }
```

2. 注册rpc函数并启动服务器

```cpp
#include "rpc_service.hpp"
#include <ylt/coro_rpc/coro_rpc_server.hpp>

int main() {
  coro_rpc_server server(/*thread_num =*/10, /*port =*/9000);
  server.register_handler<echo>(); // register function echo
  server.start(); // start the server & block
}
```

3. rpc客户端连接服务器并调用rpc函数

```cpp
#include "rpc_service.hpp"
#include <ylt/coro_rpc/coro_rpc_client.hpp>

Lazy<void> test_client() {
  coro_rpc_client client;
  co_await client.connect("localhost", /*port =*/"9000"); // connect to the server

  auto r = co_await client.call<echo>("hello coro_rpc"); // call remote function echo
  std::cout << r.result.value() << "\n"; //will print "hello coro_rpc"
}

int main() {
  syncAwait(test_client());
}
```
更多示例[请见](https://github.com/alibaba/yalantinglibs/tree/main/src/coro_rpc/examples).

## struct_pack

struct_pack是一个基于编译期反射，易用且高性能的序列化库，head-only。一行代码即可完成序列化/反序列化。性能远超protobuf等传统序列化库。

[简介与设计文档](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_intro.html)

[API文档](https://alibaba.github.io/yalantinglibs/doxygen_cn/html/group__struct__pack.html)

[(Slides) A Faster Serialization Library Based on Compile-time Reflection and C++20](https://alibaba.github.io/yalantinglibs/resource/A%20Faster%20Serialization%20Library%20Based%20on%20Compile-time%20Reflection%20and%20C++%2020.pdf) CppCon2022 的演讲稿。

[(Video)  A Faster Serialization Library Based on Compile-time Reflection and C++20](https://www.youtube.com/watch?v=myhB8ZlwOlE) CppCon2022 的演讲视频。

[(Slides) 基于编译期反射和模板元编程的序列化库：struct_pack简介](https://alibaba.github.io/yalantinglibs/resource/CppSummit_struct_pack.pdf) Purecpp的演讲稿。

[(Video) 基于编译期反射和模板元编程的序列化库：struct_pack简介](https://live.csdn.net/room/csdnlive1/bKFbKP7T) Purecpp的演讲视频, 从 01:32:20 开始

### 快速开始
```cpp
// 定义一个C++结构体
struct person {
  int64_t id;
  std::string name;
  int age;
  double salary;
};
// 初始化对象
person person1{.id = 1, .name = "hello struct pack", .age = 20, .salary = 1024.42};

// 一行代码序列化
std::vector<char> buffer = struct_pack::serialize(person1);

// 一行代码反序列化
auto person2 = deserialize<person>(buffer);
```
更多示例[请见](https://alibaba.github.io/yalantinglibs/zh/struct_pack/struct_pack_intro.html#%E5%BA%8F%E5%88%97%E5%8C%96).

## struct_json

基于反射的json库，轻松实现结构体和json之间的映射。

### 快速开始
```cpp
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

struct person {
  std::string name;
  int age;
};
// C++17下反射功能不完善，需要使用宏：
// YLT_REFL(person, name, age);

int main() {
  person p{.name = "tom", .age = 20};
  std::string str;
  struct_json::to_json(p, str); // {"name":"tom","age":20}

  person p1;
  struct_json::from_json(p1, str);
}
```

## struct_xml
基于反射的xml库，轻松实现结构体和xml之间的映射。

### quick example
```cpp
#include "struct_xml/xml_reader.h"
#include "struct_xml/xml_writer.h"

struct person {
  std::string name;
  int age;
};
// C++17下反射功能不完善，需要使用宏：
// YLT_REFL(person, name, age);

void basic_usage() {
  std::string xml = R"(
<person>
    <name>tom</name>
    <age>20</age>
</person>
)";

  person p;
  bool r = struct_xml::from_xml(p, xml.data());
  assert(r);
  assert(p.name == "tom" && p.age == 20);

  std::string str;
  r = struct_xml::to_xml_pretty(p, str);
  assert(r);
  std::cout << str;
}
```

## coro_http

coro_http 是一个 C++20 的协程http(https)库，包括服务端和客户端, 支持: get/post, websocket, multipart file , chunked 和 ranges 请求。[more examples](https://github.com/alibaba/yalantinglibs/blob/main/src/coro_http/examples/example.cpp)

### get/post
```cpp
#include "ylt/coro_http/coro_http_server.hpp"
#include "ylt/coro_http/coro_http_client.hpp"
using namespace ylt;

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
  server.aync_start(); // aync_start() don't block, sync_start() will block.
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto result = co_await client.async_get("http://127.0.0.1:9001/get");
  assert(result.status == 200);
  assert(result.resp_body == "ok");
  for (auto [key, val] : result.resp_headers) {
    std::cout << key << ": " << val << "\n";
  }
}

async_simple::coro::Lazy<void> get_post(coro_http_client &client) {
  std::string uri = "http://www.example.com";
  auto result = co_await client.async_get(uri);
  std::cout << result.status << "\n";
  
  result = co_await client.async_post(uri, "hello", req_content_type::string);
  std::cout << result.status << "\n";
}

int main() {
  coro_http_client client{};
  async_simple::coro::syncAwait(get_post(client));
}
```

### websocket
```cpp
async_simple::coro::Lazy<void> websocket(coro_http_client &client) {
  // connect to your websocket server.
  bool r = co_await client.async_connect("ws://example.com/ws");
  if (!r) {
    co_return;
  }

  co_await client.write_websocket("hello websocket");
  auto data = co_await client.read_websocket();
  CHECK(data.resp_body == "hello websocket");
  co_await client.write_websocket("test again");
  data = co_await client.read_websocket();
  CHECK(data.resp_body == "test again");
  co_await client.write_websocket("ws close");
  data = co_await client.read_websocket();
  CHECK(data.net_err == asio::error::eof);
  CHECK(data.resp_body == "ws close");
}
```

### 上传/下载
```cpp
async_simple::coro::Lazy<void> upload_files(coro_http_client &client) {
  std::string uri = "http://example.com";
  
  client.add_str_part("hello", "world");
  client.add_str_part("key", "value");
  client.add_file_part("test", "test.jpg");
  auto result = co_await client.async_upload(uri);
  std::cout << result.status << "\n";
  
  result = co_await client.async_upload(uri, "test", "test.jpg");
}

async_simple::coro::Lazy<void> download_files(coro_http_client &client) {
  // chunked download
  auto result = co_await client.async_download("http://example.com/test.jpg",
                                               "myfile.jpg");
  std::cout << result.status << "\n";
  
  // ranges download
  result = co_await client.async_download("http://example.com/test.txt",
                                               "myfile.txt", "1-10,11-16");
  std::cout << result.status << "\n";
}
```

## async_simple

async_simple是一个C++20协程库，提供各种轻量且易用的组件，帮助用户编写异步代码。
请见[async_simple](https://github.com/alibaba/async_simple)

# 其他

## 配置选项

yalantinglibs工程自身支持如下配置项，值可以是`ON`或`OFF`。如果你使用cmake find_package或者fetchContent来导入yalantinglibs，你的工程也可以使用下面这些配置项。否则，你需要手动添加相关的宏并导入相关的依赖。

|工程选项|默认值|描述|
|----------|------------|------|
|YLT_ENABLE_SSL|automatic|为rpc/http启用可选的ssl支持,如果你已经安装了ssl，它会自动启用|
|YLT_ENABLE_NTLS|automatic|使用国密NTLS进行加密|
|YLT_ENABLE_PMR|OFF|启用pmr优化|
|YLT_ENABLE_IO_URING|OFF|在linux上使用io_uring作为后端（代替epoll）|
|YLT_ENABLE_FILE_IO_URING|OFF|启用io_uring优化|
|YLT_ENABLE_STRUCT_PACK_UNPORTABLE_TYPE|OFF|struct_pack启用对不跨平台的特殊类型的支持（如wstring, in128_t）|
|YLT_ENABLE_STRUCT_PACK_OPTIMIZE|OFF|struct_pack启用激进的模板展开优化（会花费更多编译时间）|

## 安装选项

默认情况下，ylt会把第三方依赖和子库直接安装到安装目录下。

如果你不想让ylt安装第三方依赖，你可以使用选项：`-DINSTALL_THIRDPARTY=OFF`。

如果你想让ylt将第三方依赖和子库安装到`ylt/thirdparty` 和`ylt/standalone`下，你可以开启选项：`-DINSTALL_INDEPENDENT_THIRDPARTY=OFF` 和`-DINSTALL_INDEPENDENT_STANDALONE=OFF`.

|选项|默认值|
|----------|------------|
|INSTALL_THIRDPARTY|ON|
|INSTALL_STANDALONE|ON|
|INSTALL_INDEPENDENT_THIRDPARTY|ON|
|INSTALL_INDEPENDENT_STANDALONE|ON|

## 开发选项

以下这些Cmake选项只适用于yalantinglibs自身的开发。它们不会对你的项目造成影响，因为yalantinglibs是head-only的。

|选项|默认值|
|----------|------------|
|BUILD_EXAMPLES|ON|
|BUILD_BENCHMARK|ON|
|BUILD_UNIT_TESTS|ON|
|BUILD_*(BUILD_CORO_RPC, BUILD_STRUCT_PACK等)|ON|
|COVERAGE_TEST|OFF|
|GENERATE_BENCHMARK_DATA|ON|
|CORO_RPC_USE_OTHER_RPC|ON|

## 第三方依赖清单

以下是我们使用的第三方依赖（async_simple虽然也是ylt的一部分，但其首先开源，故计为一个独立的第三方依赖）

### coro_io/coro_rpc/coro_http

这些依赖会被默认安装，可以通过安装选项来控制。

- [asio](https://think-async.com/Asio)
- [async_simple](https://github.com/alibaba/async_simple)
- [openssl](https://www.openssl.org/) (optional)

### easylog

无依赖。

### struct_pack, struct_json, struct_xml, struct_yaml

无依赖。

### struct_pb

无依赖。

### metric

无依赖。

### reflection

无依赖

## 独立子仓库

coro_http 由独立子仓库实现： [cinatra](https://github.com/qicosmos/cinatra)

struct_json、struct_xml、struct_yaml 由独立子仓库实现： [iguana](https://github.com/qicosmos/iguana)

## Benchmark

### coro_rpc

选项:

```bash
./benchmark_client # [线程数（默认为硬件线程数）] [每线程客户端数（默认为20）] [pipeline大小(默认为1，当设为1时相当于ping-pong模式)] [主机地址（默认为127.0.0.1）] [端口号（默认为9000）] [测试数据文件夹地址（默认为"./test_data/echo_test"] [测试秒数（默认为30）] [热身秒数（默认为5）]
```
## 跨语言支持
### 如何在python 中使用coro_http_client
参考 `yalantinglibs/src/coro_http/examples/py_example`

#### 编译和运行 py_example
- 设置 option `ENABLE_pybind11`(in `yalantinglibs/src/coro_http/examples/CMakeLists.txt`) ON 然后编译 `py_example`.

- 进入到py_example目录: `yalantinglibs/src/coro_http/examples/py_example`

- python3 test.py

### how to use coro_rpc with golang
参考 `yalantinglibs/src/coro_rpc/examples/basic_example/go_example`

#### 编译和运行 go_example
- 设置 option `ENABLE_go`(in `yalantinglibs/src/coro_rpc/examples/base_examples/CMakeLists.txt`) ON and then build `coro_rpc`.

- 进入到go_example 目录 `yalantinglibs/src/coro_rpc/examples/base_examples/go_example`

- go build test_rpc.go

- ./test_rpc

## 如何生成文档

请见[生成网站](https://github.com/alibaba/yalantinglibs/blob/main/website/README.md)

## 如何贡献代码
1. 根据issue模板提交一个issue。
2. 在本地修改代码，通过测试并使用 `git-clang-format HEAD^` 格式化代码。
3. 创建一个Pull Request，填写模板中的内容。
4. 提交Pull Request，并选择审核者: (如： qicosmos, poor-circle, PikachuHyA).
5. 通过github的全平台测试，审核者完成审核，代码合入主线。

# 讨论组

钉钉群

<center>
<img src="../../public/img/yalantinglibs_ding_talk_group.png" alt="dingtalk" width="200" height="200" align="bottom" />
</center>

## 许可证

yaLanTingLibs 基于 Apache License (Version 2.0) 开发。
本产品包含了许多基于其他开源许可证的第三方组件。
你可以查看[NOTICE文件](https://github.com/alibaba/yalantinglibs/blob/main/NOTICE)来了解更多信息。
