/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <ylt/coro_rpc/coro_rpc_server.hpp>
/*!
 * \defgroup coro_rpc coro_rpc
 *
 * \brief yaLanTingLibs RPC库
 *
 * coro_rpc分为服务端和客户端，服务端包括rpc函数注册API和服务器对象的API，客户端包括rpc调用API。
 *
 */

namespace coro_rpc {

/*!
 * \ingroup coro_rpc
 * RPC Server
 * 服务端使用示例
 *
 * ```c++
 * #include <ylt/coro_rpc/coro_rpc_server.hpp>
 * using namespace ylt::coro_rpc;
 *
 * inline std::string hello_coro_rpc() { return "hello coro_rpc"; }
 *
 * int main(){
 *   coro_rpc_server server(std::thread::hardware_concurrency(), 9000);
 *   server.register_handler<hello_coro_rpc>();
 *   server.start();
 * }
 * ```
 */
class coro_rpc_server {
 public:
  /*!
   * 构造coro_rpc_server，需要传入线程数、端口和连接超时时间。服务端会根据线程数启动
   * io_context以充分利用多核的能力。
   * 服务端在启动的时候会监听该端口
   * 如果conn_timeout_seconds大于0的时候，服务端会启动心跳检查，默认不开启。
   * 当超过conn_timeout_seconds时间内没有收到来自客户端的消息时，服务端会主动关闭客户端连接。
   *
   * @param thread_num
   * @param port
   * @param conn_timeout_seconds
   */
  coro_rpc_server(size_t thread_num, unsigned short port,
                  size_t conn_timeout_seconds = 0);
  /*!
   * 启动server(协程)，server启动后会监听端口等待连接到来，如果端口被占用会返回错误码；
   * 如果accept失败也会返回错误码；客户端连接到来之后启动会话。
   *
   * @return 正常启动返回空，否则返回错误码
   */
  async_simple::coro::Lazy<coro_rpc::err_code> async_start() noexcept;
  /*!
   * 阻塞方式启动server, 如果端口被占用将会返回非空的错误码
   *
   * @return 正常启动返回空，否则返回错误码
   */
  coro_rpc::err_code start();
  /*!
   * 停止server，阻塞等待直到server停止；
   */
  void stop();

  /*!
   * 获取监听的端口
   * @return 端口号
   */
  uint16_t port();

  /*!
   * 获取内部的调度器
   * @return
   */
  auto &get_executor();

  /*!
   * 注册rpc服务函数（非成员函数），服务端启动之前需要先注册rpc服务函数以对外提供rpc服务。
   * 在模版参数中填入函数地址即可完成注册，如果注册的函数重复的话，程序会终止退出。
   *
   * 函数注册示例
   * ```c++
   * // RPC函数(普通函数)
   * void hello() {}
   * std::string get_str() { return ""; }
   * int add(int a, int b) {return a + b; }
   *
   * // RPC函数(成员函数)
   * class test_class {
   *  public:
   *   void plus_one(int val) {}
   *   std::string get_str(std::string str) { return str; }
   * };
   *
   * int main() {
   *   coro_rpc_server server;
   *
   *   server.register_handler<hello>(); //一次注册一个RPC函数
   *   server.register_handler<get_str, add>(); //一次注册多个RPC函数
   *
   *   test_class obj{};
   *   server.register_handler<&test_class::plus_one,
   * &test_class::get_str>(&obj);//注册成员函数
   * }
   * ```
   *
   * @tparam first 需要注册的非成员函数，至少注册一个，允许同时注册多个
   * @tparam func
   */
  template <auto first, auto... func>
  void register_handler();

  /*!
   * 注册rpc服务函数（成员函数），服务端启动之前需要先注册rpc服务函数以对外提供rpc服务。
   * 在模版参数中填入成员函数地址即可完成注册，如果注册的函数重复的话，程序会终止退出；
   * 如果成员函数对应的对象指针为空程序会终止退出。
   *
   * @tparam first 需要注册的成员函数，至少注册一个，允许同时注册多个
   * @tparam func
   * @param self 成员函数对应的对象指针
   */
  template <auto first, auto... func>
  void register_handler(util::class_type_t<decltype(first)> *self);
};

/*!
 * \ingroup coro_rpc
 *
 * rpc_result本质是一个expected类型，使用之前需要判断一下，如果有错误的时候直接取值则会抛异常，如果有值的时候去取错误码则会抛异常。
 *
 * ```
 * template <typename T>
 * using rpc_result = expected<T, rpc_error>;
 * ```
 *
 * 使用示例
 * ```
 * auto result = co_await client.call<hello_coro_rpc>();
 * if (!result) {
 *   std::cout << "err: " << ret.error().msg << std::endl;
 * }
 * assert(result.value() == "hello coro_rpc"s);
 * ```
 *
 */
struct rpc_error {
  coro_rpc::err_code code;
  std::string msg;
};

/*!
 * \ingroup coro_rpc
 * 使用示例
 *
 * ```c++
 * #include <ylt/coro_rpc/coro_rpc_client.hpp>
 *
 * using namespace ylt::coro_rpc;
 * using namespace async_simple::coro;
 *
 * Lazy<void> show_rpc_call(coro_rpc_client &client) {
 *   auto ec = co_await client.connect("127.0.0.1", "8801");
 *   assert(!ec);
 *   auto result = co_await client.call<hello_coro_rpc>();
 *   if (!result) {
 *     std::cout << "err: " << result.error().msg << std::endl;
 *   }
 *   assert(result.value() == "hello coro_rpc"s);
 * }
 *
 * int main() {
 *   coro_rpc_client client;
 *   syncAwait(show_rpc_call(client));
 * }
 * ```
 *
 */
class coro_rpc_client {
 public:
  /*!
   * 创建client
   */
  coro_rpc_client();

  /*!
   * 通过io_context创建client
   * @param io_context 异步事件处理器
   */
  coro_rpc_client(asio::io_context &io_context);
  /*!
   * client是否已经关闭。
   * @return 是否关闭
   */
  bool has_closed();
  /*!
   * 连接服务端，成功返回空err，否则失败并返回实际错误码，错误码可能是超时也可能是连接错误等。
   *
   * @param host 服务端地址
   * @param port 服务端监听端口
   * @param timeout_duration rpc调用超时时间
   * @return 连接错误码，为空表示连接失败
   */
  async_simple::coro::Lazy<coro_rpc::err_code> connect(
      const std::string &host, const std::string &port,
      std::chrono::steady_clock::duration timeout_duration =
          std::chrono::seconds(5));

  /*!
   * client发起rpc调用，返回rpc_result，内部调用了call_for接口，超时时间为5秒。
   *
   * @tparam func 服务端注册的rpc函数
   * @tparam Args
   * @param args 服务端rpc函数需要的参数
   * @return rpc调用结果
   */
  template <auto func, typename... Args>
  async_simple::coro::Lazy<
      rpc_result<util::function_return_type_t<decltype(func)>>>
  call(Args &&...args);

  /*!
   * client发起rpc调用，返回rpc_result，调用超时时间由用户设置
   * @tparam func 服务端注册的rpc函数
   * @tparam Args
   * @param duration 调用rpc的超时时间
   * @param args 服务端rpc函数需要的参数
   * @return rpc调用结果
   */
  template <auto func, typename... Args>
  async_simple::coro::Lazy<
      rpc_result<util::function_return_type_t<decltype(func)>>>
  call_for(const auto &duration, Args &&...args);

  /*!
   * 获取内部的调度器
   * @return
   */
  auto &get_executor();
};
}  // namespace coro_rpc
