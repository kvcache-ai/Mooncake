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
#ifndef CORO_RPC_RPC_API_HPP
#define CORO_RPC_RPC_API_HPP
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_context.hpp>

#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"

void hi();
inline std::string_view test_string_view(std::string_view sv) {
  auto ctx = coro_rpc::get_context();
  std::string str;
  str.reserve(sizeof(std::string));
  str = std::string{sv}.append("OK");
  std::string_view result = str;
  ctx->set_complete_handler([str = std::move(str)](auto&&, auto) {
  });
  return result;
}
std::string hello();
std::string hello_timeout();
std::string client_hello();
std::string client_hello_not_reg();
std::string large_arg_fun(std::string data);
void function_not_registered();
int long_run_func(int val);
std::string async_hi();
struct my_context {
  coro_rpc::context<void> ctx_;
  my_context(coro_rpc::context<void>&& ctx) : ctx_(std::move(ctx)) {}
  using return_type = void;
};
void echo_with_attachment(coro_rpc::context<void> conn);
inline void error_with_context(coro_rpc::context<void> conn) {
  conn.response_error(coro_rpc::err_code{1004}, "My Error.");
}
void test_context();
void test_callback_context(coro_rpc::context<void> conn);
async_simple::coro::Lazy<void> test_lazy_context();
void test_response_error5();
async_simple::coro::Lazy<void> test_response_error6();
void coro_fun_with_user_define_connection_type(my_context conn);
void coro_fun_with_delay_return_void(coro_rpc::context<void> conn);
void coro_fun_with_delay_return_string(coro_rpc::context<std::string> conn);
void coro_fun_with_delay_return_void_twice(coro_rpc::context<void> conn);
void coro_fun_with_delay_return_string_twice(
    coro_rpc::context<std::string> conn);
void coro_fun_with_delay_return_void_cost_long_time(
    coro_rpc::context<void> conn);
inline async_simple::coro::Lazy<void> coro_func_return_void(int i) {
  auto ctx = co_await coro_rpc::get_context_in_coro();
  ELOGV(INFO,
        "call function coro_func_return_void, connection id:%d,request id:%d",
        ctx->get_connection_id(), ctx->get_request_id());
  co_return;
}
inline async_simple::coro::Lazy<int> coro_func(int i) { co_return i; }

class HelloService {
 public:
  std::string hello();
  static std::string static_hello();
  async_simple::coro::Lazy<int> coro_func(int i) { co_return i; }
  async_simple::coro::Lazy<void> coro_func_return_void(int i) { co_return; }

 private:
};
namespace ns_login {
class LoginService {
 public:
  bool login(std::string username, std::string password);
};
}  // namespace ns_login
std::unique_ptr<coro_rpc::coro_rpc_server> start_extern_server();

#endif  // CORO_RPC_RPC_API_HPP
