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
#include "rpc_api.hpp"

#include <memory>
#include <stdexcept>
#include <ylt/coro_rpc/coro_rpc_context.hpp>
#include <ylt/easylog.hpp>

#include "async_simple/Future.h"
#include "ylt/coro_rpc/impl/coro_rpc_server.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/errno.h"

using namespace coro_rpc;
using namespace std::chrono_literals;
using namespace std::string_literals;

void hi() { ELOGV(INFO, "call hi"); }
std::string hello() { return "hello"; }
std::string hello_timeout() {
  std::this_thread::sleep_for(40ms);
  return "hello";
}

void function_not_registered() {}

std::string client_hello() { return "client hello"; }

std::string client_hello_not_reg() { return "client hello"; }

std::string large_arg_fun(std::string data) {
  ELOGV(INFO, "data size: %d", data.size());
  return data;
}

int long_run_func(int val) {
  std::this_thread::sleep_for(40ms);
  return val;
}

void echo_with_attachment(coro_rpc::context<void> conn) {
  ELOGV(INFO, "call function echo_with_attachment, conn ID:%d",
        conn.get_context_info()->get_connection_id());
  auto str = conn.get_context_info()->release_request_attachment();
  conn.get_context_info()->set_response_attachment(std::move(str));
  conn.response_msg();
}
template <typename T>
void test_ctx_impl(T *ctx, std::string_view name) {
  if (ctx->has_closed()) {
    throw std::runtime_error("connection is close!");
  }
  ELOGV(INFO, "call function echo_with_attachment, conn ID:%d, request ID:%d",
        ctx->get_connection_id(), ctx->get_request_id());
  ELOGI << "remote endpoint: " << ctx->get_remote_endpoint()
        << ", local endpoint:" << ctx->get_local_endpoint();
  if (ctx->get_rpc_function_name() != name) {
    throw std::runtime_error("get error rpc function name!");
  }
  ELOGI << "rpc function name:" << ctx->get_rpc_function_name();
  std::string sv{ctx->get_request_attachment()};
  auto str = ctx->release_request_attachment();
  if (sv != str) {
    throw std::runtime_error("coro_rpc::errc::rpc_throw_exception");
  }
  ctx->set_response_attachment(std::move(str));
}
void test_context() {
  auto *ctx = coro_rpc::get_context();
  test_ctx_impl(ctx, "test_context");
  return;
}
void test_callback_context(coro_rpc::context<void> conn) {
  auto *ctx = conn.get_context_info();
  test_ctx_impl(ctx, "test_callback_context");
  [](coro_rpc::context<void> conn) -> async_simple::coro::Lazy<void> {
    co_await coro_io::sleep_for(514ms);
    ELOGV(INFO, "response in another executor");
    conn.response_msg();
  }(std::move(conn))
                                          .via(coro_io::get_global_executor())
                                          .detach();
  return;
}
using namespace async_simple::coro;

Lazy<void> test_lazy_context() {
  auto *ctx = co_await coro_rpc::get_context_in_coro();
  test_ctx_impl(ctx, "test_lazy_context");
  co_await coro_io::sleep_for(514ms, coro_io::get_global_executor());
  ELOGV(INFO, "response in another executor");
  co_return;
}

void test_response_error5() {
  throw coro_rpc::rpc_error{coro_rpc::errc::address_in_used,
                            "error with user-defined msg"};
  return;
}

Lazy<void> test_response_error6() {
  throw coro_rpc::rpc_error{coro_rpc::errc::address_in_used,
                            "error with user-defined msg"};
}

void coro_fun_with_user_define_connection_type(my_context conn) {
  conn.ctx_.response_msg();
}

void fun_with_delay_return_void(coro_rpc::context<void> conn) {
  conn.response_msg();
}

void fun_with_delay_return_string(coro_rpc::context<std::string> conn) {
  conn.response_msg("string"s);
}

void fun_with_delay_return_void_twice(coro_rpc::context<void> conn) {
  conn.response_msg();
  conn.response_msg();
}

void fun_with_delay_return_string_twice(coro_rpc::context<std::string> conn) {
  conn.response_msg("string"s);
  conn.response_msg("string"s);
}
void coro_fun_with_delay_return_void(coro_rpc::context<void> conn) {
  fun_with_delay_return_void(std::move(conn));
}

void coro_fun_with_delay_return_string(coro_rpc::context<std::string> conn) {
  fun_with_delay_return_string(std::move(conn));
}

void coro_fun_with_delay_return_void_twice(coro_rpc::context<void> conn) {
  fun_with_delay_return_void_twice(std::move(conn));
}

void coro_fun_with_delay_return_string_twice(
    coro_rpc::context<std::string> conn) {
  fun_with_delay_return_string_twice(std::move(conn));
}

void fun_with_delay_return_void_cost_long_time(coro_rpc::context<void> conn) {
  std::thread([conn = std::move(conn)]() mutable {
    std::this_thread::sleep_for(700ms);
    conn.response_msg();
  }).detach();
}

void coro_fun_with_delay_return_void_cost_long_time(
    coro_rpc::context<void> conn) {
  fun_with_delay_return_void_cost_long_time(std::move(conn));
}

std::string async_hi() { return "async hi"; }

std::string HelloService::hello() {
  ELOGV(INFO, "call HelloServer hello");
  return "hello";
}
std::string HelloService::static_hello() {
  ELOGV(INFO, "call static hello");
  return "static hello";
}

namespace ns_login {
bool LoginService::login(std::string username, std::string password) {
  return true;
}
}  // namespace ns_login

int client_oldapi_server_newapi(int a, struct_pack::compatible<int> b) {
  return a + b.value_or(1);
}
int client_newapi_server_oldapi(int a) { return a; }

std::tuple<int, struct_pack::compatible<int>>
client_oldapi_server_newapi_ret() {
  return {42, 1};
}
int client_newapi_server_oldapi_ret() { return 42; }

std::tuple<std::monostate, struct_pack::compatible<int>>
client_oldapi_server_newapi_ret_void() {
  return {std::monostate{}, 1};
}
void client_newapi_server_oldapi_ret_void() { return; }
std::unique_ptr<coro_rpc_server> start_extern_server() {
  auto s = std::make_unique<coro_rpc_server>(1, 0);
  s->register_handler<client_newapi_server_oldapi, client_oldapi_server_newapi,
                      client_newapi_server_oldapi_ret,
                      client_oldapi_server_newapi_ret,
                      client_newapi_server_oldapi_ret_void,
                      client_oldapi_server_newapi_ret_void>();
  s->async_start();
  return s;
}