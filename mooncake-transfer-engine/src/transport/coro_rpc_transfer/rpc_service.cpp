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
#include "rpc_service.h"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/easylog.hpp>

#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Sleep.h"
#include "ylt/coro_io/client_pool.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"

using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::chrono_literals;

std::string_view echo(std::string_view data) {
  ELOGV(INFO, "call echo");
  return data;
}

void async_echo_by_callback(
    coro_rpc::context<std::string_view /*rpc response data here*/> conn,
    std::string_view /*rpc request data here*/ data) {
  ELOGV(INFO, "call async_echo_by_callback");
  /* rpc function runs in global io thread pool */
  coro_io::get_global_block_executor()->schedule([conn, data]() mutable {
    /* send work to global non-io thread pool */
    std::this_thread::sleep_for(1s);
    conn.response_msg(data); /*response here*/
  });
}

Lazy<std::string_view> async_echo_by_coroutine(std::string_view sv) {
  ELOGV(INFO, "call async_echo_by_coroutine");
  co_await coro_io::sleep_for(1s);  // sleeping
  co_return sv;
}

Lazy<void> get_ctx_info() {
  ELOGV(INFO, "call get_ctx_info");
  auto *ctx = co_await coro_rpc::get_context_in_coro();
  /*in callback rpc function, you can get ctx from coro_rpc::context*/
  /*in normal rpc function, you can get ctx by  coro_rpc::get_context() */
  if (ctx->has_closed()) {
    throw std::runtime_error("connection is close!");
  }
  ELOGV(INFO, "call function echo_with_attachment, conn ID:%d, request ID:%d",
        ctx->get_connection_id(), ctx->get_request_id());
  ELOGI << "remote endpoint: " << ctx->get_remote_endpoint() << "local endpoint"
        << ctx->get_local_endpoint();
  std::string sv{ctx->get_request_attachment()};
  auto str = ctx->release_request_attachment();
  if (sv != str) {
    ctx->close();
    throw rpc_error{coro_rpc::errc::io_error, "attachment error!"};
    co_return;
  }
  ctx->set_response_attachment(std::move(str));
  co_await coro_io::sleep_for(514ms, coro_io::get_global_executor());
  ELOGV(INFO, "response in another executor");
  co_return;
  co_return;
}

void echo_with_attachment() {
  ELOGV(INFO, "call echo_with_attachment");
  auto ctx = coro_rpc::get_context();
  ctx->set_response_attachment(
      ctx->get_request_attachment()); /*zero-copy by string_view*/
}

Lazy<std::string_view> nested_echo(std::string_view sv) {
  ELOGV(INFO, "start nested echo");
  /*get a client_pool of global*/
  auto client_pool =
      coro_io::g_clients_pool<coro_rpc::coro_rpc_client>().at("127.0.0.1:8802");
  assert(client_pool != nullptr);
  ELOGV(INFO, "connect another server");
  auto ret = co_await client_pool->send_request(
      [sv](coro_rpc_client &client)
          -> Lazy<coro_rpc::rpc_result<std::string_view>> {
        co_return co_await client.call<echo>(sv);
      });
  co_return ret.value().value();
}

std::string_view HelloService::hello() {
  ELOGV(INFO, "call HelloServer::hello");
  return "HelloService::hello";
}

void return_error_by_context(coro_rpc::context<void> conn) {
  conn.response_error(coro_rpc::err_code{404}, "404 Not Found.");
}

void return_error_by_exception() {
  throw coro_rpc::rpc_error{coro_rpc::errc{404}, "rpc not found."};
}

Lazy<std::string> rpc_with_state_by_tag() {
  auto *ctx = co_await coro_rpc::get_context_in_coro();
  if (!ctx->tag().has_value()) {
    ctx->tag() = std::uint64_t{0};
  }
  auto &cnter = std::any_cast<uint64_t &>(ctx->tag());
  ELOGV(INFO, "call count: %d", ++cnter);
  co_return std::to_string(cnter);
}
std::string_view rpc_with_complete_handler() {
  std::string s;
  s.reserve(sizeof(std::string));
  s = "Hello";
  std::string_view result = s;
  auto *ctx = coro_rpc::get_context();
  ctx->set_complete_handler(
      [s = std::move(s)](const std::error_code &ec, std::size_t len) {
        std::cout << "RPC result write to socket, msg: " << ec.message()
                  << " length:" << len << std::endl;
      });
  return result;
}