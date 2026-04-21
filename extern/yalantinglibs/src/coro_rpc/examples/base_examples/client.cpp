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
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "async_simple/Try.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "rpc_service.h"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"
using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::string_literals;
/*!
 * \example helloworld/client.main.cpp
 * \brief helloworld example client code
 */

Lazy<void> show_rpc_call() {
  coro_rpc_client client;

  [[maybe_unused]] auto ec = co_await client.connect("127.0.0.1", "8801");
  assert(!ec);

  auto ret = co_await client.call<echo>("hello");
  assert(ret.value() == "hello");

  ret = co_await client.call<async_echo_by_coroutine>("42");
  assert(ret.value() == "42");

  ret = co_await client.call<async_echo_by_callback>("hi");
  assert(ret.value() == "hi");

  client.set_req_attachment("This is attachment.");
  auto ret_void = co_await client.call<echo_with_attachment>();
  assert(client.get_resp_attachment() == "This is attachment.");

  client.set_req_attachment("This is attachment.");
  char buf[100];
  client.set_resp_attachment_buf(buf);
  ret_void = co_await client.call<echo_with_attachment>();
  assert(client.get_resp_attachment() == "This is attachment.");
  assert(client.is_resp_attachment_in_external_buf());
  assert(client.get_resp_attachment().data() == buf);

  ret = co_await client.call<nested_echo>("nested_echo"s);
  assert(ret.value() == "nested_echo"s);

  ret = co_await client.call<&HelloService::hello>();
  assert(ret.value() == "HelloService::hello"s);

  ret_void = co_await client.call<get_ctx_info>();
  assert(ret_void);

  ret_void = co_await client.call<return_error_by_context>();

  assert(ret_void.error().code.val() == 404);
  assert(ret_void.error().msg == "404 Not Found.");

  ret_void = co_await client.call<return_error_by_exception>();

  assert(ret_void.error().code.val() == 404);

  auto ret2 = co_await client.call<rpc_with_state_by_tag>();
  auto str = ret2.value();
  std::cout << ret2.value() << std::endl;
  assert(ret2.value() == "1");
  ret2 = co_await client.call<rpc_with_state_by_tag>();
  std::cout << ret2.value() << std::endl;
  assert(ret2.value() == "2");
  ret2 = co_await client.call<rpc_with_state_by_tag>();
  std::cout << ret2.value() << std::endl;
  assert(ret2.value() == "3");

  ret = co_await client.call<rpc_with_complete_handler>();
  assert(ret == "Hello");
}
/*send multi request with same socket in the same time*/
Lazy<void> connection_reuse() {
  coro_rpc_client client;
  [[maybe_unused]] auto ec = co_await client.connect("127.0.0.1", "8801");
  assert(!ec);
  std::vector<Lazy<async_rpc_result<int>>> handlers;
  for (int i = 0; i < 10; ++i) {
    /* send_request is thread-safe, so you can call it in different thread with
     * same client*/
    handlers.push_back(co_await client.send_request<add>(i, i + 1));
  }
  std::vector<async_simple::Try<async_rpc_result<int>>> results =
      co_await collectAll(std::move(handlers));
  for (int i = 0; i < 10; ++i) {
    std::cout << results[i].value()->result() << std::endl;
    assert(results[i].value()->result() == 2 * i + 1);
  }
  co_return;
}

/*send multi request*/

int client_oldapi_server_newapi(int a);
int client_newapi_server_oldapi(int a, struct_pack::compatible<int> b);
int client_oldapi_server_newapi_ret();
std::tuple<int, struct_pack::compatible<int>> client_newapi_server_oldapi_ret();
void client_oldapi_server_newapi_ret_void();
std::tuple<std::monostate, struct_pack::compatible<int>>
client_newapi_server_oldapi_ret_void();

Lazy<void> abi_compatible() {
  coro_rpc_client client;
  [[maybe_unused]] auto ec = co_await client.connect("127.0.0.1", "8801");
  assert(!ec);
  auto ret = co_await client.call<client_oldapi_server_newapi>(42);
  assert(ret.value() == 43);
  ret = co_await client.call<client_newapi_server_oldapi>(42, 1);
  assert(ret.value() == 42);
  ret = co_await client.call<client_oldapi_server_newapi_ret>();
  assert(ret.value() == 42);
  auto ret2 = co_await client.call<client_newapi_server_oldapi_ret>();
  auto expected =
      std::tuple<int, struct_pack::compatible<int>>(42, std::nullopt);
  assert(ret2.value() == expected);
  {
    auto ret = co_await client.call<client_oldapi_server_newapi_ret_void>();
    assert(ret.has_value());
  }
  {
    auto ret = co_await client.call<client_newapi_server_oldapi_ret_void>();
    auto expected = std::tuple<std::monostate, struct_pack::compatible<int>>(
        std::monostate{}, std::nullopt);
    assert(ret.value() == expected);
  }
  co_return;
}

int main() {
  try {
    syncAwait(show_rpc_call());
    syncAwait(connection_reuse());
    syncAwait(abi_compatible());
    std::cout << "Done!" << std::endl;
  } catch (const std::exception& e) {
    std::cout << "Error:" << e.what() << std::endl;
  }
  return 0;
}