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
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "../config/rest_rpc_protocol.hpp"
#include "rpc_service.h"
using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

/*
  // TODO: add user-defined rpc client
  // use rest_rpc(https://github.com/qicosmos/rest_rpc) client to test:
  #include <rest_rpc.hpp>
  void test_rest_rpc_client(){
    rpc_client client("127.0.0.1", 8801);
    bool r = client.connect();
    if (!r) {
      std::cout << "connect timeout" << std::endl;
      return;
    }

    auto result = client.call<std::string>("echo", "it is a test");
    std::cout << result << std::endl;

    auto r1 = client.call<int>("add", 2, 3);
    std::cout << r1 << std::endl;

    auto r2 = client.call<std::string>("hello_world");
    std::cout << r2 << std::endl;

    auto ret = client.call<std::string>("hello_with_delay");
    std::cout << ret << std::endl;

    auto r3 = client.call<std::string>("HelloService::hello");
    std::cout << r2 << std::endl;

    auto r4 = client.call<std::string>("HelloService::echo", "test");
    std::cout << r4 << std::endl;
  }
*/
int main() {
  // start rpc server
  coro_rpc_server_base<config::rest_rpc_config> server(
      std::thread::hardware_concurrency(), 8801);

  // regist normal function for rpc
  server.register_handler<hello_world, add, echo>();

  // regist member function for rpc
  HelloService hello_service;
  server.register_handler<&HelloService::hello, &HelloService::echo>(
      &hello_service);

  auto ec = server.start();
  return ec == std::errc{};
}