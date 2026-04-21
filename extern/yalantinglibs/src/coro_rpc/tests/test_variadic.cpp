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
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "doctest.h"
using namespace coro_rpc;

inline std::string test_func(int64_t v1, double v2, std::string v3,
                             const std::array<std::string, 3>& v4) {
  return std::to_string(v1) + std::to_string(v2) + v3 + v4[0] + v4[1] + v4[2];
}
TEST_CASE("test varadic param") {
  using namespace coro_rpc;
  using namespace std;

  auto server = std::make_unique<coro_rpc::coro_rpc_server>(
      std::thread::hardware_concurrency(), 8808);
  server->register_handler<test_func>();
  auto res = server->async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());

  syncAwait(client.connect("localhost", std::to_string(server->port())));

  auto ret = syncAwait(client.call<test_func>(114514, 2.0f, "Hello coro_rpc!"s,
                                              std::array{
                                                  "hello"s,
                                                  "hi"s,
                                                  "what"s,
                                              }));
  ELOGV(INFO, "begin to stop server");
  server->stop();
  ELOGV(INFO, "finished stop server");
  if (ret) {
    ELOGV(INFO, "ret value %s", ret.value().data());
  }
  else {
    ELOGV(INFO, "ret err msg %s", ret.error().msg.data());
  }

  CHECK(ret);
  if (ret) {
    CHECK(ret == "1145142.000000Hello coro_rpc!hellohiwhat");
  }
}
