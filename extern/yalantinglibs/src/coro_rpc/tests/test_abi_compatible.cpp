/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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
#include <variant>
#include <ylt/coro_rpc/coro_rpc_context.hpp>

#include "doctest.h"
#include "rpc_api.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
using namespace coro_rpc;
using namespace coro_rpc::internal;

int client_oldapi_server_newapi(int a);
int client_newapi_server_oldapi(int a, struct_pack::compatible<int> b);
int client_oldapi_server_newapi_ret();
std::tuple<int, struct_pack::compatible<int>> client_newapi_server_oldapi_ret();
void client_oldapi_server_newapi_ret_void();
std::tuple<std::monostate, struct_pack::compatible<int>>
client_newapi_server_oldapi_ret_void();
TEST_CASE("test api break") {
  ELOG_INFO << "test abi break";
  auto server = start_extern_server();
  auto port = server->port();
  coro_rpc::coro_rpc_client cli;
  auto ec = syncAwait(cli.connect("127.0.0.1", std::to_string(port)));
  REQUIRE_MESSAGE(!ec, "connect failed");
  SUBCASE("client new server old") {
    ELOG_INFO << "test client new server old";
    auto result = syncAwait(cli.call<client_newapi_server_oldapi>(42, 1));
    REQUIRE(result.has_value());
    CHECK(result.value() == 42);
  }
  SUBCASE("client old server new") {
    ELOG_INFO << "test client old server new";
    auto result = syncAwait(cli.call<client_oldapi_server_newapi>(42));
    REQUIRE(result.has_value());
    CHECK(result.value() == 43);
  }
  SUBCASE("client new server old ret") {
    ELOG_INFO << "client new server old ret";
    auto result = syncAwait(cli.call<client_newapi_server_oldapi_ret>());
    REQUIRE(result.has_value());
    CHECK(std::get<0>(result.value()) == 42);
    CHECK(!std::get<1>(result.value()).has_value());
  }
  SUBCASE("client old server new ret") {
    ELOG_INFO << "client old server new ret";
    auto result = syncAwait(cli.call<client_oldapi_server_newapi_ret>());
    REQUIRE(result.has_value());
    CHECK(result.value() == 42);
  }
  SUBCASE("client new server old ret void") {
    ELOG_INFO << "client new server old ret void";
    auto result = syncAwait(cli.call<client_newapi_server_oldapi_ret_void>());
    REQUIRE(result.has_value());
    CHECK(std::get<0>(result.value()) == std::monostate{});
    CHECK(!std::get<1>(result.value()).has_value());
  }
  SUBCASE("client old server new ret void") {
    ELOG_INFO << "client old server new ret void";
    auto result = syncAwait(cli.call<client_oldapi_server_newapi_ret_void>());
    REQUIRE(result.has_value());
  }
}