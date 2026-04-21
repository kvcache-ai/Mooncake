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
#include <ylt/coro_rpc/coro_rpc_context.hpp>

#include "doctest.h"
#include "rpc_api.hpp"
using namespace coro_rpc;
using namespace coro_rpc::internal;
TEST_CASE("test get_return_type") {
  SUBCASE("return void") {
    get_return_type<hi>();
    static_assert(std::is_same_v<void, decltype(get_return_type<hi>())>);
  }
  SUBCASE("return std::string") {
    auto s = get_return_type<async_hi>();
    CHECK(s.empty());
    static_assert(
        std::is_same_v<std::string, decltype(get_return_type<async_hi>())>);
  }
  SUBCASE("return void with conn") {
    get_return_type<coro_fun_with_delay_return_void>();
    static_assert(
        std::is_same_v<void, decltype(get_return_type<
                                      coro_fun_with_delay_return_void>())>);
  }
  SUBCASE("return std::string with conn") {
    auto s1 = get_return_type<coro_fun_with_delay_return_string>();
    CHECK(s1.empty());
    static_assert(
        std::is_same_v<
            std::string,
            decltype(get_return_type<coro_fun_with_delay_return_string>())>);
  }
}