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
#include <ylt/util/function_name.h>

#include "doctest.h"
#include "rpc_api.hpp"
using namespace coro_rpc;
namespace {
void a_1() {}
class class_a {
 public:
  void f_1() {}
  static void g_1() {}
};
namespace ns1 {
void b_1() {}
class class_b {
 public:
  void f_1() {}
  static void g_1() {}
};
namespace {
[[maybe_unused]] void c_1() {}
class class_c {
 public:
  void f_1() {}
  static void g_1() {}
};
namespace ns2 {}
}  // namespace
}  // namespace ns1
}  // namespace
namespace a {
namespace {
[[maybe_unused]] void bar() {}
namespace b {
namespace {
[[maybe_unused]] void foo() {}
}  // namespace
}  // namespace b
}  // namespace
}  // namespace a
TEST_CASE("testing member name") {
  SUBCASE("free func") {
    auto name = get_func_name<hi>();
    CHECK(name == "hi");
  }
  SUBCASE("member function") {
    auto name = get_func_name<&HelloService::hello>();
    CHECK(name == "HelloService::hello");
  }
  SUBCASE("static member function") {
    auto name = get_func_name<&HelloService::static_hello>();
    CHECK(name == "HelloService::static_hello");
  }
  SUBCASE("anony ns 1") {
    SUBCASE("free func") {
      auto name = get_func_name<a_1>();
      CHECK(name == "a_1");
      auto name2 = get_func_name<ns1::b_1>();
      CHECK(name2 == "ns1::b_1");
    }
    SUBCASE("1st") {
      auto name = get_func_name<&class_a::f_1>();
      CHECK(name == "class_a::f_1");
      auto name2 = get_func_name<&class_a::g_1>();
      CHECK(name2 == "class_a::g_1");
    }
    SUBCASE("2nd") {
      auto name = get_func_name<&ns1::class_b::f_1>();
      CHECK(name == "ns1::class_b::f_1");
      auto name2 = get_func_name<&ns1::class_b::g_1>();
      CHECK(name2 == "ns1::class_b::g_1");
    }
    //    SUBCASE("3rd") {
    //      auto name = get_func_name<&ns1::class_c::f_1>();
    //      CHECK(name == "ns1::class_c::f_1");
    //      auto name2 = get_func_name<&ns1::class_c::g_1>();
    //      CHECK(name2 == "ns1::class_c::g_1");
    //    }
    // TODO:
    //    auto name3 = get_func_name<ns1::c_1>();
    //    CHECK(name3 == "ns1::c_1");
  }
  // TODO: support nested anonymous namespace
  //  SUBCASE("anony ns 1") {
  //    auto name = get_func_name<a::bar>();
  //    CHECK(name == "a::bar");
  //  }
  //  SUBCASE("anony ns 2") {
  //    auto name = get_func_name<a::b::foo>();
  //    CHECK(name == "a::b::foo");
  //  }
}