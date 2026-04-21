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
#include <tuple>
#include <type_traits>
#define CORO_RPC_TEST
#include <iostream>
#include <memory>
#include <string_view>
#include <system_error>
#include <variant>
#include <ylt/coro_rpc/coro_rpc_context.hpp>
#include <ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/util/utils.hpp>

#include "doctest.h"
#include "ylt/struct_pack.hpp"
using namespace coro_rpc;

coro_rpc::protocol::router<coro_rpc::protocol::coro_rpc_protocol> router;

template <typename T>
struct rpc_return_type {
  using type = T;
};
template <>
struct rpc_return_type<void> {
  using type = std::monostate;
};
template <typename T>
using rpc_return_type_t = typename rpc_return_type<T>::type;

namespace test_util {
template <typename T>
struct RPC_trait {
  using return_type = T;
};
template <>
struct RPC_trait<void> {
  using return_type = std::monostate;
};
using coro_rpc_protocol = coro_rpc::protocol::coro_rpc_protocol;
template <auto func>
rpc_result<util::function_return_type_t<decltype(func)>> get_result(
    const auto &pair) {
  auto &&[rpc_errc, buffer] = pair;
  using T = util::function_return_type_t<decltype(func)>;
  using return_type = rpc_result<util::function_return_type_t<decltype(func)>>;
  rpc_return_type_t<T> ret;
  struct_pack::err_code ec;
  rpc_error err;
  if (!rpc_errc) {
    ec = struct_pack::deserialize_to(ret, buffer);
    if (!ec) {
      if constexpr (std::is_same_v<T, void>) {
        return {};
      }
      else {
        return std::move(ret);
      }
    }
  }
  else {
    err.code = rpc_errc;
    ec = struct_pack::deserialize_to(err.msg, buffer);
    if (!ec) {
      return return_type{unexpect_t{}, std::move(err)};
    }
  }
  // deserialize failed.
  err = {coro_rpc::errc::invalid_rpc_arguments,
         "failed to deserialize rpc return value"};
  return return_type{unexpect_t{}, std::move(err)};
}

template <typename R>
void check_result(const auto &pair, size_t offset = 0) {
  auto [err, buffer] = pair;
  assert(!err);
  std::string_view data(buffer.data(), buffer.size());
  typename RPC_trait<R>::return_type r;
  auto res = struct_pack::deserialize_to(r, data);
  if (res) {
    rpc_error r;
    auto res = struct_pack::deserialize_to(r, data);
    CHECK(!res);
  }
}

auto pack() { return std::vector<char>{}; }

template <typename Arg>
auto pack(Arg &&arg) {
  return struct_pack::serialize(std::forward<Arg>(arg));
}

template <typename... Args>
auto pack(Args &&...args) {
  return struct_pack::serialize(std::make_tuple(std::forward<Args>(args)...));
}

size_t g_head_offset = 0;
size_t g_tail_offset = 0;

template <auto func, typename... Args>
auto test_route(auto ctx, Args &&...args) {
  auto buf = pack(std::forward<Args>(args)...);
  constexpr auto id = func_id<func>();
  auto handler = router.get_handler(id);

  ctx->req_head_.function_id = id;

  return router.route(
      handler,
      std::string_view{buf.data() + g_head_offset, buf.size() - g_tail_offset},
      ctx, std::variant<coro_rpc::protocol::struct_pack_protocol>{}, id);
}

template <auto func, typename... Args>
void test_route_and_check(auto conn, Args &&...args) {
  auto pair = test_route<func>(conn, std::forward<Args>(args)...);
  using R = util::function_return_type_t<decltype(func)>;
  check_result<R>(pair, coro_rpc_protocol::RESP_HEAD_LEN);
}
}  // namespace test_util

void foo(int val) { ELOG_INFO << "foo " << val << "\n"; }
void foo1(coro_rpc::context<void> conn, int val) {
  ELOG_INFO << "foo1 " << val << "\n";
}
void foo2(coro_rpc::context<void> conn) {
  ELOG_INFO << "foo2 "
            << "\n";
}

void bar() {}

void bar3(int val) { ELOG_INFO << "bar3 val=" << val << "\n"; }

using namespace test_util;

auto ctx = std::make_shared<
    coro_rpc::context_info_t<coro_rpc::protocol::coro_rpc_protocol>>(router,
                                                                     nullptr);

struct person {
  int id;
  std::string name;
};

person get_person(const person &p) {
  ELOG_INFO << "get_person: " << p.id << ", " << p.name << "\n";
  return p;
}

person get_person1(const person &p, int id, std::string name) {
  ELOG_INFO << "get_person1: " << p.id << ", " << p.name << ", " << id << ", "
            << name << "\n";
  return p;
}

void not_register_func(int) {}

async_simple::coro::Lazy<void> coro_func() {
  ELOG_INFO << "hello, it's in coro function";
  co_return;
}

TEST_CASE("test string literal") {
  char arr[10] = "JACK";
  char arr2[10] = "jack";
  struct_pack::string_literal<char, 9> s1(arr);
  struct_pack::string_literal<char, 9> s2(arr2);

  CHECK(s1 != s2);

  constexpr struct_pack::string_literal s3("aaa");
  constexpr struct_pack::string_literal s4("aaa");
  constexpr struct_pack::string_literal s5("bbb");
  static_assert(s3 == s4);
  static_assert(s3 != s5);

  std::tuple<int> tp;
  bool r = struct_pack::get_type_literal<decltype(tp)>() !=
           struct_pack::get_type_literal<std::tuple<int, int>>();
  CHECK(r);
}

TEST_CASE("testing coro_handler") {
  router.register_handler<coro_func>(1);
  router.register_handler<coro_func>();
  auto buf = pack();
  constexpr auto id = func_id<coro_func>();
  auto handler = router.get_coro_handler(id);

  ctx->req_head_.function_id = id;

  async_simple::coro::syncAwait(router.route_coro(
      handler,
      std::string_view{buf.data() + g_head_offset, buf.size() - g_tail_offset},
      std::variant<coro_rpc::protocol::struct_pack_protocol>{}, id));
}

TEST_CASE("testing not registered func") {
  auto pair = test_route<not_register_func>(ctx, 42);
  CHECK(pair.first == coro_rpc::errc::function_not_registered);
}

void plus_one(int val) {}
void plus_one1(std::shared_ptr<coro_connection> conn, int val) {}
std::string get_str(std::string str) { return str; }

static void plus_two(int a, int b) {}

class test_class {
 public:
  void plus_one(int val) {}
  std::string get_str(std::string str) { return str; }
  static void plus_two(int a, int b) {}
};

TEST_CASE("testing invalid arguments") {
  std::pair<coro_rpc::errc, std::string> pair{};

  SUBCASE("test member functions") {
    test_class obj{};
    router.register_handler<&test_class::plus_one>(&obj, 2);
    router.register_handler<&test_class::plus_one, &test_class::get_str>(&obj);
    pair = test_route<plus_one>(ctx, 42);
    CHECK(pair.first == coro_rpc::errc::function_not_registered);

    pair = test_route<&test_class::plus_one>(ctx, 42);
    CHECK(!pair.first);

    pair = test_route<&test_class::plus_one>(ctx);
    CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

    pair = test_route<&test_class::plus_one>(ctx, 42, 42);
    CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

    pair = test_route<&test_class::plus_one>(ctx, "test");
    CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

    pair = test_route<&test_class::get_str>(ctx, "test");
    CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

    pair = test_route<&test_class::get_str>(ctx, std::string("test"));
    CHECK(!pair.first);

    auto r = get_result<&test_class::get_str>(pair);
    CHECK(r.value() == "test");
  }

  router.register_handler<plus_one>();
  router.register_handler<get_str>();

  pair = test_route<get_str>(ctx, "test");
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

  pair = test_route<get_str>(ctx, std::string("test"));
  CHECK(!pair.first);
  auto r = get_result<get_str>(pair);
  CHECK(r.value() == "test");

  pair = test_route<plus_one>(ctx, 42, 42);
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

  pair = test_route<plus_one>(ctx);
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

  pair = test_route<plus_one>(ctx, 42);
  CHECK(!pair.first);

  pair = test_route<plus_one>(ctx, std::string("invalid arguments"));
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);

  // register_handler<plus_one1>();
  // test_route<plus_one1>(ctx, 42); // will crash
}

TEST_CASE("testing invalid buffer") {
  std::pair<coro_rpc::errc, std::string> pair{};
  pair = test_route<plus_one>(ctx, 42);
  CHECK(!pair.first);

  g_head_offset = 2;
  pair = test_route<plus_one>(ctx, 42);
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);
  g_head_offset = 0;

  g_tail_offset = 2;
  pair = test_route<plus_one>(ctx, 42);
  CHECK(pair.first == coro_rpc::errc::invalid_rpc_arguments);
  g_tail_offset = 0;
}

void throw_exception_func() { throw std::runtime_error("runtime error"); }

void throw_exception_func1() { throw 1; }

TEST_CASE("testing exceptions") {
  router.register_handler<throw_exception_func, throw_exception_func1>();

  std::pair<coro_rpc::errc, std::string> pair{};
  pair = test_route<throw_exception_func>(ctx);
  CHECK(pair.first == coro_rpc::errc::rpc_throw_exception);
  auto r = get_result<throw_exception_func>(pair);
  ELOG_INFO << r.error().msg << "\n";

  pair = test_route<throw_exception_func1>(ctx);
  CHECK(pair.first == coro_rpc::errc::rpc_throw_exception);
  r = get_result<throw_exception_func>(pair);
  ELOG_INFO << r.error().msg << "\n";
}

TEST_CASE("testing object arguments") {
  router.register_handler<get_person>();

  person p{1, "tom"};
  auto buf = struct_pack::serialize(p);
  person p1;
  auto ec = struct_pack::deserialize_to(p1, buf);
  REQUIRE(!ec);
  test_route_and_check<get_person>(ctx, p);

  router.register_handler<get_person1>();
  test_route_and_check<get_person1>(ctx, p, 42, std::string("jerry"));
}

TEST_CASE("testing basic rpc register and route") {
  router.register_handler<bar>();
  router.register_handler<bar3>();

  router.register_handler<foo, foo1, foo2>();

  test_route_and_check<foo>(ctx, 42);
  test_route_and_check<foo1>(ctx, 42);
  test_route_and_check<foo2>(ctx);

  SUBCASE("test static functions") {
    router.register_handler<plus_two>();
    router.register_handler<test_class::plus_two>();

    test_route_and_check<plus_two>(ctx, 42, 42);
    test_route_and_check<test_class::plus_two>(ctx, 42, 42);
  }
}

using namespace coro_rpc;
using namespace coro_rpc::internal;
TEST_CASE("test get_return_type in connection") {
  SUBCASE("no conn") {
    auto ret = get_return_type<false, int>();
    CHECK(ret == 0);
  }
  SUBCASE("return void") {
    static_assert(
        std::is_same_v<decltype(get_return_type<true, context<void>>()), void>);
  }
  SUBCASE("return std::string") {
    static_assert(
        std::is_same_v<decltype(get_return_type<true, context<std::string>>()),
                       std::string>);
  }
}