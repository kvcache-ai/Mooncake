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
#include <cstddef>
#include <iostream>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
using namespace struct_pack;
using namespace doctest;
// clang-format off
static std::string alignment_requirement_err_msg = "different alignment requirement, we must regard as different type";
// clang-format on
TEST_SUITE_BEGIN("test_alignas");
namespace test_alignas {

template <typename A, typename B>
bool operator==(const A& a, const B& b) {
  return a.a == b.a && a.b == b.b;
}
struct dummy {
  char a;
  short b;
};
}  // namespace test_alignas
TEST_CASE("testing no alignas") {
  using T = test_alignas::dummy;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)131, (char)131, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  auto ret = struct_pack::deserialize<T>(buf);
  REQUIRE_MESSAGE(ret, ret.error().message());
  T d_t = ret.value();
  CHECK(t == d_t);
}
namespace test_alignas {
struct alignas(2) dummy_2 {
  char a;
  short b;
};
}  // namespace test_alignas
TEST_CASE("testing alignas(2)") {
  using T = test_alignas::dummy_2;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)131, (char)131, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_alignas::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
  SUBCASE("deserialize to dummy_2") {
    using DT = test_alignas::dummy_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
}
namespace test_alignas {
struct alignas(4) dummy_4 {
  char a;
  short b;
};
}  // namespace test_alignas
TEST_CASE("testing alignas(4)") {
  using T = test_alignas::dummy_4;
  static_assert(std::alignment_of_v<T> == 4);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 4);
  static_assert(alignof(T) == 4);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)131, (char)133, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_alignas::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_2") {
    using DT = test_alignas::dummy_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_4") {
    using DT = test_alignas::dummy_4;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
}
namespace test_alignas {
struct alignas(8) dummy_8 {
  char a;
  short b;
};
}  // namespace test_alignas
TEST_CASE("testing alignas(8)") {
  using T = test_alignas::dummy_8;
  static_assert(std::alignment_of_v<T> == 8);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 8);
  static_assert(alignof(T) == 8);
  static_assert(sizeof(T) == 8);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)131, (char)137, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_alignas::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_2") {
    using DT = test_alignas::dummy_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_4") {
    using DT = test_alignas::dummy_4;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_8") {
    using DT = test_alignas::dummy_8;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
}
namespace test_alignas {
struct alignas(4) A {
  char a;
  short b;
};
struct alignas(8) B {
  char a;
  int b;
};
struct alignas(16) Outer {
  A a;
  B b;
};

}  // namespace test_alignas

TEST_CASE("testing nesting alignas") {
  using T = test_alignas::Outer;
  static_assert(std::alignment_of_v<T> == 16);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 8);
  static_assert(struct_pack::detail::align::alignment_v<T> == 16);
  static_assert(alignof(T) == 16);
  static_assert(sizeof(T) == 16);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 8);
  auto literal = struct_pack::get_type_literal<T>();
  // clang-format off
  string_literal<char, 16> val{{(char)-3,
                               (char)-3,12, 7, (char)131,  (char)133, (char)-1,
                               (char)-3,12, 1, (char)133,  (char)137, (char)-1,
                                      (char)137, (char)145, (char)-1}};
  // clang-format on
  REQUIRE(literal == val);
}
TEST_SUITE_END;
