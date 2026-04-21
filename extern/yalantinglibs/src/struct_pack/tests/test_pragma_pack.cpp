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
#include <map>
#include <type_traits>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
using namespace struct_pack;
using namespace doctest;
// clang-format off
static std::string alignment_requirement_err_msg = "different alignment requirement, we must regard as different type";
// clang-format on
TEST_SUITE_BEGIN("test_pragma_pack");
namespace test_pragma_pack {

template <typename A, typename B>
bool operator==(const A& a, const B& b) {
  return a.a == b.a && a.b == b.b;
}
struct dummy {
  char a;
  short b;
};
struct dummy2 {
  struct nest {
    int a;
    char ch[15];
    double c;
  };
  std::map<int, std::vector<nest>> hi;
};
struct dummy3 {
  struct nest {
    struct_pack::compatible<int, 10234> ewr2;
    int a;
    char ch[15];
    struct_pack::compatible<int> ewr;
    double c;
  };
  struct_pack::compatible<int, 1021234> ewr2;
  std::map<int, std::vector<nest>> hi;
};

}  // namespace test_pragma_pack
TEST_CASE("testing no #pragam pack") {
  using T = test_pragma_pack::dummy;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<test_pragma_pack::dummy>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)131, (char)131, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  auto ret = struct_pack::deserialize<T>(buf);
  REQUIRE_MESSAGE(ret, ret.error().message());
  T d_t = ret.value();
  CHECK(t == d_t);
}
namespace test_pragma_pack {
#pragma pack(1)
struct dummy_1 {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack
template <>
constexpr std::size_t struct_pack::pack_alignment_v<test_pragma_pack::dummy_1> =
    1;
namespace test_pragma_pack {}  // namespace test_pragma_pack
TEST_CASE("testing #pragma pack(1)") {
  using T = test_pragma_pack::dummy_1;
  static_assert(std::alignment_of_v<T> == 1);
  static_assert(struct_pack::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::alignment_v<T> == 1);
  static_assert(alignof(T) == 1);
  static_assert(sizeof(T) == 3);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 1);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)130, (char)130, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_pragma_pack::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_1") {
    using DT = test_pragma_pack::dummy_1;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
}
namespace test_pragma_pack {
#pragma pack(2)
struct dummy_2 {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack
template <>
constexpr std::size_t struct_pack::pack_alignment_v<test_pragma_pack::dummy_2> =
    2;
namespace test_pragma_pack {}  // namespace test_pragma_pack
TEST_CASE("testing #pragma pack(2)") {
  using T = test_pragma_pack::dummy_2;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 2);
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
    using DT = test_pragma_pack::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
  }
  SUBCASE("deserialize to dummy_1") {
    using DT = test_pragma_pack::dummy_1;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_2") {
    using DT = test_pragma_pack::dummy_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
}

namespace test_pragma_pack {
#pragma pack(1)
struct A {
  char a;
  short b;
};
#pragma pack()
#pragma pack(2)
struct B {
  char a;
  int b;
};
#pragma pack()
struct Outer {
  A a;
  B b;
};
#pragma pack(1)
struct Outer_1 {
  A a;
  B b;
};
#pragma pack()
}  // namespace test_pragma_pack
template <>
constexpr std::size_t struct_pack::pack_alignment_v<test_pragma_pack::A> = 1;
template <>
constexpr std::size_t struct_pack::pack_alignment_v<test_pragma_pack::B> = 2;
template <>
constexpr std::size_t struct_pack::pack_alignment_v<test_pragma_pack::Outer_1> =
    1;
TEST_CASE("testing nesting #pragma pack()") {
  using T = test_pragma_pack::Outer;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 0);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 10);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 4);
  auto literal = struct_pack::get_type_literal<T>();
  // clang-format off
  string_literal<char, 16> val{{(char)-3,
                                (char)-3,12, 7, (char)130,(char) 130, (char)-1,
                                (char)-3,12, 1, (char)131,(char) 131, (char)-1,
                                      (char)131, (char)131, (char)-1}};
  // clang-format on
  REQUIRE(literal == val);
}
TEST_CASE("testing nesting #pragma pack(), outer pack(1)") {
  using T = test_pragma_pack::Outer_1;
  static_assert(std::alignment_of_v<T> == 1);
  static_assert(struct_pack::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::alignment_v<T> == 1);
  static_assert(alignof(T) == 1);
  static_assert(sizeof(T) == 9);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 3);
  auto literal = struct_pack::get_type_literal<T>();
  // clang-format off
  string_literal<char, 16> val{{(char)-3,
                                (char)-3,12, 7, (char)130,(char) 130, (char)-1,
                                (char)-3,12, 1, (char)131, (char)131, (char)-1,
                                       (char)130, (char)130, (char)-1}};
  // clang-format on
  REQUIRE(literal == val);
}
static_assert(struct_pack::get_type_literal<test_pragma_pack::dummy2>() ==
              struct_pack::get_type_literal<test_pragma_pack::dummy3>());

TEST_SUITE_END;
