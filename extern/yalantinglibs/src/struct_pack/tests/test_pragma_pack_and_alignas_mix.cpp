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
TEST_SUITE_BEGIN("test_pragma_pack_and_alignas_mix");
namespace test_pragma_pack_and_alignas_mix {

template <typename A, typename B>
bool operator==(const A& a, const B& b) {
  return a.a == b.a && a.b == b.b;
}
struct dummy {
  char a;
  short b;
};
}  // namespace test_pragma_pack_and_alignas_mix
TEST_CASE("testing no one") {
  using T = test_pragma_pack_and_alignas_mix::dummy;
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
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(1)
struct alignas(2) dummy_1_2 {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
template <>
constexpr std::size_t
    struct_pack::pack_alignment_v<test_pragma_pack_and_alignas_mix::dummy_1_2> =
        1;
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(1)
struct alignas(2) dummy_1_2_forget {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
TEST_CASE("testing #pragam pack(1), alignas(2)") {
  using T = test_pragma_pack_and_alignas_mix::dummy_1_2;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 1);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)130, (char)131, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_pragma_pack_and_alignas_mix::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_1_2") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
  SUBCASE("deserialize to dummy_1_2_forget") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_2_forget;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
}
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(1)
struct alignas(4) dummy_1_4 {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
template <>
constexpr std::size_t
    struct_pack::pack_alignment_v<test_pragma_pack_and_alignas_mix::dummy_1_4> =
        1;
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(1)
struct alignas(2) dummy_1_4_forget {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
TEST_CASE("testing #pragam pack(1), alignas(2)") {
  using T = test_pragma_pack_and_alignas_mix::dummy_1_4;
  static_assert(std::alignment_of_v<T> == 4);
  static_assert(struct_pack::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 1);
  static_assert(struct_pack::detail::align::alignment_v<T> == 4);
  static_assert(alignof(T) == 4);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 1);
  T t{'a', 666};
  auto literal = struct_pack::get_type_literal<T>();
  string_literal<char, 6> val{
      {(char)-3, 12, 7, (char)130, (char)133, (char)-1}};
  REQUIRE(literal == val);
  auto buf = struct_pack::serialize(t);
  SUBCASE("deserialize to dummy") {
    using DT = test_pragma_pack_and_alignas_mix::dummy;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_1_2") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_2;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_1_2_forget") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_2_forget;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
  SUBCASE("deserialize to dummy_1_4") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_4;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(ret, ret.error().message());
    DT d_t = ret.value();
    CHECK(t == d_t);
  }
  SUBCASE("deserialize to dummy_1_4_forget") {
    using DT = test_pragma_pack_and_alignas_mix::dummy_1_4_forget;
    auto ret = struct_pack::deserialize<DT>(buf);
    REQUIRE_MESSAGE(!ret, alignment_requirement_err_msg);
  }
}

namespace test_pragma_pack_and_alignas_mix {
#pragma pack(4)
struct alignas(2) dummy_4_2 {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
template <>
constexpr std::size_t
    struct_pack::pack_alignment_v<test_pragma_pack_and_alignas_mix::dummy_4_2> =
        4;
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(4)
struct alignas(2) dummy_4_2_forget {
  char a;
  short b;
};
#pragma pack()
}  // namespace test_pragma_pack_and_alignas_mix
TEST_CASE("testing #pragam pack(4), alignas(2)") {
  using T = test_pragma_pack_and_alignas_mix::dummy_4_2;
  static_assert(std::alignment_of_v<T> == 2);
  static_assert(struct_pack::pack_alignment_v<T> == 4);
  static_assert(struct_pack::detail::align::pack_alignment_v<T> == 4);
  static_assert(struct_pack::detail::align::alignment_v<T> == 2);
  static_assert(alignof(T) == 2);
  static_assert(sizeof(T) == 4);
  static_assert(offsetof(T, a) == 0);
  static_assert(offsetof(T, b) == 2);
  // auto buf = struct_pack::serialize(t);
  // cannot compile
  // #pragma pack may decrease the alignment of a class,
  // however, it cannot make a class over aligned.
}
namespace test_pragma_pack_and_alignas_mix {
#pragma pack(1)
struct alignas(4) A {
  char a;
  short b;
};
#pragma pack()
#pragma pack(2)
struct alignas(8) B {
  char a;
  int b;
};
#pragma pack()
struct alignas(16) Outer {
  A a;
  B b;
};
}  // namespace test_pragma_pack_and_alignas_mix
template <>
constexpr std::size_t
    struct_pack::pack_alignment_v<test_pragma_pack_and_alignas_mix::A> = 1;
template <>
constexpr std::size_t
    struct_pack::pack_alignment_v<test_pragma_pack_and_alignas_mix::B> = 2;
TEST_CASE("testing mix nested") {
  using T = test_pragma_pack_and_alignas_mix::Outer;
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
                                (char)-3,12, 7, (char)130,  (char)133, (char)-1,
                                (char)-3,12, 1, (char)131,  (char)137, (char)-1,
                                      (char)137, (char)145, (char)-1}};
  // clang-format on
  REQUIRE(literal == val);
}

namespace test_pragma_pack_and_alignas_and_compatible_mix {
#pragma pack(2)
struct alignas(4) dummy_2_4_v1 {
  char a;
  int b;
  bool operator==(const dummy_2_4_v1& o) const { return a == o.a && b == o.b; }
};
#pragma pack()
#pragma pack(2)
struct alignas(4) dummy_2_4_v2 {
  char a;
  struct_pack::compatible<int> c;
  int b;
  bool operator==(const dummy_2_4_v2& o) const {
    return a == o.a && b == o.b && c == o.c;
  }
};
#pragma pack()

}  // namespace test_pragma_pack_and_alignas_and_compatible_mix

template <>
constexpr std::size_t struct_pack::pack_alignment_v<
    test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v1> = 2;
template <>
constexpr std::size_t struct_pack::pack_alignment_v<
    test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v2> = 2;
template <>
constexpr std::size_t struct_pack::alignment_v<
    test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v2> = 4;
TEST_CASE("testing mix and compatible nested") {
  {
    using T = test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v1;
    static_assert(std::alignment_of_v<T> == 4);
    static_assert(struct_pack::pack_alignment_v<T> == 2);
    static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
    static_assert(struct_pack::detail::align::alignment_v<T> == 4);
    static_assert(alignof(T) == 4);
    static_assert(sizeof(T) == 8);
    static_assert(offsetof(T, a) == 0);
    static_assert(offsetof(T, b) == 2);
    auto literal = struct_pack::get_type_literal<T>();
    // clang-format off
    string_literal<char, 6> val{{
                                (char)-3,12, 1, (char)131,  (char)133, (char)-1}};
    // clang-format on
    CHECK(literal == val);
  }
  {
    using T = test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v2;
    static_assert(std::alignment_of_v<T> == 4);
    static_assert(struct_pack::pack_alignment_v<T> == 2);
    static_assert(struct_pack::detail::align::pack_alignment_v<T> == 2);
    static_assert(struct_pack::detail::align::alignment_v<T> == 4);
    static_assert(alignof(T) == 4);
    auto literal = struct_pack::get_type_literal<T>();
    // clang-format off
    string_literal<char, 6> val{{
                                (char)-3,12, 1, (char)131,  (char)133, (char)-1}};
    // clang-format on
    CHECK(literal == val);
  }
  test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v1 v1 = {1,
                                                                      123321};
  test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v2 v2 = {
      1, std::nullopt, 123321};
  {
    auto buffer = struct_pack::serialize(v1);
    auto result = struct_pack::deserialize<
        test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v2>(buffer);
    CHECK(result.has_value() == true);
    auto val = result.value();
    CHECK(val == v2);
  }
  {
    auto buffer = struct_pack::serialize(v2);
    auto result = struct_pack::deserialize<
        test_pragma_pack_and_alignas_and_compatible_mix::dummy_2_4_v1>(buffer);
    CHECK(result.has_value() == true);
    auto val = result.value();
    CHECK(val == v1);
  }
}

TEST_SUITE_END;
