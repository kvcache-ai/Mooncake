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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ratio>
#include <string_view>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "ylt/struct_pack/compatible.hpp"
#include "ylt/struct_pack/endian_wrapper.hpp"
#include "ylt/struct_pack/error_code.hpp"
#include "ylt/struct_pack/reflection.hpp"

#define private public
#include <ylt/struct_pack.hpp>
#undef private

#include "doctest.h"
#include "test_struct.hpp"
using namespace struct_pack;

TEST_CASE("testing deserialize") {
  person p{32, "tom"};
  auto ret = serialize(p);
  {
    auto res = deserialize<person>(ret);
    CHECK(res);
    CHECK(res.value() == p);
  }
  {
    size_t len = 114514;
    auto res = deserialize<person>(ret, len);
    CHECK(res);
    CHECK(res.value() == p);
    CHECK(len == ret.size());
  }
  {
    size_t offset = 0;
    auto res = deserialize_with_offset<person>(ret, offset);
    CHECK(res);
    CHECK(res.value() == p);
    CHECK(offset == ret.size());
  }
  {
    person p2;
    auto res = deserialize_to(p2, ret);
    CHECK(!res);
    CHECK(p2 == p);
  }
  {
    size_t len = 114514;
    person p2;
    auto res = deserialize_to(p2, ret, len);
    CHECK(!res);
    CHECK(p2 == p);
    CHECK(len == ret.size());
  }
  {
    size_t offset = 0;
    person p2;
    auto res = deserialize_to(p2, ret, offset);
    CHECK(!res);
    CHECK(p2 == p);
    CHECK(offset == ret.size());
  }
  {
    person p2;
    auto res = get_field<person, 1>(ret);
    CHECK(res);
    CHECK(res.value() == p.name);
  }
}

TEST_CASE("testing api") {
  person p{16, "jack"};
  int offset = 20;
  SUBCASE("return buffer") {
    auto ret1 = serialize(p);
    auto ret2 = serialize_with_offset(offset, p);
    // TODO: CHECK offset
    CHECK(ret1.size() == ret2.size() - offset);
    auto p1 = deserialize<person>(ret1.data(), ret1.size());
    CHECK(p1);
    auto p2 = deserialize<person>(ret2.data() + offset, ret2.size() - offset);
    CHECK(p2);
    CHECK(p == p1.value());
    CHECK(p == p2.value());
  }
  SUBCASE("serialize to") {
    auto size = get_needed_size(p);
    std::vector<std::byte> my_buffer, my_buffer2;
    my_buffer.resize(size);
    my_buffer2.resize(size + offset);
    serialize_to((char *)my_buffer.data(), size, p);
    serialize_to((char *)my_buffer2.data() + offset, size, p);
    person p1, p2;
    auto ec1 = deserialize_to(p1, my_buffer);
    CHECK(!ec1);
    auto ec2 = deserialize_to(p2, (const char *)my_buffer2.data() + offset,
                              my_buffer2.size() - offset);
    CHECK(!ec2);
    CHECK(p == p1);
    CHECK(p == p2);
  }
  SUBCASE("append buffer") {
    std::vector<unsigned char> my_buffer, my_buffer2;
    serialize_to(my_buffer, p);
    serialize_to_with_offset(my_buffer2, offset, p);
    CHECK(my_buffer.size() == my_buffer2.size() - offset);
    auto ret1 = get_field<person, 0>(my_buffer);
    CHECK(ret1);
    auto ret2 = get_field<person, 0>((const char *)my_buffer2.data() + offset,
                                     my_buffer2.size() - offset);
    CHECK(ret2);
    CHECK(ret1.value() == p.age);
    CHECK(ret2.value() == p.age);
    auto ret3 = get_field<person, 1>(my_buffer);
    CHECK(ret3);
    auto ret4 = get_field<person, 1>((const char *)my_buffer2.data() + offset,
                                     my_buffer2.size() - offset);
    CHECK(ret4);
    CHECK(ret3.value() == p.name);
    CHECK(ret4.value() == p.name);
  }
}

TEST_CASE("testing pack object") {
  person p{20, "tom"};
  auto ret = serialize(p);

  person p1{};
  auto ec = deserialize_to(p1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(p == p1);

  CHECK(deserialize_to(p1, ret.data(), 4) ==
        struct_pack::errc::no_buffer_space);

  SUBCASE("test empty string") {
    p.name = "";
    ret = serialize(p);
    person p2{};
    ec = deserialize_to(p2, ret.data(), ret.size());
    CHECK(!ec);
    CHECK(p == p2);
  }
}

template <typename T>
void test_container(T &v) {
  auto ret = serialize(v);

  T v1{};
  auto ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v == v1);

  v.clear();
  v1.clear();
  ret = serialize(v);
  ec = deserialize_to(v1, ret.data(), ret.size());
  CHECK(!ec);
  CHECK(v1.empty());
  CHECK(v == v1);
}

TEST_CASE("testing exceptions") {
  std::vector<int> data_list = {1, 2, 3};
  std::vector<std::byte> my_byte_buffer;
  serialize_to(my_byte_buffer, data_list);
  std::vector<int> data_list2;
  auto ec = deserialize_to(data_list2, (const char *)my_byte_buffer.data(), 0);
  CHECK(ec == struct_pack::errc::no_buffer_space);
  ec = deserialize_to(data_list2, (const char *)my_byte_buffer.data(),
                      my_byte_buffer.size() - 1);
  CHECK(ec == struct_pack::errc::no_buffer_space);

  std::vector<std::byte> my_byte_buffer2;
  std::optional<bool> bool_opt;
  serialize_to(my_byte_buffer2, bool_opt);
  auto ret3 = deserialize<std::optional<bool>>(
      (const char *)my_byte_buffer2.data(), my_byte_buffer2.size() - 1);
  CHECK(!ret3);
  if (!ret3) {
    CHECK(ret3.error() == struct_pack::errc::no_buffer_space);
  }
}
using namespace std::string_literals;
TEST_CASE("testing serialize/deserialize variadic params") {
  {
    person p{24, "Betty"};
    auto buffer = struct_pack::serialize(p);
    auto res = struct_pack::deserialize<int, std::string>(buffer);
    CHECK(res);
    CHECK(std::get<0>(res.value()) == 24);
    CHECK(std::get<1>(res.value()) == "Betty");
  }
  {
    person p{24, "Betty"};
    auto buffer = struct_pack::serialize(24, "Betty"s);
    auto res = struct_pack::deserialize<person>(buffer);
    CHECK(res);
    CHECK(res.value() == p);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    auto res = struct_pack::deserialize<std::tuple<int, int, int, int, int>>(
        ret.data(), ret.size());
    CHECK(res);
    CHECK(std::get<0>(res.value()) == 1);
    CHECK(std::get<1>(res.value()) == 2);
    CHECK(std::get<2>(res.value()) == 3);
    CHECK(std::get<3>(res.value()) == 4);
    CHECK(std::get<4>(res.value()) == 5);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    auto res =
        struct_pack::deserialize<std::tuple<int, int, int, int, int>>(ret);
    CHECK(res);
    CHECK(std::get<0>(res.value()) == 1);
    CHECK(std::get<1>(res.value()) == 2);
    CHECK(std::get<2>(res.value()) == 3);
    CHECK(std::get<3>(res.value()) == 4);
    CHECK(std::get<4>(res.value()) == 5);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    auto res = struct_pack::deserialize<int, int, int, int, int>(ret.data(),
                                                                 ret.size());
    CHECK(res);
    CHECK(std::get<0>(res.value()) == 1);
    CHECK(std::get<1>(res.value()) == 2);
    CHECK(std::get<2>(res.value()) == 3);
    CHECK(std::get<3>(res.value()) == 4);
    CHECK(std::get<4>(res.value()) == 5);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    auto res = struct_pack::deserialize<int, int, int, int, int>(ret);
    CHECK(res);
    CHECK(std::get<0>(res.value()) == 1);
    CHECK(std::get<1>(res.value()) == 2);
    CHECK(std::get<2>(res.value()) == 3);
    CHECK(std::get<3>(res.value()) == 4);
    CHECK(std::get<4>(res.value()) == 5);
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(ret, 1, 2, 3, 4, 5);
    auto res = struct_pack::deserialize<int, int, int, int, int>(ret.data(),
                                                                 ret.size());
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 1);
    CHECK(std::get<1>(values) == 2);
    CHECK(std::get<2>(values) == 3);
    CHECK(std::get<3>(values) == 4);
    CHECK(std::get<4>(values) == 5);
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(ret, 1, 2, 3, 4, 5);
    auto res = struct_pack::deserialize<int, int, int, int, int>(ret);
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 1);
    CHECK(std::get<1>(values) == 2);
    CHECK(std::get<2>(values) == 3);
    CHECK(std::get<3>(values) == 4);
    CHECK(std::get<4>(values) == 5);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    int a[5];
    auto res = struct_pack::deserialize_to(a[0], ret.data(), ret.size(), a[1],
                                           a[2], a[3], a[4]);
    CHECK(!res);
    CHECK(a[0] == 1);
    CHECK(a[1] == 2);
    CHECK(a[2] == 3);
    CHECK(a[3] == 4);
    CHECK(a[4] == 5);
  }
  {
    auto ret = struct_pack::serialize(1, 2, 3, 4, 5);
    int a[5];
    auto res = struct_pack::deserialize_to(a[0], ret, a[1], a[2], a[3], a[4]);
    CHECK(!res);
    CHECK(a[0] == 1);
    CHECK(a[1] == 2);
    CHECK(a[2] == 3);
    CHECK(a[3] == 4);
    CHECK(a[4] == 5);
  }
  {
    auto ret = struct_pack::serialize(
        42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res = struct_pack::deserialize<std::tuple<
        int, float, double, int, unsigned long long, std::string,
        std::array<std::vector<int>, 3>, std::variant<int, float, double>>>(
        ret.data(), ret.size());
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    auto ret = struct_pack::serialize(
        42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res = struct_pack::deserialize<std::tuple<
        int, float, double, int, unsigned long long, std::string,
        std::array<std::vector<int>, 3>, std::variant<int, float, double>>>(
        ret);
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    auto ret = struct_pack::serialize(
        42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res =
        struct_pack::deserialize<int, float, double, int, unsigned long long,
                                 std::string, std::array<std::vector<int>, 3>,
                                 std::variant<int, float, double>>(ret.data(),
                                                                   ret.size());
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    auto ret = struct_pack::serialize(
        42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res =
        struct_pack::deserialize<int, float, double, int, unsigned long long,
                                 std::string, std::array<std::vector<int>, 3>,
                                 std::variant<int, float, double>>(ret);
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(
        ret, 42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res =
        struct_pack::deserialize<int, float, double, int, unsigned long long,
                                 std::string, std::array<std::vector<int>, 3>,
                                 std::variant<int, float, double>>(ret.data(),
                                                                   ret.size());
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(
        ret, 42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    auto res =
        struct_pack::deserialize<int, float, double, int, unsigned long long,
                                 std::string, std::array<std::vector<int>, 3>,
                                 std::variant<int, float, double>>(ret);
    CHECK(res);
    auto &values = res.value();
    CHECK(std::get<0>(values) == 42);
    CHECK(std::get<1>(values) == 2.71828f);
    CHECK(std::get<2>(values) == 3.1415926);
    CHECK(std::get<3>(values) == 71086291);
    CHECK(std::get<4>(values) == 1145141919810Ull);
    CHECK(std::get<5>(values) == std::string{"Hello"});
    CHECK(std::get<6>(values) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(values) == std::variant<int, float, double>{1.4});
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(
        ret, 42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    std::tuple<int, float, double, int, unsigned long long, std::string,
               std::array<std::vector<int>, 3>,
               std::variant<int, float, double>>
        t;
    auto res = struct_pack::deserialize_to(
        std::get<0>(t), ret.data(), ret.size(), std::get<1>(t), std::get<2>(t),
        std::get<3>(t), std::get<4>(t), std::get<5>(t), std::get<6>(t),
        std::get<7>(t));
    CHECK(!res);
    CHECK(std::get<0>(t) == 42);
    CHECK(std::get<1>(t) == 2.71828f);
    CHECK(std::get<2>(t) == 3.1415926);
    CHECK(std::get<3>(t) == 71086291);
    CHECK(std::get<4>(t) == 1145141919810Ull);
    CHECK(std::get<5>(t) == std::string{"Hello"});
    CHECK(std::get<6>(t) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(t) == std::variant<int, float, double>{1.4});
  }
  {
    std::vector<char> ret;
    struct_pack::serialize_to(
        ret, 42, 2.71828f, 3.1415926, 71086291, 1145141919810Ull,
        std::string{"Hello"},
        std::array<std::vector<int>, 3>{
            {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}},
        std::variant<int, float, double>{1.4});
    std::tuple<int, float, double, int, unsigned long long, std::string,
               std::array<std::vector<int>, 3>,
               std::variant<int, float, double>>
        t;
    auto res = struct_pack::deserialize_to(
        std::get<0>(t), ret, std::get<1>(t), std::get<2>(t), std::get<3>(t),
        std::get<4>(t), std::get<5>(t), std::get<6>(t), std::get<7>(t));
    CHECK(!res);
    CHECK(std::get<0>(t) == 42);
    CHECK(std::get<1>(t) == 2.71828f);
    CHECK(std::get<2>(t) == 3.1415926);
    CHECK(std::get<3>(t) == 71086291);
    CHECK(std::get<4>(t) == 1145141919810Ull);
    CHECK(std::get<5>(t) == std::string{"Hello"});
    CHECK(std::get<6>(t) ==
          std::array<std::vector<int>, 3>{
              {{1, 1, 4, 5, 1, 4}, {1919, 810}, {710862, 91}}});
    CHECK(std::get<7>(t) == std::variant<int, float, double>{1.4});
  }
}

TEST_CASE("testing deserialization") {
  person p{20, "tom"};
  auto ret = serialize(p);

  person p1{};
  CHECK(!deserialize_to(p1, ret.data(), ret.size()));
}

TEST_CASE("testing deserialization with invalid data") {
  auto ret = serialize(-1);

  std::string str{};
  auto ret2 = deserialize_to(str, ret.data(), ret.size());
  CHECK(ret2 == struct_pack::errc::invalid_buffer);

  ret2 = deserialize_to(str, ret.data(), INT32_MAX);
  CHECK(ret2 == struct_pack::errc::invalid_buffer);

  SUBCASE("test invalid int") {
    ret = serialize(std::string("hello"));

    std::tuple<int, int> tp{};
    auto ret3 = deserialize_to(tp, ret.data(), ret.size());
    CHECK(ret3 == struct_pack::errc::invalid_buffer);
  }
}

TEST_CASE("test get type code") {
  constexpr auto r = get_type_code<person>();
  constexpr auto r1 = get_type_code<nested_object>();
  constexpr auto r2 = get_type_code<int>();
  constexpr auto r3 = get_type_code<std::string>();
  std::set results = {r, r1, r2, r3};
  CHECK(results.size() == 4);

  SUBCASE("test get_types_code") {
    constexpr auto c = get_type_code<int, std::string>();
    constexpr auto c1 = get_type_code<int, std::string, person>();
    constexpr auto c2 = get_type_code<int, float, double>();
    constexpr auto c3 = get_type_code<int32_t, uint32_t>();
    constexpr auto c4 = get_type_code<uint32_t, int32_t>();
    constexpr auto c5 = get_type_code<int32_t>();
    constexpr auto c6 = get_type_code<uint32_t>();

    std::set set{c, c1, c2, c3, c4, c5, c6};
    CHECK(set.size() == 7);
  }

  SUBCASE("test get_types") {
    using namespace struct_pack::detail;
    static_assert(std::is_same_v<std::tuple<int, std::string>,
                                 decltype(get_types<person>())>);

    static_assert(
        std::is_same_v<std::tuple<int, std::string>,
                       decltype(get_types<std::tuple<int, std::string>>())>);

    static_assert(std::is_same_v<std::tuple<int>, decltype(get_types<int>())>);

    static_assert(std::is_same_v<std::tuple<std::vector<int>>,
                                 decltype(get_types<std::vector<int>>())>);

    static_assert(std::is_same_v<decltype(get_types<person>()),
                                 std::tuple<int, std::string>>);
    static_assert(std::is_same_v<
                  decltype(get_types<nested_object>()),
                  std::tuple<int, std::string, person, complicated_object>>);
  }

  SUBCASE("test get_type_literal") {
    auto str = get_type_literal<int, int, short>();
    static_assert(str.size() == 5);
    string_literal<char, 5> val{
        {(char)-3, 1, 1, 7, (char)-1}};  //{'1','1','7'};
    CHECK(str == val);

    auto str1 = get_type_literal<int64_t, uint64_t>();
    static_assert(str1.size() == 4);
    string_literal<char, 4> val1{{(char)-3, 3, 4, (char)-1}};  //{'3','4'};
    CHECK(str1 == val1);

    auto str2 =
        get_type_literal<int64_t, uint64_t, struct_pack::compatible<int32_t>>();
    static_assert(str2.size() == 4);
    string_literal<char, 4> val2{{(char)-3, 3, 4, (char)-1}};
    CHECK(str2 == val2);
  }
}
TEST_CASE("testing partial deserialization with index") {
  person p{20, "tom"};
  nested_object nested{};
  auto ret = serialize(p);

  auto pair = get_field<person, 1>(ret.data(), ret.size());
  REQUIRE(pair);
  CHECK(pair.value() == "tom");

#if __cplusplus >= 202002L
  auto pair1 =
      get_field<tuplet::tuple<int, std::string>, 0>(ret.data(), ret.size());
  REQUIRE(pair1);
  CHECK(pair1.value() == 20);
#endif
}

namespace array_test {
std::array<std::string, 1> ar0;
std::array<std::string, 126> ar1;
std::array<std::string, 127> ar2;
std::array<std::string, 127 * 127 - 1> ar3;
std::array<std::string, 127 * 127 - 1> ar3_1;
std::array<std::string, 127 * 127> ar4;
std::array<std::string, 127 * 127> ar4_1;
}  // namespace array_test

TEST_CASE("array test") {
  std::string test_str = "Hello Hi Hello Hi Hello Hi Hello Hi Hello Hi";
  using namespace array_test;
  {
    ar0[0] = test_str;
    auto ret = deserialize<decltype(ar0)>(serialize(ar0));
    CHECK(ret);
    CHECK(ret.value()[0] == test_str);
  }
  {
    ar1[7] = test_str;
    auto ret = deserialize<decltype(ar1)>(serialize(ar1));
    CHECK(ret);
    CHECK(ret.value()[7] == test_str);
  }
  {
    ar2[56] = test_str;
    auto ret = deserialize<decltype(ar2)>(serialize(ar2));
    CHECK(ret);
    CHECK(ret.value()[56] == test_str);
  }
  {
    ar3[117] = test_str;
    [[maybe_unused]] auto ret = deserialize_to(ar3_1, serialize(ar3));
    CHECK(!ret);
    CHECK(ar3_1[117] == test_str);
  }
  {
    ar4[15472] = test_str;
    auto ret = deserialize_to(ar4_1, serialize(ar4));
    CHECK(!ret);
    CHECK(ar4_1[15472] == test_str);
  }
}

struct person_with_type_info {
  int age;
  std::string name;
};

constexpr inline struct_pack::sp_config set_sp_config(person_with_type_info *) {
  return sp_config::ENABLE_TYPE_INFO;
}

struct person_with_no_type_info {
  int age;
  std::string name;
};

constexpr inline struct_pack::sp_config set_sp_config(
    person_with_no_type_info *) {
  return sp_config::DISABLE_TYPE_INFO;
}

TEST_CASE("test type info config") {
  SUBCASE("test_person") {
#ifdef NDEBUG
    {
      auto size = get_needed_size(person{24, "Betty"});
      CHECK(size == 14);
      auto buffer = serialize(person{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DEFAULT, person>() ==
          false);
    }
#else
    {
      auto size = get_needed_size(person{24, "Betty"});
      CHECK(size == 21);
      auto buffer = serialize(person{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DEFAULT, person>() ==
          true);
    }
#endif
    {
      auto size =
          get_needed_size<sp_config::DISABLE_TYPE_INFO>(person{24, "Betty"});
      CHECK(size == 14);
      auto buffer =
          serialize<sp_config::DISABLE_TYPE_INFO>(person{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DISABLE_TYPE_INFO,
                                            person>() == false);
    }
    {
      auto size =
          get_needed_size<sp_config::ENABLE_TYPE_INFO>(person{24, "Betty"});
      CHECK(size == 21);
      auto buffer = serialize<sp_config::ENABLE_TYPE_INFO>(person{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::ENABLE_TYPE_INFO,
                                            person>() == true);
    }
  }
  SUBCASE("test_person_with_type_info") {
    {
      auto size = get_needed_size(person_with_type_info{24, "Betty"});
      CHECK(size == 21);
      auto buffer = serialize(person_with_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DEFAULT,
                                            person_with_type_info>() == true);
    }
    {
      auto size = get_needed_size<sp_config::DISABLE_TYPE_INFO>(
          person_with_type_info{24, "Betty"});
      CHECK(size == 14);
      auto buffer = serialize<sp_config::DISABLE_TYPE_INFO>(
          person_with_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DISABLE_TYPE_INFO,
                                            person_with_type_info>() == false);
    }
    {
      auto size = get_needed_size<sp_config::ENABLE_TYPE_INFO>(
          person_with_type_info{24, "Betty"});
      CHECK(size == 21);
      auto buffer = serialize<sp_config::ENABLE_TYPE_INFO>(
          person_with_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::ENABLE_TYPE_INFO,
                                            person_with_type_info>() == true);
    }
  }
  SUBCASE("test_person_with_no_type_info") {
    {
      auto size = get_needed_size(person_with_no_type_info{24, "Betty"});
      CHECK(size == 14);
      auto buffer = serialize(person_with_no_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DEFAULT,
                                            person_with_no_type_info>() ==
          false);
    }
    {
      auto size = get_needed_size<sp_config::DISABLE_TYPE_INFO>(
          person_with_no_type_info{24, "Betty"});
      CHECK(size == 14);
      auto buffer = serialize<sp_config::DISABLE_TYPE_INFO>(
          person_with_no_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::DISABLE_TYPE_INFO,
                                            person_with_no_type_info>() ==
          false);
    }
    {
      auto size = get_needed_size<sp_config::ENABLE_TYPE_INFO>(
          person_with_no_type_info{24, "Betty"});
      CHECK(size == 21);
      auto buffer = serialize<sp_config::ENABLE_TYPE_INFO>(
          person_with_no_type_info{24, "Betty"});
      CHECK(buffer.size() == size);
      static_assert(
          detail::check_if_add_type_literal<sp_config::ENABLE_TYPE_INFO,
                                            person_with_no_type_info>() ==
          true);
    }
  }
}

template <typename T>
void test_no_buffer_space(T &t, std::vector<int> size_list) {
  auto ret = serialize(t);
  T t1{};
  for (auto size : size_list) {
    CHECK(deserialize_to(t1, ret.data(), size) ==
          struct_pack::errc::no_buffer_space);
  }
}

TEST_CASE("test more exceptions") {
  std::map<std::string, std::string> map{{"hello", "struct pack"}};
  test_no_buffer_space(map, {4, 11});
  std::vector<std::string> vec{"hello struct pack"};
  test_no_buffer_space(vec, {2, 4, 8});
  std::set<std::string> set{"hello struct pack"};
  test_no_buffer_space(set, {0, 4, 8});
  int t = 42;
  test_no_buffer_space(t, {4});
  std::array<std::string, 1> arr{"hello struct pack"};
  test_no_buffer_space(arr, {8});
}

TEST_CASE("test get field exceptions") {
  person p{20, "tom"};
  auto ret = serialize(p);

  auto pair = get_field<std::pair<int, int>, 0>(ret.data(), ret.size());
  CHECK(!pair);
  if (!pair) {
    CHECK(pair.error() == struct_pack::errc::invalid_buffer);
  }

  auto pair1 = get_field<person, 0>(ret.data(), 3);
  CHECK(!pair1);
  if (!pair1) {
    CHECK(pair1.error() == struct_pack::errc::no_buffer_space);
  }
}

TEST_CASE("test set_value") {
  person p{20, "tom"};
  auto ret = serialize(p);
  detail::memory_reader reader(ret.data(), ret.data() + ret.size());
  detail::unpacker in(reader);
  person p2;
  struct_pack::err_code ec;
  std::string s;
  int v;
  int v2 = -1;
  auto ret1 = in.set_value<0, UINT64_MAX, 0, 0, int, int>(ec, v, std::move(v2));
  CHECK(ret1 == true);
  auto ret2 = in.set_value<0, UINT64_MAX, 0, 1, int, int>(ec, v, std::move(v2));
  CHECK(ret2 == false);
}

TEST_CASE("test free functions") {
  person p{20, "tom"};
  auto buffer = serialize(p);
  CHECK(!buffer.empty());

  person p1{};
  CHECK(!deserialize_to(p1, buffer.data(), buffer.size()));

  std::optional<int> op1{};
  auto buf1 = serialize(op1);
  std::optional<int> op2{};
  CHECK(!deserialize_to(op2, buf1.data(), buf1.size()));
  CHECK(!op2.has_value());

  op1 = 42;
  auto buf2 = serialize(op1);
  CHECK(!deserialize_to(op2, buf2.data(), buf2.size()));
  CHECK(op2.has_value());
  CHECK(op2.value() == 42);

  CHECK(deserialize_to(op2, buf2.data(), 1) ==
        struct_pack::errc::no_buffer_space);

  auto pair = get_field<person, 0>(buffer.data(), buffer.size());
  CHECK(pair);
  CHECK(pair.value() == 20);

  auto pair1 = get_field<person, 1>(buffer.data(), buffer.size());
  CHECK(pair1);
  CHECK(pair1.value() == "tom");
}

TEST_CASE("test variant size_type") {
  {
    std::string str(255, 'A');
    auto ret = serialize<sp_config::DISABLE_TYPE_INFO>(str);
    CHECK(ret.size() == 260);
    auto str2 = deserialize<std::string_view>(ret);
    CHECK(str == str2);
  }
  {
    std::string str(256, 'A');
    auto ret = serialize<sp_config::DISABLE_TYPE_INFO>(str);
    CHECK(ret[4] == 0b01000);
    CHECK(ret.size() == 263);
    auto str2 = deserialize<std::string_view>(ret);
    CHECK(str2);
    CHECK(str == str2);
  }
  {
    std::string str(65535, 'A');
    auto ret = serialize<sp_config::DISABLE_TYPE_INFO>(str);
    CHECK(ret[4] == 0b01000);
    CHECK(ret.size() == 65542);
    auto str2 = deserialize<std::string_view>(ret);
    CHECK(str2);
    CHECK(str == str2);
  }
  {
    std::string str(65536, 'A');
    auto ret = serialize<sp_config::DISABLE_TYPE_INFO>(str);
    CHECK(ret[4] == 0b10000);
    CHECK(ret.size() == 65545);
    auto str2 = deserialize<std::string_view>(ret);
    CHECK(str2);
    CHECK(str.size() == str2->size());
    CHECK(str == str2);
  }
  // test 8 bytes size_type need at least 4GB memory, so we don't test this case
  // {
  //   std::string str((1ull << 32) - 1, 'A');
  //   auto ret =
  //       serialize<std::string,sp_config::DISABLE_TYPE_INFO>(
  //           str);
  //   CHECK(ret[4] == 0b10000);
  //   CHECK(ret.size() == (1ull << 32) + 8);
  //   auto str2 = deserialize<std::string_view>(ret);
  //   CHECK(str2);
  //   CHECK(str == str2);
  // }
  // {
  //   std::string str((1ull << 32), 'A');
  //   auto ret =
  //       serialize<std::string,sp_config::DISABLE_TYPE_INFO>(
  //           str);
  //   CHECK(ret[4] == 0b11000);
  //   CHECK(ret.size() == (1ull << 32) + 13);
  //   auto str2 = deserialize<std::string_view>(ret);
  //   CHECK(str2);
  //   CHECK(str == str2);
  // }
}

TEST_CASE("test serialize offset") {
  uint32_t offset = 4;

  person p{20, "tom"};
  std::vector<char> buffer;
  auto info = get_needed_size(p);
  buffer.resize(info.size() + offset);
  serialize_to(buffer.data() + offset, info, p);
  person p2;
  CHECK(!deserialize_to(p2, buffer.data() + offset, info.size()));
  CHECK(p2 == p);

  std::vector<char> buffer2;
  buffer2.resize(10);
  auto original_size = buffer2.size();
  auto data_offset = original_size + offset;
  serialize_to_with_offset(buffer2, offset, p);
  CHECK(data_offset + info.size() == buffer2.size());
  person p3;
  CHECK(!deserialize_to(p3, buffer2.data() + data_offset, info.size()));
  CHECK(p3 == p);
}

TEST_CASE("test de_serialize offset") {
  std::string hi = "Hello struct_pack, hi struct_pack, hey struct_pack.";
  std::vector<char> ho;
  std::vector<std::size_t> sizes;
  for (int i = 0; i < 100; ++i) {
    struct_pack::serialize_to(ho, hi + std::to_string(i));
    sizes.push_back(ho.size());
  }
  for (size_t i = 0, offset = 0; i < 100; ++i) {
    auto ret = struct_pack::deserialize_with_offset<std::string>(
        ho.data(), ho.size(), offset);
    CHECK(ret);
    CHECK(ret.value() == hi + std::to_string(i));
    CHECK(offset == sizes[i]);
  }
  for (size_t i = 0, offset = 0; i < 100; ++i) {
    auto ret = struct_pack::deserialize_with_offset<std::string>(ho, offset);
    CHECK(ret);
    CHECK(ret.value() == hi + std::to_string(i));
    CHECK(offset == sizes[i]);
  }
  for (size_t i = 0, offset = 0; i < 100; ++i) {
    std::string res;
    auto ec = struct_pack::deserialize_to_with_offset(res, ho.data(), ho.size(),
                                                      offset);
    CHECK(!ec);
    CHECK(res == hi + std::to_string(i));
    CHECK(offset == sizes[i]);
  }
  for (size_t i = 0, offset = 0; i < 100; ++i) {
    std::string res;
    auto ec = struct_pack::deserialize_to_with_offset(res, ho, offset);
    CHECK(!ec);
    CHECK(res == hi + std::to_string(i));
    CHECK(offset == sizes[i]);
  }
}

TEST_CASE("compatible convert to optional") {
  std::optional<std::string> a = "hello world";
  struct_pack::compatible<std::string> b = a;
  a = b;
  CHECK(b.value() == "hello world");
  CHECK(a.value() == "hello world");
}
#if (__GNUC__ || __clang__) && defined(STRUCT_PACK_ENABLE_INT128)
struct test_int_128 {
  __int128_t x;
  __uint128_t y;
  bool operator==(const test_int_128 &o) const { return x == o.x && y == o.y; }
};

TEST_CASE("test 128-bit int") {
  __int128_t i = INT64_MAX;
  i *= INT64_MAX;
  CHECK(i > INT64_MAX);
  __uint128_t j = UINT64_MAX;
  j *= UINT64_MAX;
  CHECK(j > UINT64_MAX);
  auto vec = std::vector<test_int_128>{{i, j}, {-1 * i, j + UINT64_MAX}};
  auto buffer = struct_pack::serialize(vec);
  auto result = struct_pack::deserialize<std::vector<test_int_128>>(buffer);
  CHECK(result == vec);
  CHECK(struct_pack::detail::is_trivial_serializable<test_int_128>::value);
}

#endif

#if __cpp_lib_span >= 202002L

struct span_test {
  std::string hello;
  std::span<int, 4> sp;
};
template <>
constexpr std::size_t struct_pack::members_count<span_test> = 2;
struct span_test2 {
  std::string hello;
  std::array<int, 4> ar;
};

struct span_test3 {
  std::string hello;
  std::span<std::string, 4> sp;
};
template <>
constexpr std::size_t struct_pack::members_count<span_test3> = 2;
struct span_test4 {
  std::string hello;
  std::array<std::string, 4> ar;
};

struct span_test5 {
  std::string hello;
  std::span<person, 4> sp;
};
template <>
constexpr std::size_t struct_pack::members_count<span_test5> = 2;
struct span_test6 {
  std::string hello;
  std::array<person, 4> ar;
};

TEST_CASE("test static span") {
  SUBCASE("test static span<int,4>") {
    {
      std::array<int, 4> ar = {1, 4, 5, 3};
      span_test s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      span_test2 s2;
      auto res = struct_pack::deserialize<span_test2>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
    {
      std::array<int, 4> ar = {1, 4, 5, 32}, ar2;
      span_test s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      span_test s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(ar == ar2);
    }
    {
      std::array<int, 4> ar2;
      span_test2 s = {"Hello", {1, 4, 5, 3}};
      auto buffer = struct_pack::serialize(s);
      span_test s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(s.ar == ar2);
    }
  }
  SUBCASE("test static span<std::string ,4>") {
    {
      std::array<std::string, 4> ar = {"1", "14", "145", "1453"};
      span_test3 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      auto res = struct_pack::deserialize<span_test4>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
    {
      std::array<std::string, 4> ar = {"1", "14", "145", "1453"}, ar2;
      span_test3 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      span_test3 s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(ar == ar2);
    }
    {
      std::array<std::string, 4> ar2;
      span_test4 s = {"Hello", {"1", "14", "145", "1453"}};
      auto buffer = struct_pack::serialize(s);
      span_test3 s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(s.ar == ar2);
    }
  }
  SUBCASE("test static span<person ,4>") {
    {
      std::array<person, 4> ar = {person{24, "Betty"}, person{241, "Betty2"},
                                  person{2414, "Betty3"},
                                  person{24141, "Betty4"}};
      span_test5 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      auto res = struct_pack::deserialize<span_test6>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
    {
      std::array<person, 4> ar = {person{24, "Betty"}, person{241, "Betty2"},
                                  person{2414, "Betty3"},
                                  person{24141, "Betty4"}},
                            ar2;
      span_test5 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      span_test5 s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(ar == ar2);
    }
    {
      std::array<person, 4> ar2;
      span_test6 s = {"Hello",
                      {person{24, "Betty"}, person{241, "Betty2"},
                       person{2414, "Betty3"}, person{24141, "Betty4"}}};
      auto buffer = struct_pack::serialize(s);
      span_test5 s2 = {"", ar2};
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(s.ar == ar2);
    }
  }
}

struct dspan_test {
  std::string hello;
  std::span<int> sp;
};
struct dspan_test2 {
  std::string hello;
  std::vector<int> ar;
};
struct dspan_test3 {
  std::string hello;
  std::span<std::string> sp;
};
struct dspan_test4 {
  std::string hello;
  std::vector<std::string> ar;
};
struct dspan_test5 {
  std::string hello;
  std::span<person> sp;
};
struct dspan_test6 {
  std::string hello;
  std::vector<person> ar;
};
struct dspan_test7 {
  std::string hello;
  std::vector<uint8_t> ar;
};

TEST_CASE("test dynamic span") {
#ifdef TEST_IN_LITTLE_ENDIAN
  SUBCASE("test dynamic span<int>") {
    {
      std::vector<int> ar = {1, 4, 5, 3};
      dspan_test s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      dspan_test2 s2;
      auto res = struct_pack::deserialize<dspan_test2>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
    {
      std::vector<int> ar = {1, 4, 5, 32};
      dspan_test s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      dspan_test s2;
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(std::equal(ar.begin(), ar.end(), s2.sp.begin()));
    }
    {
      dspan_test2 s = {"Hello", {1, 4, 5, 3}};
      auto buffer = struct_pack::serialize(s);
      dspan_test s2;
      auto ec = struct_pack::deserialize_to(s2, buffer);
      CHECK(!ec);
      CHECK(s.hello == s2.hello);
      CHECK(std::equal(s.ar.begin(), s.ar.end(), s2.sp.begin()));
    }
  }
  SUBCASE("test dynamic span<std::string>") {
    {
      std::vector<std::string> ar = {"1", "14", "145", "1453"};
      dspan_test3 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      auto res = struct_pack::deserialize<dspan_test4>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
  }
  SUBCASE("test dynamic span<person>") {
    {
      std::vector<person> ar = {person{24, "Betty"}, person{241, "Betty2"},
                                person{2414, "Betty3"},
                                person{24141, "Betty4"}};
      dspan_test5 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      auto res = struct_pack::deserialize<dspan_test6>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
  }
#endif
  SUBCASE("test dynamic span<uint8_t>") {
    {
      std::vector<uint8_t> ar = {1, 2, 132, 214};
      dspan_test7 s = {"Hello", ar};
      auto buffer = struct_pack::serialize(s);
      auto res = struct_pack::deserialize<dspan_test7>(buffer);
      CHECK(res.has_value());
      CHECK(res.value().hello == s.hello);
      CHECK(res.value().ar == ar);
    }
  }
}
#endif

TEST_CASE("test width too big") {
  SUBCASE("1") {
    std::string buffer;
    buffer.push_back(0b11000);
    auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                           std::string>(buffer);
    REQUIRE(result.has_value() == false);
    if constexpr (sizeof(std::size_t) < 8) {
      CHECK(result.error() ==
            struct_pack::errc::invalid_width_of_container_length);
    }
    else {
      CHECK(result.error() == struct_pack::errc::no_buffer_space);
    }
  }
  SUBCASE("2") {
    std::string buffer;
    buffer.push_back(0b11000);
    std::size_t len = 0;
    auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                           std::string>(buffer, len);
    REQUIRE(result.has_value() == false);
    if constexpr (sizeof(std::size_t) < 8) {
      CHECK(result.error() ==
            struct_pack::errc::invalid_width_of_container_length);
    }
    else {
      CHECK(result.error() == struct_pack::errc::no_buffer_space);
    }
  }
  SUBCASE("3") {
    std::string buffer;
    buffer.push_back(0b11000);
    auto result =
        struct_pack::get_field<std::pair<std::string, std::string>, 0,
                               struct_pack::DISABLE_ALL_META_INFO>(buffer);
    REQUIRE(result.has_value() == false);
    if constexpr (sizeof(std::size_t) < 8) {
      CHECK(result.error() ==
            struct_pack::errc::invalid_width_of_container_length);
    }
    else {
      CHECK(result.error() == struct_pack::errc::no_buffer_space);
    }
  }
  SUBCASE("4") {
    std::string buffer;
    using T = std::pair<std::string, struct_pack::compatible<int>>;
    auto code = struct_pack::get_type_code<T>() + 1;
    buffer.resize(4);
    if constexpr (!struct_pack::detail::is_system_little_endian) {
      code = struct_pack::detail::bswap32(code);
    }
    memcpy(buffer.data(), &code, sizeof(code));
    buffer.push_back(0b11);
    auto result = struct_pack::deserialize<
        std::pair<std::string, struct_pack::compatible<int>>>(buffer);
    REQUIRE(result.has_value() == false);
    if constexpr (sizeof(std::size_t) < 8) {
      CHECK(result.error() ==
            struct_pack::errc::invalid_width_of_container_length);
    }
    else {
      CHECK(result.error() == struct_pack::errc::no_buffer_space);
    }
  }
}

TEST_CASE("test broken length") {
  auto buffer =
      struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO, std::string>(
          std::string{"ABCDEFGHIJKL"});
  if (sizeof(std::size_t) == 8) {
    buffer[0] = 0b11000;
    std::size_t i = UINT64_MAX;
    memcpy(buffer.data() + 1, &i, sizeof(i));
  }
  else {
    buffer[0] = 0b10000;
    std::size_t i = UINT32_MAX;
    memcpy(buffer.data() + 1, &i, sizeof(i));
  }
  auto result =
      struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO, std::string>(
          buffer);
  REQUIRE(result.has_value() == false);
  CHECK(result.error() == struct_pack::errc::no_buffer_space);
}

TEST_CASE("test broken length with overflow") {
  auto buffer =
      struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO, std::string>(
          std::u16string{u"ABCDEFGHIJKL"});
  if (sizeof(std::size_t) == 8) {
    buffer[0] = 0b11000;
    std::size_t i = UINT64_MAX;
    memcpy(buffer.data() + 1, &i, sizeof(i));
  }
  else {
    buffer[0] = 0b10000;
    std::size_t i = UINT32_MAX;
    memcpy(buffer.data() + 1, &i, sizeof(i));
  }
  auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,
                                         std::u16string>(buffer);
  REQUIRE(result.has_value() == false);
  CHECK(result.error() == struct_pack::errc::no_buffer_space);
}