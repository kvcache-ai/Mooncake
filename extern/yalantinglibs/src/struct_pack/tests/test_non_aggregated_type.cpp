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

#include <map>
#include <string>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "test_struct.hpp"

// 1. make sure your type has a default constructor
// 2. add marco YLT_REFL(Type, field1, field2...) in the same namespace
namespace example {
class person : std::vector<int> {
 private:
  std::string mess;

 public:
  int age;
  std::string name;
  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
  person() = default;
  person(int age, const std::string& name) : age(age), name(name) {}
};

YLT_REFL(person, age, name);
}  // namespace example

// 3. if you want to use private field, add friend declartion marco

namespace example2 {
class person {
 private:
  int age;
  std::string name;

 public:
  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
  person() = default;
  person(int age, const std::string& name) : age(age), name(name) {}
  YLT_REFL(person, age, name);
};
}  // namespace example2

// 4. you can also add function which return class member reference as
// struct_pack field.

namespace example3 {
class person {
 private:
  std::string name_;
  int age_;

 public:
  auto operator==(const person& rhs) const {
    return age_ == rhs.age_ && name_ == rhs.name_;
  }
  person() = default;
  person(int age, const std::string& name) : age_(age), name_(name) {}

  int& age() { return age_; };
  const int& age() const { return age_; };
  std::string& name() { return name_; };
  const std::string& name() const { return name_; };
};
YLT_REFL(person, age(), name());
}  // namespace example3

// 5. Remember, the STURCT_PACK_REFL marco disable the trivial_serialize
// optimize. So don't use it for trivial type.
namespace example4 {
struct point {
  int x, y, z;
};
YLT_REFL(point, x, y, z);
struct point2 {
  int x, y, z;
};
}  // namespace example4

// 6. example5::person ,example::person, example2::person, example3:person are
// same type in struct_pack type system.
namespace example5 {
struct person {
  int age;
  std::string name;
  auto operator==(const person& rhs) const {
    return age == rhs.age && name == rhs.name;
  }
};
}  // namespace example5

namespace example6 {
class complicated_object {
  Color color;
  int a;
  std::string b;
  std::vector<person> c;
  std::list<std::string> d;
  std::deque<int> e;
  std::map<int, person> f;
  std::multimap<int, person> g;
  std::set<std::string> h;
  std::multiset<int> i;
  std::unordered_map<int, person> j;
  std::unordered_multimap<int, int> k;
  std::array<person, 2> m;
  person n[2];
  std::pair<std::string, person> o;

 public:
  complicated_object() = default;
  complicated_object(const Color& color, int a, const std::string& b,
                     const std::vector<person>& c,
                     const std::list<std::string>& d, const std::deque<int>& e,
                     const std::map<int, person>& f,
                     const std::multimap<int, person>& g,
                     const std::set<std::string>& h,
                     const std::multiset<int>& i,
                     const std::unordered_map<int, person>& j,
                     const std::unordered_multimap<int, int>& k,
                     const std::array<person, 2>& m, const person (&n)[2],
                     const std::pair<std::string, person>& o)
      : color(color),
        a(a),
        b(b),
        c(c),
        d(d),
        e(e),
        f(f),
        g(g),
        h(h),
        i(i),
        j(j),
        k(k),
        m(m),
        o(o) {
    this->n[0] = n[0];
    this->n[1] = n[1];
  }
  bool operator==(const complicated_object& o) const {
    return color == o.color && a == o.a && b == o.b && c == o.c && d == o.d &&
           e == o.e && f == o.f && g == o.g && h == o.h && i == o.i &&
           j == o.j && k == o.k && m == o.m && n[0] == o.n[0] &&
           n[1] == o.n[1] && this->o == o.o;
  }
  YLT_REFL(complicated_object, a, b, c, d, e, f, g, h, i, j, k, m, n, o);
};
}  // namespace example6

TEST_CASE("test non aggregated type") {
  example::person p{20, "tom"};
  auto buffer = struct_pack::serialize(p);
  auto p2 = struct_pack::deserialize<example::person>(buffer);
  CHECK(p2);
  CHECK(p == p2.value());
}
TEST_CASE("test non aggregated type with private field") {
  example2::person p{20, "tom"};
  auto buffer = struct_pack::serialize(p);
  auto p2 = struct_pack::deserialize<example2::person>(buffer);
  CHECK(p2);
  CHECK(p == p2.value());
}
TEST_CASE("test non aggregated type with function field") {
  example2::person p{20, "tom"};
  auto buffer = struct_pack::serialize(p);
  auto p3 = struct_pack::deserialize<example2::person>(buffer);
  CHECK(p3);
  CHECK(p == p3.value());
}
TEST_CASE("test disable trivial serialize optimize") {
  CHECK(struct_pack::get_type_code<example4::point>() !=
        struct_pack::get_type_code<example4::point2>());
}
TEST_CASE("test same type with different reflection way") {
  example5::person p{25, "Betty"};
  {
    example::person p2{25, "Betty"};
    CHECK(struct_pack::get_type_code<example5::person>() ==
          struct_pack::get_type_code<example::person>());
    {
      auto buffer = struct_pack::serialize(p2);
      auto result = struct_pack::deserialize<example5::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p);
    }
    {
      auto buffer = struct_pack::serialize(p);
      auto result = struct_pack::deserialize<example::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p2);
    }
  }
  {
    example2::person p2{25, "Betty"};
    CHECK(struct_pack::get_type_code<example5::person>() ==
          struct_pack::get_type_code<example2::person>());
    {
      auto buffer = struct_pack::serialize(p2);
      auto result = struct_pack::deserialize<example5::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p);
    }
    {
      auto buffer = struct_pack::serialize(p);
      auto result = struct_pack::deserialize<example2::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p2);
    }
  }
  {
    example3::person p2{25, "Betty"};
    CHECK(struct_pack::get_type_code<example5::person>() ==
          struct_pack::get_type_code<example3::person>());
    {
      auto buffer = struct_pack::serialize(p2);
      auto result = struct_pack::deserialize<example5::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p);
    }
    {
      auto buffer = struct_pack::serialize(p);
      auto result = struct_pack::deserialize<example3::person>(buffer);
      CHECK(result);
      CHECK(result.value() == p2);
    }
  }
}
TEST_CASE("test complicated_object") {
  example6::complicated_object o{
      Color::red,
      42,
      "hello",
      {{20, "tom"}, {22, "jerry"}},
      {"hello", "world"},
      {1, 2},
      {{1, {20, "tom"}}},
      {{1, {20, "tom"}}, {1, {22, "jerry"}}},
      {"aa", "bb"},
      {1, 2},
      {{1, {20, "tom"}}, {1, {22, "jerry"}}},
      {{1, 2}, {1, 3}},
      {person{20, "tom"}, {22, "jerry"}},
      {person{20, "tom"}, {22, "jerry"}},
      std::make_pair("aa", person{20, "tom"}),
  };
  auto buffer = struct_pack::serialize(o);
  auto result = struct_pack::deserialize<example6::complicated_object>(buffer);
  CHECK(result);
  CHECK(result.value() == o);
}
