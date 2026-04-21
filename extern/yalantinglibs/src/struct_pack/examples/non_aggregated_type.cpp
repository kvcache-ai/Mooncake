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

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>

// It's easy to use struct_pack, just include it!
#include <ylt/struct_pack.hpp>

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
  int age_;
  std::string name_;

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

//clang-format off
void non_aggregated_type() {
  {
    example::person p{20, "tom"};
    auto buffer = struct_pack::serialize(p);
    auto p2 = struct_pack::deserialize<example::person>(buffer);
    assert(p2);
    assert(p == p2.value());
  }
  {
    example2::person p{20, "tom"};
    auto buffer = struct_pack::serialize(p);
    auto p2 = struct_pack::deserialize<example2::person>(buffer);
    assert(p2);
    assert(p == p2.value());
  }
  {
    example2::person p{20, "tom"};
    auto buffer = struct_pack::serialize(p);
    auto p3 = struct_pack::deserialize<example2::person>(buffer);
    assert(p3);
    assert(p == p3.value());
  }
  {
    assert(struct_pack::get_type_code<example4::point>() !=
           struct_pack::get_type_code<example4::point2>());
  }
  {
    assert(struct_pack::get_type_code<example5::person>() ==
           struct_pack::get_type_code<example::person>());
    assert(struct_pack::get_type_code<example5::person>() ==
           struct_pack::get_type_code<example2::person>());
    assert(struct_pack::get_type_code<example5::person>() ==
           struct_pack::get_type_code<example3::person>());
  }
}