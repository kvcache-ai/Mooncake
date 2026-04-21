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

#include "ylt/struct_pack.hpp"
namespace virtual_base_class {

//    base
//   /   |
//  obj1 obj2
//  |
//  obj3

struct base {
  uint64_t ID;
  virtual uint32_t get_struct_pack_id()
      const = 0;  // user must declare this virtual function in derived
                  // class
  virtual ~base(){};
  virtual std::string_view hello() = 0;
};
struct obj1 : public base {
  std::string name;
  virtual uint32_t get_struct_pack_id()
      const override;  // user must declare this virtual function in derived
                       // class
  std::string_view hello() override { return "obj1"; }
};
YLT_REFL(obj1, ID, name);
struct obj2 : public base {
  std::array<float, 5> data;
  virtual uint32_t get_struct_pack_id() const override;
  std::string_view hello() override { return "obj2"; }
};
YLT_REFL(obj2, ID, data);
struct obj3 : public obj1 {
  int age;
  virtual uint32_t get_struct_pack_id() const override;
  std::string_view hello() override { return "obj3"; }
};
YLT_REFL(obj3, ID, name, age);
// the declartion for derived relation for struct_pack
STRUCT_PACK_DERIVED_DECL(base, obj1, obj2, obj3);
// the implement for derived relation for struct_pack
STRUCT_PACK_DERIVED_IMPL(base, obj1, obj2, obj3);
}  // namespace virtual_base_class
void virtual_base_class_example(void) {
  using namespace virtual_base_class;
  std::vector<std::unique_ptr<base>> data;
  data.emplace_back(std::make_unique<obj2>());
  data.emplace_back(std::make_unique<obj1>());
  data.emplace_back(std::make_unique<obj3>());
  auto checker = std::vector{"obj2", "obj1", "obj3"};
  auto ret = struct_pack::serialize(data);
  auto result =
      struct_pack::deserialize<std::vector<std::unique_ptr<base>>>(ret);
  assert(result.has_value());   // check deserialize ok
  assert(result->size() == 3);  // check vector size
  for (size_t i = 0; i < result->size(); ++i) {
    assert(checker[i] == result.value()[i]->hello());  // check type
  }
}

namespace base_class {

//    base
//   /   |
//  obj1 obj2
//  |
//  obj3

struct base {
  uint64_t ID;
  virtual uint32_t get_struct_pack_id() const;
  virtual ~base(){};
  virtual std::string_view hello() { return "base"; }
};
YLT_REFL(base, ID);
struct obj1 : public base {
  std::string name;
  virtual uint32_t get_struct_pack_id() const override;
  std::string_view hello() override { return "obj1"; }
};
YLT_REFL(obj1, ID, name);
struct obj2 : public base {
  std::array<float, 5> data;
  virtual uint32_t get_struct_pack_id() const override;
  std::string_view hello() override { return "obj2"; }
};
YLT_REFL(obj2, ID, data);
struct obj3 : public obj1 {
  virtual uint32_t get_struct_pack_id() const override;
  static constexpr uint64_t struct_pack_id = 114514;
  std::string_view hello() override { return "obj3"; }
};
YLT_REFL(obj3, ID, name);
STRUCT_PACK_DERIVED_DECL(base, obj1, obj2, obj3);
STRUCT_PACK_DERIVED_IMPL(base, obj1, obj2, obj3);
}  // namespace base_class

void base_class_example(void) {
  using namespace base_class;
  std::vector<std::unique_ptr<base>> data;
  data.emplace_back(std::make_unique<obj2>());
  data.emplace_back(std::make_unique<obj1>());
  data.emplace_back(std::make_unique<obj3>());
  data.emplace_back(std::make_unique<base>());
  auto ret = struct_pack::serialize(data);
  auto checker = std::vector{"obj2", "obj1", "obj3", "base"};
  auto result =
      struct_pack::deserialize<std::vector<std::unique_ptr<base>>>(ret);
  assert(result.has_value());
  assert(result->size() == 4);  // check vector size
  for (size_t i = 0; i < result->size(); ++i) {
    assert(checker[i] == result.value()[i]->hello());  // check type
  }
}

void deserialize_derived_class_example() {
  using namespace base_class;
  auto buffer = struct_pack::serialize(obj3{});
  auto result =
      struct_pack::deserialize_derived_class<base, obj1, obj2, obj3>(buffer);
  assert(result.has_value());
  std::unique_ptr<base> ptr = std::move(result.value());
  assert(ptr != nullptr);
  assert(ptr->hello() == "obj3");
}

void derived_class_example() {
  virtual_base_class_example();
  base_class_example();
  deserialize_derived_class_example();
}