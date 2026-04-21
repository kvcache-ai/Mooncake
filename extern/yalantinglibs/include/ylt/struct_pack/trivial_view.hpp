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
#pragma once

#include <cassert>

#include "reflection.hpp"

namespace struct_pack {
/*!
 * \ingroup struct_pack
 * \struct trivial_view
 * \tparam T trivial_view指向的类型
 * \brief
 * trivial_view<T> is a view for trivial struct. It's equals T in type system.
 * It can decrease memory copy in proto initialization/deserialization
 *
 * For example:
 *
 * ```cpp
 * struct Data {
 *   int x[10000],y[10000],z[10000];
 * };
 *
 * struct Proto {
 *   std::string name;
 *   Data data;
 * };
 * void serialzie(std::string_view name, Data& data) {
 *   Proto proto={.name = name, .data = data};
 *   // heavy cost in initialization.
 *   auto buffer = struct_pack::serialize(proto);
 *   auto result = struct_pack::deserialize<Proto>(data);
 *   // memory copy in deserialization.
 *   assert(result->name == name && result->data == data);
 * }
 * ```
 *
 * The solution: use view type:
 * ```cpp
 * struct ProtoView {
 *   std::string_view name;
 *   struct_pack::trivial_view<Data> data;
 * };
 * void serialzie(std::string_view name, Data& data) {
 *   ProtoView proto={.name = name, .data = data};
 *   // zero-copy initialization.
 *   auto buffer = struct_pack::serialize(proto);
 *   auto result = struct_pack::deserialize<ProtoView>(data);
 *   // zero-copy deserialization.
 *   assert(result->name == name && result->data.get() == data);
 * }
 * ```
 * trivial_view<T> has same layout as T. So it's legal to serialize T then
 * deserialize to trivial_view<T>
 * ```cpp
 * void serialzie(Proto& proto) {
 *   auto buffer = struct_pack::serialize(proto);
 *   auto result = struct_pack::deserialize<ProtoView>(data);
 *   // zero-copy deserialization.
 *   assert(result->name == name && result->data.get() == data);
 * }
 * ```
 *
 */
template <typename T, typename>
struct trivial_view {
 private:
  const T* ref;

 public:
  trivial_view(const T* t) : ref(t){};
  trivial_view(const T& t) : ref(&t){};
  trivial_view(const trivial_view&) = default;
  trivial_view() : ref(nullptr){};
  trivial_view& operator=(const trivial_view&) = default;

  using value_type = T;

  void set(const T& obj) { ref = &obj; }
  const T& get() const {
    assert(ref != nullptr);
    return *ref;
  }
  const T* operator->() const {
    assert(ref != nullptr);
    return ref;
  }
};
}  // namespace struct_pack
