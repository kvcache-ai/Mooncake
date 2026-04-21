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
#include <ylt/struct_pack.hpp>
namespace coro_rpc::protocol {

struct struct_pack_protocol {
  template <typename T>
  static bool deserialize_to(T& t, std::string_view buffer) {
    if constexpr (std::tuple_size_v<T> == 1) {
      return !struct_pack::deserialize_to(std::get<0>(t), buffer);
    }
    else {
      return !struct_pack::deserialize_to(t, buffer);
    }
  }
  template <typename T>
  static std::string serialize(const T& t) {
    return struct_pack::serialize<std::string>(t);
  }
  static std::string serialize() {
    return struct_pack::serialize<std::string>(std::monostate{});
  }
};
}  // namespace coro_rpc::protocol