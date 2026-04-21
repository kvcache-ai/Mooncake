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

#include <msgpack.hpp>
#include <string_view>
#include <tuple>
#include <type_traits>

namespace coro_rpc::protocol {
struct msgpack_protocol {
  template <typename T>
  static bool deserialize_to(T& t, std::string_view buffer) {
    try {
      msgpack::unpacked unpack;
      msgpack::unpack(unpack, buffer.data(), buffer.size());
      t = unpack.get().as<T>();
    } catch (...) {
      throw std::invalid_argument("unpack failed: Args not match!");
      return false;
    }

    return true;
  }
  template <typename T>
  static std::string serialize(const T& t) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, std::make_tuple(0, t));
    std::string str(buffer.data(), buffer.size());
    return str;
  }
  static std::string serialize() {
    msgpack::sbuffer buffer(4);
    msgpack::pack(buffer, 0);
    return buffer.data();
  }
};
}  // namespace coro_rpc::protocol