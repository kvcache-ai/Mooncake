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

#include <cstdint>
#include <optional>

namespace struct_pack {
/*!
 * \ingroup struct_pack
 * \brief similar to std::optional<T>.
 * However, its semantics are forward compatible field.
 *
 * For example,
 *
 * ```cpp
 * struct person_v1 {
 *   int age;
 *   std::string name;
 * };
 *
 * struct person_v2 {
 *   int age;
 *   struct_pack::compatible<bool, 20210101> id; // version number is 20210101
 *   struct_pack::compatible<double, 20210101> salary;
 *   std::string name;
 * };
 *
 * struct person_v3 {
 *   int age;
 *   struct_pack::compatible<std::string, 20210302> nickname; // new version
 * number is bigger than 20210101 struct_pack::compatible<bool, 20210101> id;
 *   struct_pack::compatible<double, 20210101> salary;
 *   std::string name;
 * };
 * ```
 *
 * `struct_pack::compatible<T, version_number>` can be null, thus ensuring
 * forward and backward compatibility.
 *
 * For example, serialize person_v2, and then deserialize it according to
 * person_v1, the extra fields will be directly ignored during deserialization.
 * If person_v1 is serialized and then deserialized according to person_v2, the
 * compatibale fields in person_v2 are all null values.
 *
 * The default value of version_number in `struct_pack::compatible<T,
 * version_number>` is 0.
 *
 * ```cpp
 * person_v2 p2{20, "tom", 114.1, "tom"};
 * auto buf = struct_pack::serialize(p2);
 *
 * person_v1 p1;
 * // deserialize person_v2 as person_v1
 * auto ec = struct_pack::deserialize_to(p1, buf.data(), buf.size());
 * CHECK(ec == std::errc{});
 *
 * auto buf1 = struct_pack::serialize(p1);
 * person_v2 p3;
 * // deserialize person_v1 as person_v2
 * auto ec = struct_pack::deserialize_to(p3, buf1.data(), buf1.size());
 * CHECK(ec == std::errc{});
 * ```
 *
 * When you update your struct:
 * 1. The new compatible field's version number should bigger than last changed.
 * 2. Don't remove or change any old field.
 *
 * For example,
 *
 * ```cpp
 * struct loginRequest_V1
 * {
 *   string user_name;
 *   string pass_word;
 *   struct_pack::compatible<string> verification_code;
 * };
 *
 * struct loginRequest_V2
 * {
 *   string user_name;
 *   string pass_word;
 *   struct_pack::compatible<network::ip> ip_address;
 * };
 *
 * auto data=struct_pack::serialize(loginRequest_V1{});
 * loginRequest_V2 req;
 * struct_pack::deserialize_to(req, data); // undefined behavior!
 *
 * ```
 *
 * ```cpp
 * struct loginRequest_V1
 * {
 *   string user_name;
 *   string pass_word;
 *   struct_pack::compatible<string, 20210101> verification_code;
 * };
 *
 * struct loginRequest_V2
 * {
 *   string user_name;
 *   string pass_word;
 *   struct_pack::compatible<string, 20210101> verification_code;
 *   struct_pack::compatible<network::ip, 20000101> ip_address;
 * };
 *
 * auto data=struct_pack::serialize(loginRequest_V1{});
 * loginRequest_V2 req;
 * struct_pack::deserialize_to(req, data);  // undefined behavior!
 *
 * The value of compatible can be empty.
 *
 * TODO: add doc
 *
 * @tparam T field type
 */

template <typename T, uint64_t version>
struct compatible : public std::optional<T> {
  constexpr compatible() = default;
  constexpr compatible(const compatible &other) = default;
  constexpr compatible(compatible &&other) = default;
  constexpr compatible(std::optional<T> &&other)
      : std::optional<T>(std::move(other)){};
  constexpr compatible(const std::optional<T> &other)
      : std::optional<T>(other){};
  constexpr compatible &operator=(const compatible &other) = default;
  constexpr compatible &operator=(compatible &&other) = default;
  using std::optional<T>::optional;

  static constexpr uint64_t version_number = version;
};

template <typename T, uint64_t version1, uint64_t version2>
inline bool operator==(const compatible<T, version1> &lhs,
                       const compatible<T, version2> &rhs) {
  return static_cast<bool>(lhs) == static_cast<bool>(rhs) &&
         (!lhs || *lhs == *rhs);
}
}  // namespace struct_pack
