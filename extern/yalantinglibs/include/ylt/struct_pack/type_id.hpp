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
#include <climits>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <variant>

#include "varint.hpp"
namespace struct_pack::detail {
enum class type_id {
  // compatible template type
  compatible_t = 0,
  // fundamental integral type
  int32_t = 1,
  uint32_t,
  int64_t,
  uint64_t,
  int8_t,
  uint8_t,
  int16_t,
  uint16_t,
  int128_t,  // Tips: We only support 128-bit integer on gcc clang
  uint128_t,
  bool_t,
  char_8_t,
  char_16_t,
  char_32_t,
  w_char_t,  // Note: this type is not portable!Enable it with marco
             // STRUCT_PACK_ENABLE_UNPORTABLE_TYPE
  // fundamental float type
  float16_t,  // TODO: wait for C++23 standard float type
  float32_t,
  float64_t,
  float128_t,
  vint32_t,        // variable size int
  vint64_t,        // variable size int
  vuint32_t,       // variable size unsigned int
  vuint64_t,       // variable size unsigned int
  fast_vint32_t,   // variable size int with fast encoding
  fast_vint64_t,   // variable size int with fast encoding
  fast_vuint32_t,  // variable size unsigned int with fast encoding
  fast_vuint64_t,  // variable size unsigned int with fast encoding
  // template type
  string_t = 128,
  array_t,
  map_container_t,
  set_container_t,
  container_t,
  optional_t,
  variant_t,
  expected_t,
  bitset_t,
  polymorphic_unique_ptr_t,
  // flag for user-defined type
  user_defined_type = 249,
  // monostate, or void
  monostate_t = 250,
  // circle_flag
  circle_flag = 251,
  // end helper with user defined type ID
  type_end_flag_with_id = 252,
  // class type
  struct_t = 253,
  // end helper
  type_end_flag = 255,
};

template <typename T, uint64_t parent_tag>
constexpr type_id get_varint_type() {
  if constexpr (is_enable_fast_varint_coding(parent_tag)) {
    if constexpr (std::is_same_v<var_int32_t, T> ||
                  std::is_same_v<int32_t, T>) {
      return type_id::fast_vint32_t;
    }
    else if constexpr (std::is_same_v<var_int64_t, T> ||
                       std::is_same_v<int64_t, T>) {
      return type_id::fast_vint64_t;
    }
    else if constexpr (std::is_same_v<var_uint32_t, T> ||
                       std::is_same_v<uint32_t, T>) {
      return type_id::fast_vuint32_t;
    }
    else if constexpr (std::is_same_v<var_uint64_t, T> ||
                       std::is_same_v<uint64_t, T>) {
      return type_id::fast_vuint64_t;
    }
    else {
      static_assert(!sizeof(T), "unsupported varint type!");
    }
  }
  else {
    if constexpr (std::is_same_v<var_int32_t, T> ||
                  std::is_same_v<int32_t, T>) {
      return type_id::vint32_t;
    }
    else if constexpr (std::is_same_v<var_int64_t, T> ||
                       std::is_same_v<int64_t, T>) {
      return type_id::vint64_t;
    }
    else if constexpr (std::is_same_v<var_uint32_t, T> ||
                       std::is_same_v<uint32_t, T>) {
      return type_id::vuint32_t;
    }
    else if constexpr (std::is_same_v<var_uint64_t, T> ||
                       std::is_same_v<uint64_t, T>) {
      return type_id::vuint64_t;
    }
    else {
      static_assert(!sizeof(T), "unsupported varint type!");
    }
  }
}

template <typename T>
constexpr type_id get_integral_type() {
  if constexpr (std::is_same_v<int32_t, T>) {
    return type_id::int32_t;
  }
  else if constexpr (std::is_same_v<uint32_t, T>) {
    return type_id::uint32_t;
  }
  else if constexpr (std::is_same_v<int64_t, T> ||
                     (sizeof(long long) == 8 && std::is_same_v<T, long long>)) {
    return type_id::int64_t;
  }
  else if constexpr (std::is_same_v<uint64_t, T> ||
                     (sizeof(unsigned long long) == 8 &&
                      std::is_same_v<T, unsigned long long>)) {
    return type_id::uint64_t;
  }
  else if constexpr (std::is_same_v<int8_t, T> ||
                     std::is_same_v<signed char, T>) {
    return type_id::int8_t;
  }
  else if constexpr (std::is_same_v<uint8_t, T> ||
                     std::is_same_v<unsigned char, T>) {
    return type_id::uint8_t;
  }
  else if constexpr (std::is_same_v<int16_t, T>) {
    return type_id::int16_t;
  }
  else if constexpr (std::is_same_v<uint16_t, T>) {
    return type_id::uint16_t;
  }
  // In struct_pack, the char will be saved as unsigned!
  else if constexpr (std::is_same_v<char, T>
#ifdef __cpp_lib_char8_t
                     || std::is_same_v<char8_t, T>
#endif
  ) {
    return type_id::char_8_t;
  }
#ifdef STRUCT_PACK_ENABLE_UNPORTABLE_TYPE
  else if constexpr (std::is_same_v<wchar_t, T>) {
    return type_id::w_char_t;
  }
#endif
  // char16_t's size maybe bigger than 16 bits, which is not supported.
  else if constexpr (std::is_same_v<char16_t, T>) {
    static_assert(sizeof(char16_t) == 2,
                  "sizeof(char16_t)!=2, which is not supported.");
    return type_id::char_16_t;
  }
  // char32_t's size maybe bigger than 32bits, which is not supported.
  else if constexpr (std::is_same_v<char32_t, T> && sizeof(char32_t) == 4) {
    static_assert(sizeof(char32_t) == 4,
                  "sizeof(char32_t)!=4, which is not supported.");
    return type_id::char_32_t;
  }
  else if constexpr (std::is_same_v<bool, T> && sizeof(bool)) {
    static_assert(sizeof(bool) == 1,
                  "sizeof(bool)!=1, which is not supported.");
    return type_id::bool_t;
  }
#if (__GNUC__ || __clang__) && defined(STRUCT_PACK_ENABLE_INT128)
  //-std=gnu++20
  else if constexpr (std::is_same_v<__int128, T>) {
    return type_id::int128_t;
  }
  else if constexpr (std::is_same_v<unsigned __int128, T>) {
    return type_id::uint128_t;
  }
#endif
  else if constexpr (std::is_same_v<long, T>) {
    if constexpr (sizeof(long) == sizeof(int32_t)) {
      return type_id::int32_t;
    }
    else if constexpr (sizeof(long) == sizeof(int64_t)) {
      return type_id::int64_t;
    }
    else {
      static_assert(!sizeof(T), "unsupport size of long type");
    }
  }
  else if constexpr (std::is_same_v<unsigned long, T>) {
    if constexpr (sizeof(unsigned long) == sizeof(uint32_t)) {
      return type_id::uint32_t;
    }
    else if constexpr (sizeof(unsigned long) == sizeof(uint64_t)) {
      return type_id::uint64_t;
    }
    else {
      static_assert(!sizeof(T), "unsupport size of long type");
    }
  }
  else {
    /*
     * Due to different data model,
     * the following types are not allowed on macOS
     * but work on Linux
     * For example,
     * on macOS, `typedef unsigned long long uint64_t;`
     * on Linux, `typedef unsigned long int  uint64_t;`
     *
     * - long
     * - long int
     * - signed long
     * - signed long int
     * - unsigned long
     * - unsigned long int
     *
     * We add this static_assert to give more information about not supported
     * type.
     */
    static_assert(
        !std::is_same_v<wchar_t, T>,
        "Tips: Add macro STRUCT_PACK_ENABLE_UNPORTABLE_TYPE to support "
        "wchar_t");
    static_assert(!sizeof(T), "not supported type");
    // This branch will always compiled error.
  }
}

template <typename T>
constexpr type_id get_floating_point_type() {
  if constexpr (std::is_same_v<float, T>) {
    if constexpr (!std::numeric_limits<float>::is_iec559 ||
                  sizeof(float) != 4) {
      static_assert(
          !sizeof(T),
          "The float type in this machine is not standard IEEE 754 32bits "
          "float point!");
    }
    return type_id::float32_t;
  }
  else if constexpr (std::is_same_v<double, T>) {
    if constexpr (!std::numeric_limits<double>::is_iec559 ||
                  sizeof(double) != 8) {
      static_assert(
          !sizeof(T),
          "The double type in this machine is not standard IEEE 754 64bits "
          "float point!");
    }
    return type_id::float64_t;
  }
  else if constexpr (std::is_same_v<long double, T>) {
    if constexpr (sizeof(long double) != 16 ||
                  std::numeric_limits<long double>::is_iec559) {
      static_assert(!sizeof(T),
                    "The long double type in this machine is not standard IEEE "
                    "754 128bits "
                    "float point!");
    }
    return type_id::float128_t;
  }
  else {
    static_assert(!sizeof(T), "not supported type");
  }
}

template <typename T, std::size_t parent_tag = 0>
constexpr type_id get_type_id() {
  static_assert(CHAR_BIT == 8);
  // compatible member, which should be ignored in MD5 calculated.
  if constexpr (user_defined_serialization<T>) {
    return type_id::user_defined_type;
  }
  else if constexpr (ylt::reflection::optional<T> && is_compatible_v<T>) {
    return type_id::compatible_t;
  }
  else if constexpr (detail::varint_t<T, parent_tag>) {
    return get_varint_type<T, parent_tag>();
  }
  else if constexpr (std::is_enum_v<T>) {
    return get_integral_type<std::underlying_type_t<T>>();
  }
  else if constexpr (std::is_integral_v<T>
#if (__GNUC__ || __clang__) && defined(STRUCT_PACK_ENABLE_INT128)
                     || std::is_same_v<__int128, T> ||
                     std::is_same_v<unsigned __int128, T>
#endif
  ) {
    return get_integral_type<T>();
  }
  else if constexpr (std::is_floating_point_v<T>) {
    return get_floating_point_type<T>();
  }
  else if constexpr (std::is_same_v<T, std::monostate> ||
                     std::is_same_v<T, void> || std::is_abstract_v<T>) {
    return type_id::monostate_t;
  }
#ifdef STRUCT_PACK_ENABLE_UNPORTABLE_TYPE
  else if constexpr (bitset<T>) {
    return type_id::bitset_t;
  }
#endif
  else if constexpr (string<T>) {
    return type_id::string_t;
  }
  else if constexpr (array<T> || c_array<T> || static_span<T>) {
    return type_id::array_t;
  }
  else if constexpr (map_container<T>) {
    return type_id::map_container_t;
  }
  else if constexpr (set_container<T>) {
    return type_id::set_container_t;
  }
  else if constexpr (container<T>) {
    return type_id::container_t;
  }
  else if constexpr (ylt::reflection::optional<T>) {
    return type_id::optional_t;
  }
  else if constexpr (unique_ptr<T>) {
    if constexpr (is_base_class<typename T::element_type>) {
      return type_id::polymorphic_unique_ptr_t;
    }
    else {
      return type_id::optional_t;
    }
  }
  else if constexpr (is_variant_v<T>) {
    static_assert(
        std::variant_size_v<T> > 1 ||
            (std::variant_size_v<T> == 1 &&
             !std::is_same_v<std::variant_alternative_t<0, T>, std::monostate>),
        "The variant should contain's at least one type!");
    static_assert(std::variant_size_v<T> < 256, "The variant is too complex!");
    return type_id::variant_t;
  }
  else if constexpr (ylt::reflection::expected<T>) {
    return type_id::expected_t;
  }
  else if constexpr (is_trivial_tuple<T> || pair<T> || std::is_class_v<T>) {
    return type_id::struct_t;
  }
  else {
    static_assert(!sizeof(T), "not supported type");
  }
}
}  // namespace struct_pack::detail