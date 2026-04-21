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
#include <ostream>
#include <system_error>
#include <type_traits>

#include "endian_wrapper.hpp"
#include "reflection.hpp"
namespace struct_pack {

namespace detail {

constexpr inline bool is_enable_fast_varint_coding(uint64_t tag) {
  return tag & struct_pack::USE_FAST_VARINT;
}

template <std::size_t bytes_width>
struct int_t;

template <>
struct int_t<1> {
  using type = int8_t;
};

template <>
struct int_t<2> {
  using type = int16_t;
};

template <>
struct int_t<4> {
  using type = int32_t;
};

template <>
struct int_t<8> {
  using type = int64_t;
};

template <std::size_t bytes_width>
struct uint_t;

template <>
struct uint_t<1> {
  using type = uint8_t;
};

template <>
struct uint_t<2> {
  using type = uint16_t;
};

template <>
struct uint_t<4> {
  using type = uint32_t;
};

template <>
struct uint_t<8> {
  using type = uint64_t;
};

template <typename T>
class varint {
 public:
  using value_type = T;
  varint() noexcept = default;
  varint(T t) noexcept : val(t) {}
  [[nodiscard]] operator T() const noexcept { return val; }
  auto& operator=(T t) noexcept {
    val = t;
    return *this;
  }
  [[nodiscard]] auto operator<(const varint& o) const noexcept {
    return val < o.val;
  }
  [[nodiscard]] auto operator<=(const varint& o) const noexcept {
    return val <= o.val;
  }
  [[nodiscard]] auto operator>(const varint& o) const noexcept {
    return val > o.val;
  }
  [[nodiscard]] auto operator>=(const varint& o) const noexcept {
    return val >= o.val;
  }
  [[nodiscard]] auto operator!=(const varint& o) const noexcept {
    return val != o.val;
  }
  [[nodiscard]] bool operator==(const varint<T>& t) const noexcept {
    return val == t.val;
  }
  [[nodiscard]] bool operator==(T t) const noexcept { return val == t; }
  [[nodiscard]] auto operator&(uint8_t mask) const noexcept {
    T new_val = val & mask;
    return varint(new_val);
  }
  template <typename U, typename = std::enable_if_t<std::is_unsigned_v<U>>>
  [[nodiscard]] auto operator<<(U shift) const noexcept {
    T new_val = val << shift;
    return varint(new_val);
  }
  template <typename U>
  [[nodiscard]] auto operator|=(U shift) noexcept {
    if constexpr (std::is_same_v<U, varint<T>>) {
      val |= shift.val;
    }
    else {
      val |= shift;
    }
    return *this;
  }
  friend std::ostream& operator<<(std::ostream& os, const varint& varint) {
    os << varint.val;
    return os;
  }

  const T& get() const noexcept { return val; }
  T& get() noexcept { return val; }

 private:
  T val;
};

template <typename T>
class sint {
 public:
  using value_type = T;
  sint() noexcept = default;
  sint(T t) noexcept : val(t) {}
  [[nodiscard]] operator T() const noexcept { return val; }
  auto& operator=(T t) noexcept {
    val = t;
    return *this;
  }
  [[nodiscard]] auto operator<(const sint& o) const noexcept {
    return val < o.val;
  }
  [[nodiscard]] auto operator<=(const sint& o) const noexcept {
    return val <= o.val;
  }
  [[nodiscard]] auto operator>(const sint& o) const noexcept {
    return val > o.val;
  }
  [[nodiscard]] auto operator>=(const sint& o) const noexcept {
    return val >= o.val;
  }
  [[nodiscard]] auto operator!=(const sint& o) const noexcept {
    return val != o.val;
  }
  [[nodiscard]] bool operator==(T t) const noexcept { return val == t; }
  [[nodiscard]] bool operator==(const sint& t) const noexcept {
    return val == t.val;
  }
  [[nodiscard]] auto operator&(uint8_t mask) const noexcept {
    T new_val = val & mask;
    return sint(new_val);
  }
  template <typename U, typename = std::enable_if_t<std::is_unsigned_v<U>>>
  auto operator<<(U shift) const noexcept {
    T new_val = val << shift;
    return sint(new_val);
  }
  const T& get() const noexcept { return val; }
  T& get() noexcept { return val; }
  friend std::ostream& operator<<(std::ostream& os, const sint& t) {
    os << t.val;
    return os;
  }

 private:
  T val;
};

template <typename T, typename U>
[[nodiscard]] STRUCT_PACK_INLINE T decode_zigzag(U u) {
  return static_cast<T>((u >> 1U)) ^ static_cast<U>(-static_cast<T>(u & 1U));
}

template <typename U, typename T, unsigned Shift>
[[nodiscard]] STRUCT_PACK_INLINE U encode_zigzag(T t) {
  return (static_cast<U>(t) << 1U) ^
         static_cast<U>(-static_cast<T>(static_cast<U>(t) >> Shift));
}
template <typename T>
[[nodiscard]] STRUCT_PACK_INLINE auto encode_zigzag(T t) {
  if constexpr (std::is_same_v<T, int32_t>) {
    return encode_zigzag<uint32_t, int32_t, 31U>(t);
  }
  else if constexpr (std::is_same_v<T, int64_t>) {
    return encode_zigzag<uint64_t, int64_t, 63U>(t);
  }
  else {
    static_assert(!sizeof(T), "error zigzag type");
  }
}

template <typename T>
[[nodiscard]] STRUCT_PACK_INLINE constexpr int calculate_varint_size(T t) {
  if constexpr (!varint_t<T>) {
    int ret = 0;
    if constexpr (std::is_unsigned_v<T>) {
      do {
        ret++;
        t >>= 7;
      } while (t != 0);
    }
    else {
      uint64_t temp = t;
      do {
        ret++;
        temp >>= 7;
      } while (temp != 0);
    }
    return ret;
  }
  else if constexpr (varintable_t<T>) {
    return calculate_varint_size(t.get());
  }
  else if constexpr (sintable_t<T>) {
    return calculate_varint_size(encode_zigzag(t.get()));
  }
  else {
    static_assert(!sizeof(T), "error type");
  }
}

template <
#if __cpp_concepts >= 201907L
    writer_t writer,
#else
    typename writer,
#endif
    typename T>
STRUCT_PACK_INLINE void serialize_varint(writer& writer_, const T& t) {
#if __cpp_concepts < 201907L
  static_assert(writer_t<writer>, "The writer type must satisfy requirements!");
#endif
  uint64_t v;
  if constexpr (sintable_t<T>) {
    v = encode_zigzag(get_varint_value(t));
  }
  else {
    v = t;
  }
  while (v >= 0x80) {
    uint8_t temp = v | 0x80u;
    write_wrapper<sizeof(char)>(writer_, (char*)&temp);
    v >>= 7;
  }
  if constexpr (is_system_little_endian) {
    write_wrapper<sizeof(char)>(writer_, (char*)&v);
  }
  else {
    uint8_t tmp = v;
    write_wrapper<sizeof(char)>(writer_, (char*)&tmp);
  }
}
#if __cpp_concepts >= 201907L
template <reader_t Reader>
#else
template <typename Reader>
#endif
[[nodiscard]] STRUCT_PACK_INLINE struct_pack::err_code deserialize_varint_impl(
    Reader& reader, uint64_t& v) {
#if __cpp_concepts < 201907L
  static_assert(reader_t<Reader>, "The writer type must satisfy requirements!");
#endif
  uint8_t now;
  constexpr const int8_t max_varint_length = sizeof(uint64_t) * 8 / 7 + 1;
  for (int8_t i = 0; i < max_varint_length; ++i) {
    if SP_UNLIKELY (!reader.read((char*)&now, sizeof(char))) {
      return struct_pack::errc::no_buffer_space;
    }
    v |= (1ull * (now & 0x7fu)) << (i * 7);
    if ((now & 0x80U) == 0) {
      return {};
    }
  }
  return struct_pack::errc::invalid_buffer;
}
template <bool NotSkip = true,
#if __cpp_concepts >= 201907L
          reader_t Reader,
#else
          typename Reader,
#endif
          typename T>
[[nodiscard]] STRUCT_PACK_INLINE struct_pack::err_code deserialize_varint(
    Reader& reader, T& t) {
#if __cpp_concepts < 201907L
  static_assert(reader_t<Reader>, "The writer type must satisfy requirements!");
#endif
  uint64_t v = 0;
  auto ec = deserialize_varint_impl(reader, v);
  if constexpr (NotSkip) {
    if SP_LIKELY (!ec) {
      if constexpr (sintable_t<T>) {
        t = decode_zigzag<int64_t>(v);
      }
      else if constexpr (std::is_enum_v<T>) {
        t = static_cast<T>(v);
      }
      else {
        t = v;
      }
    }
  }
  return ec;
  // Variable-width integers, or varints,
  // are at the core of the wire format.
  // They allow encoding unsigned 64-bit integers using anywhere
  // between one and ten bytes, with small values using fewer bytes.
  // return decode_varint_v1(f);
}

template <typename T>
const auto& get_varint_value(const T& v) {
  if constexpr (varint_t<T>) {
    return v.get();
  }
  else {
    return v;
  }
}

template <typename T>
auto& get_varint_value(T& v) {
  if constexpr (varint_t<T>) {
    return v.get();
  }
  else {
    return v;
  }
}

}  // namespace detail
using var_int32_t = detail::sint<int32_t>;
using var_int64_t = detail::sint<int64_t>;
using var_uint32_t = detail::varint<uint32_t>;
using var_uint64_t = detail::varint<uint64_t>;
}  // namespace struct_pack
