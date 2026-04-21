#pragma once
#include <cassert>
#include <cstddef>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common.hpp"
#include "detail/pb_type.hpp"
#include "util.hpp"

namespace iguana {

enum class WireType : uint32_t {
  Varint = 0,
  Fixed64 = 1,
  LengthDelimeted = 2,
  StartGroup = 3,
  EndGroup = 4,
  Fixed32 = 5,
  Unknown
};

template <typename T>
constexpr bool inherits_from_base_v = std::is_base_of_v<detail::base, T>;

namespace detail {
template <typename T>
constexpr bool is_fixed_v =
    std::is_same_v<T, fixed32_t> || std::is_same_v<T, fixed64_t> ||
    std::is_same_v<T, sfixed32_t> || std::is_same_v<T, sfixed64_t>;

template <typename T>
constexpr bool is_signed_varint_v =
    std::is_same_v<T, sint32_t> || std::is_same_v<T, sint64_t>;

template <typename T>
constexpr inline WireType get_wire_type() {
  if constexpr (std::is_integral_v<T> || is_signed_varint_v<T> ||
                std::is_enum_v<T> || std::is_same_v<T, bool>) {
    return WireType::Varint;
  }
  else if constexpr (std::is_same_v<T, fixed32_t> ||
                     std::is_same_v<T, sfixed32_t> ||
                     std::is_same_v<T, float>) {
    return WireType::Fixed32;
  }
  else if constexpr (std::is_same_v<T, fixed64_t> ||
                     std::is_same_v<T, sfixed64_t> ||
                     std::is_same_v<T, double>) {
    return WireType::Fixed64;
  }
  else if constexpr (std::is_same_v<T, std::string> ||
                     std::is_same_v<T, std::string_view> ||
                     ylt_refletable_v<T> || is_sequence_container<T>::value ||
                     is_map_container<T>::value) {
    return WireType::LengthDelimeted;
  }
  else if constexpr (optional_v<T>) {
    return get_wire_type<typename T::value_type>();
  }
  else {
    throw std::runtime_error("unknown type");
  }
}

template <typename T>
constexpr bool is_lenprefix_v = (get_wire_type<T>() ==
                                 WireType::LengthDelimeted);

[[nodiscard]] IGUANA_INLINE uint32_t encode_zigzag(int32_t v) {
  return (static_cast<uint32_t>(v) << 1U) ^
         static_cast<uint32_t>(
             -static_cast<int32_t>(static_cast<uint32_t>(v) >> 31U));
}
[[nodiscard]] IGUANA_INLINE uint64_t encode_zigzag(int64_t v) {
  return (static_cast<uint64_t>(v) << 1U) ^
         static_cast<uint64_t>(
             -static_cast<int64_t>(static_cast<uint64_t>(v) >> 63U));
}

[[nodiscard]] IGUANA_INLINE int64_t decode_zigzag(uint64_t u) {
  return static_cast<int64_t>((u >> 1U)) ^
         static_cast<uint64_t>(-static_cast<int64_t>(u & 1U));
}
[[nodiscard]] IGUANA_INLINE int64_t decode_zigzag(uint32_t u) {
  return static_cast<int64_t>((u >> 1U)) ^
         static_cast<uint64_t>(-static_cast<int64_t>(u & 1U));
}

template <class T>
IGUANA_INLINE uint64_t decode_varint(T& data, size_t& pos) {
  const int8_t* begin = reinterpret_cast<const int8_t*>(data.data());
  const int8_t* end = begin + data.size();
  const int8_t* p = begin;
  uint64_t val = 0;

  if ((static_cast<uint64_t>(*p) & 0x80) == 0) {
    pos = 1;
    return static_cast<uint64_t>(*p);
  }

  // end is always greater than or equal to begin, so this subtraction is safe
  if (size_t(end - begin) >= 10)
    IGUANA_LIKELY {  // fast path
      int64_t b;
      do {
        b = *p++;
        val = (b & 0x7f);
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 7;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 14;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 21;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 28;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 35;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 42;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 49;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x7f) << 56;
        if (b >= 0) {
          break;
        }
        b = *p++;
        val |= (b & 0x01) << 63;
        if (b >= 0) {
          break;
        }
        throw std::invalid_argument("Invalid varint value: too many bytes.");
      } while (false);
    }
  else {
    int shift = 0;
    while (p != end && *p < 0) {
      val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
      shift += 7;
    }
    if (p == end)
      IGUANA_UNLIKELY {
        throw std::invalid_argument("Invalid varint value: too few bytes.");
      }
    val |= static_cast<uint64_t>(*p++) << shift;
  }

  pos = (p - begin);
  return val;
}

// value == 0 ? 1 : floor(log2(value)) / 7 + 1
constexpr size_t variant_uint32_size_constexpr(uint32_t value) {
  if (value == 0) {
    return 1;
  }
  int log = 0;
  while (value >>= 1) ++log;
  return log / 7 + 1;
}

template <uint64_t v, typename Writer, size_t... I>
IGUANA_INLINE void append_varint_u32(Writer& writer,
                                     std::index_sequence<I...>) {
  uint8_t temp = 0;
  ((temp = static_cast<uint8_t>(v >> (7 * I)),
    writer.write((const char*)&temp, 1)),
   ...);
}

template <uint32_t v, typename Writer>
IGUANA_INLINE void serialize_varint_u32(Writer& writer) {
  constexpr auto size = variant_uint32_size_constexpr(v);
  append_varint_u32<v>(writer, std::make_index_sequence<size - 1>{});
  uint8_t temp = static_cast<uint8_t>(v >> (7 * (size - 1)));
  writer.write((const char*)&temp, 1);
}

template <typename Writer>
IGUANA_INLINE void serialize_varint(uint64_t v, Writer& writer) {
  uint8_t temp = static_cast<uint8_t>(v);
  if (v < 0x80) {
    writer.write((const char*)&temp, 1);
    return;
  }
  temp = static_cast<uint8_t>(v | 0x80);
  writer.write((const char*)&temp, 1);
  v >>= 7;
  if (v < 0x80) {
    temp = static_cast<uint8_t>(v);
    writer.write((const char*)&temp, 1);
    return;
  }
  do {
    temp = static_cast<uint8_t>(v | 0x80);
    writer.write((const char*)&temp, 1);
    v >>= 7;
  } while (v >= 0x80);
  temp = static_cast<uint8_t>(v);
  writer.write((const char*)&temp, 1);
}

IGUANA_INLINE uint32_t log2_floor_uint32(uint32_t n) {
#if defined(__GNUC__)
  return 31 ^ static_cast<uint32_t>(__builtin_clz(n));
#else
  unsigned long where;
  _BitScanReverse(&where, n);
  return where;
#endif
}

IGUANA_INLINE size_t variant_uint32_size(uint32_t value) {
  // This computes value == 0 ? 1 : floor(log2(value)) / 7 + 1
  // Use an explicit multiplication to implement the divide of
  // a number in the 1..31 range.
  // Explicit OR 0x1 to avoid calling Bits::Log2FloorNonZero(0), which is
  // undefined.
  uint32_t log2value = log2_floor_uint32(value | 0x1);
  return static_cast<size_t>((log2value * 9 + 73) / 64);
}

IGUANA_INLINE int Log2FloorNonZero_Portable(uint32_t n) {
  if (n == 0)
    return -1;
  int log = 0;
  uint32_t value = n;
  for (int i = 4; i >= 0; --i) {
    int shift = (1 << i);
    uint32_t x = value >> shift;
    if (x != 0) {
      value = x;
      log += shift;
    }
  }
  assert(value == 1);
  return log;
}

IGUANA_INLINE uint32_t Log2FloorNonZero(uint32_t n) {
#if defined(__GNUC__)
  return 31 ^ static_cast<uint32_t>(__builtin_clz(n));
#elif defined(_MSC_VER)
  unsigned long where;
  _BitScanReverse(&where, n);
  return where;
#else
  return Log2FloorNonZero_Portable(n);
#endif
}

IGUANA_INLINE int Log2FloorNonZero64_Portable(uint64_t n) {
  const uint32_t topbits = static_cast<uint32_t>(n >> 32);
  if (topbits == 0) {
    // Top bits are zero, so scan in bottom bits
    return static_cast<int>(Log2FloorNonZero(static_cast<uint32_t>(n)));
  }
  else {
    return 32 + static_cast<int>(Log2FloorNonZero(topbits));
  }
}

IGUANA_INLINE uint32_t log2_floor_uint64(uint64_t n) {
#if defined(__GNUC__)
  return 63 ^ static_cast<uint32_t>(__builtin_clzll(n));
#elif defined(_MSC_VER) && defined(_M_X64)
  unsigned long where;
  _BitScanReverse64(&where, n);
  return where;
#else
  return Log2FloorNonZero64_Portable(n);
#endif
}

IGUANA_INLINE size_t variant_uint64_size(uint64_t value) {
  // This computes value == 0 ? 1 : floor(log2(value)) / 7 + 1
  // Use an explicit multiplication to implement the divide of
  // a number in the 1..63 range.
  // Explicit OR 0x1 to avoid calling Bits::Log2FloorNonZero(0), which is
  // undefined.
  uint32_t log2value = log2_floor_uint64(value | 0x1);
  return static_cast<size_t>((log2value * 9 + 73) / 64);
}

template <typename U>
constexpr IGUANA_INLINE size_t variant_intergal_size(U value) {
  using T = std::remove_reference_t<U>;
  if constexpr (sizeof(T) == 8) {
    return variant_uint64_size(static_cast<uint64_t>(value));
  }
  else if constexpr (sizeof(T) <= 4) {
    if constexpr (std::is_signed_v<T>) {
      if (value < 0) {
        return 10;
      }
    }
    return variant_uint32_size(static_cast<uint32_t>(value));
  }
  else {
    static_assert(!sizeof(T), "intergal in not supported");
  }
}

template <typename F, size_t... I>
IGUANA_INLINE constexpr void for_each_n(F&& f, std::index_sequence<I...>) {
  (std::forward<F>(f)(std::integral_constant<size_t, I>{}), ...);
}

template <size_t key_size, bool omit_default_val, typename Type>
IGUANA_INLINE size_t str_numeric_size(Type&& t) {
  using T = std::remove_const_t<std::remove_reference_t<Type>>;
  if constexpr (std::is_same_v<T, std::string> ||
                std::is_same_v<T, std::string_view>) {
    // string
    if constexpr (omit_default_val) {
      if (t.size() == 0)
        IGUANA_UNLIKELY { return 0; }
    }
    if constexpr (key_size == 0) {
      return t.size();
    }
    else {
      return key_size + variant_uint32_size(static_cast<uint32_t>(t.size())) +
             t.size();
    }
  }
  else {
    // numeric
    if constexpr (omit_default_val) {
      if constexpr (is_fixed_v<T> || is_signed_varint_v<T>) {
        if (t.val == 0)
          IGUANA_UNLIKELY { return 0; }
      }
      else {
        if (t == static_cast<T>(0))
          IGUANA_UNLIKELY { return 0; }
      }
    }
    if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_same_v<bool, T>) {
        return 1 + key_size;
      }
      else {
        return key_size + variant_intergal_size(t);
      }
    }
    else if constexpr (detail::is_signed_varint_v<T>) {
      return key_size + variant_intergal_size(encode_zigzag(t.val));
    }
    else if constexpr (detail::is_fixed_v<T>) {
      return key_size + sizeof(typename T::value_type);
    }
    else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
      return key_size + sizeof(T);
    }
    else if constexpr (std::is_enum_v<T>) {
      using U = std::underlying_type_t<T>;
      return key_size + variant_intergal_size(static_cast<U>(t));
    }
    else {
      static_assert(!sizeof(T), "err");
    }
  }
}

template <size_t key_size, bool omit_default_val = true, typename Type,
          typename Arr>
IGUANA_INLINE size_t pb_key_value_size(Type&& t, Arr& size_arr);

template <typename Variant, typename T, size_t I>
constexpr inline size_t get_variant_index() {
  if constexpr (I == 0) {
    static_assert(std::is_same_v<std::variant_alternative_t<0, Variant>, T>,
                  "Type T is not found in Variant");
    return 0;
  }
  else if constexpr (std::is_same_v<std::variant_alternative_t<I, Variant>,
                                    T>) {
    return I;
  }
  else {
    return get_variant_index<Variant, T, I - 1>();
  }
}

template <size_t field_no, typename Type, typename Arr>
IGUANA_INLINE size_t pb_oneof_size(Type&& t, Arr& size_arr) {
  using T = std::decay_t<Type>;
  int len = 0;
  std::visit(
      [&len, &size_arr](auto&& value) IGUANA__INLINE_LAMBDA {
        using raw_value_type = decltype(value);
        using value_type =
            std::remove_const_t<std::remove_reference_t<decltype(value)>>;
        constexpr auto offset =
            get_variant_index<T, value_type, std::variant_size_v<T> - 1>();
        constexpr uint32_t key =
            ((field_no + offset) << 3) |
            static_cast<uint32_t>(get_wire_type<value_type>());
        len = pb_key_value_size<variant_uint32_size_constexpr(key), false>(
            std::forward<raw_value_type>(value), size_arr);
      },
      std::forward<Type>(t));
  return len;
}

// returns size = key_size + optional(len_size) + len
// when key_size == 0, return len
template <size_t key_size, bool omit_default_val, typename Type, typename Arr>
IGUANA_INLINE size_t pb_key_value_size(Type&& t, Arr& size_arr) {
  using T = std::remove_const_t<std::remove_reference_t<Type>>;
  if constexpr (ylt_refletable_v<T> || is_custom_reflection_v<T>) {
    size_t len = 0;
    static auto tuple = get_pb_members_tuple(std::forward<Type>(t));
    constexpr size_t SIZE = std::tuple_size_v<std::decay_t<decltype(tuple)>>;
    size_t pre_index = -1;
    if constexpr (!inherits_from_base_v<T> && key_size != 0) {
      pre_index = size_arr.size();
      size_arr.push_back(0);  // placeholder
    }
    for_each_n(
        [&len, &t, &size_arr](auto i) IGUANA__INLINE_LAMBDA {
          using field_type =
              std::tuple_element_t<decltype(i)::value,
                                   std::decay_t<decltype(tuple)>>;
          auto value = std::get<decltype(i)::value>(tuple);
          using U = typename field_type::value_type;
          using sub_type = typename field_type::sub_type;
          auto& val = value.value(t);
          if constexpr (variant_v<U>) {
            constexpr auto offset =
                get_variant_index<U, sub_type, std::variant_size_v<U> - 1>();
            if constexpr (offset == 0) {
              len += pb_oneof_size<value.field_no>(val, size_arr);
            }
          }
          else {
            constexpr uint32_t sub_key =
                (value.field_no << 3) |
                static_cast<uint32_t>(get_wire_type<U>());
            constexpr auto sub_keysize = variant_uint32_size_constexpr(sub_key);
            len += pb_key_value_size<sub_keysize>(val, size_arr);
          }
        },
        std::make_index_sequence<SIZE>{});
    if constexpr (inherits_from_base_v<T>) {
      t.cache_size = len;
    }
    else if constexpr (key_size != 0) {
      size_arr[pre_index] = len;
    }
    if constexpr (key_size == 0) {
      // for top level
      return len;
    }
    else {
      if (len == 0) {
        // equals key_size  + variant_uint32_size(len)
        return key_size + 1;
      }
      else {
        return key_size + variant_uint32_size(static_cast<uint32_t>(len)) + len;
      }
    }
  }
  else if constexpr (is_sequence_container<T>::value) {
    using item_type = typename T::value_type;
    size_t len = 0;
    if constexpr (is_lenprefix_v<item_type>) {
      for (auto& item : t) {
        len += pb_key_value_size<key_size, false>(item, size_arr);
      }
      return len;
    }
    else {
      for (auto& item : t) {
        // here 0 to get pakced size, and item must be numeric
        len += str_numeric_size<0, false>(item);
      }
      if (len == 0) {
        return 0;
      }
      else {
        return key_size + variant_uint32_size(static_cast<uint32_t>(len)) + len;
      }
    }
  }
  else if constexpr (is_map_container<T>::value) {
    size_t len = 0;
    for (auto& [k, v] : t) {
      // the key_size of  k and v  is constant 1
      auto kv_len = pb_key_value_size<1, false>(k, size_arr) +
                    pb_key_value_size<1, false>(v, size_arr);
      len += key_size + variant_uint32_size(static_cast<uint32_t>(kv_len)) +
             kv_len;
    }
    return len;
  }
  else if constexpr (optional_v<T>) {
    if (!t.has_value()) {
      return 0;
    }
    return pb_key_value_size<key_size, omit_default_val>(*t, size_arr);
  }
  else {
    return str_numeric_size<key_size, omit_default_val>(t);
  }
}

// return the payload size
template <bool skip_next = true, typename Type>
IGUANA_INLINE size_t pb_value_size(Type&& t, uint32_t*& sz_ptr) {
  using T = std::remove_const_t<std::remove_reference_t<Type>>;
  if constexpr (ylt_refletable_v<T> || is_custom_reflection_v<T>) {
    if constexpr (inherits_from_base_v<T>) {
      return t.cache_size;
    }
    else {
      // *sz_ptr is secure and logically guaranteed
      if constexpr (skip_next) {
        return *(sz_ptr++);
      }
      else {
        return *sz_ptr;
      }
    }
  }
  else if constexpr (is_sequence_container<T>::value) {
    using item_type = typename T::value_type;
    size_t len = 0;
    if constexpr (!is_lenprefix_v<item_type>) {
      for (auto& item : t) {
        len += str_numeric_size<0, false>(item);
      }
      return len;
    }
    else {
      static_assert(!sizeof(item_type), "the size of this type is meaningless");
    }
  }
  else if constexpr (is_map_container<T>::value) {
    static_assert(!sizeof(T), "the size of this type is meaningless");
  }
  else if constexpr (optional_v<T>) {
    if (!t.has_value()) {
      return 0;
    }
    return pb_value_size(*t, sz_ptr);
  }
  else {
    return str_numeric_size<0, false>(t);
  }
}

}  // namespace detail
}  // namespace iguana