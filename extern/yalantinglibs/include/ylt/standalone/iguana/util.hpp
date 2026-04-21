#pragma once

#include <math.h>

#include <filesystem>
#include <forward_list>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>

#include "define.h"
#include "detail/charconv.h"
#include "detail/pb_type.hpp"
#include "detail/traits.hpp"
#include "detail/utf.hpp"
#include "error_code.h"
#include "field_reflection.hpp"
#include "ylt/reflection/member_value.hpp"
#include "ylt/reflection/user_reflect_macro.hpp"

namespace iguana {
template <typename T>
inline constexpr bool char_v = std::is_same_v<std::decay_t<T>, char> ||
                               std::is_same_v<std::decay_t<T>, char16_t> ||
                               std::is_same_v<std::decay_t<T>, char32_t> ||
                               std::is_same_v<std::decay_t<T>, wchar_t>;

template <typename T>
inline constexpr bool bool_v =
    std::is_same_v<std::decay_t<T>, bool> ||
    std::is_same_v<std::decay_t<T>, std::vector<bool>::reference>;

template <typename T>
inline constexpr bool int_v = std::is_integral_v<std::decay_t<T>> &&
                              !char_v<std::decay_t<T>> && !bool_v<T>;

template <typename T>
inline constexpr bool float_v = std::is_floating_point_v<std::decay_t<T>>;

template <typename T>
inline constexpr bool num_v = float_v<T> || int_v<T>;

template <typename T>
inline constexpr bool enum_v = std::is_enum_v<std::decay_t<T>>;

template <typename T>
constexpr inline bool optional_v = ylt::reflection::optional<T>;

template <typename T>
constexpr inline bool pair_v = ylt::reflection::pair<T>;

template <class, class = void>
struct is_container : std::false_type {};

template <class T>
struct is_container<
    T, std::void_t<decltype(std::declval<T>().size(), std::declval<T>().begin(),
                            std::declval<T>().end())>> : std::true_type {};

template <class, class = void>
struct is_map_container : std::false_type {};

template <class T>
struct is_map_container<
    T, std::void_t<decltype(std::declval<typename T::mapped_type>())>>
    : is_container<T> {};

template <typename T>
constexpr inline bool container_v = is_container<std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool map_container_v =
    is_map_container<std::remove_cvref_t<T>>::value;

template <class T>
constexpr inline bool c_array_v = std::is_array_v<std::remove_cvref_t<T>>&&
                                      std::extent_v<std::remove_cvref_t<T>> > 0;

template <typename Type, typename = void>
struct is_array : std::false_type {};

template <typename T>
struct is_array<
    T, std::void_t<decltype(std::declval<T>().size()),
                   typename std::enable_if_t<(std::tuple_size<T>::value != 0)>>>
    : std::true_type {};

template <typename T>
constexpr inline bool array_v = is_array<std::remove_cvref_t<T>>::value;

template <typename Type>
constexpr inline bool fixed_array_v = c_array_v<Type> ||
#if __cplusplus > 201703L
#if __has_include(<span>)
                                      is_span<Type>::value ||
#endif
#endif
                                      array_v<Type>;

template <typename T>
constexpr inline bool string_view_v =
    is_template_instant_of<std::basic_string_view,
                           std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool string_v =
    is_template_instant_of<std::basic_string, std::remove_cvref_t<T>>::value;

// TODO: type must be char
template <typename T>
constexpr inline bool json_view_v = container_v<T>;

template <typename T>
constexpr inline bool json_byte_v =
    std::is_same_v<char, std::remove_cvref_t<T>> ||
    std::is_same_v<unsigned char, std::remove_cvref_t<T>> ||
    std::is_same_v<std::byte, std::remove_cvref_t<T>>;

template <typename T>
constexpr inline bool sequence_container_v =
    is_sequence_container<std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool tuple_v = is_tuple<std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool string_container_v = string_v<T> || string_view_v<T>;

template <typename T>
constexpr inline bool unique_ptr_v =
    is_template_instant_of<std::unique_ptr, std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool shared_ptr_v =
    is_template_instant_of<std::shared_ptr, std::remove_cvref_t<T>>::value;

template <typename T>
constexpr inline bool smart_ptr_v = shared_ptr_v<T> || unique_ptr_v<T>;

template <typename T>
constexpr inline bool variant_v = is_variant<std::remove_cvref_t<T>>::value;

template <size_t Idx, typename T>
using variant_element_t = std::remove_reference_t<decltype(std::get<Idx>(
    std::declval<std::remove_reference_t<T>>()))>;

template <typename F, typename Tuple, size_t... Is>
constexpr void foreach_tuple(F&& f, Tuple& tp, std::index_sequence<Is...>) {
  (void(f(std::get<Is>(tp), std::integral_constant<size_t, Is>{})), ...);
}

template <typename F, typename Tuple>
constexpr void foreach_tuple(F&& f, Tuple& tp) {
  foreach_tuple(std::forward<F>(f), tp,
                std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename F, typename T>
inline constexpr auto for_each_tuple(F&& f, T&& tup) {
  std::apply(
      [&f](auto&&... args) {
        (f(args), ...);
      },
      std::forward<T>(tup));
}

template <class T>
constexpr inline bool ylt_refletable_v =
    (ylt::reflection::is_ylt_refl_v<T> ||
     std::is_aggregate_v<
         ylt::reflection::remove_cvref_t<T>>)&&!fixed_array_v<T> &&
    !ylt::reflection::is_custom_refl_v<T> && !is_pb_type_v<T>;

template <class T>
constexpr inline bool non_ylt_refletable_v = !ylt_refletable_v<T>;

template <typename T>
constexpr inline bool plain_v =
    string_container_v<T> || num_v<T> || char_v<T> || bool_v<T> || enum_v<T>;

template <typename T>
struct underline_type {
  using type = T;
};

template <typename T>
struct underline_type<std::unique_ptr<T>> {
  using type = typename underline_type<T>::type;
};

template <typename T>
struct underline_type<std::shared_ptr<T>> {
  using type = typename underline_type<T>::type;
};

template <typename T>
struct underline_type<std::optional<T>> {
  using type = typename underline_type<T>::type;
};

template <typename T>
using underline_type_t = typename underline_type<std::remove_cvref_t<T>>::type;

struct memory_writer {
  char* buffer;
  void write(const char* data, std::size_t len) {
    memcpy(buffer, data, len);
    buffer += len;
  }
};

template <char... C, typename It>
IGUANA_INLINE void match(It&& it, It&& end) {
  const auto n = static_cast<size_t>(std::distance(it, end));
  if (n < sizeof...(C))
    IGUANA_UNLIKELY {
      // TODO: compile time generate this message, currently borken with
      // MSVC
      static constexpr auto error = "Unexpected end of buffer. Expected:";
      throw std::runtime_error(error);
    }
  if (((... || (*it++ != C))))
    IGUANA_UNLIKELY {
      // TODO: compile time generate this message, currently borken with
      // MSVC
      static constexpr char b[] = {C..., '\0'};
      throw std::runtime_error(std::string("Expected these: ").append(b));
    }
}

inline constexpr auto has_zero = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return (((chunk - 0x0101010101010101) & ~chunk) & 0x8080808080808080);
};

inline constexpr auto has_qoute = [](uint64_t chunk) IGUANA__INLINE_LAMBDA {
  return has_zero(
      chunk ^
      0b0010001000100010001000100010001000100010001000100010001000100010);
};

template <bool is_xml_serialization = false, typename Stream, typename Ch>
IGUANA_INLINE void write_unicode_to_string(Ch& it, Stream& ss) {
  static const char hexDigits[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                     '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
  unsigned codepoint = 0;
  if (!decode_utf8(it, codepoint))
    IGUANA_UNLIKELY { throw std::runtime_error("illegal unicode character"); }
  if constexpr (is_xml_serialization) {
    ss.append("&#x");
  }
  else {
    ss.push_back('\\');
    ss.push_back('u');
  }

  if (codepoint <= 0xD7FF || (codepoint >= 0xE000 && codepoint <= 0xFFFF)) {
    ss.push_back(hexDigits[(codepoint >> 12) & 15]);
    ss.push_back(hexDigits[(codepoint >> 8) & 15]);
    ss.push_back(hexDigits[(codepoint >> 4) & 15]);
    ss.push_back(hexDigits[(codepoint)&15]);
  }
  else {
    if (codepoint < 0x010000 || codepoint > 0x10FFFF)
      IGUANA_UNLIKELY { throw std::runtime_error("illegal codepoint"); }
    // Surrogate pair
    unsigned s = codepoint - 0x010000;
    unsigned lead = (s >> 10) + 0xD800;
    unsigned trail = (s & 0x3FF) + 0xDC00;
    ss.push_back(hexDigits[(lead >> 12) & 15]);
    ss.push_back(hexDigits[(lead >> 8) & 15]);
    ss.push_back(hexDigits[(lead >> 4) & 15]);
    ss.push_back(hexDigits[(lead)&15]);
    if constexpr (is_xml_serialization) {
      ss.append(";&#x");
    }
    else {
      ss.push_back('\\');
      ss.push_back('u');
    }
    ss.push_back(hexDigits[(trail >> 12) & 15]);
    ss.push_back(hexDigits[(trail >> 8) & 15]);
    ss.push_back(hexDigits[(trail >> 4) & 15]);
    ss.push_back(hexDigits[(trail)&15]);
  }
  if constexpr (is_xml_serialization) {
    ss.push_back(';');
  }
}

// https://github.com/Tencent/rapidjson/blob/master/include/rapidjson/writer.h
template <typename Ch, typename SizeType, typename Stream>
IGUANA_INLINE void write_string_with_escape(const Ch* it, SizeType length,
                                            Stream& ss) {
  static const char hexDigits[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                     '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
  static const char escape[256] = {
#define Z16 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
      // 0    1    2    3    4    5    6    7    8    9    A    B    C    D    E
      // F
      'u', 'u', 'u',  'u', 'u', 'u', 'u', 'u', 'b', 't',
      'n', 'u', 'f',  'r', 'u', 'u',  // 00
      'u', 'u', 'u',  'u', 'u', 'u', 'u', 'u', 'u', 'u',
      'u', 'u', 'u',  'u', 'u', 'u',  // 10
      0,   0,   '"',  0,   0,   0,   0,   0,   0,   0,
      0,   0,   0,    0,   0,   0,  // 20
      Z16, Z16,                     // 30~4F
      0,   0,   0,    0,   0,   0,   0,   0,   0,   0,
      0,   0,   '\\', 0,   0,   0,                       // 50
      Z16, Z16, Z16,  Z16, Z16, Z16, Z16, Z16, Z16, Z16  // 60~FF
#undef Z16
  };
  auto end = it;
  std::advance(end, length);
  while (it < end) {
    if (static_cast<unsigned>(*it) >= 0x80)
      IGUANA_UNLIKELY { write_unicode_to_string(it, ss); }
    else if (escape[static_cast<unsigned char>(*it)])
      IGUANA_UNLIKELY {
        ss.push_back('\\');
        ss.push_back(escape[static_cast<unsigned char>(*it)]);

        if (escape[static_cast<unsigned char>(*it)] == 'u') {
          // escape other control characters
          ss.push_back('0');
          ss.push_back('0');
          ss.push_back(hexDigits[static_cast<unsigned char>(*it) >> 4]);
          ss.push_back(hexDigits[static_cast<unsigned char>(*it) & 0xF]);
        }
        ++it;
      }
    else {
      ss.push_back(*(it++));
    }
  }
}

template <typename T, size_t N>
IGUANA_INLINE constexpr bool has_duplicate(const std::array<T, N>& arr) {
  for (size_t i = 0; i < arr.size(); i++) {
    for (size_t j = i + 1; j < arr.size(); j++) {
      if (arr[i] == arr[j]) {
        return true;
      }
    }
  }
  return false;
}

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
template <typename... Types>
IGUANA_INLINE constexpr bool has_duplicate_type() {
  std::array<std::string_view, sizeof...(Types)> arr{
      iguana::type_string<Types>()...};
  return has_duplicate(arr);
}

template <typename T>
struct has_duplicate_type_in_variant : std::false_type {};

template <typename... Us>
struct has_duplicate_type_in_variant<std::variant<Us...>> {
  inline constexpr static bool value = has_duplicate_type<Us...>();
};

template <typename T>
constexpr inline bool has_duplicate_type_v =
    has_duplicate_type_in_variant<T>::value;
#else
template <typename T>
constexpr inline bool has_duplicate_type_v = false;
#endif

}  // namespace iguana
