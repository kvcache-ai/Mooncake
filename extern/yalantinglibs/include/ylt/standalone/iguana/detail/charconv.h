#pragma once
#include <charconv>

#include "dragonbox_to_chars.h"
#include "fast_float.h"
#include "iguana/define.h"
#include "itoa.hpp"

namespace iguana {
template <typename T>
struct is_char_type
    : std::disjunction<std::is_same<T, char>, std::is_same<T, wchar_t>,
                       std::is_same<T, char16_t>, std::is_same<T, char32_t>> {};

inline void *to_chars_float(...) {
  throw std::runtime_error("not allowed to invoke");
  return {};
}

template <typename T, typename Ret = decltype(to_chars_float(
                          std::declval<T>(), std::declval<char *>()))>
using return_of_tochars = std::conditional_t<std::is_same_v<Ret, char *>,
                                             std::true_type, std::false_type>;
// here std::true_type is used as a type , any other type is also ok.
using has_to_chars_float = iguana::return_of_tochars<std::true_type>;

namespace detail {

// check_number==true: check if the string [first, last) is a legal number
template <bool check_number = true, typename U>
std::pair<const char *, std::errc> from_chars(const char *first,
                                              const char *last, U &value) {
  using T = std::decay_t<U>;
  if constexpr (std::is_floating_point_v<T>) {
    auto [p, ec] = fast_float::from_chars(first, last, value);
    if constexpr (check_number) {
      if (p != last || ec != std::errc{})
        IGUANA_UNLIKELY { throw std::runtime_error("Failed to parse number"); }
    }
    return {p, ec};
  }
  else {
    auto [p, ec] = std::from_chars(first, last, value);
    if constexpr (check_number) {
      if (p != last || ec != std::errc{})
        IGUANA_UNLIKELY { throw std::runtime_error("Failed to parse number"); }
    }
    return {p, ec};
  }
}

// not support uint8 for now
template <typename T>
char *to_chars(char *buffer, T value) noexcept {
  using U = std::decay_t<T>;
  if constexpr (std::is_floating_point_v<U>) {
    if constexpr (has_to_chars_float::value) {
      return static_cast<char *>(to_chars_float(value, buffer));
    }
    else {
      return jkj::dragonbox::to_chars(value, buffer);
    }
  }
  else if constexpr (std::is_signed_v<U> && (sizeof(U) >= 8)) {
    return xtoa(value, buffer, 10, 1);  // int64_t
  }
  else if constexpr (std::is_unsigned_v<U> && (sizeof(U) >= 8)) {
    return xtoa(value, buffer, 10, 0);  // uint64_t
  }
  else if constexpr (std::is_integral_v<U> && (sizeof(U) > 1)) {
    return itoa_fwd(value, buffer);  // only support more than 2 bytes intergal
  }
  else if constexpr (!is_char_type<U>::value) {
    return itoa_fwd(static_cast<int>(value),
                    buffer);  // only support more than 2 bytes intergal
  }
  else {
    static_assert(!sizeof(U), "only support arithmetic type except char type");
  }
}

}  // namespace detail
}  // namespace iguana
