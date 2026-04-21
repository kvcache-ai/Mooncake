#pragma once

#if (defined(_MSC_VER) && _MSC_VER >= 1920 && _MSVC_LANG >= 202002L) || \
    (!defined(_MSC_VER) && defined(__cplusplus) && __cplusplus >= 202002L)
#include <bit>
#endif
#include <array>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#if defined __clang__
#define IGUANA_INLINE __attribute__((always_inline)) inline
#define IGUANA__INLINE_LAMBDA __attribute__((always_inline)) constexpr
#elif defined _MSC_VER
#define IGUANA_INLINE __forceinline
#define IGUANA__INLINE_LAMBDA constexpr
#else
#define IGUANA_INLINE __attribute__((always_inline)) inline
#define IGUANA__INLINE_LAMBDA constexpr __attribute__((always_inline))
#endif

#if __has_cpp_attribute(likely) && __has_cpp_attribute(unlikely) && \
    __cplusplus >= 202002L
#define IGUANA_LIKELY [[likely]]
#define IGUANA_UNLIKELY [[unlikely]]
#else
#define IGUANA_LIKELY
#define IGUANA_UNLIKELY
#endif

#ifdef _MSC_VER

#if _MSC_VER >= 1920 && _MSVC_LANG >= 202002L
IGUANA_INLINE int countr_zero(unsigned long long x) {
  return std::countr_zero(x);
}
template <typename T>
constexpr inline bool contiguous_iterator = std::contiguous_iterator<T>;

#else
IGUANA_INLINE int countr_zero(unsigned long long n) {
  // x will never be zero in iguana
#if defined(_MSC_VER) && defined(_M_X64)
  unsigned long c;
  _BitScanForward64(&c, n);
  return static_cast<int>(c);
#elif defined(_MSC_VER) && defined(_M_IX86)
  unsigned long c;
  if (static_cast<uint32_t>(n) != 0) {
    _BitScanForward(&c, static_cast<uint32_t>(n));
    return static_cast<int>(c);
  }
  else {
    _BitScanForward(&c, static_cast<uint32_t>(n >> 32));
    return static_cast<int>(c) + 32;
  }
#endif
}

namespace std {
template <typename T>
using remove_cvref_t =
    typename remove_cv<typename remove_reference<T>::type>::type;
}
template <typename T>
constexpr inline bool contiguous_iterator =
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::vector<char>::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::vector<char>::const_iterator> ||
    std::is_same_v<std::remove_cvref_t<T>, typename std::string::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string::const_iterator> ||
    std::is_same_v<std::remove_cvref_t<T>, char *> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string_view::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string_view::const_iterator>;
#endif

#else

#if __cplusplus >= 202002L
IGUANA_INLINE int countr_zero(unsigned long long x) {
  return std::countr_zero(x);
}
template <typename T>
constexpr inline bool contiguous_iterator = std::contiguous_iterator<T>;
#else
IGUANA_INLINE int countr_zero(unsigned long long x) {
  // x will never be zero in iguana
  return __builtin_ctzll(x);
}
namespace std {
template <typename T>
using remove_cvref_t =
    typename remove_cv<typename remove_reference<T>::type>::type;
}
template <typename T>
constexpr inline bool contiguous_iterator =
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::vector<char>::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::vector<char>::const_iterator> ||
    std::is_same_v<std::remove_cvref_t<T>, typename std::string::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string::const_iterator> ||
    std::is_same_v<std::remove_cvref_t<T>, char *> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string_view::iterator> ||
    std::is_same_v<std::remove_cvref_t<T>,
                   typename std::string_view::const_iterator>;
#endif

#endif
