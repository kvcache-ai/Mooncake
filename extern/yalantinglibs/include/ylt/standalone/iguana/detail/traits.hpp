//
// Created by QY on 2017-01-05.
//

#ifndef SERIALIZE_TRAITS_HPP
#define SERIALIZE_TRAITS_HPP

#include <deque>
#include <list>
#include <map>
#include <queue>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#if __cplusplus > 201703L
#if __has_include(<span>)
#include <span>
#endif
#endif

#include "iguana/define.h"

namespace iguana {

template <class T>
struct is_signed_intergral_like
    : std::integral_constant<bool, (std::is_integral<T>::value) &&
                                       std::is_signed<T>::value> {};

template <class T>
struct is_unsigned_intergral_like
    : std::integral_constant<bool, (std::is_integral<T>::value) &&
                                       std::is_unsigned<T>::value> {};

template <template <typename...> class U, typename T>
struct is_template_instant_of : std::false_type {};

template <template <typename...> class U, typename... args>
struct is_template_instant_of<U, U<args...>> : std::true_type {};

template <typename T>
struct is_stdstring : is_template_instant_of<std::basic_string, T> {};

template <typename T>
struct is_tuple : is_template_instant_of<std::tuple, T> {};

#if __cplusplus > 201703L
#if __has_include(<span>)
template <typename>
struct is_span : std::false_type {};

template <typename T, size_t Extent>
struct is_span<std::span<T, Extent>> : std::true_type {};
#endif
#endif

template <class T>
struct is_sequence_container
    : std::integral_constant<
          bool, is_template_instant_of<std::deque, T>::value ||
                    is_template_instant_of<std::list, T>::value ||
                    is_template_instant_of<std::vector, T>::value ||
                    is_template_instant_of<std::queue, T>::value> {};

template <class T>
struct is_associat_container
    : std::integral_constant<
          bool, is_template_instant_of<std::map, T>::value ||
                    is_template_instant_of<std::unordered_map, T>::value> {};

template <class T>
struct is_emplace_back_able
    : std::integral_constant<
          bool, is_template_instant_of<std::deque, T>::value ||
                    is_template_instant_of<std::list, T>::value ||
                    is_template_instant_of<std::vector, T>::value> {};

template <typename T, typename Tuple>
struct has_type;

template <typename T, typename... Us>
struct has_type<T, std::tuple<Us...>>
    : std::disjunction<std::is_same<T, Us>...> {};

template <class T>
struct member_tratis {};

template <class T, class Owner>
struct member_tratis<T Owner::*> {
  using owner_type = Owner;
  using value_type = T;
};

template <typename T>
inline constexpr bool is_int64_v =
    std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>;

template <typename T>
struct is_variant : std::false_type {};

template <typename... T>
struct is_variant<std::variant<T...>> : std::true_type {};

template <class T>
struct member_traits {
  using value_type = T;
};

template <class T, class Owner>
struct member_traits<T Owner::*> {
  using owner_type = Owner;
  using value_type = T;
};

template <class T>
using member_value_type_t = typename member_traits<T>::value_type;

template <typename T, std::size_t I, typename = void>
struct variant_type_at {
  using type = T;
};

template <typename T, std::size_t I>
struct variant_type_at<T, I, std::enable_if_t<is_variant<T>::value>> {
  using type = std::variant_alternative_t<I, T>;
};
template <typename T, std::size_t I>
using variant_type_at_t =
    typename variant_type_at<typename member_traits<T>::value_type, I>::type;

/// concepts
template <typename Type>
constexpr bool is_char_v =
    std::is_same_v<Type, signed char> || std::is_same_v<Type, char> ||
    std::is_same_v<Type, unsigned char> || std::is_same_v<Type, wchar_t> ||
    std::is_same_v<Type, char16_t> || std::is_same_v<Type, char32_t>
#ifdef __cpp_lib_char8_t
    || std::is_same_v<Type, char8_t>
#endif
    ;

#if __cplusplus >= 202002L
template <typename Type>
concept is_container_v = requires(Type container) {
  typename std::remove_cvref_t<Type>::value_type;
  container.size();
  container.begin();
  container.end();
};

template <typename Type>
concept is_resizable_char_container_v = requires(Type container) {
  typename std::remove_cvref_t<Type>::value_type;
  container.size();
  container.begin();
  container.end();
  requires is_char_v<typename std::remove_cvref_t<Type>::value_type>;
  container.resize(std::size_t{});
};

template <typename T>
concept char_writer = requires(T t) {
  t.write((const char *)nullptr, std::size_t{});
};

template <typename T>
concept char_reader = requires(T t) {
  t.read((char *)nullptr, std::size_t{});
};
#else
template <typename T, typename = void>
struct container_impl : std::false_type {};

template <typename T>
struct container_impl<T,
                      std::void_t<typename std::remove_cvref_t<T>::value_type,
                                  decltype(std::declval<T>().size()),
                                  decltype(std::declval<T>().begin()),
                                  decltype(std::declval<T>().end())>>
    : std::true_type {};

template <typename T>
constexpr bool is_container_v = container_impl<T>::value;

template <typename T, typename = void>
struct resizable_char_container_impl : std::false_type {};

template <typename T>
struct resizable_char_container_impl<
    T, std::void_t<typename std::remove_cvref_t<T>::value_type,
                   decltype(std::declval<T>().size()),
                   decltype(std::declval<T>().begin()),
                   decltype(std::declval<T>().end()),
                   std::enable_if_t<
                       is_char_v<typename std::remove_cvref_t<T>::value_type>>,
                   decltype(std::declval<T>().resize(std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool is_resizable_char_container_v =
    resizable_char_container_impl<T>::value;

template <typename T, typename = void>
struct char_writer_impl : std::false_type {};

template <typename T>
struct char_writer_impl<T, std::void_t<decltype(std::declval<T>().write(
                               (const char *)nullptr, std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool char_writer = char_writer_impl<T>::value;

template <typename T, typename = void>
struct char_reader_impl : std::false_type {};

template <typename T>
struct char_reader_impl<T, std::void_t<decltype(std::declval<T>().read(
                               (char *)nullptr, std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool char_reader = char_reader_impl<T>::value;
#endif

}  // namespace iguana
#endif  // SERIALIZE_TRAITS_HPP
