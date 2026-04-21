#pragma once
#include <array>
#include <string_view>

#include "frozen/string.h"
#include "frozen/unordered_map.h"

namespace ylt::reflection {

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)

template <typename T>
constexpr std::string_view get_raw_name() {
#ifdef _MSC_VER
  return __FUNCSIG__;
#else
  return __PRETTY_FUNCTION__;
#endif
}

template <auto T>
constexpr std::string_view get_raw_name() {
#ifdef _MSC_VER
  return __FUNCSIG__;
#else
  return __PRETTY_FUNCTION__;
#endif
}

template <typename T>
inline constexpr std::string_view type_string() {
  constexpr std::string_view sample = get_raw_name<int>();
  constexpr size_t prefix_length = sample.find("int");
  constexpr std::string_view str = get_raw_name<T>();
  constexpr size_t suffix_length = sample.size() - prefix_length - 3;
  constexpr auto name =
      str.substr(prefix_length, str.size() - prefix_length - suffix_length);
#if defined(_MSC_VER)
  constexpr size_t space_pos = name.find(" ");
  if constexpr (space_pos != std::string_view::npos) {
    constexpr auto prefix = name.substr(0, space_pos);
    if constexpr (prefix != "const" && prefix != "volatile") {
      return name.substr(space_pos + 1);
    }
  }
#endif
  return name;
}

template <auto T>
inline constexpr std::string_view enum_string() {
  constexpr std::string_view sample = get_raw_name<int>();
  constexpr size_t pos = sample.find("int");
  constexpr std::string_view str = get_raw_name<T>();
  constexpr auto next1 = str.rfind(sample[pos + 3]);
#if defined(__clang__) || defined(_MSC_VER)
  constexpr auto s1 = str.substr(pos, next1 - pos);
#else
  constexpr auto s1 = str.substr(pos + 5, next1 - pos - 5);
#endif
  return s1;
}

template <auto field>
inline constexpr std::string_view field_string() {
  constexpr std::string_view raw_name = enum_string<field>();
  return raw_name.substr(raw_name.rfind(":") + 1);
}

#if defined(__clang__) && (__clang_major__ >= 17)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wenum-constexpr-conversion"
#endif

template <typename E, E V>
constexpr std::string_view get_raw_name_with_v() {
#ifdef _MSC_VER
  return __FUNCSIG__;
#else
  return __PRETTY_FUNCTION__;
#endif
}

// True means the V is a valid enum value, and the second of the result is the
// name
template <typename E, E V>
constexpr std::pair<bool, std::string_view> try_get_enum_name() {
  constexpr std::string_view sample_raw_name = get_raw_name_with_v<int, 5>();
  constexpr size_t pos = sample_raw_name.find("5");
  constexpr std::string_view enum_raw_name = get_raw_name_with_v<E, V>();
  constexpr auto enum_end = enum_raw_name.rfind(&sample_raw_name[pos + 1]);
#ifdef _MSC_VER
  constexpr auto enum_begin = enum_raw_name.rfind(',', enum_end) + 1;
#else
  constexpr auto enum_begin = enum_raw_name.rfind(' ', enum_end) + 1;
#endif
  constexpr std::string_view res =
      enum_raw_name.substr(enum_begin, enum_end - enum_begin);

  constexpr size_t pos_brackets = res.find(')');

  size_t pos_colon = res.find("::");
  return {pos_brackets == std::string_view::npos,
          res.substr(pos_colon == std::string_view::npos ? 0 : pos_colon + 2)};
}

// Enumerate the numbers in a integer sequence to see if they are legal enum
// value
template <typename E, std::int64_t... Is>
constexpr inline auto get_enum_arr(
    const std::integer_sequence<std::int64_t, Is...> &) {
  constexpr std::size_t N = sizeof...(Is);
  std::array<std::string_view, N> enum_names = {};
  std::array<E, N> enum_values = {};
  std::size_t num = 0;
  (([&]() {
     constexpr auto res = try_get_enum_name<E, static_cast<E>(Is)>();
     if constexpr (res.first) {
       //  the Is is a valid enum value
       enum_names[num] = res.second;
       enum_values[num] = static_cast<E>(Is);
       ++num;
     }
   })(),
   ...);
  return std::make_tuple(num, enum_values, enum_names);
}

template <std::size_t N, const std::array<int, N> &arr, size_t... Is>
constexpr auto array_to_seq(const std::index_sequence<Is...> &) {
  return std::integer_sequence<std::int64_t, arr[Is]...>();
}

// convert array to map
template <typename E, size_t N, size_t... Is>
constexpr inline auto get_enum_to_str_map(
    const std::array<std::string_view, N> &enum_names,
    const std::array<E, N> &enum_values, const std::index_sequence<Is...> &) {
  return frozen::unordered_map<E, frozen::string, sizeof...(Is)>{
      {enum_values[Is], enum_names[Is]}...};
}

template <typename E, size_t N, size_t... Is>
constexpr inline auto get_str_to_enum_map(
    const std::array<std::string_view, N> &enum_names,
    const std::array<E, N> &enum_values, const std::index_sequence<Is...> &) {
  return frozen::unordered_map<frozen::string, E, sizeof...(Is)>{
      {enum_names[Is], enum_values[Is]}...};
}

#endif

// the default generic enum_value
// if the user has not defined a specialization template, this will be called
template <typename T>
struct enum_value {
  constexpr static std::array<int, 0> value = {};
};

template <bool str_to_enum, typename E>
constexpr inline auto get_enum_map() {
#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
  constexpr auto &arr = enum_value<E>::value;
  constexpr auto arr_size = arr.size();
  if constexpr (arr_size > 0) {
    // the user has defined a specialization template
    constexpr auto arr_seq =
        array_to_seq<arr_size, arr>(std::make_index_sequence<arr_size>());
    constexpr auto t = get_enum_arr<E>(arr_seq);
    if constexpr (str_to_enum) {
      return get_str_to_enum_map<E, arr_size>(
          std::get<2>(t), std::get<1>(t),
          std::make_index_sequence<std::get<0>(t)>{});
    }
    else {
      return get_enum_to_str_map<E, arr_size>(
          std::get<2>(t), std::get<1>(t),
          std::make_index_sequence<std::get<0>(t)>{});
    }
  }
  else {
    return false;
  }
#else
  return false;
#endif
}

#if defined(__clang__) && (__clang_major__ >= 17)
#pragma clang diagnostic pop
#endif

}  // namespace ylt::reflection