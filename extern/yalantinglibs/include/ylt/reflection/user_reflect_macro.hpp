#pragma once
#include <array>
#include <string_view>
#include <tuple>
#include <type_traits>

#include "internal/arg_list_macro.hpp"

namespace ylt::reflection {
template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

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

#define YLT_REFL(STRUCT, ...)                                                \
  template <typename Visitor>                                                \
  inline static constexpr decltype(auto) refl_visit_members(                 \
      STRUCT &t, Visitor &&visitor) {                                        \
    return visitor(WRAP_ARGS(CONCAT_MEMBER, t, ##__VA_ARGS__));              \
  }                                                                          \
  template <typename Visitor>                                                \
  inline static constexpr decltype(auto) refl_visit_members(                 \
      const STRUCT &t, Visitor &&visitor) {                                  \
    return visitor(WRAP_ARGS(CONCAT_MEMBER, t, ##__VA_ARGS__));              \
  }                                                                          \
  [[maybe_unused]] inline static decltype(auto) refl_object_to_tuple(        \
      STRUCT &t) {                                                           \
    return std::tie(WRAP_ARGS(CONCAT_MEMBER, t, ##__VA_ARGS__));             \
  }                                                                          \
  [[maybe_unused]] inline static decltype(auto) refl_object_to_tuple(        \
      const STRUCT &t) {                                                     \
    return std::tie(WRAP_ARGS(CONCAT_MEMBER, t, ##__VA_ARGS__));             \
  }                                                                          \
  [[maybe_unused]] inline static constexpr decltype(auto) refl_member_names( \
      const ylt::reflection::identity<STRUCT> &t) {                          \
    constexpr std::array<std::string_view, YLT_ARG_COUNT(__VA_ARGS__)> arr{  \
        WRAP_ARGS(CONCAT_NAME, t, ##__VA_ARGS__)};                           \
    return arr;                                                              \
  }                                                                          \
  [[maybe_unused]] inline static constexpr std::size_t refl_member_count(    \
      const ylt::reflection::identity<STRUCT> &t) {                          \
    return (std::size_t)YLT_ARG_COUNT(__VA_ARGS__);                          \
  }

template <typename T, typename Tuple, typename Visitor>
inline constexpr auto visit_private_fields_impl(T &&t, const Tuple &tp,
                                                Visitor &&visitor) {
  return std::apply(
      [&](auto... args) {
        return visitor(t.*args...);
      },
      tp);
}

template <typename T, typename Visitor>
inline constexpr auto visit_private_fields(T &&t, Visitor &&visitor) {
  auto tp = get_private_ptrs(
      identity<std::remove_const_t<std::remove_reference_t<T>>>{});
  return visit_private_fields_impl(std::forward<T>(t), tp, visitor);
}

template <typename T>
inline static decltype(auto) refl_object_to_tuple_impl(T &&t) {
  auto tp = get_private_ptrs(
      identity<std::remove_const_t<std::remove_reference_t<T>>>{});
  auto to_ref = [&t](auto... fields) {
    return std::tie(t.*fields...);
  };
  return std::apply(to_ref, tp);
}

#define YLT_REFL_PRIVATE_(STRUCT, ...)                                    \
  namespace ylt::reflection {                                             \
  inline constexpr auto get_private_ptrs(const identity<STRUCT> &t);      \
  template struct private_visitor<STRUCT, ##__VA_ARGS__>;                 \
  }                                                                       \
  template <typename Visitor>                                             \
  inline static constexpr decltype(auto) refl_visit_members(              \
      STRUCT &t, Visitor &&visitor) {                                     \
    return visit_private_fields(t, std::forward<Visitor>(visitor));       \
  }                                                                       \
  template <typename Visitor>                                             \
  inline static constexpr decltype(auto) refl_visit_members(              \
      const STRUCT &t, Visitor &&visitor) {                               \
    return visit_private_fields(t, std::forward<Visitor>(visitor));       \
  }                                                                       \
  [[maybe_unused]] inline static decltype(auto) refl_object_to_tuple(     \
      STRUCT &t) {                                                        \
    return refl_object_to_tuple_impl(t);                                  \
  }                                                                       \
  [[maybe_unused]] inline static decltype(auto) refl_object_to_tuple(     \
      const STRUCT &t) {                                                  \
    return refl_object_to_tuple_impl(t);                                  \
  }                                                                       \
  [[maybe_unused]] inline static constexpr std::size_t refl_member_count( \
      const ylt::reflection::identity<STRUCT> &t) {                       \
    return (std::size_t)YLT_ARG_COUNT(__VA_ARGS__);                       \
  }

#define YLT_REFL_PRIVATE(STRUCT, ...)                                        \
  [[maybe_unused]] inline static constexpr decltype(auto) refl_member_names( \
      const ylt::reflection::identity<STRUCT> &t) {                          \
    constexpr std::array<std::string_view, YLT_ARG_COUNT(__VA_ARGS__)> arr{  \
        WRAP_ARGS(CONCAT_NAME, t, ##__VA_ARGS__)};                           \
    return arr;                                                              \
  }                                                                          \
  YLT_REFL_PRIVATE_(STRUCT, WRAP_ARGS(CONCAT_ADDR, STRUCT, ##__VA_ARGS__))

template <typename T, typename = void>
struct is_out_ylt_refl : std::false_type {};

template <typename T>
struct is_out_ylt_refl<T, std::void_t<decltype(refl_member_count(
                              std::declval<ylt::reflection::identity<T>>()))>>
    : std::true_type {};

template <typename T>
constexpr bool is_out_ylt_refl_v = is_out_ylt_refl<T>::value;

template <typename T, typename = void>
struct is_inner_ylt_refl : std::false_type {};

template <typename T>
struct is_inner_ylt_refl<
    T, std::void_t<decltype(std::declval<T>().refl_member_count(
           std::declval<ylt::reflection::identity<T>>()))>> : std::true_type {};

template <typename T>
constexpr bool is_inner_ylt_refl_v = is_inner_ylt_refl<T>::value;

template <typename T, typename = void>
struct is_ylt_refl : std::false_type {};

template <typename T>
inline constexpr bool is_ylt_refl_v = is_ylt_refl<remove_cvref_t<T>>::value;

template <typename T>
struct is_ylt_refl<T, std::enable_if_t<is_inner_ylt_refl_v<T>>>
    : std::true_type {};

template <typename T>
struct is_ylt_refl<T, std::enable_if_t<is_out_ylt_refl_v<T>>> : std::true_type {
};

template <typename T, typename = void>
struct is_custom_reflect : std::false_type {};

template <typename T>
struct is_custom_reflect<
    T, std::void_t<decltype(ylt_custom_reflect((T *)nullptr))>>
    : std::true_type {};

template <typename T>
inline constexpr bool is_custom_refl_v =
    is_custom_reflect<remove_cvref_t<T>>::value;
}  // namespace ylt::reflection
