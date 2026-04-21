#pragma once
#include "member_count.hpp"

// modified based on:
// https://github.com/getml/reflect-cpp/blob/main/include/rfl/internal/bind_fake_object_to_tuple.hpp
// thanks for alxn4's great idea!
namespace ylt::reflection {
namespace internal {

template <class T, std::size_t n>
struct object_tuple_view_helper {
  static constexpr auto tuple_view() {
    static_assert(
        sizeof(T) < 0,
        "\n\nThis error occurs for one of two reasons:\n\n"
        "1) You have created a struct with more than 256 fields, which is "
        "unsupported. \n\n"
        "2) Your struct is not an aggregate type. You can make it aggregated, "
        "or defined a YLT_REFL macro. \n\n");
  }

  static constexpr auto tuple_view(T&) {
    static_assert(
        sizeof(T) < 0,
        "\n\nThis error occurs for one of two reasons:\n\n"
        "1) You have created a struct with more than 256 fields, which is "
        "unsupported. \n\n"
        "2) Your struct is not an aggregate type. You can make it aggregated, "
        "or defined a YLT_REFL macro. \n\n");
  }

  template <typename Visitor>
  static constexpr decltype(auto) tuple_view(T&&, Visitor&&) {
    static_assert(
        sizeof(T) < 0,
        "\n\nThis error occurs for one of two reasons:\n\n"
        "1) You have created a struct with more than 256 fields, which is "
        "unsupported. \n\n"
        "2) Your struct is not an aggregate type. You can make it aggregated, "
        "or defined a YLT_REFL macro. \n\n");
  }
};

template <class T>
struct object_tuple_view_helper<T, 0> {
  static constexpr auto tuple_view() { return std::tie(); }
  static constexpr auto tuple_view(T&) { return std::tie(); }

  template <typename Visitor>
  static constexpr decltype(auto) tuple_view(T&&, Visitor&&) {}
};

template <class T>
struct wrapper {
  inline static remove_cvref_t<T> value;
};

template <class T>
inline constexpr remove_cvref_t<T>& get_fake_object() noexcept {
  return wrapper<remove_cvref_t<T>>::value;
}

#define RFL_INTERNAL_OBJECT_IF_YOU_SEE_AN_ERROR_REFER_TO_DOCUMENTATION_ON_C_ARRAYS( \
    n, ...)                                                                         \
  template <class T>                                                                \
  struct object_tuple_view_helper<T, n> {                                           \
    static constexpr auto tuple_view() {                                            \
      auto& [__VA_ARGS__] = get_fake_object<remove_cvref_t<T>>();                   \
      auto ref_tup = std::tie(__VA_ARGS__);                                         \
      auto get_ptrs = [](auto&... _refs) {                                          \
        return std::make_tuple(&_refs...);                                          \
      };                                                                            \
      return std::apply(get_ptrs, ref_tup);                                         \
    }                                                                               \
    static constexpr auto tuple_view(T& t) {                                        \
      auto& [__VA_ARGS__] = t;                                                      \
      return std::tie(__VA_ARGS__);                                                 \
    }                                                                               \
    template <typename Visitor>                                                     \
    static constexpr decltype(auto) tuple_view(T&& t, Visitor&& visitor) {          \
      auto&& [__VA_ARGS__] = t;                                                     \
      return visitor(__VA_ARGS__);                                                  \
    }                                                                               \
  }

// such file is generate macro file
#include "internal/generate/member_macro.hpp"

template <class T>
inline constexpr auto tuple_view(T&& t) {
  return internal::object_tuple_view_helper<T, members_count_v<T>>::tuple_view(
      t);
}

template <size_t Count, class T, typename Visitor>
inline constexpr decltype(auto) tuple_view(T&& t, Visitor&& visitor) {
  return internal::object_tuple_view_helper<T, Count>::tuple_view(
      std::forward<T>(t), std::forward<Visitor>(visitor));
}
}  // namespace internal

template <class T>
inline constexpr auto struct_to_tuple() {
  return internal::object_tuple_view_helper<T,
                                            members_count_v<T>>::tuple_view();
}

template <class T>
inline constexpr auto object_to_tuple(T&& t) {
  using type = remove_cvref_t<T>;
  if constexpr (is_out_ylt_refl_v<type>) {
    return refl_object_to_tuple(std::forward<T>(t));
  }
  else if constexpr (is_inner_ylt_refl_v<type>) {
    return type::refl_object_to_tuple(std::forward<T>(t));
  }
  else {
    return internal::tuple_view(std::forward<T>(t));
  }
}

template <class T, typename Visitor, size_t Count = members_count_v<T>>
inline constexpr decltype(auto) visit_members(T&& t, Visitor&& visitor) {
  using type = remove_cvref_t<T>;
  if constexpr (is_out_ylt_refl_v<type>) {
    return refl_visit_members(std::forward<T>(t),
                              std::forward<Visitor>(visitor));
  }
  else if constexpr (is_inner_ylt_refl_v<type>) {
    return t.refl_visit_members(std::forward<T>(t),
                                std::forward<Visitor>(visitor));
  }
  else {
    return internal::tuple_view<Count>(std::forward<T>(t),
                                       std::forward<Visitor>(visitor));
  }
}
}  // namespace ylt::reflection
