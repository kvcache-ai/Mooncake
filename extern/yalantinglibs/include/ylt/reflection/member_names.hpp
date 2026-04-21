#pragma once
#include <string_view>
#include <variant>

#include "member_ptr.hpp"
#include "template_string.hpp"

namespace ylt::reflection {

template <typename T>
inline constexpr auto get_alias_field_names();

namespace internal {

template <class T>
struct Wrapper {
  using Type = T;
  T v;
};

template <class T>
Wrapper(T) -> Wrapper<T>;

// This workaround is necessary for clang.
template <class T>
inline constexpr auto wrap(const T& arg) noexcept {
  return Wrapper{arg};
}

template <auto ptr>
inline constexpr std::string_view get_member_name() {
#if defined(_MSC_VER)
  constexpr std::string_view func_name = __FUNCSIG__;
#else
  constexpr std::string_view func_name = __PRETTY_FUNCTION__;
#endif

#if defined(__clang__)
  auto split = func_name.substr(0, func_name.size() - 2);
  return split.substr(split.find_last_of(":.") + 1);
#elif defined(__GNUC__)
  auto split = func_name.substr(0, func_name.rfind(")}"));
  return split.substr(split.find_last_of(":") + 1);
#elif defined(_MSC_VER)
  auto split = func_name.substr(0, func_name.rfind("}>"));
  return split.substr(split.rfind("->") + 2);
#else
  static_assert(false,
                "You are using an unsupported compiler. Please use GCC, Clang "
                "or MSVC or switch to the rfl::Field-syntax.");
#endif
}

template <class T>
struct member_tratis {};

template <class T, class Owner>
struct member_tratis<T Owner::*> {
  using owner_type = Owner;
  using value_type = T;
};

template <typename T, typename = void>
struct has_alias_struct_name_t : std::false_type {};

template <typename T>
struct has_alias_struct_name_t<
    T, std::void_t<decltype(get_alias_struct_name((T*)nullptr))>>
    : std::true_type {};

template <typename T, typename = void>
struct has_inner_alias_struct_name_t : std::false_type {};

template <typename T>
struct has_inner_alias_struct_name_t<
    T, std::void_t<decltype(T::get_alias_struct_name((T*)nullptr))>>
    : std::true_type {};

template <typename T>
constexpr bool has_alias_struct_name_v = has_alias_struct_name_t<T>::value;

template <typename T>
constexpr bool has_inner_alias_struct_name_v =
    has_inner_alias_struct_name_t<T>::value;

template <typename T, typename = void>
struct has_alias_field_names_t : std::false_type {};

template <typename T>
struct has_alias_field_names_t<
    T, std::void_t<decltype(get_alias_field_names((T*)nullptr))>>
    : std::true_type {};

template <typename T, typename = void>
struct has_inner_alias_field_names_t : std::false_type {};

template <typename T>
struct has_inner_alias_field_names_t<
    T, std::void_t<decltype(T::get_alias_field_names((T*)nullptr))>>
    : std::true_type {};

template <typename T>
constexpr bool has_alias_field_names_v = has_alias_field_names_t<T>::value;

template <typename T>
constexpr bool has_inner_alias_field_names_v =
    has_inner_alias_field_names_t<T>::value;

template <typename T, typename U, size_t... Is>
inline constexpr void init_arr_with_tuple(U& arr, std::index_sequence<Is...>) {
  constexpr auto tp = struct_to_tuple<T>();
  ((arr[Is] = internal::get_member_name<internal::wrap(std::get<Is>(tp))>()),
   ...);
}

template <typename T>
inline constexpr std::array<std::string_view, members_count_v<T>>
get_member_names() {
  constexpr size_t Count = members_count_v<T>;
  using type = remove_cvref_t<T>;
  if constexpr (is_out_ylt_refl_v<type>) {
    return refl_member_names(ylt::reflection::identity<type>{});
  }
  else if constexpr (is_inner_ylt_refl_v<type>) {
    return type::refl_member_names(ylt::reflection::identity<type>{});
  }
  else {
    std::array<std::string_view, Count> arr;
#if __cplusplus >= 202002L && (!defined(_MSC_VER) || _MSC_VER >= 1930)
    constexpr auto tp = struct_to_tuple<T>();
    [&]<size_t... Is>(std::index_sequence<Is...>) mutable {
      ((arr[Is] =
            internal::get_member_name<internal::wrap(std::get<Is>(tp))>()),
       ...);
    }
    (std::make_index_sequence<Count>{});
#else
    init_arr_with_tuple<T>(arr, std::make_index_sequence<Count>{});
#endif
    return arr;
  }
}

template <typename T, size_t... Is>
inline constexpr auto get_member_names_map_impl(T& name_arr,
                                                std::index_sequence<Is...>) {
  return frozen::unordered_map<frozen::string, size_t, sizeof...(Is)>{
      {name_arr[Is], Is}...};
}

template <typename T>
inline constexpr auto get_member_names_map() {
  constexpr auto name_arr = get_member_names<T>();
#if __cplusplus >= 202002L
  return [&]<size_t... Is>(std::index_sequence<Is...>) mutable {
    return frozen::unordered_map<frozen::string, size_t, name_arr.size()>{
        {name_arr[Is], Is}...};
  }
  (std::make_index_sequence<name_arr.size()>{});
#else
  return get_member_names_map_impl(name_arr,
                                   std::make_index_sequence<name_arr.size()>{});
#endif
}

template <typename T, typename Tuple, size_t... Is>
inline auto get_member_offset_arr_impl(T& t, Tuple& tp,
                                       std::index_sequence<Is...>) {
  std::array<size_t, sizeof...(Is)> arr;
  ((arr[Is] = size_t((const char*)&std::get<Is>(tp) - (char*)(&t))), ...);
  return arr;
}

template <typename T>
inline const auto& get_member_offset_arr(T&& t) {
  constexpr size_t Count = members_count_v<T>;
  auto tp = ylt::reflection::object_to_tuple(std::forward<T>(t));

#if __cplusplus >= 202002L
  [[maybe_unused]] static std::array<size_t, Count> arr = {[&]<size_t... Is>(
      std::index_sequence<Is...>) mutable {std::array<size_t, Count> arr;
  ((arr[Is] = size_t((const char*)&std::get<Is>(tp) - (char*)(&t))), ...);
  return arr;
}
(std::make_index_sequence<Count>{})
};  // namespace internal

return arr;
#else
  [[maybe_unused]] static std::array<size_t, Count> arr =
      get_member_offset_arr_impl(t, tp, std::make_index_sequence<Count>{});
  return arr;
#endif
}  // namespace ylt::reflection

template <typename T>
inline const auto& get_member_offset_arr() {
  return get_member_offset_arr(internal::wrapper<T>::value);
}  // namespace ylt::reflection
}  // namespace internal

template <typename... Args>
inline constexpr auto tuple_to_variant(std::tuple<Args...>) {
  return std::variant<std::add_pointer_t<Args>...>{};
}

template <typename T>
using struct_variant_t = decltype(tuple_to_variant(
    std::declval<decltype(struct_to_tuple<remove_cvref_t<T>>())>));

template <typename T>
constexpr auto member_names_map = internal::get_member_names_map<T>();

template <typename T>
inline auto member_offsets = internal::get_member_offset_arr<T>();

template <auto member>
inline constexpr size_t index_of() {
  using T = typename internal::member_tratis<
      std::remove_const_t<decltype(member)>>::owner_type;
  constexpr auto name = field_string<member>();
  constexpr auto names = internal::get_member_names<T>();
  for (size_t i = 0; i < names.size(); i++) {
    if (name == names[i]) {
      return i;
    }
  }
  return names.size();
}

struct field_alias_t {
  std::string_view alias_name;
  size_t index;
};

template <typename T>
inline constexpr auto get_alias_field_names() {
  if constexpr (internal::has_alias_field_names_v<T>) {
    return get_alias_field_names((T*)nullptr);
  }
  else if constexpr (internal::has_inner_alias_field_names_v<T>) {
    return T::get_alias_field_names((T*)nullptr);
  }
  else {
    return std::array<std::string_view, 0>{};
  }
}

template <typename T>
constexpr std::string_view get_struct_name() {
  if constexpr (internal::has_alias_struct_name_v<T>) {
    return get_alias_struct_name((T*)nullptr);
  }
  else if constexpr (internal::has_inner_alias_struct_name_v<T>) {
    return T::get_alias_struct_name((T*)nullptr);
  }
  else {
    return type_string<T>();
  }
}

template <typename T>
inline constexpr std::array<std::string_view, members_count_v<T>>
get_member_names() {
  auto arr = internal::get_member_names<T>();
  using U = ylt::reflection::remove_cvref_t<T>;
  if constexpr (internal::has_alias_field_names_v<U> ||
                internal::has_inner_alias_field_names_v<U>) {
    constexpr auto alias_arr = get_alias_field_names<U>();
    for (size_t i = 0; i < alias_arr.size(); i++) {
      arr[alias_arr[i].index] = alias_arr[i].alias_name;
    }
  }
  return arr;
}

template <std::size_t N>
struct FixedString {
  char data[N];
  template <std::size_t... I>
  constexpr FixedString(const char (&s)[N], std::index_sequence<I...>)
      : data{s[I]...} {}
  constexpr FixedString(const char (&s)[N])
      : FixedString(s, std::make_index_sequence<N>()) {}
  constexpr std::string_view str() const {
    return std::string_view{data, N - 1};
  }
};

template <typename T>
inline constexpr size_t index_of(std::string_view name) {
  constexpr auto arr = get_member_names<T>();
  for (size_t i = 0; i < arr.size(); i++) {
    if (arr[i] == name) {
      return i;
    }
  }

  return arr.size();
}

#if __cplusplus >= 202002L
template <typename T, FixedString name>
inline constexpr size_t index_of() {
  return index_of<T>(name.str());
}
#endif

template <typename T, size_t index>
inline constexpr std::string_view name_of() {
  static_assert(index < members_count_v<T>, "index out of range");
  constexpr auto arr = get_member_names<T>();
  return arr[index];
}

template <typename T>
inline constexpr std::string_view name_of(size_t index) {
  constexpr auto arr = get_member_names<T>();
  if (index >= arr.size()) {
    return "";
  }

  return arr[index];
}

template <typename Visit, typename U, size_t... Is>
inline constexpr void for_each_impl(Visit&& func, U& arr,
                                    std::index_sequence<Is...>) {
  if constexpr (std::is_invocable_v<Visit, std::string_view, size_t>) {
    (func(arr[Is], Is), ...);
  }
  else if constexpr (std::is_invocable_v<Visit, std::string_view>) {
    (func(arr[Is]), ...);
  }
  else {
    static_assert(sizeof(Visit) < 0,
                  "invalid arguments, full arguments: [std::string_view, "
                  "size_t], at least has std::string_view and make sure keep "
                  "the order of arguments");
  }
}

template <typename T, typename Visit>
inline constexpr void for_each(Visit&& func) {
  constexpr auto arr = get_member_names<T>();
#if __cplusplus >= 202002L
  [&]<size_t... Is>(std::index_sequence<Is...>) mutable {
    if constexpr (std::is_invocable_v<Visit, std::string_view, size_t>) {
      (func(arr[Is], Is), ...);
    }
    else if constexpr (std::is_invocable_v<Visit, std::string_view>) {
      (func(arr[Is]), ...);
    }
    else {
      static_assert(sizeof(Visit) < 0,
                    "invalid arguments, full arguments: [std::string_view, "
                    "size_t], at least has std::string_view and make sure keep "
                    "the order of arguments");
    }
  }
  (std::make_index_sequence<arr.size()>{});
#else
  for_each_impl(std::forward<Visit>(func), arr,
                std::make_index_sequence<arr.size()>{});
#endif
}

}  // namespace ylt::reflection
