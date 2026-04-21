#pragma once
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <variant>

#include "member_names.hpp"
#include "template_string.hpp"
#include "template_switch.hpp"

namespace ylt::reflection {

namespace internal {
template <typename Member, typename T>
inline void set_member_ptr(Member& member, T t) {
  if constexpr (std::is_constructible_v<Member, T>) {
    member = t;
  }
  else {
    std::string str = "given type: ";
    str.append(type_string<std::remove_pointer_t<Member>>());
    str.append(" is not equal with real type: ")
        .append(type_string<std::remove_pointer_t<T>>());
    throw std::invalid_argument(str);
  }
}

template <typename T>
struct is_variant : std::false_type {};

template <typename... T>
struct is_variant<std::variant<T...>> : std::true_type {};

struct switch_helper {
  template <size_t index, typename Member, class Tuple>
  static constexpr size_t run(Member& member, Tuple& t) {
    if constexpr (index >= std::tuple_size_v<Tuple>) {
      return index;
    }
    else {
      if constexpr (is_variant<Member>::value) {
        member = Member{std::in_place_index<index>, &std::get<index>(t)};
      }
      else {
        set_member_ptr(member, &std::get<index>(t));
      }
      return index;
    }
  }
};

inline constexpr frozen::string filter_str(const frozen::string& str) {
  if (str.size() > 3 && str[0] == '_' && str[1] == '_' && str[2] == '_') {
    auto ptr = str.data() + 3;
    return frozen::string(ptr, str.size() - 3);
  }
  return str;
}

template <typename value_type>
struct offset_t {
  using type = value_type;
  size_t value;
};

template <typename T, typename Tuple, size_t... Is>
inline auto get_variant_type() {
  return std::variant<offset_t<
      ylt::reflection::remove_cvref_t<std::tuple_element_t<Is, Tuple>>>...>{};
}

template <typename T, size_t... Is>
inline constexpr auto get_variant_map_impl(std::index_sequence<Is...>) {
  using U = ylt::reflection::remove_cvref_t<T>;
  constexpr auto arr = ylt::reflection::get_member_names<U>();
  auto& offset_arr = get_member_offset_arr(wrapper<U>::value);
  using Tuple = decltype(ylt::reflection::object_to_tuple(std::declval<U>()));
  using ValueType = decltype(get_variant_type<U, Tuple, Is...>());
  return frozen::unordered_map<frozen::string, ValueType, sizeof...(Is)>{
      {filter_str(arr[Is]),
       ValueType{std::in_place_index<Is>,
                 offset_t<ylt::reflection::remove_cvref_t<
                     std::tuple_element_t<Is, Tuple>>>{offset_arr[Is]}}}...};
}

}  // namespace internal

template <typename T>
inline constexpr auto get_variant_map() {
  return internal::get_variant_map_impl<T>(
      std::make_index_sequence<members_count_v<T>>{});
}

template <typename Member, typename T>
inline Member& get(T& t, size_t index) {
  auto ref_tp = object_to_tuple(t);
  constexpr size_t tuple_size = std::tuple_size_v<decltype(ref_tp)>;

  if (index >= tuple_size) {
    std::string str = "index out of range, ";
    str.append("index: ")
        .append(std::to_string(index))
        .append(" is greater equal than member count ")
        .append(std::to_string(tuple_size));
    throw std::out_of_range(str);
  }
  Member* member_ptr = nullptr;
  template_switch<internal::switch_helper>(index, member_ptr, ref_tp);
  return *member_ptr;
}

template <typename Member, typename T>
inline Member& get(T& t, std::string_view name) {
  static constexpr auto map = member_names_map<T>;
  size_t index = map.at(name);  // may throw out_of_range: unknown key.
  auto ref_tp = object_to_tuple(t);

  Member* member_ptr = nullptr;
  template_switch<internal::switch_helper>(index, member_ptr, ref_tp);
  return *member_ptr;
}

template <typename T>
inline auto get(T& t, size_t index) {
  auto ref_tp = object_to_tuple(t);
  constexpr size_t tuple_size = std::tuple_size_v<decltype(ref_tp)>;
  if (index >= tuple_size) {
    std::string str = "index out of range, ";
    str.append("index: ")
        .append(std::to_string(index))
        .append(" is greater equal than member count ")
        .append(std::to_string(tuple_size));
    throw std::out_of_range(str);
  }

  using variant = decltype(tuple_to_variant(ref_tp));
  variant member_ptr;
  template_switch<internal::switch_helper>(index, member_ptr, ref_tp);
  return member_ptr;
}

template <typename T>
inline constexpr auto get(T& t, std::string_view name) {
  constexpr auto& map = member_names_map<T>;
  size_t index = map.at(name);  // may throw out_of_range: unknown key.
  return get(t, index);
}

template <size_t index, typename T>
inline constexpr auto& get(T& t) {
  auto ref_tp = object_to_tuple(t);

  static_assert(index < std::tuple_size_v<decltype(ref_tp)>,
                "index out of range");

  return std::get<index>(ref_tp);
}

#if __cplusplus >= 202002L
template <FixedString name, typename T>
inline constexpr auto& get(T& t) {
  constexpr size_t index = index_of<T, name>();
  return get<index>(t);
}
#endif

template <typename T, typename Field>
inline size_t index_of(T& t, Field& value) {
  const auto& offset_arr = member_offsets<T>;
  size_t cur_offset = (const char*)(&value) - (const char*)(&t);
  auto it = std::lower_bound(offset_arr.begin(), offset_arr.end(), cur_offset);
  if (it == offset_arr.end()) {
    return offset_arr.size();
  }

  return std::distance(offset_arr.begin(), it);
}

template <typename Member>
inline size_t index_of(Member member) {
  using T = typename member_traits<Member>::owner_type;
  static auto& t = internal::get_fake_object<T>();
  return index_of(t, t.*member);
}

template <typename T, typename Field>
inline constexpr std::string_view name_of(T& t, Field& value) {
  size_t index = index_of(t, value);
  constexpr auto arr = get_member_names<T>();
  if (index == arr.size()) {
    return "";
  }

  return arr[index];
}

template <typename T, typename Visit, size_t... Is, typename... Args>
inline constexpr void visit_members_impl0(Visit&& func,
                                          std::index_sequence<Is...>,
                                          Args&... args) {
  constexpr auto arr = get_member_names<T>();
  (func(args, arr[Is]), ...);
}

template <typename T, typename Visit, size_t... Is, typename... Args>
inline constexpr void visit_members_impl(Visit&& func,
                                         std::index_sequence<Is...>,
                                         Args&... args) {
  constexpr auto arr = get_member_names<T>();
  (func(args, arr[Is], Is), ...);
}

template <typename T, typename Visit>
inline constexpr void for_each(T&& t, Visit&& func) {
  using Tuple = decltype(object_to_tuple(t));
  using first_t = std::tuple_element_t<0, Tuple>;
  if constexpr (std::is_invocable_v<Visit, first_t>) {
    visit_members(t, [&func](auto&... args) {
      (func(args), ...);
    });
  }
  else {
    if constexpr (std::is_invocable_v<Visit, first_t, std::string_view>) {
      visit_members(t, [&](auto&... args) {
#if __cplusplus >= 202002L && (!defined(_MSC_VER) || _MSC_VER >= 1930)
        [&]<size_t... Is>(std::index_sequence<Is...>) mutable {
          constexpr auto arr = get_member_names<T>();
          (func(args, arr[Is]), ...);
        }
        (std::make_index_sequence<sizeof...(args)>{});
#else
            visit_members_impl0<T>(std::forward<Visit>(func),
                                   std::make_index_sequence<sizeof...(args)>{},
                                   args...);
#endif
      });
    }
    else if constexpr (std::is_invocable_v<Visit, first_t, std::string_view,
                                           size_t>) {
      visit_members(t, [&](auto&... args) {
#if __cplusplus >= 202002L && (!defined(_MSC_VER) || _MSC_VER >= 1930)
        [&]<size_t... Is>(std::index_sequence<Is...>) mutable {
          constexpr auto arr = get_member_names<T>();
          (func(args, arr[Is], Is), ...);
        }
        (std::make_index_sequence<sizeof...(args)>{});
#else
            visit_members_impl<T>(std::forward<Visit>(func),
                                  std::make_index_sequence<sizeof...(args)>{},
                                  args...);
#endif
      });
    }
    else {
      static_assert(sizeof(Visit) < 0,
                    "invalid arguments, full arguments: [field_value&, "
                    "std::string_view, size_t], at least has field_value and "
                    "make sure keep the order of arguments");
    }
  }
}

}  // namespace ylt::reflection

#if (defined(__GNUC__) && __GNUC__ > 10) || \
    ((defined(__clang__) || defined(_MSC_VER)) && __has_include(<concepts>))
#if __cplusplus >= 202002L
template <ylt::reflection::FixedString s>
inline constexpr auto operator""_ylts() {
  return s;
}
#endif
#endif
