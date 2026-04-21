/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "alignment.hpp"
#include "marco.h"
#include "md5_constexpr.hpp"
#include "reflection.hpp"
#include "type_id.hpp"
namespace struct_pack {
template <typename... Args>
STRUCT_PACK_INLINE constexpr decltype(auto) get_type_literal();
namespace detail {
template <size_t size>
constexpr decltype(auto) get_size_literal() {
  static_assert(sizeof(size_t) <= 8);
  if constexpr (size < 1ull * 127) {
    return string_literal<char, 1>{{static_cast<char>(size + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127) {
    return string_literal<char, 2>{{static_cast<char>(size % 127 + 1),
                                    static_cast<char>(size / 127 + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127 * 127) {
    return string_literal<char, 3>{
        {static_cast<char>(size % 127 + 1),
         static_cast<char>(size / 127 % 127 + 1),
         static_cast<char>(size / (127ull * 127) + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127 * 127 * 127) {
    return string_literal<char, 4>{
        {static_cast<char>(size % 127 + 1),
         static_cast<char>(size / 127 % 127 + 1),
         static_cast<char>(size / (127ull * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127) + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127 * 127 * 127 * 127) {
    return string_literal<char, 5>{
        {static_cast<char>(size % 127 + 1),
         static_cast<char>(size / 127 % 127 + 1),
         static_cast<char>(size / (127ull * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127) + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127 * 127 * 127 * 127 * 127) {
    return string_literal<char, 6>{
        {static_cast<char>(size % 127 + 1),
         static_cast<char>(size / 127 % 127 + 1),
         static_cast<char>(size / (127ull * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127 * 127) + 129)}};
  }
  else if constexpr (size < 1ull * 127 * 127 * 127 * 127 * 127 * 127 * 127) {
    return string_literal<char, 7>{
        {static_cast<char>(size % 127 + 1),
         static_cast<char>(size / 127 % 127 + 1),
         static_cast<char>(size / (127ull * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127 * 127) % 127 + 1),
         static_cast<char>(size / (127ull * 127 * 127 * 127 * 127 * 127) +
                           129)}};
  }
  else if constexpr (size <
                     1ull * 127 * 127 * 127 * 127 * 127 * 127 * 127 * 127) {
    return string_literal<char, 8>{{
        static_cast<char>(size % 127 + 1),
        static_cast<char>(size / 127 % 127 + 1),
        static_cast<char>(size / (127ull * 127) % 127 + 1),
        static_cast<char>(size / (127ull * 127 * 127) % 127 + 1),
        static_cast<char>(size / (127ull * 127 * 127 * 127) % 127 + 1),
        static_cast<char>(size / (127ull * 127 * 127 * 127 * 127) % 127 + 1),
        static_cast<char>(size / (127ull * 127 * 127 * 127 * 127 * 127) % 127 +
                          1),
        static_cast<char>(size / (127ull * 127 * 127 * 127 * 127 * 127 * 127) +
                          129),
    }};
  }
  else {
    static_assert(
        size >= 1ull * 127 * 127 * 127 * 127 * 127 * 127 * 127 * 127 * 127,
        "The size is too large.");
  }
}

template <typename Arg, typename... ParentArgs, std::size_t... I>
constexpr std::size_t check_circle_impl(std::index_sequence<I...>) {
  using types_tuple = std::tuple<ParentArgs...>;
  return (std::max)(
      {(std::is_same_v<std::tuple_element_t<I, types_tuple>, Arg> ? I + 1
                                                                  : 0)...});
}

template <typename Arg, typename... ParentArgs>
constexpr std::size_t check_circle() {
  if constexpr (sizeof...(ParentArgs) != 0) {
    return check_circle_impl<Arg, ParentArgs...>(
        std::make_index_sequence<sizeof...(ParentArgs)>());
  }
  else {
    return 0;
  }
}

template <typename T>
struct get_array_element {
  using type = typename T::value_type;
};

template <typename T, std::size_t sz>
struct get_array_element<T[sz]> {
  using type = T;
};

template <typename T>
std::size_t constexpr get_array_size() {
  if constexpr (array<T> || c_array<T>) {
    return sizeof(T) / sizeof(typename get_array_element<T>::type);
  }
  else if constexpr (bitset<T>) {
    return T{}.size();
  }
  else {
    return T::extent;
  }
}

template <typename Arg>
constexpr decltype(auto) get_type_end_flag() {
  if constexpr (has_user_defined_id<Arg>) {
    return string_literal<char, 1>{
               {static_cast<char>(type_id::type_end_flag_with_id)}} +
           get_size_literal<Arg::struct_pack_id>();
  }
  else if constexpr (has_user_defined_id_ADL<Arg>) {
    return string_literal<char, 1>{
               {static_cast<char>(type_id::type_end_flag_with_id)}} +
           get_size_literal<struct_pack_id((Arg *)nullptr)>();
  }
  else {
    return string_literal<char, 1>{{static_cast<char>(type_id::type_end_flag)}};
  }
}

template <typename T>
constexpr sp_config get_type_config() {
  if constexpr (struct_pack::detail::user_defined_config_by_ADL<T>) {
    return set_sp_config((T *)nullptr);
  }
  else if constexpr (struct_pack::detail::user_defined_config<T>) {
    return static_cast<sp_config>(T::struct_pack_config);
  }
  else if constexpr (struct_pack::detail::has_default_config<T>) {
    return set_default(decltype(delay_sp_config_eval<T>()){});
  }
  else {
    return sp_config::DEFAULT;
  }
}
template <typename T, typename... Args>
constexpr uint64_t get_parent_tag_impl() {
  return static_cast<uint64_t>(get_type_config<T>());
}

template <typename... Args>
constexpr uint64_t get_parent_tag() {
  if constexpr (sizeof...(Args) == 0) {
    return 0;
  }
  else {
    return get_parent_tag_impl<Args...>();
  }
}

template <typename Args, typename... ParentArgs, std::size_t... I>
constexpr decltype(auto) get_type_literal(std::index_sequence<I...>);

template <typename Args, typename... ParentArgs, std::size_t... I>
constexpr decltype(auto) get_variant_literal(std::index_sequence<I...>);

template <typename Arg, typename... ParentArgs>
constexpr decltype(auto) get_type_literal() {
  if constexpr (is_trivial_view_v<Arg>) {
    return get_type_literal<typename Arg::value_type, ParentArgs...>();
  }
  else if constexpr (user_defined_serialization<Arg>) {
    constexpr auto begin = string_literal<char, 1>{
        {static_cast<char>(type_id::user_defined_type)}};
    constexpr auto end =
        string_literal<char, 1>{{static_cast<char>(type_id::type_end_flag)}};
    if constexpr (user_defined_type_name<Arg>) {
      constexpr auto type_name = sp_set_type_name((Arg *)nullptr);
      string_literal<char, type_name.size()> ret{type_name};
      return begin + ret + end;
    }
    else {
      constexpr auto type_name = type_string<Arg>();
      string_literal<char, type_name.size()> ret{type_name};
      return begin + ret + end;
    }
  }
  else {
    constexpr std::size_t has_cycle = check_circle<Arg, ParentArgs...>();
    if constexpr (has_cycle != 0) {
      static_assert(has_cycle >= 2);
      return string_literal<char, 1>{
                 {static_cast<char>(type_id::circle_flag)}} +
             get_size_literal<has_cycle - 2>();
    }
    else {
      constexpr auto parent_tag = get_parent_tag<ParentArgs...>();
      constexpr auto id = get_type_id<Arg, parent_tag>();
      constexpr auto begin = string_literal<char, 1>{{static_cast<char>(id)}};
      if constexpr (id == type_id::struct_t) {
        using Args = decltype(get_types<Arg>());
        constexpr auto end = get_type_end_flag<Arg>();
        constexpr auto body = get_type_literal<Args, Arg, ParentArgs...>(
            std::make_index_sequence<std::tuple_size_v<Args>>());
        if constexpr (is_trivial_serializable<Arg, true>::value) {
          static_assert(
              align::pack_alignment_v<Arg> <= align::alignment_v<Arg>,
              "If you add #pragma_pack to a struct, please specify the "
              "struct_pack::pack_alignment_v<T>.");
          return begin + body +
                 get_size_literal<align::pack_alignment_v<Arg>>() +
                 get_size_literal<align::alignment_v<Arg>>() + end;
        }
        else {
          return begin + body + end;
        }
      }
      else if constexpr (id == type_id::variant_t) {
        constexpr auto sz = std::variant_size_v<Arg>;
        static_assert(sz > 0, "empty param of std::variant is not allowed!");
        static_assert(sz < 256, "too many alternative type in variant!");
        constexpr auto body = get_variant_literal<Arg, ParentArgs...>(
            std::make_index_sequence<std::variant_size_v<Arg>>());
        constexpr auto end = string_literal<char, 1>{
            {static_cast<char>(type_id::type_end_flag)}};
        return begin + body + end;
      }
      else if constexpr (id == type_id::array_t) {
        constexpr auto sz = get_array_size<Arg>();
        static_assert(sz > 0, "The array's size must greater than zero!");
        return begin +
               get_type_literal<
                   remove_cvref_t<decltype(std::declval<Arg>()[0])>, Arg,
                   ParentArgs...>() +
               get_size_literal<sz>();
      }
      else if constexpr (id == type_id::bitset_t) {
        constexpr auto sz = get_array_size<Arg>();
        static_assert(sz > 0, "The array's size must greater than zero!");
        return begin + get_size_literal<sz>();
      }
      else if constexpr (unique_ptr<Arg>) {
        return begin +
               get_type_literal<remove_cvref_t<typename Arg::element_type>, Arg,
                                ParentArgs...>();
      }
      else if constexpr (id == type_id::container_t ||
                         id == type_id::optional_t || id == type_id::string_t) {
        return begin +
               get_type_literal<remove_cvref_t<typename Arg::value_type>, Arg,
                                ParentArgs...>();
      }
      else if constexpr (id == type_id::set_container_t) {
        return begin + get_type_literal<remove_cvref_t<typename Arg::key_type>,
                                        Arg, ParentArgs...>();
      }
      else if constexpr (id == type_id::map_container_t) {
        return begin +
               get_type_literal<remove_cvref_t<typename Arg::key_type>, Arg,
                                ParentArgs...>() +
               get_type_literal<remove_cvref_t<typename Arg::mapped_type>, Arg,
                                ParentArgs...>();
      }
      else if constexpr (id == type_id::expected_t) {
        return begin +
               get_type_literal<remove_cvref_t<typename Arg::value_type>, Arg,
                                ParentArgs...>() +
               get_type_literal<remove_cvref_t<typename Arg::error_type>, Arg,
                                ParentArgs...>();
      }
      else if constexpr (id != type_id::compatible_t) {
        return begin;
      }
      else {
        return string_literal<char, 0>{};
      }
    }
  }
}

template <typename Args, typename... ParentArgs, std::size_t... I>
constexpr decltype(auto) get_type_literal(std::index_sequence<I...>) {
  return ((get_type_literal<remove_cvref_t<std::tuple_element_t<I, Args>>,
                            ParentArgs...>()) +
          ...);
}

template <typename Args, typename... ParentArgs, std::size_t... I>
constexpr decltype(auto) get_variant_literal(std::index_sequence<I...>) {
  return ((get_type_literal<remove_cvref_t<std::variant_alternative_t<I, Args>>,
                            Args, ParentArgs...>()) +
          ...);
}

template <typename Parent, typename... Args>
constexpr decltype(auto) get_types_literal_impl() {
  if constexpr (std::is_same_v<Parent, void>)
    return (get_type_literal<Args>() + ...);
  else
    return (get_type_literal<Args, Parent>() + ...);
}

template <typename T, typename... Args>
constexpr decltype(auto) get_types_literal() {
  if constexpr (is_trivial_view_v<T>) {
    return get_types_literal<T::value_type, Args...>();
  }
  else if constexpr (user_defined_serialization<T>) {
    constexpr auto begin = string_literal<char, 1>{
        {static_cast<char>(type_id::user_defined_type)}};
    constexpr auto end =
        string_literal<char, 1>{{static_cast<char>(type_id::type_end_flag)}};
    if constexpr (user_defined_type_name<T>) {
      constexpr auto type_name = sp_set_type_name((T *)nullptr);
      string_literal<char, type_name.size()> ret{type_name};
      return begin + ret + end;
    }
    else {
      constexpr auto type_name = type_string<T>();
      string_literal<char, type_name.size()> ret{type_name};
      return begin + ret + end;
    }
  }
  else {
    constexpr auto root_id = get_type_id<remove_cvref_t<T>>();
    if constexpr (root_id == type_id::struct_t) {
      constexpr auto end = get_type_end_flag<remove_cvref_t<T>>();
      constexpr auto begin =
          string_literal<char, 1>{{static_cast<char>(root_id)}};
      constexpr auto body = get_types_literal_impl<T, Args...>();
      if constexpr (is_trivial_serializable<T, true>::value) {
        static_assert(align::pack_alignment_v<T> <= align::alignment_v<T>,
                      "If you add #pragma_pack to a struct, please specify the "
                      "struct_pack::pack_alignment_v<T>.");
        return begin + body + get_size_literal<align::pack_alignment_v<T>>() +
               get_size_literal<align::alignment_v<T>>() + end;
      }
      else {
        return begin + body + end;
      }
    }
    else {
      return get_types_literal_impl<void, Args...>();
    }
  }
}

template <typename T, typename... Args>
constexpr decltype(auto) get_types_literal_without_outer_brace() {
  if constexpr (is_trivial_view_v<T> || user_defined_serialization<T>) {
    return get_types_literal<T, Args...>();
  }
  else {
    constexpr auto root_id = get_type_id<remove_cvref_t<T>>();
    if constexpr (root_id == type_id::struct_t) {
      constexpr auto not_compatible_cnt =
          (!struct_pack::detail::is_compatible_v<Args> + ...);
      if constexpr (not_compatible_cnt <= 1) {
        return get_types_literal_impl<T, Args...>();
      }
      else {
        return get_types_literal<T, Args...>();
      }
    }
    else {
      return get_types_literal_impl<void, Args...>();
    }
  }
}

template <typename T, typename Tuple, std::size_t... I>
constexpr decltype(auto) get_types_literal(std::index_sequence<I...>) {
  return get_types_literal<T,
                           remove_cvref_t<std::tuple_element_t<I, Tuple>>...>();
}
template <typename T, typename Tuple, std::size_t... I>
constexpr decltype(auto) get_types_literal_without_outer_brace(
    std::index_sequence<I...>) {
  return get_types_literal_without_outer_brace<
      T, remove_cvref_t<std::tuple_element_t<I, Tuple>>...>();
}

template <uint64_t version, typename Args, typename... ParentArgs>
constexpr bool check_if_compatible_element_exist_impl_helper();

template <uint64_t version, typename Args, typename... ParentArgs,
          std::size_t... I>
constexpr bool check_if_compatible_element_exist_impl(
    std::index_sequence<I...>) {
  return (check_if_compatible_element_exist_impl_helper<
              version, remove_cvref_t<std::tuple_element_t<I, Args>>,
              ParentArgs...>() ||
          ...);
}

template <uint64_t version, typename Arg, typename... ParentArgs,
          std::size_t... I>
constexpr bool check_if_compatible_element_exist_impl_variant(
    std::index_sequence<I...>) {
  return (check_if_compatible_element_exist_impl_helper<
              version, remove_cvref_t<std::variant_alternative_t<I, Arg>>,
              ParentArgs...>() ||
          ...);
}

template <uint64_t version, typename Arg, typename... ParentArgs>
constexpr bool check_if_compatible_element_exist_impl_helper() {
  using T = remove_cvref_t<Arg>;
  constexpr auto id = get_type_id<T>();
  if constexpr (is_trivial_view_v<Arg>) {
    return check_if_compatible_element_exist_impl_helper<
        version, typename Arg::value_type, ParentArgs...>();
  }
  else if constexpr (check_circle<Arg, ParentArgs...>() != 0) {
    return false;
  }
  else if constexpr (id == type_id::compatible_t) {
    if constexpr (version != UINT64_MAX)
      return T::version_number == version;
    else
      return true;
  }
  else {
    if constexpr (id == type_id::struct_t) {
      using subArgs = decltype(get_types<T>());
      return check_if_compatible_element_exist_impl<version, subArgs, T,
                                                    ParentArgs...>(
          std::make_index_sequence<std::tuple_size_v<subArgs>>());
    }
    else if constexpr (id == type_id::optional_t) {
      if constexpr (unique_ptr<T>) {
        if constexpr (is_base_class<typename T::element_type>) {
          return check_if_compatible_element_exist_impl<
              version, derived_class_set_t<typename T::element_type>, T,
              ParentArgs...>();
        }
        else {
          return check_if_compatible_element_exist_impl_helper<
              version, typename T::element_type, T, ParentArgs...>();
        }
      }
      else {
        return check_if_compatible_element_exist_impl_helper<
            version, typename T::value_type, T, ParentArgs...>();
      }
    }
    else if constexpr (id == type_id::array_t) {
      return check_if_compatible_element_exist_impl_helper<
          version, remove_cvref_t<typename get_array_element<T>::type>, T,
          ParentArgs...>();
    }
    else if constexpr (id == type_id::map_container_t) {
      return check_if_compatible_element_exist_impl_helper<
                 version, typename T::key_type, T, ParentArgs...>() ||
             check_if_compatible_element_exist_impl_helper<
                 version, typename T::mapped_type, T, ParentArgs...>();
    }
    else if constexpr (id == type_id::set_container_t ||
                       id == type_id::container_t) {
      return check_if_compatible_element_exist_impl_helper<
          version, typename T::value_type, T, ParentArgs...>();
    }
    else if constexpr (id == type_id::expected_t) {
      return check_if_compatible_element_exist_impl_helper<
                 version, typename T::value_type, T, ParentArgs...>() ||
             check_if_compatible_element_exist_impl_helper<
                 version, typename T::error_type, T, ParentArgs...>();
    }
    else if constexpr (id == type_id::variant_t) {
      return check_if_compatible_element_exist_impl_variant<version, T, T,
                                                            ParentArgs...>(
          std::make_index_sequence<std::variant_size_v<T>>{});
    }
    else {
      return false;
    }
  }
}

template <typename T, typename... Args>
constexpr uint32_t get_types_code_impl() {
#ifdef STRUCT_PACK_DISABLE_OUTER_BRACE
  constexpr auto str =
      get_types_literal_without_outer_brace<T, remove_cvref_t<Args>...>();
#else
  constexpr auto str = get_types_literal<T, remove_cvref_t<Args>...>();
#endif
  return MD5::MD5Hash32Constexpr(str.data(), str.size()) & 0xFFFFFFFE;
}

template <typename T, typename Tuple, size_t... I>
constexpr uint32_t get_types_code(std::index_sequence<I...>) {
  return get_types_code_impl<T, std::tuple_element_t<I, Tuple>...>();
}

template <typename T>
constexpr uint32_t get_types_code() {
  using tuple_t = decltype(get_types<T>());
  return detail::get_types_code<T, tuple_t>(
      std::make_index_sequence<std::tuple_size_v<tuple_t>>{});
}

template <typename Args, typename... ParentArgs>
constexpr std::size_t calculate_compatible_version_size();

template <typename Buffer, typename Args, typename... ParentArgs>
constexpr void get_compatible_version_numbers(Buffer &buffer, std::size_t &sz);

template <typename T>
constexpr auto STRUCT_PACK_INLINE get_sorted_compatible_version_numbers() {
  std::array<uint64_t, calculate_compatible_version_size<T>()> buffer{};
  std::size_t sz = 0;
  get_compatible_version_numbers<decltype(buffer), T>(buffer, sz);
  compile_time_sort(buffer);
  return buffer;
}

template <typename T>
constexpr auto STRUCT_PACK_INLINE
get_sorted_and_uniqued_compatible_version_numbers() {
  constexpr auto buffer = get_sorted_compatible_version_numbers<T>();
  std::array<uint64_t, calculate_uniqued_size(buffer)> uniqued_buffer{};
  compile_time_unique(buffer, uniqued_buffer);
  return uniqued_buffer;
}

template <typename T>
constexpr auto compatible_version_number =
    get_sorted_and_uniqued_compatible_version_numbers<T>();

template <typename T, uint64_t version = UINT64_MAX>
constexpr bool check_if_compatible_element_exist() {
  using U = remove_cvref_t<T>;
  return detail::check_if_compatible_element_exist_impl<version, U>(
      std::make_index_sequence<std::tuple_size_v<U>>{});
}

template <typename T, uint64_t version = UINT64_MAX>
constexpr bool exist_compatible_member =
    check_if_compatible_element_exist<decltype(get_types<T>()), version>();
// clang-format off
template <typename T, uint64_t version = UINT64_MAX>
constexpr bool unexist_compatible_member = !
exist_compatible_member<decltype(get_types<T>()), version>;
// clang-format on

template <typename Args, typename... ParentArgs, std::size_t... I>
constexpr std::size_t calculate_compatible_version_size(
    std::index_sequence<I...>) {
  return (calculate_compatible_version_size<
              remove_cvref_t<std::tuple_element_t<I, Args>>, ParentArgs...>() +
          ...);
}

template <typename Arg, typename... ParentArgs, std::size_t... I>
constexpr std::size_t calculate_variant_compatible_version_size(
    std::index_sequence<I...>) {
  return (
      calculate_compatible_version_size<
          remove_cvref_t<std::variant_alternative_t<I, Arg>>, ParentArgs...>() +
      ...);
}

template <typename Arg, typename... ParentArgs>
constexpr std::size_t calculate_compatible_version_size() {
  using T = remove_cvref_t<Arg>;
  constexpr auto id = get_type_id<T>();
  std::size_t sz = 0;
  if constexpr (is_trivial_view_v<T>) {
    return 0;
  }
  if constexpr (check_circle<T, ParentArgs...>())
    sz = 0;
  else if constexpr (id == type_id::compatible_t) {
    sz = 1;
  }
  else {
    if constexpr (id == type_id::struct_t) {
      using subArgs = decltype(get_types<T>());
      return calculate_compatible_version_size<subArgs, T, ParentArgs...>(
          std::make_index_sequence<std::tuple_size_v<subArgs>>());
    }
    else if constexpr (id == type_id::optional_t) {
      if constexpr (unique_ptr<T>) {
        if constexpr (is_base_class<typename T::element_type>) {
          sz = calculate_compatible_version_size<
              derived_class_set_t<typename T::element_type>, T,
              ParentArgs...>();
        }
        else {
          sz = calculate_compatible_version_size<typename T::element_type, T,
                                                 ParentArgs...>();
        }
      }
      else {
        sz = calculate_compatible_version_size<typename T::value_type, T,
                                               ParentArgs...>();
      }
    }
    else if constexpr (id == type_id::array_t) {
      return calculate_compatible_version_size<
          remove_cvref_t<typename get_array_element<T>::type>, T,
          ParentArgs...>();
    }
    else if constexpr (id == type_id::map_container_t) {
      return calculate_compatible_version_size<typename T::key_type, T,
                                               ParentArgs...>() +
             calculate_compatible_version_size<typename T::mapped_type, T,
                                               ParentArgs...>();
    }
    else if constexpr (id == type_id::set_container_t ||
                       id == type_id::container_t) {
      return calculate_compatible_version_size<typename T::value_type, T,
                                               ParentArgs...>();
    }
    else if constexpr (id == type_id::expected_t) {
      return calculate_compatible_version_size<typename T::value_type, T,
                                               ParentArgs...>() +
             calculate_compatible_version_size<typename T::error_type, T,
                                               ParentArgs...>();
    }
    else if constexpr (id == type_id::variant_t) {
      return calculate_variant_compatible_version_size<T, T, ParentArgs...>(
          std::make_index_sequence<std::variant_size_v<T>>{});
    }
  }
  return sz;
}

template <typename Buffer, typename Args, typename... ParentArgs,
          std::size_t... I>
constexpr void get_compatible_version_numbers(Buffer &buffer, std::size_t &sz,
                                              std::index_sequence<I...>) {
  return (
      get_compatible_version_numbers<
          Buffer, remove_cvref_t<std::tuple_element_t<I, Args>>, ParentArgs...>(
          buffer, sz),
      ...);
}

template <typename Buffer, typename Arg, typename... ParentArgs,
          std::size_t... I>
constexpr void get_variant_compatible_version_numbers(
    Buffer &buffer, std::size_t &sz, std::index_sequence<I...>) {
  return (get_compatible_version_numbers<
              Buffer, remove_cvref_t<std::variant_alternative_t<I, Arg>>,
              ParentArgs...>(buffer, sz),
          ...);
}

template <typename Buffer, typename Arg, typename... ParentArgs>
constexpr void get_compatible_version_numbers(Buffer &buffer, std::size_t &sz) {
  using T = remove_cvref_t<Arg>;
  constexpr auto id = get_type_id<T>();
  if constexpr (is_trivial_view_v<T>) {
    return;
  }
  else if constexpr (check_circle<T, ParentArgs...>()) {
    return;
  }
  else if constexpr (id == type_id::compatible_t) {
    buffer[sz++] = T::version_number;
    return;
  }
  else {
    if constexpr (id == type_id::struct_t) {
      using subArgs = decltype(get_types<T>());
      get_compatible_version_numbers<Buffer, subArgs, T, ParentArgs...>(
          buffer, sz, std::make_index_sequence<std::tuple_size_v<subArgs>>());
    }
    else if constexpr (id == type_id::optional_t) {
      if constexpr (unique_ptr<T>) {
        if constexpr (is_base_class<typename T::element_type>) {
          sz = get_compatible_version_numbers<
              Buffer, derived_class_set_t<typename T::element_type>, T,
              ParentArgs...>(buffer, sz);
        }
        else {
          get_compatible_version_numbers<Buffer, typename T::element_type, T,
                                         ParentArgs...>(buffer, sz);
        }
      }
      else {
        get_compatible_version_numbers<Buffer, typename T::value_type, T,
                                       ParentArgs...>(buffer, sz);
      }
    }
    else if constexpr (id == type_id::array_t) {
      get_compatible_version_numbers<
          Buffer, remove_cvref_t<typename get_array_element<T>::type>, T,
          ParentArgs...>(buffer, sz);
    }
    else if constexpr (id == type_id::map_container_t) {
      get_compatible_version_numbers<Buffer, typename T::key_type, T,
                                     ParentArgs...>(buffer, sz);
      get_compatible_version_numbers<Buffer, typename T::mapped_type, T,
                                     ParentArgs...>(buffer, sz);
    }
    else if constexpr (id == type_id::set_container_t ||
                       id == type_id::container_t) {
      get_compatible_version_numbers<Buffer, typename T::value_type, T,
                                     ParentArgs...>(buffer, sz);
    }
    else if constexpr (id == type_id::expected_t) {
      get_compatible_version_numbers<Buffer, typename T::value_type, T,
                                     ParentArgs...>(buffer, sz);
      get_compatible_version_numbers<Buffer, typename T::error_type, T,
                                     ParentArgs...>(buffer, sz);
    }
    else if constexpr (id == type_id::variant_t) {
      get_variant_compatible_version_numbers<Buffer, T, T, ParentArgs...>(
          buffer, sz, std::make_index_sequence<std::variant_size_v<T>>{});
    }
  }
}

template <typename T>
struct serialize_static_config {
  static constexpr bool has_compatible = exist_compatible_member<T>;
#ifdef NDEBUG
  static constexpr bool has_type_literal = false;
#else
  static constexpr bool has_type_literal = true;
#endif
};
}  // namespace detail

namespace detail {

template <uint64_t conf, typename T>
constexpr bool check_if_add_type_literal() {
  constexpr auto config = conf & 0b11;
  if constexpr (config == sp_config::DEFAULT) {
    constexpr auto config = get_type_config<T>() & 0b11;
    if constexpr (config == sp_config::DEFAULT) {
      return serialize_static_config<T>::has_type_literal;
    }
    else {
      return config == sp_config::ENABLE_TYPE_INFO;
    }
  }
  else {
    return config == sp_config::ENABLE_TYPE_INFO;
  }
}

template <typename Arg, typename... ParentArgs>
constexpr bool check_if_has_container();

template <typename Arg, typename... ParentArgs, std::size_t... I>
constexpr bool check_if_has_container_helper(std::index_sequence<I...> idx) {
  return ((check_if_has_container<remove_cvref_t<std::tuple_element_t<I, Arg>>,
                                  ParentArgs...>()) ||
          ...);
}

template <typename Arg, typename... ParentArgs, std::size_t... I>
constexpr bool check_if_has_container_variant_helper(
    std::index_sequence<I...> idx) {
  return ((check_if_has_container<
              remove_cvref_t<std::variant_alternative_t<I, Arg>>, Arg,
              ParentArgs...>()) ||
          ...);
}

template <typename Arg, typename... ParentArgs>
constexpr bool check_if_has_container() {
  if constexpr (is_trivial_view_v<Arg>) {
    return check_if_has_container<typename Arg::value_type, ParentArgs...>();
  }
  else {
    constexpr std::size_t has_cycle = check_circle<Arg, ParentArgs...>();
    if constexpr (has_cycle != 0) {
      return false;
    }
    else {
      constexpr auto id = get_type_id<Arg>();
      if constexpr (id == type_id::struct_t) {
        using Args = decltype(get_types<Arg>());
        return check_if_has_container_helper<Args, Arg, ParentArgs...>(
            std::make_index_sequence<std::tuple_size_v<Args>>());
      }
      else if constexpr (id == type_id::variant_t) {
        constexpr auto sz = std::variant_size_v<Arg>;
        static_assert(sz > 0, "empty param of std::variant is not allowed!");
        static_assert(sz < 256, "too many alternative type in variant!");
        return check_if_has_container_variant_helper<Arg, ParentArgs...>(
            std::make_index_sequence<std::variant_size_v<Arg>>());
      }
      else if constexpr (id == type_id::array_t) {
        return check_if_has_container<
            remove_cvref_t<decltype(std::declval<Arg>()[0])>, Arg,
            ParentArgs...>();
      }
      else if constexpr (id == type_id::bitset_t) {
        return false;
      }
      else if constexpr (unique_ptr<Arg>) {
        if constexpr (is_base_class<typename Arg::element_type>) {
          // We can't make sure if derived class has container or not
          return true;
        }
        else {
          return check_if_has_container<
              remove_cvref_t<typename Arg::element_type>, Arg, ParentArgs...>();
        }
      }
      else if constexpr (id == type_id::container_t ||
                         id == type_id::string_t ||
                         id == type_id::set_container_t ||
                         id == type_id::map_container_t) {
        return true;
      }
      else if constexpr (id == type_id::optional_t ||
                         id == type_id::compatible_t) {
        return check_if_has_container<remove_cvref_t<typename Arg::value_type>,
                                      Arg, ParentArgs...>();
      }
      else if constexpr (id == type_id::expected_t) {
        return check_if_has_container<remove_cvref_t<typename Arg::value_type>,
                                      Arg, ParentArgs...>() ||
               check_if_has_container<remove_cvref_t<typename Arg::error_type>,
                                      Arg, ParentArgs...>();
      }
      else {
        return false;
      }
    }
  }
}

template <uint64_t conf, typename T>
constexpr bool check_if_disable_hash_head_impl() {
  constexpr auto config = conf & 0b11;
  if constexpr (config == sp_config::DEFAULT) {
    constexpr auto config = get_type_config<T>() & 0b11;
    return config == sp_config::DISABLE_ALL_META_INFO;
  }
  else {
    return config == sp_config::DISABLE_ALL_META_INFO;
  }
}

template <uint64_t conf, typename T>
constexpr bool check_if_disable_hash_head() {
  if constexpr (check_if_disable_hash_head_impl<conf, T>()) {
    static_assert(
        !check_if_compatible_element_exist<decltype(get_types<T>())>(),
        "It's not allow add compatible member when you disable hash head");
    return true;
  }
  else {
    return false;
  }
}

template <uint64_t conf, typename T>
constexpr bool check_has_metainfo() {
  return serialize_static_config<T>::has_compatible ||
         (!check_if_disable_hash_head<conf, T>() &&
          check_if_add_type_literal<conf, T>()) ||
         ((check_if_disable_hash_head<conf, T>() &&
           check_if_has_container<T>()));
}

template <typename U>
constexpr auto get_types() {
  using T = remove_cvref_t<U>;
  if constexpr (user_defined_serialization<T>) {
    return declval<std::tuple<T>>();
  }
  else if constexpr (std::is_fundamental_v<T> || std::is_enum_v<T> ||
                     varint_t<T> || string<T> || container<T> ||
                     ylt::reflection::optional<T> || unique_ptr<T> ||
                     is_variant_v<T> || ylt::reflection::expected<T> ||
                     array<T> || c_array<T> ||
                     std::is_same_v<std::monostate, T> || bitset<T>
#if (__GNUC__ || __clang__) && defined(STRUCT_PACK_ENABLE_INT128)
                     || std::is_same_v<__int128, T> ||
                     std::is_same_v<unsigned __int128, T>
#endif
  ) {
    return declval<std::tuple<T>>();
  }
  else if constexpr (tuple<T>) {
    return declval<T>();
  }
  else if constexpr (is_trivial_tuple<T>) {
    return declval<T>();
  }
  else if constexpr (pair<T>) {
    return declval<
        std::tuple<typename T::first_type, typename T::second_type>>();
  }
  else if constexpr (std::is_class_v<T>) {
    // clang-format off
    return visit_members(
        declval<T>(), [](auto &&... args) constexpr {
          return declval<std::tuple<remove_cvref_t<decltype(args)>...>>();
        });
    // clang-format on
  }
  else {
    static_assert(!sizeof(T), "the type is not supported!");
  }
}

}  // namespace detail
}  // namespace struct_pack
