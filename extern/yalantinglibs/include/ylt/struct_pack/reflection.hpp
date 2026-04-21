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
// clang-format off
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include "ylt/reflection/member_count.hpp"
#include "ylt/reflection/internal/arg_list_macro.hpp"

#if __has_include(<span>)
#include <span>
#endif

#include "derived_helper.hpp"
#include "foreach_macro.h"
#include "marco.h"
#include "util.h"
#include "ylt/reflection/template_switch.hpp"
#include "ylt/reflection/member_ptr.hpp"


#if __cpp_concepts >= 201907L
#include <concepts>
#endif


namespace struct_pack {

enum sp_config : uint64_t {
  DEFAULT = 0,
  DISABLE_TYPE_INFO = 0b1,
  ENABLE_TYPE_INFO = 0b10,
  DISABLE_ALL_META_INFO = 0b11,
  ENCODING_WITH_VARINT = 0b100,
  USE_FAST_VARINT = 0b1000
};

namespace detail {

template <typename... Args>
using get_args_type = remove_cvref_t<typename std::conditional<
    sizeof...(Args) == 1, std::tuple_element_t<0, std::tuple<Args...>>,
    std::tuple<Args...>>::type>;

template <typename U>
constexpr auto get_types();

template <typename T, template <typename, typename, std::size_t> typename Op,
          typename... Contexts, std::size_t... I>
constexpr void for_each_impl(std::index_sequence<I...>, Contexts &...contexts) {
  using type = decltype(get_types<T>());
  (Op<T, std::tuple_element_t<I, type>, I>{}(contexts...), ...);
}

template <typename T, template <typename, typename, std::size_t> typename Op,
          typename... Contexts>
constexpr void for_each(Contexts &...contexts) {
  using type = decltype(get_types<T>());
  for_each_impl<T, Op>(std::make_index_sequence<std::tuple_size_v<type>>(),
                       contexts...);
}

template <typename T>
constexpr std::size_t members_count();
template <typename T>
constexpr std::size_t pack_align();
template <typename T>
constexpr std::size_t alignment();
}  // namespace detail

template <typename T>
constexpr std::size_t members_count = detail::members_count<T>();
template <typename T>
constexpr std::size_t pack_alignment_v = 0;
template <typename T>
constexpr std::size_t alignment_v = 0;

#if __cpp_concepts >= 201907L

template <typename T>
concept writer_t = requires(T t) {
  t.write((const char *)nullptr, std::size_t{});
};

template <typename T>
concept reader_t = requires(T t) {
  t.read((char *)nullptr, std::size_t{});
  t.ignore(std::size_t{});
  t.tellg();
};

template <typename T>
concept view_reader_t = reader_t<T> && requires(T t) {
  { t.read_view(std::size_t{}) } -> std::convertible_to<const char *>;
};

#else

template <typename T, typename = void>
struct writer_t_impl : std::false_type {};

template <typename T>
struct writer_t_impl<T, std::void_t<decltype(std::declval<T>().write(
                            (const char *)nullptr, std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool writer_t = writer_t_impl<T>::value;

template <typename T, typename = void>
struct reader_t_impl : std::false_type {};

template <typename T>
struct reader_t_impl<
    T, std::void_t<decltype(std::declval<T>().read((char *)nullptr,
                                                   std::size_t{})),
                   decltype(std::declval<T>().ignore(std::size_t{})),
                   decltype(std::declval<T>().tellg())>> : std::true_type {};

template <typename T>
constexpr bool reader_t = reader_t_impl<T>::value;

template <typename T, typename = void>
struct view_reader_t_impl : std::false_type {};

template <typename T>
struct view_reader_t_impl<
    T, std::void_t<decltype(std::declval<T>().read_view(std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool view_reader_t = reader_t<T> &&view_reader_t_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L

template <typename T>
concept can_reserve = requires(T t) {
  t.reserve(std::size_t{});
};

template <typename T>
concept can_shrink_to_fit = requires(T t) {
  t.shrink_to_fit();
};

#else

template <typename T, typename = void>
struct can_reserve_impl : std::false_type {};

template <typename T>
struct can_reserve_impl<
    T, std::void_t<decltype(std::declval<T>().reserve(std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool can_reserve = can_reserve_impl<T>::value;

template <typename T, typename = void>
struct can_shrink_to_fit_impl : std::false_type {};

template <typename T>
struct can_shrink_to_fit_impl<
    T, std::void_t<decltype(std::declval<T>().shrink_to_fit())>>
    : std::true_type {};

template <typename T>
constexpr bool can_shrink_to_fit = can_shrink_to_fit_impl<T>::value;

#endif

template <typename T, uint64_t version = 0>
struct compatible;

namespace detail {

#if __cpp_concepts >= 201907L

template <typename T>
concept has_user_defined_id = requires {
  typename std::integral_constant<std::size_t, T::struct_pack_id>;
};

template <typename T>
concept has_user_defined_id_ADL = requires {
  typename std::integral_constant<std::size_t,
                                  struct_pack_id((T*)nullptr)>;
};

#else

template <typename T, typename = void>
struct has_user_defined_id_impl : std::false_type {};

template <typename T>
struct has_user_defined_id_impl<
    T, std::void_t<std::integral_constant<std::size_t, T::struct_pack_id>>>
    : std::true_type {};

template <typename T>
constexpr bool has_user_defined_id = has_user_defined_id_impl<T>::value;

template <std::size_t sz>
struct constant_checker{};

template <typename T, typename = void>
struct has_user_defined_id_ADL_impl : std::false_type {};

#ifdef _MSC_VER
// FIXME: we can't check if it's compile-time calculated in msvc with C++17
template <typename T>
struct has_user_defined_id_ADL_impl<
    T, std::void_t<decltype(struct_pack_id((T*)nullptr))>>
    : std::true_type {};
#else

template <typename T>
struct has_user_defined_id_ADL_impl<
    T, std::void_t<constant_checker<struct_pack_id((T*)nullptr)>>>
    : std::true_type {};

#endif

template <typename T>
constexpr bool has_user_defined_id_ADL = has_user_defined_id_ADL_impl<T>::value;

#endif



#if __cpp_concepts >= 201907L
template <typename T>
concept is_base_class = requires (T* t) {
  std::is_same_v<uint32_t,decltype(t->get_struct_pack_id())>;
  typename struct_pack::detail::derived_class_set_t<T>;
};
#else
template <typename T, typename = void>
struct is_base_class_impl : std::false_type {};

template <typename T>
struct is_base_class_impl<
    T, std::void_t<
    std::enable_if<std::is_same_v<decltype(((T*)nullptr)->get_struct_pack_id()), uint32_t>>,
    typename struct_pack::detail::derived_class_set_t<T>>>
    : std::true_type {};
template <typename T>
constexpr bool is_base_class=is_base_class_impl<T>::value;

#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept deserialize_view = requires(Type container) {
    container.size();
    container.data();
  };
#else

template <typename T, typename = void>
struct deserialize_view_impl : std::false_type {};
template <typename T>
struct deserialize_view_impl<
    T, std::void_t<decltype(std::declval<T>().size()),decltype(std::declval<T>().data())>>
    : std::true_type {};

template <typename Type>
constexpr bool deserialize_view = deserialize_view_impl<Type>::value;

#endif

  struct memory_writer {
    char *buffer;
    STRUCT_PACK_INLINE void write(const char *data, std::size_t len) {
      memcpy(buffer, data, len);
      buffer += len;
    }
  };


#if __cpp_concepts >= 201907L
  template <typename Type>
  concept container_adapter = requires(Type container) {
    typename remove_cvref_t<Type>::value_type;
    container.size();
    container.pop();
  };
#else
  template <typename T, typename = void>
  struct container_adapter_impl : std::false_type {};

  template <typename T>
  struct container_adapter_impl<T, std::void_t<
    typename remove_cvref_t<T>::value_type,
    decltype(std::declval<T>().size()),
    decltype(std::declval<T>().pop())>>
      : std::true_type {};

  template <typename T>
  constexpr bool container_adapter = container_adapter_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept container = requires(Type container) {
    typename remove_cvref_t<Type>::value_type;
    container.size();
    container.begin();
    container.end();
  };
#else
  template <typename T, typename = void>
  struct container_impl : std::false_type {};

  template <typename T>
  struct container_impl<T, std::void_t<
    typename remove_cvref_t<T>::value_type,
    decltype(std::declval<T>().size()),
    decltype(std::declval<T>().begin()),
    decltype(std::declval<T>().end())>>
      : std::true_type {};

  template <typename T>
  constexpr bool container = container_impl<T>::value;
#endif

  template <typename Type>
  constexpr bool is_char_t = std::is_same_v<Type, signed char> ||
      std::is_same_v<Type, char> || std::is_same_v<Type, unsigned char> ||
      std::is_same_v<Type, wchar_t> || std::is_same_v<Type, char16_t> ||
      std::is_same_v<Type, char32_t>
#ifdef __cpp_lib_char8_t
 || std::is_same_v<Type, char8_t>
#endif
;


#if __cpp_concepts >= 201907L
  template <typename Type>
  concept string = container<Type> && requires(Type container) {
    requires is_char_t<typename remove_cvref_t<Type>::value_type>;
    container.length();
    container.data();
  };
#else
  template <typename T, typename = void>
  struct string_impl : std::false_type {};

  template <typename T>
  struct string_impl<T, std::void_t<
    std::enable_if_t<is_char_t<typename remove_cvref_t<T>::value_type>>,
    decltype(std::declval<T>().length()),
    decltype(std::declval<T>().data())>> 
      : std::true_type {};

  template <typename T>
  constexpr bool string = string_impl<T>::value && container<T>;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept string_view = string<Type> && !requires(Type container) {
    container.resize(std::size_t{});
  };
#else
  template <typename T, typename = void>
  struct string_view_impl : std::true_type {};

  template <typename T>
  struct string_view_impl<T, std::void_t<
    decltype(std::declval<T>().resize(std::size_t{}))>> 
      : std::false_type {};

  template <typename T>
  constexpr bool string_view = string<T> && string_view_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept span = container<Type> && requires(Type t) {
    Type{(typename Type::value_type*)nullptr ,std::size_t{} };
    t.subspan(std::size_t{},std::size_t{});
  };
#else
  template <typename T, typename = void>
  struct span_impl : std::false_type {};

  template <typename T>
  struct span_impl<T, std::void_t<
    decltype(T{(typename T::value_type*)nullptr ,std::size_t{}}),
    decltype(std::declval<T>().subspan(std::size_t{},std::size_t{}))>> 
      : std::true_type {};

  template <typename T>
  constexpr bool span = container<T> && span_impl<T>::value;
#endif 


#if __cpp_concepts >= 201907L
  template <typename Type>
  concept dynamic_span = span<Type> && Type::extent == SIZE_MAX;
#else
  template <typename T, typename = void>
  struct dynamic_span_impl : std::false_type {};

  template <typename T>
  struct dynamic_span_impl<T, std::void_t<
    std::enable_if_t<(T::extent == SIZE_MAX)>>> 
      : std::true_type {};

  template <typename T>
  constexpr bool dynamic_span = span<T> && dynamic_span_impl<T>::value;
#endif
  template <typename Type>
  constexpr bool static_span = span<Type> && !dynamic_span<Type>;


#if __cpp_lib_span >= 202002L && __cpp_concepts>=201907L

  template <typename Type>
  concept continuous_container =
      string<Type> || (container<Type> && requires(Type container) {
        std::span{container};
      });

#else

  template <typename Type>
  constexpr inline bool is_std_basic_string_v = false;

  template <typename... args>
  constexpr inline bool is_std_basic_string_v<std::basic_string<args...>> =
      true;

  template <typename Type>
  constexpr inline bool is_std_vector_v = false;

  template <typename... args>
  constexpr inline bool is_std_vector_v<std::vector<args...>> = true;

  template <typename Type>
  constexpr bool continuous_container =
      string<Type> || (container<Type> && (is_std_vector_v<Type> || is_std_basic_string_v<Type>));
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept map_container = container<Type> && requires(Type container) {
    typename remove_cvref_t<Type>::mapped_type;
  };
#else
template <typename T, typename = void>
  struct map_container_impl : std::false_type {};

  template <typename T>
  struct map_container_impl<T, std::void_t<
    typename remove_cvref_t<T>::mapped_type>> 
      : std::true_type {};

  template <typename T>
  constexpr bool map_container = container<T> && map_container_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept hash_map_container = map_container<Type> && requires(Type container) {
    typename remove_cvref_t<Type>::hasher;
  };
#else
template <typename T, typename = void>
  struct hash_map_container_impl : std::false_type {};

  template <typename T>
  struct hash_map_container_impl<T, std::void_t<
    typename remove_cvref_t<T>::hasher>> 
      : std::true_type {};

  template <typename T>
  constexpr bool hash_map_container = map_container<T> && hash_map_container_impl<T>::value;
#endif

  template <typename Type>
  constexpr inline bool is_std_unordered_map_v = false;

  template <typename... args>
  constexpr inline bool is_std_unordered_map_v<std::unordered_map<args...>> = true;

  template <typename... args>
  constexpr inline bool is_std_unordered_map_v<std::unordered_multimap<args...>> = true;


#if __cpp_concepts >= 201907L
  template <typename Type>
  concept set_container = container<Type> && requires {
    typename remove_cvref_t<Type>::key_type;
  };
#else
  template <typename T, typename = void>
  struct set_container_impl : std::false_type {};

  template <typename T>
  struct set_container_impl<T, std::void_t<
    typename remove_cvref_t<T>::key_type>> 
      : std::true_type {};

  template <typename T>
  constexpr bool set_container = container<T> && set_container_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept bitset = requires (Type t){
    t.flip();
    t.set();
    t.reset();
    t.count();
  } && (Type{}.size()+7)/8 == sizeof(Type);
#else
  template <typename T, typename = void>
  struct bitset_impl : std::false_type {};


  template <typename T>
  struct bitset_impl<T, std::void_t<
    decltype(std::declval<T>().flip()),
    decltype(std::declval<T>().set()),
    decltype(std::declval<T>().reset()),
    decltype(std::declval<T>().count()),
    decltype(std::declval<T>().size())>> 
      : std::true_type {};

  template<typename T>
  constexpr bool bitset_size_checker() {
    if constexpr (bitset_impl<T>::value) {
      return (T{}.size()+7)/8==sizeof(T);
    }
    else {
      return false;
    }
  }

  template <typename T>
  constexpr bool bitset = bitset_impl<T>::value && bitset_size_checker<T>();
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept tuple = requires(Type tuple) {
    std::get<0>(tuple);
    sizeof(std::tuple_size<remove_cvref_t<Type>>);
  };
#else
template <typename T, typename = void>
  struct tuple_impl : std::false_type {};

  template <typename T>
  struct tuple_impl<T, std::void_t<
    decltype(std::get<0>(std::declval<T>())),
    decltype(sizeof(std::tuple_size<remove_cvref_t<T>>::value))>> 
      : std::true_type {};

  template <typename T>
  constexpr bool tuple = tuple_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept user_defined_refl = ylt::reflection::is_out_ylt_refl_v<Type> || ylt::reflection::is_inner_ylt_refl_v<Type>;
#else
  template <typename T>
  constexpr bool user_defined_refl = ylt::reflection::is_out_ylt_refl_v<T> || ylt::reflection::is_inner_ylt_refl_v<T>;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept user_defined_config_by_ADL = std::is_same_v<decltype(set_sp_config(std::declval<Type*>())),struct_pack::sp_config>;
#else
  template <typename T, typename = void>
  struct user_defined_config_by_ADL_impl : std::false_type {};

  template <typename T>
  struct user_defined_config_by_ADL_impl<T, std::void_t<
    std::enable_if_t<std::is_same_v<decltype(set_sp_config(std::declval<T*>())),struct_pack::sp_config>>>>
      : std::true_type {};

  template <typename T>
  constexpr bool user_defined_config_by_ADL = user_defined_config_by_ADL_impl<T>::value;
#endif

template<typename T>
constexpr decltype(auto) delay_sp_config_eval() {
  if constexpr (sizeof(T)==0) {
    return (T*)nullptr;
  }
  else {
    return (sp_config*)nullptr;
  }
}

#if __cpp_concepts >= 201907L
  template<typename T>
  concept has_default_config = std::is_same_v<decltype(set_default(decltype(delay_sp_config_eval<T>()){})),struct_pack::sp_config>;
#else
  template <typename T, typename = void>
  struct has_default_config_impl : std::false_type {};

  template <typename T>
  struct has_default_config_impl<T, std::void_t<
    std::enable_if_t<std::is_same_v<decltype(set_default(decltype(delay_sp_config_eval<T>()){})),struct_pack::sp_config>>>>
      : std::true_type {};

  template <typename T>
  constexpr bool has_default_config = has_default_config_impl<T>::value;
#endif


#if __cpp_concepts >= 201907L
  template <typename Type>
  concept user_defined_config = requires {
    Type::struct_pack_config;
  };
#else
  template <typename T, typename = void>
  struct user_defined_config_impl : std::false_type {};

  template <typename T>
  struct user_defined_config_impl<T, std::void_t<decltype(T::struct_pack_config)>>
      : std::true_type {};

  template <typename T>
  constexpr bool user_defined_config = user_defined_config_impl<T>::value;
#endif

struct memory_reader;

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept user_defined_serialization = requires (Type& t) {
    sp_serialize_to(std::declval<struct_pack::detail::memory_writer&>(),(const Type&)t);
    {sp_deserialize_to(std::declval<struct_pack::detail::memory_reader&>(),t)} -> std::same_as<struct_pack::err_code>;
    {sp_get_needed_size((const Type&)t)}->std::same_as<std::size_t>;
  };
  template <typename Type>
  concept user_defined_type_name = requires {
    { sp_set_type_name((Type*)nullptr) } -> std::same_as<std::string_view>;
  };
#else

  template <typename T, typename = void>
  struct user_defined_serialization_impl : std::false_type {};

  template <typename T>
  struct user_defined_serialization_impl<T, std::void_t<
    decltype(sp_serialize_to(std::declval<struct_pack::detail::memory_writer&>(),std::declval<const T&>())),
    std::enable_if<std::is_same_v<decltype(sp_deserialize_to(std::declval<struct_pack::detail::memory_reader&>(),std::declval<T&>())), struct_pack::err_code>,
    std::enable_if<std::is_same_v<decltype(sp_get_needed_size(std::declval<const T&>())), std::string_view>>>>>
      : std::true_type {};

  template <typename Type>
  constexpr bool user_defined_serialization = user_defined_serialization_impl<Type>::value;

  template <typename T, typename = void>
  struct user_defined_type_name_impl : std::false_type {};

  template <typename T>
  struct user_defined_type_name_impl<T, std::void_t<
    std::enable_if<std::is_same_v<decltype(sp_set_type_name((T*)nullptr)), std::string_view>>>>
      : std::true_type {};

  template <typename Type>
  constexpr bool user_defined_type_name = user_defined_type_name_impl<Type>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept tuple_size = requires(Type tuple) {
    std::tuple_size<remove_cvref_t<Type>>::value;
  };
#else
  template <typename T, typename = void>
  struct tuple_size_impl : std::false_type {};

  template <typename T>
  struct tuple_size_impl<T, std::void_t<
    decltype(std::tuple_size<remove_cvref_t<T>>::value)>> 
      : std::true_type {};

  template <typename T>
  constexpr bool tuple_size = tuple_size_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept array = requires(Type arr) {
    arr.size();
    std::tuple_size<remove_cvref_t<Type>>{};
  };
#else
  template <typename T, typename = void>
  struct array_impl : std::false_type {};

  template <typename T>
  struct array_impl<T, std::void_t<
    decltype(std::declval<T>().size()),
    decltype(std::tuple_size<remove_cvref_t<T>>{})>> 
      : std::true_type {};

  template <typename T>
  constexpr bool array = array_impl<T>::value;
#endif


  template <class T>
  constexpr bool c_array =
      std::is_array_v<T> && std::extent_v<remove_cvref_t<T>> > 0;

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept pair = requires(Type p) {
    typename remove_cvref_t<Type>::first_type;
    typename remove_cvref_t<Type>::second_type;
    p.first;
    p.second;
  };
#else
  template <typename T, typename = void>
  struct pair_impl : std::false_type {};

  template <typename T>
  struct pair_impl<T, std::void_t<
    typename remove_cvref_t<T>::first_type,
    typename remove_cvref_t<T>::second_type,
    decltype(std::declval<T>().first),
    decltype(std::declval<T>().second)>> 
      : std::true_type {};

  template <typename T>
  constexpr bool pair = pair_impl<T>::value;
#endif

#if __cpp_concepts >= 201907L
  template <typename Type>
  concept unique_ptr = requires(Type ptr) {
    ptr.operator*();
    typename remove_cvref_t<Type>::element_type;
  }
  &&!requires(Type ptr, Type ptr2) { ptr = ptr2; };
#else
  template <typename T, typename = void>
  struct unique_ptr_impl : std::false_type {};

  template <typename T>
  struct unique_ptr_impl<T, std::void_t<
    typename remove_cvref_t<T>::element_type,
    decltype(std::declval<T>().operator*())>> 
      : std::true_type {};

  template <typename T>
  constexpr bool unique_ptr = unique_ptr_impl<T>::value;
#endif




  template <typename Type>
  constexpr inline bool is_compatible_v = false;

  template <typename Type, uint64_t version>
  constexpr inline bool is_compatible_v<compatible<Type,version>> = true;

  template <typename Type>
  constexpr inline bool is_variant_v = false;

  template <typename... args>
  constexpr inline bool is_variant_v<std::variant<args...>> = true;

  template <typename T>
  constexpr inline bool is_trivial_tuple = false;

  template <typename T>
  class varint;

  template <typename T>
  class sint;

  template <typename T>
  constexpr bool varintable_t =
      std::is_same_v<T, varint<int32_t>> || std::is_same_v<T, varint<int64_t>> ||
      std::is_same_v<T, varint<uint32_t>> || std::is_same_v<T, varint<uint64_t>>;
  template <typename T>
  constexpr bool sintable_t =
      std::is_same_v<T, sint<int32_t>> || std::is_same_v<T, sint<int64_t>>;

  template <typename T, uint64_t parent_tag = 0 >
  constexpr bool varint_t = varintable_t<T> || sintable_t<T> || ((parent_tag&struct_pack::ENCODING_WITH_VARINT) && (std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> || std::is_same_v<T,int64_t> || std::is_same_v<T,uint64_t>));

  template <typename Type>
  constexpr inline bool is_trivial_view_v = false;

  template <typename... Args>
  constexpr uint64_t get_parent_tag();

  template <typename T, bool ignore_compatible_field = false, uint64_t parent_tag = 0>
  struct is_trivial_serializable {
    private:
      template<typename U,uint64_t parent_tag_=0, std::size_t... I>
      static constexpr bool class_visit_helper(std::index_sequence<I...>) {
        return (is_trivial_serializable<std::tuple_element_t<I, U>,
                                            ignore_compatible_field,parent_tag_>::value &&
                    ...);
      }
    
    public:
      static constexpr bool solve() {
        if constexpr (user_defined_serialization<T>) {
          return false;
        }
        else if constexpr (std::is_same_v<T,std::monostate>) {
          return true;
        }
        else if constexpr (std::is_abstract_v<T>) {
          return false;
        }
        else if constexpr (varint_t<T,parent_tag>) {
          return false;
        }
        else if constexpr (is_compatible_v<T> || is_trivial_view_v<T>) {
          return ignore_compatible_field;
        }
        else if constexpr (std::is_enum_v<T> || std::is_fundamental_v<T>  || bitset<T>
#if (__GNUC__ || __clang__) && defined(STRUCT_PACK_ENABLE_INT128)
        || std::is_same_v<__int128,T> || std::is_same_v<unsigned __int128,T>
#endif
        ) {
          return true;
        }
        else if constexpr (array<T>) {
          return is_trivial_serializable<typename T::value_type,
                                        ignore_compatible_field>::value;
        }
        else if constexpr (c_array<T>) {
          return is_trivial_serializable<typename std::remove_all_extents<T>::type,
                                        ignore_compatible_field>::value;
        }
        else if constexpr (!pair<T> && tuple<T> && !is_trivial_tuple<T>) {
          return false;
        }
        else if constexpr (user_defined_refl<T>) {
          return false;
        }
        else if constexpr (container<T> || ylt::reflection::optional<T> || is_variant_v<T> ||
                          unique_ptr<T> || ylt::reflection::expected<T> || container_adapter<T>) {
          return false;
        }
        else if constexpr (pair<T>) {
          return is_trivial_serializable<typename T::first_type,
                                        ignore_compatible_field>::value &&
                is_trivial_serializable<typename T::second_type,
                                        ignore_compatible_field>::value;
        }
        else if constexpr (is_trivial_tuple<T>) {
          return class_visit_helper<T>(std::make_index_sequence<std::tuple_size_v<T>>{});
        }
        else if constexpr (std::is_class_v<T>) {
          constexpr auto tag = get_parent_tag<T>();
          using U = decltype(get_types<T>());
          return class_visit_helper<U , tag>(std::make_index_sequence<std::tuple_size_v<U>>{});
        }
        else
          return false;
      }

      static inline constexpr bool value = is_trivial_serializable<std::remove_cv_t<T>,ignore_compatible_field,parent_tag>::solve();
  };

}
template <typename T, typename = std::enable_if_t<detail::is_trivial_serializable<T>::value>>
struct trivial_view;
namespace detail {

#if __cpp_concepts < 201907L

template <typename T, typename = void>
struct trivially_copyable_container_impl : std::false_type {};

template <typename T>
struct trivially_copyable_container_impl<T, std::void_t<std::enable_if_t<is_trivial_serializable<typename T::value_type>::value>>>
    : std::true_type {};

template <typename Type>
constexpr bool trivially_copyable_container =
    continuous_container<Type> && trivially_copyable_container_impl<Type>::value;

#else

template <typename Type>
constexpr bool trivially_copyable_container =
    continuous_container<Type> &&
    requires(Type container) {
      requires is_trivial_serializable<typename Type::value_type>::value;
    };

#endif

  template <typename Type>
  constexpr inline bool is_trivial_view_v<struct_pack::trivial_view<Type>> = true;

  template <typename T>
  constexpr std::size_t members_count() {
    return ylt::reflection::members_count_v<T>;
  }

  template<typename Object,typename Visitor>
  constexpr decltype(auto) STRUCT_PACK_INLINE visit_members(Object &&object,
                                                            Visitor &&visitor) {
    using type = remove_cvref_t<decltype(object)>;
    constexpr auto Count = struct_pack::members_count<type>;
    if constexpr (Count == 0 && std::is_class_v<type> &&
                  !std::is_same_v<type, std::monostate>) {
      static_assert(!sizeof(type), "1. If the struct is empty, which is not allowed in struct_pack type system.\n"
      "2. If the strut is not empty, it means struct_pack can't calculate your struct members' count. You can use macro YLT_REFL(Typename, field1, field2...).");
    }
    // If you see any structured binding error in the follow line, it
    // means struct_pack can't calculate your struct's members count
    // correctly. 
    // The best way is use macro YLT_REFL(Typename, field1, field2...)
    // See the src/struct_pack/example/non_aggregated_type.cpp for more details.
    //
    // You can also to mark it manually.
    // For example, there is a struct named Hello,
    // and it has 3 members.
    //
    // You can mark it as:
    //
    // template <>
    // constexpr size_t struct_pack::members_count<Hello> = 3;

    return ylt::reflection::visit_members<Object, Visitor, Count>(std::forward<Object>(object), std::forward<Visitor>(visitor));
  }
}  // namespace detail
#if __cpp_concepts >= 201907L

template <typename T>
concept checkable_reader_t = reader_t<T> && requires(T t) {
  t.check(std::size_t{});
};

#else

template <typename T, typename = void>
struct checkable_reader_t_impl : std::false_type {};

template <typename T>
struct checkable_reader_t_impl<
    T, std::void_t<decltype(std::declval<T>().check(std::size_t{}))>>
    : std::true_type {};

template <typename T>
constexpr bool checkable_reader_t = reader_t<T> &&checkable_reader_t_impl<T>::value;
#endif
}  // namespace struct_pack

// This marco is only for compatible with old version. Please instead it by YLT_REFL
#define STRUCT_PACK_REFL(Type,...) \
  YLT_REFL(Type,__VA_ARGS__)
