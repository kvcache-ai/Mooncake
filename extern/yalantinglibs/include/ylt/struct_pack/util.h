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
#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "marco.h"

namespace struct_pack::detail {

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
  constexpr size_t pos = sample.find("int");
  constexpr std::string_view str = get_raw_name<T>();
  constexpr auto next1 = str.rfind(sample[pos + 3]);
#if defined(_MSC_VER)
  constexpr std::size_t npos = str.find_first_of(" ", pos);
  if (npos != std::string_view::npos)
    return str.substr(npos + 1, next1 - npos - 1);
  else
    return str.substr(pos, next1 - pos);
#else
  return str.substr(pos, next1 - pos);
#endif
}

template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

[[noreturn]] inline void unreachable() {
  // Uses compiler specific extensions if possible.
  // Even if no extension is used, undefined behavior is still raised by
  // an empty function body and the noreturn attribute.
#ifdef __GNUC__  // GCC, Clang, ICC
  __builtin_unreachable();
#elif defined(_MSC_VER)  // msvc
  __assume(false);
#endif
}

template <typename T>
[[noreturn]] constexpr T declval() {
  unreachable();
}

template <typename T, std::size_t sz>
constexpr void STRUCT_PACK_INLINE compile_time_sort(std::array<T, sz> &array) {
  // FIXME: use faster compile-time sort
  for (std::size_t i = 0; i < array.size(); ++i) {
    for (std::size_t j = i + 1; j < array.size(); ++j) {
      if (array[i] > array[j]) {
        auto tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
      }
    }
  }
  return;
}

template <typename T, std::size_t sz>
constexpr std::size_t STRUCT_PACK_INLINE
calculate_uniqued_size(const std::array<T, sz> &input) {
  std::size_t unique_cnt = sz;
  for (std::size_t i = 1; i < input.size(); ++i) {
    if (input[i] == input[i - 1]) {
      --unique_cnt;
    }
  }
  return unique_cnt;
}

template <typename T, std::size_t sz1, std::size_t sz2>
constexpr void STRUCT_PACK_INLINE compile_time_unique(
    const std::array<T, sz1> &input, std::array<T, sz2> &output) {
  std::size_t j = 0;
  static_assert(sz1 != 0, "not allow empty input!");
  output[0] = input[0];
  for (std::size_t i = 1; i < input.size(); ++i) {
    if (input[i] != input[i - 1]) {
      output[++j] = input[i];
    }
  }
}

#ifndef __clang__
#define struct_pack_has_feature(X) false
#else
#define struct_pack_has_feature __has_feature
#endif

#if __cpp_lib_string_resize_and_overwrite >= 202110L
template <typename ch>
inline void resize(std::basic_string<ch> &str, std::size_t sz) {
  str.resize_and_overwrite(sz, [sz](ch *, std::size_t) {
    return sz;
  });
}
#elif (defined(_MSC_VER) && _MSC_VER <= 1920)
// old msvc don't support visit private, discard it.

#else

template <typename Function, Function func_ptr>
class string_thief {
 public:
  friend void string_set_length_hacker(std::string &self, std::size_t sz) {
#if defined(_MSVC_STL_VERSION)
    (self.*func_ptr)._Myval2._Mysize = sz;
#else
#if defined(_LIBCPP_VERSION)
    (self.*func_ptr)(sz);
#else
#if (_GLIBCXX_USE_CXX11_ABI == 0) && defined(__GLIBCXX__)
    (self.*func_ptr)()->_M_set_length_and_sharable(sz);
#else
#if defined(__GLIBCXX__)
    (self.*func_ptr)(sz);
#endif
#endif
#endif
#endif
  }
};

#if defined(__GLIBCXX__)  // libstdc++
#if (_GLIBCXX_USE_CXX11_ABI == 0)
template class string_thief<decltype(&std::string::_M_rep),
                            &std::string::_M_rep>;
#else
template class string_thief<decltype(&std::string::_M_set_length),
                            &std::string::_M_set_length>;
#endif
#elif defined(_LIBCPP_VERSION)
template class string_thief<decltype(&std::string::__set_size),
                            &std::string::__set_size>;
#elif defined(_MSVC_STL_VERSION)
template class string_thief<decltype(&std::string::_Mypair),
                            &std::string::_Mypair>;
#endif

void string_set_length_hacker(std::string &, std::size_t);

template <typename ch>
inline void resize(std::basic_string<ch> &raw_str, std::size_t sz) {
#if defined(_GLIBCXX_USE_CXX11_ABI)
  constexpr bool is_use_cxx11_abi = _GLIBCXX_USE_CXX11_ABI;
#else
  constexpr bool is_use_cxx11_abi = true;
#endif
  if constexpr (std::is_same_v<ch, char> == false &&
                is_use_cxx11_abi == false) {
    raw_str.resize(sz);
  }
  else {
#if defined(__SANITIZE_ADDRESS__) ||              \
    struct_pack_has_feature(address_sanitizer) || \
    (!defined(NDEBUG) && defined(_MSVC_STL_VERSION))
    raw_str.resize(sz);
#elif defined(__GLIBCXX__) || defined(_LIBCPP_VERSION) || \
    defined(_MSVC_STL_VERSION)
    if (sz > raw_str.capacity()) {
      raw_str.reserve(sz);
    }
    std::string &str = *reinterpret_cast<std::string *>(&raw_str);
    string_set_length_hacker(str, sz);
    *(raw_str.data() + sz) = 0;
#else
    raw_str.resize(sz);
#endif
  }
}

#endif

#if (defined(_MSC_VER) && _MSC_VER <= 1920)
#else
void vector_set_length_hacker(std::vector<char> &self, std::size_t sz);

template <typename Function, Function func_ptr>
class vector_thief {
 public:
  friend void vector_set_length_hacker(std::vector<char> &self,
                                       std::size_t sz) {
#if defined(_MSVC_STL_VERSION)
    (self.*func_ptr)._Myval2._Mylast = self.data() + sz;
#else
#if defined(_LIBCPP_VERSION)
#if _LIBCPP_VERSION < 14000
    ((*(std::__vector_base<char, std::allocator<char>> *)(&self)).*func_ptr) =
        self.data() + sz;
#else
    (self.*func_ptr) = self.data() + sz;
#endif
#else
#if defined(__GLIBCXX__)
    ((*(std::_Vector_base<char, std::allocator<char>> *)(&self)).*func_ptr)
        ._M_finish = self.data() + sz;
#endif
#endif
#endif
  }
};

#if defined(__GLIBCXX__)  // libstdc++
template class vector_thief<decltype(&std::vector<char>::_M_impl),
                            &std::vector<char>::_M_impl>;
#elif defined(_LIBCPP_VERSION)
template class vector_thief<decltype(&std::vector<char>::__end_),
                            &std::vector<char>::__end_>;
#elif defined(_MSVC_STL_VERSION)
template class vector_thief<decltype(&std::vector<char>::_Mypair),
                            &std::vector<char>::_Mypair>;
#endif

template <typename ch>
inline void resize(std::vector<ch> &raw_vec, std::size_t sz) {
#if defined(__SANITIZE_ADDRESS__) || struct_pack_has_feature(address_sanitizer)
  raw_vec.resize(sz);
#elif defined(__GLIBCXX__) ||                                     \
    (defined(_LIBCPP_VERSION) && defined(_LIBCPP_HAS_NO_ASAN)) || \
    defined(_MSVC_STL_VERSION)
  std::vector<char> &vec = *reinterpret_cast<std::vector<char> *>(&raw_vec);
  vec.reserve(sz * sizeof(ch));
  vector_set_length_hacker(vec, sz * sizeof(ch));
#else
  raw_vec.resize(sz);
#endif
}
#endif

#undef struct_pack_has_feature

template <typename T>
inline void resize(T &str, std::size_t sz) {
  str.resize(sz);
}

}  // namespace struct_pack::detail