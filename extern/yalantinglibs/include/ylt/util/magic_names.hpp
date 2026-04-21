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
 *
 * Author's Email: metabeyond@outlook.com
 * Author's Github: https://github.com/refvalue/
 * Description: this source file contains code for parsing function names from
 * their signatures, especially optimized for the MSVC implementation.
 */

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#if __has_include(<span>)
#include <span>

#include "meta_string.hpp"
#include "string_finder.hpp"
#endif

#include <string_view>
#include <type_traits>
#include <utility>

namespace refvalue {
namespace detail {
#if __has_include(<span>)
template <meta_string Signature>
struct parse_qualified_function_name {
#if defined(_MSC_VER) && defined(_WIN64)
  static constexpr std::array calling_conventions{"__cdecl", "__vectorcall"};
#elif defined(_MSC_VER) && defined(_WIN32)
  static constexpr std::array calling_conventions{
      "__cdecl", "__stdcall", "__fastcall", "__vectorcall", "__thiscall"};
#elif defined(__clang__) || defined(__GNUC__)
  static constexpr std::array<const char*, 0> calling_conventions{};
#else
#error "Unsupported platform."
#endif

  static constexpr std::string_view view{Signature};
  static constexpr auto depths = [] {
    constexpr std::string_view left_tokens{"<(["};
    constexpr std::string_view right_tokens{">)]"};
    std::array<std::size_t, view.size()> result{};

    for (std::size_t i = 0, j = 0; i < view.size(); i++) {
      if (left_tokens.find(view[i]) != std::string_view::npos) {
        j++;
      }

      if (right_tokens.find(view[i]) != std::string_view::npos && j != 0) {
        j--;
      }

      result[i] = j;
    }

    return result;
  }();

  template <find_mode_type Mode>
  static constexpr std::size_t find_significant_index(
      std::string_view keyword, std::size_t depth = 0,
      std::size_t index = string_finder_traits<Mode>::default_index) {
    for (auto i = uniform_find_string<Mode>(view, keyword, index);
         i < view.size(); i = uniform_find_string<Mode>(
                              view, keyword, skip_keyword<Mode>(i, keyword))) {
      if (depths[i] == depth) {
        return skip_keyword<Mode>(i, keyword);
      }
    }

    return std::string_view::npos;
  }

  static constexpr auto value = [] {
    // When using MSVC, Finds the start index of the function name past the
    // top-level calling convention token (e.g. __cdecl/__vectorcall for x64,
    // __stdcall/__cdecl/__fastcall/__vectorcall/__thiscall for x86).
    // When using GCC or Clang, returns zero immediately.
    constexpr auto start_index = []() -> std::size_t {
      // Workaround for failing compilation on x86-64 Clang assertions trunk.
      // std::array<T, 0>::begin() and std::array<T, 0>::end() are not
      // odr-usable.
      if constexpr (!calling_conventions.empty()) {
        for (auto&& item : calling_conventions) {
          if (auto index =
                  find_significant_index<find_mode_type::full_match>(item);
              index != std::string_view::npos) {
            return index + 1;
          }
        }
      }

      return 0;
    }();

    // Finds the end index of the function name before the top-level left
    // "<" or "(" indicating the start of the template or function arguments.
    constexpr auto function_name_start_index = (std::max)(
        start_index,
        find_significant_index<find_mode_type::full_match_reverse>("::"));

    constexpr auto end_index = find_significant_index<find_mode_type::any_of>(
        "<(", 1,
        function_name_start_index != std::string_view::npos
            ? function_name_start_index
            : start_index);

    constexpr auto final_size = end_index != std::string_view::npos
                                    ? (end_index - 1 - start_index)
                                    : (view.size() - start_index);

    return meta_string{std::span<const char, final_size>{
        view.data() + start_index, final_size}};
  }();
};

template <meta_string Signature>
inline constexpr auto&& parse_qualified_function_name_v =
    parse_qualified_function_name<Signature>::value;
#endif

template <auto Func>
constexpr auto qualified_name_of_impl() noexcept {
#ifdef _MSC_VER
  constexpr std::size_t suffix_size{16};
  constexpr std::string_view keyword{
      "refvalue::detail::qualified_name_of_impl<"};
  constexpr std::string_view signature{__FUNCSIG__};
  constexpr std::string_view anonymous_namespace{"`anonymous-namespace'::"};
#elif defined(__clang__)
  constexpr std::size_t suffix_size{1};
  constexpr std::string_view keyword{"[Func = "};
  constexpr std::string_view signature{__PRETTY_FUNCTION__};
  constexpr std::string_view anonymous_namespace{"(anonymous namespace)::"};
#elif defined(__GNUC__)
  constexpr std::size_t suffix_size{1};
  constexpr std::string_view keyword{"Func = "};
  constexpr std::string_view signature{__PRETTY_FUNCTION__};
  constexpr std::string_view anonymous_namespace{"{anonymous}::"};
#else
#error "Unsupported compiler."
#endif
  // Skips the possible '&' token for GCC and Clang.
  constexpr auto prefix_size = signature.find(keyword) + keyword.size();
  constexpr auto additional_size = signature[prefix_size] == '&' ? 1 : 0;
  constexpr auto intermediate = signature.substr(
      prefix_size + additional_size,
      signature.size() - prefix_size - additional_size - suffix_size);

  constexpr std::string_view result = intermediate;
  constexpr size_t rpos = result.rfind(anonymous_namespace);
  if constexpr (rpos != std::string_view::npos) {
    constexpr std::string_view str =
        result.substr(rpos + anonymous_namespace.size());
    constexpr size_t right = str.find('(');
    if constexpr (right != std::string_view::npos) {
      return str.substr(0, right);
    }
    else {
      return str;
    }
  }
  else {
    constexpr size_t left = result.find("l ") + 2;
    constexpr size_t right = result.find('(');
    if constexpr (left != std::string_view::npos) {
      if constexpr (right != std::string_view::npos) {
        return result.substr(left, right - left);
      }
      else {
        return result;
      }
    }
    else {
      return result;
    }
  }
}
}  // namespace detail

template <auto Func>
struct qualified_name_of {
  static constexpr auto value = detail::qualified_name_of_impl<Func>();
};

template <auto Func>
inline constexpr auto&& qualified_name_of_v = qualified_name_of<Func>::value;

#if __has_include(<span>)
template <auto Func>
struct name_of {
  static constexpr auto value = [] {
    // a::b::c::function_name<nested1::nested2::nested3::class_name<...>>
    constexpr std::string_view qualified_name{qualified_name_of_v<Func>};
    constexpr auto template_argument_start_token_index =
        qualified_name.find_first_of('<');
    constexpr auto scope_token_index =
        qualified_name.rfind("::", template_argument_start_token_index);
    constexpr auto class_name_index =
        scope_token_index != std::string_view::npos ? scope_token_index + 2 : 0;
    constexpr auto result = qualified_name.substr(class_name_index);

    return meta_string{
        std::span<const char, result.size()>{result.data(), result.size()}};
  }();
};

template <auto Func>
inline constexpr auto&& name_of_v = name_of<Func>::value;
#endif
}  // namespace refvalue
