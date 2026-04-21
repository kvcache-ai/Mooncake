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
#include <string_view>

#include "magic_names.hpp"

template <size_t N>
constexpr std::string_view string_view_array_has(
    const std::array<std::string_view, N>& array, std::string_view value) {
  for (const auto& v : array) {
    if (value.find(v) == 0)
      return v;
  }
  return std::string_view{""};
}

namespace coro_rpc {
template <auto func>
constexpr std::string_view get_func_name() {
  constexpr std::array func_style_array{
      std::string_view{"__cdecl "},    std::string_view{"__clrcall "},
      std::string_view{"__stdcall "},  std::string_view{"__fastcall "},
      std::string_view{"__thiscall "}, std::string_view{"__vectorcall "}};
  constexpr auto qualified_name =
      std::string_view{refvalue::qualified_name_of_v<func>};
  constexpr auto func_style =
      string_view_array_has(func_style_array, qualified_name);
  if constexpr (func_style.length() > 0) {
    return std::string_view{qualified_name.data() + func_style.length(),
                            qualified_name.length() - func_style.length()};
  }
  return qualified_name;
};
}  // namespace coro_rpc
