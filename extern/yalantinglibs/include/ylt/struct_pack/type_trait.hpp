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
#include "reflection.hpp"
#include "type_calculate.hpp"

#if __cplusplus >= 202002L
#include "tuple.hpp"
#endif

namespace struct_pack::detail {
template <typename T>
constexpr bool struct_pack_byte =
    std::is_same_v<char, T> || std::is_same_v<unsigned char, T> ||
    std::is_same_v<signed char, T> || std::is_same_v<std::byte, T>;

template <typename T>
#if __cpp_concepts < 201907L
constexpr bool
#else
concept
#endif

    struct_pack_buffer = trivially_copyable_container<T> &&
    struct_pack_byte<typename T::value_type>;

#if __cplusplus >= 202002L
template <typename... T>
constexpr inline bool is_trivial_tuple<tuplet::tuple<T...>> = true;
#endif

template <typename Reader>
struct MD5_reader_wrapper;

template <typename T>
constexpr bool is_MD5_reader_wrapper = false;

template <typename T>
constexpr bool is_MD5_reader_wrapper<MD5_reader_wrapper<T>> = true;

}  // namespace struct_pack::detail