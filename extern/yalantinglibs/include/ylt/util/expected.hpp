/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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
#include "tl/expected.hpp"
namespace ylt {
#if __cpp_lib_expected >= 202202L && __cplusplus > 202002L
template <class T, class E>
using expected = std::expected<T, E>;

template <class T>
using unexpected = std::unexpected<T>;

using unexpect_t = std::unexpect_t;

#else
template <class T, class E>
using expected = tl::expected<T, E>;

template <class T>
using unexpected = tl::unexpected<T>;

using unexpect_t = tl::unexpect_t;
#endif

static constexpr unexpect_t unexpect;

}  // namespace ylt
// decrepeted, instead by ylt::expected
namespace coro_rpc {
template <class T, class E>
using expected = ylt::expected<T, E>;

template <class T>
using unexpected = ylt::unexpected<T>;

using unexpect_t = ylt::unexpect_t;
}  // namespace coro_rpc
namespace struct_pack {
template <class T, class E>
using expected = ylt::expected<T, E>;

template <class T>
using unexpected = ylt::unexpected<T>;

using unexpect_t = ylt::unexpect_t;
}  // namespace struct_pack