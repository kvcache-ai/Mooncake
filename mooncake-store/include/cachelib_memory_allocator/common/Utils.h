/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

namespace facebook {
namespace cachelib {
namespace util {

// @return size aligned up to the next multiple of _alignment_
template <typename T>
std::enable_if_t<std::is_arithmetic<T>::value, T> getAlignedSize(
    T size, uint32_t alignment) {
  const T rem = size % alignment;
  return rem == 0 ? size : size + alignment - rem;
}

} // namespace util
} // namespace cachelib
} // namespace facebook
