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
#include <algorithm>
namespace struct_pack::detail {
struct size_info {
  std::size_t total;
  std::size_t size_cnt;
  std::size_t max_size;
  constexpr size_info &operator+=(const size_info &other) {
    this->total += other.total;
    this->size_cnt += other.size_cnt;
    this->max_size = (std::max)(this->max_size, other.max_size);
    return *this;
  }
  constexpr size_info operator+(const size_info &other) {
    return {this->total + other.total, this->size_cnt + other.size_cnt,
            (std::max)(this->max_size, other.max_size)};
  }
};
}  // namespace struct_pack::detail