// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef HASH_UTILS_H_
#define HASH_UTILS_H_

#include <cstddef>
#include <functional>
#include <utility>

namespace mooncake {

/**
 * @brief Hash functor for std::pair to be used with std::unordered_map
 *
 * This struct provides a hash function for pairs by combining the hashes
 * of both elements using XOR and bit shifting.
 */
struct PairHash {
    template <typename T1, typename T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        std::size_t h1 = std::hash<T1>{}(p.first);
        std::size_t h2 = std::hash<T2>{}(p.second);
        return h1 ^ (h2 << 1);
    }
};

}  // namespace mooncake

#endif  // HASH_UTILS_H_
