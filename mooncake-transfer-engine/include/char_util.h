// Copyright 2024 KVCache.AI
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

#ifndef CHAR_UTIL_H
#define CHAR_UTIL_H

#include <cctype>

namespace mooncake {

// Lowercase a single byte safely. Passing a (possibly signed) char straight
// to std::tolower is UB when the byte is > 0x7F; the argument must be
// representable as unsigned char or equal EOF.
static inline char to_lower(char c) {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

}  // namespace mooncake

#endif  // CHAR_UTIL_H
