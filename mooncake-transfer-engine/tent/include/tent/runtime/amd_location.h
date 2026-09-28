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

// Self-contained AMD GPU location-prefix helpers, shared by the TENT
// runtime (topology.h) and the out-of-tree rocm device plugin, which
// cannot pull in the full runtime headers.
#ifndef TENT_RUNTIME_AMD_LOCATION_H
#define TENT_RUNTIME_AMD_LOCATION_H

#include <string>

namespace mooncake {
namespace tent {

// Canonical AMD GPU location type — the component before ':' in location
// strings such as "hip:0" (no trailing colon in the constant itself).
// Matches classic TE GPU_PREFIX ("hip:") so location strings and custom NIC
// matrices can be shared across backends.
// "rocm" remains a parse-only alias for existing TENT configs.
inline constexpr const char kAmdGpuLocationType[] = "hip";
inline constexpr const char kAmdGpuLocationTypeAlias[] = "rocm";

inline bool isAmdGpuLocationType(const std::string& type) {
    return type == kAmdGpuLocationType || type == kAmdGpuLocationTypeAlias;
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_AMD_LOCATION_H
