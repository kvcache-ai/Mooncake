// Copyright 2026 KVCache.AI
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

#ifndef ASCEND_DEVICE_INDEX_H_
#define ASCEND_DEVICE_INDEX_H_

#include <cstdint>
#include <optional>

namespace mooncake {

// Map a physical device id across endpoints in reverse modulo order:
// index = S - 1 - (physical_device_id % S).
// Unlike formulas scaled by ACL's visible device count, this does not collapse
// to the last endpoint when a client process sees only one masked device via
// ASCEND_RT_VISIBLE_DEVICES.
inline std::optional<uint32_t> ReverseDeviceIndex(uint32_t physical_device_id,
                                                  uint32_t endpoint_count) {
    if (endpoint_count == 0) {
        return std::nullopt;
    }
    return endpoint_count - 1 - (physical_device_id % endpoint_count);
}

}  // namespace mooncake

#endif  // ASCEND_DEVICE_INDEX_H_
