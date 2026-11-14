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

#include <acl/acl.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

#include <glog/logging.h>

namespace mooncake {

// Split client ranks evenly across endpoints, in reverse endpoint order.
// index = S - 1 - floor(r * S / C).
inline std::optional<uint32_t> ReverseDeviceIndex(uint32_t client_rank,
                                                  uint32_t client_count,
                                                  uint32_t endpoint_count) {
    if (client_count == 0 || endpoint_count == 0 ||
        client_rank >= client_count) {
        return std::nullopt;
    }
    const uint64_t group =
        static_cast<uint64_t>(client_rank) * endpoint_count / client_count;
    return endpoint_count - 1 - static_cast<uint32_t>(group);
}

// ACL view of this process. aclrtGetDeviceCount / aclrtGetPhyDevIdByLogicDevId
// follow ASCEND_RT_VISIBLE_DEVICES: with no mask this is the full ACL device
// set, and with a mask it is only the visible set. It is not a host-wide count
// of devices hidden from this process. Returned ids are sorted and unique; C
// is that set's size and r is a physical id's position in it. Empty means the
// query failed and a multi-candidate route must fail instead of guessing.
inline std::vector<uint32_t> GetAscendPhysicalDeviceIds() {
    uint32_t count = 0;
    if (aclrtGetDeviceCount(&count) != ACL_ERROR_NONE || count == 0) {
        LOG(ERROR) << "Cannot query ACL device count; a masked ACL view is "
                      "not the hidden host device total";
        return {};
    }
    std::vector<uint32_t> ids;
    ids.reserve(count);
    for (uint32_t logic = 0; logic < count; ++logic) {
        int32_t physical = -1;
        if (aclrtGetPhyDevIdByLogicDevId(static_cast<int32_t>(logic),
                                         &physical) != ACL_ERROR_NONE ||
            physical < 0) {
            LOG(ERROR) << "Cannot resolve ACL logic device " << logic
                       << " to a physical id";
            return {};
        }
        ids.push_back(static_cast<uint32_t>(physical));
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids;
}

}  // namespace mooncake

#endif  // ASCEND_DEVICE_INDEX_H_
