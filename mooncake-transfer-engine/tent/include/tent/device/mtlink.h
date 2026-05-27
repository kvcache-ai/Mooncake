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

#ifndef TENT_DEVICE_MTLINK_H_
#define TENT_DEVICE_MTLINK_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"

namespace mooncake {
namespace tent {

// Opaque MUSA IPC memory handle represented as 32-bit words so callers can
// exchange it through language bindings without including musa_runtime.h.
struct MtLinkIpcHandle {
    std::vector<int32_t> words;
};

class MtLinkDeviceTransport {
   public:
    virtual ~MtLinkDeviceTransport() = default;

    virtual Status allocatePeerAccessTables(int rank, int num_ranks) = 0;

    // Export an IPC handle for a musaMalloc-style local buffer.
    // device_id is needed to call musaSetDevice before musaIpcGetMemHandle.
    virtual Status exportIpcHandle(int device_id, void* local_buffer,
                                   MtLinkIpcHandle& handle) = 0;

    virtual Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<MtLinkIpcHandle>& remote_handles,
        const std::vector<int>& active_ranks_mask) = 0;

    virtual MtLinkDeviceContext deviceContext() const = 0;
    virtual bool allPeersAccessible() const = 0;
    virtual void** hostPeerPtrs() const = 0;
};

std::unique_ptr<MtLinkDeviceTransport> createMtLinkDeviceTransport();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_MTLINK_H_
