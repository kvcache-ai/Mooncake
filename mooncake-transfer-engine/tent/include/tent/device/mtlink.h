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
#include "tent/runtime/device_transport.h"

namespace mooncake {
namespace tent {

// Opaque MUSA IPC memory handle represented as 32-bit words so callers can
// exchange it through language bindings without including musa_runtime.h.
struct MtLinkIpcHandle {
    std::vector<int32_t> words;
};

/// MTLink-specific DeviceTransport implementation.
///
/// Inherits the unified DeviceTransport interface.  P2P methods have real
/// implementations here; RDMA methods (initializeRdmaDevice, registerMemory,
/// createQueuePairs, connectRdmaPeers) return Status::NotSupported() or
/// no-op defaults.
class MtLinkDeviceTransport : public DeviceTransport {
   public:
    ~MtLinkDeviceTransport() override = default;
};

/// Factory: create an MTLink DeviceTransport.
std::unique_ptr<MtLinkDeviceTransport> createMtLinkDeviceTransport();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_MTLINK_H_
