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

#ifndef TENT_DEVICE_NVLINK_H_
#define TENT_DEVICE_NVLINK_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"
#include "tent/runtime/device_transport.h"

namespace mooncake {
namespace tent {

// Opaque CUDA IPC memory handle represented as 32-bit words so callers can
// exchange it through language bindings without including cuda_runtime.h.
struct NvLinkIpcHandle {
    std::vector<int32_t> words;
};

/// NVLink-specific DeviceTransport implementation.
///
/// Inherits the unified DeviceTransport interface.  P2P methods have real
/// implementations here; RDMA methods (initializeRdmaDevice, registerMemory,
/// createQueuePairs, connectRdmaPeers) return Status::NotSupported() or
/// no-op defaults.
class NvLinkDeviceTransport : public DeviceTransport {
   public:
    ~NvLinkDeviceTransport() override = default;
};

/// Factory: create an NVLink DeviceTransport.
std::unique_ptr<NvLinkDeviceTransport> createNvLinkDeviceTransport();

/// Runtime probe for CUDA fabric memory handles used by MNNVL-style NVLink
/// fabrics.  This keeps environment parsing and CUDA attribute checks in TENT
/// rather than duplicating them in EP/PG.
bool nvLinkSupportsFabricMemory();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_NVLINK_H_
