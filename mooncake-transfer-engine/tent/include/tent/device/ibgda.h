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

#ifndef TENT_DEVICE_IBGDA_H_
#define TENT_DEVICE_IBGDA_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"
#include "tent/runtime/device_transport.h"

namespace mooncake {
namespace tent {

// IbGdaLocalMetadata is now defined in device_resources.h (shared by all
// DeviceTransport implementations).  This header re-exports it for backward
// compatibility.
using IbGdaLocalMetadata [[deprecated("Use tent::IbGdaLocalMetadata from "
                                     "tent/runtime/device_resources.h")]] =
    IbGdaLocalMetadata;

/// IBGDA-specific DeviceTransport implementation.
///
/// Inherits the unified DeviceTransport interface.  The base class methods
/// that are RDMA-specific have real implementations here; P2P methods
/// (allocatePeerAccessTables, exportIpcHandle, configurePeers) return
/// Status::NotSupported().
///
/// This class also exposes IBGDA-specific convenience methods that are not
/// part of the base DeviceTransport interface.
class IbGdaDeviceTransport : public DeviceTransport {
   public:
    ~IbGdaDeviceTransport() override = default;

    // IBGDA-specific convenience methods (not in base DeviceTransport).
    // These are kept for callers that know they have an IBGDA transport
    // and want type-safe access.

    /// Size of the control buffer allocation.
    virtual size_t controlBufferSize() const = 0;
};

/// Factory: create an IBGDA DeviceTransport.
/// Returns nullptr if IBGDA is not available on this platform.
std::unique_ptr<IbGdaDeviceTransport> createIbGdaDeviceTransport();

/// Size of one GPU-visible IBGDA queue-pair context entry.  Callers use
/// this to allocate the opaque qp_devctxs array without depending on mlx5
/// detail types.
size_t ibGdaQueuePairDeviceContextSize();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_IBGDA_H_
