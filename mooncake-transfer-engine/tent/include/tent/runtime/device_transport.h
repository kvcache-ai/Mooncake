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

#ifndef TENT_RUNTIME_DEVICE_TRANSPORT_H_
#define TENT_RUNTIME_DEVICE_TRANSPORT_H_

#include <cstddef>
#include <cstdint>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"

namespace mooncake {
namespace tent {

class DeviceTransport {
   public:
    virtual ~DeviceTransport() = default;

    virtual Status symAlloc(void** ptr, size_t size) = 0;

    virtual Status symFree(void* ptr) = 0;

    virtual void* getRemotePtr(void* local_ptr, int dst_rank) = 0;

    virtual Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                                  uint32_t& rkey) = 0;

    virtual Status unregisterMemory(void* ptr) = 0;

    virtual Status getChannelResources(int channel_id,
                                       DeviceChannelResources& resources) = 0;

    virtual Status getPeerInfo(int rank, DevicePeerInfo& peer) = 0;

    virtual Status connect(int rank) = 0;

    virtual Status barrier() = 0;

    virtual const DeviceCommCapabilities deviceCapabilities() const = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_DEVICE_TRANSPORT_H_
