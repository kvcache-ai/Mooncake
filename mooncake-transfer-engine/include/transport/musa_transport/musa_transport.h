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

#ifndef MUSA_TRANSPORT_H_
#define MUSA_TRANSPORT_H_

#include "transport/nvlink_transport/nvlink_transport.h"

namespace mooncake {

// MUSA's IPC context and copy-stream rules differ from CUDA's.  The actual
// transfer algorithm is shared with NvlinkTransport through its policy hook;
// this class only selects the MUSA runtime policy and exposes the allocator
// used by the GPU-vendor compatibility header.
class MusaTransport final : public NvlinkTransport {
   public:
    MusaTransport();
    ~MusaTransport() override = default;

    static void* allocatePinnedLocalMemory(size_t length);
    static void freePinnedLocalMemory(void* addr);
};

}  // namespace mooncake

#endif  // MUSA_TRANSPORT_H_
