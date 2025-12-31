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

#pragma once

#ifdef USE_HIP
#include <transport/hip_transport/hip_transport.h>
#define allocateFabricMemory(size) \
    mooncake::HipTransport::allocatePinnedLocalMemory(size)
#define freeFabricMemory(addr) \
    mooncake::HipTransport::freePinnedLocalMemory(addr)
#else
#include <transport/nvlink_transport/nvlink_transport.h>
#include <transport/nvlink_transport/intranode_nvlink_transport.h>

#define allocateFabricMemory_intra(size) \
    mooncake::IntraNodeNvlinkTransport::allocatePinnedLocalMemory(size)
#define allocateFabricMemory(size) \
    mooncake::NvlinkTransport::allocatePinnedLocalMemory(size)
#define freeFabricMemory_intra(addr) \
    mooncake::IntraNodeNvlinkTransport::freePinnedLocalMemory(addr)
#define freeFabricMemory(addr) \
    mooncake::NvlinkTransport::freePinnedLocalMemory(addr)
#endif
