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

// Transfer-engine side of the ascend_allocator.so config bridge. The allocator
// library must not reference globalConfig() (a TENT-only consumer loads it
// without the transfer engine), so the engine registers callbacks that read
// the process-global config live. A store that flips ascend_agent_mode /
// ascend_use_fabric_mem after init is therefore visible to the next
// allocation. Nothing in this file is compiled unless the allocator library
// is part of the build.

#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)

#include "ascend_allocator.h"

#include "config.h"

namespace mooncake {
namespace {

bool AllocatorAgentMode() { return globalConfig().ascend_agent_mode; }

bool AllocatorUseFabricMem() { return globalConfig().ascend_use_fabric_mem; }

// transfer_engine is a shared library in the direct/UBSHMEM configurations, so
// this constructor runs when the engine is loaded, before any allocation.
struct AscendAllocatorConfigBridge {
    AscendAllocatorConfigBridge() {
        ascend_allocator_set_config_provider(&AllocatorAgentMode,
                                             &AllocatorUseFabricMem);
    }
};

AscendAllocatorConfigBridge g_ascend_allocator_config_bridge;

}  // namespace
}  // namespace mooncake

#endif  // USE_ASCEND_DIRECT || USE_UBSHMEM
