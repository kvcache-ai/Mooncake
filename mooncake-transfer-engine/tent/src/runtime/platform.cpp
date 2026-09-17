// Copyright 2024 KVCache.AI
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

#include "tent/runtime/platform.h"

#include <mutex>
#include <unordered_set>
#include <vector>

#ifdef USE_CUDA
#include "tent/platform/cuda.h"
#elif defined(USE_HIP)
#include "tent/platform/rocm.h"
#elif defined(USE_SUNRISE)
#include "tent/platform/sunrise.h"
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
#include "tent/platform/ascend.h"
#elif defined(USE_TPU)
#include "tent/platform/tpu.h"
#else
#include "tent/platform/cpu.h"
#endif

namespace mooncake {
namespace tent {

Platform& Platform::getLoader(std::shared_ptr<Config> conf) {
    static std::shared_ptr<Platform> g_instance;
    static std::once_flag flag;
    std::call_once(flag, [&]() {
#ifdef USE_CUDA
        g_instance = std::make_shared<CudaPlatform>(conf);
#elif defined(USE_HIP)
        g_instance = std::make_shared<RocmPlatform>(conf);
#elif defined(USE_SUNRISE)
        g_instance = std::make_shared<SunrisePlatform>(conf);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
        g_instance = std::make_shared<AscendPlatform>(conf);
#elif defined(USE_TPU)
        g_instance = std::make_shared<TpuPlatform>(conf);
#else
        g_instance = std::make_shared<CpuPlatform>(conf);
#endif
    });
    return *g_instance;
}

Status Platform::synchronizeDevices(const Topology* topology) {
    (void)topology;
    return Status::OK();
}

std::vector<int> Platform::topologyDeviceIndices(const Topology* topology,
                                                 Topology::MemType mem_type) {
    std::vector<int> devices;
    if (!topology) return devices;

    std::unordered_set<int> seen;
    const size_t mem_count = topology->getMemCount();
    for (size_t i = 0; i < mem_count; ++i) {
        const auto* mem =
            topology->getMemEntry(static_cast<Topology::MemID>(i));
        if (!mem || mem->type != mem_type) continue;
        LocationParser parser(mem->name);
        const int device = parser.index();
        if (device < 0 || !seen.insert(device).second) continue;
        devices.push_back(device);
    }
    return devices;
}
}  // namespace tent
}  // namespace mooncake
