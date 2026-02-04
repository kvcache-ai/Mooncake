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

#ifdef USE_CUDA
#include "tent/platform/cuda.h"
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
#include "tent/platform/ascend.h"
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
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
        g_instance = std::make_shared<AscendPlatform>(conf);
#else
        g_instance = std::make_shared<CpuPlatform>(conf);
#endif
    });
    return *g_instance;
}
}  // namespace tent
}  // namespace mooncake
