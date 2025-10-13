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

#ifdef USE_DYNAMIC_LOADER
#include "v1/runtime/platform.h"
#include "v1/runtime/loader.h"
#include "v1/platform/cpu.h"
#include <glog/logging.h>

namespace mooncake {
namespace v1 {

Platform& Platform::getLoader(std::shared_ptr<ConfigManager> conf) {
    static std::shared_ptr<Platform> g_instance;
    if (!g_instance) {
        auto platform_name = conf->get("platform_name", "cuda");
        g_instance = Loader::instance().loadPlugin<Platform>(
            "platform", platform_name, conf.get());
        if (!g_instance) {
            g_instance = std::make_shared<CpuPlatform>(conf);
        }
    }
    return *g_instance;
}
}  // namespace v1
}  // namespace mooncake
#else
#include "v1/runtime/platform.h"

#ifdef USE_CUDA
#include "v1/platform/cuda.h"
#else
#include "v1/platform/cpu.h"
#endif

namespace mooncake {
namespace v1 {

Platform& Platform::getLoader(std::shared_ptr<ConfigManager> conf) {
    static std::shared_ptr<Platform> g_instance;
    if (!g_instance) {
#ifdef USE_CUDA
        g_instance = std::make_shared<CudaPlatform>(conf);
#else
        g_instance = std::make_shared<CpuPlatform>(conf);
#endif
    }
    return *g_instance;
}
}  // namespace v1
}  // namespace mooncake
#endif