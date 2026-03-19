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

#ifndef TENT_MEMORY_PROBER_H
#define TENT_MEMORY_PROBER_H

#include "tent/common/status.h"
#include "tent/runtime/topology.h"
#include "tent/device_plugin.h"

#include <string>
#include <vector>
#include <mutex>
#include <filesystem>
#include <dlfcn.h>

namespace mooncake {
namespace tent {

class MemoryProber {
   public:
    struct LoadedPlugin {
        void* so_handle = nullptr;
        device_plugin_t iface{};
        void* ctx = nullptr;
        std::string path;
    };

    static MemoryProber& Instance() {
        static MemoryProber inst;
        return inst;
    }

    Status loadPlugins(const std::string& path);

    void unloadPlugins();

    Status listPlugins(std::vector<std::string>& plugins);

    Status alloc(void** pptr, size_t size, const std::string& location);

    Status free(void* ptr, size_t size);

    Status memcpy(void* dst, void* src, size_t size);

    const std::vector<RangeLocation> locate(void* addr, size_t size,
                                            bool skip_prefault = false);

    std::string type(void* addr);

    Status probe(const std::vector<Topology::NicEntry>& nic_list,
                 std::vector<Topology::MemEntry>& mem_list);

   private:
    void probeDeviceMemory(MemoryProber::LoadedPlugin& plugin,
                           const std::vector<Topology::NicEntry>& nic_list,
                           std::vector<Topology::MemEntry>& mem_list);

   private:
    MemoryProber() = default;
    ~MemoryProber() { unloadPlugins(); }

    MemoryProber(const MemoryProber&) = delete;
    MemoryProber& operator=(const MemoryProber&) = delete;

    std::vector<LoadedPlugin> plugins_;

    std::mutex mu_;
    std::unordered_map<void*, std::string> memory_type_map_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_MEMORY_PROBER_H