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

#ifndef TENT_PLATFORM_MLU_H_
#define TENT_PLATFORM_MLU_H_

#include <mutex>
#include <unordered_map>

#include "tent/platform/cpu.h"

namespace mooncake::tent {
// Native Neuware device discovery and synchronous host/device copies.
class MluPlatform : public CpuPlatform {
   public:
    explicit MluPlatform(std::shared_ptr<Config> conf) : CpuPlatform(conf) {}
    Status probe(std::vector<Topology::NicEntry>& nics,
                 std::vector<Topology::MemEntry>& mems) override;
    Status allocate(void** ptr, size_t size, MemoryOptions& options) override;
    Status free(void* ptr, size_t size) override;
    Status copy(void* dst, void* src, size_t length) override;
    MemoryType getMemoryType(void* addr) override;
    const std::vector<RangeLocation> getLocation(
        void* start, size_t len, bool skip_prefault = false) override;
    Status exportDmabuf(void* addr, size_t length, DmabufExport& out) override;
    Status synchronizeDevices(const Topology* topology) override;
    // Same-host physical device identity, independent of visible ordinals.
    Status devicePciBusId(int device, std::string& identity);
    // Resolve through CNRT and cache successful lookups of physical devices.
    Status deviceByPciBusId(const std::string& identity, int& device);
    const std::string type() const override { return "mlu"; }

   private:
    // CNRT's visible device ordinals stay fixed for this platform's lifetime.
    std::mutex pci_devices_mutex_;
    std::unordered_map<std::string, int> pci_devices_;
};
}  // namespace mooncake::tent
#endif  // TENT_PLATFORM_MLU_H_
