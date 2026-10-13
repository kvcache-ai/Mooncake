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

#include "tpu_token_registry.h"

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

namespace mooncake {
namespace tent {

void tpuAdapterLog(const std::string &message) {
    std::fprintf(stderr, "[mooncake-tpu-adapter] %s\n", message.c_str());
}

TpuTokenRegistry::~TpuTokenRegistry() {
    for (auto &[token, entry] : regions_) {
        munmap(reinterpret_cast<void *>(token), entry.reserved_len);
    }
}

TpuTokenRegistry &TpuTokenRegistry::instance() {
    // Leaked on purpose: TENT may call into the adapter from its own static
    // destructors, after this library's static objects would otherwise be
    // destroyed.
    static TpuTokenRegistry *registry = new TpuTokenRegistry();
    return *registry;
}

uintptr_t TpuTokenRegistry::add(std::unique_ptr<DeviceRegion> region) {
    if (!region) {
        tpuAdapterLog("cannot register a null region");
        return 0;
    }
    const size_t size = region->size();
    const int device = region->deviceIndex();
    if (size == 0 || device < 0) {
        tpuAdapterLog("refusing region with size=" + std::to_string(size) +
                      " device=" + std::to_string(device));
        return 0;
    }

    const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    if (size > SIZE_MAX - (page - 1)) {
        tpuAdapterLog("region size " + std::to_string(size) +
                      " overflows page rounding");
        return 0;
    }
    const size_t reserved_len = (size + page - 1) / page * page;

    // PROT_NONE + MAP_NORESERVE costs address space only, never memory, and
    // faults on any access.
    void *base = mmap(nullptr, reserved_len, PROT_NONE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (base == MAP_FAILED) {
        tpuAdapterLog(
            "reserving " + std::to_string(reserved_len) +
            " bytes of address space failed: " + std::strerror(errno));
        return 0;
    }

    const int numa = region->numaNode();
    const auto token = reinterpret_cast<uintptr_t>(base);
    std::unique_lock lock(mutex_);
    regions_[token] = Entry{size, reserved_len,
                            std::shared_ptr<DeviceRegion>(std::move(region))};
    auto [it, inserted] = device_numa_.emplace(device, numa);
    if (!inserted && it->second < 0) it->second = numa;
    return token;
}

bool TpuTokenRegistry::remove(uintptr_t token) {
    Entry entry;
    {
        std::unique_lock lock(mutex_);
        auto it = regions_.find(token);
        if (it == regions_.end()) return false;
        entry = std::move(it->second);
        regions_.erase(it);
    }
    // Release the address range only once it is out of the map, so a lookup
    // can never resolve an address that has been returned to the kernel.
    munmap(reinterpret_cast<void *>(token), entry.reserved_len);
    return true;
}

std::shared_ptr<DeviceRegion> TpuTokenRegistry::resolve(const void *addr,
                                                        size_t len,
                                                        size_t *offset) const {
    const auto a = reinterpret_cast<uintptr_t>(addr);
    std::shared_lock lock(mutex_);
    auto it = regions_.upper_bound(a);
    if (it == regions_.begin()) return nullptr;
    --it;
    const size_t off = a - it->first;
    const size_t size = it->second.size;
    if (off >= size) return nullptr;
    if (len > size - off) return nullptr;  // would run past the region's end
    if (offset) *offset = off;
    return it->second.region;
}

bool TpuTokenRegistry::contains(const void *addr) const {
    return resolve(addr, 0, nullptr) != nullptr;
}

int TpuTokenRegistry::deviceIndex(const void *addr) const {
    auto region = resolve(addr, 0, nullptr);
    return region ? region->deviceIndex() : -1;
}

int TpuTokenRegistry::copyD2H(void *host_dst, const void *device_src,
                              size_t len) const {
    if (!host_dst || !device_src) return 1;
    size_t offset = 0;
    auto region = resolve(device_src, len, &offset);
    if (!region) {
        tpuAdapterLog("D2H source range of " + std::to_string(len) +
                      " bytes is not inside one registered TPU buffer");
        return 1;
    }
    if (len == 0) return 0;
    return region->copyToHost(host_dst, offset, len) ? 0 : 1;
}

int TpuTokenRegistry::copyH2D(void *device_dst, const void *host_src,
                              size_t len) const {
    if (!device_dst || !host_src) return 1;
    size_t offset = 0;
    auto region = resolve(device_dst, len, &offset);
    if (!region) {
        tpuAdapterLog("H2D destination range of " + std::to_string(len) +
                      " bytes is not inside one registered TPU buffer");
        return 1;
    }
    if (len == 0) return 0;
    return region->copyFromHost(host_src, offset, len) ? 0 : 1;
}

int TpuTokenRegistry::deviceCount() const {
    std::shared_lock lock(mutex_);
    return device_numa_.empty() ? 0 : device_numa_.rbegin()->first + 1;
}

int TpuTokenRegistry::deviceNuma(int index) const {
    std::shared_lock lock(mutex_);
    auto it = device_numa_.find(index);
    return it == device_numa_.end() ? -1 : it->second;
}

size_t TpuTokenRegistry::regionSize(uintptr_t token) const {
    std::shared_lock lock(mutex_);
    auto it = regions_.find(token);
    return it == regions_.end() ? 0 : it->second.size;
}

size_t TpuTokenRegistry::size() const {
    std::shared_lock lock(mutex_);
    return regions_.size();
}

}  // namespace tent
}  // namespace mooncake
