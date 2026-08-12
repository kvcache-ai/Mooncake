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

#ifndef PLATFORM_H
#define PLATFORM_H

#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {

// MTYPE_TPU is appended last so the numeric values of the existing entries are
// preserved. TPU HBM is not NIC-addressable, so transfers touching it are
// staged through host DRAM by capability path synthesis and ProxyManager.
enum MemoryType { MTYPE_UNKNOWN, MTYPE_CPU, MTYPE_CUDA, MTYPE_ROCM, MTYPE_TPU };

inline bool isKnownMemoryType(MemoryType type) {
    return type != MTYPE_UNKNOWN;
}

inline bool isDeviceMemoryType(MemoryType type) {
    return type == MTYPE_CUDA || type == MTYPE_ROCM || type == MTYPE_TPU;
}

inline const char *memoryTypeName(MemoryType type) {
    switch (type) {
        case MTYPE_CPU:
            return "cpu";
        case MTYPE_CUDA:
            return "cuda";
        case MTYPE_ROCM:
            return "rocm";
        case MTYPE_TPU:
            return "tpu";
        case MTYPE_UNKNOWN:
            return "unknown";
    }
    return "unknown";
}

class Platform {
   public:
    static Platform &getLoader(std::shared_ptr<Config> conf = nullptr);

    Platform() {}

    virtual ~Platform() {}

    virtual Status probe(std::vector<Topology::NicEntry> &nic_list,
                         std::vector<Topology::MemEntry> &mem_list) = 0;

    virtual Status allocate(void **pptr, size_t size,
                            MemoryOptions &options) = 0;

    virtual Status free(void *ptr, size_t size) = 0;

    virtual Status copy(void *dst, void *src, size_t length) = 0;

    virtual MemoryType getMemoryType(void *addr) = 0;

    virtual const std::vector<RangeLocation> getLocation(
        void *start, size_t len, bool skip_prefault = false) = 0;

    virtual const std::string type() const = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // PLATFORM_H
