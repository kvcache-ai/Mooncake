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

// New device memory types are appended last so the numeric values of the
// existing entries are preserved. TPU HBM is not NIC-addressable, so transfers
// touching it are staged through host DRAM by ProxyManager (see
// findStagingPolicy). Intel XPU VRAM is NIC-addressable only when the
// allocation can be exported as a dma-buf and registered with
// ibv_reg_dmabuf_mr (see Platform::exportDmabuf); otherwise it is staged the
// same way. Any device memory type added here must also be reported by
// isGpuMemoryType() below, so the selector's routing predicate and the staging
// capability checks stay in agreement.
enum MemoryType {
    MTYPE_UNKNOWN,
    MTYPE_CPU,
    MTYPE_CUDA,
    MTYPE_ROCM,
    MTYPE_TPU,
    MTYPE_XPU
};

// Single source of truth for "is this a device (GPU/NPU/TPU/XPU) memory type?".
// Both the selector's routing predicate and the staging capability checks
// consult this, so a device memory type can never be classified as a device on
// one path and as host on the other. Device types stage their device<->host hop
// through the matching transport (gpu_to_dram / dram_to_gpu) unless a network
// transport advertises direct device access (gpu_to_gpu) for them.
inline bool isGpuMemoryType(MemoryType t) {
    return t == MTYPE_CUDA || t == MTYPE_ROCM || t == MTYPE_TPU ||
           t == MTYPE_XPU;
}

// A device allocation exported as a Linux dma-buf so a NIC can register it
// with ibv_reg_dmabuf_mr. `fd` is owned by the exporting runtime and stays
// valid for the lifetime of the allocation; callers must not close it. Level
// Zero, for instance, caches one descriptor per allocation and returns that
// same fd on every export, so a caller-side close would leave the driver
// holding a stale number. The MR keeps its own reference to the dma-buf.
// `offset` is the position of the requested address within the exported
// buffer, which covers the whole underlying allocation, not just the
// requested range.
struct DmabufExport {
    int fd = -1;
    uint64_t offset = 0;
};

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

    // Make GPUDirect / device DMA visible on devices named in `topology`.
    // GPU platforms flush those devices; CPU and others no-op. Failures are
    // logged and still return OK so teardown can continue.
    virtual Status synchronizeDevices(const Topology *topology);

    // Export the device allocation containing [addr, addr+length) as a
    // dma-buf for direct NIC registration. Returns NotImplemented when the
    // platform has no dma-buf export path (the default), InvalidArgument when
    // `addr` is not device memory owned by this platform (register it as host
    // memory instead), and InternalError when the export itself failed. Only
    // an OK status populates `out`; per the DmabufExport contract the fd stays
    // owned by the exporting runtime and the caller must not close it.
    virtual Status exportDmabuf(void *addr, size_t length, DmabufExport &out);

   protected:
    static std::vector<int> topologyDeviceIndices(const Topology *topology,
                                                  Topology::MemType mem_type);
};

}  // namespace tent
}  // namespace mooncake

#endif  // PLATFORM_H
