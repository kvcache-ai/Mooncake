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

// Append new memory types to preserve existing numeric values. Add device
// types to isGpuMemoryType() as well so routing and staging agree.
// Intel XPU VRAM is NIC-addressable only when exported as a dma-buf and
// registered with ibv_reg_dmabuf_mr (see Platform::exportDmabuf); otherwise
// transfers are staged through host DRAM.
enum MemoryType {
    MTYPE_UNKNOWN,
    MTYPE_CPU,
    MTYPE_CUDA,
    MTYPE_ROCM,
    MTYPE_TPU,
    MTYPE_XPU,
    MTYPE_MLU
};

// Shared by routing and staging. Copy capabilities depend on the transport.
inline bool isGpuMemoryType(MemoryType t) {
    return t == MTYPE_CUDA || t == MTYPE_ROCM || t == MTYPE_TPU ||
           t == MTYPE_XPU || t == MTYPE_MLU;
}

// A device allocation exported as a Linux dma-buf so a NIC can register it
// with ibv_reg_dmabuf_mr. Level Zero caches a borrowed descriptor per
// allocation; CNDrv returns a new descriptor that the caller closes after
// registration. The MR keeps its own reference to the dma-buf.
// `offset` is the position of the requested address within the exported
// buffer. The export must cover [offset, offset + requested length); the
// exporter may use the whole allocation or a page-aligned subrange.
struct DmabufExport {
    int fd = -1;
    uint64_t offset = 0;
    bool close_fd = false;
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
    // an OK status populates `out`. The caller closes the fd after registration
    // only when `out.close_fd` is true.
    virtual Status exportDmabuf(void *addr, size_t length, DmabufExport &out);

    // Page-aligned host memory that this platform's device copies reach by
    // DMA (pinned / page-locked), for staging buffers that are allocated once
    // and reused. Release it with freePinnedHost, never with free(). The
    // default returns ordinary page-aligned host memory, so platforms without
    // a pinned-host API still work, only without the faster copies.
    virtual Status allocatePinnedHost(void **pptr, size_t size);

    virtual Status freePinnedHost(void *ptr, size_t size);

   protected:
    static std::vector<int> topologyDeviceIndices(const Topology *topology,
                                                  Topology::MemType mem_type);
};

}  // namespace tent
}  // namespace mooncake

#endif  // PLATFORM_H
