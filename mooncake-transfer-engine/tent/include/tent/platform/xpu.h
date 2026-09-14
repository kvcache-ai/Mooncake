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

#ifndef TENT_PLATFORM_XPU_H_
#define TENT_PLATFORM_XPU_H_

#include "tent/common/config.h"
#include "tent/platform/cpu.h"
#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {

// XpuPlatform models an Intel XPU host: the host DRAM and RDMA topology are
// identical to CpuPlatform, so those paths (host allocation, NUMA probing,
// host<->host copy) are inherited unchanged. Only the XPU-device-aware
// operations are overridden, and every one of them is delegated to the SYCL
// backend (XpuSyclBackend). That backend links libsycl directly, so the XPU
// platform sources are compiled with the Intel DPC++ compiler (icpx); the SYCL
// dependency is confined to platform_xpu and never leaks into this header.
//
// In the MVP, XPU VRAM cannot be reached by the NIC, so there is no direct
// device transport. This platform provides the device-side building blocks:
// USM allocation ("xpu:N"), pointer classification, and the VRAM<->host copy()
// primitive. Wiring these into the transport-layer staging path (so getTypeEnum
// / isGpuType / findStagingPolicy let ProxyManager chain the VRAM<->host and
// host<->host hops end-to-end over the network) is not yet complete and is
// tracked separately; today "xpu:N" memory is classified and copied locally but
// is not routed over the network. Direct VRAM RDMA (dma-buf) and PCIe P2P are
// future work.
class XpuPlatform : public CpuPlatform {
   public:
    explicit XpuPlatform(std::shared_ptr<Config> config)
        : CpuPlatform(config) {}

    ~XpuPlatform() override {}

    // Host + RDMA discovery from CpuPlatform, plus one MemEntry per XPU device
    // ("xpu:N") so findNearMem() can resolve a device to its nearest host node.
    Status probe(std::vector<Topology::NicEntry> &nic_list,
                 std::vector<Topology::MemEntry> &mem_list) override;

    // Device USM allocation ("xpu:N") via sycl::malloc_device; host allocation
    // is inherited from CpuPlatform.
    Status allocate(void **pptr, size_t size, MemoryOptions &options) override;

    // Routes device pointers to the SYCL backend's free; host pointers use the
    // inherited host deallocator.
    Status free(void *ptr, size_t size) override;

    // VRAM<->host copy via the SYCL backend when either side is XPU memory;
    // otherwise the inherited host memcpy.
    Status copy(void *dst, void *src, size_t length) override;

    MemoryType getMemoryType(void *addr) override;

    const std::vector<RangeLocation> getLocation(
        void *start, size_t len, bool skip_prefault = false) override;

    const std::string type() const override { return "xpu"; }
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_XPU_H_
