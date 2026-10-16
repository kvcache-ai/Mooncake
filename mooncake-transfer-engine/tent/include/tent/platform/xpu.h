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

#include <sys/types.h>

#include <array>
#include <cstddef>
#include <cstdint>

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
// This platform provides the device-side building blocks: USM allocation
// ("xpu:N"), pointer classification, the copy() primitive (VRAM<->host and
// VRAM<->VRAM), and dma-buf export of device allocations. When the RNIC
// supports ibv_reg_dmabuf_mr, RdmaContext registers exported XPU buffers
// directly and the engine moves VRAM<->VRAM / VRAM<->DRAM data over RDMA
// without a host bounce (GPUDirect-style). Buffers that cannot be exported, or
// hosts without dma-buf capable verbs, keep using the staging path: getTypeEnum
// / isGpuType / findStagingPolicy let ProxyManager chain the VRAM<->host hop
// (executed by XpuTransport via copy()) and the host<->host hop end-to-end over
// the network. Within one process, VRAM<->VRAM copies between two XPUs run
// directly over PCIe (or Xe Link) when the devices have peer access
// (sycl_ext_oneapi_peer_access), and are bounced through host memory
// otherwise. Across processes on one host, XpuTransport publishes each
// registered XPU buffer's Level Zero IPC handle (exportIpc) in the segment
// descriptor and the peer maps it into its own device address space
// (importIpc), so the same PCIe P2P copy serves a same-node prefill/decode
// pair.
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

    // VRAM<->host copy via the SYCL backend when either side is XPU memory,
    // VRAM<->VRAM (same device, PCIe P2P, or host bounce) when both are;
    // otherwise the inherited host memcpy.
    Status copy(void *dst, void *src, size_t length) override;

    MemoryType getMemoryType(void *addr) override;

    const std::vector<RangeLocation> getLocation(
        void *start, size_t len, bool skip_prefault = false) override;

    // Level Zero dma-buf export of the USM device allocation containing
    // [addr, addr+length). InvalidArgument for non-XPU pointers so the caller
    // registers them as host memory; InternalError when the driver refuses or
    // when a foreign allocation sits in a runtime-managed USM pool (its
    // dma-buf is shared, so the allocation cannot be registered on its own).
    Status exportDmabuf(void *addr, size_t length, DmabufExport &out) override;

    // Same-host, cross-process sharing of XPU allocations (used by
    // XpuTransport, not part of the generic Platform interface).
    //
    // exportIpc describes the whole USM allocation containing
    // [addr, addr+length): its Level Zero IPC handle (zeMemGetIpcHandle; an
    // opaque blob that names the allocation's dma-buf by a descriptor of this
    // process, so it is only meaningful together with this process's pid),
    // its base address in this process and its size. Fails for host memory
    // and for ranges that span more than one allocation; reported without
    // logging so a registration that also failed dma-buf export does not warn
    // twice.
    static constexpr size_t kIpcHandleSize = 64;  // ZE_MAX_IPC_HANDLE_SIZE
    using IpcHandle = std::array<uint8_t, kIpcHandleSize>;
    struct IpcExport {
        IpcHandle handle{};
        uint64_t base = 0;
        uint64_t size = 0;
    };
    Status exportIpc(void *addr, size_t length, IpcExport &out);

    // importIpc maps the allocation behind `handle`, exported by process
    // `exporter_pid` (possibly this one) and `size` bytes long, as device
    // memory addressable from local device `device_index`, returning the
    // local pointer in *pptr; copy() then treats it like any other XPU
    // allocation. Fetching the exporter's descriptor needs ptrace permission
    // over `exporter_pid` (Yama ptrace_scope / CAP_SYS_PTRACE) unless it is
    // this process; errno is EPERM when that is refused. closeImport unmaps
    // the range.
    Status importIpc(const IpcHandle &handle, pid_t exporter_pid, size_t size,
                     int device_index, void **pptr);
    Status closeImport(void *ptr);

    // Local ordinal N of the "xpu:N" device owning `addr`, or -1 for host
    // memory.
    int deviceIndex(void *addr);

    const std::string type() const override { return "xpu"; }
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_XPU_H_
