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

// This translation unit links SYCL directly and therefore MUST be compiled by
// the Intel DPC++ compiler (icpx / clang with -fsycl); see xpu_sycl_backend.h.

#include "tent/platform/xpu.h"

#include <glog/logging.h>

#include <string>

#include "tent/common/status.h"
#include "xpu_sycl_backend.h"

namespace mooncake {
namespace tent {

namespace {
// The SYCL backend is created and initialized lazily on first use. init() is
// idempotent and cheap after the first call.
XpuSyclBackend& backend() {
    XpuSyclBackend& be = XpuSyclBackend::instance();
    be.init();
    return be;
}
}  // namespace

Status XpuPlatform::probe(std::vector<Topology::NicEntry>& nic_list,
                          std::vector<Topology::MemEntry>& mem_list) {
    // Host DRAM + RDMA topology is identical to a CPU host.
    CHECK_STATUS(CpuPlatform::probe(nic_list, mem_list));

    // Register one memory node per visible XPU device so findNearMem("xpu:N")
    // resolves to the nearest host NUMA node for staging. Device I/O never
    // touches the NIC directly in the MVP, so these entries only carry NIC
    // affinity used to place the host staging buffers.
    auto& be = backend();
    int device_count = be.deviceCount();
    for (int i = 0; i < device_count; ++i) {
        Topology::MemEntry entry;
        entry.name = "xpu:" + std::to_string(i);
        entry.numa_node = be.deviceNumaNode(i);
        entry.type = Topology::MEM_UNKNOWN;
        int nic_id = 0;
        for (const auto& nic : nic_list) {
            if (entry.numa_node >= 0 && nic.numa_node == entry.numa_node)
                entry.device_list[0].push_back(nic_id);
            else
                entry.device_list[2].push_back(nic_id);
            nic_id++;
        }
        mem_list.push_back(std::move(entry));
    }
    return Status::OK();
}

Status XpuPlatform::allocate(void** pptr, size_t size, MemoryOptions& options) {
    LocationParser location(options.location);
    // A bare "xpu" location has no ':' and therefore parses as the wildcard
    // type, so match the raw string as well and treat it as device 0.
    const bool bare_xpu = options.location == "xpu";
    if (bare_xpu || location.type() == "xpu") {
        // XPU VRAM is allocated via sycl::malloc_device.
        int device_index;
        if (bare_xpu) {
            device_index = 0;
        } else if (location.index() >= 0) {
            device_index = location.index();
        } else {
            // "xpu:<garbage>" -- a malformed ordinal must not silently fall
            // back to device 0.
            return Status::InvalidArgument(
                "XPU location has a malformed device ordinal: " +
                options.location + LOC_MARK);
        }
        if (backend().allocDevice(pptr, size, device_index) != 0)
            return Status::InternalError(
                "XPU device allocation failed" LOC_MARK);
        return Status::OK();
    }
    // Host DRAM staging buffers use the inherited NUMA-aware allocator.
    return CpuPlatform::allocate(pptr, size, options);
}

Status XpuPlatform::free(void* ptr, size_t size) {
    auto& be = backend();
    if (be.isDevicePtr(ptr)) {
        if (be.freeDevice(ptr) != 0)
            return Status::InternalError("XPU device free failed" LOC_MARK);
        return Status::OK();
    }
    return CpuPlatform::free(ptr, size);
}

Status XpuPlatform::copy(void* dst, void* src, size_t length) {
    auto& be = backend();
    bool src_is_device = be.isDevicePtr(src);
    bool dst_is_device = be.isDevicePtr(dst);
    if (src_is_device && dst_is_device) {
        return Status::NotImplemented(
            "XpuPlatform: device-to-device copy is not supported in the MVP; "
            "transfers are staged through host DRAM" LOC_MARK);
    }
    if (src_is_device) {
        if (be.copyD2H(dst, src, length) != 0)
            return Status::InternalError(
                "XPU device->host copy failed" LOC_MARK);
        return Status::OK();
    }
    if (dst_is_device) {
        if (be.copyH2D(dst, src, length) != 0)
            return Status::InternalError(
                "XPU host->device copy failed" LOC_MARK);
        return Status::OK();
    }
    // Neither side is XPU memory: plain host copy.
    return CpuPlatform::copy(dst, src, length);
}

MemoryType XpuPlatform::getMemoryType(void* addr) {
    // Only USM allocated through this platform's backend is classified as XPU.
    // Externally allocated device USM (e.g. a PyTorch XPU tensor) registered
    // via registerLocalMemory is not in the backend's registry and is reported
    // as CPU here; classifying such external pointers is future work.
    if (backend().isDevicePtr(addr)) return MTYPE_XPU;
    return CpuPlatform::getMemoryType(addr);
}

const std::vector<RangeLocation> XpuPlatform::getLocation(void* start,
                                                          size_t len,
                                                          bool skip_prefault) {
    auto& be = backend();
    if (be.isDevicePtr(start)) {
        int index = be.deviceIndex(start);
        std::string location =
            index >= 0 ? "xpu:" + std::to_string(index) : kWildcardLocation;
        return {RangeLocation{reinterpret_cast<uint64_t>(start), len,
                              std::move(location)}};
    }
    return CpuPlatform::getLocation(start, len, skip_prefault);
}

}  // namespace tent
}  // namespace mooncake
