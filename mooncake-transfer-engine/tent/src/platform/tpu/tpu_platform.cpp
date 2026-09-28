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

#include "tent/platform/tpu.h"

#include <glog/logging.h>

#include <string>

#include "tent/common/status.h"
#include "tent/platform/tpu_pjrt_shim.h"

namespace mooncake {
namespace tent {

Status TpuPlatform::probe(std::vector<Topology::NicEntry>& nic_list,
                          std::vector<Topology::MemEntry>& mem_list) {
    // Host DRAM + RDMA topology is identical to a CPU host.
    CHECK_STATUS(CpuPlatform::probe(nic_list, mem_list));

    // Register one memory node per visible TPU device so findNearMem("tpu:N")
    // resolves to the nearest host NUMA node for staging. Device I/O never
    // touches the NIC directly, so these entries only carry NIC affinity used
    // to place the host staging buffers.
    auto& shim = TpuPjrtShim::instance();
    int device_count = shim.deviceCount();
    for (int i = 0; i < device_count; ++i) {
        Topology::MemEntry entry;
        entry.name = "tpu:" + std::to_string(i);
        entry.numa_node = shim.deviceNumaNode(i);
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

Status TpuPlatform::allocate(void** pptr, size_t size, MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "tpu") {
        // TPU HBM buffers are owned by the serving framework and registered
        // with TENT; TENT never allocates them itself.
        return Status::NotImplemented(
            "TpuPlatform does not allocate TPU device memory" LOC_MARK);
    }
    // Host DRAM staging buffers use the inherited NUMA-aware allocator.
    return CpuPlatform::allocate(pptr, size, options);
}

Status TpuPlatform::copy(void* dst, void* src, size_t length) {
    auto& shim = TpuPjrtShim::instance();
    bool src_is_device = shim.isDevicePtr(src);
    bool dst_is_device = shim.isDevicePtr(dst);
    if (src_is_device && dst_is_device) {
        return Status::NotImplemented(
            "TpuPlatform: device-to-device copy is not supported; transfers "
            "are staged through host DRAM" LOC_MARK);
    }
    if (src_is_device) return shim.copyD2H(dst, src, length);
    if (dst_is_device) return shim.copyH2D(dst, src, length);
    // Neither side is TPU memory: plain host copy.
    return CpuPlatform::copy(dst, src, length);
}

MemoryType TpuPlatform::getMemoryType(void* addr) {
    if (TpuPjrtShim::instance().isDevicePtr(addr)) return MTYPE_TPU;
    return CpuPlatform::getMemoryType(addr);
}

const std::vector<RangeLocation> TpuPlatform::getLocation(void* start,
                                                          size_t len,
                                                          bool skip_prefault) {
    auto& shim = TpuPjrtShim::instance();
    if (shim.isDevicePtr(start)) {
        int index = shim.deviceIndex(start);
        std::string location =
            index >= 0 ? "tpu:" + std::to_string(index) : kWildcardLocation;
        return {RangeLocation{reinterpret_cast<uint64_t>(start), len,
                              std::move(location)}};
    }
    return CpuPlatform::getLocation(start, len, skip_prefault);
}

}  // namespace tent
}  // namespace mooncake
