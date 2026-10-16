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

#ifndef TENT_PLATFORM_TPU_H_
#define TENT_PLATFORM_TPU_H_

#include "tent/common/config.h"
#include "tent/platform/cpu.h"
#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {

// TpuPlatform models a TPU host: the host DRAM and RDMA topology are identical
// to CpuPlatform, so those paths (host allocation, NUMA probing, host<->host
// copy) are inherited unchanged. Only the TPU-device-aware operations are
// overridden, and every one of them is delegated to TpuPjrtShim so that this
// class carries no direct PJRT dependency.
//
// TPU HBM cannot be reached by the NIC, so there is no direct device transport:
// the HBM<->host hop is performed here via copy(), and the host<->host hop is
// carried by RDMA/TCP. ProxyManager chains the two (see findStagingPolicy).
class TpuPlatform : public CpuPlatform {
   public:
    explicit TpuPlatform(std::shared_ptr<Config> config)
        : CpuPlatform(config) {}

    ~TpuPlatform() override {}

    // Host + RDMA discovery from CpuPlatform, plus one MemEntry per TPU device
    // ("tpu:N") so findNearMem() can resolve a device to its nearest host node.
    Status probe(std::vector<Topology::NicEntry> &nic_list,
                 std::vector<Topology::MemEntry> &mem_list) override;

    // Device allocation is owned by the serving framework (JAX / torch-XLA);
    // host allocation is inherited. "tpu" locations therefore return
    // NotImplemented here.
    Status allocate(void **pptr, size_t size, MemoryOptions &options) override;

    // HBM<->host copy via the adapter when either side is TPU memory; otherwise
    // the inherited host memcpy.
    Status copy(void *dst, void *src, size_t length) override;

    MemoryType getMemoryType(void *addr) override;

    const std::vector<RangeLocation> getLocation(
        void *start, size_t len, bool skip_prefault = false) override;

    const std::string type() const override { return "tpu"; }
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_TPU_H_
