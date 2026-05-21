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

#ifndef TENT_DEVICE_IBGDA_H_
#define TENT_DEVICE_IBGDA_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"
#include "tent/runtime/device_transport.h"

namespace mooncake {
namespace tent {

// Host-visible metadata that callers exchange before connecting GPU-initiated
// IBGDA queue pairs.  This is a stable TENT public projection; concrete verbs,
// DevX, mlx5 and CUDA queue-builder objects stay hidden behind the façade.
struct IbGdaLocalMetadata {
    int64_t raddr = 0;
    int32_t rkey = 0;
    bool is_roce = false;
    int64_t subnet_prefix = 0;
    int64_t interface_id = 0;
    std::vector<int32_t> qpns;
    std::vector<int32_t> lids;
};

class IbGdaDeviceTransport : public DeviceTransport {
   public:
    ~IbGdaDeviceTransport() override = default;

    virtual Status initializeDevice(const std::string& device_name,
                                    uint8_t port_num = 1) = 0;
    virtual Status allocateControlBuffer(size_t size) = 0;
    virtual Status releaseControlBuffer() = 0;
    virtual void* controlBuffer() const = 0;
    virtual size_t controlBufferSize() const = 0;

    // cuda_stream is intentionally opaque in this public host header so users
    // do not need to include cuda_runtime.h merely to depend on the IBGDA API.
    virtual Status createQueuePairs(int num_qps, int wqe, void* cuda_stream,
                                    void* qp_devctxs) = 0;
    virtual Status recreateQueuePairs(int num_qps, int wqe, void* cuda_stream,
                                      void* qp_devctxs) = 0;
    virtual Status destroyQueuePairs() = 0;

    virtual Status connectPeers(
        const std::vector<int64_t>& remote_addrs,
        const std::vector<int32_t>& remote_keys,
        const std::vector<std::vector<int32_t>>& peer_qpns,
        const std::vector<std::vector<int32_t>>& peer_lids,
        const std::vector<int64_t>& subnet_prefixes,
        const std::vector<int64_t>& interface_ids,
        const std::vector<int>& active_ranks_mask, int rank, int num_ranks,
        void* raddrs, void* rkeys) = 0;

    virtual IbGdaLocalMetadata localMetadata() const = 0;
    virtual bool isRoce() const = 0;
    virtual int gidIndex() const = 0;
};

std::unique_ptr<IbGdaDeviceTransport> createIbGdaDeviceTransport();

// Size of one GPU-visible IBGDA queue-pair context entry.  Callers use this to
// allocate the opaque qp_devctxs array without depending on mlx5 detail types.
size_t ibGdaQueuePairDeviceContextSize();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_IBGDA_H_
