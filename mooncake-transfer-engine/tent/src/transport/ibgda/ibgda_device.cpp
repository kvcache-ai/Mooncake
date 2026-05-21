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

#include "tent/device/ibgda.h"

#include <cuda_runtime.h>

#include "tent/transport/ibgda/detail/mlx5gda.h"
#include "tent/transport/ibgda/ibgda_transport.h"

namespace mooncake {
namespace tent {

namespace {

class IbGdaDeviceTransportImpl final : public IbGdaDeviceTransport {
   public:
    Status symAlloc(void** ptr, size_t size) override {
        return transport_.symAlloc(ptr, size);
    }

    Status symFree(void* ptr) override { return transport_.symFree(ptr); }

    void* getRemotePtr(void* local_ptr, int dst_rank) override {
        return transport_.getRemotePtr(local_ptr, dst_rank);
    }

    Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                          uint32_t& rkey) override {
        return transport_.registerMemory(ptr, size, lkey, rkey);
    }

    Status unregisterMemory(void* ptr) override {
        return transport_.unregisterMemory(ptr);
    }

    Status getChannelResources(int channel_id,
                               DeviceChannelResources& resources) override {
        return transport_.getChannelResources(channel_id, resources);
    }

    Status getPeerInfo(int rank, DevicePeerInfo& peer) override {
        return transport_.getPeerInfo(rank, peer);
    }

    Status connect(int rank) override { return transport_.connect(rank); }

    Status barrier() override { return transport_.barrier(); }

    const DeviceCommCapabilities deviceCapabilities() const override {
        return transport_.deviceCapabilities();
    }

    Status initializeDevice(const std::string& device_name,
                            uint8_t port_num) override {
        return transport_.initializeDevice(device_name, port_num);
    }

    Status allocateControlBuffer(size_t size) override {
        return transport_.allocateControlBuffer(size);
    }

    Status releaseControlBuffer() override {
        return transport_.releaseControlBuffer();
    }

    void* controlBuffer() const override { return transport_.controlBuffer(); }

    size_t controlBufferSize() const override {
        return transport_.controlBufferSize();
    }

    Status createQueuePairs(int num_qps, int wqe, void* cuda_stream,
                            void* qp_devctxs) override {
        return transport_.createQueuePairs(
            num_qps, wqe, reinterpret_cast<cudaStream_t>(cuda_stream),
            qp_devctxs);
    }

    Status recreateQueuePairs(int num_qps, int wqe, void* cuda_stream,
                              void* qp_devctxs) override {
        return transport_.recreateQueuePairs(
            num_qps, wqe, reinterpret_cast<cudaStream_t>(cuda_stream),
            qp_devctxs);
    }

    Status destroyQueuePairs() override { return transport_.destroyQueuePairs(); }

    Status connectPeers(
        const std::vector<int64_t>& remote_addrs,
        const std::vector<int32_t>& remote_keys,
        const std::vector<std::vector<int32_t>>& peer_qpns,
        const std::vector<std::vector<int32_t>>& peer_lids,
        const std::vector<int64_t>& subnet_prefixes,
        const std::vector<int64_t>& interface_ids,
        const std::vector<int>& active_ranks_mask, int rank, int num_ranks,
        void* raddrs, void* rkeys) override {
        return transport_.connectPeers(remote_addrs, remote_keys, peer_qpns,
                                       peer_lids, subnet_prefixes,
                                       interface_ids, active_ranks_mask, rank,
                                       num_ranks, raddrs, rkeys);
    }

    IbGdaLocalMetadata localMetadata() const override {
        return transport_.localMetadata();
    }

    bool isRoce() const override { return transport_.isRoce(); }

    int gidIndex() const override { return transport_.gidIndex(); }

   private:
    IbGdaTransport transport_;
};

}  // namespace

std::unique_ptr<IbGdaDeviceTransport> createIbGdaDeviceTransport() {
    return std::make_unique<IbGdaDeviceTransportImpl>();
}

size_t ibGdaQueuePairDeviceContextSize() {
    return sizeof(mlx5gda_qp_devctx);
}

}  // namespace tent
}  // namespace mooncake
