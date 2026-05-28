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
    // -------------------------------------------------------------------
    // DeviceTransport — capabilities
    // -------------------------------------------------------------------
    const DeviceCommCapabilities deviceCapabilities() const override {
        return transport_.deviceCapabilities();
    }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU buffer allocation
    // -------------------------------------------------------------------
    Status allocateBuffer(void** ptr, size_t size,
                          bool allow_fabric) override {
        return transport_.allocateBuffer(ptr, size);
    }

    Status freeBuffer(void* ptr) override { return transport_.freeBuffer(ptr); }

    // -------------------------------------------------------------------
    // DeviceTransport — P2P peer setup (not supported by IBGDA)
    // -------------------------------------------------------------------
    Status allocatePeerAccessTables(int /*rank*/,
                                    int /*num_ranks*/) override {
        return Status::NotSupported("IBGDA does not support P2P peer tables");
    }

    Status exportIpcHandle(int /*device_id*/, void* /*local_buffer*/,
                           std::vector<int32_t>& /*handle_words*/) override {
        return Status::NotSupported("IBGDA does not use IPC handles");
    }

    Status configurePeers(
        int /*local_device_id*/, void* /*local_buffer*/,
        const std::vector<std::vector<int32_t>>& /*remote_handles*/,
        const std::vector<int>& /*active_ranks_mask*/) override {
        return Status::NotSupported("IBGDA does not use IPC-based P2P");
    }

    bool allPeersAccessible() const override { return false; }

    // -------------------------------------------------------------------
    // DeviceTransport — RDMA / IBGDA setup
    // -------------------------------------------------------------------
    Status initializeRdmaDevice(const std::string& device_name,
                                uint8_t port_num) override {
        return transport_.initializeDevice(device_name, port_num);
    }

    Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                          uint32_t& rkey) override {
        return transport_.registerMemory(ptr, size, lkey, rkey);
    }

    Status unregisterMemory(void* ptr) override {
        return transport_.unregisterMemory(ptr);
    }

    Status allocateControlBuffer(size_t size) override {
        return transport_.allocateControlBuffer(size);
    }

    Status releaseControlBuffer() override {
        return transport_.releaseControlBuffer();
    }

    void* controlBuffer() const override { return transport_.controlBuffer(); }

    Status createQueuePairs(int num_qps, int wqe, void* stream,
                            void* qp_devctxs) override {
        return transport_.createQueuePairs(
            num_qps, wqe, reinterpret_cast<cudaStream_t>(stream), qp_devctxs);
    }

    Status recreateQueuePairs(int num_qps, int wqe, void* stream,
                              void* qp_devctxs) override {
        return transport_.recreateQueuePairs(
            num_qps, wqe, reinterpret_cast<cudaStream_t>(stream), qp_devctxs);
    }

    Status destroyQueuePairs() override {
        return transport_.destroyQueuePairs();
    }

    Status connectRdmaPeers(
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

    // -------------------------------------------------------------------
    // DeviceTransport — metadata accessors
    // -------------------------------------------------------------------
    IbGdaLocalMetadata localMetadata() const override {
        return transport_.localMetadata();
    }

    bool isRoce() const override { return transport_.isRoce(); }

    int gidIndex() const override { return transport_.gidIndex(); }

    bool ibgdaDisabled() const override { return false; }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU-kernel-visible context
    // -------------------------------------------------------------------
    const void* deviceContextPtr() const override {
        return transport_.deviceContextPtr();
    }

    size_t deviceContextSize() const override {
        return transport_.deviceContextSize();
    }

    DeviceContextAbi deviceContextAbi() const override {
        return transport_.deviceContextAbi();
    }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU-kernel-visible tables (P2P)
    // -------------------------------------------------------------------
    int32_t* availableTablePtr() const override { return nullptr; }

    void** peerPtrsTablePtr() const override { return nullptr; }

    // -------------------------------------------------------------------
    // DeviceTransport — utility
    // -------------------------------------------------------------------
    void** hostPeerPtrs() const override { return nullptr; }

    void* getRemotePtr(void* local_ptr, int dst_rank) override {
        return transport_.getRemotePtr(local_ptr, dst_rank);
    }

    // -------------------------------------------------------------------
    // IbGdaDeviceTransport — IBGDA-specific
    // -------------------------------------------------------------------
    size_t controlBufferSize() const override {
        return transport_.controlBufferSize();
    }

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
