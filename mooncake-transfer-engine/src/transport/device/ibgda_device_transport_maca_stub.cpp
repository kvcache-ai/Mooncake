// IBGDA device transport — MACA stub.
//
// IBGDA (GPU-initiated RDMA via mlx5gda) is CUDA/InfiniBand only.
// On MACA, all inter-rank communication goes through P2P (MCXLink).
// This stub satisfies the link requirement for createIbgdaDeviceTransport
// and returns a transport whose initialize() always fails, causing the EP
// buffer to set ibgda_disabled_ = true and fall back to P2P-only.

#include "transport/device/device_transport.h"

namespace mooncake {
namespace device {

class NullRdmaTransport : public RdmaTransport {
   public:
    int initialize(const std::string&, int, int) override { return -1; }
    int registerMemory(void*, size_t) override { return -1; }
    int allocateControlBuffer() override { return -1; }
    int createQueuePairs(void*) override { return -1; }
    int recreateQueuePairs(void*) override { return -1; }
    int connectPeers(bool, const std::vector<int64_t>&,
                     const std::vector<int32_t>&,
                     const std::vector<int32_t>&,
                     const std::vector<int32_t>&,
                     const std::vector<int64_t>&,
                     const std::vector<int64_t>&,
                     const std::vector<int>&) override { return -1; }
    RdmaLocalMetadata localMetadata() const override { return {}; }
    void* raddrsPtr() override { return nullptr; }
    void* rkeysPtr() override { return nullptr; }
    void* qpDevCtxsPtr() override { return nullptr; }
    bool isRoce() const override { return false; }
    int gidIndex() const override { return -1; }
};

std::unique_ptr<RdmaTransport> createIbgdaDeviceTransport(
    const std::vector<std::string>&) {
    return std::make_unique<NullRdmaTransport>();
}

}  // namespace device
}  // namespace mooncake
