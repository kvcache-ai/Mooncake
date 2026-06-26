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
    int connectPeers(int, bool, const std::vector<int64_t>&,
                     const std::vector<int32_t>&, const std::vector<int32_t>&,
                     const std::vector<int32_t>&, const std::vector<int64_t>&,
                     const std::vector<int64_t>&,
                     const std::vector<int>&) override {
        return -1;
    }
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
