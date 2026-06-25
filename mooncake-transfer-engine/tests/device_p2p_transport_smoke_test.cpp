#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transport/device/device_transport.h"

namespace {

void checkCuda(cudaError_t err, const char* what) {
    if (err != cudaSuccess) {
        throw std::runtime_error(std::string(what) + ": " +
                                 cudaGetErrorString(err));
    }
}

void check(bool cond, const char* what) {
    if (!cond) throw std::runtime_error(what);
}

void runTransportSmoke(mooncake::device::P2pTransport& transport) {
    constexpr size_t kBytes = 4096;
    void* buffer = transport.allocateBuffer(kBytes);
    check(buffer != nullptr, "allocateBuffer returned null");

    try {
        checkCuda(cudaMemset(buffer, 0x5a, kBytes), "cudaMemset(buffer)");

        auto handle = transport.exportIpcHandle(buffer);
        check(!handle.empty(), "exportIpcHandle returned an empty handle");

        std::vector<std::vector<int32_t>> handles{handle};
        std::vector<int> active{1};
        transport.importPeerHandles(buffer, 0, 1, handles, active);

        check(transport.allPeersAccessible(),
              "single-rank import should mark all peers accessible");

        int32_t available = 0;
        checkCuda(cudaMemcpy(&available, transport.availableTablePtr(),
                             sizeof(available), cudaMemcpyDeviceToHost),
                  "copy available table");
        check(available == 1, "available table did not mark rank 0");

        void* peer_ptr = nullptr;
        checkCuda(cudaMemcpy(&peer_ptr, transport.peerPtrsTablePtr(),
                             sizeof(peer_ptr), cudaMemcpyDeviceToHost),
                  "copy peer pointer table");
        check(peer_ptr == buffer, "peer pointer table did not contain buffer");

        check(transport.verifyPeerAccess(),
              "single-rank verifyPeerAccess should succeed");
    } catch (...) {
        transport.freeBuffer(buffer);
        throw;
    }

    transport.freeBuffer(buffer);
}

}  // namespace

int main() {
    int device_count = 0;
    checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
    if (device_count <= 0) {
        std::cout << "No CUDA-like devices found; skipping\n";
        return 0;
    }
    checkCuda(cudaSetDevice(0), "cudaSetDevice(0)");

    auto owned = mooncake::device::createP2pDeviceTransport(1);
    check(static_cast<bool>(owned), "createP2pDeviceTransport returned null");
    runTransportSmoke(*owned);

    mooncake::TransferEngine engine;
    auto* engine_transport = engine.getOrCreateP2pTransport(1);
    check(engine_transport != nullptr,
          "TransferEngine::getOrCreateP2pTransport returned null");
    runTransportSmoke(*engine_transport);

    auto* rdma_transport = engine.getOrCreateRdmaTransport();
#if defined(USE_MACA) && !defined(USE_CUDA) && !defined(USE_MUSA)
    check(rdma_transport == nullptr,
          "MACA RDMA DeviceTransport should be unavailable for now");
#else
    (void)rdma_transport;
#endif

    std::cout << "device_p2p_transport_smoke_test passed\n";
    return 0;
}
