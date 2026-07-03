#ifndef MOONCAKE_PG_DEVICE_RUNTIME_STATE_H
#define MOONCAKE_PG_DEVICE_RUNTIME_STATE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <transport/device/device_transport.h>

namespace mooncake {

struct DeviceCollectiveRuntimeState {
    int cuda_device_index{-1};

    bool direct_p2p_ready{false};
    uint32_t* device_sequence_counter{nullptr};
    uint32_t* device_sequence_slots{nullptr};
    uint32_t device_sequence_slot_cursor{0};

    std::unique_ptr<device::P2pTransport> p2p_transport;
    std::unique_ptr<device::RdmaTransport> rdma_transport;
    bool rdma_ready{false};
    int rdma_qps_per_rank{1};
    std::vector<void*> p2p_peer_ptrs_host;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_RUNTIME_STATE_H
