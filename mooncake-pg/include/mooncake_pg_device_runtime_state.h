#ifndef MOONCAKE_PG_DEVICE_RUNTIME_STATE_H
#define MOONCAKE_PG_DEVICE_RUNTIME_STATE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <transport/device/device_transport.h>

namespace mooncake {

// Backend-owned runtime resources shared by later Device API collective paths.
// This state intentionally stops at transport/bootstrap/sequence ownership and
// does not embed any collective-specific protocol workspace.
struct DeviceCollectiveRuntimeState {
    int cuda_device_index{-1};

    // Direct transport readiness and graph-safe sequence resources.
    bool direct_p2p_ready{false};
    uint32_t* device_sequence_counter{nullptr};
    uint32_t* device_sequence_slots{nullptr};
    uint32_t device_sequence_slot_cursor{0};

    // Transport ownership and imported peer view for later collective launch.
    std::unique_ptr<device::P2pTransport> p2p_transport;
    std::unique_ptr<device::RdmaTransport> rdma_transport;
    bool rdma_ready{false};
    int rdma_qps_per_rank{1};
    std::vector<void*> p2p_peer_ptrs_host;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_RUNTIME_STATE_H
