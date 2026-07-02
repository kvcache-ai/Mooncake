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
    bool sync_regions_on_device{false};

    bool direct_p2p_ready{false};
    uint32_t direct_p2p_sequence{1};
    uint32_t* device_sequence_counter{nullptr};
    uint32_t* device_sequence_slots{nullptr};
    uint32_t device_sequence_slot_cursor{0};

    std::unique_ptr<device::P2pTransport> p2p_transport;
    std::unique_ptr<device::RdmaTransport> rdma_transport;
    bool rdma_ready{false};
    int rdma_qps_per_rank{1};
    std::vector<void*> p2p_peer_ptrs_host;

    uint32_t* host_signals{nullptr};
    size_t host_signals_bytes{0};
};

struct DeviceCollectiveRuntimeSnapshot {
    bool enabled{false};
    bool direct_p2p_ready{false};
    bool rdma_ready{false};
    bool same_host_only{false};
    bool has_sequence_counter{false};
    bool has_sequence_slots{false};
    bool has_host_signals{false};
    int active_size{0};
    int p2p_peer_ptr_count{0};
    int rdma_qps_per_rank{0};
    size_t host_signals_bytes{0};
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_RUNTIME_STATE_H
