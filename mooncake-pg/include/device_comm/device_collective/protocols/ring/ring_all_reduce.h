#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/protocols/ring/ring_types.cuh"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class DeviceTransferService;

// Owns the host-side decisions specific to Ring AllReduce. The common runtime
// supplies ordering and recovery lifecycle only.
class RingAllReduceProtocol {
   public:
    static PGResult<std::unique_ptr<RingAllReduceProtocol>> create(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace,
        DeviceCollectiveInvocationState* invocation_state,
        DeviceCollectiveRecoveryMailbox* recovery_mailbox,
        uint64_t timeout_ticks, GpuStream& control_stream, int device_index,
        InGroupRank self_rank, uint32_t max_group_size);

    ~RingAllReduceProtocol() noexcept;

    RingAllReduceProtocol(const RingAllReduceProtocol&) = delete;
    RingAllReduceProtocol& operator=(const RingAllReduceProtocol&) = delete;

    PGResult<void> useLocalOnly();
    PGResult<void> applyGroupView(const GroupView& view);
    PGResult<void> invalidate();

    [[nodiscard]] bool ready() const noexcept;
    [[nodiscard]] const RingAllReduceEndpoint& localEndpoint() const noexcept;

    PGResult<void> enqueue(const void* send_buffer, void* recv_buffer,
                           size_t count, DataType datatype, ReduceOp op,
                           cudaStream_t stream,
                           int32_t* failed_ranks_hint) const;

   private:
    static constexpr size_t kMinBytesPerChannelStep = 256ull << 10;

    struct HostState;

    RingAllReduceProtocol(DeviceTransferService& transfer_service,
                          DeviceCollectiveWorkspace& workspace,
                          const DeviceTransferHandle* transfer_handle,
                          DeviceCollectiveInvocationState* invocation_state,
                          DeviceCollectiveRecoveryMailbox* recovery_mailbox,
                          uint64_t timeout_ticks, GpuStream& control_stream,
                          int device_index, InGroupRank self_rank,
                          uint32_t max_group_size, RegionSlice signals,
                          RingSignalLayout signal_layout) noexcept;

    PGResult<void> initializeDeviceState();
    void releaseDeviceState() noexcept;
    PGResult<void> initializeHostState();
    void releaseHostState() noexcept;
    PGResult<void> publish(int32_t self_active_index,
                           uint32_t participant_count, uint64_t buffer_size,
                           RingPeerTarget predecessor, RingPeerTarget successor,
                           char* staging_ptr);
    [[nodiscard]] static uint32_t chooseChannelCount(size_t payload_size,
                                                     uint32_t participant_count,
                                                     uint64_t buffer_size);

    DeviceTransferService& transfer_service_;
    DeviceCollectiveWorkspace& workspace_;
    const DeviceTransferHandle* transfer_handle_ = nullptr;
    DeviceCollectiveInvocationState* invocation_state_ = nullptr;
    DeviceCollectiveRecoveryMailbox* recovery_mailbox_ = nullptr;
    uint64_t timeout_ticks_ = 0;
    GpuStream& control_stream_;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    uint32_t max_group_size_ = 0;
    RegionSlice signals_;
    RingSignalLayout signal_layout_;
    RingAllReduceDeviceState* state_ = nullptr;
    RingAllReduceEndpoint endpoint_;
    HostState* host_state_ = nullptr;
    bool ready_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_ALL_REDUCE_H
