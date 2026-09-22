#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/ring/ring_types.cuh"
#include "device_comm/device_collective/device_collective.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class DeviceTransferService;
class ControlUpdateBuilder;
class SimpleResources;

// Owns the host-side decisions specific to Ring AllReduce. The common runtime
// supplies Simple resources, view-epoch signals, ordering, and recovery; the
// Ring Plan supplies the exact peers that preparation must check.
class RingAllReduceAlgorithm {
   public:
    static PGResult<std::unique_ptr<RingAllReduceAlgorithm>> create(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace, SimpleResources& simple,
        const uint64_t* view_epoch_signals, InvocationState* invocation_state,
        ControlMailbox* control_mailbox, uint64_t timeout_ticks,
        int device_index, InGroupRank self_rank, uint32_t max_group_size);

    ~RingAllReduceAlgorithm() noexcept;

    RingAllReduceAlgorithm(const RingAllReduceAlgorithm&) = delete;
    RingAllReduceAlgorithm& operator=(const RingAllReduceAlgorithm&) = delete;

    // These methods update only the host Plan. Runtime publication is a
    // separate step that encodes the complete collective state below.
    void useLocalOnly(uint64_t view_epoch);
    PGResult<void> applyGroupView(
        const DeviceCollectiveRuntime::ResolvedGroupView& view);
    void invalidateHostPlan() noexcept;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder) const;

    [[nodiscard]] bool ready() const noexcept;

    PGResult<void> enqueue(const void* send_buffer, void* recv_buffer,
                           size_t count, DataType datatype, ReduceOp op,
                           cudaStream_t stream,
                           int32_t* failed_ranks_hint) const;

   private:
    RingAllReduceAlgorithm(DeviceCollectiveWorkspace& workspace,
                           SimpleResources& simple,
                           const DeviceTransferHandle* transfer_handle,
                           const uint64_t* view_epoch_signals,
                           InvocationState* invocation_state,
                           ControlMailbox* control_mailbox,
                           uint64_t timeout_ticks, int device_index,
                           InGroupRank self_rank) noexcept;

    PGResult<void> initializeDeviceState();
    void releaseDeviceState() noexcept;
    [[nodiscard]] RingAllReducePlan makePlan(
        uint64_t view_epoch, int32_t self_active_index,
        uint32_t participant_count, uint64_t buffer_size,
        InGroupRank predecessor, InGroupRank successor,
        uint64_t send_buffer_offset, char* staging_ptr) const;
    DeviceCollectiveWorkspace& workspace_;
    SimpleResources& simple_;
    const DeviceTransferHandle* transfer_handle_ = nullptr;
    const uint64_t* view_epoch_signals_ = nullptr;
    InvocationState* invocation_state_ = nullptr;
    ControlMailbox* control_mailbox_ = nullptr;
    uint64_t timeout_ticks_ = 0;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    RingAllReduceDeviceState* state_ = nullptr;
    RingAllReducePlanSlot host_plan_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H
