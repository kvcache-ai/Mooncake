#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/oneshot/one_shot_types.cuh"
#include "device_comm/device_collective/device_collective.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;
class DeviceCollectiveWorkspace;
class LLResources;

// Owns the One-shot Plan, borrowing LL resources from the runtime and payload
// storage from the shared workspace. Routes must support native peer atomics.
class OneShotAllReduceAlgorithm {
   public:
    static PGResult<std::unique_ptr<OneShotAllReduceAlgorithm>> create(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace, LLResources& ll,
        const uint64_t* view_epoch_signals, InvocationState* invocation_state,
        ControlMailbox* control_mailbox,
        uint64_t timeout_ticks, int device_index, InGroupRank self_rank,
        uint32_t max_group_size);

    ~OneShotAllReduceAlgorithm() noexcept;

    OneShotAllReduceAlgorithm(const OneShotAllReduceAlgorithm&) = delete;
    OneShotAllReduceAlgorithm& operator=(const OneShotAllReduceAlgorithm&) =
        delete;

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
    OneShotAllReduceAlgorithm(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace, LLResources& ll,
        uint64_t timeout_ticks, int device_index, InGroupRank self_rank,
        uint32_t max_group_size) noexcept;

    PGResult<void> initializeDeviceState(const uint64_t* view_epoch_signals,
                                         InvocationState* invocation_state,
                                         ControlMailbox* control_mailbox);
    void releaseDeviceState() noexcept;
    [[nodiscard]] OneShotAllReducePlan makePlan(
        uint64_t view_epoch, uint32_t self_active_index,
        uint32_t participant_count, uint64_t buffer_size) const;

    DeviceTransferService& transfer_service_;
    DeviceCollectiveWorkspace& workspace_;
    LLResources& ll_;
    uint64_t timeout_ticks_ = 0;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    uint32_t max_group_size_ = 0;
    OneShotAllReduceDeviceState* state_ = nullptr;
    OneShotAllReducePlanSlot host_plan_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H
