#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/algorithms/oneshot/one_shot_types.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;
class LLResources;

class OneShotAllReduceAlgorithm {
   public:
    // Transfer service and protocol resources must outlive the algorithm.
    static PGResult<std::unique_ptr<OneShotAllReduceAlgorithm>> create(
        DeviceTransferService& transfer_service, uint32_t max_group_size,
        CollectiveRuntimeBindings collective, const LLResources& ll);

    ~OneShotAllReduceAlgorithm() noexcept;

    OneShotAllReduceAlgorithm(const OneShotAllReduceAlgorithm&) = delete;
    OneShotAllReduceAlgorithm& operator=(const OneShotAllReduceAlgorithm&) =
        delete;

    [[nodiscard]] OneShotEndpoint localEndpoint() const noexcept {
        return {.buffer_offset = packets_.offset(),
                .buffer_size = packets_.size()};
    }

    // Construct a candidate without changing published state.
    PGResult<OneShotAllReducePlan> buildPlan(
        const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const OneShotAllReducePlan& plan) const;
    PGResult<void> enqueue(const AllReduceRequest& request,
                           cudaStream_t stream) const;

   private:
    OneShotAllReduceAlgorithm(RegionSlice packets, OneShotBufferLayout layout,
                              int device_index) noexcept;

    // Kept private across invocations so packet tags remain valid.
    RegionSlice packets_;
    OneShotBufferLayout layout_;
    int device_index_ = -1;
    OneShotAllReduceDeviceState* state_ = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALL_REDUCE_H
