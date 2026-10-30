#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/twoshot/two_shot_types.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class ControlUpdateBuilder;
class SimpleResources;
class DeviceTransferService;

class TwoShotAllReduceAlgorithm {
   public:
    // Workspace and protocol resources must outlive the algorithm.
    static PGResult<std::unique_ptr<TwoShotAllReduceAlgorithm>> create(
        DeviceTransferService& transfer_service,
        CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleResources& simple);

    ~TwoShotAllReduceAlgorithm() noexcept;

    TwoShotAllReduceAlgorithm(const TwoShotAllReduceAlgorithm&) = delete;
    TwoShotAllReduceAlgorithm& operator=(const TwoShotAllReduceAlgorithm&) =
        delete;

    // Construct a candidate without changing published state.
    PGResult<TwoShotAllReducePlan> buildPlan(
        const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const TwoShotAllReducePlan& plan) const;
    PGResult<void> enqueue(const AllReduceRequest& request,
                           cudaStream_t stream) const;

   private:
    TwoShotAllReduceAlgorithm(DeviceTransferService& transfer_service,
                              int device_index,
                              DeviceCollectiveWorkspace& workspace) noexcept
        : workspace_(workspace),
          transfer_service_(transfer_service),
          device_index_(device_index) {}

    DeviceCollectiveWorkspace& workspace_;
    DeviceTransferService& transfer_service_;
    int device_index_ = -1;
    TwoShotAllReduceDeviceState* state_ = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_ALL_REDUCE_H
