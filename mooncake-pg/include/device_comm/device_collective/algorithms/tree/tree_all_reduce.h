#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_ALL_REDUCE_H

#include <memory>

#include "device_comm/device_collective/algorithms/tree/tree_types.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class SimpleResources;
class ControlUpdateBuilder;

class TreeAllReduceAlgorithm {
   public:
    // Runtime workspace and Simple resources outlive this algorithm.
    static PGResult<std::unique_ptr<TreeAllReduceAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleResources& simple);
    ~TreeAllReduceAlgorithm() noexcept;

    TreeAllReduceAlgorithm(const TreeAllReduceAlgorithm&) = delete;
    TreeAllReduceAlgorithm& operator=(const TreeAllReduceAlgorithm&) = delete;

    PGResult<TreeAllReducePlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const TreeAllReducePlan& plan) const;
    PGResult<void> enqueue(const AllReduceRequest& request,
                           cudaStream_t stream) const;

   private:
    TreeAllReduceAlgorithm(int device_index,
                           DeviceCollectiveWorkspace& workspace,
                           const SimpleResources& simple) noexcept
        : workspace_(workspace), simple_(simple), device_index_(device_index) {}

    DeviceCollectiveWorkspace& workspace_;
    const SimpleResources& simple_;
    int device_index_ = -1;
    TreeAllReduceDeviceState* state_ = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_ALL_REDUCE_H
