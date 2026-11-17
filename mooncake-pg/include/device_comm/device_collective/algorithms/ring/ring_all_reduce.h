#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/ring/ring_types.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class ControlUpdateBuilder;
class SimpleResources;

class RingAllReduceAlgorithm {
   public:
    // Workspace and protocol resources must outlive the algorithm.
    static PGResult<std::unique_ptr<RingAllReduceAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleResources& simple);

    ~RingAllReduceAlgorithm() noexcept;

    RingAllReduceAlgorithm(const RingAllReduceAlgorithm&) = delete;
    RingAllReduceAlgorithm& operator=(const RingAllReduceAlgorithm&) = delete;

    // Construct a candidate without changing published state.
    PGResult<RingAllReducePlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const RingAllReducePlan& plan) const;
    PGResult<void> enqueue(const AllReduceRequest& request,
                           cudaStream_t stream) const;

   private:
    RingAllReduceAlgorithm(int device_index,
                           DeviceCollectiveWorkspace& workspace,
                           const SimpleResources& simple) noexcept
        : workspace_(workspace), simple_(simple), device_index_(device_index) {}

    DeviceCollectiveWorkspace& workspace_;
    const SimpleResources& simple_;
    int device_index_ = -1;
    RingAllReduceDeviceState* state_ = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALL_REDUCE_H
