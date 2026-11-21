#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/ring/ring_algorithm.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class ControlUpdateBuilder;
class SimpleProtocol;

// Implements Ring collectives using the Simple protocol.
class RingAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey = "ring";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<RingAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple);

    ~RingAlgorithm() noexcept override = default;

    RingAlgorithm(const RingAlgorithm&) = delete;
    RingAlgorithm& operator=(const RingAlgorithm&) = delete;

    [[nodiscard]] AlgorithmEndpoint localEndpoint() const override;

    PGResult<void> updatePlan(const ResolvedGroupView& view,
                              ControlUpdateBuilder& builder) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder& builder) const override;

   protected:
    PGResult<void> enqueueImpl(const AllReduceRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const AllGatherRequest& request,
                               cudaStream_t stream) const override;

   private:
    RingAlgorithm(DeviceCollectiveWorkspace& workspace,
                  const SimpleProtocol& simple);

    PGResult<RingPlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const RingPlan& plan) const;

    DeviceCollectiveWorkspace& workspace_;
    const SimpleProtocol& simple_;
    GpuBuffer<RingDeviceState> state_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_H
