#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/pairwise/pairwise_algorithm.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class ControlUpdateBuilder;
class SimpleProtocol;

// Implements Pairwise collectives using the Simple protocol.
class PairwiseAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey = "pairwise";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<PairwiseAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple);

    ~PairwiseAlgorithm() noexcept override = default;

    PairwiseAlgorithm(const PairwiseAlgorithm&) = delete;
    PairwiseAlgorithm& operator=(const PairwiseAlgorithm&) = delete;

    [[nodiscard]] AlgorithmEndpoint localEndpoint() const override;

    PGResult<void> updatePlan(const ResolvedGroupView& view,
                              ControlUpdateBuilder& builder) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder& builder) const override;

   protected:
    PGResult<void> enqueueImpl(const AllToAllRequest& request,
                               cudaStream_t stream) const override;

   private:
    PairwiseAlgorithm(DeviceCollectiveWorkspace& workspace,
                      const SimpleProtocol& simple);

    PGResult<PairwisePlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const PairwisePlan& plan) const;

    DeviceCollectiveWorkspace& workspace_;
    const SimpleProtocol& simple_;
    GpuBuffer<PairwiseDeviceState> state_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_H
