#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/star/star_algorithm.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class ControlUpdateBuilder;
class SimpleProtocol;

// Implements Star collectives using the Simple protocol.
class StarAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey = "star";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<StarAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple);

    ~StarAlgorithm() noexcept override = default;

    StarAlgorithm(const StarAlgorithm&) = delete;
    StarAlgorithm& operator=(const StarAlgorithm&) = delete;

    [[nodiscard]] AlgorithmEndpoint localEndpoint() const override;

    PGResult<void> updatePlan(const ResolvedGroupView& view,
                              ControlUpdateBuilder& builder) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder& builder) const override;

   protected:
    PGResult<void> enqueueImpl(const GatherRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const ScatterRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const BroadcastRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const ReduceRequest& request,
                               cudaStream_t stream) const override;

   private:
    StarAlgorithm(DeviceCollectiveWorkspace& workspace,
                  const SimpleProtocol& simple);

    PGResult<StarPlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const StarPlan& plan) const;

    DeviceCollectiveWorkspace& workspace_;
    const SimpleProtocol& simple_;
    GpuBuffer<StarDeviceState> state_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_H
