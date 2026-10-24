#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/one_shot/one_shot_algorithm.cuh"
#include "device_comm/device_collective/resolved_group_view.h"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;
class LLProtocol;

// Implements OneShot collectives using the LL protocol.
class OneShotAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey = "one_shot";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<OneShotAlgorithm>> create(
        DeviceTransferService& transfer_service, uint32_t max_group_size,
        CollectiveRuntimeBindings collective, const LLProtocol& ll);

    ~OneShotAlgorithm() noexcept override = default;

    OneShotAlgorithm(const OneShotAlgorithm&) = delete;
    OneShotAlgorithm& operator=(const OneShotAlgorithm&) = delete;

    [[nodiscard]] AlgorithmEndpoint localEndpoint() const override;

    PGResult<void> updatePlan(const ResolvedGroupView& view,
                              ControlUpdateBuilder& builder) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder& builder) const override;

   protected:
    PGResult<void> enqueueImpl(const AllReduceRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const AllGatherRequest& request,
                               cudaStream_t stream) const override;
    PGResult<void> enqueueImpl(const ReduceScatterRequest& request,
                               cudaStream_t stream) const override;

   private:
    OneShotAlgorithm(RegionSlice packets, OneShotBufferLayout layout,
                     const LLProtocol& ll);

    PGResult<OneShotPlan> buildPlan(const ResolvedGroupView& view) const;
    PGResult<void> appendPlanUpdate(ControlUpdateBuilder& builder,
                                    const OneShotPlan& plan) const;

    RegionSlice packets_;
    OneShotBufferLayout layout_;
    GpuBuffer<OneShotDeviceState> state_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_H
