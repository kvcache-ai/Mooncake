#pragma once

#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/hierarchical/hierarchical_kernel.cuh"
#include "device_comm/device_transfer/transfer_region.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class DeviceTransferService;
class LLProtocol;
class SimpleProtocol;

class HierarchicalAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey =
        "hierarchical_oneshot_ring";
    static PGResult<std::unique_ptr<HierarchicalAlgorithm>> create(
        DeviceTransferService& transfer, uint32_t capacity,
        CollectiveRuntimeBindings bindings,
        DeviceCollectiveWorkspace& workspace, const LLProtocol& ll,
        const SimpleProtocol& simple);

    AlgorithmEndpoint localEndpoint() const override;
    PGResult<void> updatePlan(const ResolvedGroupView&,
                              ControlUpdateBuilder&) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder&) const override;

   private:
    HierarchicalAlgorithm(RegionSlice packets, OneShotBufferLayout layout,
                          DeviceCollectiveWorkspace&, const LLProtocol&,
                          const SimpleProtocol&);
    PGResult<void> enqueueImpl(const AllReduceRequest&,
                               cudaStream_t) const override;

    RegionSlice packets_;
    OneShotBufferLayout layout_;
    DeviceCollectiveWorkspace& workspace_;
    const SimpleProtocol& simple_;
    GpuBuffer<HierarchicalKernelState> state_;
};

}  // namespace mooncake
