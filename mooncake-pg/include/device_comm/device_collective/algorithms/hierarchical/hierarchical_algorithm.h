#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_HIERARCHICAL_ALGORITHM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_HIERARCHICAL_ALGORITHM_H

#include <cstdint>
#include <memory>
#include <string_view>

#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/algorithms/ring/ring_algorithm.h"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceCollectiveWorkspace;
class SimpleProtocol;

// Static hierarchical AllReduce: intra-node Reduce, inter-node AllReduce
// among node leaders, then intra-node Broadcast. Each phase reuses Ring
// Algorithm with a subgroup plan derived from the published node topology.
class HierarchicalAlgorithm final : public AlgorithmProvider {
   public:
    static constexpr std::string_view kAlgorithmKey = "hierarchical";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<HierarchicalAlgorithm>> create(
        int device_index, CollectiveRuntimeBindings collective,
        DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple);

    ~HierarchicalAlgorithm() noexcept override = default;

    HierarchicalAlgorithm(const HierarchicalAlgorithm&) = delete;
    HierarchicalAlgorithm& operator=(const HierarchicalAlgorithm&) = delete;

    [[nodiscard]] AlgorithmEndpoint localEndpoint() const override;

    PGResult<void> updatePlan(const ResolvedGroupView& view,
                              ControlUpdateBuilder& builder) const override;
    PGResult<void> invalidatePlan(ControlUpdateBuilder& builder) const override;

   protected:
    PGResult<void> enqueueImpl(const AllReduceRequest& request,
                               cudaStream_t stream) const override;

   private:
    struct HostPlan {
        uint32_t local_size = 0;
        uint32_t node_count = 0;
        bool self_is_leader = false;
    };

    HierarchicalAlgorithm(DeviceCollectiveWorkspace& workspace,
                          const SimpleProtocol& simple);

    DeviceCollectiveWorkspace& workspace_;
    const SimpleProtocol& simple_;
    std::unique_ptr<RingAlgorithm> intra_reduce_;
    std::unique_ptr<RingAlgorithm> inter_all_reduce_;
    std::unique_ptr<RingAlgorithm> intra_broadcast_;
    mutable HostPlan plan_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_HIERARCHICAL_ALGORITHM_H
