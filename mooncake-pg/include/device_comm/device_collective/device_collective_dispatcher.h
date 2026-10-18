#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_DISPATCHER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_DISPATCHER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/algorithms/algorithm_provider.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/protocol_provider.h"
#include "device_comm/device_collective/resolved_group_view.h"
#include "error_types.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class DeviceTransferService;
class ControlUpdateBuilder;

// Owns the collective algorithms and their shared protocols. Selects
// implementations using the installed algorithm policy.
class DeviceCollectiveDispatcher {
   public:
    static PGResult<std::unique_ptr<DeviceCollectiveDispatcher>> create(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace, int device_index,
        InGroupRank self_rank, uint32_t max_group_size,
        CollectiveRuntimeBindings collective);

    ~DeviceCollectiveDispatcher() noexcept;
    DeviceCollectiveDispatcher(const DeviceCollectiveDispatcher&) = delete;
    DeviceCollectiveDispatcher& operator=(const DeviceCollectiveDispatcher&) =
        delete;

    // Fill protocol and algorithm endpoints.
    void fillLocalEndpoint(DeviceGroupEndpoint& endpoint) const;

    // Update protocol state and plans through the builder, and retain the
    // view's algorithm policy for dispatch.
    PGResult<void> installGroupView(const ResolvedGroupView& view,
                                    ControlUpdateBuilder& builder);

    // Clear the algorithm policy and mark all plans unavailable.
    // Protocol bindings and progress remain intact.
    PGResult<void> invalidatePlans(ControlUpdateBuilder& builder);

    PGResult<const AlgorithmProvider*> selectAlgorithm(
        const DeviceCollectiveRequest& request) const;

   private:
    DeviceCollectiveDispatcher() = default;

    template <typename Protocol, typename... Args>
    Protocol* tryLoadProtocol(Args&&... args);
    template <typename Algorithm, typename... Args>
    void tryLoadAlgorithm(Args&&... args);

    PGResult<const AlgorithmProvider*> findAlgorithm(
        DeviceCollectiveAlgorithm type) const;

    // Destruction runs in reverse order: algorithms borrow protocol resources.
    std::vector<std::unique_ptr<ProtocolProvider>> protocols_;
    std::vector<std::unique_ptr<AlgorithmProvider>> algorithms_;
    std::vector<DeviceCollectiveAlgorithmChoice> algorithm_choices_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_DISPATCHER_H
