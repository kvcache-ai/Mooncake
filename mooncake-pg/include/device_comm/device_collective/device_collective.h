#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective_dispatcher.h"
#include "device_comm/device_collective/device_collective_recovery.h"
#include "device_comm/device_collective/resolved_group_view.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_transfer/transfer_service.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class StrongStream;

// Runs device collectives for one communicator.
class DeviceCollectiveRuntime {
   public:
    using FailureRecoveryCallback = std::function<PGResult<void>(InGroupRank)>;

    static PGResult<std::unique_ptr<DeviceCollectiveRuntime>> create(
        DeviceTransferService& transfer_service,
        DeviceCollectiveWorkspace& workspace, StrongStream& strong_stream,
        int device_index, InGroupRank self_rank, uint32_t max_group_size,
        int32_t* active_ranks_mirror, size_t collective_timeout_us);

    ~DeviceCollectiveRuntime() noexcept;

    DeviceCollectiveRuntime(const DeviceCollectiveRuntime&) = delete;
    DeviceCollectiveRuntime& operator=(const DeviceCollectiveRuntime&) = delete;

    [[nodiscard]] DeviceGroupEndpoint localEndpoint() const;

    PGResult<void> useLocalOnly(const GroupView& view);
    PGResult<void> applyGroupView(const GroupView& view);

    PGResult<void> enableRecovery(DeviceCollectiveRecoveryWorker& worker,
                                  FailureRecoveryCallback callback);

    PGResult<void> enqueue(const DeviceCollectiveRequest& request,
                           cudaStream_t user_stream_handle);

    PGResult<void> shutdown();

   private:
    friend class MooncakeCommunicator;

    using RecoveryRequest = ControlMailbox::RecoverySlot::ReceivedRequest;

    struct PublishedViewUpdate {
        ControlUpdate update;
        ControlMailbox::ControlUpdateSlot::RequestHandle handle;
    };

    DeviceCollectiveRuntime(DeviceTransferService& transfer_service,
                            DeviceCollectiveWorkspace& workspace,
                            int device_index, InGroupRank self_rank,
                            uint32_t max_group_size,
                            int32_t* active_ranks_mirror,
                            RegionSlice view_epoch_signals,
                            StrongStream& strong_stream,
                            GpuEvent handoff_event);

    PGResult<ResolvedGroupView> resolveGroupView(const GroupView& view) const;
    PGResult<void> installGroupView(const ResolvedGroupView& view);
    [[nodiscard]] bool hasPendingRecovery() const noexcept;
    PGResult<void> handleFailure(const RecoveryRequest& request);
    PGResult<void> attachGraphUse(const GpuCaptureInfo& capture);
    void releaseState() noexcept;

    DeviceTransferService& transfer_service_;
    DeviceCollectiveWorkspace& workspace_;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    RegionSlice view_epoch_signals_;
    GpuBuffer<DeviceGroupView> device_group_view_;
    GpuBuffer<InvocationState> invocation_state_;
    std::unique_ptr<DeviceCollectiveDispatcher> dispatcher_;
    StrongStream& strong_stream_;
    MappedHostBuffer<ControlMailbox> control_mailbox_;
    // Last ordinary publication; recovery reuses it only if still unconsumed.
    std::optional<PublishedViewUpdate> published_view_update_;
    // Skip repeated GroupView updates when both the epoch and local-only mode
    // match.
    std::optional<uint64_t> installed_view_epoch_;
    bool installed_local_only_ = false;
    // 0/1 mask indexed by stable group rank, updated when a group view is
    // installed. Copied to the caller-owned mirror in recovery control updates.
    std::array<int32_t, kMaxNumRanks> active_ranks_source_{};
    int32_t* active_ranks_mirror_ = nullptr;
    size_t active_ranks_count_ = 0;
    FailureRecoveryCallback failure_recovery_callback_;
    DeviceCollectiveRecoveryWorker* recovery_worker_ = nullptr;

    GpuEvent handoff_event_;
    mutable std::mutex mutex_;
    std::atomic<size_t> live_graph_uses_{0};
    bool shutdown_requested_ = false;
    bool shutdown_complete_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
