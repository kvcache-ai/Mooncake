#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective_recovery.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_transfer/transfer_service.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class RingAllReduceAlgorithm;
class OneShotAllReduceAlgorithm;
class SimpleResources;
class LLResources;
class StrongStream;

// Collective lifecycle facade. Owns communicator protocol resources,
// view-epoch synchronization, invocation/recovery state, control-update
// publication, and graph references. StrongStream supplies device-wide
// ordering.
class DeviceCollectiveRuntime {
   public:
    struct Peer {
        GlobalRank global_rank = kInvalidGlobalRank;
        InGroupRank in_group_rank = kInvalidInGroupRank;
        DeviceGroupEndpoint endpoint;
        DeviceCollectiveWorkspaceEndpoint workspace;
        // Remote word written by this communicator's rank.
        uint64_t view_epoch_signal_offset = 0;
    };

    // Host-only input to protocol binding and algorithm Plan construction.
    // Runtime resolves active peers and View-epoch bindings; protocols
    // interpret their own endpoints. Algorithms choose connections and payload
    // layout.
    struct ResolvedGroupView {
        uint64_t epoch = 0;
        int32_t self_active_index = -1;
        uint64_t buffer_size = 0;
        std::vector<Peer> participants;
    };

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

    PGResult<void> useLocalOnly(uint64_t view_epoch);
    PGResult<void> applyGroupView(const GroupView& view);

    PGResult<void> enableRecovery(DeviceCollectiveRecoveryWorker& worker,
                                  FailureRecoveryCallback callback);

    PGResult<void> enqueueAllReduce(const void* send_buffer, void* recv_buffer,
                                    size_t count, DataType datatype,
                                    ReduceOp op,
                                    cudaStream_t user_stream_handle,
                                    int32_t* failed_ranks_hint);

    PGResult<void> shutdown();

   private:
    friend class MooncakeCommunicator;

    DeviceCollectiveRuntime(DeviceTransferService& transfer_service,
                            DeviceCollectiveWorkspace& workspace,
                            int device_index, InGroupRank self_rank,
                            uint32_t max_group_size,
                            int32_t* active_ranks_mirror,
                            RegionSlice view_epoch_signals,
                            StrongStream& strong_stream,
                            GpuEvent handoff_event);

    PGResult<void> attachGraphUse(const GpuCaptureInfo& capture);
    PGResult<ResolvedGroupView> resolveGroupView(const GroupView& view) const;
    [[nodiscard]] DeviceAllReduceAlgorithm allReduceAlgorithm(
        size_t bytes, DataType datatype, ReduceOp op) const noexcept;
    [[nodiscard]] bool hasPendingRecovery() const noexcept;
    PGResult<void> publishControlState(bool pinned,
                                       bool include_active_ranks_mirror);
    PGResult<void> prepareFailureResume(const CollectiveFailureReport& failure);
    void releaseState() noexcept;

    DeviceTransferService& transfer_service_;
    DeviceCollectiveWorkspace& workspace_;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    RegionSlice view_epoch_signals_;
    InvocationState* invocation_state_ = nullptr;
    // Protocol resources outlive all algorithms borrowing them.
    std::unique_ptr<SimpleResources> simple_;
    std::unique_ptr<LLResources> ll_;
    std::unique_ptr<RingAllReduceAlgorithm> ring_all_reduce_;
    std::unique_ptr<OneShotAllReduceAlgorithm> one_shot_all_reduce_;
    std::vector<DeviceAllReduceAlgorithmChoice> all_reduce_algorithm_choices_;
    StrongStream& strong_stream_;
    ControlMailbox* control_mailbox_ = nullptr;
    int32_t* active_ranks_mirror_ = nullptr;
    size_t active_ranks_count_ = 0;
    std::array<int32_t, kMaxNumRanks> host_active_ranks_{};
    FailureRecoveryCallback failure_recovery_callback_;
    DeviceCollectiveRecoveryWorker* recovery_worker_ = nullptr;

    GpuEvent handoff_event_;
    mutable std::mutex mutex_;
    std::atomic<size_t> live_graph_uses_{0};
    uint64_t view_epoch_ = kInvalidViewEpoch;
    bool shutdown_requested_ = false;
    bool shutdown_complete_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
