#ifndef MOONCAKE_PG_COMMUNICATOR_H
#define MOONCAKE_PG_COMMUNICATOR_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <transfer_engine.h>

#include "control_plane/agent_host.h"
#include "control_plane/coordinator_host.h"
#include "control_plane/link_manager.h"
#include "error_types.h"
#include "mooncake_pg.h"
#include "mooncake_worker.cuh"
#include "p2p_proxy.h"
#include "comm_types.h"

namespace mooncake {

static constexpr size_t kDefaultCollectiveTimeoutUs = 10000000;  // 10 s
static constexpr int64_t kDefaultP2PTimeoutUs = 10000000;        // 10 s

// Must be greater than collective_timeout_us so that timeout-based
// failure reporters can contribute before the reconciliation window
// expires. (Some ranks report failures based on timeout, while others
// report based on failure status.)
static constexpr int64_t kDefaultFaultReconciliationWindowUs =
    3 * kDefaultCollectiveTimeoutUs;

struct MooncakePGContext {
    std::string host_ip = "127.0.0.1";
    size_t collective_timeout_us = kDefaultCollectiveTimeoutUs;
    int64_t p2p_timeout_us = kDefaultP2PTimeoutUs;
    int64_t fault_reconciliation_window_us =
        kDefaultFaultReconciliationWindowUs;

    std::unique_ptr<TransferEngine> owned_engine =
        std::make_unique<TransferEngine>(true);
    TransferEngine* engine = owned_engine.get();
    bool engine_initialized = false;
    int global_rank = -1;
    int max_world_size = 0;

    LinkManager link_manager;
    MooncakeWorkerManager worker_manager;
    P2PDeviceWorkerManager p2p_device_worker_manager;
    // Coordinator (rank 0 only).
    // It must be started before the local AgentHost connects to it.
    std::unique_ptr<CoordinatorHost> coordinator_host;
    std::unique_ptr<AgentHost> agent_host;

    MooncakePGContext() = default;
    ~MooncakePGContext();

    // Non-copyable: engine points to either owned_engine or an external engine
    // whose lifetime is controlled by the caller.
    MooncakePGContext(const MooncakePGContext&) = delete;
    MooncakePGContext& operator=(const MooncakePGContext&) = delete;

    PGResult<void> initialize(int rank, int world_size);
    PGResult<std::string> launchCoordinator();
    PGResult<void> connectCoordinator(const std::string& coordinator_address);
    PGResult<void> setHostIp(std::string value);
    PGResult<void> setExternalEngine(TransferEngine* transfer_engine);
    PGResult<void> setDeviceFilter(std::vector<std::string> filters);
    PGResult<void> setCollectiveTimeout(size_t timeout_us);
    PGResult<void> setP2PTimeout(int64_t timeout_us);
    PGResult<void> setFaultReconciliationWindow(int64_t timeout_us);
    PGResult<void> incrementCommUseCount();
    void decrementCommUseCount() noexcept;
    PGResult<void> shutdown();

   private:
    PGResult<void> checkRunning() const;

    std::vector<std::string> device_filters_;
    std::mutex state_mutex_;
    size_t comm_use_count_ = 0;
    bool initialized_ = false;
    bool shutdown_requested_ = false;
};

struct MooncakeCommunicatorConfig {
    int rank = 0;
    int size = 1;
    int max_group_size = -1;
    std::vector<GlobalRank> global_ranks;
    GroupBootstrapId group_bootstrap_id;
    bool is_cpu = false;
    int device_index = -1;
    GroupBootstrapIdResolvePolicy group_resolve_policy =
        GroupBootstrapIdResolvePolicy::CreateOrAttach;
    bool auto_deactivate_on_failure = true;
    bool auto_sync_on_failure = true;

    // Optional caller-owned mirror of the communicator's active ranks. Its
    // memory location is independent of the communicator's device.
    int32_t* active_ranks_mirror = nullptr;
    size_t active_ranks_mirror_count = 0;
    bool active_ranks_mirror_is_device = false;
    int active_ranks_mirror_device_index = -1;
};

class MooncakeCommunicator {
   public:
    static PGResult<std::unique_ptr<MooncakeCommunicator>> create(
        MooncakePGContext& context, MooncakeCommunicatorConfig config);
    ~MooncakeCommunicator();

    MooncakeCommunicator(const MooncakeCommunicator&) = delete;
    MooncakeCommunicator& operator=(const MooncakeCommunicator&) = delete;

    int getRank() const { return rank_; }
    int getSize() const;
    int getMaxGroupSize() const { return max_group_size_; }
    bool isCpu() const { return is_cpu_; }

    PGResult<std::unique_ptr<WorkCompletion>> sendCpu(
        const void* buffer, size_t count, DataType datatype, int peer,
        int32_t* failed_ranks_hint, size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> sendGpu(
        const void* buffer, size_t count, DataType datatype, int peer,
        cudaStream_t stream, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> recvCpu(
        void* buffer, size_t count, DataType datatype, int peer,
        int32_t* failed_ranks_hint, size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> recvGpu(
        void* buffer, size_t count, DataType datatype, int peer,
        cudaStream_t stream, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);

    PGResult<std::unique_ptr<WorkCompletion>> broadcastCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, int root, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> broadcastGpu(const void* send_buffer, void* recv_buffer,
                                size_t count, DataType datatype, int root,
                                cudaStream_t stream, int32_t* failed_ranks_hint,
                                size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> allReduceCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, ReduceOp op, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> allReduceGpu(const void* send_buffer, void* recv_buffer,
                                size_t count, DataType datatype, ReduceOp op,
                                cudaStream_t stream, int32_t* failed_ranks_hint,
                                size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> allGatherCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> allGatherGpu(const void* send_buffer, void* recv_buffer,
                                size_t count, DataType datatype,
                                cudaStream_t stream, int32_t* failed_ranks_hint,
                                size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> reduceScatterCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, ReduceOp op, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> reduceScatterGpu(const void* send_buffer, void* recv_buffer,
                                    size_t count, DataType datatype,
                                    ReduceOp op, cudaStream_t stream,
                                    int32_t* failed_ranks_hint,
                                    size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> allToAllCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> allToAllGpu(const void* send_buffer, void* recv_buffer,
                               size_t count, DataType datatype,
                               cudaStream_t stream, int32_t* failed_ranks_hint,
                               size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> barrierCpu(
        int32_t* failed_ranks_hint, size_t failed_ranks_hint_count);
    PGResult<void> barrierGpu(cudaStream_t stream, int32_t* failed_ranks_hint,
                              size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> reduceCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, ReduceOp op, int root, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> reduceGpu(const void* send_buffer, void* recv_buffer,
                             size_t count, DataType datatype, ReduceOp op,
                             int root, cudaStream_t stream,
                             int32_t* failed_ranks_hint,
                             size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> gatherCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, int root, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> gatherGpu(const void* send_buffer, void* recv_buffer,
                             size_t count, DataType datatype, int root,
                             cudaStream_t stream, int32_t* failed_ranks_hint,
                             size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> scatterCpu(
        const void* send_buffer, void* recv_buffer, size_t count,
        DataType datatype, int root, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<void> scatterGpu(const void* send_buffer, void* recv_buffer,
                              size_t count, DataType datatype, int root,
                              cudaStream_t stream, int32_t* failed_ranks_hint,
                              size_t failed_ranks_hint_count);

    PGResult<void> shutdown();
    std::vector<int32_t> getActiveRanks() const;
    int getNumSyncedRanks() const;
    PGResult<std::vector<bool>> getPeerState(
        const std::vector<int>& ranks) const;
    PGResult<ProposeViewUpdateResponse> activateRanks(
        const std::vector<int>& ranks);
    PGResult<ProposeViewUpdateResponse> deactivateRanks(
        const std::vector<int>& ranks);
    PGResult<void> joinGroup();

    // Returns the current GroupView epoch.
    // Epoch starts at 0 (bootstrap) and increments on membership changes,
    // auto-deactivation, and recovery.
    uint64_t getCurrentEpoch() const;

    // Notify the Coordinator of a detected failure and block until a membership
    // decision has been made and the Agent has ACKed the resulting ViewUpdate.
    PGResult<SyncAfterFailureResponse> syncAfterFailure();

    // Update the data-plane view. Called by AgentHost when a ViewUpdatePush is
    // received or rank states change. rank_states and activatable are computed
    // by the state machine.
    void applyViewUpdate(const GroupView& view,
                         const std::vector<RankState>& rank_states,
                         const std::vector<uint64_t>& rank_epochs,
                         const std::vector<bool>& activatable);
    // Called by AgentHost when a TE link to a peer comes back up.
    void onPeerLinkReset(InGroupRank peer);

    // Called by NotifyLinkRefreshed effect: refresh the cached TE segment ID
    // for `local` (InGroupRank) from the LinkManager. If the link is not up,
    // segmentID is set to -1.
    void refreshSegmentID(InGroupRank local);
    GroupEndpointPublication buildEndpointMetadata() const;

    AgentInterface& getAgent() { return agent_; }

   private:
    MooncakeCommunicator(MooncakePGContext& context,
                         const MooncakeCommunicatorConfig& config);
    PGResult<void> initialize(MooncakeCommunicatorConfig config);

    PGResult<std::unique_ptr<WorkCompletion>> enqueueSend(
        const void* buffer, size_t count, DataType datatype, int peer,
        cudaStream_t stream, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);
    PGResult<std::unique_ptr<WorkCompletion>> enqueueRecv(
        void* buffer, size_t count, DataType datatype, int peer,
        cudaStream_t stream, int32_t* failed_ranks_hint,
        size_t failed_ranks_hint_count);

    // Guard: checks that the rank is not Offline (always) and, for collectives,
    // that it is active in this group. Called at the top of every operation.
    PGResult<void> checkOpState(OpType op) const;

    // Validate and initialize the caller-owned failed-ranks output.
    PGResult<void> initializeFailedRanksHint(
        int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) const;

    // Reject operations if this communicator is invalid.
    PGResult<void> checkValidGroup(const char* operation) const;

    // A rejected registration has no Coordinator-assigned group id and is
    // restricted to local-only collectives.
    bool isValidGroup() const { return meta_ && !meta_->group_id.empty(); }

    // Sync the caller-provided host/device active-ranks mirror from the current
    // GroupView.
    void syncActiveRanksMirror() const;

    MooncakePGContext& context_;
    AgentInterface& agent_;
    int rank_ = 0;
    int initial_size_ = 1;
    int max_group_size_ =
        1;  // per-group capacity (max active members for this group)
    int device_index_ = -1;
    bool is_cpu_ = false;
    bool is_shutdown_ = false;
    int32_t* active_ranks_mirror_ = nullptr;
    bool active_ranks_mirror_is_device_ = false;
    int active_ranks_mirror_device_index_ = -1;
    std::optional<GpuStream> active_ranks_mirror_stream_;

    std::shared_ptr<MooncakeWorker> worker_;
    std::array<void*, 2> send_buffer_{};
    std::array<void*, 2> recv_buffer_{};
    std::array<int32_t*, 2> cpu_sync_send_region_{};
    std::array<int32_t*, 2> cpu_sync_recv_region_{};
    std::shared_ptr<TransferGroupMeta> meta_;

    // P2P async infrastructure. p2p_proxy_ is created by this communicator but
    // can live longer because P2PDeviceWorker retains it until all transfers
    // complete.
    std::shared_ptr<P2PProxy> p2p_proxy_;

    // Created by P2PDeviceWorkerManager and shared between communicators on the
    // same device.
    std::shared_ptr<P2PDeviceWorker> p2p_device_worker_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COMMUNICATOR_H
