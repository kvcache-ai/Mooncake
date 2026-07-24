#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <array>
#include <vector>
#include <mooncake_worker.cuh>
#include <work_handles.h>
#include <p2p_proxy.h>
#include <sys/types.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <transfer_engine.h>

#include <ATen/cuda/CUDAContext.h>

#include "control_plane/coordinator_host.h"
#include "control_plane/link_manager.h"

namespace mooncake {

static constexpr size_t kDefaultCollectiveTimeoutUs = 10000000;  // 10 s
static constexpr int64_t kDefaultP2PTimeoutUs = 10000000;        // 10 s

// Must be greater than collective_timeout_us so that timeout-based
// failure reporters can contribute before the reconciliation window
// expires. (Some ranks report failures based on timeout, while others
// report based on failure status.)
static constexpr int64_t kDefaultFaultReconciliationWindowUs =
    3 * kDefaultCollectiveTimeoutUs;

class AgentInterface;
class AgentHost;

struct MooncakeProcessContext {
    TransferEngine* external_engine = nullptr;
    std::string host_ip = "127.0.0.1";
    size_t collective_timeout_us = kDefaultCollectiveTimeoutUs;
    int64_t p2p_timeout_us = kDefaultP2PTimeoutUs;
    int64_t fault_reconciliation_window_us =
        kDefaultFaultReconciliationWindowUs;

    // Eagerly created so set_device_filter works before init_process_group.
    // engine points to either owned_engine or external_engine.
    std::unique_ptr<TransferEngine> owned_engine =
        std::make_unique<TransferEngine>(true);
    TransferEngine* engine = owned_engine.get();
    bool engine_initialized = false;
    int max_world_size = 0;

    LinkManager link_manager;
    MooncakeWorkerManager worker_manager;
    P2PDeviceWorkerManager p2p_device_worker_manager;

    // Coordinator (rank 0 only).  Created before AgentHost so the
    // coordinator_addr key is in the Store when AgentHost::start() reads it.
    std::unique_ptr<CoordinatorHost> coordinator_host;

    std::unique_ptr<AgentHost> agent_host;

    MooncakeProcessContext();
    ~MooncakeProcessContext();

    // Non-copyable, non-movable: engine pointer points to either owned_engine
    // or external_engine, and would dangle after a move.
    MooncakeProcessContext(const MooncakeProcessContext&) = delete;
    MooncakeProcessContext& operator=(const MooncakeProcessContext&) = delete;
    MooncakeProcessContext(MooncakeProcessContext&&) = delete;
    MooncakeProcessContext& operator=(MooncakeProcessContext&&) = delete;
};

// Forward declaration – MooncakeP2PShim holds a non-owning pointer to
// MooncakeBackend, which is defined below.
class MooncakeBackend;

// Lightweight Backend shim that delegates P2P send/recv back to the owning
// MooncakeBackend.  PyTorch's P2P dispatch (batch_isend_irecv, isend, irecv)
// requires getBackend() to return a registered c10d::Backend instance.
// Since MooncakeBackend inherits from ProcessGroup (not Backend), we register
// this shim in the ProcessGroup's deviceTypeToBackend_ map so that the P2P
// path can find it.  The shim holds a non-owning pointer to its owner and
// delegates only the operations that the P2P dispatch path calls (send, recv,
// getBackendName, supportsCoalescing).
class MooncakeP2PShim final : public ::c10d::Backend {
   public:
    MooncakeP2PShim(MooncakeBackend* owner, int maxGroupSize);

    const std::string getBackendName() const override;

    bool supportsCoalescing() const override { return false; }

    c10::intrusive_ptr<c10d::Work> send(std::vector<at::Tensor>& tensors,
                                        int dstRank, int tag) override;

    c10::intrusive_ptr<c10d::Work> recv(std::vector<at::Tensor>& tensors,
                                        int srcRank, int tag) override;

    c10::intrusive_ptr<c10d::Work> recvAnysource(
        std::vector<at::Tensor>& tensors, int tag) override;

    c10::intrusive_ptr<c10d::Work> barrier(
        const c10d::BarrierOptions& opts) override;

   private:
    // Non-owning: the shim is stored in ProcessGroup's backend maps which are
    // cleared on destruction, and MooncakeBackend always outlives the shim.
    MooncakeBackend* owner_;
};

class MooncakeBackend final : public ::c10d::ProcessGroup {
   public:
    struct MooncakeBackendOptions final : torch::CustomClassHolder {
        explicit MooncakeBackendOptions(int maxGroupSize)
            : maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1} {}

        MooncakeBackendOptions(int maxGroupSize, bool autoDeactivateOnFailure,
                               bool autoSyncOnFailure)
            : maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1},
              autoDeactivateOnFailure_{autoDeactivateOnFailure},
              autoSyncOnFailure_{autoSyncOnFailure} {}

        // If activeRanks is provided, only its storage is used -- the contents
        // are populated by the Coordinator.
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : activeRanks_{activeRanks} {}

        // Deprecated constructors: isExtension is ignored
        MooncakeBackendOptions(at::Tensor activeRanks, bool /*isExtension*/)
            : activeRanks_{activeRanks} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool /*isExtension*/,
                               int maxGroupSize)
            : activeRanks_{activeRanks},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool /*isExtension*/,
                               int maxGroupSize, bool autoDeactivateOnFailure)
            : activeRanks_{activeRanks},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1},
              autoDeactivateOnFailure_{autoDeactivateOnFailure} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool /*isExtension*/,
                               int maxGroupSize, bool autoDeactivateOnFailure,
                               bool autoSyncOnFailure)
            : activeRanks_{activeRanks},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1},
              autoDeactivateOnFailure_{autoDeactivateOnFailure},
              autoSyncOnFailure_{autoSyncOnFailure} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;

        int maxGroupSize_ = -1;

        // Automatically deactivate failed ranks on timeout / operation failure.
        //
        // When true (default), failed ranks are removed from the active set
        // automatically.  When false, failures are only reported through
        // per-operation failedRanks hints, so the caller can decide how to
        // handle the failure.
        //
        // Default: MOONCAKE_PG_AUTO_DEACTIVATE_ON_FAILURE (1)
        bool autoDeactivateOnFailure_ = true;

        // Fence a failed collective on Coordinator reconciliation.
        //
        // When true (default), the worker reports a locally detected transfer
        // failure and waits for the authoritative membership view to be
        // applied before completing the task. Consequently, CPU work and
        // CUDA stream execution cannot pass the failed collective before the
        // view is updated. CUDA Work::wait() itself remains asynchronous and
        // does not imply that the host can immediately observe the new view.
        //
        // Requires autoDeactivateOnFailure_ == true.
        //
        // Default: MOONCAKE_PG_AUTO_SYNC_ON_FAILURE (1)
        bool autoSyncOnFailure_ = true;
    };

    /**
     * @brief Construct a Mooncake process-group backend instance.
     *
     * `distBackendOpts` contains the PyTorch process-group information for this
     * backend instance. `options` contains Mooncake-specific settings and may
     * be null when callers omit `pg_options`.
     *
     * @param distBackendOpts Process-group information supplied by PyTorch.
     * @param options *Optional* Mooncake-specific backend options.
     * @param agent Reference to the process-level AgentInterface instance.
     * @param isCpu Whether to initialize the CPU backend variant.
     */
    MooncakeBackend(c10d::DistributedBackendOptions distBackendOpts,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    AgentInterface& agent, struct MooncakeProcessContext& ctx,
                    bool isCpu = false);

    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

    // In Normal mode, return the active rank-space extent (highest active
    // InGroupRank plus one). During bootstrap, PyTorch still needs the
    // group_size declared at construction to validate future ranks passed to
    // new_group() before joinGroup() can run:
    // https://github.com/pytorch/pytorch/blob/release/2.13/torch/distributed/distributed_c10d.py#L6012
    int getSize() const override {
        if (!meta_) return size_;
        if (meta_->extensionMode.load(std::memory_order_acquire) !=
            CollectiveExtensionState::Normal) {
            return size_;
        }
        return meta_->activeSize.load(std::memory_order_acquire);
    }

    // Point-to-point send/recv for torch.distributed P2POp/batch_isend_irecv.
    // Only single-tensor ops are supported.
    c10::intrusive_ptr<c10d::Work> send(std::vector<at::Tensor>& tensors,
                                        int dstRank, int tag) override;

    c10::intrusive_ptr<c10d::Work> recv(std::vector<at::Tensor>& tensors,
                                        int srcRank, int tag) override;

    c10::intrusive_ptr<c10d::Work> broadcast(
        std::vector<at::Tensor>& tensors,
        const c10d::BroadcastOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const c10d::AllreduceOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllgatherOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> _allgather_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::AllgatherOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> _reduce_scatter_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::ReduceScatterOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> alltoall(
        std::vector<at::Tensor>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllToAllOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> barrier(
        const c10d::BarrierOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> reduce(
        std::vector<at::Tensor>& tensors,
        const c10d::ReduceOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> gather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::GatherOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> scatter(
        std::vector<at::Tensor>& outputTensors,
        std::vector<std::vector<at::Tensor>>& inputTensors,
        const c10d::ScatterOptions& opts) override;

    void shutdown() override;

    std::string getPreferredHca(std::string location) {
        static std::once_flag topo_once;
        static std::shared_ptr<Topology> topology;
        static TopologyMatrix matrix;
        std::call_once(topo_once, [this] {
            // FIXME: getLocalTopology is deprecated in TENT
            topology = ctx_.engine->getLocalTopology();
            if (topology) {
                matrix = topology->getMatrix();
            }
            if (!topology || matrix.empty()) {
                topology = std::make_shared<Topology>();
                topology->discover();
                matrix = topology->getMatrix();
            }
        });

        auto it = matrix.find(location);
        if (it == matrix.end()) {
            LOG(INFO) << "Topology is " << topology->toJson();
            LOG(ERROR) << "Topology entry not found for location: " << location;
            return "";
        }
        if (it->second.preferred_hca.empty()) {
            LOG(INFO) << "Topology is " << topology->toJson();
            LOG(ERROR) << "Preferred HCA list is empty for location: "
                       << location;
            return "";
        }
        return it->second.preferred_hca[0];
    }

    at::Tensor getActiveRanksTensor() { return meta_->activeRanksTensor; }

    int getNumSyncedRanks();

    void extendGroupSizeTo(int size);

    std::vector<bool> getPeerState(const std::vector<int>& ranks);

    // alias to activateRanks
    ProposeViewUpdateResponse recoverRanks(const std::vector<int>& ranks);

    ProposeViewUpdateResponse activateRanks(const std::vector<int>& ranks);

    ProposeViewUpdateResponse deactivateRanks(const std::vector<int>& ranks);

    void joinGroup();

    // Update the data plane view.
    // Called by AgentHost when a ViewUpdatePush is received or rank states
    // change.  rank_states and activatable are computed by the state
    // machine.
    void applyViewUpdate(const GroupView& view,
                         const std::vector<RankState>& rank_states,
                         const std::vector<uint64_t>& rank_epochs,
                         const std::vector<bool>& activatable);

    // Returns the current GroupView epoch.
    // Epoch starts at 0 (bootstrap) and increments on membership changes,
    // auto-deactivation, and recovery.
    uint64_t getCurrentEpoch() const {
        return meta_ ? meta_->epoch.load(std::memory_order_acquire) : 0;
    }

    // Called by AgentHost when a TE link to peer goes up
    void onPeerLinkReset(InGroupRank peer);

    // Called by NotifyLinkRefreshed effect: refresh the cached TE segment
    // ID for `local` (InGroupRank) from the LinkManager.
    // If the link is not up, segmentID is set to -1.
    void refreshSegmentID(InGroupRank local);

    // Notify the Coordinator of a detected failure and block until a membership
    // decision has been made and the Agent has ACKed the resulting ViewUpdate.
    SyncAfterFailureResponse syncAfterFailure();

    // Sync the activeRanksTensor on CPU/GPU from the current GroupView.
    void syncActiveRanksTensor();

    GroupEndpointPublication buildEndpointMetadata() const;

    // Guard: checks that the rank is Healthy (always) and, for collectives,
    // that it is active in this group.  Called at the top of every operation.
    void prepareOp(c10d::OpType op) const;

    const GroupEndpointInfo& getLocalEndpointInfo() const {
        return meta_->segmentInfos[meta_->rank];
    }
    AgentInterface& getAgent() { return agent_; }
    MooncakeProcessContext& getProcessContext() { return ctx_; }
    const MooncakeProcessContext& getProcessContext() const { return ctx_; }

   private:
    MooncakeProcessContext& ctx_;
    std::shared_ptr<MooncakeWorker> worker_;
    const c10::intrusive_ptr<MooncakeBackendOptions> options_;
    bool isCpu_{false};
    void* send_buffer_[2];
    void* recv_buffer_[2];
    int32_t* cpu_sync_send_region_[2];
    int32_t* cpu_sync_recv_region_[2];
    AgentInterface& agent_;
    std::shared_ptr<TransferGroupMeta> meta_;
    bool isShutdown_{false};
    int max_group_size_ =
        0;  // per-group capacity (max active members for this group)

    // P2P async infrastructure
    // p2p_proxy_ is created in MooncakeBackend, but can live longer than
    // MooncakeBackend. Because it is shared in P2PDeviceWorker, which must
    // ensure P2PProxy's resources are not released until all transfers are
    // completed.
    std::shared_ptr<P2PProxy> p2p_proxy_;
    // p2p_device_worker_ is created in P2PDeviceWorkerManager,
    // and is shared between backends in the same device.
    std::shared_ptr<P2PDeviceWorker> p2p_device_worker_;
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
