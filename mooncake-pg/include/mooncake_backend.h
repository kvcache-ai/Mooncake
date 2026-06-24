#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
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

#include "control_plane/agent_host.h"
#include "control_plane/coordinator_host.h"
#include "control_plane/link_manager.h"

namespace mooncake {

// =========================================================================
// MooncakeProcessContext — process-level container.
//
// Owned by pg_py.cpp (file-scope static).  Config fields can be set by Python
// setters BEFORE the first backend is created.  engine is eagerly allocated
// so that set_device_filter works pre-init.  engine->init() and agent_host
// are lazily initialised on the first createMooncakeBackend() call.
// =========================================================================

struct MooncakeProcessContext {
    // === Configuration (Python setters may modify before first backend) ===
    TransferEngine* external_engine = nullptr;
    std::string host_ip = "127.0.0.1";
    size_t collective_timeout_us =
        100000;                        // 100 ms (kDefaultCollectiveTimeoutUs)
    int64_t p2p_timeout_us = 3000000;  // 3 s (kDefaultP2PTimeoutUs)

    // === Runtime ===
    // Eagerly created so set_device_filter works before init_process_group.
    // engine points to either owned_engine or external_engine.
    std::unique_ptr<TransferEngine> owned_engine =
        std::make_unique<TransferEngine>(true);
    TransferEngine* engine = owned_engine.get();
    bool engine_initialized = false;
    int next_group_id = 0;

    // === Process-level subsystems (no more singletons) ===
    LinkManager link_manager;
    MooncakeWorkerManager worker_manager;
    P2PDeviceWorkerManager p2p_device_worker_manager;

    // Coordinator (rank 0 only).  Created before AgentHost so the
    // coordinator_addr key is in the Store when AgentHost::start() reads it.
    std::unique_ptr<CoordinatorHost> coordinator_host;

    std::unique_ptr<AgentHost> agent_host;

    MooncakeProcessContext() = default;

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
    explicit MooncakeP2PShim(MooncakeBackend* owner);

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
    static constexpr size_t kDefaultCollectiveTimeoutUs = 100000;  // 100 ms
    static constexpr int64_t kDefaultP2PTimeoutUs = 3000000;       // 3 s

    struct MooncakeBackendOptions final : torch::CustomClassHolder {
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : activeRanks_{activeRanks} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension)
            : activeRanks_{activeRanks}, isExtension_{isExtension} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension,
                               int maxWorldSize)
            : activeRanks_{activeRanks},
              isExtension_{isExtension},
              maxWorldSize_{maxWorldSize} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension,
                               int maxWorldSize, bool autoDeactivateOnFailure)
            : activeRanks_{activeRanks},
              isExtension_{isExtension},
              maxWorldSize_{maxWorldSize},
              autoDeactivateOnFailure_{autoDeactivateOnFailure} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
        bool isExtension_ = false;
        // Optional upper bound for connection polling / reserved rank slots.
        // When > 0, the backend may pre-size internal rank metadata to this
        // value (while PyTorch's group_size() remains unchanged).
        int maxWorldSize_ = -1;

        // Controls whether PG automatically deactivates failed ranks on
        // timeout or operation failure.
        //
        // When set to true (default), failed ranks are removed from the local
        // active rank automatically.
        // When set to false, PG only reports failures through per-operation
        // failedRanks, while leaving the active rank unchanged so that the
        // caller can decide whether and how to handle the failure.
        //
        // Note that the upper layer is responsible for maintaining active rank
        // consistency. Any automatic deactivation in PG is local to the current
        // rank only; no cross-rank synchronization is performed implicitly.
        bool autoDeactivateOnFailure_ = true;
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

    int getSize() const override { return meta_ ? meta_->activeSize : size_; }

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

    int getNumSyncedRanks() { return meta_ ? meta_->activeSize : 0; }

    void extendGroupSizeTo(int size);

    std::vector<bool> getPeerState(const std::vector<int>& ranks);

    void recoverRanks(const std::vector<int>& ranks);

    // alias to recoverRanks
    ProposeViewUpdateResponse activateRank(const std::vector<int>& ranks);

    ProposeViewUpdateResponse deactivateRank(const std::vector<int>& ranks);

    void joinGroup();

    // ---- New methods for Agent/Host integration ----

    // Atomically update the worker-visible group view (descriptor + members).
    // Called by AgentHost from the executor thread when a ViewUpdatePush
    // is received from the Coordinator.
    void applyViewChange(const GroupDescriptor& descriptor,
                         const GroupView& view);

    // Called by AgentHost when a TE link to `peer` goes down.
    // Resets the per-backend P2PProxy state for the affected peer.
    void onPeerLinkReset(GlobalRank peer);

    // Mark this backend's view as stale (called when Agent goes OFFLINE).
    void markViewStale();

    // Sync the activeRanksTensor on CPU/GPU from the current GroupView.
    void syncActiveRanksTensor();

    // Free meta_->activeRanks and reset device pointers.
    void destroyMeta();

    // Build a GroupEndpointMetadata for this backend's current local endpoint.
    // Called by AgentHost after (re-)registration to re-publish endpoints.
    GroupEndpointPublication buildEndpointMetadata() const;

    const GroupEndpointInfo& getLocalEndpointInfo() const {
        return meta_->segmentInfos[meta_->globalRank];
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
    std::string localServerName_;

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
