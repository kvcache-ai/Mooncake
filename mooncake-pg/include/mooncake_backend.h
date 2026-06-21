#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <mooncake_worker.cuh>
#include <connection_poller.h>
#include <p2p_proxy.h>
#include <sys/types.h>
#include <torch/torch.h>
#if __has_include(<torch/headeronly/version.h>)
#include <torch/headeronly/version.h>
#elif __has_include(<torch/version.h>)
#include <torch/version.h>
#endif
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <transfer_engine.h>

#include <ATen/cuda/CUDAContext.h>

namespace mooncake {

#if TORCH_VERSION_MAJOR == 2 && TORCH_VERSION_MINOR < 6
#define MOONCAKE_C10D_OVERRIDE
#else
#define MOONCAKE_C10D_OVERRIDE override
#endif

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

    bool supportsCoalescing() const MOONCAKE_C10D_OVERRIDE { return false; }

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
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : activeRanks_{activeRanks} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension)
            : activeRanks_{activeRanks}, isExtension_{isExtension} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension,
                               int maxWorldSize)
            : activeRanks_{activeRanks},
              isExtension_{isExtension},
              maxWorldSize_{maxWorldSize} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
        bool isExtension_ = false;
        // Optional upper bound for connection polling / reserved rank slots.
        // When > 0, the backend may pre-size internal rank metadata to this
        // value (while PyTorch's group_size() remains unchanged).
        int maxWorldSize_ = -1;
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
     * @param isCpu Whether to initialize the CPU backend variant.
     */
    MooncakeBackend(c10d::DistributedBackendOptions distBackendOpts,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    bool isCpu = false);

    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

    int getSize() const MOONCAKE_C10D_OVERRIDE {
        return meta_ ? meta_->activeSize : size_;
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

    void shutdown() MOONCAKE_C10D_OVERRIDE;

    static void setHostIp(const std::string& hostIp) { hostIp_ = hostIp; }

    static void setDeviceFilter(std::vector<std::string> filters) {
        engine_->setWhitelistFilters(std::move(filters));
    }

    /// Set an external TransferEngine to be used by MooncakeBackend
    /// instead of creating its own. Must be called before
    /// init_process_group(backend="mooncake"). The engine must already
    /// be initialized. The caller is responsible for ensuring the engine
    /// outlives all MooncakeBackend instances. Pass nullptr to reset.
    static void setExternalEngine(TransferEngine* engine);

    std::string getPreferredHca(std::string location) {
        static std::once_flag topo_once;
        static std::shared_ptr<Topology> topology;
        static TopologyMatrix matrix;
        std::call_once(topo_once, [this] {
            // FIXME: getLocalTopology is deprecated in TENT
            topology = engine_->getLocalTopology();
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

    void recoverRanks(const std::vector<int>& ranks);

    void joinGroup();

   private:
    void waitForExtensionState();
    void publishLocalPeerMetadata();
    void setLocalOnlyActiveRanks();
    void syncActiveRanksTensor();

    static TransferEngine* engine_;
    std::shared_ptr<MooncakeWorker> worker_;
    static bool engineInitialized_;
    static int backendIndex_;
    // External engine injection: when set, MooncakeBackend uses this engine
    // instead of the default self-created one. Non-owning pointer.
    // The caller is responsible for ensuring the engine outlives all
    // MooncakeBackend instances.
    static TransferEngine* externalEngine_;
    const c10::intrusive_ptr<MooncakeBackendOptions> options_;
    bool isCpu_{false};
    static std::string hostIp_;
    void* send_buffer_[2];
    void* recv_buffer_[2];
    int32_t* cpu_sync_send_region_[2];
    int32_t* cpu_sync_recv_region_[2];
    SegmentInfo rank_info;
    std::shared_ptr<TransferGroupMeta> meta_;
    bool isShutdown_{false};
    uint64_t local2global_rank_map_[kMaxNumRanks];
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

    // Connection Poller Context
    // Similar to p2p_proxy_, connection_ctx_ is created in MooncakeBackend, but
    // can live longer than MooncakeBackend.
    std::shared_ptr<ConnectionContext> connection_ctx_;
    bool connectionPollerRegistered_{false};
};

struct ExtensionState {
    std::vector<bool> activeRanks;
    std::vector<uint32_t> p2pEpochs;
    int taskCount = -1;
};
std::vector<uint8_t> serialize(const ExtensionState& state);
ExtensionState deserialize(const std::vector<uint8_t>& buffer);

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
