#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <memory>
#include <mooncake_worker.cuh>
#include <connection_poller.h>
#include <p2p_proxy.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <transfer_engine.h>

namespace mooncake {

class MooncakeBackend final : public ::c10d::Backend {
   public:
    struct MooncakeBackendOptions final : ::c10d::Backend::Options {
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : Options{"mooncake"}, activeRanks_{activeRanks} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension)
            : Options{"mooncake"},
              activeRanks_{activeRanks},
              isExtension_{isExtension} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
        bool isExtension_ = false;
    };

    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    bool isCpu = false);

    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

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

    static void setHostIp(const std::string& hostIp) { hostIp_ = hostIp; }

    static void setDeviceFilter(std::vector<std::string> filters) {
        engine_->setWhitelistFilters(std::move(filters));
    }

    std::string getPreferredHca(std::string location) {
        auto matrix = engine_->getLocalTopology()->getMatrix();
        auto it = matrix.find(location);
        if (it == matrix.end()) {
            LOG(INFO) << "Topology is "
                      << engine_->getLocalTopology()->toJson();
            LOG(ERROR) << "Topology entry not found for location: " << location;
        } else if (it->second.preferred_hca.empty()) {
            LOG(INFO) << "Topology is "
                      << engine_->getLocalTopology()->toJson();
            LOG(ERROR) << "Preferred HCA list is empty for location: "
                       << location;
        }
        return it->second.preferred_hca[0];
    }

    at::Tensor getActiveRanksTensor() { return meta_->activeRanksTensor; }

    int getNumSyncedRanks();

    void extendGroupSizeTo(int size);

    std::vector<bool> getPeerState(const std::vector<int>& ranks);

    void recoverRanks(const std::vector<int>& ranks);

   private:
    static TransferEngine* engine_;
    static bool engineInitialized_;
    static int backendIndex_;
    bool isCpu_{false};
    static std::string hostIp_;
    void* send_buffer_[2];
    void* recv_buffer_[2];
    int32_t* cpu_sync_send_region_[2];
    int32_t* cpu_sync_recv_region_[2];
    static MooncakeWorker worker_;
    SegmentInfo rank_info;
    std::shared_ptr<TransferGroupMeta> meta_;
    bool isShutdown_{false};

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
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
