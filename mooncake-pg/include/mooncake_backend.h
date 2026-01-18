#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <transfer_engine.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

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

    void shutdown() override;

    static void setHostIp(const std::string& hostIp) { hostIp_ = hostIp; }

    static void setDeviceFilter(std::vector<std::string> filters) {
        engine_.setWhitelistFilters(std::move(filters));
    }

    std::string getPreferredHca(std::string location) {
        auto matrix = engine_.getLocalTopology()->getMatrix();
        return matrix[location].preferred_hca[0];
    }

    at::Tensor getActiveRanksTensor() { return meta_.activeRanksTensor; }

    int getNumSyncedRanks();

    void extendGroupSizeTo(int size);

    std::vector<bool> getPeerState(const std::vector<int>& ranks);

    void recoverRanks(const std::vector<int>& ranks);

   private:
    enum class P2POpType { SEND, RECV };

    struct P2POp {
        P2POpType opType;
        at::Tensor tensor;  // For SEND: contiguous tensor to send; For RECV:
                            // contiguous target tensor
        at::Tensor originalTensor;  // For RECV: original (possibly
                                    // non-contiguous) tensor to update
        int peerRank;
        int tag;
        int64_t seq;  // Sequence number assigned at enqueue time for ordering
        std::shared_ptr<std::atomic<bool>> completed;
        std::shared_ptr<std::string> errorMsg;
    };

    void startP2PWorker();
    void stopP2PWorker();
    void p2PSendWorkerThread();
    void p2PRecvWorkerThread();
    void processSendOp(const P2POp& op);
    void processRecvOp(const P2POp& op);

    static TransferEngine engine_;
    static bool engineInitialized_;
    static int backendIndex_;
    bool isCpu_{false};
    static std::string hostIp_;
    void* send_buffer_[2];
    void* recv_buffer_[2];
    int32_t* cpu_sync_send_region_[2];
    int32_t* cpu_sync_recv_region_[2];
    int32_t* warmup_send_region_;
    int32_t* warmup_recv_region_;
    static MooncakeWorker worker_;
    SegmentInfo rank_info;
    TransferGroupMeta meta_;
    bool isShutdown_{false};
    int nextRankForConnection_ = 0;

    void connectionPoller(c10::intrusive_ptr<::c10d::Store> store,
                          int backendIndex);

    // P2P async infrastructure: separate queues and threads for send/recv
    std::queue<P2POp> p2pSendQueue_;
    std::mutex p2pSendQueueMutex_;
    std::condition_variable p2pSendQueueCv_;
    std::atomic<bool> p2pSendWorkerRunning_{false};
    std::thread p2pSendWorkerThread_;

    std::queue<P2POp> p2pRecvQueue_;
    std::mutex p2pRecvQueueMutex_;
    std::condition_variable p2pRecvQueueCv_;
    std::atomic<bool> p2pRecvWorkerRunning_{false};
    std::thread p2pRecvWorkerThread_;
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
