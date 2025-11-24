#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <transfer_engine.h>

namespace mooncake {

class MooncakeBackendImpl {
   public:
    MooncakeBackendImpl(c10::intrusive_ptr<::c10d::Store> store, int rank,
                        int size, at::Tensor activeRanks, bool isCpu = false);

    ~MooncakeBackendImpl() = default;

    c10::intrusive_ptr<c10d::Work> broadcast(
        std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts);

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts);

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllgatherOptions& opts);

    c10::intrusive_ptr<c10d::Work> _allgather_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::AllgatherOptions& opts);

    c10::intrusive_ptr<c10d::Work> _reduce_scatter_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::ReduceScatterOptions& opts);

    c10::intrusive_ptr<c10d::Work> alltoall(
        std::vector<at::Tensor>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllToAllOptions& opts);

    c10::intrusive_ptr<c10d::Work> barrier(const c10d::BarrierOptions& opts);

    void shutdown();

    static void setHostIp(const std::string& hostIp) { hostIp_ = hostIp; }

    static void setDeviceFilter(std::vector<std::string> filters) {
        engine_.setWhitelistFilters(std::move(filters));
    }

    std::string getPreferredHca(std::string location) {
        auto matrix = engine_.getLocalTopology()->getMatrix();
        return matrix[location].preferred_hca[0];
    }

    at::Tensor getActiveRanksTensor() { return meta_.activeRanksTensor; }

    static TransferEngine engine_;
    static Transport* transport_;
    static int backendIndex_;
    int rank_;
    int size_;
    bool isCpu_{false};
    static std::string hostIp_;
    void* send_buffer_[2];
    void* recv_buffer_[2];
    int32_t* cpu_sync_send_region_[2];
    int32_t* cpu_sync_recv_region_[2];
    static MooncakeWorker worker_;
    TransferGroupMeta meta_;
};
}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
