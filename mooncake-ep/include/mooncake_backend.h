#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <transfer_engine.h>

namespace mooncake {

class MooncakeBackend final : public ::c10d::Backend {
   public:
    struct MooncakeBackendOptions final : ::c10d::Backend::Options {
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : Options{"mooncake"}, activeRanks_{activeRanks} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
    };

    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    bool isCpu = false);

    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

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

    static void setHostIp(const std::string& hostIp) { hostIp_ = hostIp; }

    static void setDeviceFilter(std::vector<std::string> filters) {
        engine_.setWhitelistFilters(std::move(filters));
    }

    std::string getPreferredHca(std::string location) {
        auto matrix = engine_.getLocalTopology()->getMatrix();
        return matrix[location].preferred_hca[0];
    }

   private:
    static TransferEngine engine_;
    static Transport* transport_;
    static int backendIndex_;
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
