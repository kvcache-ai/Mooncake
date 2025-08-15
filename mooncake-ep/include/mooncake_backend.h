#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <transfer_engine.h>

namespace mooncake {

class MooncakeBackend final : public ::c10d::Backend {
   public:
    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<Options> options);

    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllgatherOptions& opts) override;

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const c10d::AllreduceOptions& opts) override;

   private:
    TransferEngine engine_{true};
    std::string p2p_ip_ = "127.0.0.1";
    int device_id_;
    void* send_buffer_;
    void* recv_buffer_;
    int32_t* cpu_sync_send_region_;
    int32_t* cpu_sync_recv_region_;
    MooncakeWorker worker_;
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
