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
    void* buffer_;
    MooncakeWorker worker_{&engine_};
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
