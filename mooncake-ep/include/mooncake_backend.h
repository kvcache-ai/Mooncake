#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>

namespace mooncake {

class MooncakeBackend : public ::c10d::Backend {
   public:
    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<Options> options);

    const std::string getBackendName() const override;

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const c10d::AllreduceOptions& opts) override;
};

class MooncakeWork : public ::c10d::Work {
   public:
    MooncakeWork(c10d::OpType opType,
                 c10::intrusive_ptr<c10::ivalue::Future> future)
        : Work(-1, opType), future_(std::move(future)) {}

    bool isCompleted() override { return future_->completed(); }

    bool isSuccess() const override {
        return future_->completed() && !future_->hasError();
    }

    bool wait(std::chrono::milliseconds timeout) override {
        future_->wait();
        return isSuccess();
    }

    c10::intrusive_ptr<c10::ivalue::Future> getFuture() override {
        return future_;
    }

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
