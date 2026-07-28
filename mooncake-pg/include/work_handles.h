#ifndef MOONCAKE_WORK_HANDLES_H
#define MOONCAKE_WORK_HANDLES_H

#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <ATen/cuda/CUDAContext.h>
#include <cstdint>

#include <atomic>
#include <memory>
#include <vector>

namespace mooncake {

struct TransferGroupMeta;
class MooncakeWorker;

// Per-operation failedRanksHint buffer
struct FailedRanksHint {
    at::Tensor tensor;

    FailedRanksHint() = default;
    explicit FailedRanksHint(at::Tensor tensor_in)
        : tensor(std::move(tensor_in)) {}

    FailedRanksHint(const FailedRanksHint&) = delete;
    FailedRanksHint& operator=(const FailedRanksHint&) = delete;

    FailedRanksHint(FailedRanksHint&& o) noexcept = default;
    FailedRanksHint& operator=(FailedRanksHint&& o) noexcept = default;
    ~FailedRanksHint() = default;

    int* data() { return tensor.data_ptr<int>(); }
    const int* data() const { return tensor.data_ptr<int>(); }

    bool isLocalSuccess(int size) const {
        const int* d = data();
        for (int i = 0; i < size; ++i) {
            if (d[i] != 0) return false;
        }
        return true;
    }

    static FailedRanksHint allocate(int n);
};

// Collective Work handles
class MooncakeWorkCpu : public ::c10d::Work {
   public:
    MooncakeWorkCpu(c10d::OpType opType,
                    c10::intrusive_ptr<c10::ivalue::Future> future,
                    std::shared_ptr<TransferGroupMeta> meta,
                    MooncakeWorker* worker, uint64_t hintRouteId,
                    FailedRanksHint failedRanksHint)
        : Work(-1, opType),
          future_(std::move(future)),
          meta_(std::move(meta)),
          worker_(worker),
          hintRouteId_(hintRouteId),
          failedRanksHint_(std::move(failedRanksHint)) {}

    ~MooncakeWorkCpu() override;

    bool isCompleted() override { return future_->completed(); }

    bool wait(std::chrono::milliseconds timeout) override;

    at::Tensor getFailedRanksHint() const;
    bool getLocalSuccess() const;

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
    std::shared_ptr<TransferGroupMeta> meta_;
    MooncakeWorker* worker_;
    uint64_t hintRouteId_;
    FailedRanksHint failedRanksHint_;
};

struct CudaTaskSubmissionToken {
    size_t task_id;
    uint64_t sequence;
};

class MooncakeWorkCuda : public ::c10d::Work {
   public:
    MooncakeWorkCuda(c10d::OpType opType, std::shared_ptr<torch::Event> event,
                     std::shared_ptr<TransferGroupMeta> meta,
                     MooncakeWorker* worker, uint64_t hintRouteId,
                     std::vector<CudaTaskSubmissionToken> submitted_tasks,
                     FailedRanksHint failedRanksHint)
        : Work(-1, opType),
          event_(std::move(event)),
          meta_(std::move(meta)),
          worker_(worker),
          hintRouteId_(hintRouteId),
          submitted_tasks_(std::move(submitted_tasks)),
          failedRanksHint_(std::move(failedRanksHint)) {}

    ~MooncakeWorkCuda() override;

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override;

    at::Tensor getFailedRanksHint() const;
    bool getLocalSuccess() const;

   protected:
    std::shared_ptr<torch::Event> event_;
    std::shared_ptr<TransferGroupMeta> meta_;
    MooncakeWorker* worker_;
    uint64_t hintRouteId_;
    std::vector<CudaTaskSubmissionToken> submitted_tasks_;
    FailedRanksHint failedRanksHint_;
};

class MooncakeBarrierWorkCuda : public MooncakeWorkCuda {
   public:
    using MooncakeWorkCuda::MooncakeWorkCuda;
    bool wait(std::chrono::milliseconds timeout) override;
};

// P2P Work handle
class MooncakeP2PWork : public ::c10d::Work {
   public:
    enum class Status : uint8_t { kPending = 0, kSuccess = 1, kFailed = 2 };

    explicit MooncakeP2PWork(std::shared_ptr<std::atomic<Status>> status,
                             FailedRanksHint failedRanksHint)
        : Work(-1, c10d::OpType::UNKNOWN),
          status_(status),
          failedRanksHint_(std::move(failedRanksHint)) {}

    bool isCompleted() override;
    bool isSuccess() const override;
    bool wait(std::chrono::milliseconds timeout) override;
    at::Tensor getFailedRanksHint() const { return failedRanksHint_.tensor; }
    bool getLocalSuccess() const;

   private:
    std::shared_ptr<std::atomic<Status>> status_;
    FailedRanksHint failedRanksHint_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORK_HANDLES_H
