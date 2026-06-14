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

// Per-operation failedRanks buffer (non-copyable, movable)
// CPU: owning tensor.
// CUDA: pinned mapped memory (tensor owns the allocation via custom deleter;
//       dev_ptr is the device-visible alias of the same buffer).
struct FailedRanks {
    at::Tensor tensor;
    int* dev_ptr = nullptr;  // CUDA only: device pointer for GPU kernel.

    FailedRanks() = default;
    FailedRanks(at::Tensor tensor_in, int* dev_ptr_in)
        : tensor(std::move(tensor_in)), dev_ptr(dev_ptr_in) {}

    FailedRanks(const FailedRanks&) = delete;
    FailedRanks& operator=(const FailedRanks&) = delete;

    FailedRanks(FailedRanks&& o) noexcept = default;
    FailedRanks& operator=(FailedRanks&& o) noexcept = default;
    ~FailedRanks() = default;

    int* data() { return tensor.data_ptr<int>(); }

    static FailedRanks allocate(int n, bool isCpu);
};

// Collective Work handles
class MooncakeWorkCpu : public ::c10d::Work {
   public:
    MooncakeWorkCpu(c10d::OpType opType,
                    c10::intrusive_ptr<c10::ivalue::Future> future,
                    std::shared_ptr<TransferGroupMeta> meta,
                    FailedRanks failedRanks)
        : Work(-1, opType),
          future_(std::move(future)),
          meta_(std::move(meta)),
          failedRanks_(std::move(failedRanks)) {}

    bool isCompleted() override { return future_->completed(); }

    bool wait(std::chrono::milliseconds timeout) override {
        future_->wait();
        return future_->completed() && !future_->hasError();
    }

    at::Tensor getFailedRanks() const { return failedRanks_.tensor; }

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
    std::shared_ptr<TransferGroupMeta> meta_;
    FailedRanks failedRanks_;
};

struct CudaTaskSubmissionToken {
    size_t task_id;
    uint64_t sequence;
};

class MooncakeWorkCuda : public ::c10d::Work {
   public:
    MooncakeWorkCuda(c10d::OpType opType, std::shared_ptr<torch::Event> event,
                     std::shared_ptr<TransferGroupMeta> meta,
                     const MooncakeWorker* worker,
                     std::vector<CudaTaskSubmissionToken> submitted_tasks,
                     FailedRanks failedRanks)
        : Work(-1, opType),
          event_(std::move(event)),
          meta_(std::move(meta)),
          worker_(worker),
          submitted_tasks_(std::move(submitted_tasks)),
          failedRanks_(std::move(failedRanks)) {}

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override;

    at::Tensor getFailedRanks() const;

   protected:
    std::shared_ptr<torch::Event> event_;
    std::shared_ptr<TransferGroupMeta> meta_;
    const MooncakeWorker* worker_;
    std::vector<CudaTaskSubmissionToken> submitted_tasks_;
    FailedRanks failedRanks_;
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
                             FailedRanks failedRanks)
        : Work(-1, c10d::OpType::UNKNOWN),
          status_(status),
          failedRanks_(std::move(failedRanks)) {}

    bool isCompleted() override;
    bool isSuccess() const override;
    bool wait(std::chrono::milliseconds timeout) override;
    at::Tensor getFailedRanks() const { return failedRanks_.tensor; }

   private:
    std::shared_ptr<std::atomic<Status>> status_;
    FailedRanks failedRanks_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORK_HANDLES_H
