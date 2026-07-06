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

// Per-operation failedRanksHint / attemptedRanks buffer (non-copyable, movable)
// CPU: owning tensor.
// CUDA: pinned mapped memory (tensor owns the allocation via custom deleter;
//       dev_ptr is the device-visible alias of the same buffer).
// local_success: true iff ALL attempted peers succeeded in this operation.
//   Set by the Work handle on wait() completion.
struct FailedRanksHint {
    at::Tensor tensor;
    int* dev_ptr = nullptr;  // CUDA only: device pointer for GPU kernel.

    // CUDA only: bitmap of ranks that were attempted in this op.  Used by the
    // control plane to distinguish "peer failed" from "peer not attempted".
    at::Tensor attempted_tensor;
    int* attempted_dev_ptr = nullptr;

    // Set to true on wait() if no peer failed this operation locally.
    bool local_success = false;

    FailedRanksHint() = default;
    FailedRanksHint(at::Tensor tensor_in, int* dev_ptr_in)
        : tensor(std::move(tensor_in)), dev_ptr(dev_ptr_in) {}
    FailedRanksHint(at::Tensor tensor_in, int* dev_ptr_in,
                    at::Tensor attempted_tensor_in, int* attempted_dev_ptr_in)
        : tensor(std::move(tensor_in)),
          dev_ptr(dev_ptr_in),
          attempted_tensor(std::move(attempted_tensor_in)),
          attempted_dev_ptr(attempted_dev_ptr_in) {}

    FailedRanksHint(const FailedRanksHint&) = delete;
    FailedRanksHint& operator=(const FailedRanksHint&) = delete;

    FailedRanksHint(FailedRanksHint&& o) noexcept = default;
    FailedRanksHint& operator=(FailedRanksHint&& o) noexcept = default;
    ~FailedRanksHint() = default;

    int* data() { return tensor.data_ptr<int>(); }
    int* attemptedData() { return attempted_tensor.data_ptr<int>(); }

    static FailedRanksHint allocate(int n, bool isCpu);
};

// Collective Work handles
class MooncakeWorkCpu : public ::c10d::Work {
   public:
    MooncakeWorkCpu(c10d::OpType opType,
                    c10::intrusive_ptr<c10::ivalue::Future> future,
                    std::shared_ptr<TransferGroupMeta> meta,
                    FailedRanksHint failedRanksHint)
        : Work(-1, opType),
          future_(std::move(future)),
          meta_(std::move(meta)),
          failedRanksHint_(std::move(failedRanksHint)) {}

    bool isCompleted() override { return future_->completed(); }

    bool wait(std::chrono::milliseconds timeout) override;

    at::Tensor getFailedRanksHint() const { return failedRanksHint_.tensor; }
    bool getLocalSuccess() const { return failedRanksHint_.local_success; }

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
    std::shared_ptr<TransferGroupMeta> meta_;
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
                     const MooncakeWorker* worker,
                     std::vector<CudaTaskSubmissionToken> submitted_tasks,
                     FailedRanksHint failedRanksHint)
        : Work(-1, opType),
          event_(std::move(event)),
          meta_(std::move(meta)),
          worker_(worker),
          submitted_tasks_(std::move(submitted_tasks)),
          failedRanksHint_(std::move(failedRanksHint)) {}

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override;

    at::Tensor getFailedRanksHint() const;
    bool getLocalSuccess() const;

   protected:
    std::shared_ptr<torch::Event> event_;
    std::shared_ptr<TransferGroupMeta> meta_;
    const MooncakeWorker* worker_;
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
    bool getLocalSuccess() const { return isSuccess(); }

   private:
    std::shared_ptr<std::atomic<Status>> status_;
    FailedRanksHint failedRanksHint_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORK_HANDLES_H
