#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <mooncake_pg.h>
#include <work_handles.h>

#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <torch/torch.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {

class MooncakeBackend final : public ::c10d::ProcessGroup {
   public:
    struct MooncakeBackendOptions final : torch::CustomClassHolder {
        explicit MooncakeBackendOptions(int maxGroupSize)
            : maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1} {}

        // isExtension=false maps to CreateOrAttach; true maps to
        // AttachOrExtend.
        MooncakeBackendOptions(int maxGroupSize, bool isExtension)
            : isExtension_{isExtension},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1} {}
        MooncakeBackendOptions(int maxGroupSize, bool isExtension,
                               bool autoDeactivateOnFailure,
                               bool autoSyncOnFailure)
            : isExtension_{isExtension},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1},
              autoDeactivateOnFailure_{autoDeactivateOnFailure},
              autoSyncOnFailure_{autoSyncOnFailure} {}

        // If activeRanks is provided, only its storage is used -- the contents
        // are populated by the Coordinator.
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : activeRanks_{std::move(activeRanks)} {}

        // Main-compatible tensor overloads use the same isExtension mapping.
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension)
            : activeRanks_{std::move(activeRanks)}, isExtension_{isExtension} {}
        MooncakeBackendOptions(at::Tensor activeRanks, bool isExtension,
                               int maxGroupSize)
            : activeRanks_{std::move(activeRanks)},
              isExtension_{isExtension},
              maxGroupSize_{maxGroupSize > 0 ? maxGroupSize : -1} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
        bool isExtension_ = false;
        int maxGroupSize_ = -1;

        // Automatically deactivate failed ranks on timeout / operation failure.
        //
        // When true (default), failed ranks are removed from the active set
        // automatically.  When false, failures are only reported through
        // per-operation failedRanks hints, so the caller can decide how to
        // handle the failure.
        //
        // Default: MOONCAKE_PG_AUTO_DEACTIVATE_ON_FAILURE (1)
        bool autoDeactivateOnFailure_ = true;

        // Fence a failed collective on Coordinator reconciliation.
        //
        // When true (default), the worker reports a locally detected transfer
        // failure and waits for the authoritative membership view to be
        // applied before completing the task. Consequently, CPU work and
        // CUDA stream execution cannot pass the failed collective before the
        // view is updated. CUDA Work::wait() itself remains asynchronous and
        // does not imply that the host can immediately observe the new view.
        //
        // Requires autoDeactivateOnFailure_ == true.
        //
        // Default: MOONCAKE_PG_AUTO_SYNC_ON_FAILURE (1)
        bool autoSyncOnFailure_ = true;
    };

    /**
     * @brief Construct a Mooncake process-group backend instance.
     *
     * `distBackendOpts` contains the PyTorch process-group information for this
     * backend instance. `options` contains Mooncake-specific settings and may
     * be null when callers omit `pg_options`.
     *
     * @param distBackendOpts Process-group information supplied by PyTorch.
     * @param options Optional Mooncake-specific backend options.
     * @param context Process-wide Mooncake PG core context.
     * @param isCpu Whether to initialize the CPU backend variant.
     */
    MooncakeBackend(c10d::DistributedBackendOptions distBackendOpts,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    mooncakePgContext_t context, bool isCpu = false);
    ~MooncakeBackend() override;

    const std::string getBackendName() const override;

    // In Normal mode, return the active rank-space extent (highest active
    // InGroupRank plus one). During bootstrap, PyTorch still needs the
    // group_size declared at construction to validate future ranks passed to
    // new_group() before joinGroup() can run:
    // https://github.com/pytorch/pytorch/blob/release/2.13/torch/distributed/distributed_c10d.py#L6012
    int getSize() const override;

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

    at::Tensor getActiveRanksTensor() { return activeRanks_; }
    int getNumSyncedRanks();
    void extendGroupSizeTo(int size);
    std::vector<bool> getPeerState(const std::vector<int>& ranks);
    mooncakePgProposalResponse_t activateRanks(const std::vector<int>& ranks);
    mooncakePgProposalResponse_t deactivateRanks(const std::vector<int>& ranks);
    void joinGroup();
    uint64_t getCurrentEpoch() const;
    mooncakePgSyncAfterFailureResponse_t syncAfterFailure();

   private:
    template <auto CpuFn, auto GpuFn, typename... Args>
    c10::intrusive_ptr<c10d::Work> launchCollective(
        c10d::OpType opType, const char* operation,
        const at::Tensor& streamTensor, std::vector<at::Tensor> keepAlive,
        std::function<void()> postCompletion, Args... args);

    template <auto CpuFn, auto GpuFn, typename... Args>
    c10::intrusive_ptr<c10d::Work> launchP2P(
        c10d::OpType opType, const char* operation,
        const at::Tensor& streamTensor, std::vector<at::Tensor> keepAlive,
        std::function<void()> postCompletion, Args... args);

    const c10::intrusive_ptr<MooncakeBackendOptions> options_;
    mooncakePgComm_t comm_ = nullptr;
    at::Tensor activeRanks_;
    bool isCpu_ = false;
    bool isShutdown_ = false;
    int max_group_size_ =
        0;  // per-group capacity (max active members for this group)
    std::shared_ptr<MooncakeWorkTracker> work_tracker_;
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_H
