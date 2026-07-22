#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <atomic>

#include <ATen/ATen.h>
#include <ATen/cuda/CUDAContext.h>
#include <c10/util/intrusive_ptr.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>

#include "control_plane/types.h"

#include <cuda_alike.h>
#include <transfer_engine.h>
#include <mooncake_worker_kernels.cuh>
#include <work_handles.h>

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {

static constexpr size_t kBufferSize = 1u << 24;

class MooncakeBackend;

// Local collective extension state. Every backend starts in Isolated.
//
// Founding member:
//   Isolated --(Active view)--> Normal
//
// Joining member:
//   Isolated --(joinGroup: drain preparation collectives)--> Quiescing
//            -----------------(Active view)----------------> Normal
//
// Isolated admits local-only collectives with an {self} active ranks mask.
// Quiescing rejects new collectives while waiting for activation. Normal uses
// the Coordinator's committed membership as active ranks. These are local
// extension phases, not membership states: an auto-deactivated backend remains
// Normal and fails its next collective through the inactive self bit.
enum class CollectiveExtensionState : uint8_t {
    Isolated = 0,   // Local-only collectives
    Quiescing = 1,  // awaiting activation; no collectives may be issued.
    Normal = 2,  // Collectives use the membership committed by the coordinator.
};

struct TransferGroupMeta {
    InGroupRank rank;
    GlobalRank globalRank;
    // rank_order maps InGroupRank (0 .. maxGroupSize-1) to GlobalRank.
    GlobalRank rank_order[kMaxNumRanks];

    int maxGroupSize;
    // Highest active InGroupRank plus one.
    std::atomic<int> activeSize{0};
    int taskCount;

    GroupId group_id;
    std::atomic<uint64_t> epoch{0};
    std::atomic<CollectiveExtensionState> extensionMode{
        CollectiveExtensionState::Isolated};

    bool* activeRanks;
    bool* activeRanksDevice;
    at::Tensor activeRanksTensor;
    bool* maybeActivatable;
    RankState rankStates[kMaxNumRanks];  // per GlobalRank
    uint64_t rankEpochs[kMaxNumRanks];
    TransferEngine* engine;
    TransferMetadata::SegmentID segmentIDs[kMaxNumRanks];
    GroupEndpointInfo segmentInfos[kMaxNumRanks];
    const size_t* collectiveTimeoutUs = nullptr;
    MooncakeBackend* backend = nullptr;
    bool autoSyncOnFailure = true;
};

void launchReduceKernel(at::Tensor dst, size_t pos, size_t realSize, void* src,
                        size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, size_t pos, size_t realSize, void* src,
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks);

class MooncakeWorker {
   public:
    explicit MooncakeWorker(int cuda_device_index = -1);
    ~MooncakeWorker();

    c10::intrusive_ptr<c10d::Work> putTaskCpu(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        FailedRanksHint failedRanksHint,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            bufferToTensor);

    c10::intrusive_ptr<c10d::Work> putTaskCuda(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        const at::cuda::CUDAStream& issue_stream,
        FailedRanksHint failedRanksHint,
        const std::function<void(void* dst, size_t pos, size_t realSize,
                                 const at::cuda::CUDAStream&)>& tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize,
                                 const at::cuda::CUDAStream&)>& bufferToTensor);

    void Start();

    /**
     * @brief Waits for all active collective tasks for the given backend to
     * complete.
     *
     * Used during graceful shutdown to ensure no pending collective operations
     * are active before releasing resources. Blocks until all tasks complete
     * or the timeout expires.
     *
     * @param meta The transfer group metadata identifying the backend.
     * @return True if all tasks completed within the timeout; false if timed
     * out.
     */
    bool drainTasks(const TransferGroupMeta* meta) const;

    bool waitUntilTasksSubmitted(
        const std::vector<CudaTaskSubmissionToken>& tasks,
        std::chrono::milliseconds timeout) const;

   private:
    friend class MooncakeWorkCpu;
    friend class MooncakeWorkCuda;

    void startWorker();
    void removeHintRoute(uint64_t hint_route_id);

    static constexpr size_t kNumTasks_ = 4;

    static constexpr size_t kDrainTasksTimeoutMs = 5000;  // 5s

    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    int cuda_device_index_;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
    std::atomic<uint64_t> next_cuda_task_sequence_{1};
    std::atomic<uint64_t> next_hint_route_id_{1};
    std::atomic<uint64_t> submitted_task_sequence_[kNumTasks_]{};

    // Optional failed-ranks hint routing. Task execution and link reporting use
    // worker-local state and remain independent of whether a route exists.
    struct HintRoute {
        at::Tensor tensor;
    };

    // Routes remain registered while their Work handle is alive so captured
    // CUDA tasks can resolve the same hint across replays.
    std::mutex hint_routes_mutex_;
    std::unordered_map<uint64_t, HintRoute> hint_routes_by_id_;

    std::thread worker_thread_;
};

class MooncakeWorkerManager {
   public:
    MooncakeWorkerManager() = default;

    std::shared_ptr<MooncakeWorker> GetCPUWorker();
    std::shared_ptr<MooncakeWorker> GetCUDAWorker(int cuda_device_index);

   private:
    std::shared_ptr<MooncakeWorker> GetWorker(int worker_id);
    static constexpr int CPUWorkerID = -1;
    std::mutex manager_mutex_;
    // Keep workers alive for the entire process lifetime because their
    // detached threads must not outlive the MooncakeWorker object.
    std::unordered_map<int, std::shared_ptr<MooncakeWorker>> workers_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
