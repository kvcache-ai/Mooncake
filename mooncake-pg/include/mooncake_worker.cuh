#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <atomic>
#include <functional>

#include "control_plane/control_types.h"
#include "gpu_runtime.h"

#include <transfer_engine.h>
#include <mooncake_worker_kernels.cuh>
#include "comm_types.h"

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {

static constexpr size_t kBufferSize = 1u << 24;

class MooncakeCommunicator;

// Local collective extension state. Every communicator starts in Isolated.
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
// extension phases, not membership states: an auto-deactivated communicator
// remains Normal and fails its next collective through the inactive self bit.
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
    bool* maybeActivatable;
    RankState rankStates[kMaxNumRanks];  // per GlobalRank
    uint64_t rankEpochs[kMaxNumRanks];
    TransferEngine* engine;
    TransferMetadata::SegmentID segmentIDs[kMaxNumRanks];
    GroupEndpointInfo segmentInfos[kMaxNumRanks];
    const size_t* collectiveTimeoutUs = nullptr;
    MooncakeCommunicator* communicator = nullptr;
    bool autoSyncOnFailure = true;
};

void launchReduceKernel(void* dst, DataType datatype, size_t pos,
                        size_t realSize, void* src, size_t numRanks,
                        ReduceOp op, bool* activeRanks, cudaStream_t stream);

void launchReduceCpu(void* dst, DataType datatype, size_t pos, size_t realSize,
                     void* src, size_t numRanks, ReduceOp op,
                     bool* activeRanks);

class MooncakeWorker {
   public:
    explicit MooncakeWorker(int cuda_device_index = -1);
    ~MooncakeWorker();

    std::unique_ptr<WorkCompletion> putTaskCpu(
        OpType opType, size_t dataSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        int32_t* failedRanksHint,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            copyToSendBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            copyFromRecvBuffer);

    void putTaskCuda(
        OpType opType, size_t dataSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        cudaStream_t issueStream, int32_t* failedRanksHint,
        const std::function<void(void* dst, size_t pos, size_t realSize,
                                 cudaStream_t)>& copyToSendBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize,
                                 cudaStream_t)>& copyFromRecvBuffer);

    void Start();

    /**
     * @brief Waits for all active collective tasks for the given communicator
     * to complete.
     *
     * Used during graceful shutdown to ensure no pending collective operations
     * are active before releasing resources. Blocks until all tasks complete
     * or the timeout expires.
     *
     * @param meta The transfer group metadata identifying the communicator.
     * @return True if all tasks completed within the timeout; false if timed
     * out.
     */
    bool drainTasks(const TransferGroupMeta* meta) const;

   private:
    void startWorker();
    void waitUntilTasksSubmitted(
        const std::vector<CudaTaskSubmissionToken>& tasks) const;

    static constexpr size_t kNumTasks_ = 4;

    static constexpr size_t kDrainTasksTimeoutMs = 5000;  // 5s

    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    int cuda_device_index_;
    std::optional<GpuStream> enqueue_stream_;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
    std::atomic<uint64_t> next_cuda_task_sequence_{1};
    std::atomic<uint64_t> submitted_task_sequence_[kNumTasks_]{};

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
