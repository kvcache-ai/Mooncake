#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <utility>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "gpu_runtime.h"

#include <transfer_engine.h>
#include <mooncake_worker_kernels.cuh>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {

static constexpr size_t kBufferSize = 1u << 24;

// Sync regions of a buffer half. Peers address them by offset, so their layout
// is part of what ranks agree on.
//
// A run flag. It carries the sender's read counts (see
// TransferGroupMeta::halfReads) as they were when the flag was posted, one word
// per half. A word holds the count in its low 32 bits and the sender's tag (see
// syncFlagTag) in its high 32 bits. The flag word is the word of the flag's own
// half, and a tag there with the number of the reader's run marks the flag of
// that run as set.
//
// A reader reads each word with one aligned 8-byte load and trusts a count
// only next to the tag it expects: its run's number and the process now in the
// peer's slot. The words of one flag may land in either order, but each word
// must land whole, as an aligned 8-byte store does on the transports in use. A
// count that a reader sees late is lower, which only costs a READ. Between
// admissions a word left by an earlier run has another number, so a flag that
// lands after its reader has given up on it neither ends nor feeds a later run
// (see TransferGroupMeta::bufferHalfRuns for what is left).
struct alignas(16) SyncFlag {
    uint64_t words[2];
};
static_assert(sizeof(SyncFlag) == 16, "a flag is two 8-byte words");
// The bytes of a flag that are sent. tests/transfer_fault_preload.cpp
// recognizes flag writes by this length.
static constexpr size_t kSyncFlagBytes = sizeof(SyncFlag);

// The bits of a run's number on its half that a tag holds.
static constexpr uint32_t kSyncFlagRunMask = (1u << 23) - 1;

// The tag a rank puts into the flag words of a run. Bit 0 is always set, so a
// tag is never zero. Bits 1-23 hold the run's number on its half (see
// TransferGroupMeta::bufferHalfRuns), which every rank counts alike. Bits 24-31
// hold the low 8 bits of the sender's rank epoch, which differ between
// successive processes of a slot and repeat only after 256 of them.
inline uint32_t syncFlagTag(uint64_t rank_epoch, uint32_t run) {
    return static_cast<uint32_t>(rank_epoch) << 24 |
           (run & kSyncFlagRunMask) << 1 | 1u;
}

// True when a tag was put there for the given run, by any process.
inline bool syncFlagTagIsRun(uint32_t tag, uint32_t run) {
    return (tag & 1u) != 0 &&
           ((tag >> 1) & kSyncFlagRunMask) == (run & kSyncFlagRunMask);
}

inline uint64_t syncFlagWord(uint32_t tag, uint32_t count) {
    return static_cast<uint64_t>(tag) << 32 | count;
}

inline uint32_t syncFlagWordTag(uint64_t word) {
    return static_cast<uint32_t>(word >> 32);
}

inline uint32_t syncFlagWordCount(uint64_t word) {
    return static_cast<uint32_t>(word);
}

// What a rank receives for a half.
struct HalfRecvSync {
    // Two sets of flags, used by the half's consecutive runs in turn (see
    // TransferGroupMeta::bufferHalfRuns). A set has one flag per sending rank.
    SyncFlag flags[2][kMaxNumRanks];
    // Where the host worker reads a peer's counts into, one pair per peer.
    uint32_t readCountsIn[kMaxNumRanks][2];
};
// The region is allocated as an int32_t array.
static_assert(alignof(HalfRecvSync) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);

// What a rank sends from for a half.
struct HalfSendSync {
    // The flag of a run, one per flag set.
    SyncFlag flags[2];
    // This rank's read counts for both halves. Only half 0's copy is used:
    // peers read it at send_sync[0].
    uint32_t halfReads[2];
};
// A CPU communicator allocates the region as an int32_t array.
static_assert(alignof(HalfSendSync) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);

class MooncakeCommunicator;

class WorkCompletion {
   public:
    explicit WorkCompletion(std::shared_future<void> completion)
        : completion_(std::move(completion)) {}

    bool isCompleted() const {
        if (completion_.wait_for(std::chrono::microseconds(0)) !=
            std::future_status::ready) {
            return false;
        }
        completion_.get();
        return true;
    }

    bool wait(std::chrono::microseconds timeout) const {
        if (timeout.count() < 0) {
            completion_.wait();
        } else if (completion_.wait_for(timeout) != std::future_status::ready) {
            return false;
        }
        completion_.get();
        return true;
    }

   private:
    std::shared_future<void> completion_;
};

// Local collective extension state. Every communicator starts in Isolated.
//
// Founding member:
//   Isolated --(Active view)--> Normal
//
// Joining member:
//   Isolated ---------(joinGroup: drain operations)--------> Quiescing
//            -----------------(Active view)----------------> Normal
//
// In-place rejoining member:
//   Normal + inactive --(joinGroup: drain operations)------> Quiescing
//                     ---------(Active view)---------------> Normal
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
    // Runs of each buffer half so far, counted by the host worker. A half has
    // two sets of sync flags and a run uses the set of its count's parity; its
    // flags carry the count as the run's number (see syncFlagTag), so ranks
    // must count a half's runs alike: each rank must apply every view that
    // admits a rank, where all of them restart the counts (see
    // applyGroupState). A flag that landed late in a run whose 23-bit number is
    // 0 or 1, as the first two runs of a half after an admission are, can still
    // match a run after the next admission. The end of a run clears its flag
    // set whatever run a word is of; a flag that a rank admitted at a later
    // view posts before this rank has finished its earlier runs is lost, which
    // the quiescence required at an admission rules out.
    std::atomic<uint32_t> bufferHalfRuns[2]{};
    // Reads of each buffer half so far. A run is read when its copy-out is
    // done, and whoever issues the copy-out counts it right behind it: a kernel
    // on the same stream for a GPU collective, the host worker for a CPU one.
    // The counts live in registered memory (see HalfSendSync), so a peer can
    // read them. A run flag says that a rank's data landed, not that the
    // receiver read it. So before a rank stores into a peer's half again, that
    // peer's count for the half must have passed the count that its flag of the
    // half's last run carried. halfReadsDevice is the device's view of the
    // counts.
    uint32_t* halfReads = nullptr;
    uint32_t* halfReadsDevice = nullptr;
    // What the host worker knows about a peer's reads. Only the worker uses it.
    // The knowledge belongs to one process: the worker forgets it when a new
    // process takes the peer's slot, which has a new rank epoch and counts
    // from zero. A peer that stays, or rejoins in place, keeps counting.
    struct PeerReads {
        // The rank epoch of the process this entry describes.
        uint64_t rankEpoch;
        // The peer's latest counts that this rank has seen.
        uint32_t known[2];
        bool knownValid[2];
        // The count the peer must reach before this rank stores into its half
        // again: one more than its flag of the half's last run carried.
        uint32_t needed[2];
        bool awaited[2];
    };
    PeerReads peerReads[kMaxNumRanks]{};

    GroupId group_id;
    std::atomic<uint64_t> epoch{0};
    std::atomic<CollectiveExtensionState> extensionMode{
        CollectiveExtensionState::Isolated};

    bool* activeRanks;
    bool* activeRanksDevice;
    int32_t* activeRanksMirrorDevice = nullptr;
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

    bool hasPendingActiveRanksMirrorUpdate(const TransferGroupMeta* meta) const;

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
    struct CudaTaskSubmissionToken {
        size_t task_id;
        uint64_t sequence;
    };

    void startWorker();
    void waitUntilTasksSubmitted(
        const std::vector<CudaTaskSubmissionToken>& tasks) const;

    static constexpr size_t kNumCpuTasks_ = 2;
    static constexpr size_t kNumCudaTasks_ = 2;
    static constexpr size_t kCudaTaskOffset_ = kNumCpuTasks_;
    static constexpr size_t kNumTasks_ = kNumCpuTasks_ + kNumCudaTasks_;

    static constexpr size_t kDrainTasksTimeoutMs = 5000;  // 5s

    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    int cuda_device_index_;
    std::array<std::optional<GpuStream>, kNumCudaTasks_> enqueue_streams_;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    size_t cudaOpCount = 0;
    std::atomic<uint64_t> next_cuda_task_sequence_{1};
    std::atomic<uint64_t> next_active_ranks_mirror_generation_{1};
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
