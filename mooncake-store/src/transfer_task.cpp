#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <string>
#include <vector>
#include "device/accelerator_registry.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "nof/nvmeof_initiator.h"

static int GetPositiveEnvOrDefault(const char* name, int default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end_ptr = nullptr;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                     << ", using default " << default_value;
        return default_value;
    }

    return static_cast<int>(parsed);
}

static bool IsTruthyEnv(const char* value) {
    if (!value) {
        return false;
    }
    std::string normalized(value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return normalized == "1" || normalized == "true" || normalized == "yes" ||
           normalized == "on";
}

static bool IsNofDebugEnabled() {
    static const bool enabled = IsTruthyEnv(std::getenv("MC_NOF_DEBUG"));
    return enabled;
}

static int GetNofDebugIntervalMs() {
    static const int interval_ms = []() {
        const char* raw_value = std::getenv("MC_NOF_DEBUG_INTERVAL_MS");
        if (!raw_value) {
            return 1000;
        }
        char* end_ptr = nullptr;
        long parsed = std::strtol(raw_value, &end_ptr, 10);
        if (end_ptr == raw_value || (end_ptr != nullptr && *end_ptr != '\0') ||
            parsed <= 0) {
            return 1000;
        }
        return static_cast<int>(parsed);
    }();
    return interval_ms;
}

static int GetNofSubmitChunkBytes() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_SUBMIT_CHUNK_BYTES", mooncake::kDefaultNofSubmitChunkBytes);
    return value;
}

static int GetNofInflightBytesLimit() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_INFLIGHT_BYTES_LIMIT", mooncake::kDefaultNofInflightBytesLimit);
    return value;
}

static int GetNofWorkerCount() {
    static const int value =
        GetPositiveEnvOrDefault("MC_NOF_WORKERS", mooncake::kDefaultNofWorkers);
    return value;
}

static int CountNofQueuedTasks(const mooncake::NofTask* head) {
    int count = 0;
    const mooncake::NofTask* cursor = head;
    while (cursor != nullptr) {
        ++count;
        cursor = cursor->nxt;
    }
    return count;
}

static inline void NofTaskCompletion(mooncake::NofTask* task) {
    if (task->remaining_bytes == 0 && task->outstanding_sub_io == 0) {
        task->state->set_completed(task->failed
                                       ? mooncake::ErrorCode::TRANSFER_FAIL
                                       : mooncake::ErrorCode::OK,
                                   std::move(task->error_detail));
        if (!task->on_chain) {
            delete task;
        }
    }
}

static void nof_io_complete(void* ctx, const mooncake::NofIOCompletion& c) {
    if (!ctx) {
        LOG(ERROR) << "nof_io_complete ctx is null";
        return;
    }

    mooncake::NofSubTask* sub_task =
        reinterpret_cast<mooncake::NofSubTask*>(ctx);
    mooncake::NofTask* task = sub_task->task;
    mooncake::NofQos* nof_qos = task->nof_qos;
    size_t op = static_cast<size_t>(task->op);

    if (task->io_count &&
        task->io_count->fetch_sub(1, std::memory_order_acq_rel) <= 0) {
        LOG(ERROR) << "total outstanding io < 0";
    }

    if (--(task->outstanding_sub_io) < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    nof_qos->inflight_blocks[op] -=
        static_cast<int>(sub_task->submit_bytes / nof_qos->block_size);
    if (nof_qos->inflight_blocks[op] < 0) {
        LOG(ERROR) << "qos inflight blocks < 0";
    }

    if (!c.success) {
        LOG(ERROR) << "nof task I/O failed: sc=" << c.sc << " sct=" << c.sct
                   << " " << c.error_string;
        task->remaining_bytes = 0;
        task->failed = true;
        if (task->error_detail.empty()) {
            task->error_detail = c.error_string;  // keep the first error detail
        }
    }

    NofTaskCompletion(task);

    sub_task->sub_task_pool->push(sub_task);
}
namespace mooncake {

NofQos::NofQos(uint32_t bs)
    : block_size(bs == 0 || bs == kInvalidBlockSize ? 1 : bs),
      blocks_per_chunk(
          std::max(1, GetNofSubmitChunkBytes() / static_cast<int>(block_size))),
      inflight_blocks_limit(std::max(
          1, GetNofInflightBytesLimit() / static_cast<int>(block_size))) {
    for (size_t i = 0; i < static_cast<size_t>(NofIOOp::kNum); ++i) {
        inflight_blocks[i] = 0;
        head[i] = nullptr;
        tail[i] = nullptr;
    }
}

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 10 worker
// threads.
constexpr int kDefaultFilereadWorkers = 10;

// The number of fileread workers can be tuned via the MC_FILEREAD_WORKERS
// environment variable. Falls back to kDefaultFilereadWorkers when unset,
// empty, or invalid.
static int GetFilereadWorkerCount() {
    static const int value =
        GetPositiveEnvOrDefault("MC_FILEREAD_WORKERS", kDefaultFilereadWorkers);
    return value;
}

FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    const int num_workers = GetFilereadWorkerCount();
    VLOG(1) << "Creating FilereadWorkerPool with " << num_workers << " workers";

    // Start worker threads
    workers_.reserve(num_workers);
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back(&FilereadWorkerPool::workerThread, this);
    }
    backend_ = backend;
}

FilereadWorkerPool::~FilereadWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "FilereadWorkerPool destroyed";
}

void FilereadWorkerPool::submitTask(FilereadTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown FilereadWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void FilereadWorkerPool::workerThread() {
    VLOG(2) << "FilereadWorkerPool worker thread started";

    while (true) {
        FilereadTask task("", 0, {}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                if (!backend_) {
                    LOG(ERROR)
                        << "Backend is not initialized, cannot load object";
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    continue;
                }

                auto load_result = backend_->LoadObject(
                    task.file_path, task.slices, task.object_size);
                if (load_result) {
                    VLOG(2) << "Fileread task completed successfully with "
                            << task.file_path;
                    task.state->set_completed(ErrorCode::OK);
                } else {
                    LOG(ERROR)
                        << "Fileread task failed for file: " << task.file_path
                        << " with error: " << toString(load_result.error());
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async fileread: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "FilereadWorkerPool worker thread exiting";
}

// ============================================================================
// NofWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 4 worker
// threads.

NofWorkerPool::NofWorkerPool(std::shared_ptr<NVMeoFInitiator> initiator,
                             int numa_socket_id)
    : initiator_(std::move(initiator)),
      worker_count_(GetNofWorkerCount()),
      numa_socket_id_(numa_socket_id),
      task_queue_(std::make_unique<std::queue<NofTask>[]>(worker_count_)),
      queue_mutex_(std::make_unique<std::mutex[]>(worker_count_)),
      queue_cv_(std::make_unique<std::condition_variable[]>(worker_count_)),
      shutdown_(false) {
    VLOG(1) << "Creating NofWorkerPool with " << worker_count_ << " workers";

    // Start worker threads
    workers_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&NofWorkerPool::workerThread, this, i);
    }
}

NofWorkerPool::~NofWorkerPool() {
    if (shutdown_.exchange(true)) {
        return;
    }

    for (int i = 0; i < worker_count_; ++i) {
        queue_cv_[i].notify_all();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "NofWorkerPool destroyed";
}

void NofWorkerPool::submitTask(NofTask task) {
    if (!task.state) {
        LOG(ERROR) << "Attempting to submit nof task without state";
        return;
    }

    if (shutdown_.load()) {
        LOG(WARNING) << "Attempting to submit task to shutdown NofWorkerPool";
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    int worker_idx = -1;
    {
        std::lock_guard<std::mutex> lock(seg_mutex_);
        NofSegmentHandle* seg = task.seg_handle;
        bool new_binding = false;
        if (seg_to_worker_.find(seg) != seg_to_worker_.end()) {
            worker_idx = seg_to_worker_[seg];
        } else {
            worker_idx = (seg_num++ % worker_count_);
            seg_to_worker_[seg] = worker_idx;
            new_binding = true;
        }
        if (new_binding && IsNofDebugEnabled()) {
            LOG(INFO) << "nof_worker_bind seg_handle=" << seg
                      << " worker_idx=" << worker_idx;
        }
    }
    if (worker_idx < 0 || worker_idx >= worker_count_) {
        LOG(ERROR) << "seg is not bind to invalid worker " << worker_idx;
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex_[worker_idx]);
        task_queue_[worker_idx].push(std::move(task));
    }
    queue_cv_[worker_idx].notify_one();
}

static bool HasBufferedTask(
    const std::map<NofSegmentHandle*, std::unique_ptr<NofQos>>& seg_to_qos) {
    for (const auto& [_, nof_qos] : seg_to_qos) {
        if (!nof_qos->Empty()) {
            return true;
        }
    }
    return false;
}

constexpr int kDefaultNofSubTaskChunkSize = 4096;

static inline bool CheckSubTaskPool(std::stack<NofSubTask*>& sub_task_pool,
                                    std::vector<NofSubTask*>& sub_task_chunks,
                                    int work_idx) {
    if (!sub_task_pool.empty()) {
        return true;
    }
    NofSubTask* sub_tasks =
        new (std::nothrow) NofSubTask[kDefaultNofSubTaskChunkSize];
    if (!sub_tasks) {
        LOG(ERROR) << "alloc NofSubTask failed, worker " << work_idx;
        return false;
    }
    sub_task_chunks.push_back(sub_tasks);

    for (int i = 0; i < kDefaultNofSubTaskChunkSize; ++i) {
        sub_tasks[i].sub_task_pool = &sub_task_pool;
        sub_task_pool.push(&sub_tasks[i]);
    }

    return true;
}

void NofWorkerPool::workerThread(int work_idx) {
    bindToSocket(numa_socket_id_);
    VLOG(2) << "NofWorkerPool worker thread started";

    // No longer a stack-variable address: the worker and the task share
    // ownership.
    auto total_outstanding_io = std::make_shared<std::atomic<int64_t>>(0);
    std::map<NofSegmentHandle*, std::unique_ptr<NofQos>> seg_to_qos;
    std::stack<NofSubTask*> sub_task_pool;
    std::vector<NofSubTask*> sub_task_chunks;
    auto& task_queue = task_queue_[work_idx];
    auto& queue_cv = queue_cv_[work_idx];
    auto& queue_mutex = queue_mutex_[work_idx];
    auto last_debug_snapshot = std::chrono::steady_clock::now();

    if (!CheckSubTaskPool(sub_task_pool, sub_task_chunks, work_idx)) {
        return;
    }

    while (true) {
        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this, &task_queue, &total_outstanding_io,
                                 &seg_to_qos] {
                return shutdown_.load() || !task_queue.empty() ||
                       total_outstanding_io->load(std::memory_order_acquire) !=
                           0 ||
                       HasBufferedTask(seg_to_qos);
            });

            if (shutdown_.load() && task_queue.empty() &&
                total_outstanding_io->load(std::memory_order_acquire) == 0 &&
                !HasBufferedTask(seg_to_qos)) {
                break;
            }

            while (!task_queue.empty()) {
                NofTask* task =
                    new (std::nothrow) NofTask(std::move(task_queue.front()));
                if (task == nullptr) {
                    LOG(ERROR) << "alloc NofTask failed, worker " << work_idx;
                    continue;
                }

                NofQos* nof_qos = nullptr;
                auto it = seg_to_qos.find(task->seg_handle);
                if (it == seg_to_qos.end()) {
                    auto qos = std::make_unique<NofQos>(
                        initiator_->GetBlockSize(task->seg_handle));
                    if (qos == nullptr) {
                        LOG(ERROR)
                            << "alloc NofQos failed, worker " << work_idx;
                        delete task;
                        continue;
                    }
                    nof_qos = qos.get();
                    seg_to_qos[task->seg_handle] = std::move(qos);
                    if (IsNofDebugEnabled()) {
                        LOG(INFO)
                            << "nof_qos_create worker_idx=" << work_idx
                            << " seg_handle=" << task->seg_handle
                            << " blocks_per_chunk="
                            << seg_to_qos[task->seg_handle]->blocks_per_chunk
                            << " inflight_blocks_limit="
                            << seg_to_qos[task->seg_handle]
                                   ->inflight_blocks_limit;
                    }
                } else {
                    nof_qos = it->second.get();
                }
                task->io_count = total_outstanding_io;
                task->nof_qos = nof_qos;
                task->on_chain = true;
                nof_qos->PushTask(task);
                task_queue.pop();
            }
        }

        for (auto& [seg_handle, nof_qos] : seg_to_qos) {
            // block_size comes from the QoS cache; the hot loop no longer
            // queries the initiator.
            const uint32_t block_size = nof_qos->block_size;
            const uint64_t chunk_bytes =
                static_cast<uint64_t>(nof_qos->blocks_per_chunk) * block_size;
            for (size_t i = 0; i < static_cast<size_t>(NofIOOp::kNum); ++i) {
                int avail_blocks = nof_qos->inflight_blocks_limit -
                                   nof_qos->inflight_blocks[i];
                while (nof_qos->head[i] && avail_blocks > 0) {
                    NofTask* task = nof_qos->head[i];
                    NofSubTask* sub_task;
                    while (task->remaining_bytes > 0 && avail_blocks > 0) {
                        const uint64_t avail_bytes =
                            static_cast<uint64_t>(avail_blocks) * block_size;
                        const uint64_t sent_bytes =
                            task->byte_length - task->remaining_bytes;
                        const uint32_t submit_bytes =
                            static_cast<uint32_t>(std::min<uint64_t>(
                                {avail_bytes, task->remaining_bytes,
                                 chunk_bytes}));
                        const uint64_t submit_off =
                            task->byte_offset + sent_bytes;
                        void* submit_ptr =
                            reinterpret_cast<char*>(task->ptr) + sent_bytes;

                        if (!CheckSubTaskPool(sub_task_pool, sub_task_chunks,
                                              work_idx)) {
                            task->failed = true;
                            task->remaining_bytes = 0;
                            break;
                        }
                        sub_task = sub_task_pool.top();
                        sub_task_pool.pop();
                        sub_task->task = task;
                        sub_task->submit_bytes = submit_bytes;
                        sub_task->adaptor = {&nof_io_complete, sub_task};

                        int ret = initiator_->SubmitIO(
                            task->seg_handle, submit_ptr, submit_off,
                            submit_bytes, task->op, &sub_task->adaptor);
                        if (ret != 0) {
                            LOG(ERROR) << "work " << work_idx << ", seg "
                                       << task->seg_handle << " submit io fail";
                            // Return the pool slot: a sub-task taken out
                            // and then failing to submit must be returned,
                            // or repeated failures would exhaust the 4096
                            // slots and allocate new chunks unboundedly.
                            sub_task->sub_task_pool->push(sub_task);
                            task->failed = true;
                            task->remaining_bytes = 0;
                        } else {
                            const int submit_blocks =
                                static_cast<int>(submit_bytes / block_size);
                            task->idx++;
                            task->remaining_bytes -= submit_bytes;
                            nof_qos->inflight_blocks[i] += submit_blocks;
                            avail_blocks -= submit_blocks;
                            task->outstanding_sub_io++;
                            total_outstanding_io->fetch_add(
                                1, std::memory_order_acq_rel);
                        }
                    }
                    if (task->remaining_bytes == 0) {
                        nof_qos->PopTask(static_cast<NofIOOp>(i));
                        task->on_chain = false;
                        NofTaskCompletion(task);
                    }
                }
            }
        }

        if (total_outstanding_io->load(std::memory_order_acquire) > 0) {
            for (auto& it : seg_to_qos) {
                int64_t ret = initiator_->PollCompletion(it.first, 0);
                if (ret < 0) {
                    LOG(ERROR) << "poll completion error: ret " << ret;
                }
            }
        }

        if (IsNofDebugEnabled()) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_debug_snapshot);
            if (elapsed.count() >= GetNofDebugIntervalMs()) {
                for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
                    LOG(INFO)
                        << "nof_qos_state worker_idx=" << work_idx
                        << " seg_handle=" << seg_handle
                        << " inflight_read=" << nof_qos->inflight_blocks[0]
                        << " inflight_write=" << nof_qos->inflight_blocks[1]
                        << " inflight_limit=" << nof_qos->inflight_blocks_limit
                        << " queued_read="
                        << CountNofQueuedTasks(nof_qos->head[0])
                        << " queued_write="
                        << CountNofQueuedTasks(nof_qos->head[1])
                        << " total_outstanding_io="
                        << total_outstanding_io->load(
                               std::memory_order_acquire);
                }
                last_debug_snapshot = now;
            }
        }
    }

    for (auto* sub_tasks : sub_task_chunks) {
        delete[] sub_tasks;
    }
    // seg_to_qos will automatically clean up NofQos objects using
    // unique_ptr

    VLOG(2) << "NofWorkerPool worker thread exiting";
}

// ============================================================================
// MemcpyWorkerPool Implementation
// ============================================================================
// Since memcpy is bound by memory bandwidth, we only need one worker thread.
constexpr int kDefaultMemcpyWorkers = 1;

MemcpyWorkerPool::MemcpyWorkerPool() : shutdown_(false) {
    VLOG(1) << "Creating MemcpyWorkerPool with " << kDefaultMemcpyWorkers
            << " workers";

    // Start worker threads
    workers_.reserve(kDefaultMemcpyWorkers);
    for (int i = 0; i < kDefaultMemcpyWorkers; ++i) {
        workers_.emplace_back(&MemcpyWorkerPool::workerThread, this);
    }
}

MemcpyWorkerPool::~MemcpyWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "MemcpyWorkerPool destroyed";
}

void MemcpyWorkerPool::submitTask(MemcpyTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown MemcpyWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void MemcpyWorkerPool::workerThread() {
    VLOG(2) << "MemcpyWorkerPool worker thread started";

    while (true) {
        MemcpyTask task({}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                bool ok = true;
                auto runtime_accelerator =
                    device::GetAcceleratorRegistry().RuntimeAccelerators();
                for (const auto& op : task.operations) {
                    device::PointerInfo src_info;
                    device::PointerInfo dst_info;
                    auto* src_device = runtime_accelerator.FindDeviceForPointer(
                        op.src, &src_info);
                    auto* dst_device = runtime_accelerator.FindDeviceForPointer(
                        op.dest, &dst_info);

                    if (!src_device && !dst_device) {
                        std::memcpy(op.dest, op.src, op.size);
                    } else {
                        if (src_device && dst_device &&
                            src_device != dst_device) {
                            LOG(ERROR)
                                << "GPU memcpy failed: source and destination "
                                   "belong to different accelerator runtimes"
                                << " src_dev=" << src_info.device_id
                                << " dst_dev=" << dst_info.device_id
                                << " size=" << op.size;
                            ok = false;
                            break;
                        }
                        const device::AcceleratorDevice* accelerator = nullptr;
                        int32_t device_id = -1;
                        device::CopyDirection direction;
                        if (src_device) {
                            accelerator = src_device;
                            device_id = src_info.device_id;
                            direction = device::CopyDirection::kDeviceToHost;
                            if (dst_device) {
                                direction =
                                    device::CopyDirection::kDeviceToDevice;
                            }
                        } else {
                            accelerator = dst_device;
                            device_id = dst_info.device_id;
                            direction = device::CopyDirection::kHostToDevice;
                        }
                        accelerator->SetContext(device_id);
                        if (!accelerator->Copy(op.dest, op.src, op.size,
                                               direction)) {
                            LOG(ERROR) << "GPU memcpy failed: src_dev="
                                       << src_info.device_id
                                       << " dst_dev=" << dst_info.device_id
                                       << " size=" << op.size;
                            ok = false;
                            break;
                        }
                    }
                }

                VLOG(2) << "Memcpy task completed with "
                        << task.operations.size() << " operations"
                        << (ok ? "" : " (with GPU copy failure)");
                task.state->set_completed(ok ? ErrorCode::OK
                                             : ErrorCode::TRANSFER_FAIL);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async memcpy: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "MemcpyWorkerPool worker thread exiting";
}

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

bool TransferEngineOperationState::is_completed() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.has_value()) {
        return true;
    }

    check_task_status();
    return result_.has_value();
}

void TransferEngineOperationState::check_task_status() {
    // Check all transfers in the batch.
    // Wait for ALL tasks to reach a terminal state before setting the result,
    // even if some have already failed. This prevents the caller from seeing
    // "completed" while background transfers are still in progress, which
    // could cause issues when freeBatchID is called in the destructor.
    bool all_terminated = true;
    std::vector<size_t> failed_task_ids;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s = engine_.getTransferStatus(batch_id_, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status for batch "
                       << batch_id_ << " task " << i << " with error "
                       << s.message();
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done successfully
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                VLOG(1) << "Transfer failed for batch " << batch_id_ << " task "
                        << i << " with status " << static_cast<int>(status.s);
#endif
                failed_task_ids.push_back(i);
                break;
            default:
                // Transfer is still in progress (WAITING, PENDING, etc.)
                all_terminated = false;
                break;
        }
    }

    if (!all_terminated) {
        // Some tasks are still in progress; wait for next poll iteration.
        // Do NOT set result yet, even if some tasks have already failed.
        return;
    }

    // All tasks have reached a terminal state.
    ErrorCode ec = ErrorCode::OK;
    if (!failed_task_ids.empty()) {
        std::ostringstream oss;
        for (size_t j = 0; j < failed_task_ids.size(); ++j) {
            if (j > 0) oss << ", ";
            oss << failed_task_ids[j];
        }
        LOG(ERROR) << "Batch " << batch_id_
                   << " completed with task failures: task_ids=[" << oss.str()
                   << "]";
        ec = ErrorCode::TRANSFER_FAIL;
    }

    set_result_internal(ec);
}

void TransferEngineOperationState::set_result_internal(ErrorCode error_code) {
    if (result_.has_value()) {
        LOG(ERROR) << "Attempting to set result multiple times for batch "
                   << batch_id_
                   << ". Previous result: " << static_cast<int>(result_.value())
                   << ", attempted new result: " << static_cast<int>(error_code)
                   << ". This indicates a race condition or logic error.";
        return;  // Don't crash, just return early
    }

    VLOG(1) << "Setting transfer result for batch " << batch_id_ << " to "
            << static_cast<int>(error_code);
    result_.emplace(error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    // 60 seconds
    constexpr int64_t timeout_milliseconds = 60 * 1000;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    VLOG(1) << "Waiting for transfer engine completion for batch " << batch_id_;

    // Wait directly on BatchDesc's condition variable.
    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    bool completed;
    bool failed = false;

    // Fast path: if already finished, avoid taking the mutex and waiting.
    // Use acquire here to pair with the writer's release-store, because this
    // path may skip taking the mutex. It ensures all prior updates are visible.
    completed = batch_desc.is_finished.load(std::memory_order_acquire);
    if (!completed) {
        // Use the same mutex as the notifier when updating the predicate to
        // avoid missed notifications. The predicate is re-checked under the
        // lock. Under the mutex, relaxed is sufficient; the mutex acquire
        // orders prior writes.
        std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
        const int64_t elapsed_milliseconds =
            getCurrentTimeInMilli() - start_ts_;
        if (elapsed_milliseconds < timeout_milliseconds) {
            completed = batch_desc.completion_cv.wait_for(
                lock,
                std::chrono::milliseconds(timeout_milliseconds -
                                          elapsed_milliseconds),
                [&batch_desc] {
                    return batch_desc.is_finished.load(
                        std::memory_order_relaxed);
                });
        }
    }  // Explicitly release completion_mutex before acquiring mutex_

    // Once completion is observed, read failure flag.
    if (completed) {
        failed = batch_desc.has_failure.load(std::memory_order_relaxed);
    }

    ErrorCode error_code =
        completed ? (failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK)
                  : ErrorCode::TRANSFER_FAIL;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        set_result_internal(error_code);
    }

    if (completed) {
        VLOG(1) << "Transfer engine operation completed for batch " << batch_id_
                << " with result: " << static_cast<int>(error_code);
    } else {
        LOG(ERROR) << "Failed to complete transfers after "
                   << timeout_milliseconds << " milliseconds for batch "
                   << batch_id_;
    }
#else
    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    while (true) {
        if (getCurrentTimeInMilli() - start_ts_ > timeout_milliseconds) {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_milliseconds << " milliseconds for batch "
                       << batch_id_;
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        check_task_status();
        if (result_.has_value()) {
            VLOG(1) << "Transfer engine operation completed for batch "
                    << batch_id_
                    << " with result: " << static_cast<int>(result_.value());
            break;
        }
        // Continue polling
        VLOG(1) << "Transfer engine operation still pending for batch "
                << batch_id_;
    }
#endif
}

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    if (!state_) {
        LOG(ERROR) << "TransferFuture requires valid state";
        throw std::invalid_argument("TransferFuture requires valid state");
    }
}

bool TransferFuture::isReady() const { return state_->is_completed(); }

ErrorCode TransferFuture::wait() {
    if (!isReady()) {
        state_->wait_for_completion();
    }
    return state_->get_result();
}

ErrorCode TransferFuture::get() { return wait(); }

TransferStrategy TransferFuture::strategy() const {
    return state_->get_strategy();
}

// ============================================================================
// TransferSubmitter Implementation
// ============================================================================

TransferSubmitter::TransferSubmitter(
    TransferEngine& engine, std::shared_ptr<StorageBackend>& backend,
    const std::string& local_hostname, TransferMetric* transfer_metric,
    int numa_socket_id, std::shared_ptr<NVMeoFInitiator> nof_initiator)
    : engine_(engine),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
      nof_initiator_(std::move(nof_initiator)),
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      local_hostname_(local_hostname),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable.
    // When not set, auto-detect based on transport type:
    //   - TCP-only environment: enable memcpy (avoids TCP loopback overhead)
    //   - RDMA/other transports: disable memcpy (RDMA is more efficient)
    const char* env_value = std::getenv("MC_STORE_MEMCPY");
    if (env_value == nullptr) {
        memcpy_enabled_ = engine_.isTcpOnly();
        LOG(INFO) << "MC_STORE_MEMCPY not set, auto-detected: "
                  << (memcpy_enabled_ ? "TCP-only environment, memcpy enabled"
                                      : "non-TCP transport available, memcpy "
                                        "disabled");
    } else {
        std::string env_str(env_value);
        // Convert to lowercase for case-insensitive comparison
        std::transform(env_str.begin(), env_str.end(), env_str.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (env_str == "false" || env_str == "0" || env_str == "no" ||
            env_str == "off") {
            memcpy_enabled_ = false;
        } else if (env_str == "true" || env_str == "1" || env_str == "yes" ||
                   env_str == "on") {
            memcpy_enabled_ = true;
        } else {
            LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY: " << env_str
                         << ", defaulting to enabled";
            memcpy_enabled_ = true;
        }
    }

    if (nof_initiator_) {
        nof_pool_ =
            std::make_unique<NofWorkerPool>(nof_initiator_, numa_socket_id);
    }

    VLOG(1) << "TransferSubmitter initialized with memcpy_enabled="
            << memcpy_enabled_;
}

std::optional<TransferFuture> TransferSubmitter::submit(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, void* ptr, size_t size) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        if (op_code == TransferRequest::READ) {
            future = submitMemoryReadOperation(handle, slices, 0);
        } else {
            TransferStrategy strategy = selectStrategy(handle, slices);

            switch (strategy) {
                case TransferStrategy::LOCAL_MEMCPY:
                    future = submitMemcpyOperation(handle, slices, op_code);
                    break;
                case TransferStrategy::TRANSFER_ENGINE:
                    future =
                        submitTransferEngineOperation(handle, slices, op_code);
                    break;
                default:
                    LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                    return std::nullopt;
            }
        }
    } else if (replica.is_nof_replica()) {
        auto& ssd_desc = replica.get_nof_descriptor();
        auto& handle = ssd_desc.buffer_descriptor;

        if (!ptr || (size == 0)) {
            return std::nullopt;
        }

        future = submitNofOperation(handle, ptr, size, op_code);
    } else {
        future = submitFileReadOperation(replica, slices, op_code);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < replicas.size(); ++i) {
        auto& replica = replicas[i];
        auto& slices = all_slices[i];
        auto& mem_desc = replica.get_memory_descriptor();
        if (!validateTransferParams(mem_desc.buffer_descriptor, slices)) {
            return std::nullopt;
        }
        auto& handle = mem_desc.buffer_descriptor;
        uint64_t offset = 0;
        SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment "
                       << handle.transport_endpoint_;
            return std::nullopt;
        }
        for (auto slice : slices) {
            TransferRequest request;
            request.opcode = op_code;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = handle.buffer_address_ + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    future = submitTransfer(requests);
    // Update metrics on successful submission
    if (future.has_value()) {
        for (auto& slices : all_slices) {
            updateTransferMetrics(slices, op_code);
        }
    }
    return future;
}

TransferEngine::ScatterTransferOperation TransferSubmitter::submitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers) {
    return engine_.submitScatter(transfers);
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_get_offload_object(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices,
    OffloadBufferAccess buffer_access) {
    if (keys.size() != pointers.size()) {
        LOG(ERROR) << "Mismatched offload transfer argument counts";
        return std::nullopt;
    }

    const bool use_local_memcpy =
        buffer_access == OffloadBufferAccess::kLocalAddress;
    if (use_local_memcpy && !canUseLocalMemcpy(transfer_engine_addr)) {
        LOG(ERROR) << "Offload source is not locally addressable: "
                   << transfer_engine_addr;
        return std::nullopt;
    }

    std::vector<TransferRequest> requests;
    std::vector<MemcpyOperation> operations;
    constexpr uint64_t kMaxAddress = std::numeric_limits<uint64_t>::max();
    SegmentHandle seg = 0;
    if (!use_local_memcpy) {
        // Open once: all keys share the same transfer endpoint.
        seg = engine_.openSegment(transfer_engine_addr);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
            return std::nullopt;
        }
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;
        }
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            if (slice.size == 0) continue;
            if (!slice.ptr || pointers[i] > kMaxAddress - offset ||
                slice.size > kMaxAddress - pointers[i] - offset) {
                LOG(ERROR) << "Invalid offload transfer range for key: " << key;
                return std::nullopt;
            }
            if (use_local_memcpy) {
                operations.emplace_back(
                    slice.ptr,
                    reinterpret_cast<const void*>(pointers[i] + offset),
                    slice.size);
            } else {
                requests.emplace_back(TransferRequest{
                    .opcode = TransferRequest::READ,
                    .source = static_cast<char*>(slice.ptr),
                    .target_id = seg,
                    .target_offset = pointers[i] + offset,
                    .length = slice.size,
                });
            }
            offset += slice.size;
        }
    }
    return use_local_memcpy ? submitMemcpyOperations(std::move(operations))
                            : submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];

        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;

        if (op_code == TransferRequest::READ) {
            // READ: from handle (remote buffer) to slice (local buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            // WRITE: from slice (local buffer) to handle (remote buffer)
            dest = reinterpret_cast<void*>(base_address + offset);
            src = slice.ptr;
        }
        offset += slice.size;

        operations.emplace_back(dest, src, slice.size);
    }

    return submitMemcpyOperations(std::move(operations));
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperations(
    std::vector<MemcpyOperation> operations) {
    auto state = std::make_shared<MemcpyOperationState>();
    const size_t operation_count = operations.size();
    MemcpyTask task(std::move(operations), state);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with "
            << operation_count << " operations";

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransfer(
    std::vector<TransferRequest>& requests) {
    // Allocate batch ID
    const size_t batch_size = requests.size();
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return std::nullopt;
    }

    // Submit transfer
    Status s = engine_.submitTransfer(batch_id, requests);
    if (!s.ok()) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << s.code();
        // Note: batch_id will be freed by TransferEngineOperationState
        // destructor if we create the state object, otherwise we need to free
        // it here
        engine_.freeBatchID(batch_id);
        return std::nullopt;
    }

    if (batch_id == INVALID_BATCH_ID) {  // INVALID_BATCH_ID
        LOG(ERROR) << "Invalid batch ID for transfer engine operation";
        return std::nullopt;
    }

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size);

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Transport endpoint is empty for handle with address "
                   << handle.buffer_address_;
        return std::nullopt;
    }
    SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);

    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment for endpoint='"
                   << handle.transport_endpoint_ << "'";
        return std::nullopt;
    }

    // Create transfer requests
    std::vector<TransferRequest> requests;
    requests.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) continue;

        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = slice.size;

        offset += slice.size;
        requests.emplace_back(request);
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryReadOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t src_offset) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::READ,
                                     src_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(handle, slices,
                                             TransferRequest::READ, src_offset);
    }

    LOG(ERROR) << "Read only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryWriteOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t dst_offset) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::WRITE,
                                     dst_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(
            handle, slices, TransferRequest::WRITE, dst_offset);
    }

    LOG(ERROR) << "Write only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryReadOperation(handle, slices, src_offset);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range read not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range read not supported for disk replicas (use full read)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::READ);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeWrite(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (dst_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            dst_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range write overflow: dst_offset=" << dst_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryWriteOperation(handle, slices, dst_offset);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range write not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range write not supported for disk replicas (use full write)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::WRITE);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitNofOperation(
    const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
    const TransferRequest::OpCode op_code) {
    if (!nof_pool_) {
        // Runtime form of the former "NoF transfer requested while USE_NOF
        // is disabled" error.
        LOG(ERROR) << "NoF transfer requested but NoF is unavailable "
                      "(no initiator in this process)";
        return std::nullopt;
    }
    if (handle.transport_endpoint_.empty() || handle.size_ < size) {
        LOG(ERROR) << "Invalid NoF request endpoint="
                   << handle.transport_endpoint_
                   << ", buffer_size=" << handle.size_
                   << ", request_size=" << size;
        return std::nullopt;
    }

    NofSegmentHandle* seg_handle =
        nof_initiator_->OpenSegment(handle.transport_endpoint_);
    if (!seg_handle) {
        LOG(ERROR) << "Failed to open NoF segment endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    uint32_t block_size = nof_initiator_->GetBlockSize(seg_handle);
    // Consumer of dma_alignment: without SGL, ptr must still be
    // block-aligned (the existing precondition from RFC §5.3); once #3251
    // lands SGL with supports_sgl=true, the ptr requirement relaxes to DWORD
    // automatically, with no interface change.
    const auto caps = nof_initiator_->GetCapabilities();
    const uint64_t ptr_alignment =
        caps.supports_sgl ? caps.dma_alignment : block_size;
    if (block_size == kInvalidBlockSize ||
        handle.buffer_address_ % block_size != 0 || size % block_size != 0 ||
        reinterpret_cast<std::uintptr_t>(ptr) % ptr_alignment != 0) {
        LOG(ERROR) << "NoF request offset=" << handle.buffer_address_
                   << ", ptr=" << ptr << ", size=" << size
                   << " is not aligned to block size " << block_size;
        return std::nullopt;
    }

    const NofIOOp op =
        (op_code == TransferRequest::READ) ? NofIOOp::kRead : NofIOOp::kWrite;
    auto state = std::make_shared<NofOperationState>();
    NofTask task(seg_handle, ptr, handle.buffer_address_, size, op, state);
    nof_pool_->submitTask(std::move(task));

    VLOG(1) << "NoF transfer submitted to " << handle.transport_endpoint_;
    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    auto state = std::make_shared<FilereadOperationState>();
    auto disk_replica = replica.get_disk_descriptor();
    std::string file_path = disk_replica.file_path;
    size_t file_length = disk_replica.object_size;

    // Submit memcpy operations to worker pool for async execution
    FilereadTask task(file_path, file_length, slices, state);
    fileread_pool_->submitTask(std::move(task));

    VLOG(1) << "Fileread transfer submitted to worker pool with " << file_path;

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& /* slices */) const {
    return canUseLocalMemcpy(handle.transport_endpoint_)
               ? TransferStrategy::LOCAL_MEMCPY
               : TransferStrategy::TRANSFER_ENGINE;
}

bool TransferSubmitter::canUseLocalMemcpy(const std::string& endpoint) const {
    return memcpy_enabled_ &&
           (isSameProcessEndpoint(endpoint, local_hostname_) ||
            isSameProcessEndpoint(endpoint, local_endpoint_));
}

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    return !handle_endpoint.empty() && handle_endpoint == local_endpoint;
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ != all_slice_len) {
        LOG(ERROR) << "handles len:" << handle.size_
                   << ", all_slice_len:" << all_slice_len;
        return false;
    }
    return true;
}

void TransferSubmitter::updateTransferMetrics(const std::vector<Slice>& slices,
                                              TransferRequest::OpCode op_code) {
    size_t total_bytes = 0;
    for (const auto& slice : slices) {
        total_bytes += slice.size;
    }

    if (transfer_metric_ == nullptr) {
        return;
    }

    if (op_code == TransferRequest::READ) {
        transfer_metric_->total_read_bytes.inc(total_bytes);

    } else if (op_code == TransferRequest::WRITE) {
        transfer_metric_->total_write_bytes.inc(total_bytes);
    }
}

}  // namespace mooncake
