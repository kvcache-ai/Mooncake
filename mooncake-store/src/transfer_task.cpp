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
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

#ifdef USE_NOF
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

static bool IsSpdkNofDebugEnabled() {
    static const bool enabled = IsTruthyEnv(std::getenv("MC_NOF_DEBUG"));
    return enabled;
}

static int GetSpdkNofDebugIntervalMs() {
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

static int GetSpdkNofSubmitChunkBytes() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_SUBMIT_CHUNK_BYTES", mooncake::kDefaultSpdkNofSubmitChunkBytes);
    return value;
}

static int GetSpdkNofInflightBytesLimit() {
    static const int value =
        GetPositiveEnvOrDefault("MC_NOF_INFLIGHT_BYTES_LIMIT",
                                mooncake::kDefaultSpdkNofInflightBytesLimit);
    return value;
}

static int GetSpdkNofWorkerCount() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_WORKERS", mooncake::kDefaultSpdkNofWorkers);
    return value;
}

static int CountSpdkNofQueuedTasks(const mooncake::SpdkNofTask* head) {
    int count = 0;
    const mooncake::SpdkNofTask* cursor = head;
    while (cursor != nullptr) {
        ++count;
        cursor = cursor->nxt;
    }
    return count;
}

static void nvmf_reset_sgl(void* ctx, uint32_t offset) {
    auto* sub_task = reinterpret_cast<mooncake::SpdkNofSubTask*>(ctx);
    if (sub_task == nullptr) {
        return;
    }

    sub_task->cursor_valid = false;
    sub_task->cursor_sge_index = 0;
    sub_task->cursor_sge_offset = 0;
    sub_task->cursor_remaining = 0;

    if (sub_task->task == nullptr ||
        static_cast<uint64_t>(offset) > sub_task->payload_size ||
        sub_task->payload_offset > std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(offset)) {
        return;
    }

    uint64_t skip = sub_task->payload_offset + static_cast<uint64_t>(offset);
    const auto& sgl = sub_task->task->sgl;
    size_t index = 0;

    while (index < sgl.size()) {
        const auto& entry = sgl[index];
        if (skip < entry.length) {
            sub_task->cursor_sge_index = index;
            sub_task->cursor_sge_offset = skip;
            sub_task->cursor_remaining = sub_task->payload_size - static_cast<uint64_t>(offset);
            sub_task->cursor_valid = true;
            return;
        }
        skip -= entry.length;
        ++index;
    }

    if (skip == 0 &&
        static_cast<uint64_t>(offset) == sub_task->payload_size) {
        sub_task->cursor_sge_index = sgl.size();
        sub_task->cursor_sge_offset = 0;
        sub_task->cursor_remaining = 0;
        sub_task->cursor_valid = true;
    }
}

static int nvmf_next_sge(void* ctx, void** address, uint32_t* length) {
    auto* sub_task = reinterpret_cast<mooncake::SpdkNofSubTask*>(ctx);
    if (sub_task == nullptr || sub_task->task == nullptr ||
        address == nullptr || length == nullptr ||
        !sub_task->cursor_valid || sub_task->cursor_remaining == 0) {
        return -EINVAL;
    }

    const auto& sgl = sub_task->task->sgl;
    while (sub_task->cursor_sge_index < sgl.size()) {
        const auto& entry = sgl[sub_task->cursor_sge_index];

        uint64_t available = entry.length - sub_task->cursor_sge_offset;
        available = std::min(available, sub_task->cursor_remaining);
        constexpr uint64_t kMaxSgeLength = static_cast<uint64_t>(UINT32_MAX) & ~3ULL;
        available = std::min(available, kMaxSgeLength);
        if (available == 0) {
            return -EINVAL;
        }

        const uintptr_t base = reinterpret_cast<uintptr_t>(entry.address);
        
        *address = reinterpret_cast<void*>(base + sub_task->cursor_sge_offset);
        *length = static_cast<uint32_t>(available);

        sub_task->cursor_sge_offset += available;
        sub_task->cursor_remaining -= available;
        if (sub_task->cursor_sge_offset == entry.length) {
            ++sub_task->cursor_sge_index;
            sub_task->cursor_sge_offset = 0;
        }
        return 0;
    }

    return -EINVAL;
}

static inline void SpdkNofTaskCompletion(mooncake::SpdkNofTask* task) {
    if (task->remaining_lba == 0 && task->outstanding_sub_io == 0) {
        task->state->set_completed(task->failed
                                       ? mooncake::ErrorCode::TRANSFER_FAIL
                                       : mooncake::ErrorCode::OK);
        if (!task->on_chain) {
            delete task;
        }
    }
}

static void nvmf_io_complete(void* ctx, const struct spdk_nvme_cpl* cpl) {
    if (!ctx) {
        LOG(ERROR) << "nvmf_io_complete ctx is null";
        return;
    }

    mooncake::SpdkNofSubTask* sub_task =
        reinterpret_cast<mooncake::SpdkNofSubTask*>(ctx);
    mooncake::SpdkNofTask* task = sub_task->task;
    mooncake::SpdkNofQos* nof_qos = task->nof_qos;
    int op = task->op;
    if (--(*task->io_count) < 0) {
        LOG(ERROR) << "total outstanding io < 0";
    }

    if (--(task->outstanding_sub_io) < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    nof_qos->inflight_blocks[op] -= sub_task->submit_lba_count;
    if (nof_qos->inflight_blocks[op] < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    if (spdk_nvme_cpl_is_error(cpl)) {
        LOG(ERROR) << "task_complete: I/O failed"
                   << spdk_nvme_cpl_get_status_string(&cpl->status);
        task->remaining_lba = 0;
        task->failed = true;
    }

    auto* sub_task_pool = sub_task->sub_task_pool;
    sub_task->task = nullptr;
    sub_task_pool->push(sub_task);
    SpdkNofTaskCompletion(task);
}
#endif
namespace mooncake {

#ifdef USE_NOF
SpdkNofQos::SpdkNofQos(uint32_t block_size) {
    int block_size_int = static_cast<int>(block_size);
    if (block_size_int <= 0) {
        block_size_int = 1;
    }

    blocks_per_chunk =
        std::max(1, GetSpdkNofSubmitChunkBytes() / block_size_int);
    inflight_blocks_limit =
        std::max(1, GetSpdkNofInflightBytesLimit() / block_size_int);
    for (int i = 0; i < kSpdkNofOpNum; ++i) {
        inflight_blocks[i] = 0;
        head[i] = nullptr;
        tail[i] = nullptr;
    }
}
#endif

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 10 worker
// threads.
constexpr int kDefaultFilereadWorkers = 10;

FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    VLOG(1) << "Creating FilereadWorkerPool with " << kDefaultFilereadWorkers
            << " workers";

    // Start worker threads
    workers_.reserve(kDefaultFilereadWorkers);
    for (int i = 0; i < kDefaultFilereadWorkers; ++i) {
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
// SpdkNofWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 4 worker
// threads.

#ifdef USE_NOF
SpdkNofWorkerPool::SpdkNofWorkerPool(int numa_socket_id)
    : worker_count_(GetSpdkNofWorkerCount()),
      numa_socket_id_(numa_socket_id),
      task_queue_(std::make_unique<std::queue<SpdkNofTask>[]>(worker_count_)),
      queue_mutex_(std::make_unique<std::mutex[]>(worker_count_)),
      queue_cv_(std::make_unique<std::condition_variable[]>(worker_count_)),
      shutdown_(false) {
    VLOG(1) << "Creating SpdkNofWorkerPool with " << worker_count_
            << " workers";

    // Start worker threads
    workers_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&SpdkNofWorkerPool::workerThread, this, i);
    }
}

SpdkNofWorkerPool::~SpdkNofWorkerPool() {
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

    VLOG(1) << "SpdkNofWorkerPool destroyed";
}

void SpdkNofWorkerPool::submitTask(SpdkNofTask task) {
    if (!task.state) {
        LOG(ERROR) << "Attempting to submit spdk nof task without state";
        return;
    }

    if (shutdown_.load()) {
        LOG(WARNING)
            << "Attempting to submit task to shutdown SpdkNofWorkerPool";
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    int worker_idx = -1;
    {
        std::lock_guard<std::mutex> lock(seg_mutex_);
        nof_seg_handle* seg = task.seg_handle;
        bool new_binding = false;
        if (seg_to_worker_.find(seg) != seg_to_worker_.end()) {
            worker_idx = seg_to_worker_[seg];
        } else {
            worker_idx = (seg_num++ % worker_count_);
            seg_to_worker_[seg] = worker_idx;
            new_binding = true;
        }
        if (new_binding && IsSpdkNofDebugEnabled()) {
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
    const std::map<nof_seg_handle*, std::unique_ptr<SpdkNofQos>>& seg_to_qos) {
    for (const auto& [_, nof_qos] : seg_to_qos) {
        if (!nof_qos->Empty()) {
            return true;
        }
    }
    return false;
}

constexpr int kSpdkNofSubTaskChunkSize = 4096;

static inline bool CheckSubTaskPool(
    std::stack<SpdkNofSubTask*>& sub_task_pool,
    std::vector<SpdkNofSubTask*>& sub_task_chunks, int work_idx) {
    if (!sub_task_pool.empty()) {
        return true;
    }
    SpdkNofSubTask* sub_tasks =
        new (std::nothrow) SpdkNofSubTask[kSpdkNofSubTaskChunkSize];
    if (!sub_tasks) {
        LOG(ERROR) << "alloc SpdkNofSubTask failed, worker " << work_idx;
        return false;
    }
    sub_task_chunks.push_back(sub_tasks);

    for (int i = 0; i < kSpdkNofSubTaskChunkSize; ++i) {
        sub_tasks[i].sub_task_pool = &sub_task_pool;
        sub_task_pool.push(&sub_tasks[i]);
    }

    return true;
}

void SpdkNofWorkerPool::workerThread(int work_idx) {
    bindToSocket(numa_socket_id_);
    VLOG(2) << "SpdkNofWorkerPool worker thread started";

    int64_t total_outstanding_io = 0;
    // std::set<nof_seg_handle *> seg_set;
    std::map<nof_seg_handle*, std::unique_ptr<SpdkNofQos>> seg_to_qos;
    std::stack<SpdkNofSubTask*> sub_task_pool;
    std::vector<SpdkNofSubTask*> sub_task_chunks;
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
            queue_cv.wait(
                lock, [this, &task_queue, &total_outstanding_io, &seg_to_qos] {
                    return shutdown_.load() || !task_queue.empty() ||
                           total_outstanding_io || HasBufferedTask(seg_to_qos);
                });

            if (shutdown_.load() && task_queue.empty() &&
                (total_outstanding_io == 0) && !HasBufferedTask(seg_to_qos)) {
                break;
            }

            while (!task_queue.empty()) {
                SpdkNofTask* task = new (std::nothrow)
                    SpdkNofTask(std::move(task_queue.front()));
                if (task == nullptr) {
                    LOG(ERROR)
                        << "alloc SpdkNofTask failed, worker " << work_idx;
                    task_queue.front().state->set_completed(ErrorCode::TRANSFER_FAIL);
                    task_queue.pop();
                    continue;
                }

                SpdkNofQos* nof_qos = nullptr;
                auto it = seg_to_qos.find(task->seg_handle);
                if (it == seg_to_qos.end()) {
                    std::unique_ptr<SpdkNofQos> qos(
                        new (std::nothrow) SpdkNofQos(
                            SpdkWrapper::GetInstance().GetBlockSize(
                                task->seg_handle)));
                    if (qos == nullptr) {
                        LOG(ERROR)
                            << "alloc SpdkNofQos failed, worker " << work_idx;
                        task->state->set_completed(ErrorCode::TRANSFER_FAIL);
                        delete task;
                        task_queue.pop();
                        continue;
                    }
                    nof_qos = qos.get();
                    seg_to_qos[task->seg_handle] = std::move(qos);
                    if (IsSpdkNofDebugEnabled()) {
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
                task->io_count = &total_outstanding_io;
                task->nof_qos = nof_qos;
                task->on_chain = true;
                nof_qos->PushTask(task);
                task_queue.pop();
            }
        }

        for (auto& [seg_handle, nof_qos] : seg_to_qos) {
            uint32_t block_size =
                SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
            for (int i = 0; i < kSpdkNofOpNum; ++i) {
                int avail_blocks = nof_qos->inflight_blocks_limit -
                                   nof_qos->inflight_blocks[i];
                while (nof_qos->head[i] && avail_blocks > 0) {
                    SpdkNofTask* task = nof_qos->head[i];

                    while (task->remaining_lba > 0 && avail_blocks > 0) {
                        const uint32_t submit_budget = static_cast<uint32_t>(std::min(
                                avail_blocks, nof_qos->blocks_per_chunk));
                        const uint32_t submit_lba_count =
                            std::min(task->remaining_lba, submit_budget);
                        const uint64_t lba_offset =
                            static_cast<uint64_t>(task->lba_count - task->remaining_lba);
                        const uint64_t submit_lba = task->lba + lba_offset;
                        const uint64_t payload_offset = lba_offset * block_size;
                        const uint64_t payload_size = static_cast<uint64_t>(submit_lba_count) * block_size;

                        if (payload_offset > task->sgl_size ||
                            payload_size > task->sgl_size - payload_offset) {
                            LOG(ERROR)
                                << "Invalid NoF SGL window"
                                << ", payload_offset=" << payload_offset
                                << ", payload_size=" << payload_size
                                << ", sgl_size=" << task->sgl_size;
                            task->failed = true;
                            task->remaining_lba = 0;
                            break;
                        }

                        if (!CheckSubTaskPool(sub_task_pool, sub_task_chunks,
                                              work_idx)) {
                            task->failed = true;
                            task->remaining_lba = 0;
                            break;
                        }
                        SpdkNofSubTask* sub_task = sub_task_pool.top();
                        sub_task_pool.pop();
                        sub_task->task = task;
                        sub_task->submit_lba_count = submit_lba_count;
                        sub_task->payload_offset = payload_offset;
                        sub_task->payload_size = payload_size;

                        nvmf_reset_sgl(sub_task, 0);
                        if (!sub_task->cursor_valid) {
                            LOG(ERROR)
                                << "Failed to initialize NoF SGL cursor";
                            sub_task->task = nullptr;
                            sub_task_pool.push(sub_task);
                            task->failed = true;
                            task->remaining_lba = 0;
                            break;
                        }

                        const int ret = SpdkWrapper::GetInstance().SubmitSglRequest(
                                task->seg_handle, submit_lba,
                                submit_lba_count, task->op, nvmf_io_complete,
                                sub_task, nvmf_reset_sgl, nvmf_next_sge);
                        if (ret != 0) {
                            LOG(ERROR)
                                << "worker " << work_idx << ", seg "
                                << task->seg_handle
                                << " submit SGL I/O failed, ret=" << ret;
                            sub_task->task = nullptr;
                            sub_task_pool.push(sub_task);
                            task->failed = true;
                            task->remaining_lba = 0;
                        } else {
                            
                            task->remaining_lba -= submit_lba_count;
                            nof_qos->inflight_blocks[i] += submit_lba_count;
                            avail_blocks -= submit_lba_count;
                            task->outstanding_sub_io++;
                            total_outstanding_io++;
                        }
                    }
                    if (task->remaining_lba == 0) {
                        nof_qos->PopTask(i);
                        task->on_chain = false;
                        SpdkNofTaskCompletion(task);
                    }
                }
            }
        }

        if (total_outstanding_io > 0) {
            int64_t ret = 0;
            for (auto& it : seg_to_qos) {
                ret = SpdkWrapper::GetInstance().NvmePollProcessCompletion(
                    it.first, 0);
                if (ret < 0) {
                    LOG(ERROR) << "poll completion error: ret " << ret;
                }
            }
        }

        if (IsSpdkNofDebugEnabled()) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_debug_snapshot);
            if (elapsed.count() >= GetSpdkNofDebugIntervalMs()) {
                for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
                    LOG(INFO)
                        << "nof_qos_state worker_idx=" << work_idx
                        << " seg_handle=" << seg_handle
                        << " inflight_read=" << nof_qos->inflight_blocks[0]
                        << " inflight_write=" << nof_qos->inflight_blocks[1]
                        << " inflight_limit=" << nof_qos->inflight_blocks_limit
                        << " queued_read="
                        << CountSpdkNofQueuedTasks(nof_qos->head[0])
                        << " queued_write="
                        << CountSpdkNofQueuedTasks(nof_qos->head[1])
                        << " total_outstanding_io=" << total_outstanding_io;
                }
                last_debug_snapshot = now;
            }
        }
    }

    for (auto* sub_tasks : sub_task_chunks) {
        delete[] sub_tasks;
    }
    // seg_to_qos will automatically clean up SpdkNofQos objects using
    // unique_ptr

    VLOG(2) << "SpdkNofWorkerPool worker thread exiting";
}
#endif

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

TransferSubmitter::TransferSubmitter(TransferEngine& engine,
                                     std::shared_ptr<StorageBackend>& backend,
                                     const std::string& local_hostname,
                                     TransferMetric* transfer_metric,
                                     int numa_socket_id)
    : engine_(engine),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
#ifdef USE_NOF
      spdk_nvmf_pool_(std::make_unique<SpdkNofWorkerPool>(numa_socket_id)),
#endif
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
#ifdef USE_NOF
        auto& ssd_desc = replica.get_nof_descriptor();
        auto& handle = ssd_desc.buffer_descriptor;

        if (!ptr || (size == 0)) {
            return std::nullopt;
        }

        future = submitSpdkNofOperation(handle, ptr, size, op_code);
#else
        LOG(ERROR) << "NoF transfer requested while USE_NOF is disabled";
        return std::nullopt;
#endif
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
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    // Open the segment once — all keys share the same transfer_engine_addr.
    SegmentHandle seg = engine_.openSegment(transfer_engine_addr);
    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
        // nullopt = failure (caller checks !future).  The function returns
        // std::optional so tl::unexpected is not available here.
        return std::nullopt;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const uint64_t pointer = pointers[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;  // fail closed
        }
        // Emit one TransferRequest per slice: the on-disk blob is read
        // sequentially while slices may point to non-contiguous GPU memory.
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            TransferRequest request;
            request.opcode = TransferRequest::READ;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = pointer + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
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

    // Submit memcpy operations to worker pool for async execution
    MemcpyTask task(std::move(operations), state);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with " << slices.size()
            << " operations";

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

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryReadOperation(handle, slices, src_offset);
    } else if (replica.is_nof_replica()) {
#ifdef USE_NOF
        void* range_ptr = slices.front().ptr;
        if (range_ptr == nullptr) {
            return std::nullopt;
        }

        uintptr_t expected = reinterpret_cast<uintptr_t>(range_ptr);
        size_t range_size = 0;
        for (const auto& slice : slices) {
            if (slice.ptr == nullptr ||
                reinterpret_cast<uintptr_t>(slice.ptr) != expected ||
                slice.size > std::numeric_limits<size_t>::max() - range_size ||
                slice.size > std::numeric_limits<uintptr_t>::max() - expected) {
                LOG(ERROR)
                    << "NoF range read requires contiguous slices";
                return std::nullopt;
            }
            range_size += slice.size;
            expected += slice.size;
        }
        future = submitSpdkNofRangeReadOperation(
            replica.get_nof_descriptor(), range_ptr, range_size, src_offset);
#else
        LOG(ERROR)
            << "NoF range read requested while USE_NOF is disabled";
        return std::nullopt;
#endif
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
#ifdef USE_NOF
namespace {
bool AppendDmaSges(void* ptr, uint64_t size,
                   std::vector<SpdkNofSge>* sgl) {
    if (ptr == nullptr || size == 0 || sgl == nullptr) {
        return false;
    }

    uintptr_t cursor = reinterpret_cast<uintptr_t>(ptr);
    uint64_t remaining = size;
    while (remaining != 0) {
        uint64_t contiguous = remaining;
        void* current = reinterpret_cast<void*>(cursor);
        const uint64_t physical_address =
            spdk_vtophys(current, &contiguous);
        if (physical_address == SPDK_VTOPHYS_ERROR || contiguous == 0) {
            LOG(ERROR) << "NoF buffer is not registered for SPDK DMA"
                       << ", address=" << current
                       << ", remaining=" << remaining;
            return false;
        }

        contiguous = std::min(contiguous, remaining);
        sgl->push_back(SpdkNofSge::Data(current, contiguous));
        if (contiguous >
            std::numeric_limits<uintptr_t>::max() - cursor) {
            return false;
        }
        cursor += contiguous;
        remaining -= contiguous;
    }
    return true;
}

bool ValidateSgl(const std::vector<SpdkNofSge>& sgl,
                 uint64_t expected_size,
                 const SpdkSglCapabilities& capabilities) {
    if (!capabilities.supported || sgl.empty()) {
        return false;
    }

    uint64_t total_size = 0;
    for (const auto& entry : sgl) {
        if (entry.address == nullptr || entry.length == 0 ||
            total_size >
                std::numeric_limits<uint64_t>::max() - entry.length) {
            return false;
        }
        if (capabilities.requires_dword_alignment &&
            ((reinterpret_cast<uintptr_t>(entry.address) & 3ULL) != 0 ||
             (entry.length & 3ULL) != 0)) {
            return false;
        }
        total_size += entry.length;
    }
    return total_size == expected_size;
}

bool SizeToLbaCount(uint64_t physical_size, uint32_t block_size,
                    uint32_t* lba_count) {
    if (lba_count == nullptr || block_size == 0 || physical_size == 0 ||
        physical_size % block_size != 0) {
        return false;
    }

    const uint64_t count = physical_size / block_size;
    if (count == 0 || count > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *lba_count = static_cast<uint32_t>(count);
    return true;
}

std::shared_ptr<void> AllocateDmaOwner(size_t size, size_t alignment,
                                       void** buffer) {
    if (buffer == nullptr || size == 0) {
        return {};
    }

    *buffer = SpdkWrapper::GetInstance().Alloc(size, alignment, -1);
    if (*buffer == nullptr) {
        return {};
    }

    return std::shared_ptr<void>(
        *buffer, [](void* ptr) { SpdkWrapper::GetInstance().Free(ptr); });
}

} 

std::optional<TransferFuture> TransferSubmitter::submitSpdkNofOperation(
    const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
    const TransferRequest::OpCode op_code) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Invalid NoF request endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    nof_seg_handle* seg_handle =
        SpdkWrapper::GetInstance().OpenNofSegment(handle.transport_endpoint_);
    if (seg_handle == nullptr) {
        LOG(ERROR) << "Failed to open NoF segment endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    const uint32_t block_size =
        SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0 ||
        handle.buffer_address_ % block_size != 0 ||
        handle.size_ % block_size != 0) {
        LOG(ERROR) << "Invalid NoF block alignment"
                   << ", remote_address=" << handle.buffer_address_
                   << ", allocated_size=" << handle.size_
                   << ", block_size=" << block_size;
        return std::nullopt;
    }

    if (static_cast<uint64_t>(size) >
        std::numeric_limits<uint64_t>::max() - (block_size - 1ULL)) {
        LOG(ERROR) << "NoF physical size overflow";
        return std::nullopt;
    }

    const uint64_t physical_size =
        ((static_cast<uint64_t>(size) + block_size - 1ULL) / block_size) *
        block_size;
    if (physical_size > handle.size_) {
        LOG(ERROR) << "NoF allocation is smaller than request"
                   << ", logical_size=" << size
                   << ", physical_size=" << physical_size
                   << ", allocated_size=" << handle.size_;
        return std::nullopt;
    }

    uint32_t lba_count = 0;
    if (!SizeToLbaCount(physical_size, block_size, &lba_count)) {
        return std::nullopt;
    }

    SpdkSglCapabilities capabilities;
    if (!SpdkWrapper::GetInstance().GetSglCapabilities(
            seg_handle, &capabilities) ||
        !capabilities.supported) {
        LOG(ERROR) << "NVMe Controller does not support SGL";
        return std::nullopt;
    }

    std::vector<SpdkNofSge> sgl;
    std::vector<std::shared_ptr<void>> owners;
    if (!AppendDmaSges(ptr, static_cast<uint64_t>(size), &sgl)) {
        return std::nullopt;
    }

    const uint64_t padding_size =
        physical_size - static_cast<uint64_t>(size);
    if (padding_size != 0) {
        void* padding_buffer = nullptr;
        auto padding_owner =
            AllocateDmaOwner(block_size, block_size, &padding_buffer);
        if (!padding_owner) {
            LOG(ERROR) << "Failed to allocate NoF padding DMA buffer";
            return std::nullopt;
        }
        if (!AppendDmaSges(padding_buffer, padding_size, &sgl)) {
            return std::nullopt;
        }
        owners.push_back(std::move(padding_owner));
    }

    if (!ValidateSgl(sgl, physical_size, capabilities)) {
        LOG(ERROR) << "NoF SGL does not satisfy Controller requirements"
                   << ", dword_required="
                   << capabilities.requires_dword_alignment
                   << ", logical_size=" << size
                   << ", physical_size=" << physical_size
                   << ", ptr=" << ptr;
        return std::nullopt;
    }

    auto state = std::make_shared<SpdkNofOperationState>();
    SpdkNofTask task(seg_handle, std::move(sgl),
                     handle.buffer_address_ / block_size, lba_count, op_code,
                     state, std::move(owners));
    spdk_nvmf_pool_->submitTask(std::move(task));
    return TransferFuture(state);
}

std::optional<TransferFuture>
TransferSubmitter::submitSpdkNofRangeReadOperation(
    const NoFDescriptor& descriptor, void* ptr, size_t size,
    uint64_t src_offset) {
    const auto& handle = descriptor.buffer_descriptor;
    if (handle.transport_endpoint_.empty() || ptr == nullptr || size == 0) {
        LOG(ERROR) << "Invalid NoF range-read request";
        return std::nullopt;
    }

    if (src_offset > descriptor.object_size ||
        static_cast<uint64_t>(size) >
            descriptor.object_size - src_offset) {
        LOG(ERROR) << "NoF logical range-read overflow"
                   << ", src_offset=" << src_offset
                   << ", size=" << size
                   << ", object_size=" << descriptor.object_size;
        return std::nullopt;
    }

    nof_seg_handle* seg_handle =
        SpdkWrapper::GetInstance().OpenNofSegment(handle.transport_endpoint_);
    if (seg_handle == nullptr) {
        LOG(ERROR) << "Failed to open NoF segment endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    const uint32_t block_size =
        SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0 ||
        descriptor.block_size == 0 || descriptor.block_size != block_size ||
        handle.buffer_address_ % block_size != 0 ||
        handle.size_ % block_size != 0) {
        LOG(ERROR) << "Invalid NoF range-read metadata/alignment"
                   << ", descriptor_block_size=" << descriptor.block_size
                   << ", namespace_block_size=" << block_size
                   << ", remote_address=" << handle.buffer_address_
                   << ", allocated_size=" << handle.size_;
        return std::nullopt;
    }

    const uint64_t physical_relative_start =
        (src_offset / block_size) * block_size;
    const uint64_t head_discard =
        src_offset - physical_relative_start;
    if (static_cast<uint64_t>(size) >
        std::numeric_limits<uint64_t>::max() - head_discard) {
        return std::nullopt;
    }

    const uint64_t covered_size =
        head_discard + static_cast<uint64_t>(size);
    if (covered_size >
        std::numeric_limits<uint64_t>::max() - (block_size - 1ULL)) {
        return std::nullopt;
    }

    const uint64_t physical_size =
        ((covered_size + block_size - 1ULL) / block_size) * block_size;
    if (physical_relative_start > handle.size_ ||
        physical_size > handle.size_ - physical_relative_start) {
        LOG(ERROR) << "NoF physical range-read overflow";
        return std::nullopt;
    }
    const uint64_t tail_discard = physical_size - covered_size;

    uint32_t lba_count = 0;
    if (!SizeToLbaCount(physical_size, block_size, &lba_count)) {
        return std::nullopt;
    }

    SpdkSglCapabilities capabilities;
    if (!SpdkWrapper::GetInstance().GetSglCapabilities(
            seg_handle, &capabilities) ||
        !capabilities.supported) {
        LOG(ERROR) << "NVMe Controller does not support SGL";
        return std::nullopt;
    }

    std::vector<SpdkNofSge> sgl;
    std::vector<std::shared_ptr<void>> owners;
    void* discard_buffer = nullptr;
    const uint64_t discard_size = head_discard + tail_discard;
    if (discard_size > std::numeric_limits<size_t>::max()) {
        LOG(ERROR) << "NoF discard DMA buffer size overflows size_t";
        return std::nullopt;
    }
    std::shared_ptr<void> discard_owner;
    if (discard_size != 0) {
        discard_owner = AllocateDmaOwner(static_cast<size_t>(discard_size), block_size, &discard_buffer);
        if (!discard_owner) {
            LOG(ERROR) << "Failed to allocate NoF discard DMA buffer";
            return std::nullopt;
        }
    }

    if (head_discard != 0 &&
        !AppendDmaSges(discard_buffer, head_discard, &sgl)) {
        return std::nullopt;
    }
    if (!AppendDmaSges(ptr, static_cast<uint64_t>(size), &sgl)) {
        return std::nullopt;
    }
    if (tail_discard != 0) {
        const uintptr_t discard_address =
            reinterpret_cast<uintptr_t>(discard_buffer);
        if (head_discard >
            std::numeric_limits<uintptr_t>::max() - discard_address) {
            LOG(ERROR) << "NoF discard DMA buffer address overflows";
            return std::nullopt;
        }
        const uintptr_t tail_address =
            discard_address + static_cast<uintptr_t>(head_discard);
        if (!AppendDmaSges(reinterpret_cast<void*>(tail_address), tail_discard, &sgl)) {
            return std::nullopt;
        }
    }
    if (discard_owner) {
        owners.push_back(std::move(discard_owner));
    }

    if (!ValidateSgl(sgl, physical_size, capabilities)) {
        LOG(ERROR) << "NoF range-read SGL violates Controller requirements"
                   << ", dword_required=" << capabilities.requires_dword_alignment
                   << ", src_offset=" << src_offset << ", destination=" << ptr
                   << ", size=" << size << ", head_discard=" << head_discard
                   << ", tail_discard=" << tail_discard;
        return std::nullopt;
    }

    const uint64_t start_lba = handle.buffer_address_ / block_size + physical_relative_start / block_size;
    auto state = std::make_shared<SpdkNofOperationState>();
    SpdkNofTask task(seg_handle, std::move(sgl), start_lba, lba_count,
                     TransferRequest::READ, state, std::move(owners));
    spdk_nvmf_pool_->submitTask(std::move(task));
    return TransferFuture(state);
}
#endif

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
    const std::vector<Slice>& slices) const {
    // Check if memcpy operations are enabled via environment variable
    if (!memcpy_enabled_) {
        VLOG(2) << "Memcpy operations disabled via MC_STORE_MEMCPY environment "
                   "variable";
        return TransferStrategy::TRANSFER_ENGINE;
    }

    // Check conditions for local memcpy optimization
    if (isLocalTransfer(handle)) {
        return TransferStrategy::LOCAL_MEMCPY;
    }

    return TransferStrategy::TRANSFER_ENGINE;
}

namespace {
// Helper function to extract IP address from endpoint string (ip:port format).
// Supports both IPv4 (ip:port) and IPv6 ([ipv6]:port) formats.
std::string extractIpAddress(const std::string& endpoint) {
    if (endpoint.empty()) {
        return "";
    }

    // Handle IPv6 format: [ipv6]:port
    if (endpoint[0] == '[') {
        size_t closing_bracket = endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            LOG(WARNING) << "Invalid IPv6 endpoint format: " << endpoint;
            return "";
        }
        return endpoint.substr(1, closing_bracket - 1);
    }

    // Handle IPv4 or hostname:port format.
    size_t colon_pos = endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        return endpoint.substr(0, colon_pos);
    }

    // No colon found, return the whole string (might be just IP or hostname).
    return endpoint;
}
}  // namespace

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    if (handle_endpoint.empty() || local_endpoint.empty()) {
        return false;
    }
    if (handle_endpoint == local_endpoint) {
        return true;
    }

    const std::string handle_ip = extractIpAddress(handle_endpoint);
    const std::string local_ip = extractIpAddress(local_endpoint);
    if (!handle_ip.empty() && handle_ip == local_ip) {
        VLOG(2) << "Disabling local memcpy for same-host endpoints with "
                   "different process endpoints: handle="
                << handle_endpoint << ", local=" << local_endpoint;
    }

    return false;
}

bool TransferSubmitter::isLocalTransfer(
    const AllocatedBuffer::Descriptor& handle) const {
    return isSameProcessEndpoint(handle.transport_endpoint_, local_hostname_) ||
           isSameProcessEndpoint(handle.transport_endpoint_, local_endpoint_);
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ < all_slice_len) {
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
