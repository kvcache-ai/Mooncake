// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transport/gds/gds_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <mutex>

#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
class GdsFileContext {
   public:
    explicit GdsFileContext(const std::string& path) : ready_(false) {
        int fd = open(path.c_str(), O_RDWR | O_DIRECT);
        if (fd < 0) {
            PLOG(ERROR) << "Failed to open file " << path;
            return;
        }
        memset(&desc_, 0, sizeof(desc_));
        desc_.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc_.handle.fd = fd;
        auto result = cuFileHandleRegister(&handle_, &desc_);
        if (result.err != CU_FILE_SUCCESS) {
            LOG(ERROR) << "Failed to register GDS file handle: Code "
                       << result.err;
            return;
        }
        ready_ = true;
    }

    GdsFileContext(const GdsFileContext&) = delete;
    GdsFileContext& operator=(const GdsFileContext&) = delete;

    ~GdsFileContext() {
        if (handle_) cuFileHandleDeregister(handle_);
        if (desc_.handle.fd) close(desc_.handle.fd);
    }

    CUfileHandle_t getHandle() const { return handle_; }

    bool ready() const { return ready_; }

   private:
    CUfileHandle_t handle_ = NULL;
    CUfileDescr_t desc_;
    bool ready_;
};

TransferStatusEnum parseTransferStatus(CUfileStatus_t status) {
    switch (status) {
        case CUFILE_WAITING:
            return PENDING;
        case CUFILE_PENDING:
            return PENDING;
        case CUFILE_INVALID:
            return INVALID;
        case CUFILE_CANCELED:
            return CANCELED;
        case CUFILE_COMPLETE:
            return COMPLETED;
        case CUFILE_TIMEOUT:
            return TIMEOUT;
        case CUFILE_FAILED:
        default:
            return FAILED;
    }
}

bool isTerminalCuFileStatus(CUfileStatus_t status) {
    return status != CUFILE_WAITING && status != CUFILE_PENDING;
}

TransferStatus GdsTransport::aggregateTransferStatus(
    const std::vector<CUfileIOEvents_t>& events, size_t base, size_t count,
    bool& all_terminal) {
    all_terminal = true;
    TransferStatus result{COMPLETED, 0};
    if (count == 0 || base > events.size() || count > events.size() - base) {
        result.s = INVALID;
        return result;
    }

    // Use a fixed precedence so completion order does not change the result.
    int failure_priority = 0;
    for (size_t i = base; i < base + count; ++i) {
        const auto& event = events[i];
        const auto slice_status = parseTransferStatus(event.status);
        switch (slice_status) {
            case PENDING:
                all_terminal = false;
                break;
            case COMPLETED:
                if (event.ret > 0) {
                    result.transferred_bytes += static_cast<size_t>(event.ret);
                }
                break;
            case INVALID:
                if (failure_priority < 1) {
                    result.s = INVALID;
                    failure_priority = 1;
                }
                break;
            case CANCELED:
                if (failure_priority < 2) {
                    result.s = CANCELED;
                    failure_priority = 2;
                }
                break;
            case TIMEOUT:
                if (failure_priority < 3) {
                    result.s = TIMEOUT;
                    failure_priority = 3;
                }
                break;
            case FAILED:
                result.s = FAILED;
                failure_priority = 4;
                break;
            case INITIAL:
                all_terminal = false;
                break;
        }
    }

    if (!all_terminal && failure_priority == 0) {
        result.s = PENDING;
    }
    return result;
}

Status GdsTransport::updateBatchStatus(GdsSubBatch* batch) {
    if (batch->io_events.size() < batch->io_params.size() ||
        batch->cached_events.size() < batch->io_params.size()) {
        return Status::InternalError(
            "GDS completion buffers are smaller than the submitted IO "
            "set" LOC_MARK);
    }

    unsigned num_events = static_cast<unsigned>(batch->io_params.size());
    if (num_events == 0) return Status::OK();

    auto result =
        cuFileBatchIOGetStatus(batch->batch_handle->handle, 0, &num_events,
                               batch->io_events.data(), nullptr);
    if (result.err != CU_FILE_SUCCESS) {
        return Status::InternalError(
            std::string("Failed to get GDS batch status: Code ") +
            std::to_string(result.err) + LOC_MARK);
    }

    for (size_t index = 0; index < num_events; ++index) {
        const auto& event = batch->io_events[index];
        const auto cookie = reinterpret_cast<std::uintptr_t>(event.cookie);
        if (cookie == 0 || cookie > batch->cached_events.size()) {
            LOG(ERROR) << "Invalid GDS batch IO cookie: " << cookie;
            continue;
        }

        auto& cached_event = batch->cached_events[cookie - 1];
        if (!isTerminalCuFileStatus(cached_event.status) ||
            isTerminalCuFileStatus(event.status)) {
            cached_event = event;
        }
    }
    return Status::OK();
}

Status GdsTransport::cancelBatch(GdsSubBatch* batch) {
    auto result = cuFileBatchIOCancel(batch->batch_handle->handle);
    if (result.err != CU_FILE_SUCCESS) {
        return Status::InternalError(
            std::string("Failed to cancel GDS batch IO: Code ") +
            std::to_string(result.err) + LOC_MARK);
    }
    return Status::OK();
}

bool GdsTransport::allBatchIOsTerminal(const GdsSubBatch* batch) {
    if (batch->cached_events.size() < batch->io_params.size()) return false;
    return std::all_of(batch->cached_events.begin(),
                       batch->cached_events.begin() + batch->io_params.size(),
                       [](const CUfileIOEvents_t& event) {
                           return isTerminalCuFileStatus(event.status);
                       });
}

bool GdsTransport::isTerminalFailure(TransferStatusEnum status) {
    return status == INVALID || status == CANCELED || status == TIMEOUT ||
           status == FAILED;
}

void GdsTransport::destroySubBatch(GdsSubBatch* batch) {
    if (batch->batch_handle) {
        cuFileBatchIODestroy(batch->batch_handle->handle);
        delete batch->batch_handle;
        batch->batch_handle = nullptr;
    }
    Slab<GdsSubBatch>::Get().deallocate(batch);
}

void GdsTransport::cleanupQuarantinedBatches() {
    std::lock_guard<std::mutex> lock(quarantined_batches_lock_);
    auto it = quarantined_batches_.begin();
    while (it != quarantined_batches_.end()) {
        auto* batch = *it;
        bool ready_to_destroy = false;
        {
            std::lock_guard<std::mutex> status_lock(batch->status_mutex);
            auto status = updateBatchStatus(batch);
            ready_to_destroy = status.ok() && allBatchIOsTerminal(batch);
        }
        if (!ready_to_destroy) {
            ++it;
            continue;
        }
        destroySubBatch(batch);
        it = quarantined_batches_.erase(it);
    }
}

GdsTransport::GdsTransport() : installed_(false) {
    static std::once_flag g_once_flag;
    auto fork_init = []() { cuFileDriverOpen(); };
    std::call_once(g_once_flag, fork_init);
}

GdsTransport::~GdsTransport() { uninstall(); }

Status GdsTransport::install(std::string& local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "GDS transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    io_batch_depth_ = conf_->get("transports/gds/io_batch_depth", 32);
    caps.dram_to_file = true;
    caps.gpu_to_file = true;
    return Status::OK();
}

Status GdsTransport::uninstall() {
    if (installed_) {
        // Clean up all allocated sub-batches (if user forgot to free them)
        {
            std::lock_guard<std::mutex> lock(allocated_batches_lock_);
            for (auto* gds_batch : allocated_batches_) {
                destroySubBatch(gds_batch);
            }
            allocated_batches_.clear();
        }

        {
            std::lock_guard<std::mutex> lock(quarantined_batches_lock_);
            for (auto* gds_batch : quarantined_batches_) {
                destroySubBatch(gds_batch);
            }
            quarantined_batches_.clear();
        }

        // Clean up all handles in the pool
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        for (auto* batch_handle : handle_pool_) {
            cuFileBatchIODestroy(batch_handle->handle);
            delete batch_handle;
        }
        handle_pool_.clear();

        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status GdsTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    cleanupQuarantinedBatches();

    auto gds_batch = Slab<GdsSubBatch>::Get().allocate();
    if (!gds_batch)
        return Status::InternalError("Unable to allocate GDS sub-batch");

    // Get or create BatchHandle from pool
    BatchHandle* batch_handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        if (!handle_pool_.empty()) {
            batch_handle = handle_pool_.back();
            handle_pool_.pop_back();
        }
    }

    // If pool is empty or handle size mismatch, create new handle (expensive
    // operation)
    if (!batch_handle || batch_handle->max_nr != io_batch_depth_) {
        // Destroy mismatched handle if exists
        if (batch_handle) {
            cuFileBatchIODestroy(batch_handle->handle);
            delete batch_handle;
        }

        batch_handle = new BatchHandle();
        batch_handle->max_nr = io_batch_depth_;
        // cuFileBatchIOSetUp is time-costly, so we reuse handles
        auto result =
            cuFileBatchIOSetUp(&batch_handle->handle, io_batch_depth_);
        if (result.err != CU_FILE_SUCCESS) {
            delete batch_handle;
            Slab<GdsSubBatch>::Get().deallocate(gds_batch);
            return Status::InternalError(
                std::string("Failed to setup GDS batch IO: Code ") +
                std::to_string(result.err) + LOC_MARK);
        }
    }

    gds_batch->batch_handle = batch_handle;
    gds_batch->max_size = max_size;
    gds_batch->io_events.resize(io_batch_depth_);
    gds_batch->io_params.clear();
    gds_batch->io_params.reserve(io_batch_depth_);
    gds_batch->io_param_ranges.clear();
    gds_batch->cached_events.clear();
    gds_batch->cached_events.reserve(io_batch_depth_);
    gds_batch->reusable = true;
    gds_batch->cancel_requested = false;

    // Track this batch for cleanup on uninstall
    {
        std::lock_guard<std::mutex> lock(allocated_batches_lock_);
        allocated_batches_.push_back(gds_batch);
    }

    batch = gds_batch;
    return Status::OK();
}

Status GdsTransport::freeSubBatch(SubBatchRef& batch) {
    auto gds_batch = dynamic_cast<GdsSubBatch*>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);

    // Remove from tracking list
    {
        std::lock_guard<std::mutex> lock(allocated_batches_lock_);
        auto it = std::find(allocated_batches_.begin(),
                            allocated_batches_.end(), gds_batch);
        if (it != allocated_batches_.end()) {
            allocated_batches_.erase(it);
        }
    }

    // A normal batch is returned to the handle pool only after callers have
    // observed terminal status for every task. A failed batch with active
    // slices retains both its handle and parameter storage until cuFile no
    // longer references them.
    bool reusable = false;
    {
        std::lock_guard<std::mutex> lock(gds_batch->status_mutex);
        reusable = gds_batch->reusable;
    }
    if (reusable) {
        {
            std::lock_guard<std::mutex> lock(handle_pool_lock_);
            handle_pool_.push_back(gds_batch->batch_handle);
        }
        gds_batch->batch_handle = nullptr;
        Slab<GdsSubBatch>::Get().deallocate(gds_batch);
    } else {
        std::lock_guard<std::mutex> lock(quarantined_batches_lock_);
        quarantined_batches_.push_back(gds_batch);
    }
    batch = nullptr;
    cleanupQuarantinedBatches();
    return Status::OK();
}

std::string GdsTransport::getGdsFilePath(SegmentID target_id) {
    std::string ret;
    auto status = metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            if (segment->type != SegmentType::File)
                return Status::NeedsRefreshCache(
                    "Segment type is not File" LOC_MARK);
            auto& detail = std::get<FileSegmentDesc>(segment->detail);
            if (detail.buffers.empty())
                return Status::NeedsRefreshCache("No buffers found" LOC_MARK);
            ret = detail.buffers[0].path;
            return Status::OK();
        });
    if (!status.ok()) return "";
    return ret;
}

GdsFileContext* GdsTransport::findFileContext(SegmentID target_id) {
    thread_local FileContextMap tl_file_context_map;
    if (tl_file_context_map.count(target_id))
        return tl_file_context_map[target_id].get();

    RWSpinlock::WriteGuard guard(file_context_lock_);
    if (!file_context_map_.count(target_id)) {
        std::string path = getGdsFilePath(target_id);
        if (path.empty()) return nullptr;
        file_context_map_[target_id] = std::make_shared<GdsFileContext>(path);
    }

    tl_file_context_map = file_context_map_;
    return tl_file_context_map[target_id].get();
}

Status GdsTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    const static size_t kMaxSliceSize = 16ull << 20;
    auto gds_batch = dynamic_cast<GdsSubBatch*>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    size_t num_params = 0;
    size_t first_param_index = gds_batch->io_params.size();
    for (auto& request : request_list)
        num_params += (request.length + kMaxSliceSize - 1) / kMaxSliceSize;
    if (first_param_index + num_params > io_batch_depth_)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto& request : request_list) {
        GdsFileContext* context = findFileContext(request.target_id);
        if (!context || !context->ready())
            return Status::InvalidArgument("Invalid remote segment" LOC_MARK);
        IOParamRange range;
        range.base = gds_batch->io_params.size();
        for (size_t offset = 0; offset < request.length;
             offset += kMaxSliceSize) {
            size_t length = std::min(kMaxSliceSize, request.length - offset);
            const size_t slice_id = gds_batch->io_params.size();
            CUfileIOParams_t params;
            params.mode = CUFILE_BATCH;
            params.opcode =
                (request.opcode == Request::READ) ? CUFILE_READ : CUFILE_WRITE;
            // Use a one-based slice index so every completion can be cached
            // independently, including the first slice (cookie 0 is avoided).
            params.cookie = reinterpret_cast<void*>(
                static_cast<std::uintptr_t>(slice_id + 1));
            params.u.batch.devPtr_base = request.source;
            params.u.batch.devPtr_offset = offset;
            params.u.batch.file_offset = request.target_offset + offset;
            params.u.batch.size = length;
            params.fh = context->getHandle();
            gds_batch->io_params.push_back(params);
            CUfileIOEvents_t cached_event{};
            cached_event.cookie = params.cookie;
            cached_event.status = CUFILE_PENDING;
            gds_batch->cached_events.push_back(cached_event);
            range.count++;
        }
        gds_batch->io_param_ranges.push_back(range);
    }

    auto result =
        cuFileBatchIOSubmit(gds_batch->batch_handle->handle, num_params,
                            &gds_batch->io_params[first_param_index], 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to submit GDS batch IO: Code ") +
            std::to_string(result.err) + LOC_MARK);
    return Status::OK();
}

Status GdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus& status) {
    auto gds_batch = dynamic_cast<GdsSubBatch*>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    unsigned num_tasks = gds_batch->io_param_ranges.size();
    if (task_id < 0 || task_id >= (int)num_tasks)
        return Status::InvalidArgument("Invalid task ID");
    std::lock_guard<std::mutex> lock(gds_batch->status_mutex);
    auto& range = gds_batch->io_param_ranges[task_id];
    if (range.status != PENDING) {
        status = TransferStatus{range.status, range.transferred_bytes};
        return Status::OK();
    }

    auto update_status = updateBatchStatus(gds_batch);
    if (!update_status.ok()) {
        // The runtime may release the sub-batch after a polling error. Keep
        // cuFile-owned storage out of the reusable pool unless we observed a
        // terminal event for every submitted slice.
        gds_batch->reusable = false;
        if (!gds_batch->cancel_requested) {
            gds_batch->cancel_requested = true;
            auto cancel_status = cancelBatch(gds_batch);
            if (!cancel_status.ok()) {
                LOG(WARNING) << cancel_status.ToString();
            }
        }
        return update_status;
    }
    bool all_terminal = false;
    auto task_status = aggregateTransferStatus(
        gds_batch->cached_events, range.base, range.count, all_terminal);
    if (range.known_failure == PENDING && isTerminalFailure(task_status.s)) {
        range.known_failure = task_status.s;
    }

    if (range.known_failure != PENDING && !all_terminal) {
        if (!gds_batch->cancel_requested) {
            gds_batch->cancel_requested = true;
            auto cancel_status = cancelBatch(gds_batch);
            if (!cancel_status.ok()) {
                LOG(WARNING) << cancel_status.ToString();
            }
        }

        // Cancellation is best effort. Poll once more, but do not publish a
        // terminal status while cuFile may still access the user buffer.
        auto repoll_status = updateBatchStatus(gds_batch);
        if (!repoll_status.ok()) {
            LOG(WARNING) << repoll_status.ToString();
        }
        task_status = aggregateTransferStatus(
            gds_batch->cached_events, range.base, range.count, all_terminal);
        if (!all_terminal) {
            gds_batch->reusable = false;
        }
    }

    // Expose partial progress while the task is still pending, and keep the
    // reported byte count monotonic across repeated polls.
    range.transferred_bytes =
        std::max(range.transferred_bytes, task_status.transferred_bytes);
    if (all_terminal) {
        range.status = range.known_failure != PENDING ? range.known_failure
                                                      : task_status.s;
        gds_batch->reusable = allBatchIOsTerminal(gds_batch);
    }
    status = TransferStatus{range.status, range.transferred_bytes};
    return Status::OK();
}

Status GdsTransport::addMemoryBuffer(BufferDesc& desc,
                                     const MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() != "cuda") return Status::OK();
    auto result = cuFileBufRegister((void*)desc.addr, desc.length, 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to register GDS buffer: Code ") +
            std::to_string(result.err) + LOC_MARK);
    desc.transports.push_back(GDS);
    return Status::OK();
}

Status GdsTransport::removeMemoryBuffer(BufferDesc& desc) {
    LocationParser location(desc.location);
    if (location.type() != "cuda") return Status::OK();
    auto result = cuFileBufDeregister((void*)desc.addr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to deregister GDS buffer: Code ") +
            std::to_string(result.err) + LOC_MARK);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
