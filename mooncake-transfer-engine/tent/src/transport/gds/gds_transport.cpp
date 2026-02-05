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
                // Destroy the batch handle (don't return to pool since we're
                // shutting down)
                cuFileBatchIODestroy(gds_batch->batch_handle->handle);
                delete gds_batch->batch_handle;
                // Deallocate the sub-batch
                Slab<GdsSubBatch>::Get().deallocate(gds_batch);
            }
            allocated_batches_.clear();
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

    // Return the handle to pool for reuse (avoid expensive
    // cuFileBatchIODestroy) Note: Caller should ensure all IOs are completed
    // (via getTransferStatus) before calling freeSubBatch, as cuFile may still
    // access io_params otherwise
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        handle_pool_.push_back(gds_batch->batch_handle);
    }

    // Deallocate the GdsSubBatch (each allocation gets a fresh one)
    Slab<GdsSubBatch>::Get().deallocate(gds_batch);
    batch = nullptr;
    return Status::OK();
}

std::string GdsTransport::getGdsFilePath(SegmentID target_id) {
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok() || desc->type != SegmentType::File) return "";
    auto& detail = std::get<FileSegmentDesc>(desc->detail);
    if (detail.buffers.empty()) return "";
    return detail.buffers[0].path;
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
        IOParamRange range{gds_batch->io_params.size(), 0};
        for (size_t offset = 0; offset < request.length;
             offset += kMaxSliceSize) {
            size_t length = std::min(kMaxSliceSize, request.length - offset);
            CUfileIOParams_t params;
            params.mode = CUFILE_BATCH;
            params.opcode =
                (request.opcode == Request::READ) ? CUFILE_READ : CUFILE_WRITE;
            params.cookie = (void*)0;
            params.u.batch.devPtr_base = request.source;
            params.u.batch.devPtr_offset = offset;
            params.u.batch.file_offset = request.target_offset + offset;
            params.u.batch.size = length;
            params.fh = context->getHandle();
            gds_batch->io_params.push_back(params);
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
    unsigned num_tasks = gds_batch->io_param_ranges.size();
    if (task_id < 0 || task_id >= (int)num_tasks)
        return Status::InvalidArgument("Invalid task ID");
    auto range = gds_batch->io_param_ranges[task_id];
    auto result =
        cuFileBatchIOGetStatus(gds_batch->batch_handle->handle, 0, &num_tasks,
                               gds_batch->io_events.data(), nullptr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to get GDS batch status: Code ") +
            std::to_string(result.err) + LOC_MARK);
    status.s = PENDING;
    size_t complete_count = 0;
    for (size_t index = range.base; index < range.base + range.count; ++index) {
        auto& event = gds_batch->io_events[index];
        auto s = parseTransferStatus(event.status);
        if (s == COMPLETED)
            complete_count++;
        else if (s != PENDING)
            status.s = s;
        status.transferred_bytes += event.ret;
    }
    if (complete_count == range.count) status.s = COMPLETED;
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
