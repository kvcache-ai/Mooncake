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

#include "v1/transport/gds/gds_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "v1/memory/slab.h"

namespace mooncake {
namespace v1 {
class GdsFileContext {
   public:
    explicit GdsFileContext(const std::string &path) : ready_(false) {
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

    GdsFileContext(const GdsFileContext &) = delete;
    GdsFileContext &operator=(const GdsFileContext &) = delete;

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
            return WAITING;
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

Status GdsTransport::install(std::string &local_segment_name,
                             std::shared_ptr<MetadataService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "GDS transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    return Status::OK();
}

Status GdsTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status GdsTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto gds_batch = Slab<GdsSubBatch>::Get().allocate();
    if (!gds_batch)
        return Status::InternalError("Unable to allocate GDS sub-batch");
    batch = gds_batch;
    gds_batch->max_size = max_size;
    gds_batch->io_params.reserve(max_size);
    gds_batch->io_events.resize(max_size);
    auto result = cuFileBatchIOSetUp(&gds_batch->handle, max_size);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to setup GDS batch IO: Code ") +
            std::to_string(result.err) + LOC_MARK);
    return Status::OK();
}

Status GdsTransport::freeSubBatch(SubBatchRef &batch) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    cuFileBatchIODestroy(gds_batch->handle);
    Slab<GdsSubBatch>::Get().deallocate(gds_batch);
    batch = nullptr;
    return Status::OK();
}

std::string GdsTransport::getGdsFilePath(SegmentID target_id) {
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok() || desc->type != SegmentType::File) return "";
    auto &detail = std::get<FileSegmentDesc>(desc->detail);
    if (detail.buffers.empty()) return "";
    return detail.buffers[0].path;
}

GdsFileContext *GdsTransport::findFileContext(SegmentID target_id) {
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
    SubBatchRef batch, const std::vector<Request> &request_list) {
    const static size_t kMaxSliceSize = 16ull << 20;
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    if (!gds_batch)
        return Status::InvalidArgument("Invalid GDS sub-batch" LOC_MARK);
    size_t num_params = 0;
    size_t first_param_index = gds_batch->io_params.size();
    for (auto &request : request_list)
        num_params += (request.length + kMaxSliceSize - 1) / kMaxSliceSize;
    if (first_param_index + num_params > gds_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        GdsFileContext *context = findFileContext(request.target_id);
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
            params.cookie = (void *)0;
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
        cuFileBatchIOSubmit(gds_batch->handle, num_params,
                            &gds_batch->io_params[first_param_index], 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to submit GDS batch IO: Code ") +
            std::to_string(result.err) + LOC_MARK);
    return Status::OK();
}

Status GdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto gds_batch = dynamic_cast<GdsSubBatch *>(batch);
    unsigned num_tasks = gds_batch->io_param_ranges.size();
    if (task_id < 0 || task_id >= (int)num_tasks)
        return Status::InvalidArgument("Invalid task ID");
    auto range = gds_batch->io_param_ranges[task_id];
    auto result = cuFileBatchIOGetStatus(gds_batch->handle, 0, &num_tasks,
                                         gds_batch->io_events.data(), nullptr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to get GDS batch status: Code ") +
            std::to_string(result.err) + LOC_MARK);
    status.s = WAITING;
    size_t complete_count = 0;
    for (size_t index = range.base; index < range.base + range.count; ++index) {
        auto &event = gds_batch->io_events[index];
        auto s = parseTransferStatus(event.status);
        if (s == COMPLETED)
            complete_count++;
        else if (s != WAITING)
            status.s = s;
        status.transferred_bytes += event.ret;
    }
    if (complete_count == range.count) status.s = COMPLETED;
    return Status::OK();
}

Status GdsTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    auto location = parseLocation(options.location);
    if (location.first != "cuda") return Status::OK();
    auto result = cuFileBufRegister((void *)desc.addr, desc.length, 0);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to register GDS buffer: Code ") +
            std::to_string(result.err) + LOC_MARK);
    desc.transports.push_back(GDS);
    return Status::OK();
}

Status GdsTransport::removeMemoryBuffer(BufferDesc &desc) {
    auto location = parseLocation(desc.location);
    if (location.first != "cuda") return Status::OK();
    auto result = cuFileBufDeregister((void *)desc.addr);
    if (result.err != CU_FILE_SUCCESS)
        return Status::InternalError(
            std::string("Failed to deregister GDS buffer: Code ") +
            std::to_string(result.err) + LOC_MARK);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
