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

#include "tent/transport/nvlink/nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>
#include <sys/mman.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {

NVLinkTransport::NVLinkTransport() : installed_(false) {}

NVLinkTransport::~NVLinkTransport() { uninstall(); }

Status NVLinkTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "NVLink transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;
    installed_ = true;
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 0) * 1024;
    host_register_ = conf_->get("transports/nvlink/host_register", false);
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    return setPeerAccess();
}

Status NVLinkTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        for (auto& relocate_map : relocate_map_) {
            for (auto& entry : relocate_map.second) {
                CHECK_CUDA(cudaIpcCloseMemHandle(entry.second.shm_addr));
            }
        }
        relocate_map_.clear();
        installed_ = false;
    }
    return Status::OK();
}

struct CudaStreamNVLinkRAII {
    cudaStream_t stream_;
    CudaStreamNVLinkRAII() {
        cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking);
    }
    ~CudaStreamNVLinkRAII() { cudaStreamDestroy(stream_); }
};

thread_local CudaStreamNVLinkRAII tl_stream_nvlink;

Status NVLinkTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto shm_batch = Slab<NVLinkSubBatch>::Get().allocate();
    if (!shm_batch)
        return Status::InternalError("Unable to allocate NVLink sub-batch");
    batch = shm_batch;
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
    shm_batch->stream = tl_stream_nvlink.stream_;
    // CHECK_CUDA(cudaStreamCreateWithFlags(&shm_batch->stream,
    // cudaStreamNonBlocking));
    return Status::OK();
}

Status NVLinkTransport::freeSubBatch(SubBatchRef& batch) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid NVLink sub-batch" LOC_MARK);
    // CHECK_CUDA(cudaStreamDestroy(shm_batch->stream));
    Slab<NVLinkSubBatch>::Get().deallocate(shm_batch);
    batch = nullptr;
    return Status::OK();
}

Status NVLinkTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid NVLink sub-batch" LOC_MARK);
    if (request_list.size() + shm_batch->task_list.size() > shm_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto& request : request_list) {
        shm_batch->task_list.push_back(NVLinkTask{});
        auto& task = shm_batch->task_list[shm_batch->task_list.size() - 1];
        uint64_t target_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task, shm_batch);
    }
    return Status::OK();
}

void NVLinkTransport::startTransfer(NVLinkTask* task, NVLinkSubBatch* batch) {
    cudaError_t err;
    void *src = nullptr, *dst = nullptr;

    // Determine direction and addresses
    if (task->request.opcode == Request::READ) {
        dst = task->request.source;      // read into source buffer
        src = (void*)task->target_addr;  // from remote
    } else {
        src = task->request.source;      // write from source buffer
        dst = (void*)task->target_addr;  // to remote
    }

    bool is_async = (task->request.length >= async_memcpy_threshold_);

    cudaPointerAttributes src_attr_info, dst_attr_info;
    cudaMemoryType src_type = cudaMemoryTypeHost, dst_type = cudaMemoryTypeHost;
    if (cudaPointerGetAttributes(&src_attr_info, src) == cudaSuccess) {
        src_type = src_attr_info.type;
    }
    if (cudaPointerGetAttributes(&dst_attr_info, dst) == cudaSuccess) {
        dst_type = dst_attr_info.type;
    }

    cudaMemcpyKind kind = cudaMemcpyDefault;
    if (src_type == cudaMemoryTypeDevice && dst_type == cudaMemoryTypeHost) {
        kind = cudaMemcpyDeviceToHost;
    } else if (src_type == cudaMemoryTypeHost &&
               dst_type == cudaMemoryTypeDevice) {
        kind = cudaMemcpyHostToDevice;
    } else if (src_type == cudaMemoryTypeDevice &&
               dst_type == cudaMemoryTypeDevice) {
        kind = cudaMemcpyDeviceToDevice;
    } else if (src_type == cudaMemoryTypeHost &&
               dst_type == cudaMemoryTypeHost) {
        kind = cudaMemcpyHostToHost;
    }

    if (!is_async) {
        err = cudaMemcpy(dst, src, task->request.length, kind);
        if (err != cudaSuccess) {
            task->status_word = TransferStatusEnum::FAILED;
        } else {
            task->transferred_bytes = task->request.length;
            task->status_word = TransferStatusEnum::COMPLETED;
        }
        return;
    }

    err = cudaMemcpyAsync(dst, src, task->request.length, kind, batch->stream);

    if (err != cudaSuccess) task->status_word = TransferStatusEnum::FAILED;
}

Status NVLinkTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto& task = shm_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    if (task.status_word == TransferStatusEnum::PENDING) {
        auto err = cudaStreamQuery(shm_batch->stream);
        if (err == cudaSuccess) {
            cudaStreamSynchronize(shm_batch->stream);
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (err != cudaErrorNotReady) {
            task.status_word = TransferStatusEnum::FAILED;
        }
    }
    return Status::OK();
}

Status NVLinkTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    LocationParser location(desc.location);
    if (location.type() == "cuda") {
        // If the memory region is allocated using cuMemAlloc,
        // we cannot use cudaIpcGetMemHandle, so skip it
        if (options.type == MNNVL) return Status::OK();
        cudaIpcMemHandle_t handle;
        CHECK_CUDA(cudaIpcGetMemHandle(&handle, (void*)desc.addr));
        desc.shm_path =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
    } else if (location.type() == "cpu" ||
               location.type() == kWildcardLocation) {
        if (host_register_)
            CHECK_CUDA(cudaHostRegister(((void*)desc.addr), desc.length,
                                        cudaHostRegisterDefault));
    } else
        return Status::InvalidArgument(
            "Unrecognized location - neither cpu or cuda: " + location.type());
    desc.transports.push_back(TransportType::NVLINK);
    return Status::OK();
}

Status NVLinkTransport::removeMemoryBuffer(BufferDesc& desc) {
    desc.shm_path.clear();
    LocationParser location(desc.location);
    if (location.type() == "cpu" && host_register_) {
        CHECK_CUDA(cudaHostUnregister((void*)desc.addr));
    }
    return Status::OK();
}

Status NVLinkTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                    uint64_t length,
                                                    uint64_t target_id) {
    thread_local HashMap tl_relocate_map;
    if (tl_relocate_map.empty()) {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        tl_relocate_map = relocate_map_;
    }

    for (auto& entry : tl_relocate_map[target_id]) {
        if (entry.first <= dest_addr &&
            dest_addr + length <= entry.first + entry.second.length) {
            auto shm_addr = entry.second.shm_addr;
            dest_addr = dest_addr - entry.first + ((uint64_t)shm_addr);
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;

    auto buffer = desc->findBuffer(dest_addr, length);
    if (!buffer || buffer->shm_path.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    if (!relocate_map_[target_id].count(buffer->addr)) {
        void* shm_addr = nullptr;
        LocationParser location(buffer->location);
        if (location.type() != "cuda") {
            return Status::InvalidArgument(
                "Requested address is not in registered CUDA buffer" LOC_MARK);
        }
        std::vector<unsigned char> output_buffer;
        deserializeBinaryData(buffer->shm_path, output_buffer);
        cudaIpcMemHandle_t handle;
        memcpy(&handle, output_buffer.data(), sizeof(handle));
        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        cudaSetDevice(location.index());
        CHECK_CUDA(cudaIpcOpenMemHandle(&shm_addr, handle,
                                        cudaIpcMemLazyEnablePeerAccess));
        cudaSetDevice(cuda_dev);
        OpenedShmEntry shm_entry;
        shm_entry.shm_addr = shm_addr;
        shm_entry.length = buffer->length;
        shm_entry.cuda_id = location.index();
        relocate_map_[target_id][buffer->addr] = shm_entry;
        tl_relocate_map = relocate_map_;
    }

    auto shm_addr = relocate_map_[target_id][buffer->addr].shm_addr;
    dest_addr = dest_addr - buffer->addr + ((uint64_t)shm_addr);
    return Status::OK();
}

Status NVLinkTransport::setPeerAccess() {
    int device_count = 0;
    int cuda_dev = 0;
    CHECK_CUDA(cudaGetDevice(&cuda_dev));
    CHECK_CUDA(cudaGetDeviceCount(&device_count));
    if (device_count < 2) return Status::OK();
    for (int i = 0; i < device_count; ++i) {
        cudaSetDevice(i);
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            cudaDeviceCanAccessPeer(&can_access, i, j);
            if (!can_access) {
                continue;
            }
            cudaError_t err = cudaDeviceEnablePeerAccess(j, 0);
            if (err != cudaSuccess) {
                if (err == cudaErrorPeerAccessAlreadyEnabled) {
                    cudaGetLastError();
                } else {
                    cudaSetDevice(cuda_dev);
                    return Status::InternalError(
                        "cudaDeviceEnablePeerAccess failed");
                }
            }
        }
    }
    cudaSetDevice(cuda_dev);
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
