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

#include "tent/transport/mnnvl/mnnvl_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {

static Status buildCUmemAllocationProp(CUmemAllocationHandleType handle_type,
                                       CUmemAllocationProp &prop,
                                       int device_id) {
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = handle_type;
    CUdevice device;
    CHECK_CU(cuDeviceGet(&device, device_id));
    prop.location.id = device;
    int flag = 0;
    CHECK_CU(cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        device));
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    return Status::OK();
}

static Status roundGranularity(CUmemAllocationProp &prop, size_t granularity,
                               size_t &size) {
    CHECK_CU(cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM));
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    return Status::OK();
}

static bool supportFabricMem() {
    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) {
        LOG(ERROR) << "Unable to find CUDA devices: "
                   << cudaGetErrorString(err);
        return false;
    }

    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        auto result = cuDeviceGetAttribute(
            &device_support_fabric_mem,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, device_id);
        if (result != CUDA_SUCCESS || !device_support_fabric_mem) {
            LOG(ERROR) << "Some CUDA devices does not support FABRIC mode";
            return false;
        }
    }
    return true;
}

MnnvlTransport::MnnvlTransport() : installed_(false) {}

MnnvlTransport::~MnnvlTransport() { uninstall(); }

Status MnnvlTransport::install(std::string &local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "MNNVL transport has been installed " LOC_MARK);
    }

    if (!supportFabricMem()) {
        return Status::InvalidArgument(
            "CUDA Fabric handle type not supported " LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;

    // It seems that cudaMemcpy is much faster than cudaMemcpyAsync
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 1024) * 1024 *
        128;

    caps.dram_to_gpu = true;
    if (Platform::getLoader().type() == "cuda") caps.gpu_to_gpu = true;

    installed_ = true;
    supported_ = true;
    auto handle_type_str =
        conf_->get("transports/nvlink/handle_type", "fabric");
    if (handle_type_str == "fabric")
        handle_type_ = CU_MEM_HANDLE_TYPE_FABRIC;
    else
        handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    host_register_ = conf_->get("transports/nvlink/host_register", true);
    return setPeerAccess();
}

Status MnnvlTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        // TODO close all opened entries
        relocate_map_.clear();
        installed_ = false;
    }
    return Status::OK();
}

struct CudaStreamMnnvlRAII {
    cudaStream_t stream_;
    CudaStreamMnnvlRAII() {
        cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking);
    }
    ~CudaStreamMnnvlRAII() { cudaStreamDestroy(stream_); }
};

thread_local CudaStreamMnnvlRAII tl_stream_mnnvl;

Status MnnvlTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mnnvl_batch = Slab<MnnvlSubBatch>::Get().allocate();
    if (!mnnvl_batch)
        return Status::InternalError("Unable to allocate MNNVL sub-batch");
    batch = mnnvl_batch;
    mnnvl_batch->task_list.reserve(max_size);
    mnnvl_batch->max_size = max_size;
    mnnvl_batch->stream = tl_stream_mnnvl.stream_;
    // CHECK_CUDA(cudaStreamCreateWithFlags(&mnnvl_batch->stream,
    // cudaStreamNonBlocking));
    return Status::OK();
}

Status MnnvlTransport::freeSubBatch(SubBatchRef &batch) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);
    // CHECK_CUDA(cudaStreamDestroy(mnnvl_batch->stream));
    Slab<MnnvlSubBatch>::Get().deallocate(mnnvl_batch);
    batch = nullptr;
    return Status::OK();
}

Status MnnvlTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);
    if (request_list.size() + mnnvl_batch->task_list.size() >
        mnnvl_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        mnnvl_batch->task_list.push_back(MnnvlTask{});
        auto &task = mnnvl_batch->task_list[mnnvl_batch->task_list.size() - 1];
        uint64_t target_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to relocate shared memory address: "
                           << status.ToString();
                task.status_word = TransferStatusEnum::FAILED;
                return status;
            }
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task, mnnvl_batch);
    }
    return Status::OK();
}

void MnnvlTransport::startTransfer(MnnvlTask *task, MnnvlSubBatch *batch) {
    cudaError_t err;
    void *src = nullptr, *dst = nullptr;

    // Determine direction and addresses
    if (task->request.opcode == Request::READ) {
        dst = task->request.source;       // read into source buffer
        src = (void *)task->target_addr;  // from remote
    } else {
        src = task->request.source;       // write from source buffer
        dst = (void *)task->target_addr;  // to remote
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

Status MnnvlTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus &status) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)mnnvl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mnnvl_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    if (task.status_word == TransferStatusEnum::PENDING) {
        auto err = cudaStreamQuery(mnnvl_batch->stream);
        if (err == cudaSuccess) {
            cudaStreamSynchronize(mnnvl_batch->stream);
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (err != cudaErrorNotReady) {
            task.status_word = TransferStatusEnum::FAILED;
        }
    }
    return Status::OK();
}

Status MnnvlTransport::addMemoryBuffer(BufferDesc &desc,
                                       const MemoryOptions &options) {
    LocationParser location(desc.location);
    if (location.type() == "cpu" || location.type() == kWildcardLocation) {
        if (host_register_) {
            CHECK_CUDA(cudaHostRegister(((void *)desc.addr), desc.length,
                                        cudaHostRegisterDefault));
        }
        return Status::OK();
    } else if (location.type() != "cuda")
        return Status::InvalidArgument(
            "Unrecognized location - neither cpu or cuda: " + location.type());

    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, (void *)desc.addr);
    if (result != CUDA_SUCCESS) {
        LOG(INFO) << "Memory region " << (void *)desc.addr
                  << "  will not be registered for MNNVL transport.";
        return Status::OK();
    }

    CUmemAllocationProp prop = {};
    size_t granularity = 0;
    CHECK_STATUS(
        buildCUmemAllocationProp(handle_type_, prop, location.index()));
    CHECK_STATUS(roundGranularity(prop, granularity, desc.length));

    if (handle_type_ == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
        int shared_fd;
        CHECK_CU(
            cuMemExportToShareableHandle(&shared_fd, handle, handle_type_, 0));
        desc.mnnvl_handle =
            std::to_string(getpid()) + "-" + std::to_string(shared_fd);
    } else {
        CUmemFabricHandle export_handle;
        CHECK_CU(cuMemExportToShareableHandle(&export_handle, handle,
                                              handle_type_, 0));
        desc.mnnvl_handle =
            serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
    }

    desc.transports.push_back(TransportType::MNNVL);
    return Status::OK();
}

Status MnnvlTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.mnnvl_handle.clear();
    LocationParser location(desc.location);
    if (location.type() == "cpu" && host_register_) {
        CHECK_CUDA(cudaHostUnregister((void *)desc.addr));
    }
    return Status::OK();
}

Status MnnvlTransport::allocateLocalMemory(void **addr, size_t size,
                                           MemoryOptions &options) {
    LocationParser location(options.location);
    if (location.type() != "cuda") {
        return Platform::getLoader().allocate(addr, size, options);
    }

    CUmemAllocationProp prop = {};
    size_t granularity = 0;
    CHECK_STATUS(
        buildCUmemAllocationProp(handle_type_, prop, location.index()));
    CHECK_STATUS(roundGranularity(prop, granularity, size));

    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    auto result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        // return Status::InternalError(std::string("cuMemCreate: ") +
        //                              std::to_string(result) + LOC_MARK);
        LOG(WARNING) << "Fallback to cudaMalloc because the platform does not "
                        "support fabric";
        return Platform::getLoader().allocate(addr, size, options);
    }

    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        cuMemRelease(handle);
        return Status::InternalError(
            std::string("cuMemAddressReserve: cuResult ") +
            std::to_string(result) + LOC_MARK);
    }

    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return Status::InternalError(std::string("cuMemMap: cuResult ") +
                                     std::to_string(result) + LOC_MARK);
    }

    int device_count;
    cudaGetDeviceCount(&device_count);
    CUmemAccessDesc accessDesc[device_count];
    for (int device_id = 0; device_id < device_count; ++device_id) {
        accessDesc[device_id].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[device_id].location.id = device_id;
        accessDesc[device_id].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }

    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc, device_count);
    if (result != CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return Status::InternalError(std::string("cuMemSetAccess: cuResult ") +
                                     std::to_string(result) + LOC_MARK);
    }

    *addr = ptr;
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    allocate_set_.insert(*addr);
    return Status::OK();
}

Status MnnvlTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    if (!allocate_set_.count(addr)) {
        return Platform::getLoader().free(addr, size);
    }
    CUmemGenericAllocationHandle handle;
    cuMemRetainAllocationHandle(&handle, addr);
    cuMemUnmap((CUdeviceptr)addr, size);
    cuMemAddressFree((CUdeviceptr)addr, size);
    cuMemRelease(handle);
    allocate_set_.erase(addr);
    return Status::OK();
}

static std::pair<pid_t, int> parsePidFd(const std::string &handle_str) {
    std::stringstream ss(handle_str);
    std::string token;
    std::vector<std::string> parts;

    while (std::getline(ss, token, '-')) parts.push_back(token);
    if (parts.size() != 2) {
        throw std::runtime_error("Invalid mnnvl_handle format: " + handle_str);
    }

    pid_t pid = static_cast<pid_t>(std::stoi(parts[0]));
    int fd = std::stoi(parts[1]);
    return {pid, fd};
}

static int importFdFromProcess(pid_t pid, int remote_fd) {
    int pidfd = syscall(SYS_pidfd_open, pid, 0);
    if (pidfd < 0) {
        perror("pidfd_open failed");
        return -1;
    }

    int newfd = syscall(SYS_pidfd_getfd, pidfd, remote_fd, 0);
    if (newfd < 0) {
        perror("pidfd_getfd failed");
        close(pidfd);
        return -1;
    }

    close(pidfd);
    return newfd;
}

Status MnnvlTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                   uint64_t length,
                                                   uint64_t target_id) {
    thread_local HashMap tl_relocate_map;
    if (tl_relocate_map.empty()) {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        tl_relocate_map = relocate_map_;
    }

    for (auto &entry : tl_relocate_map[target_id]) {
        if (entry.first <= dest_addr &&
            dest_addr + length <= entry.first + entry.second.length) {
            auto mnnvl_addr = entry.second.mnnvl_addr;
            dest_addr = dest_addr - entry.first + ((uint64_t)mnnvl_addr);
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDesc *desc = nullptr;
    CHECK_STATUS(metadata_->segmentManager().getRemoteCached(desc, target_id));

    auto buffer = desc->findBuffer(dest_addr, length);
    if (!buffer || buffer->mnnvl_handle.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    if (!relocate_map_[target_id].count(buffer->addr)) {
        CUmemGenericAllocationHandle handle;
        void *mnnvl_addr = nullptr;
        LocationParser location(buffer->location);
        if (location.type() != "cuda") {
            return Status::InvalidArgument(
                "Requested address is not in registered CUDA buffer" LOC_MARK);
        }

        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        cudaSetDevice(location.index());
        if (handle_type_ == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
            auto [pid, remote_fd] = parsePidFd(buffer->mnnvl_handle);
            int local_fd = importFdFromProcess(pid, remote_fd);
            if (local_fd < 0)
                return Status::InternalError(
                    "Unable to import fd from remote process");
            CHECK_CU(cuMemImportFromShareableHandle(&handle, &local_fd,
                                                    handle_type_));
        } else {
            std::vector<unsigned char> output_buffer;
            deserializeBinaryData(buffer->mnnvl_handle, output_buffer);
            if (output_buffer.size() != sizeof(CUmemFabricHandle)) {
                return Status::InternalError(
                    "Received MNNVL handle length incorrect");
            }
            CUmemFabricHandle export_handle;
            memcpy(&export_handle, output_buffer.data(), sizeof(export_handle));
            CHECK_CU(cuMemImportFromShareableHandle(&handle, &export_handle,
                                                    handle_type_));
        }
        CHECK_CU(cuMemAddressReserve((CUdeviceptr *)&mnnvl_addr, buffer->length,
                                     0, 0, 0));
        CHECK_CU(
            cuMemMap((CUdeviceptr)mnnvl_addr, buffer->length, 0, handle, 0));
        int device_count;
        cudaGetDeviceCount(&device_count);
        CUmemAccessDesc accessDesc[device_count];
        for (int device_id = 0; device_id < device_count; ++device_id) {
            accessDesc[device_id].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            accessDesc[device_id].location.id = device_id;
            accessDesc[device_id].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        }
        CHECK_CU(cuMemSetAccess((CUdeviceptr)mnnvl_addr, buffer->length,
                                accessDesc, device_count));
        OpenedMnnvlEntry mnnvl_entry;
        mnnvl_entry.mnnvl_addr = mnnvl_addr;
        mnnvl_entry.length = buffer->length;
        mnnvl_entry.cuda_id = location.index();
        relocate_map_[target_id][buffer->addr] = mnnvl_entry;
        cudaSetDevice(cuda_dev);
    }

    auto mnnvl_addr = relocate_map_[target_id][buffer->addr].mnnvl_addr;
    dest_addr = dest_addr - buffer->addr + ((uint64_t)mnnvl_addr);
    tl_relocate_map = relocate_map_;
    return Status::OK();
}

Status MnnvlTransport::setPeerAccess() {
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
