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

#include "v1/transport/mnnvl/mnnvl_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#endif

#include "v1/common.h"
#include "v1/metadata/metadata.h"

namespace mooncake {
namespace v1 {

static Status getCurrentCudaDevice(CUdevice &currentDev) {
    int cudaDev;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess)
        return Status::InternalError("Failed to get cuda device context");
    CUresult result = cuDeviceGet(&currentDev, cudaDev);
    if (result != CUDA_SUCCESS)
        return Status::InternalError("Failed to get cuda device index");
    return Status::OK();
}

static Status buildCUmemAllocationProp(CUmemAllocationProp &prop) {
    CUdevice currentDev;
    CHECK_STATUS(getCurrentCudaDevice(currentDev));
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.location.id = currentDev;
    int flag = 0;
    auto result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS)
        return Status::InternalError("Failed to get cuda device attribute");
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    return Status::OK();
}

static Status roundGranularity(CUmemAllocationProp &prop, size_t &size) {
    size_t granularity = 0;
    auto result = cuMemGetAllocationGranularity(
        &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS)
        return Status::InternalError("Failed to get cuda alloc granularity");
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    return Status::OK();
}

class MnnvlThreadPool {
   public:
    MnnvlThreadPool(size_t threadCount)
        : ioService_(),
          work_(asio::make_work_guard(ioService_)),
          stopped_(false) {
        for (size_t i = 0; i < threadCount; ++i) {
            threads_.create_thread(
                boost::bind(&asio::io_service::run, &ioService_));
        }
    }

    ~MnnvlThreadPool() { stop(); }

    void submit(std::function<void()> task) {
        ioService_.post(std::move(task));
    }

    void stop() {
        if (!stopped_) {
            stopped_ = true;
            ioService_.stop();
            threads_.join_all();
        }
    }

   private:
    asio::io_service ioService_;
    asio::executor_work_guard<asio::io_service::executor_type> work_;
    boost::thread_group threads_;
    bool stopped_;
};

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
                               std::shared_ptr<MetadataService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<ConfigManager> conf) {
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

    const static size_t kDefaultThreadPoolSize = 1;
    workers_ = std::make_unique<MnnvlThreadPool>(kDefaultThreadPoolSize);

    installed_ = true;
    return Status::OK();
}

Status MnnvlTransport::uninstall() {
    if (installed_) {
        workers_->stop();
        workers_.reset();
        metadata_.reset();
        for (auto &entry : relocate_map_) {
            // TBD
        }
        relocate_map_.clear();
        installed_ = false;
    }
    return Status::OK();
}

Status MnnvlTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mnnvl_batch = new MnnvlSubBatch();
    batch = mnnvl_batch;
    mnnvl_batch->task_list.reserve(max_size);
    mnnvl_batch->max_size = max_size;
    return Status::OK();
}

Status MnnvlTransport::freeSubBatch(SubBatchRef &batch) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);
    delete mnnvl_batch;
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
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task);
    }
    return Status::OK();
}

void MnnvlTransport::startTransfer(MnnvlTask *task) {
    workers_->submit([task]() {
        cudaError_t err;
        if (task->request.opcode == Request::READ)
            err = cudaMemcpy(task->request.source, (void *)task->target_addr,
                             task->request.length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)task->target_addr, task->request.source,
                             task->request.length, cudaMemcpyDefault);
        if (err == cudaSuccess) {
            task->transferred_bytes = task->request.length;
            task->status_word = TransferStatusEnum::COMPLETED;
        } else
            task->status_word = TransferStatusEnum::FAILED;
    });
}

Status MnnvlTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus &status) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)mnnvl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mnnvl_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

void MnnvlTransport::queryOutstandingTasks(SubBatchRef batch,
                                           std::vector<int> &task_id_list) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch) return;
    for (int task_id = 0; task_id < (int)mnnvl_batch->task_list.size();
         ++task_id) {
        auto &task = mnnvl_batch->task_list[task_id];
        if (task.status_word != TransferStatusEnum::COMPLETED &&
            task.status_word != TransferStatusEnum::FAILED) {
            task_id_list.push_back(task_id);
        }
    }
}

Status MnnvlTransport::addMemoryBuffer(BufferDesc &desc,
                                       const MemoryOptions &options) {
    auto location = options.location;
    if (location == kWildcardLocation) {
        auto entries = getMemoryLocation((void *)desc.addr, desc.length);
        if (!entries.empty()) location = entries[0].location;
    }
    desc.location = location;
    if (!location.starts_with("cuda")) return Status::OK();

    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, desc.addr);
    if (result != CUDA_SUCCESS) {
        LOG(WARNING) << "Memory region " << desc.addr
                     << " not allocated by cuMemCreate, "
                     << "but it can be used as local buffer";
        return Status::OK();
    }

    CUmemAllocationProp prop = {};
    CHECK_STATUS(buildCUmemAllocationProp(prop));
    CHECK_STATUS(roundGranularity(prop, desc.length));

    CUmemFabricHandle export_handle;
    result = cuMemExportToShareableHandle(&export_handle, handle,
                                          CU_MEM_HANDLE_TYPE_FABRIC, 0);
    if (result != CUDA_SUCCESS)
        return Status::InternalError("Failed to export shareable handle");

    desc.mnnvl_handle =
        serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
    return Status::OK();
}

Status MnnvlTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.mnnvl_handle.clear();
    // desc.mnnvl_offset = 0;
    return Status::OK();
}

Status MnnvlTransport::allocateLocalMemory(BufferEntry &buffer, size_t size,
                                           const Location &location) {
    CUmemAllocationProp prop = {};
    CHECK_STATUS(buildCUmemAllocationProp(prop));
    CHECK_STATUS(roundGranularity(prop, size));

    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        return Status::InternalError("Failed to create cuda memory");
    }

    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        cuMemRelease(handle);
        return Status::InternalError("Failed to reserve cuda address space");
    }

    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return Status::InternalError("Failed to map cuda memory address space");
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
        return Status::InternalError("Failed to set cuda memory access");
    }

    buffer.addr = (void *)ptr;
    buffer.length = size;
    buffer.location = location;
    return Status::OK();
}

Status MnnvlTransport::freeLocalMemory(const BufferEntry &buffer) {
    CUmemGenericAllocationHandle handle;
    cuMemRetainAllocationHandle(&handle, buffer.addr);
    cuMemUnmap((CUdeviceptr)buffer.addr, buffer.size);
    cuMemAddressFree((CUdeviceptr)buffer.addr, buffer.size);
    cuMemRelease(handle);
    return Status::OK();
}

Status MnnvlTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                   uint64_t length,
                                                   uint64_t target_id) {
    SegmentDescRef desc;
    auto status = metadata_->segmentManager().getRemote(desc, target_id);
    if (!status.ok()) return status;
    int index = 0;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    auto &relocate_map = relocate_map_[target_id];
    for (auto &entry : detail.buffers) {
        if (!entry.mnnvl_handle.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            std::lock_guard<std::mutex> lock(relocate_mutex_);
            if (!relocate_map.count(entry.addr)) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                if (output_buffer.size() != sizeof(CUmemFabricHandle)) {
                    return Status::InternalError(
                        "Received MNNVL handle length incorrect");
                }
                CUmemFabricHandle export_handle;
                memcpy(&export_handle, output_buffer.data(),
                       sizeof(export_handle));
                void *mnnvl_addr = nullptr;
                CUmemGenericAllocationHandle handle;
                auto result = cuMemImportFromShareableHandle(
                    &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                if (result != CUDA_SUCCESS) {
                    return Status::InternalError(
                        "Failed to import from shareable handle");
                }
                result = cuMemAddressReserve((CUdeviceptr *)&mnnvl_addr,
                                             entry.length, 0, 0, 0);
                if (result != CUDA_SUCCESS) {
                    return Status::InternalError(
                        "Failed to reserve memory address space");
                }
                result = cuMemMap((CUdeviceptr)mnnvl_addr, entry.length, 0,
                                  handle, 0);
                if (result != CUDA_SUCCESS) {
                    return Status::InternalError(
                        "Failed to map memory address space");
                }

                int device_count;
                cudaGetDeviceCount(&device_count);
                CUmemAccessDesc accessDesc[device_count];
                for (int device_id = 0; device_id < device_count; ++device_id) {
                    accessDesc[device_id].location.type =
                        CU_MEM_LOCATION_TYPE_DEVICE;
                    accessDesc[device_id].location.id = device_id;
                    accessDesc[device_id].flags =
                        CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                }
                result = cuMemSetAccess((CUdeviceptr)mnnvl_addr, entry.length,
                                        accessDesc, device_count);
                if (result != CUDA_SUCCESS) {
                    return Status::InternalError("Failed to set memory access");
                }

                OpenedMnnvlEntry mnnvl_entry;
                mnnvl_entry.mnnvl_addr = mnnvl_addr;
                mnnvl_entry.length = length;
                relocate_map[entry.addr] = mnnvl_entry;
            }
            auto mnnvl_addr = relocate_map[entry.addr].mnnvl_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)mnnvl_addr);
            return Status::OK();
        }
        index++;
    }
    return Status::InvalidArgument(
        "Requested address is not in registered buffer" LOC_MARK);
}

bool MnnvlTransport::precheck(const Request &request) {
    SegmentDescRef desc;
    auto status =
        metadata_->segmentManager().getRemote(desc, request.target_id);
    if (!status.ok()) return false;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.mnnvl_handle.empty() &&
            entry.addr <= request.target_offset &&
            request.target_offset + request.length <=
                entry.addr + entry.length) {
            return true;
        }
    }
    return false;
}
}  // namespace v1
}  // namespace mooncake
