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

#include "transport/nvlink_transport/nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
static bool supportFabricMem() {
    if (getenv("MC_USE_NVLINK_IPC")) return false;
    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDeviceCount failed: "
                   << cudaGetErrorString(err);
        return false;
    }
    if (num_devices == 0) {
        LOG(ERROR) << "NvlinkTransport: not device found";
        return false;
    }
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        cuDeviceGetAttribute(&device_support_fabric_mem,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             device_id);
        if (!device_support_fabric_mem) {
            return false;
        }
    }
    return true;
}

NvlinkTransport::NvlinkTransport() : use_fabric_mem_(supportFabricMem()) {}

NvlinkTransport::~NvlinkTransport() {
    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            cudaIpcCloseMemHandle(entry.second.shm_addr);
        }
    }
    remap_entries_.clear();
}

int NvlinkTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status NvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        if (slice->opcode == TransferRequest::READ)
            cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                       slice->length, cudaMemcpyDefault);
        else
            cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                       slice->length, cudaMemcpyDefault);
        slice->markSuccess();
    }

    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status NvlinkTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        if (slice->opcode == TransferRequest::READ)
            cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                       slice->length, cudaMemcpyDefault);
        else
            cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                       slice->length, cudaMemcpyDefault);
        slice->markSuccess();
    }
    return Status::OK();
}

int hexCharToValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    throw std::invalid_argument("Invalid hexadecimal character");
}

std::string serializeBinaryData(const void *data, size_t length) {
    if (!data) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    std::string hexString;
    hexString.reserve(length * 2);

    const unsigned char *byteData = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < length; ++i) {
        hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
        hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
    }

    return hexString;
}

void deserializeBinaryData(const std::string &hexString,
                           std::vector<unsigned char> &buffer) {
    if (hexString.length() % 2 != 0) {
        throw std::invalid_argument("Input string length must be even");
    }

    buffer.clear();
    buffer.reserve(hexString.length() / 2);

    for (size_t i = 0; i < hexString.length(); i += 2) {
        int high = hexCharToValue(hexString[i]);
        int low = hexCharToValue(hexString[i + 1]);
        buffer.push_back(static_cast<unsigned char>((high << 4) | low));
    }
}

int NvlinkTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    if (!use_fabric_mem_) {
        cudaPointerAttributes attr;
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed";
            return -1;
        }

        if (attr.type != cudaMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return -1;
        }

        cudaIpcMemHandle_t handle;
        err = cudaIpcGetMemHandle(&handle, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcGetMemHandle failed";
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        return metadata_->addLocalMemoryBuffer(desc, true);
    } else {
        CUmemGenericAllocationHandle handle;
        auto result = cuMemRetainAllocationHandle(&handle, addr);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemRetainAllocationHandle failed: "
                << result;
            return -1;
        }

        CUmemFabricHandle export_handle;
        result = cuMemExportToShareableHandle(&export_handle, handle,
                                              CU_MEM_HANDLE_TYPE_FABRIC, 0);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemExportToShareableHandle failed: "
                << result;
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int NvlinkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int NvlinkTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            remap_lock_.lockShared();
            if (remap_entries_.count(entry.addr)) {
                auto shm_addr = remap_entries_[entry.addr].shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();
            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(entry.addr)) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR)
                            << "NvlinkTransport: cudaIpcOpenMemHandle failed: "
                            << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = length;
                    remap_entries_[entry.addr] = shm_entry;
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
                    CUmemFabricHandle export_handle;
                    memcpy(&export_handle, output_buffer.data(),
                           sizeof(export_handle));
                    void *shm_addr = nullptr;
                    CUmemGenericAllocationHandle handle;
                    auto result = cuMemImportFromShareableHandle(
                        &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: "
                                      "cuMemImportFromShareableHandle failed: "
                                   << result;
                        return -1;
                    }
                    result = cuMemAddressReserve((CUdeviceptr *)&shm_addr,
                                                 entry.length, 0, 0, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemAddressReserve failed: "
                            << result;
                        return -1;
                    }
                    result = cuMemMap((CUdeviceptr)shm_addr, entry.length, 0,
                                      handle, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return -1;
                    }

                    CUmemAccessDesc accessDesc = {};
                    accessDesc.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                    accessDesc.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
                    accessDesc.location.id = 0;
                    result = cuMemSetAccess((CUdeviceptr)shm_addr, entry.length,
                                            &accessDesc, 1);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: "
                                   << result;
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = length;
                    remap_entries_[entry.addr] = shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched NVLink data transfer method";
                    return -1;
                }
            }
            auto shm_addr = remap_entries_[entry.addr].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    return ERR_INVALID_ARGUMENT;
}

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

void *NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        cudaMalloc(&ptr, size);
        return ptr;
    }
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemAccessDesc accessDesc = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int cudaDev;
    int flag = 0;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }
    CUresult result = cuDeviceGet(&currentDev, cudaDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGet failed: " << result;
        return nullptr;
    }
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.location.id = currentDev;
    result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGetAttribute failed: "
                   << result;
        return nullptr;
    }
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    result = cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAllocationGranularity failed: "
                   << result;
        return nullptr;
    }
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemCreate failed: " << result;
        return nullptr;
    }
    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemAddressReserve failed: " << result;
        cuMemRelease(handle);
        return nullptr;
    }
    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    accessDesc.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    accessDesc.location.id = currentDev;
    accessDesc.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    result = cuMemSetAccess((CUdeviceptr)ptr, size, &accessDesc, 1);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: " << result;
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    return ptr;
}

void NvlinkTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        cudaFree(ptr);
        return;
    }
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemRetainAllocationHandle failed: "
                   << result;
        return;
    }
    result = cuMemGetAddressRange(NULL, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
}
}  // namespace mooncake
