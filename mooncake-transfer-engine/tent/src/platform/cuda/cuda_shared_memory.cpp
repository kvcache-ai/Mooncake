// Copyright 2026 KVCache.AI
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

#include "tent/platform/cuda.h"

#include <cuda_runtime.h>
#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <charconv>

#include "tent/common/utils/string_builder.h"

// Older glibc kernel headers omit these arch-independent syscall numbers.
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#ifndef SYS_pidfd_getfd
#define SYS_pidfd_getfd 438
#endif

namespace mooncake::tent {
namespace {

Status cudaStatus(CUresult result, const char* operation) {
    if (result == CUDA_SUCCESS) return Status::OK();
    const char* reason = nullptr;
    cuGetErrorString(result, &reason);
    if (result == CUDA_ERROR_NOT_SUPPORTED)
        return Status::NotImplemented(
            std::string(operation) + ": " +
            (reason ? reason : "unsupported by device"));
    return Status::InternalError(std::string(operation) + ": " +
                                 (reason ? reason : "CUDA driver error"));
}

struct UniqueFd {
    int value = -1;
    ~UniqueFd() {
        if (value >= 0) close(value);
    }
};

}  // namespace

Status CudaSharedMemory::setSize(size_t size,
                                 const CUmemAllocationProp& properties) {
    CHECK_STATUS(cudaStatus(
        cuMemGetAllocationGranularity(&granularity_, &properties,
                                      CU_MEM_ALLOC_GRANULARITY_MINIMUM),
        "cuMemGetAllocationGranularity"));
    auto granularity = granularity_;
    if (size > std::numeric_limits<size_t>::max() - (granularity - 1))
        return Status::InvalidArgument(
            "CUDA allocation size overflows" LOC_MARK);
    size_ = size;
    mapped_size_ = (size + granularity - 1) / granularity * granularity;
    return Status::OK();
}

Status CudaSharedMemory::create(const CUmemAllocationProp& properties,
                                size_t size,
                                std::unique_ptr<CudaSharedMemory>* out) {
    if (!out || !size)
        return Status::InvalidArgument(
            "Invalid CUDA allocation arguments" LOC_MARK);
    auto memory = std::unique_ptr<CudaSharedMemory>(new CudaSharedMemory);
    CHECK_STATUS(memory->setSize(size, properties));
    CHECK_STATUS(cudaStatus(
        cuMemCreate(&memory->handle_, memory->mapped_size_, &properties, 0),
        "cuMemCreate"));
    *out = std::move(memory);
    return Status::OK();
}

Status CudaSharedMemory::importHandle(const std::string& handle,
                                      CUmemAllocationHandleType type,
                                      size_t size,
                                      std::unique_ptr<CudaSharedMemory>* out) {
    if (!out || !size)
        return Status::InvalidArgument(
            "Invalid CUDA import arguments" LOC_MARK);
    auto memory = std::unique_ptr<CudaSharedMemory>(new CudaSharedMemory);
    if (type == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
        int pid = 0, fd = -1;
        auto separator = handle.find('-');
        if (separator == std::string::npos)
            return Status::InvalidArgument(
                "Invalid shared-memory fd handle" LOC_MARK);
        auto first =
            std::from_chars(handle.data(), handle.data() + separator, pid);
        auto last = std::from_chars(handle.data() + separator + 1,
                                    handle.data() + handle.size(), fd);
        if (first.ec != std::errc{} || first.ptr != handle.data() + separator ||
            last.ec != std::errc{} ||
            last.ptr != handle.data() + handle.size() || pid <= 0 || fd < 0)
            return Status::InvalidArgument(
                "Invalid shared-memory fd handle" LOC_MARK);
        UniqueFd process{static_cast<int>(syscall(SYS_pidfd_open, pid, 0))};
        if (process.value < 0)
            return Status::InternalError(std::string("pidfd_open: ") +
                                         strerror(errno));
        UniqueFd imported{
            static_cast<int>(syscall(SYS_pidfd_getfd, process.value, fd, 0))};
        if (imported.value < 0)
            return Status::InternalError(std::string("pidfd_getfd: ") +
                                         strerror(errno));
        CHECK_STATUS(cudaStatus(
            cuMemImportFromShareableHandle(
                &memory->handle_, (void*)(uintptr_t)imported.value, type),
            "cuMemImportFromShareableHandle"));
    } else if (type == CU_MEM_HANDLE_TYPE_FABRIC) {
        std::vector<unsigned char> decoded;
        deserializeBinaryData(handle, decoded);
        if (decoded.size() != sizeof(CUmemFabricHandle))
            return Status::InvalidArgument(
                "Invalid CUDA fabric handle" LOC_MARK);
        CUmemFabricHandle fabric;
        memcpy(&fabric, decoded.data(), sizeof(fabric));
        CHECK_STATUS(cudaStatus(
            cuMemImportFromShareableHandle(&memory->handle_, &fabric, type),
            "cuMemImportFromShareableHandle"));
    } else {
        return Status::NotImplemented(
            "Unsupported CUDA shared handle type" LOC_MARK);
    }
    CUmemAllocationProp properties{};
    CHECK_STATUS(cudaStatus(
        cuMemGetAllocationPropertiesFromHandle(&properties, memory->handle_),
        "cuMemGetAllocationPropertiesFromHandle"));
    CHECK_STATUS(memory->setSize(size, properties));
    *out = std::move(memory);
    return Status::OK();
}

Status CudaSharedMemory::exportHandle(CUmemAllocationHandleType type,
                                      std::string* out) {
    if (!out) return Status::InvalidArgument("Handle output is null" LOC_MARK);
    if (type == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
        if (export_fd_ < 0)
            CHECK_STATUS(cudaStatus(
                cuMemExportToShareableHandle(&export_fd_, handle_, type, 0),
                "cuMemExportToShareableHandle"));
        *out = std::to_string(getpid()) + "-" + std::to_string(export_fd_);
    } else if (type == CU_MEM_HANDLE_TYPE_FABRIC) {
        CUmemFabricHandle handle;
        CHECK_STATUS(
            cudaStatus(cuMemExportToShareableHandle(&handle, handle_, type, 0),
                       "cuMemExportToShareableHandle"));
        *out = serializeBinaryData(&handle, sizeof(handle));
    } else
        return Status::NotImplemented(
            "Unsupported CUDA shared handle type" LOC_MARK);
    return Status::OK();
}

Status CudaSharedMemory::map(const std::vector<CUmemAccessDesc>& access,
                             uint64_t requested_address) {
    if (!handle_ || reserved_ || access.empty() ||
        (granularity_ && requested_address % granularity_) ||
        mapped_size_ > UINT64_MAX - requested_address)
        return Status::InvalidArgument(
            "Invalid CUDA mapping arguments" LOC_MARK);
    CHECK_STATUS(
        cudaStatus(cuMemAddressReserve(&address_, mapped_size_, granularity_,
                                       (CUdeviceptr)requested_address, 0),
                   "cuMemAddressReserve"));
    reserved_ = true;
    Status status;
    if (requested_address && address() != requested_address) {
        status = Status::InvalidArgument(
            "CUDA could not reserve the requested global address" LOC_MARK);
    } else {
        status = cudaStatus(cuMemMap(address_, mapped_size_, 0, handle_, 0),
                            "cuMemMap");
        if (status.ok()) {
            mapped_ = true;
            status = setAccess(access);
        }
    }
    if (!status.ok()) {
        auto cleanup = unmap();
        if (!cleanup.ok()) LOG(WARNING) << cleanup.ToString();
    }
    return status;
}

Status CudaSharedMemory::setAccess(const std::vector<CUmemAccessDesc>& access) {
    if (!mapped_ || access.empty())
        return Status::InvalidArgument("CUDA region is not mapped" LOC_MARK);
    return cudaStatus(
        cuMemSetAccess(address_, mapped_size_, access.data(), access.size()),
        "cuMemSetAccess");
}

Status CudaSharedMemory::unmap() {
    if (mapped_) {
        CHECK_STATUS(
            cudaStatus(cuMemUnmap(address_, mapped_size_), "cuMemUnmap"));
        mapped_ = false;
    }
    if (reserved_) {
        CHECK_STATUS(cudaStatus(cuMemAddressFree(address_, mapped_size_),
                                "cuMemAddressFree"));
        reserved_ = false;
        address_ = 0;
    }
    return Status::OK();
}

Status CudaSharedMemory::release() {
    // A failed unmap keeps the mapping state available for retry. Releasing
    // the generic handle and closing the exported fd are independent of it.
    Status first = unmap();
    if (handle_) {
        auto status = cudaStatus(cuMemRelease(handle_), "cuMemRelease");
        if (status.ok())
            handle_ = 0;
        else if (first.ok())
            first = status;
    }
    if (export_fd_ >= 0) {
        close(export_fd_);
        export_fd_ = -1;
    }
    return first;
}

CudaSharedMemory::~CudaSharedMemory() {
    auto status = release();
    if (!status.ok())
        LOG(WARNING) << "CUDA shared-memory cleanup: " << status.ToString();
}

CUmemAccessDesc CudaSharedMemory::deviceAccess(int device) {
    CUmemAccessDesc access{};
    access.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    access.location.id = device;
    access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    return access;
}

Status CudaSharedMemory::allDeviceAccess(std::vector<CUmemAccessDesc>* access) {
    if (!access)
        return Status::InvalidArgument("Access output is null" LOC_MARK);
    int devices = 0;
    CHECK_CUDA(cudaGetDeviceCount(&devices));
    access->clear();
    for (int device = 0; device < devices; ++device)
        access->push_back(deviceAccess(device));
    return Status::OK();
}

}  // namespace mooncake::tent
