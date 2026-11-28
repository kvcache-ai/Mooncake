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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <unistd.h>

#include "shared_segment/host_memfd.h"
#include "shared_segment_internal.h"

#if defined(USE_ASCEND_DIRECT)
#include <acl/acl.h>
#endif

namespace mooncake {
namespace {
constexpr uint16_t kMmapBackendId = 3;

Status HostRegister(void* addr, uint64_t size, int32_t device_id,
                    void** ascend_dev_ptr) {
#if defined(USE_ASCEND_DIRECT)
    // The registration follows the calling thread's device, so the caller has
    // to already be on the one the segment is granted to. Switching it here
    // would leave the caller on a device it never asked for.
    int32_t current_device = -1;
    auto ret = aclrtGetDevice(&current_device);
    if (ret != ACL_ERROR_NONE || current_device != device_id) {
        return Status::InvalidArgument(
            "Shared segment mmap must run on device " +
            std::to_string(device_id) + ", but the current device is " +
            std::to_string(current_device));
    }
    void* dev_ptr = nullptr;
    ret = aclrtHostRegister(addr, size, ACL_HOST_REGISTER_MAPPED, &dev_ptr);
    if (ret != ACL_ERROR_NONE) {
        return Status::Memory(
            "aclrtHostRegister failed for shared segment mmap, ret " +
            std::to_string(ret));
    }
    if (ascend_dev_ptr != nullptr) {
        *ascend_dev_ptr = dev_ptr;
    }
    return Status::OK();
#else
    (void)addr;
    (void)size;
    (void)device_id;
    (void)ascend_dev_ptr;
    return Status::NotImplemented(
        "Shared segment mmap needs Ascend for HostRegister");
#endif
}

void HostUnregister(void* addr, void* /*ascend_dev_ptr*/) {
#if defined(USE_ASCEND_DIRECT)
    if (addr != nullptr) {
        (void)aclrtHostUnregister(addr);
    }
#else
    (void)addr;
#endif
}

class MmapSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~MmapSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(
        const SharedSegmentOptions& /*options*/) const override {
        // Align length to the THP PMD size so MADV_HUGEPAGE can use 2MiB
        // pages. 4KiB fallback still accepts a PMD-aligned length.
        return host_memfd::Granularity();
    }

    Status Prepare(uint64_t total_size, const OwnerChunk* local_chunk,
                   const SharedSegmentOptions& options, uintptr_t& base_addr,
                   std::vector<uint8_t>& handle) override;

    Status CompleteMapping(const std::vector<OwnerChunk>& chunks,
                           const std::vector<std::vector<uint8_t>>& handles,
                           const SharedSegmentOptions& options) override;

    uint16_t BackendId() const override { return kMmapBackendId; }

    uintptr_t DeviceAddr() const override {
        return reinterpret_cast<uintptr_t>(ascend_dev_ptr_);
    }

   private:
    Status RegisterMapped(const SharedSegmentOptions& options);
    void Release();

    void* addr_ = nullptr;
    uint64_t size_ = 0;
    int memfd_ = -1;
    bool registered_ = false;
    void* ascend_dev_ptr_ = nullptr;
};

Status MmapSharedSegmentBackend::RegisterMapped(
    const SharedSegmentOptions& options) {
    if (!options.host_register) {
        return Status::OK();
    }
    auto status =
        HostRegister(addr_, size_, options.device_id, &ascend_dev_ptr_);
    if (!status.ok()) {
        return status;
    }
    registered_ = true;
    return Status::OK();
}

Status MmapSharedSegmentBackend::Prepare(
    uint64_t total_size, const OwnerChunk* local_chunk,
    const SharedSegmentOptions& /*options*/, uintptr_t& base_addr,
    std::vector<uint8_t>& handle) {
    void* reserved =
        host_memfd::ReserveAlignedWindow(total_size, host_memfd::Granularity());
    if (reserved == MAP_FAILED) {
        return Status::Memory("Shared segment mmap reserve failed: " +
                              std::string(std::strerror(errno)));
    }
    addr_ = reserved;
    size_ = total_size;
    base_addr = reinterpret_cast<uintptr_t>(addr_);

    if (local_chunk != nullptr) {
        if (local_chunk->offset > size_ ||
            local_chunk->size > size_ - local_chunk->offset) {
            return Status::InvalidArgument(
                "Shared segment local owner chunk exceeds reserved window");
        }
        int fd = -1;
        std::string error;
        void* chunk_addr = static_cast<uint8_t*>(addr_) + local_chunk->offset;
        if (!host_memfd::CreateAt(local_chunk->size, chunk_addr, fd, error)) {
            return Status::Memory(error);
        }
        memfd_ = fd;
        LOG(INFO) << "Shared segment mmap: unnamed memfd + THP hint "
                     "(no HugeTLB), base 0x"
                  << std::hex << base_addr << " offset 0x"
                  << local_chunk->offset << std::dec << ", chunk "
                  << local_chunk->size << " / total " << total_size
                  << ", shmem_thp="
                  << (host_memfd::ShmemThpEnabled() ? "on" : "off");
        const std::string name = host_memfd::ExportHandle(memfd_);
        handle.assign(name.begin(), name.end());
    }
    return Status::OK();
}

Status MmapSharedSegmentBackend::CompleteMapping(
    const std::vector<OwnerChunk>& chunks,
    const std::vector<std::vector<uint8_t>>& handles,
    const SharedSegmentOptions& options) {
    if (addr_ == nullptr || size_ == 0) {
        return Status::InvalidArgument(
            "Shared segment import needs a reserved address window");
    }
    if (chunks.size() != handles.size()) {
        return Status::InvalidArgument(
            "Shared segment owner chunk and handle count mismatch");
    }
    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        if (chunk.offset > size_ || chunk.size > size_ - chunk.offset) {
            return Status::InvalidArgument(
                "Shared segment owner chunk exceeds reserved window");
        }
        if (chunk.owner_rank == options.rank_id) {
            continue;
        }
        const auto& handle = handles[i];
        if (handle.empty() || handle.size() > kMaxHandleBytes) {
            return Status::InvalidArgument(
                "Shared segment owner handle has an unexpected length");
        }
        const std::string name(handle.begin(), handle.end());
        if (!host_memfd::IsHandle(name)) {
            return Status::InvalidArgument(
                "Shared segment owner handle is not a memfd /proc/<pid>/fd "
                "path");
        }
        void* chunk_addr = static_cast<uint8_t*>(addr_) + chunk.offset;
        std::string error;
        if (!host_memfd::Import(name, chunk.size, chunk_addr, error)) {
            return Status::Memory("Shared segment import failed: " + error);
        }
    }
    return RegisterMapped(options);
}

void MmapSharedSegmentBackend::Release() {
    if (registered_) {
        HostUnregister(addr_, ascend_dev_ptr_);
        registered_ = false;
        ascend_dev_ptr_ = nullptr;
    }
    if (addr_ != nullptr) {
        (void)munmap(addr_, size_);
        addr_ = nullptr;
    }
    if (memfd_ >= 0) {
        close(memfd_);
        memfd_ = -1;
    }
    size_ = 0;
}
}  // namespace

std::unique_ptr<SharedSegmentBackend> CreateMmapSharedSegmentBackend() {
    // Anonymous memfd + mmap is always available. HostRegister is optional and
    // is checked when SharedSegmentOptions::host_register is set.
    return std::make_unique<MmapSharedSegmentBackend>();
}

}  // namespace mooncake
