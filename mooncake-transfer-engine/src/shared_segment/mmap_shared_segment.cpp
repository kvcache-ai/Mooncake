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

#include <glog/logging.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shared_segment_internal.h"

#if defined(USE_ASCEND_DIRECT)
#include <acl/acl.h>
#endif

namespace mooncake {
namespace {
constexpr uint16_t kMmapBackendId = 3;
constexpr uint64_t kFallbackPageSize = 4096;
// Matches the common PMD THP size when sysfs is unavailable.
constexpr uint64_t kFallbackHugePageSize = 2ULL * 1024 * 1024;
constexpr const char* kThpPmdSizePath =
    "/sys/kernel/mm/transparent_hugepage/hpage_pmd_size";
constexpr const char* kShmemThpEnabledPath =
    "/sys/kernel/mm/transparent_hugepage/shmem_enabled";

uint64_t BasePageSize() {
    long page = sysconf(_SC_PAGESIZE);
    if (page <= 0) {
        return kFallbackPageSize;
    }
    return static_cast<uint64_t>(page);
}

// mmap uses shm_open (tmpfs/shmem), so only shmem THP matters. Sysfs looks like
// "always within_size [advise] never deny force"; the active mode is bracketed.
bool ShmemThpEnabled() {
    static const bool kEnabled = []() -> bool {
        FILE* fp = std::fopen(kShmemThpEnabledPath, "r");
        if (fp == nullptr) {
            return false;
        }
        char buf[256];
        const char* line = std::fgets(buf, sizeof(buf), fp);
        std::fclose(fp);
        if (line == nullptr) {
            return false;
        }
        const char* open = std::strchr(buf, '[');
        if (open == nullptr) {
            return false;
        }
        const char* close = std::strchr(open + 1, ']');
        if (close == nullptr || close <= open + 1) {
            return false;
        }
        const std::string mode(open + 1, close);
        return mode != "never" && mode != "deny";
    }();
    return kEnabled;
}

uint64_t HugePageSize() {
    static const uint64_t kSize = []() -> uint64_t {
        FILE* fp = std::fopen(kThpPmdSizePath, "r");
        if (fp != nullptr) {
            unsigned long long value = 0;
            const int matched = std::fscanf(fp, "%llu", &value);
            std::fclose(fp);
            if (matched == 1 && value > 0 && (value & (value - 1)) == 0) {
                return static_cast<uint64_t>(value);
            }
        }
        return kFallbackHugePageSize;
    }();
    return kSize;
}

// Hint only when shmem THP is on; failure must not abort the segment.
void AdviseTransparentHugePages(void* addr, uint64_t size) {
    if (!ShmemThpEnabled()) {
        return;
    }
#ifdef MADV_HUGEPAGE
    if (addr == nullptr || size == 0) {
        return;
    }
    if (madvise(addr, size, MADV_HUGEPAGE) != 0) {
        VLOG(1) << "madvise(MADV_HUGEPAGE) failed for shared segment mmap: "
                << std::strerror(errno);
    }
#else
    (void)addr;
    (void)size;
#endif
}

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

// POSIX shm names are limited to NAME_MAX and must start with a slash. The pid
// separates owners that live on one host at the same time; the random suffix
// covers pid reuse after a crash left a name behind.
std::string MakeShmName() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    char name[48];
    std::snprintf(name, sizeof(name), "/mcss_%d_%016llx",
                  static_cast<int>(getpid()),
                  static_cast<unsigned long long>(rng()));
    return name;
}

class MmapSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~MmapSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(
        const SharedSegmentOptions& /*options*/) const override {
        if (ShmemThpEnabled()) {
            return HugePageSize();
        }
        return BasePageSize();
    }

    Status CreateOwner(uint64_t size, const SharedSegmentOptions& options,
                       uintptr_t& base_addr,
                       std::vector<uint8_t>& handle) override;

    Status ReserveLocal(uint64_t size, const SharedSegmentOptions& options,
                        uintptr_t& base_addr) override;

    Status ImportAndMap(uint64_t size, const SharedSegmentOptions& options,
                        const std::vector<uint8_t>& handle) override;

    uint16_t BackendId() const override { return kMmapBackendId; }

    uintptr_t DeviceAddr() const override {
        return reinterpret_cast<uintptr_t>(ascend_dev_ptr_);
    }

   private:
    Status MapShared(const std::string& shm_name, uint64_t size, bool create,
                     void*& addr);
    Status RegisterMapped(const SharedSegmentOptions& options);
    void Release();

    void* addr_ = nullptr;
    uint64_t size_ = 0;
    bool registered_ = false;
    bool unlink_on_release_ = false;
    std::string shm_name_;
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

Status MmapSharedSegmentBackend::MapShared(const std::string& shm_name,
                                           uint64_t size, bool create,
                                           void*& addr) {
    int flags = create ? (O_CREAT | O_EXCL | O_RDWR) : O_RDWR;
    int fd = shm_open(shm_name.c_str(), flags, 0600);
    if (fd < 0) {
        return Status::Memory(std::string("shm_open failed: ") +
                              std::strerror(errno));
    }
    if (create && ftruncate(fd, static_cast<off_t>(size)) != 0) {
        const int err = errno;
        close(fd);
        shm_unlink(shm_name.c_str());
        return Status::Memory(std::string("ftruncate failed: ") +
                              std::strerror(err));
    }

    int map_flags = MAP_SHARED;
    void* hint = addr;
    if (hint != nullptr) {
        map_flags |= MAP_FIXED;
    }
    void* mapped = mmap(hint, size, PROT_READ | PROT_WRITE, map_flags, fd, 0);
    const int map_err = errno;
    close(fd);
    if (mapped == MAP_FAILED) {
        if (create) {
            shm_unlink(shm_name.c_str());
        }
        return Status::Memory(std::string("mmap failed: ") +
                              std::strerror(map_err));
    }
    AdviseTransparentHugePages(mapped, size);
    addr = mapped;
    return Status::OK();
}

Status MmapSharedSegmentBackend::CreateOwner(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr,
    std::vector<uint8_t>& handle) {
    const std::string name = MakeShmName();
    void* mapped = nullptr;
    auto status = MapShared(name, size, /*create=*/true, mapped);
    if (!status.ok()) {
        return status;
    }
    addr_ = mapped;
    size_ = size;
    unlink_on_release_ = true;
    shm_name_ = name;
    status = RegisterMapped(options);
    if (!status.ok()) {
        return status;
    }
    base_addr = reinterpret_cast<uintptr_t>(addr_);
    handle.assign(name.begin(), name.end());
    return Status::OK();
}

Status MmapSharedSegmentBackend::ReserveLocal(
    uint64_t size, const SharedSegmentOptions& /*options*/,
    uintptr_t& base_addr) {
    void* reserved =
        mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (reserved == MAP_FAILED) {
        return Status::Memory(std::string("mmap reserve failed: ") +
                              std::strerror(errno));
    }
    addr_ = reserved;
    size_ = size;
    base_addr = reinterpret_cast<uintptr_t>(addr_);
    return Status::OK();
}

Status MmapSharedSegmentBackend::ImportAndMap(
    uint64_t size, const SharedSegmentOptions& options,
    const std::vector<uint8_t>& handle) {
    if (addr_ == nullptr || size_ == 0) {
        return Status::InvalidArgument(
            "Shared segment import needs a reserved address window");
    }
    if (size != size_) {
        return Status::InvalidArgument(
            "Shared segment import size does not match the reservation");
    }
    if (handle.empty() || handle.size() > kMaxHandleBytes) {
        return Status::InvalidArgument(
            "Shared segment owner handle has an unexpected length");
    }
    const std::string name(handle.begin(), handle.end());
    if (name.empty() || name[0] != '/') {
        return Status::InvalidArgument(
            "Shared segment owner handle is not a POSIX shm name");
    }

    void* mapped = addr_;
    auto status = MapShared(name, size, /*create=*/false, mapped);
    if (!status.ok()) {
        return status;
    }
    addr_ = mapped;
    shm_name_ = name;
    status = RegisterMapped(options);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
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
    if (unlink_on_release_ && !shm_name_.empty()) {
        (void)shm_unlink(shm_name_.c_str());
        unlink_on_release_ = false;
    }
    shm_name_.clear();
    size_ = 0;
}
}  // namespace

std::unique_ptr<SharedSegmentBackend> CreateMmapSharedSegmentBackend() {
    // POSIX shm + mmap is always available. HostRegister is optional and is
    // checked when SharedSegmentOptions::host_register is set.
    return std::make_unique<MmapSharedSegmentBackend>();
}

}  // namespace mooncake
