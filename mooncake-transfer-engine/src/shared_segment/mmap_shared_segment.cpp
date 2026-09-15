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
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifdef __has_include
#if __has_include(<linux/memfd.h>)
#include <linux/memfd.h>
#endif
#endif

#include "shared_segment_internal.h"

#if defined(USE_ASCEND_DIRECT)
#include <acl/acl.h>
#endif

#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

namespace mooncake {
namespace {
constexpr uint16_t kMmapBackendId = 3;
// Matches the common PMD THP size when sysfs is unavailable.
constexpr uint64_t kFallbackHugePageSize = 2ULL * 1024 * 1024;
constexpr const char* kThpPmdSizePath =
    "/sys/kernel/mm/transparent_hugepage/hpage_pmd_size";
constexpr const char* kShmemThpEnabledPath =
    "/sys/kernel/mm/transparent_hugepage/shmem_enabled";

// memfd without MFD_HUGETLB is a tmpfs/shmem inode, so shmem THP applies.
// Sysfs looks like "always within_size [advise] never deny force".
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

void AdviseTransparentHugePages(void* addr, uint64_t size) {
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

void AdviseDontFork(void* addr, uint64_t size) {
#ifdef MADV_DONTFORK
    if (addr == nullptr || size == 0) {
        return;
    }
    if (madvise(addr, size, MADV_DONTFORK) != 0) {
        LOG(WARNING) << "madvise(MADV_DONTFORK) failed for shared segment: "
                     << std::strerror(errno);
    }
#else
    (void)addr;
    (void)size;
#endif
}

int CreateAnonymousMemFd() {
#ifdef __NR_memfd_create
    return static_cast<int>(syscall(__NR_memfd_create, "mcss", MFD_CLOEXEC));
#else
    errno = ENOSYS;
    return -1;
#endif
}

// Leave a PROT_NONE window whose address is aligned to the THP PMD size so a
// later MAP_FIXED memfd map can use 2MiB pages. Falls back to an unaligned
// reservation if the oversized mmap fails.
void* ReserveAlignedWindow(uint64_t size, uint64_t align) {
    if (align == 0 || (align & (align - 1)) != 0) {
        align = HugePageSize();
    }
    const uint64_t span = size + align;
    void* raw =
        mmap(nullptr, span, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw != MAP_FAILED) {
        const auto start = reinterpret_cast<uintptr_t>(raw);
        const auto aligned = (start + static_cast<uintptr_t>(align) - 1) &
                             ~(static_cast<uintptr_t>(align) - 1);
        const auto end = aligned + size;
        const auto raw_end = start + span;
        if (aligned > start) {
            (void)munmap(raw, aligned - start);
        }
        if (raw_end > end) {
            (void)munmap(reinterpret_cast<void*>(end), raw_end - end);
        }
        return reinterpret_cast<void*>(aligned);
    }
    return mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
}

// Touch one byte per PMD so THP can back the range with 2MiB pages. If THP
// cannot allocate, the kernel leaves 4KiB pages. Never uses the HugeTLB pool.
void PrefaultAnonymousRange(void* addr, uint64_t size, uint64_t stride) {
    if (addr == nullptr || size == 0 || stride == 0) {
        return;
    }
    auto* bytes = static_cast<volatile uint8_t*>(addr);
    for (uint64_t off = 0; off < size; off += stride) {
        bytes[off] = 0;
    }
    bytes[size - 1] = 0;
}

Status MapMemFd(int fd, uint64_t size, void* hint, void*& addr) {
    int map_flags = MAP_SHARED;
    if (hint != nullptr) {
        map_flags |= MAP_FIXED;
    }
    void* mapped = mmap(hint, size, PROT_READ | PROT_WRITE, map_flags, fd, 0);
    if (mapped == MAP_FAILED) {
        return Status::Memory(std::string("mmap failed: ") +
                              std::strerror(errno));
    }
    addr = mapped;
    return Status::OK();
}

// Unnamed memfd (no MFD_HUGETLB, no nr_hugepages). MADV_HUGEPAGE is applied
// before any page is faulted so the kernel can use THP; otherwise 4KiB pages.
Status CreateAndMapMemFd(uint64_t size, int& fd, void*& addr) {
    fd = CreateAnonymousMemFd();
    if (fd < 0) {
        return Status::Memory(std::string("memfd_create failed: ") +
                              std::strerror(errno));
    }
    if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
        const int err = errno;
        close(fd);
        fd = -1;
        return Status::Memory(std::string("ftruncate failed: ") +
                              std::strerror(err));
    }
    void* hint = ReserveAlignedWindow(size, HugePageSize());
    if (hint == MAP_FAILED) {
        hint = nullptr;
    }
    auto status = MapMemFd(fd, size, hint, addr);
    if (!status.ok() && hint != nullptr) {
        (void)munmap(hint, size);
        hint = nullptr;
        status = MapMemFd(fd, size, nullptr, addr);
    }
    if (!status.ok()) {
        close(fd);
        fd = -1;
        return status;
    }
    AdviseTransparentHugePages(addr, size);
    PrefaultAnonymousRange(addr, size, HugePageSize());
    AdviseDontFork(addr, size);
    return Status::OK();
}

std::string EncodeMemFdHandle(int fd) {
    char name[64];
    std::snprintf(name, sizeof(name), "/proc/%d/fd/%d",
                  static_cast<int>(getpid()), fd);
    return name;
}

bool IsMemFdHandle(const std::string& name) {
    return name.rfind("/proc/", 0) == 0 &&
           name.find("/fd/") != std::string::npos;
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

class MmapSharedSegmentBackend : public SharedSegmentBackend {
   public:
    ~MmapSharedSegmentBackend() override { Release(); }

    uint64_t Granularity(
        const SharedSegmentOptions& /*options*/) const override {
        // Align length to the THP PMD size so MADV_HUGEPAGE can use 2MiB
        // pages. 4KiB fallback still accepts a PMD-aligned length.
        return HugePageSize();
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

Status MmapSharedSegmentBackend::CreateOwner(
    uint64_t size, const SharedSegmentOptions& options, uintptr_t& base_addr,
    std::vector<uint8_t>& handle) {
    int fd = -1;
    void* mapped = nullptr;
    auto status = CreateAndMapMemFd(size, fd, mapped);
    if (!status.ok()) {
        return status;
    }
    LOG(INFO) << "Shared segment mmap: unnamed memfd + THP hint (no HugeTLB), "
              << "size " << size
              << ", shmem_thp=" << (ShmemThpEnabled() ? "on" : "off");

    addr_ = mapped;
    size_ = size;
    memfd_ = fd;
    status = RegisterMapped(options);
    if (!status.ok()) {
        return status;
    }
    base_addr = reinterpret_cast<uintptr_t>(addr_);
    const std::string name = EncodeMemFdHandle(memfd_);
    handle.assign(name.begin(), name.end());
    return Status::OK();
}

Status MmapSharedSegmentBackend::ReserveLocal(
    uint64_t size, const SharedSegmentOptions& /*options*/,
    uintptr_t& base_addr) {
    void* reserved = ReserveAlignedWindow(size, HugePageSize());
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
    if (!IsMemFdHandle(name)) {
        return Status::InvalidArgument(
            "Shared segment owner handle is not a memfd /proc/<pid>/fd path");
    }

    int fd = open(name.c_str(), O_RDWR);
    if (fd < 0) {
        return Status::Memory(std::string("open memfd handle failed: ") +
                              std::strerror(errno) + " (" + name + ")");
    }

    void* mapped = addr_;
    auto status = MapMemFd(fd, size, mapped, mapped);
    close(fd);
    if (!status.ok()) {
        return status;
    }
    addr_ = mapped;
    AdviseTransparentHugePages(addr_, size);
    AdviseDontFork(addr_, size);
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
