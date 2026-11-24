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

#ifndef HOST_MEMFD_H
#define HOST_MEMFD_H

#include <glog/logging.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <cstdlib>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifdef __has_include
#if __has_include(<linux/memfd.h>)
#include <linux/memfd.h>
#endif
#endif

#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

// THP-backed host mappings, in two flavours:
//
//   Create()        unnamed memfd mapped MAP_SHARED. Another process that
//                   imports the handle maps the same physical pages.
//   CreatePrivate() plain MAP_PRIVATE|MAP_ANONYMOUS. Same page and alignment
//                   behaviour, but the pages cannot be shared.
//
// Both advise transparent huge pages before the first fault and never touch the
// HugeTLB pool (nr_hugepages), so neither needs operator configuration.
//
// Header-only on purpose: ascend_transport is built as its own shared library
// and links transfer_engine the other way round, so a .cpp here would close a
// dependency cycle between the two.
//
// Importers do not get the owner's virtual address. Every process maps at its
// own base and locates data as `local_base + offset`.

namespace mooncake {
namespace host_memfd {

// Matches the common PMD THP size when sysfs is unavailable.
inline constexpr uint64_t kFallbackHugePageSize = 2ULL * 1024 * 1024;
// Prefault stride. Always 2MiB, which is the PMD on a 4KiB kernel and the
// mTHP size on a 64KiB kernel. Alignment stays on hpage_pmd_size.
inline constexpr uint64_t kPrefaultStride = 2ULL * 1024 * 1024;
inline constexpr const char* kThpPmdSizePath =
    "/sys/kernel/mm/transparent_hugepage/hpage_pmd_size";
inline constexpr const char* kShmemThpEnabledPath =
    "/sys/kernel/mm/transparent_hugepage/shmem_enabled";

// Allocation length must be a multiple of this, and the mapped base is aligned
// to it, so every PMD in the range can be backed by a transparent huge page.
inline uint64_t Granularity() {
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

// memfd without MFD_HUGETLB is a tmpfs/shmem inode, so shmem THP applies.
// Sysfs looks like "always within_size [advise] never deny force".
inline bool ShmemThpEnabled() {
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
        const char* open_bracket = std::strchr(buf, '[');
        if (open_bracket == nullptr) {
            return false;
        }
        const char* close_bracket = std::strchr(open_bracket + 1, ']');
        if (close_bracket == nullptr || close_bracket <= open_bracket + 1) {
            return false;
        }
        const std::string mode(open_bracket + 1, close_bracket);
        return mode != "never" && mode != "deny";
    }();
    return kEnabled;
}

inline void AdviseTransparentHugePages(void* addr, uint64_t size) {
#ifdef MADV_HUGEPAGE
    if (addr == nullptr || size == 0) {
        return;
    }
    if (madvise(addr, size, MADV_HUGEPAGE) != 0) {
        VLOG(1) << "madvise(MADV_HUGEPAGE) failed for host memfd mapping: "
                << std::strerror(errno);
    }
#else
    (void)addr;
    (void)size;
#endif
}

inline void AdviseDontFork(void* addr, uint64_t size) {
#ifdef MADV_DONTFORK
    if (addr == nullptr || size == 0) {
        return;
    }
    if (madvise(addr, size, MADV_DONTFORK) != 0) {
        LOG(WARNING) << "madvise(MADV_DONTFORK) failed for host memfd mapping: "
                     << std::strerror(errno);
    }
#else
    (void)addr;
    (void)size;
#endif
}

inline int CreateAnonymousMemFd() {
#ifdef __NR_memfd_create
    return static_cast<int>(syscall(__NR_memfd_create, "mcss", MFD_CLOEXEC));
#else
    errno = ENOSYS;
    return -1;
#endif
}

// Leave a PROT_NONE window whose address is aligned to the THP PMD size so a
// later MAP_FIXED memfd map can use 2MiB pages. Falls back to an unaligned
// reservation if the oversized mmap fails. Returns MAP_FAILED on error.
inline void* ReserveAlignedWindow(uint64_t size, uint64_t align) {
    if (align == 0 || (align & (align - 1)) != 0) {
        align = Granularity();
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

// Touch one byte per stride, then the last byte. Callers pass kPrefaultStride
// (2MiB): one PMD per touch on a 4KiB kernel. On a 64KiB kernel the first
// touch in a 512MiB window faults that PMD; if it is not available, each
// touch still faults a 2MiB mTHP. Base pages remain where neither huge page
// is available. Never uses the HugeTLB pool.
inline void PrefaultRange(void* addr, uint64_t size, uint64_t stride) {
    if (addr == nullptr || size == 0 || stride == 0) {
        return;
    }
    auto* bytes = static_cast<volatile uint8_t*>(addr);
    for (uint64_t off = 0; off < size; off += stride) {
        bytes[off] = 0;
    }
    bytes[size - 1] = 0;
}

// Maps `fd` at offset 0. A non-null `hint` is replaced with MAP_FIXED, which
// only overwrites a reservation made by ReserveAlignedWindow. On failure
// `error` describes the failing step.
inline bool Map(int fd, uint64_t size, void* hint, void*& addr,
                std::string& error) {
    int map_flags = MAP_SHARED;
    if (hint != nullptr) {
        map_flags |= MAP_FIXED;
    }
    void* mapped = mmap(hint, size, PROT_READ | PROT_WRITE, map_flags, fd, 0);
    if (mapped == MAP_FAILED) {
        error = std::string("mmap failed: ") + std::strerror(errno);
        return false;
    }
    addr = mapped;
    return true;
}

// Owner side: create the memfd, map it at an aligned base, and prefault it.
// `size` should already be a multiple of Granularity().
inline bool Create(uint64_t size, int& fd, void*& addr, std::string& error) {
    fd = CreateAnonymousMemFd();
    if (fd < 0) {
        error = std::string("memfd_create failed: ") + std::strerror(errno);
        return false;
    }
    if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
        error = std::string("ftruncate failed: ") + std::strerror(errno);
        close(fd);
        fd = -1;
        return false;
    }
    void* hint = ReserveAlignedWindow(size, Granularity());
    if (hint == MAP_FAILED) {
        hint = nullptr;
    }
    bool mapped = Map(fd, size, hint, addr, error);
    if (!mapped && hint != nullptr) {
        (void)munmap(hint, size);
        mapped = Map(fd, size, nullptr, addr, error);
    }
    if (!mapped) {
        close(fd);
        fd = -1;
        return false;
    }
    AdviseTransparentHugePages(addr, size);
    PrefaultRange(addr, size, kPrefaultStride);
    AdviseDontFork(addr, size);
    return true;
}

// Same pages and alignment as Create(), without the memfd: the mapping is
// private and anonymous, so nothing can be exported to another process. Used
// where the platform only needs mmap-backed host memory rather than sharing.
inline bool CreatePrivate(uint64_t size, void*& addr, std::string& error) {
    void* mapped = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) {
        error = std::string("mmap failed: ") + std::strerror(errno);
        return false;
    }
    const uint64_t granularity = Granularity();
    if ((reinterpret_cast<uintptr_t>(mapped) & (granularity - 1)) != 0) {
        // An unaligned base leaves every PMD straddling the range, so THP can
        // never back it. Retry through an aligned reservation.
        (void)munmap(mapped, size);
        void* window = ReserveAlignedWindow(size, granularity);
        if (window == MAP_FAILED) {
            error = std::string("mmap reserve failed: ") + std::strerror(errno);
            return false;
        }
        mapped = mmap(window, size, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
        if (mapped == MAP_FAILED) {
            error = std::string("mmap fixed failed: ") + std::strerror(errno);
            (void)munmap(window, size);
            return false;
        }
    }
    AdviseTransparentHugePages(mapped, size);
    PrefaultRange(mapped, size, kPrefaultStride);
    AdviseDontFork(mapped, size);
    addr = mapped;
    return true;
}

// Per-boot UUID. Two machines never share one, so it is what tells an importer
// that a "/proc/<pid>/fd/<n>" path refers to a process here rather than a
// same-numbered process somewhere else in the cluster. Empty if unreadable.
inline const std::string& LocalBootId() {
    static const std::string kBootId = []() -> std::string {
        FILE* fp = std::fopen("/proc/sys/kernel/random/boot_id", "r");
        if (fp == nullptr) {
            LOG(WARNING) << "Cannot read boot_id; host memfd handles will not "
                            "be machine-qualified and cannot be imported";
            return {};
        }
        char buf[64] = {};
        const char* line = std::fgets(buf, sizeof(buf), fp);
        std::fclose(fp);
        if (line == nullptr) {
            return {};
        }
        std::string id(buf);
        while (!id.empty() && (id.back() == '\n' || id.back() == '\r')) {
            id.pop_back();
        }
        return id;
    }();
    return kBootId;
}

// The handle an importer opens, as "/proc/<pid>/fd/<n>:<inode>:<boot_id>".
// Valid only while the owner process lives, only on the machine that exported
// it, and only from inside the owner's PID namespace, for a process allowed to
// read /proc/<pid>/fd.
//
// Both suffixes exist to stop a path from resolving to the wrong memory:
//   inode    an fd number is reusable the moment the owner closes it, so the
//            path alone can start pointing at an unrelated memfd.
//   boot_id  inode numbers are small per-tmpfs integers, so a remote peer's
//            handle can collide with a live local one. Without this a transfer
//            to another machine could memcpy into local memory and report
//            success while the real target received nothing.
inline std::string ExportHandle(int fd) {
    struct stat st = {};
    unsigned long long ino = 0;
    if (fstat(fd, &st) == 0) {
        ino = static_cast<unsigned long long>(st.st_ino);
    } else {
        LOG(WARNING) << "fstat on host memfd failed, exporting a handle "
                        "without an inode guard: "
                     << std::strerror(errno);
    }
    char name[128];
    std::snprintf(name, sizeof(name), "/proc/%d/fd/%d:%llu:%s",
                  static_cast<int>(getpid()), fd, ino, LocalBootId().c_str());
    return name;
}

inline bool IsHandle(const std::string& name) {
    return name.rfind("/proc/", 0) == 0 &&
           name.find("/fd/") != std::string::npos;
}

struct ParsedHandle {
    std::string path;  // "/proc/<pid>/fd/<n>"
    int owner_pid = -1;
    unsigned long long inode = 0;  // 0 when absent
    std::string boot_id;           // empty when absent
};

// Splits a handle into its parts without touching the filesystem. Returns
// false for anything that is not a fully qualified handle from this machine,
// which is the only kind that can be trusted to name the owner's memfd.
inline bool ParseLocalHandle(const std::string& handle, ParsedHandle& parsed) {
    if (!IsHandle(handle)) {
        return false;
    }
    const size_t ino_at = handle.find(':', handle.rfind('/'));
    if (ino_at == std::string::npos) {
        return false;
    }
    const size_t boot_at = handle.find(':', ino_at + 1);
    if (boot_at == std::string::npos) {
        return false;
    }
    parsed.path = handle.substr(0, ino_at);
    parsed.inode = std::strtoull(handle.c_str() + ino_at + 1, nullptr, 10);
    parsed.boot_id = handle.substr(boot_at + 1);
    if (parsed.inode == 0 || parsed.boot_id.empty() ||
        parsed.boot_id != LocalBootId()) {
        return false;
    }
    char* end = nullptr;
    const long pid =
        std::strtol(handle.c_str() + std::strlen("/proc/"), &end, 10);
    if (end == nullptr || *end != '/' || pid <= 0) {
        return false;
    }
    parsed.owner_pid = static_cast<int>(pid);
    return true;
}

// Importer side: open `handle` and map it over `window`, a reservation of
// exactly `size` bytes obtained from ReserveAlignedWindow(). Fails, without
// touching `window`, whenever the handle cannot be proven to name the memfd
// the owner exported.
inline bool Import(const std::string& handle, uint64_t size, void* window,
                   std::string& error) {
    ParsedHandle parsed;
    if (!ParseLocalHandle(handle, parsed)) {
        error =
            "memfd handle is malformed or was exported on another machine "
            "(" +
            handle + ")";
        return false;
    }
    const unsigned long long expected_ino = parsed.inode;
    const std::string& path = parsed.path;
    const int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        error = std::string("open memfd handle failed: ") +
                std::strerror(errno) + " (" + handle + ")";
        return false;
    }
    struct stat st = {};
    if (fstat(fd, &st) != 0) {
        error = std::string("fstat on imported memfd failed: ") +
                std::strerror(errno);
        close(fd);
        return false;
    }
    if (static_cast<unsigned long long>(st.st_ino) != expected_ino) {
        // The owner closed that fd and the number was handed to something
        // else. Mapping it would hand back unrelated memory.
        error = "memfd handle is stale, the fd now refers to inode " +
                std::to_string(st.st_ino) + " instead of " +
                std::to_string(expected_ino);
        close(fd);
        return false;
    }
    void* mapped = window;
    const bool ok = Map(fd, size, window, mapped, error);
    close(fd);
    if (!ok) {
        return false;
    }
    AdviseTransparentHugePages(mapped, size);
    AdviseDontFork(mapped, size);
    return true;
}

}  // namespace host_memfd
}  // namespace mooncake

#endif  // HOST_MEMFD_H
