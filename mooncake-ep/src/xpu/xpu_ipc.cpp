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

#include "xpu_ipc.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <glog/logging.h>
#include <string>

#include "xpu_ep_common.h"

namespace mooncake {
namespace xpu_ep {

namespace {

// The probe word sits past the usable bytes, so no layout region covers it.
constexpr size_t kProbeBytes = 4096;
constexpr int32_t kProbeMagic = 0x4d435850;  // "MCXP"

// Both IPC handle generations keep the exporter's fd in the first 8 bytes.
// Runtimes with "opaque" handles also mirror the fd at offset 22; a mirror
// that differs from the fd marks the handle as user-modified, so the runtime
// imports the fd we supply instead of translating (and caching) it itself.
// Its cache is keyed by (exporter pid, exporter fd) and outlives the mapping,
// so a recycled fd number would silently map a freed buffer. This layout is
// the runtime's, not the Level Zero spec's (checked with compute-runtime
// 26.22); if it changes, the import or the probe read fails and the
// fallback runs.
constexpr size_t kIpcHandleFdOffset = 0;
constexpr size_t kIpcHandleMirrorOffset = 22;
// pid, size_lo, size_hi, namespace_lo, namespace_hi
constexpr int kHeaderWords = 5;
constexpr int kHandleWords = ZE_MAX_IPC_HANDLE_SIZE / sizeof(int32_t);

// Duplicates `fd` of process `pid` into this process. pidfd_getfd needs
// ptrace permission over `pid`; the Level Zero runtime marks its processes
// PR_SET_PTRACER_ANY, which covers same-user siblings under ptrace_scope 1.
int fetchFd(pid_t pid, int fd) {
    if (pid == getpid()) return fcntl(fd, F_DUPFD_CLOEXEC, 0);
#if defined(SYS_pidfd_open) && defined(SYS_pidfd_getfd)
    const int pidfd = static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
    if (pidfd < 0) return -1;
    const int local = static_cast<int>(syscall(SYS_pidfd_getfd, pidfd, fd, 0));
    const int saved = errno;
    close(pidfd);
    errno = saved;
    return local;
#else
    errno = ENOSYS;
    return -1;
#endif
}

int32_t probeValue(int rank) { return kProbeMagic ^ rank; }

// Where an exporter's pid means the same process: the running kernel
// (boot_id) and the pid namespace. A pid from another host or container names
// an unrelated local process, so such peers are not imported (FNV-1a, which
// every process computes alike).
uint64_t pidNamespaceId() {
    std::string boot_id;
    std::ifstream("/proc/sys/kernel/random/boot_id") >> boot_id;
    struct stat ns{};
    stat("/proc/self/ns/pid", &ns);
    boot_id += ":" + std::to_string(ns.st_ino);
    uint64_t h = 0xcbf29ce484222325ull;
    for (unsigned char c : boot_id) h = (h ^ c) * 0x100000001b3ull;
    return h;
}

}  // namespace

XpuIpcBuffer::XpuIpcBuffer(sycl::queue& queue, size_t bytes, int rank,
                           int num_ranks)
    : queue_(queue),
      rank_(rank),
      num_ranks_(num_ranks),
      bytes_(bytes),
      alloc_bytes_(bytes + kProbeBytes),
      imports_(num_ranks, nullptr) {
    XPU_EP_CHECK(queue.get_backend() == sycl::backend::ext_oneapi_level_zero,
                 "the queue must use the Level Zero backend");
    ze_ctx_ = sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
        queue.get_context());
    ze_dev_ = sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
        queue.get_device());

    // zeMemAllocDevice rather than sycl::malloc_device: the latter may
    // sub-allocate from a pool, and an IPC handle then names the whole pool
    // block at an offset the importer cannot recover. Peers write into this
    // memory over PCIe, which must not land on a compressed surface, so ask
    // for it uncompressed rather than relying on the driver-wide
    // RenderCompressedBuffersEnabled=0.
    ze_memory_compression_hints_ext_desc_t uncompressed{
        ZE_STRUCTURE_TYPE_MEMORY_COMPRESSION_HINTS_EXT_DESC, nullptr,
        ZE_MEMORY_COMPRESSION_HINTS_EXT_FLAG_UNCOMPRESSED};
    ze_device_mem_alloc_desc_t desc{ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC,
                                    &uncompressed, 0, 0};
    ze_result_t res =
        zeMemAllocDevice(ze_ctx_, &desc, alloc_bytes_, 4096, ze_dev_, &base_);
    XPU_EP_CHECK(res == ZE_RESULT_SUCCESS && base_,
                 "zeMemAllocDevice failed for the EP buffer");
    // Zero with uncached stores: a memset can leave the whole buffer dirty in
    // this GPU's caches, and a late write-back would erase what peers write.
    using vec4 = sycl::vec<uint32_t, 4>;
    auto* words = static_cast<vec4*>(base_);
    const size_t nvec = alloc_bytes_ / sizeof(vec4);
    auto* probe_word =
        reinterpret_cast<int32_t*>(static_cast<uint8_t*>(base_) + bytes_);
    const int32_t probe = probeValue(rank);
    queue_
        .parallel_for(sycl::range<1>(std::min<size_t>(nvec, 1 << 20)),
                      [=](sycl::id<1> idx) {
                          for (size_t i = idx[0]; i < nvec; i += 1 << 20)
                              uc_store_vec(words + i, vec4(0));
                      })
        .wait();
    queue_.single_task([=] { uc_store(probe_word, probe); }).wait();

    d_peer_bases_ = sycl::malloc_device<void*>(num_ranks, queue_);
    std::vector<void*> bases(num_ranks, nullptr);
    bases[rank] = base_;
    queue_.memcpy(d_peer_bases_, bases.data(), num_ranks * sizeof(void*))
        .wait();
    all_accessible_ = num_ranks == 1;
}

XpuIpcBuffer::~XpuIpcBuffer() {
    try {
        queue_.wait();
    } catch (...) {
    }
    closeImports();
    if (d_peer_bases_) sycl::free(d_peer_bases_, queue_);
    if (exported_) zeMemPutIpcHandle(ze_ctx_, *exported_);
    if (base_) zeMemFree(ze_ctx_, base_);
}

void XpuIpcBuffer::closeImports() {
    for (auto& p : imports_) {
        if (p) zeMemCloseIpcHandle(ze_ctx_, p);
        p = nullptr;
    }
}

std::vector<int32_t> XpuIpcBuffer::exportHandle() {
    // Exported once: peers fetch the handle's descriptor from this process at
    // any time until they connect, so it stays open until destruction.
    if (!exported_) {
        ze_ipc_mem_handle_t handle{};
        ze_result_t res = zeMemGetIpcHandle(ze_ctx_, base_, &handle);
        XPU_EP_CHECK(res == ZE_RESULT_SUCCESS, "zeMemGetIpcHandle failed");
        exported_ = handle;
    }
    const uint64_t ns = pidNamespaceId();
    std::vector<int32_t> out(kHeaderWords + kHandleWords);
    out[0] = static_cast<int32_t>(getpid());
    out[1] = static_cast<int32_t>(alloc_bytes_ & 0xffffffffu);
    out[2] = static_cast<int32_t>(alloc_bytes_ >> 32);
    out[3] = static_cast<int32_t>(ns & 0xffffffffu);
    out[4] = static_cast<int32_t>(ns >> 32);
    std::memcpy(out.data() + kHeaderWords, exported_->data,
                sizeof(exported_->data));
    return out;
}

void XpuIpcBuffer::importPeers(const std::vector<std::vector<int32_t>>& handles,
                               const std::vector<int>& active) {
    XPU_EP_CHECK(static_cast<int>(handles.size()) == num_ranks_,
                 "expected one IPC handle per rank");
    closeImports();
    std::vector<void*> bases(num_ranks_, nullptr);
    bases[rank_] = base_;
    const uint64_t ns = pidNamespaceId();
    for (int r = 0; r < num_ranks_; ++r) {
        if (r == rank_) continue;
        if (!active.empty() && !active[r]) continue;
        const auto& h = handles[r];
        if (static_cast<int>(h.size()) != kHeaderWords + kHandleWords) {
            LOG(WARNING) << "[EP XPU] malformed IPC handle from rank " << r;
            continue;
        }
        const uint64_t peer_ns =
            static_cast<uint32_t>(h[3]) |
            (static_cast<uint64_t>(static_cast<uint32_t>(h[4])) << 32);
        if (peer_ns != ns) {
            LOG(INFO) << "[EP XPU] rank " << r
                      << " is on another host or pid namespace; not mapped";
            continue;
        }
        const pid_t pid = static_cast<pid_t>(h[0]);
        const size_t size =
            static_cast<uint32_t>(h[1]) |
            (static_cast<size_t>(static_cast<uint32_t>(h[2])) << 32);
        // Peers are addressed, and probed, at this rank's offsets.
        if (size != alloc_bytes_) {
            LOG(WARNING) << "[EP XPU] rank " << r << " has a " << size
                         << "-byte EP buffer, this rank " << alloc_bytes_
                         << "; every rank must use the same size";
            continue;
        }
        ze_ipc_mem_handle_t ipc{};
        std::memcpy(ipc.data, h.data() + kHeaderWords, sizeof(ipc.data));
        int64_t exporter_fd = 0;
        std::memcpy(&exporter_fd, ipc.data + kIpcHandleFdOffset,
                    sizeof(exporter_fd));
        const int fd = fetchFd(pid, static_cast<int>(exporter_fd));
        if (fd < 0) {
            LOG(WARNING) << "[EP XPU] pidfd_getfd from rank " << r
                         << " failed: " << strerror(errno);
            continue;
        }
        const uint64_t local_fd = static_cast<uint64_t>(fd);
        const uint64_t mirror = ~uint64_t{0};
        std::memcpy(ipc.data + kIpcHandleFdOffset, &local_fd, sizeof(local_fd));
        std::memcpy(ipc.data + kIpcHandleMirrorOffset, &mirror, sizeof(mirror));
        void* p = nullptr;
        ze_result_t res = zeMemOpenIpcHandle(ze_ctx_, ze_dev_, ipc, 0, &p);
        // The import holds its own reference on the dma-buf.
        close(fd);
        if (res != ZE_RESULT_SUCCESS || !p) {
            LOG(WARNING) << "[EP XPU] zeMemOpenIpcHandle for rank " << r
                         << " failed: 0x" << std::hex << res;
            continue;
        }
        void* range_base = nullptr;
        size_t range_size = 0;
        if (zeMemGetAddressRange(ze_ctx_, p, &range_base, &range_size) !=
                ZE_RESULT_SUCCESS ||
            range_base != p || range_size < size) {
            LOG(WARNING) << "[EP XPU] mapping of rank " << r
                         << " is smaller than its buffer";
            zeMemCloseIpcHandle(ze_ctx_, p);
            continue;
        }
        imports_[r] = p;
        bases[r] = p;
    }

    // A mapping that opens may still be unreachable (no P2P path between the
    // devices), so read each peer's probe word before trusting it.
    int32_t* d_probe = sycl::malloc_device<int32_t>(num_ranks_, queue_);
    void** d_bases = d_peer_bases_;
    queue_.memcpy(d_bases, bases.data(), num_ranks_ * sizeof(void*)).wait();
    const size_t probe_off = bytes_;
    const int n = num_ranks_;
    queue_
        .parallel_for(
            sycl::range<1>(n),
            [=](sycl::id<1> idx) {
                const int r = static_cast<int>(idx[0]);
                const void* b = d_bases[r];
                d_probe[r] =
                    b ? uc_load(reinterpret_cast<const int32_t*>(
                            static_cast<const uint8_t*>(b) + probe_off))
                      : 0;
            })
        .wait();
    std::vector<int32_t> probe(num_ranks_);
    queue_.memcpy(probe.data(), d_probe, num_ranks_ * sizeof(int32_t)).wait();
    sycl::free(d_probe, queue_);

    // Inactive peers are never written to, so they do not count against the
    // fast path.
    all_accessible_ = true;
    for (int r = 0; r < num_ranks_; ++r) {
        if (probe[r] == probeValue(r)) continue;
        if (!active.empty() && !active[r]) continue;
        all_accessible_ = false;
        if (bases[r] && r != rank_) {
            LOG(WARNING) << "[EP XPU] rank " << r
                         << " is mapped but not readable over P2P";
            zeMemCloseIpcHandle(ze_ctx_, imports_[r]);
            imports_[r] = nullptr;
            bases[r] = nullptr;
        }
    }
    queue_.memcpy(d_bases, bases.data(), num_ranks_ * sizeof(void*)).wait();
}

}  // namespace xpu_ep
}  // namespace mooncake
