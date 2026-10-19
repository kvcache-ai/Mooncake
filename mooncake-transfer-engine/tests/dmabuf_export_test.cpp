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

// Hardware-free unit tests for DmabufExport and
// RdmaContext::{exportDmabuf, closeDmabufExport}.
//
// No RDMA device or GPU is required.  The tests cover:
//   - DmabufExport struct defaults
//   - closeDmabufExport: closes a real fd, is idempotent, no-ops when fd == -1
//   - exportDmabuf: host memory always yields kHostReg / fd == -1 on every
//     build (CUDA, HIP, or non-GPU), so those cases run in CI without hardware.

#include "transport/rdma_transport/rdma_context.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <vector>

#if defined(USE_CUDA)
#include <cuda_runtime.h>
#endif

using mooncake::DmabufExport;
using mooncake::RdmaContext;

namespace {

// Open a real fd via pipe() (POSIX, no _GNU_SOURCE required).
// Returns via reference so ASSERT_EQ can abort the test on failure, avoiding
// undefined behaviour from an uninitialized pipefd if pipe() fails.
static void make_test_fd(int &out_fd) {
    int pipefd[2];
    ASSERT_EQ(pipe(pipefd), 0) << "pipe() failed: " << strerror(errno);
    close(pipefd[1]);  // write end not needed
    out_fd = pipefd[0];
}

static bool fd_is_closed(int fd) {
    return fcntl(fd, F_GETFD) == -1 && errno == EBADF;
}

// ── DmabufExport struct defaults ─────────────────────────────────────────────

TEST(DmabufExport, DefaultIsHostRegWithNoFd) {
    DmabufExport exp;
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
    EXPECT_EQ(exp.offset, 0u);
}

// ── closeDmabufExport ────────────────────────────────────────────────────────

TEST(CloseDmabufExport, NoOpWhenFdIsNegative) {
    DmabufExport exp;  // fd == -1 by default
    RdmaContext::closeDmabufExport(exp);
    EXPECT_EQ(exp.fd, -1);  // still -1, no crash
}

TEST(CloseDmabufExport, ClosesLiveFdAndClearsIt) {
    int fd = -1;
    make_test_fd(fd);
    ASSERT_GE(fd, 0);

    DmabufExport exp;
    exp.method = DmabufExport::Method::kDmabufReg;
    exp.fd = fd;

    RdmaContext::closeDmabufExport(exp);

    EXPECT_EQ(exp.fd, -1);
    EXPECT_TRUE(fd_is_closed(fd)) << "fd " << fd << " should be closed";
}

TEST(CloseDmabufExport, Idempotent) {
    int fd = -1;
    make_test_fd(fd);
    ASSERT_GE(fd, 0);

    DmabufExport exp;
    exp.method = DmabufExport::Method::kDmabufReg;
    exp.fd = fd;

    RdmaContext::closeDmabufExport(exp);  // first close
    RdmaContext::closeDmabufExport(
        exp);  // second call: fd == -1, must not crash
    EXPECT_EQ(exp.fd, -1);
}

// ── exportDmabuf on host memory ──────────────────────────────────────────────
//
// malloc'd memory is host memory on every supported build:
//   - Non-GPU build: the #else branch returns kHostReg immediately.
//   - CUDA build:    cuPointerGetAttribute fails for host addrs → kHostReg.
//   - HIP build:     hipPointerGetAttributes fails / returns hipMemoryTypeHost.
//
// So these tests exercise the "not GPU memory" fast-path on all CI runners.

TEST(ExportDmabuf, HostMemoryYieldsHostReg) {
    std::vector<char> buf(4096);
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(buf.data(), buf.size(), exp);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
    // Closing a kHostReg export is always safe.
    RdmaContext::closeDmabufExport(exp);
}

TEST(ExportDmabuf, LargeHostBufferYieldsHostReg) {
    constexpr size_t kSize = 8ULL * 1024 * 1024;  // 8 MiB
    std::vector<char> buf(kSize);
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(buf.data(), kSize, exp);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
}

TEST(ExportDmabuf, MmapAnonymousYieldsHostReg) {
    void *p = mmap(nullptr, 4096, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    ASSERT_NE(p, MAP_FAILED);

    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(p, 4096, exp);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);

    munmap(p, 4096);
}

TEST(ExportDmabuf, StackAddressYieldsHostReg) {
    char stack_buf[128];
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(stack_buf, sizeof(stack_buf), exp);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
}

#if defined(USE_CUDA)
// Needs a CUDA device and DMA-BUF support; skipped otherwise. Registering a
// sub-range of a larger cudaMalloc allocation must export only that range:
// exporting the whole allocation makes every import map all of it through
// BAR1, and N concurrent imports of an N-way split (preTouchMemory) run out
// of BAR1 (ibv_reg_dmabuf_mr ENOMEM), e.g. on MIG instances.
TEST(ExportDmabuf, GpuSubRangeExportsOnlyTheRequestedRange) {
    int devices = 0;
    if (cudaGetDeviceCount(&devices) != cudaSuccess || devices == 0)
        GTEST_SKIP() << "no CUDA device";
    constexpr size_t kAlloc = 64ULL << 20;  // one 64 MiB allocation
    constexpr size_t kOffset = 16ULL << 20;
    constexpr size_t kLength = 16ULL << 20;  // register [16 MiB, 32 MiB)
    void *base = nullptr;
    ASSERT_EQ(cudaMalloc(&base, kAlloc), cudaSuccess);
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(static_cast<char *>(base) + kOffset,
                                        kLength, exp);
    if (ret != 0 || exp.method != DmabufExport::Method::kDmabufReg) {
        RdmaContext::closeDmabufExport(exp);
        cudaFree(base);
        GTEST_SKIP() << "DMA-BUF path not taken (no DMA-BUF support, or "
                        "WITH_NVIDIA_PEERMEM selects ibv_reg_mr)";
    }
    off_t exported = lseek(exp.fd, 0, SEEK_END);
    ASSERT_GT(exported, 0);
    EXPECT_GE(static_cast<size_t>(exported), exp.offset + kLength);
    EXPECT_LT(static_cast<size_t>(exported), kAlloc)
        << "exported the whole allocation instead of the requested range";
    EXPECT_LT(exp.offset, static_cast<uint64_t>(sysconf(_SC_PAGESIZE)));
    RdmaContext::closeDmabufExport(exp);
    cudaFree(base);
}
#endif

}  // namespace
