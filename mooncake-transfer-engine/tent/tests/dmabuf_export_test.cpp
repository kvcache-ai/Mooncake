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

// Hardware-free unit tests for DmabufExport and
// RdmaContext::{exportDmabuf, closeDmabufExport}. Host memory always yields
// kHostReg / fd == -1, so these cases run in CI without an RNIC or GPU.

#include "tent/transport/rdma/context.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <vector>

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

using mooncake::tent::DmabufExport;
using mooncake::tent::RdmaContext;

namespace {

static void make_test_fd(int& out_fd) {
    int pipefd[2];
    ASSERT_EQ(pipe(pipefd), 0) << "pipe() failed: " << strerror(errno);
    close(pipefd[1]);
    out_fd = pipefd[0];
}

static bool fd_is_closed(int fd) {
    return fcntl(fd, F_GETFD) == -1 && errno == EBADF;
}

}  // namespace

TEST(TentDmabufExport, DefaultIsHostRegWithNoFd) {
    DmabufExport exp;
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
    EXPECT_EQ(exp.offset, 0u);
}

TEST(TentCloseDmabufExport, NoOpWhenFdIsNegative) {
    DmabufExport exp;
    RdmaContext::closeDmabufExport(exp);
    EXPECT_EQ(exp.fd, -1);
}

TEST(TentCloseDmabufExport, ClosesLiveFdAndClearsIt) {
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

TEST(TentCloseDmabufExport, Idempotent) {
    int fd = -1;
    make_test_fd(fd);
    ASSERT_GE(fd, 0);

    DmabufExport exp;
    exp.method = DmabufExport::Method::kDmabufReg;
    exp.fd = fd;

    RdmaContext::closeDmabufExport(exp);
    RdmaContext::closeDmabufExport(exp);
    EXPECT_EQ(exp.fd, -1);
}

TEST(TentExportDmabuf, HostMemoryYieldsHostReg) {
    std::vector<char> buf(4096);
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(buf.data(), exp, false);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
    RdmaContext::closeDmabufExport(exp);
}

TEST(TentExportDmabuf, HostMemoryPeermemFlagStillHostReg) {
    std::vector<char> buf(4096);
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(buf.data(), exp, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
}

TEST(TentExportDmabuf, MmapAnonymousYieldsHostReg) {
    void* p = mmap(nullptr, 4096, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    ASSERT_NE(p, MAP_FAILED);

    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(p, exp, false);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);

    munmap(p, 4096);
}

TEST(TentExportDmabuf, StackAddressYieldsHostReg) {
    char stack_buf[128];
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(stack_buf, exp, false);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
}

#ifdef USE_CUDA
TEST(TentExportDmabuf, GpuBufferDmaBufOrHostFallback) {
    if (cudaSetDevice(0) != cudaSuccess) {
        GTEST_SKIP() << "no CUDA device";
    }
    void* p = nullptr;
    if (cudaMalloc(&p, 4096) != cudaSuccess) {
        GTEST_SKIP() << "cudaMalloc failed";
    }
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(p, exp, /*with_nvidia_peermem=*/false);
    EXPECT_EQ(ret, 0);
    if (exp.method == DmabufExport::Method::kDmabufReg) {
        EXPECT_GE(exp.fd, 0);
    } else {
        EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
        EXPECT_EQ(exp.fd, -1);
    }
    RdmaContext::closeDmabufExport(exp);
    EXPECT_EQ(cudaFree(p), cudaSuccess);
}

TEST(TentExportDmabuf, GpuBufferPeermemFlagIsHostReg) {
    if (cudaSetDevice(0) != cudaSuccess) {
        GTEST_SKIP() << "no CUDA device";
    }
    void* p = nullptr;
    if (cudaMalloc(&p, 4096) != cudaSuccess) {
        GTEST_SKIP() << "cudaMalloc failed";
    }
    DmabufExport exp;
    int ret = RdmaContext::exportDmabuf(p, exp, /*with_nvidia_peermem=*/true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(exp.method, DmabufExport::Method::kHostReg);
    EXPECT_EQ(exp.fd, -1);
    RdmaContext::closeDmabufExport(exp);
    EXPECT_EQ(cudaFree(p), cudaSuccess);
}
#endif
