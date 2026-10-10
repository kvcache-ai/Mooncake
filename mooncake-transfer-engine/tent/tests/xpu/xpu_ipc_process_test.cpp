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

// Cross-process PCIe P2P over Level Zero IPC: the peer engine runs in a child
// process (this binary re-executed in peer mode), registers an XPU buffer and
// publishes its Level Zero IPC handle in the segment descriptor. The parent
// opens the segment over P2P RPC, fetches the dma-buf behind the handle from
// the child with pidfd_getfd(), maps it and copies without a host stage. The
// child is a descendant of the parent (and the Level Zero runtime marks it
// PR_SET_PTRACER_ANY anyway), so Yama ptrace_scope 0 or 1 suffices; scope 2
// needs CAP_SYS_PTRACE and scope 3 forbids the hand-over, in which case the
// test skips. A second fixture makes the peer non-dumpable so the hand-over
// is refused regardless of scope and checks that the engine quietly takes
// the staged network route instead.
//
// The child is exec()'d rather than forked in place: fork() after the SYCL
// runtime is initialized would hand the child a half-alive Level Zero state.
#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/platform/xpu.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
};
namespace tent {
namespace {
constexpr size_t kLength = (4UL << 20) + 4096;
constexpr const char* kPeerFlag = "--xpu-ipc-peer";
// Extra peer argument: drop PR_SET_DUMPABLE so pidfd_getfd() on the peer is
// refused (EPERM) for anyone without CAP_SYS_PTRACE, whatever ptrace_scope.
constexpr const char* kUndumpableFlag = "--undumpable";

std::shared_ptr<Config> makeConfig(bool legacy) {
    auto c = std::make_shared<Config>();
    c->set("metadata_type", "p2p");
    c->set("metadata_servers", "");
    c->set("rpc_server_hostname", "127.0.0.1");
    c->set("rpc_server_port", "0");
    c->set("log_level", "warning");
    c->set("transports/tcp/enable", true);
    c->set("transports/hp_tcp/enable", false);
    c->set("transports/rdma/enable", false);
    c->set("transports/shm/enable", false);
    c->set("transports/io_uring/enable", false);
    c->set("transports/mpcomm/enable", false);
    c->set("transports/xpu/enable", true);
    c->set("use_legacy_transport_selection", legacy);
    c->set("max_failover_attempts", 1);
    return c;
}

uint8_t pattern(size_t i, uint8_t salt) { return (i * 7 + salt) % 251; }

bool readFull(int fd, void* buf, size_t len) {
    auto* p = static_cast<uint8_t*>(buf);
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        p += n;
        len -= n;
    }
    return true;
}

bool writeFull(int fd, const void* buf, size_t len) {
    auto* p = static_cast<const uint8_t*>(buf);
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        p += n;
        len -= n;
    }
    return true;
}

// Peer protocol, over two pipes. Child -> parent on start: int32 status
// (0 ready, 1 no device, 2 no IPC export), then uint64 address, then the
// segment name (uint32 length + bytes). Parent -> child: one command byte,
// 'F'<salt> fills the buffer with pattern(i, salt), 'C'<salt> compares the
// buffer against pattern(i, salt); both reply one byte (1 ok / 0 mismatch).
// 'Q' quits. The child holds its allocation until it quits.
int peerMain(int cmd_fd, int reply_fd, bool undumpable) {
    if (undumpable && prctl(PR_SET_DUMPABLE, 0) != 0) return 9;
    auto engine = std::make_unique<TransferEngineImpl>(makeConfig(true));
    auto& platform = Platform::getLoader();
    MemoryOptions opts;
    opts.location = "xpu:0";
    void* buffer = nullptr;
    int32_t status = 0;
    if (!engine->available() ||
        !engine->allocateLocalMemory(&buffer, kLength, opts).ok()) {
        status = 1;
    } else {
        auto* xpu = dynamic_cast<XpuPlatform*>(&platform);
        XpuPlatform::IpcExport exported;
        if (!xpu || !xpu->exportIpc(buffer, kLength, exported).ok()) status = 2;
    }
    // allocateLocalMemory picks a concrete transport in opts.type; register
    // with every transport so the buffer carries the XPU tag and IPC handle.
    opts.type = UNSPEC;
    if (status == 0 &&
        !engine->registerLocalMemory({buffer}, {kLength}, opts).ok())
        status = 1;
    if (!writeFull(reply_fd, &status, sizeof(status))) return 10;
    if (status != 0) return 0;
    uint64_t addr = reinterpret_cast<uint64_t>(buffer);
    std::string name = engine->getSegmentName();
    uint32_t name_len = name.size();
    if (!writeFull(reply_fd, &addr, sizeof(addr)) ||
        !writeFull(reply_fd, &name_len, sizeof(name_len)) ||
        !writeFull(reply_fd, name.data(), name_len))
        return 11;

    std::vector<uint8_t> host(kLength);
    for (;;) {
        uint8_t cmd[2] = {0, 0};
        if (!readFull(cmd_fd, cmd, 1)) return 12;
        if (cmd[0] == 'Q') break;
        if (!readFull(cmd_fd, cmd + 1, 1)) return 13;
        uint8_t ok = 0;
        if (cmd[0] == 'F') {
            for (size_t i = 0; i < kLength; ++i) host[i] = pattern(i, cmd[1]);
            ok = platform.copy(buffer, host.data(), kLength).ok();
        } else if (cmd[0] == 'C') {
            ok = platform.copy(host.data(), buffer, kLength).ok();
            for (size_t i = 0; ok && i < kLength; ++i)
                ok = host[i] == pattern(i, cmd[1]);
        }
        if (!writeFull(reply_fd, &ok, sizeof(ok))) return 14;
    }
    engine.reset();
    return 0;
}

int ptraceScope() {
    std::ifstream in("/proc/sys/kernel/yama/ptrace_scope");
    int scope = 0;
    if (!(in >> scope)) return 0;  // no Yama: classic ptrace rules
    return scope;
}

// CAP_SYS_PTRACE in our effective set lets pidfd_getfd() through even for a
// non-dumpable peer, so the fallback cannot be provoked.
bool haveSysPtrace() {
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("CapEff:", 0) != 0) continue;
        uint64_t caps = std::strtoull(line.c_str() + 7, nullptr, 16);
        return (caps >> 19) & 1;  // CAP_SYS_PTRACE
    }
    return false;
}

class XpuIpcProcessTest : public ::testing::TestWithParam<bool> {
   protected:
    // Overridden by the fallback fixture; read in SetUp() to shape the peer.
    virtual bool undumpablePeer() const { return false; }

    void SetUp() override {
        // Spawn the peer first so its startup overlaps ours.
        int to_child[2], from_child[2];
        ASSERT_EQ(pipe(to_child), 0);
        ASSERT_EQ(pipe(from_child), 0);
        // Only the child's ends survive the exec.
        fcntl(to_child[1], F_SETFD, FD_CLOEXEC);
        fcntl(from_child[0], F_SETFD, FD_CLOEXEC);
        std::string cmd_fd = std::to_string(to_child[0]);
        std::string reply_fd = std::to_string(from_child[1]);
        child_ = fork();
        ASSERT_GE(child_, 0);
        if (child_ == 0) {
            execl("/proc/self/exe", "xpu_ipc_peer", kPeerFlag, cmd_fd.c_str(),
                  reply_fd.c_str(),
                  undumpablePeer() ? kUndumpableFlag : (char*)nullptr,
                  (char*)nullptr);
            _exit(127);
        }
        close(to_child[0]);
        close(from_child[1]);
        cmd_fd_ = to_child[1];
        reply_fd_ = from_child[0];

        int32_t status = -1;
        ASSERT_TRUE(readFull(reply_fd_, &status, sizeof(status)))
            << "peer exited before reporting";
        if (status == 1) GTEST_SKIP() << "peer has no SYCL device";
        if (status == 2) GTEST_SKIP() << "peer cannot export IPC handles";
        ASSERT_EQ(status, 0);
        uint32_t name_len = 0;
        ASSERT_TRUE(readFull(reply_fd_, &peer_addr_, sizeof(peer_addr_)));
        ASSERT_TRUE(readFull(reply_fd_, &name_len, sizeof(name_len)));
        peer_name_.resize(name_len);
        ASSERT_TRUE(readFull(reply_fd_, peer_name_.data(), name_len));

        auto& platform = Platform::getLoader(makeConfig(GetParam()));
        MemoryOptions opts;
        opts.location = "xpu:0";
        void* probe = nullptr;
        if (!platform.allocate(&probe, 4096, opts).ok())
            GTEST_SKIP() << "No SYCL device available";
        ASSERT_TRUE(platform.free(probe, 4096).ok());
        engine_ = std::make_unique<TransferEngineImpl>(makeConfig(GetParam()));
        ASSERT_TRUE(engine_->available());
    }
    void TearDown() override {
        for (auto batch : batches_) EXPECT_TRUE(engine_->freeBatch(batch).ok());
        // Close our mappings before the peer drops the allocation behind them.
        engine_.reset();
        if (child_ > 0) {
            if (cmd_fd_ >= 0) (void)writeFull(cmd_fd_, "Q", 1);
            int wstatus = 0;
            (void)waitpid(child_, &wstatus, 0);
            EXPECT_TRUE(WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0)
                << "peer exit status " << wstatus;
        }
        if (cmd_fd_ >= 0) close(cmd_fd_);
        if (reply_fd_ >= 0) close(reply_fd_);
    }
    bool peer(char cmd, uint8_t salt) {
        uint8_t msg[2] = {static_cast<uint8_t>(cmd), salt};
        uint8_t ok = 0;
        return writeFull(cmd_fd_, msg, sizeof(msg)) &&
               readFull(reply_fd_, &ok, sizeof(ok)) && ok == 1;
    }
    void allocate(const char* location, void** out) {
        MemoryOptions opts;
        opts.location = location;
        ASSERT_TRUE(engine_->allocateLocalMemory(out, kLength, opts).ok());
        ASSERT_NE(*out, nullptr);
        opts.type = UNSPEC;
        ASSERT_TRUE(engine_->registerLocalMemory({*out}, {kLength}, opts).ok());
    }
    TransferStatus wait(BatchID batch) {
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            auto s = engine_->progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        return status;
    }
    // Plans `request` against the peer and requires the direct XPU route.
    // Sets `*forbidden` instead of failing when the kernel forbids the fd
    // hand-over; the caller skips then (GTEST_SKIP only returns from the
    // function that issues it, so it must happen in the test body itself).
    void expectIpcRoute(Request& request, bool* forbidden) {
        *forbidden = false;
        ASSERT_TRUE(engine_->openSegment(request.target_id, peer_name_).ok());
        auto route = TransferEngineImplTestPeer::route(*engine_, request);
        if (route.transport != XPU && ptraceScope() >= 2) {
            *forbidden = true;
            return;
        }
        ASSERT_EQ(route.transport, XPU);
        EXPECT_TRUE(route.staging_params.empty());
    }
    // Plans `request` against a peer whose buffer cannot be mapped and
    // requires the planner to fall back to a staged network route.
    void expectFallbackRoute(Request& request) {
        ASSERT_TRUE(engine_->openSegment(request.target_id, peer_name_).ok());
        auto route = TransferEngineImplTestPeer::route(*engine_, request);
        ASSERT_NE(route.transport, XPU)
            << "peer buffer was mapped although the hand-over must fail";
        ASSERT_NE(route.transport, UNSPEC) << "no fallback route planned";
        EXPECT_FALSE(route.staging_params.empty());
    }
    void run(Request& request) {
        auto batch = engine_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back(batch);
        ASSERT_TRUE(engine_->submitTransfer(batch, {request}).ok());
        auto status = wait(batch);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, request.length);
    }
    void transfer(const char* local_location) {
        void* local = nullptr;
        ASSERT_NO_FATAL_FAILURE(allocate(local_location, &local));
        auto& platform = Platform::getLoader();
        std::vector<uint8_t> host(kLength);

        // WRITE: our pattern lands in the peer's VRAM.
        for (size_t i = 0; i < kLength; ++i) host[i] = pattern(i, 1);
        ASSERT_TRUE(platform.copy(local, host.data(), kLength).ok());
        ASSERT_TRUE(peer('F', 0));
        Request request{};
        request.opcode = Request::WRITE;
        request.source = local;
        request.target_offset = peer_addr_;
        request.length = kLength;
        if (undumpablePeer()) {
            ASSERT_NO_FATAL_FAILURE(expectFallbackRoute(request));
        } else {
            bool forbidden = false;
            ASSERT_NO_FATAL_FAILURE(expectIpcRoute(request, &forbidden));
            if (forbidden)
                GTEST_SKIP() << "ptrace_scope " << ptraceScope()
                             << " forbids pidfd_getfd without CAP_SYS_PTRACE";
        }
        ASSERT_NO_FATAL_FAILURE(run(request));
        EXPECT_TRUE(peer('C', 1)) << "peer buffer does not hold our pattern";

        // READ: the peer's pattern lands in ours.
        ASSERT_TRUE(peer('F', 2));
        std::fill(host.begin(), host.end(), 0);
        ASSERT_TRUE(platform.copy(local, host.data(), kLength).ok());
        request.opcode = Request::READ;
        ASSERT_NO_FATAL_FAILURE(run(request));
        ASSERT_TRUE(platform.copy(host.data(), local, kLength).ok());
        for (size_t i = 0; i < kLength; ++i) {
            ASSERT_EQ(host[i], pattern(i, 2)) << "mismatch at " << i;
        }
    }

    pid_t child_{-1};
    int cmd_fd_{-1}, reply_fd_{-1};
    uint64_t peer_addr_{0};
    std::string peer_name_;
    std::unique_ptr<TransferEngineImpl> engine_;
    std::vector<BatchID> batches_;
};

TEST_P(XpuIpcProcessTest, DeviceToPeerDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0"));
}

TEST_P(XpuIpcProcessTest, HostToPeerDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0"));
}

INSTANTIATE_TEST_SUITE_P(SelectionModes, XpuIpcProcessTest, ::testing::Bool());

// The peer refuses the dma-buf hand-over: the planner must log and fall back
// to the staged network route, and the transfer must still be byte-exact.
class XpuIpcFallbackTest : public XpuIpcProcessTest {
   protected:
    bool undumpablePeer() const override { return true; }
    void SetUp() override {
        if (haveSysPtrace())
            GTEST_SKIP() << "CAP_SYS_PTRACE lets the hand-over succeed anyway";
        XpuIpcProcessTest::SetUp();
    }
};

TEST_P(XpuIpcFallbackTest, DeviceToPeerDeviceFallsBackToStaging) {
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0"));
}

TEST_P(XpuIpcFallbackTest, HostToPeerDeviceFallsBackToStaging) {
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0"));
}

INSTANTIATE_TEST_SUITE_P(SelectionModes, XpuIpcFallbackTest, ::testing::Bool());
}  // namespace
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    // A peer that already exited makes the command pipe EPIPE, not a crash.
    signal(SIGPIPE, SIG_IGN);
    if (argc >= 4 && std::strcmp(argv[1], mooncake::tent::kPeerFlag) == 0)
        return mooncake::tent::peerMain(
            std::atoi(argv[2]), std::atoi(argv[3]),
            argc > 4 &&
                std::strcmp(argv[4], mooncake::tent::kUndumpableFlag) == 0);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
