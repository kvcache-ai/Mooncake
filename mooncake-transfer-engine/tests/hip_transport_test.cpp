// Copyright 2025 Mooncake Authors
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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"  // P2PHANDSHAKE
#include "transport/transport.h"

extern char** environ;

using namespace mooncake;

// startAsyncTransfer() switches the active device to the source GPU; if it does
// not restore it, the calling thread (which also launches the engine's compute
// kernels) is left on the wrong GPU and the next kernel fails with
// hipErrorInvalidDevice. This pins the caller to one GPU, transfers from a
// buffer on a different GPU, and asserts the active device is unchanged.

namespace {
constexpr size_t kLen = 64 * 1024;

void* allocOnDevice(size_t size, int device) {
    EXPECT_EQ(cudaSetDevice(device), cudaSuccess);
    void* ptr = nullptr;
    EXPECT_EQ(cudaMalloc(&ptr, size), cudaSuccess);
    return ptr;
}
}  // namespace

TEST(HipTransportTest, RestoresActiveDeviceAfterTransfer) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) {
        GTEST_SKIP() << "Needs >= 2 GPUs: the transfer source must live on a "
                        "different device than the calling thread's device.";
    }

    const int kCallerDevice = 0;  // device the engine thread runs on
    const int kSourceDevice = 1;  // KV source lives on a different GPU

    // P2PHANDSHAKE: self-contained loopback, no external metadata server.
    auto engine = std::make_unique<TransferEngine>(false);
    const std::string server_name = "127.0.0.1:17813";
    if (engine->init(P2PHANDSHAKE, server_name, "127.0.0.1", 17813) != 0) {
        GTEST_SKIP() << "TransferEngine init failed in this environment.";
    }

    Transport* transport = engine->installTransport("hip", nullptr);
    if (transport == nullptr) {
        GTEST_SKIP()
            << "HIP transport unavailable (built without -DUSE_HIP=ON?).";
    }

    // Both buffers on kSourceDevice so the source GPU differs from the caller.
    void* src = allocOnDevice(kLen, kSourceDevice);
    void* dst = allocOnDevice(kLen, kSourceDevice);
    ASSERT_EQ(engine->registerLocalMemory(
                  src, kLen, GPU_PREFIX + std::to_string(kSourceDevice)),
              0);
    ASSERT_EQ(engine->registerLocalMemory(
                  dst, kLen, GPU_PREFIX + std::to_string(kSourceDevice)),
              0);

    // P2P handshake binds a free port, so open the address it actually uses.
    auto segment_id = engine->openSegment(engine->getLocalIpAndPort());
    ASSERT_GE(segment_id, 0);

    ASSERT_EQ(cudaSetDevice(kSourceDevice), cudaSuccess);
    ASSERT_EQ(cudaMemset(src, 0xAB, kLen), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // Pin the caller to a different device than the source, as the engine does.
    ASSERT_EQ(cudaSetDevice(kCallerDevice), cudaSuccess);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kLen;
    entry.source = src;
    entry.target_id = segment_id;
    entry.target_offset = reinterpret_cast<uint64_t>(dst);
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    // Invariant: the caller's device is unchanged (== kSourceDevice before
    // fix).
    int active_after_submit = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_submit), cudaSuccess);
    EXPECT_EQ(active_after_submit, kCallerDevice)
        << "startAsyncTransfer() must restore the caller's active device. "
           "Leaving it on the source GPU corrupts the engine thread's HIP "
           "context, so its next kernel launch fails with "
           "hipErrorInvalidDevice.";

    // Drain the transfer (also a basic functional check).
    TransferStatus status;
    do {
        ASSERT_TRUE(engine->getTransferStatus(batch_id, 0, status).ok());
    } while (status.s == TransferStatusEnum::WAITING);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    int active_after_wait = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_wait), cudaSuccess);
    EXPECT_EQ(active_after_wait, kCallerDevice);

    engine->freeBatchID(batch_id);
    engine->unregisterLocalMemory(src);
    engine->unregisterLocalMemory(dst);
    (void)cudaSetDevice(kSourceDevice);
    (void)cudaFree(src);
    (void)cudaFree(dst);
}

// hipIpcOpenMemHandle maps the whole allocation that holds a registered
// buffer, at the allocation's base. Transfers to and from a buffer that starts
// inside a larger allocation (as caching allocators produce) must still hit the
// buffer, not the allocation base.
//
// The destination runs in a second process, since transfers to this process's
// own segment use LOCAL_SEGMENT_ID, which bypasses relocation and the IPC path.
// It is this binary re-executed with --hip_ipc_dst, not a fork, since the
// parent may already have initialized HIP. It registers three buffers in one
// zeroed allocation and reports their addresses; the parent writes two of them
// (one of them twice, which takes the cached-mapping path) and reads the third,
// closes its mappings, and then the destination checks every byte of the
// allocation.

DEFINE_bool(hip_ipc_dst, false, "Internal: run as the IPC destination.");
DEFINE_int32(hip_ipc_dst_device, 0,
             "Device ordinal, among the ones the IPC destination sees, that it "
             "allocates its buffers on.");
DEFINE_string(hip_ipc_dst_visible_devices, "",
              "If set, HIP_VISIBLE_DEVICES for the IPC destination process.");
DEFINE_int32(hip_ipc_expect_devices, 0,
             "If set, the number of devices this process must see.");
DEFINE_int32(hip_ipc_dst_expect_devices, 0,
             "If set, the number of devices the IPC destination must see.");
DEFINE_string(
    hip_ipc_rerun_marker, "",
    "If set, the IPC transfer test creates this file when it passes.");

namespace {
constexpr size_t kAllocLen = 4 * 1024 * 1024;
constexpr size_t kSliceLen = 64 * 1024;
// Offset 0 keeps the legacy handle-only payload; 512-byte alignment is what
// PyTorch's caching allocator produces.
constexpr size_t kSliceOffsets[] = {0, 1024 * 1024 + 512,
                                    2 * 1024 * 1024 + 4096};
constexpr unsigned char kWritten0 = 0xA0;
constexpr unsigned char kWritten1First = 0xB1;
constexpr unsigned char kWritten1 = 0xB2;
constexpr unsigned char kReadSource = 0x5C;
constexpr char kDstMarker[] = "HIP_IPC_DST ";
constexpr int kDstOk = 0, kDstWrongBytes = 1, kDstRegisterFailed = 2,
              kDstSetupFailed = 3, kDstNoGo = 4;

const char* describeDstExit(int rc) {
    switch (rc) {
        case kDstOk:
            return "ok";
        case kDstWrongBytes:
            return "a transfer reached the wrong bytes of the destination "
                   "allocation (its stderr shows the first one)";
        case kDstRegisterFailed:
            return "the destination could not register its buffers";
        case kDstSetupFailed:
            return "the destination failed to set up (see its stderr)";
        case kDstNoGo:
            return "the parent stopped before the check";
        default:
            return "the destination crashed or was killed";
    }
}

int dstFailed(int rc, const char* what) {
    fprintf(stderr, "IPC destination: %s failed\n", what);
    return rc;
}

unsigned char expectedDstByte(size_t i) {
    for (size_t s = 0; s < std::size(kSliceOffsets); ++s) {
        if (i >= kSliceOffsets[s] && i < kSliceOffsets[s] + kSliceLen)
            return s == 0 ? kWritten0 : s == 1 ? kWritten1 : kReadSource;
    }
    return 0;
}

int runIpcDestination() {
    int device_count = 0;
    if (FLAGS_hip_ipc_expect_devices > 0 &&
        (cudaGetDeviceCount(&device_count) != cudaSuccess ||
         device_count != FLAGS_hip_ipc_expect_devices))
        return dstFailed(kDstSetupFailed, "the visible device count check");
    auto engine = std::make_unique<TransferEngine>(false);
    // P2PHANDSHAKE binds a free port (the one given here is ignored); the
    // address it really uses is reported below.
    if (engine->init(P2PHANDSHAKE, "127.0.0.1:17815", "127.0.0.1", 17815) != 0)
        return dstFailed(kDstSetupFailed, "TransferEngine::init");
    if (engine->installTransport("hip", nullptr) == nullptr)
        return dstFailed(kDstSetupFailed, "installTransport");

    char* alloc = nullptr;
    const int device = FLAGS_hip_ipc_dst_device;
    if (cudaSetDevice(device) != cudaSuccess ||
        cudaMalloc(reinterpret_cast<void**>(&alloc), kAllocLen) !=
            cudaSuccess ||
        cudaMemset(alloc, 0, kAllocLen) != cudaSuccess ||
        cudaMemset(alloc + kSliceOffsets[2], kReadSource, kSliceLen) !=
            cudaSuccess ||
        cudaDeviceSynchronize() != cudaSuccess)
        return dstFailed(kDstSetupFailed, "preparing the allocation");
    std::string report = kDstMarker + engine->getLocalIpAndPort();
    for (size_t off : kSliceOffsets) {
        if (engine->registerLocalMemory(alloc + off, kSliceLen,
                                        GPU_PREFIX + std::to_string(device)) !=
            0)
            return dstFailed(kDstRegisterFailed, "registerLocalMemory");
        report +=
            " " + std::to_string(reinterpret_cast<uintptr_t>(alloc + off));
    }
    printf("%s\n", report.c_str());
    fflush(stdout);

    char go = 0;
    ssize_t n;
    do {
        n = read(STDIN_FILENO, &go, 1);
    } while (n < 0 && errno == EINTR);
    if (n != 1) return kDstNoGo;

    std::vector<unsigned char> host(kAllocLen);
    if (cudaMemcpy(host.data(), alloc, kAllocLen, cudaMemcpyDeviceToHost) !=
        cudaSuccess)
        return dstFailed(kDstSetupFailed, "copying the allocation back");
    int rc = kDstOk;
    for (size_t i = 0; i < kAllocLen; ++i) {
        if (host[i] != expectedDstByte(i)) {
            fprintf(stderr, "destination byte %zu is 0x%02x, expected 0x%02x\n",
                    i, host[i], expectedDstByte(i));
            rc = kDstWrongBytes;
            break;
        }
    }
    for (size_t off : kSliceOffsets) engine->unregisterLocalMemory(alloc + off);
    engine.reset();
    (void)cudaFree(alloc);
    return rc;
}

// A copy of this process's environment with `name` set to `value` (or
// removed, for nullptr).
std::vector<std::string> environWith(const std::string& name,
                                     const char* value) {
    std::vector<std::string> env;
    for (char** e = environ; *e != nullptr; ++e) {
        if (std::string(*e).rfind(name + "=", 0) != 0) env.emplace_back(*e);
    }
    if (value != nullptr) env.push_back(name + "=" + value);
    return env;
}

std::vector<char*> cStrings(std::vector<std::string>& strings) {
    std::vector<char*> out;
    for (auto& s : strings) out.push_back(s.data());
    out.push_back(nullptr);
    return out;
}

// Owns the destination process and reaps it when destroyed.
class DestinationProcess {
   public:
    bool start() {
        int to_child[2], from_child[2];
        if (pipe(to_child) != 0) return false;
        if (pipe(from_child) != 0) {
            close(to_child[0]);
            close(to_child[1]);
            return false;
        }
        char exe[] = "/proc/self/exe";
        std::vector<std::string> args = {
            exe, "--hip_ipc_dst",
            "--hip_ipc_dst_device=" + std::to_string(FLAGS_hip_ipc_dst_device),
            "--hip_ipc_expect_devices=" +
                std::to_string(FLAGS_hip_ipc_dst_expect_devices)};
        auto argv = cStrings(args);
        std::vector<std::string> env_strings =
            environWith("HIP_VISIBLE_DEVICES",
                        FLAGS_hip_ipc_dst_visible_devices.empty()
                            ? getenv("HIP_VISIBLE_DEVICES")
                            : FLAGS_hip_ipc_dst_visible_devices.c_str());
        auto envp = cStrings(env_strings);
        posix_spawn_file_actions_t actions;
        int rc = posix_spawn_file_actions_init(&actions);
        if (rc == 0) {
            rc = posix_spawn_file_actions_adddup2(&actions, to_child[0],
                                                  STDIN_FILENO);
            if (rc == 0)
                rc = posix_spawn_file_actions_adddup2(&actions, from_child[1],
                                                      STDOUT_FILENO);
            // A pipe end can itself be fd 0 or 1 if the parent ran with stdio
            // closed; those were just replaced by dup2 and must stay open.
            for (int fd :
                 {to_child[0], to_child[1], from_child[0], from_child[1]}) {
                if (rc == 0 && fd > STDERR_FILENO)
                    rc = posix_spawn_file_actions_addclose(&actions, fd);
            }
            if (rc == 0)
                rc = posix_spawn(&pid_, exe, &actions, nullptr, argv.data(),
                                 envp.data());
            posix_spawn_file_actions_destroy(&actions);
        }
        close(to_child[0]);
        close(from_child[1]);
        if (rc != 0) {
            close(to_child[1]);
            close(from_child[0]);
            pid_ = -1;
            return false;
        }
        to_child_ = to_child[1];
        from_child_ = from_child[0];
        return true;
    }

    // Reads the destination's report line: handshake name, then addresses.
    bool readReport(std::string* name, std::vector<uint64_t>* addrs) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        std::string line;
        char c;
        while (true) {
            const auto left =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now())
                    .count();
            if (left <= 0) return false;
            pollfd pfd{from_child_, POLLIN, 0};
            const int ready = poll(&pfd, 1, static_cast<int>(left));
            if (ready < 0 && errno == EINTR) continue;
            if (ready <= 0) return false;
            const ssize_t n = read(from_child_, &c, 1);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            if (c != '\n') {
                line.push_back(c);
                continue;
            }
            if (line.rfind(kDstMarker, 0) == 0) break;
            line.clear();  // other output the engine wrote to stdout
        }
        std::istringstream in(line.substr(sizeof(kDstMarker) - 1));
        in >> *name;
        uint64_t addr;
        while (in >> addr) addrs->push_back(addr);
        reported_ = true;
        return !name->empty();
    }

    // Tells the destination to check its memory (only if it reported) and
    // returns its exit code, or -1 if it had to be killed.
    int finish() {
        if (pid_ < 0) return -1;
        if (to_child_ >= 0) {
            if (reported_) {
                const char go = 1;
                while (write(to_child_, &go, 1) < 0 && errno == EINTR) {
                }
            }
            close(to_child_);
            to_child_ = -1;
        }
        int status = 0;
        for (int i = 0; i < 600; ++i) {  // up to 60 s
            if (waitpid(pid_, &status, WNOHANG) == pid_) {
                pid_ = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            }
            usleep(100 * 1000);
        }
        kill(pid_, SIGKILL);
        while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
        }
        pid_ = -1;
        return -1;
    }

    ~DestinationProcess() {
        reported_ = false;  // an early return must not trigger the check
        finish();
        if (from_child_ >= 0) close(from_child_);
    }

   private:
    pid_t pid_ = -1;
    int to_child_ = -1;
    int from_child_ = -1;
    bool reported_ = false;
};

// Sets an environment variable for the lifetime of the object.
class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        const char* old = getenv(name);
        had_old_ = old != nullptr;
        if (had_old_) old_ = old;
        setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (had_old_)
            setenv(name_, old_.c_str(), 1);
        else
            unsetenv(name_);
    }

   private:
    const char* name_;
    std::string old_;
    bool had_old_ = false;
};

bool runTransfer(TransferEngine* engine, TransferRequest::OpCode opcode,
                 void* local, Transport::SegmentID segment, uint64_t remote) {
    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = opcode;
    entry.length = kSliceLen;
    entry.source = local;
    entry.target_id = segment;
    entry.target_offset = remote;
    bool ok = engine->submitTransfer(batch_id, {entry}).ok();
    TransferStatus status;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (ok) {
        ok = engine->getTransferStatus(batch_id, 0, status).ok();
        if (!ok || status.s != TransferStatusEnum::WAITING) break;
        if (std::chrono::steady_clock::now() > deadline) ok = false;
    }
    ok = ok && status.s == TransferStatusEnum::COMPLETED;
    const bool freed = engine->freeBatchID(batch_id).ok();
    return ok && freed;
}
}  // namespace

TEST(HipTransportTest, IpcTransfersHitBuffersInsideLargerAllocation) {
    int device_count = 0;
    const auto count_err = cudaGetDeviceCount(&device_count);
    if (count_err == cudaErrorNoDevice ||
        count_err == cudaErrorInsufficientDriver ||
        (count_err == cudaSuccess && device_count < 1))
        GTEST_SKIP() << "Needs a GPU.";
    ASSERT_EQ(count_err, cudaSuccess);
    if (FLAGS_hip_ipc_expect_devices > 0)
        ASSERT_EQ(device_count, FLAGS_hip_ipc_expect_devices);
    // MC_USE_HIP_IPC=0 or MC_USE_NVLINK_IPC=0 selects fabric handles; both
    // processes must use IPC.
    ScopedEnv hip_ipc("MC_USE_HIP_IPC", "1");
    ScopedEnv nvlink_ipc("MC_USE_NVLINK_IPC", "1");
    DestinationProcess dst;
    ASSERT_TRUE(dst.start());
    // The destination may exit before reading the go byte.
    struct IgnoreSigpipe {
        void (*previous)(int) = signal(SIGPIPE, SIG_IGN);
        ~IgnoreSigpipe() { signal(SIGPIPE, previous); }
    } ignore_sigpipe;
    std::string dst_name;
    std::vector<uint64_t> dst_addrs;
    if (!dst.readReport(&dst_name, &dst_addrs)) {
        const int rc = dst.finish();
        FAIL() << "IPC destination did not report: " << describeDstExit(rc)
               << " (exit " << rc << ")";
    }
    ASSERT_EQ(dst_addrs.size(), std::size(kSliceOffsets));

    auto engine = std::make_unique<TransferEngine>(false);
    // As in the destination, the port is ignored in P2PHANDSHAKE mode.
    ASSERT_EQ(engine->init(P2PHANDSHAKE, "127.0.0.1:17816", "127.0.0.1", 17816),
              0);
    ASSERT_NE(engine->installTransport("hip", nullptr), nullptr);
    // The source buffer is on another GPU than the destination's when one can
    // reach it, and is registered from a thread that has never touched the
    // GPU runtime, so registration cannot rely on the caller's current device
    // or context.
    int local_device = 0;
    for (int d = device_count - 1; d > 0; --d) {
        int can_access = 0;
        if (cudaDeviceCanAccessPeer(&can_access, d, 0) == cudaSuccess &&
            can_access) {
            local_device = d;
            break;
        }
    }
    const std::string local_location =
        GPU_PREFIX + std::to_string(local_device);
    ASSERT_EQ(cudaSetDevice(local_device), cudaSuccess);
    void* local = nullptr;
    ASSERT_EQ(cudaMalloc(&local, kSliceLen), cudaSuccess);
    char* past_end = static_cast<char*>(local) + 512;
    int past_end_rc = -1, wrap_rc = -1, local_rc = -1;
    bool past_end_unpublished = false, wrap_unpublished = false;
    std::thread([&] {
        // Unsupported ranges succeed without publishing an IPC buffer, so
        // other transports can still register them. Check before unregister
        // to distinguish a skipped range from an unsafe published handle.
        past_end_rc =
            engine->registerLocalMemory(past_end, kAllocLen, local_location);
        auto metadata = engine->getMetadata();
        auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        past_end_unpublished = desc && desc->buffers.empty();
        if (past_end_rc == 0)
            EXPECT_EQ(engine->unregisterLocalMemory(past_end), 0);
        wrap_rc = engine->registerLocalMemory(past_end, SIZE_MAX - 256,
                                              local_location);
        desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        wrap_unpublished = desc && desc->buffers.empty();
        if (wrap_rc == 0) EXPECT_EQ(engine->unregisterLocalMemory(past_end), 0);
        local_rc =
            engine->registerLocalMemory(local, kSliceLen, local_location);
    }).join();
    EXPECT_EQ(past_end_rc, 0);
    EXPECT_EQ(wrap_rc, 0);
    EXPECT_TRUE(past_end_unpublished);
    EXPECT_TRUE(wrap_unpublished);
    ASSERT_EQ(local_rc, 0);
    auto segment = engine->openSegment(dst_name);
    ASSERT_NE(segment, static_cast<Transport::SegmentID>(-1));

    auto fill = [&](unsigned char value) {
        return cudaMemset(local, value, kSliceLen) == cudaSuccess &&
               cudaDeviceSynchronize() == cudaSuccess;
    };
    ASSERT_TRUE(fill(kWritten0));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[0]));
    ASSERT_TRUE(fill(kWritten1First));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[1]));
    // Second write to the same buffer: takes the cached mapping.
    ASSERT_TRUE(fill(kWritten1));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[1]));
    ASSERT_TRUE(fill(0));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::READ, local, segment,
                            dst_addrs[2]));
    std::vector<unsigned char> read_back(kSliceLen);
    ASSERT_EQ(
        cudaMemcpy(read_back.data(), local, kSliceLen, cudaMemcpyDeviceToHost),
        cudaSuccess);
    EXPECT_EQ(std::count(read_back.begin(), read_back.end(), kReadSource),
              static_cast<std::ptrdiff_t>(kSliceLen))
        << "the READ did not come from the registered buffer";

    // Close this process's mappings of the destination's memory before the
    // destination frees it.
    EXPECT_EQ(engine->unregisterLocalMemory(local), 0);
    engine.reset();
    (void)cudaFree(local);
    const int dst_rc = dst.finish();
    EXPECT_EQ(dst_rc, kDstOk) << describeDstExit(dst_rc);
    if (!FLAGS_hip_ipc_rerun_marker.empty() && !HasFailure())
        close(
            open(FLAGS_hip_ipc_rerun_marker.c_str(), O_CREAT | O_WRONLY, 0600));
}

// Same-node peers need not see the same devices: in SGLang PD each role sees
// only its own GPUs, so a TP4 decode's rank 3 exports from device ordinal 3
// while a TP2 prefill process sees two devices. hipIpcOpenMemHandle checks
// the handle's owner ordinal against the importer's device count and fails
// with hipErrorInvalidValue. This reruns the test above with the destination
// seeing two devices and allocating on the second, and the source seeing
// only the first.
TEST(HipTransportTest, IpcOpensHandlesFromDeviceOrdinalsTheImporterLacks) {
    ScopedEnv workaround("MC_HIP_IPC_OWNER_DEVICE_WORKAROUND", "1");
    if (!FLAGS_hip_ipc_rerun_marker.empty())
        GTEST_SKIP() << "Already running as the restricted rerun.";
    int device_count = 0;
    const auto count_err = cudaGetDeviceCount(&device_count);
    if (count_err == cudaErrorNoDevice ||
        count_err == cudaErrorInsufficientDriver ||
        (count_err == cudaSuccess && device_count < 2))
        GTEST_SKIP() << "Needs >= 2 GPUs.";
    ASSERT_EQ(count_err, cudaSuccess);
    // The children's device sets are written as HIP_VISIBLE_DEVICES indices
    // into what ROCr exposes, which only matches this process's ordinals
    // when HIP applies no filter of its own.
    for (const char* name : {"HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES"}) {
        const char* value = getenv(name);
        if (value != nullptr && *value != '\0')
            GTEST_SKIP() << name << " is set; this test sets it itself, so "
                         << "restrict GPUs with ROCR_VISIBLE_DEVICES instead.";
    }
    // The source's only device must reach the destination's second one.
    int peer = -1;
    for (int d = 1; d < device_count && peer < 0; ++d) {
        int can_access = 0;
        ASSERT_EQ(cudaDeviceCanAccessPeer(&can_access, 0, d), cudaSuccess);
        if (can_access) peer = d;
    }
    if (peer < 0) GTEST_SKIP() << "No GPU has peer access to GPU 0.";

    char marker[] = "/tmp/hip_ipc_rerun_XXXXXX";
    const int marker_fd = mkstemp(marker);
    ASSERT_GE(marker_fd, 0);
    close(marker_fd);
    unlink(marker);
    std::vector<std::string> args = {
        "/proc/self/exe",
        "--hip_ipc_dst_device=1",
        "--hip_ipc_dst_visible_devices=0," + std::to_string(peer),
        "--hip_ipc_dst_expect_devices=2",
        "--hip_ipc_expect_devices=1",
        std::string("--hip_ipc_rerun_marker=") + marker};
    auto argv = cStrings(args);
    std::vector<std::string> env_strings =
        environWith("HIP_VISIBLE_DEVICES", "0");
    // HIP also honors CUDA_VISIBLE_DEVICES; only HIP_VISIBLE_DEVICES may
    // restrict the rerun. gflags rejects --gtest_* flags, so the filter goes
    // through the environment, and inherited sharding must not drop the test.
    for (auto it = env_strings.begin(); it != env_strings.end();) {
        const bool drop = it->rfind("CUDA_VISIBLE_DEVICES=", 0) == 0 ||
                          it->rfind("GTEST_FILTER=", 0) == 0 ||
                          it->rfind("GTEST_SHARD_INDEX=", 0) == 0 ||
                          it->rfind("GTEST_TOTAL_SHARDS=", 0) == 0;
        it = drop ? env_strings.erase(it) : it + 1;
    }
    env_strings.push_back(
        "GTEST_FILTER=HipTransportTest."
        "IpcTransfersHitBuffersInsideLargerAllocation");
    auto envp = cStrings(env_strings);
    // Its own process group, so a timeout also kills the destination the
    // rerun starts.
    posix_spawnattr_t attr;
    ASSERT_EQ(posix_spawnattr_init(&attr), 0);
    ASSERT_EQ(posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETPGROUP), 0);
    ASSERT_EQ(posix_spawnattr_setpgroup(&attr, 0), 0);
    pid_t pid = -1;
    const int spawn_rc =
        posix_spawn(&pid, argv[0], nullptr, &attr, argv.data(), envp.data());
    posix_spawnattr_destroy(&attr);
    ASSERT_EQ(spawn_rc, 0);
    int status = 0;
    pid_t waited = 0;
    for (int i = 0; i < 2400 && waited == 0; ++i) {  // up to 240 s
        waited = waitpid(pid, &status, WNOHANG);
        if (waited < 0 && errno == EINTR) waited = 0;
        if (waited == 0) usleep(100 * 1000);
    }
    if (waited == 0) {
        kill(-pid, SIGKILL);
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        unlink(marker);
        FAIL() << "the rerun did not finish within 240 s";
    }
    ASSERT_EQ(waited, pid) << "waitpid failed: " << strerror(errno);
    const bool passed = access(marker, F_OK) == 0;
    unlink(marker);
    ASSERT_TRUE(WIFEXITED(status)) << "the rerun was killed";
    EXPECT_EQ(WEXITSTATUS(status), 0)
        << "transfers to a destination on device ordinal 1 failed from a "
           "process that sees one device (its output is above)";
    EXPECT_TRUE(passed) << "the rerun did not run the transfer test to the end";
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    if (FLAGS_hip_ipc_dst) return runIpcDestination();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
