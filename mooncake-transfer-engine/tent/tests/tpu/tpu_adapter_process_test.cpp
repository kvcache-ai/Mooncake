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

// Two exec'd processes, each with its own built-in TPU adapter, mock buffer
// provider and TransferEngineImpl, over loopback TCP. Pipes carry only test
// control messages; all payload bytes must pass through TENT.
//
// In tpu_adapter_engine_test both engines share one adapter registry, so a
// peer's fake address would still resolve if the initiator wrongly staged the
// peer's side itself. Here the peer's fake addresses mean nothing to the
// initiator's adapter (they may even coincide with its own), so the remote
// HBM<->host hop must run in the peer, through the peer's adapter.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "mock_tpu_env.h"
#include "tent/common/config.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

#ifndef TPU_PJRT_ADAPTER_LIB
#error "TPU_PJRT_ADAPTER_LIB must be defined by the build"
#endif
#ifndef MOCK_TPU_BUFFER_PROVIDER_LIB
#error "MOCK_TPU_BUFFER_PROVIDER_LIB must be defined by the build"
#endif

extern char** environ;
namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static void smallStagePool(tent::TransferEngineImpl& engine) {
        engine.staging_proxy_ =
            std::make_unique<tent::ProxyManager>(&engine, 1UL << 20, 4);
    }
    static tent::Status stopStaging(tent::TransferEngineImpl& engine) {
        return engine.staging_proxy_->deconstruct();
    }
    static bool empty(tent::TransferEngineImpl& engine) {
        std::lock_guard<std::mutex> guard(engine.mutex_);
        return engine.allocated_memory_.empty() &&
               engine.metadata_->segmentManager()
                   .getLocal()
                   ->getMemory()
                   .buffers.empty();
    }
};
namespace tent {
namespace {
constexpr size_t kSize = (8UL << 20) + 4096;
// Not a multiple of 32: the mock provider accepts any offset here. tpu-sync's
// alignment rule is covered by tpu_adapter_engine_test.
constexpr size_t kOffset = 17;
constexpr int kHost = 0, kTpu = 1;  // Buffer indices.

using Params = std::tuple<bool, bool>;  // legacy selection, runtime queue

std::shared_ptr<Config> config(Params params = {}) {
    auto c = std::make_shared<Config>();
    c->set("metadata_type", "p2p");
    c->set("metadata_servers", "");
    c->set("rpc_server_hostname", "127.0.0.1");
    c->set("rpc_server_port", "0");
    c->set("log_level", "warning");
    c->set("transports/tcp/enable", true);
    c->set("transports/tcp/max_retry_count", 0);
    c->set("transports/hp_tcp/enable", false);
    c->set("transports/rdma/enable", false);
    c->set("transports/shm/enable", false);
    c->set("transports/io_uring/enable", false);
    c->set("transports/mpcomm/enable", false);
    c->set("transports/tpu/enable", true);
    c->set("use_legacy_transport_selection", std::get<0>(params));
    c->set("enable_runtime_queue", std::get<1>(params));
    c->set("max_failover_attempts", 1);
    return c;
}

bool io(int fd, void* data, size_t size, bool writing) {
    auto* cursor = static_cast<char*>(data);
    while (size) {
        auto n = writing ? write(fd, cursor, size) : read(fd, cursor, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        cursor += n;
        size -= n;
    }
    return true;
}

std::vector<uint8_t> pattern(size_t size, int seed) {
    std::vector<uint8_t> result(size);
    for (size_t i = 0; i < size; ++i) result[i] = (i * 13 + seed) % 251;
    return result;
}

bool loadAdapter() {
    std::string error;
    if (MockTpuEnv::instance().load(TPU_PJRT_ADAPTER_LIB,
                                    MOCK_TPU_BUFFER_PROVIDER_LIB, &error))
        return true;
    fprintf(stderr, "cannot load the TPU adapter: %s\n", error.c_str());
    return false;
}

// One host buffer and one mock TPU buffer. The TPU buffer is registered with
// the adapter on construction, BEFORE the engine exists, because TENT reads the
// TPU topology from the adapter when the engine is created.
class Buffers {
   public:
    explicit Buffers(int device) {
        tpu_ = MockTpuEnv::instance().create(kSize, device);
    }
    // The engine is declared after this object, so it is already destroyed
    // here: only the adapter-side buffer is left to release.
    ~Buffers() {
        engine_ = nullptr;
        cleanup();
    }

    Status attach(TransferEngineImpl& engine) {
        if (!tpu_) return Status::InternalError("adapter registration failed");
        engine_ = &engine;
        MemoryOptions opts;
        opts.location = "cpu:0";
        CHECK_STATUS(engine.allocateLocalMemory(&host_, kSize, opts));
        CHECK_STATUS(engine.registerLocalMemory({host_}, {kSize}, opts));
        host_registered_ = true;
        MemoryOptions tpu_opts;  // Location left to TENT, as mooncake.tpu does.
        CHECK_STATUS(
            engine.registerLocalMemory({tpu_.addr()}, {kSize}, tpu_opts));
        tpu_registered_ = true;
        return Status::OK();
    }

    // Address the engine sees.
    void* addr(int i) const { return i == kTpu ? tpu_.addr() : host_; }
    // Where the bytes live: the mock provider's storage for TPU.
    uint8_t* bytes(int i) const {
        return i == kTpu ? tpu_.data : static_cast<uint8_t*>(host_);
    }

    Status cleanup() {
        if (engine_) {
            auto& engine = *engine_;
            engine_ = nullptr;
            CHECK_STATUS(TransferEngineImplTestPeer::stopStaging(engine));
            if (tpu_registered_)
                CHECK_STATUS(engine.unregisterLocalMemory(tpu_.addr()));
            if (host_registered_)
                CHECK_STATUS(engine.unregisterLocalMemory(host_));
            if (host_) CHECK_STATUS(engine.freeLocalMemory(host_));
            host_ = nullptr;
            if (engine.aliveBatchCountForTest() ||
                engine.deferredStageTeardownBatchCountForTest() ||
                !TransferEngineImplTestPeer::empty(engine))
                return Status::InternalError("TPU process resources remain");
        }
        if (tpu_ && !MockTpuEnv::instance().unregister(tpu_))
            return Status::InternalError("adapter unregister failed");
        tpu_ = MockTpuBuffer{};
        return Status::OK();
    }

   private:
    TransferEngineImpl* engine_ = nullptr;
    MockTpuBuffer tpu_;
    void* host_ = nullptr;
    bool host_registered_ = false, tpu_registered_ = false;
};

class BatchGuard {
   public:
    explicit BatchGuard(TransferEngineImpl& engine)
        : engine_(engine), id(engine.allocateBatch(1)) {}
    ~BatchGuard() {
        if (id) engine_.freeBatch(id);
    }
    Status release() {
        CHECK_STATUS(engine_.freeBatch(id));
        id = 0;
        return Status::OK();
    }
    TransferEngineImpl& engine_;
    BatchID id;
};

struct Hello {
    char segment[256]{};
    uint64_t addresses[2]{};
};
struct Command {
    int buffer = 0;
    int verify = 0;
    int seed = 0;
    size_t length = 0;
};

int peerMain(Params params) {
    if (!loadAdapter()) return 1;
    // A different device than the initiator's, so the processes also differ
    // in TPU topology.
    Buffers buffers(/*device=*/1);
    TransferEngineImpl engine(config(params));
    if (!engine.available()) return 1;
    TransferEngineImplTestPeer::smallStagePool(engine);
    if (!buffers.attach(engine).ok()) return 2;
    Hello hello;
    auto name = engine.getSegmentName();
    if (name.size() >= sizeof(hello.segment)) return 3;
    std::memcpy(hello.segment, name.c_str(), name.size() + 1);
    for (int i = 0; i != 2; ++i)
        hello.addresses[i] = reinterpret_cast<uint64_t>(buffers.addr(i));
    if (!io(STDOUT_FILENO, &hello, sizeof(hello), true)) return 4;
    Command command;
    while (io(STDIN_FILENO, &command, sizeof(command), false)) {
        if (command.buffer == -1) return buffers.cleanup().ok() ? 0 : 8;
        if (command.buffer < 0 || command.buffer > 1 ||
            command.length > kSize - kOffset)
            return 5;
        auto data = pattern(command.length, command.seed);
        auto* ptr = buffers.bytes(command.buffer) + kOffset;
        int result = 0;
        if (command.verify) {
            if (std::memcmp(ptr, data.data(), command.length) != 0) result = 1;
        } else {
            std::memcpy(ptr, data.data(), command.length);
        }
        if (!io(STDOUT_FILENO, &result, sizeof(result), true)) return 6;
    }
    return 7;
}

class Peer {
   public:
    ~Peer() {
        if (input >= 0) close(input);
        if (output >= 0) close(output);
        if (pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }
    bool start(Params params = {}) {
        int in[2], out[2];
        if (pipe2(in, O_CLOEXEC)) return false;
        if (pipe2(out, O_CLOEXEC)) {
            close(in[0]);
            close(in[1]);
            return false;
        }
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_adddup2(&actions, in[0], STDIN_FILENO);
        posix_spawn_file_actions_adddup2(&actions, out[1], STDOUT_FILENO);
        char executable[] = "/proc/self/exe", option[] = "--tpu-peer";
        char legacy[] = "0", queue[] = "0";
        legacy[0] += std::get<0>(params);
        queue[0] += std::get<1>(params);
        char* argv[] = {executable, option, legacy, queue, nullptr};
        int error =
            posix_spawn(&pid, executable, &actions, nullptr, argv, environ);
        posix_spawn_file_actions_destroy(&actions);
        close(in[0]);
        close(out[1]);
        input = in[1];
        output = out[0];
        return error == 0 && io(output, &hello, sizeof(hello), false);
    }
    bool command(int buffer, bool verify, int seed, size_t length) {
        Command cmd{buffer, verify, seed, length};
        int result = -1;
        return io(input, &cmd, sizeof(cmd), true) &&
               io(output, &result, sizeof(result), false) && result == 0;
    }
    bool crash() {
        if (pid <= 0 || kill(pid, SIGKILL)) return false;
        int status = 0;
        if (waitpid(pid, &status, 0) != pid) return false;
        pid = -1;
        close(input);
        input = -1;
        close(output);
        output = -1;
        return WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    }
    int finish() {
        Command cmd;
        cmd.buffer = -1;
        if (!io(input, &cmd, sizeof(cmd), true)) return -1;
        int status = -1;
        if (waitpid(pid, &status, 0) < 0) return -1;
        pid = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
    Hello hello;

   private:
    int input = -1, output = -1;
    pid_t pid = -1;
};

TransferStatus wait(TransferEngineImpl& engine, BatchID batch) {
    TransferStatus status{};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    do {
        auto result = engine.progressBatch(batch, status);
        EXPECT_TRUE(result.ok()) << result.ToString();
        if (!result.ok() || status.s != PENDING) break;
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return status;
}

class TpuAdapterProcessTest : public ::testing::TestWithParam<Params> {
   protected:
    void SetUp() override { ASSERT_TRUE(loadAdapter()); }
};

TEST_P(TpuAdapterProcessTest, TcpReadWriteBetweenSeparateAdapters) {
    Peer peer;
    ASSERT_TRUE(peer.start(GetParam()));
    Buffers buffers(/*device=*/0);
    TransferEngineImpl engine(config(GetParam()));
    ASSERT_TRUE(engine.available());
    TransferEngineImplTestPeer::smallStagePool(engine);
    ASSERT_TRUE(buffers.attach(engine).ok());
    SegmentID remote;
    ASSERT_TRUE(engine.openSegment(remote, peer.hello.segment).ok());
    int seed = 0;
    for (auto [local, target] : {std::pair{kTpu, kTpu}, std::pair{kTpu, kHost},
                                 std::pair{kHost, kTpu}}) {
        for (auto length : {size_t(1), size_t(65539), (8UL << 20) + 37}) {
            for (auto op : {Request::READ, Request::WRITE}) {
                SCOPED_TRACE(::testing::Message()
                             << (local == kTpu ? "tpu" : "host") << " -> "
                             << (target == kTpu ? "tpu" : "host")
                             << " length=" << length << " op=" << op);
                auto expected = pattern(length, ++seed);
                auto* bytes = buffers.bytes(local) + kOffset;
                ASSERT_TRUE(peer.command(
                    target, false, op == Request::READ ? seed : 0, length));
                auto initial =
                    op == Request::WRITE ? expected : pattern(length, 0);
                std::memcpy(bytes, initial.data(), length);
                Request request{};
                request.opcode = op;
                request.source =
                    static_cast<uint8_t*>(buffers.addr(local)) + kOffset;
                request.target_id = remote;
                request.target_offset = peer.hello.addresses[target] + kOffset;
                request.length = length;
                auto route = TransferEngineImplTestPeer::route(engine, request);
                ASSERT_EQ(route.transport, TCP);
                BatchGuard owner(engine);
                ASSERT_NE(owner.id, 0u);
                ASSERT_TRUE(engine.submitTransfer(owner.id, {request}).ok());
                auto status = wait(engine, owner.id);
                ASSERT_EQ(status.s, COMPLETED);
                EXPECT_EQ(status.transferred_bytes, length);
                ASSERT_TRUE(owner.release().ok());
                if (op == Request::WRITE) {
                    ASSERT_TRUE(peer.command(target, true, seed, length));
                } else {
                    EXPECT_EQ(0, std::memcmp(bytes, expected.data(), length));
                }
            }
        }
    }
    EXPECT_TRUE(buffers.cleanup().ok());
    EXPECT_EQ(peer.finish(), 0);
}

TEST_P(TpuAdapterProcessTest, CachedPeerExitThenNewPeerRestoresTransfers) {
    Buffers buffers(/*device=*/0);
    TransferEngineImpl engine(config(GetParam()));
    ASSERT_TRUE(engine.available());
    TransferEngineImplTestPeer::smallStagePool(engine);
    ASSERT_TRUE(buffers.attach(engine).ok());
    constexpr size_t length = 65539;
    auto* local_bytes = buffers.bytes(kTpu) + kOffset;
    auto* local = static_cast<uint8_t*>(buffers.addr(kTpu)) + kOffset;
    int seed = 0;
    auto transfer = [&](Peer& peer, SegmentID remote, int target,
                        Request::OpCode op, bool alive) {
        auto expected = pattern(length, ++seed);
        auto initial = op == Request::WRITE ? expected : pattern(length, 0);
        if (alive) {
            ASSERT_TRUE(peer.command(target, false,
                                     op == Request::READ ? seed : 0, length));
        }
        std::memcpy(local_bytes, initial.data(), length);
        Request request{};
        request.opcode = op;
        request.source = local;
        request.target_id = remote;
        request.target_offset = peer.hello.addresses[target] + kOffset;
        request.length = length;
        BatchGuard owner(engine);
        ASSERT_NE(owner.id, 0u);
        ASSERT_TRUE(engine.submitTransfer(owner.id, {request}).ok());
        auto status = wait(engine, owner.id);
        ASSERT_EQ(status.s, alive ? COMPLETED : FAILED);
        EXPECT_EQ(status.transferred_bytes, alive ? length : 0);
        ASSERT_TRUE(owner.release().ok());
        EXPECT_EQ(engine.aliveBatchCountForTest(), 0u);
        if (alive && op == Request::WRITE) {
            ASSERT_TRUE(peer.command(target, true, seed, length));
        }
        EXPECT_EQ(0, std::memcmp(local_bytes,
                                 (alive ? expected : initial).data(), length));
    };
    // Keep the initiator alive across peer crashes and reuse its cached
    // segment handle; a replacement peer must restore TPU transfers.
    for (auto op : {Request::WRITE, Request::READ}) {
        SCOPED_TRACE(op);
        Peer peer;
        ASSERT_TRUE(peer.start(GetParam()));
        SegmentID remote;
        ASSERT_TRUE(engine.openSegment(remote, peer.hello.segment).ok());
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, kHost, op, true));
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, kTpu, op, true));
        ASSERT_TRUE(peer.crash());
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, kHost, op, false));
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, kTpu, op, false));
        Peer replacement;
        ASSERT_TRUE(replacement.start(GetParam()));
        ASSERT_TRUE(engine.openSegment(remote, replacement.hello.segment).ok());
        ASSERT_NO_FATAL_FAILURE(transfer(replacement, remote, kHost, op, true));
        ASSERT_NO_FATAL_FAILURE(transfer(replacement, remote, kTpu, op, true));
        EXPECT_EQ(replacement.finish(), 0);
    }
    EXPECT_TRUE(buffers.cleanup().ok());
}

INSTANTIATE_TEST_SUITE_P(
    SelectionAndDispatchModes, TpuAdapterProcessTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<Params>& info) {
        return std::string(std::get<0>(info.param) ? "Legacy" : "Selector") +
               (std::get<1>(info.param) ? "RuntimeQueue" : "Direct");
    });

}  // namespace
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    if (argc == 4 && std::strcmp(argv[1], "--tpu-peer") == 0)
        return mooncake::tent::peerMain({argv[2][0] == '1', argv[3][0] == '1'});
    signal(SIGPIPE, SIG_IGN);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
