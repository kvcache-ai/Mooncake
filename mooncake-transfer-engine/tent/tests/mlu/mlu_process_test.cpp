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

// Two exec'd processes, separate MLU allocations and loopback TCP. Pipes carry
// only test control messages; all payload bytes must pass through TENT.
#include <cnrt.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/proxy_manager.h"

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
constexpr size_t kOffset = 17;

using Params = std::tuple<bool, bool, bool>;  // HP TCP, legacy, runtime queue

std::shared_ptr<Config> config(Params params = {}) {
    auto c = std::make_shared<Config>();
    c->set("metadata_type", "p2p");
    c->set("metadata_servers", "");
    c->set("rpc_server_hostname", "127.0.0.1");
    c->set("rpc_server_port", "0");
    c->set("log_level", "warning");
    c->set("transports/tcp/enable", !std::get<0>(params));
    c->set("transports/tcp/max_retry_count", 0);
    c->set("transports/rdma/enable", false);
    c->set("transports/hp_tcp/enable", std::get<0>(params));
    c->set("use_legacy_transport_selection", std::get<1>(params));
    c->set("enable_runtime_queue", std::get<2>(params));
    c->set("max_failover_attempts", 1);
    c->set("transports/shm/enable", false);
    c->set("transports/io_uring/enable", false);
    c->set("transports/mlu/enable", true);
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

// Caller allocations are not owned by the engine destructor.
class Buffers {
   public:
    explicit Buffers(TransferEngineImpl& engine) : engine_(engine) {}
    ~Buffers() { cleanup(); }
    void*& operator[](size_t i) { return addresses_[i]; }
    Status allocate() {
        for (int i = 0; i != 2; ++i) {
            MemoryOptions opts;
            opts.location = i ? "mlu:0" : "cpu:0";
            CHECK_STATUS(
                engine_.allocateLocalMemory(&addresses_[i], kSize, opts));
            // A device-only registration must still select host staging.
            opts.type = i ? MLU : UNSPEC;
            CHECK_STATUS(
                engine_.registerLocalMemory({addresses_[i]}, {kSize}, opts));
            registered_[i] = true;
        }
        return Status::OK();
    }
    Status cleanup() {
        CHECK_STATUS(TransferEngineImplTestPeer::stopStaging(engine_));
        for (int i = 0; i != 2; ++i) {
            if (registered_[i]) {
                CHECK_STATUS(engine_.unregisterLocalMemory(addresses_[i]));
                registered_[i] = false;
            }
            if (addresses_[i]) {
                CHECK_STATUS(engine_.freeLocalMemory(addresses_[i]));
                addresses_[i] = nullptr;
            }
        }
        if (engine_.aliveBatchCountForTest() ||
            engine_.deferredStageTeardownBatchCountForTest() ||
            !TransferEngineImplTestPeer::empty(engine_))
            return Status::InternalError("MLU process resources remain live");
        return Status::OK();
    }

   private:
    TransferEngineImpl& engine_;
    std::array<void*, 2> addresses_{};
    std::array<bool, 2> registered_{};
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
    TransferEngineImpl engine(config(params));
    if (!engine.available()) return 1;
    TransferEngineImplTestPeer::smallStagePool(engine);
    Buffers addresses(engine);
    if (!addresses.allocate().ok()) return 2;
    Hello hello;
    auto name = engine.getSegmentName();
    if (name.size() >= sizeof(hello.segment)) return 3;
    std::memcpy(hello.segment, name.c_str(), name.size() + 1);
    for (int i = 0; i != 2; ++i)
        hello.addresses[i] = reinterpret_cast<uint64_t>(addresses[i]);
    if (!io(STDOUT_FILENO, &hello, sizeof(hello), true)) return 4;
    Command command;
    while (io(STDIN_FILENO, &command, sizeof(command), false)) {
        if (command.buffer == -1) return addresses.cleanup().ok() ? 0 : 8;
        if (command.buffer < 0 || command.buffer > 1 ||
            command.length > kSize - kOffset)
            return 5;
        auto data = pattern(command.length, command.seed);
        auto* ptr = static_cast<uint8_t*>(addresses[command.buffer]) + kOffset;
        int result = 0;
        auto& platform = Platform::getLoader();
        if (command.verify) {
            std::vector<uint8_t> actual(command.length);
            if (!platform.copy(actual.data(), ptr, command.length).ok() ||
                actual != data)
                result = 1;
        } else if (!platform.copy(ptr, data.data(), command.length).ok()) {
            result = 2;
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
        char executable[] = "/proc/self/exe", option[] = "--mlu-peer";
        char hp[] = "0", legacy[] = "0", queue[] = "0";
        hp[0] += std::get<0>(params);
        legacy[0] += std::get<1>(params);
        queue[0] += std::get<2>(params);
        char* argv[] = {executable, option, hp, legacy, queue, nullptr};
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

TEST(MluProcessTest, TcpReadWriteWithIndependentDeviceAllocations) {
    unsigned int count = 0;
    ASSERT_EQ(cnrtGetDeviceCount(&count), cnrtSuccess);
    if (count == 0) GTEST_SKIP() << "No MLU device visible";
    Peer peer;
    ASSERT_TRUE(peer.start());
    TransferEngineImpl engine(config());
    ASSERT_TRUE(engine.available());
    TransferEngineImplTestPeer::smallStagePool(engine);
    Buffers addresses(engine);
    ASSERT_TRUE(addresses.allocate().ok());
    SegmentID remote;
    ASSERT_TRUE(engine.openSegment(remote, peer.hello.segment).ok());
    auto& platform = Platform::getLoader();
    int seed = 0;
    for (auto [local, target] :
         {std::pair{1, 1}, std::pair{1, 0}, std::pair{0, 1}}) {
        for (auto length : {size_t(1), size_t(65539), (8UL << 20) + 37}) {
            for (auto op : {Request::READ, Request::WRITE}) {
                SCOPED_TRACE(::testing::Message()
                             << local << " -> " << target
                             << " length=" << length << " op=" << op);
                auto expected = pattern(length, ++seed);
                auto* ptr = static_cast<uint8_t*>(addresses[local]) + kOffset;
                ASSERT_TRUE(peer.command(
                    target, false, op == Request::READ ? seed : 0, length));
                auto initial =
                    op == Request::WRITE ? expected : pattern(length, 0);
                ASSERT_TRUE(platform.copy(ptr, initial.data(), length).ok());
                Request request{};
                request.opcode = op;
                request.source = ptr;
                request.target_id = remote;
                request.target_offset = peer.hello.addresses[target] + kOffset;
                request.length = length;
                auto route = TransferEngineImplTestPeer::route(engine, request);
                ASSERT_EQ(route.transport, TCP);
                ASSERT_EQ(route.staging_params.size(), 3u);
                BatchGuard owner(engine);
                auto batch = owner.id;
                ASSERT_NE(batch, 0u);
                ASSERT_TRUE(engine.submitTransfer(batch, {request}).ok());
                TransferStatus status{};
                auto deadline =
                    std::chrono::steady_clock::now() + std::chrono::seconds(20);
                do {
                    ASSERT_TRUE(engine.progressBatch(batch, status).ok());
                    if (status.s != PENDING) break;
                    std::this_thread::yield();
                } while (std::chrono::steady_clock::now() < deadline);
                ASSERT_EQ(status.s, COMPLETED);
                EXPECT_EQ(status.transferred_bytes, length);
                ASSERT_TRUE(owner.release().ok());
                if (op == Request::WRITE) {
                    ASSERT_TRUE(peer.command(target, true, seed, length));
                } else {
                    std::vector<uint8_t> actual(length);
                    ASSERT_TRUE(platform.copy(actual.data(), ptr, length).ok());
                    EXPECT_EQ(actual, expected);
                }
            }
        }
    }
    EXPECT_TRUE(addresses.cleanup().ok());
    EXPECT_EQ(peer.finish(), 0);
}

class MluPeerRecoveryTest : public ::testing::TestWithParam<Params> {};

TEST_P(MluPeerRecoveryTest, CachedPeerExitThenNewPeerRestoresTransfers) {
    unsigned int count = 0;
    ASSERT_EQ(cnrtGetDeviceCount(&count), cnrtSuccess);
    if (count == 0) GTEST_SKIP() << "No MLU device visible";
    TransferEngineImpl engine(config(GetParam()));
    ASSERT_TRUE(engine.available());
    TransferEngineImplTestPeer::smallStagePool(engine);
    Buffers addresses(engine);
    ASSERT_TRUE(addresses.allocate().ok());
    auto& platform = Platform::getLoader();
    constexpr size_t length = 65539;
    auto* local = static_cast<uint8_t*>(addresses[1]) + kOffset;
    int seed = 0;
    auto transfer = [&](Peer& peer, SegmentID remote, int target,
                        Request::OpCode op, bool alive) {
        auto expected = pattern(length, ++seed);
        auto initial = op == Request::WRITE ? expected : pattern(length, 0);
        if (alive)
            ASSERT_TRUE(peer.command(target, false,
                                     op == Request::READ ? seed : 0, length));
        ASSERT_TRUE(platform.copy(local, initial.data(), length).ok());
        Request request{};
        request.opcode = op;
        request.source = local;
        request.target_id = remote;
        request.target_offset = peer.hello.addresses[target] + kOffset;
        request.length = length;
        BatchGuard owner(engine);
        ASSERT_NE(owner.id, 0u);
        ASSERT_TRUE(engine.submitTransfer(owner.id, {request}).ok());
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            auto result = engine.progressBatch(owner.id, status);
            ASSERT_TRUE(result.ok()) << result.ToString();
            if (status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        ASSERT_EQ(status.s, alive ? COMPLETED : FAILED);
        EXPECT_EQ(status.transferred_bytes, alive ? length : 0);
        ASSERT_TRUE(owner.release().ok());
        EXPECT_EQ(engine.aliveBatchCountForTest(), 0u);
        if (alive && op == Request::WRITE)
            ASSERT_TRUE(peer.command(target, true, seed, length));
        std::vector<uint8_t> actual(length);
        ASSERT_TRUE(platform.copy(actual.data(), local, length).ok());
        EXPECT_EQ(actual, alive ? expected : initial);
    };
    // Keep the initiator alive across crashes. Warm host and MLU routes before
    // killing only the child owned by this test, then reuse the cached segment.
    for (auto op : {Request::WRITE, Request::READ}) {
        SCOPED_TRACE(op);
        Peer peer;
        ASSERT_TRUE(peer.start(GetParam()));
        SegmentID remote;
        ASSERT_TRUE(engine.openSegment(remote, peer.hello.segment).ok());
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, 0, op, true));
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, 1, op, true));
        ASSERT_TRUE(peer.crash());
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, 0, op, false));
        ASSERT_NO_FATAL_FAILURE(transfer(peer, remote, 1, op, false));
        Peer replacement;
        ASSERT_TRUE(replacement.start(GetParam()));
        ASSERT_TRUE(engine.openSegment(remote, replacement.hello.segment).ok());
        ASSERT_NO_FATAL_FAILURE(transfer(replacement, remote, 0, op, true));
        ASSERT_NO_FATAL_FAILURE(transfer(replacement, remote, 1, op, true));
        EXPECT_EQ(replacement.finish(), 0);
    }
    EXPECT_TRUE(addresses.cleanup().ok());
}

INSTANTIATE_TEST_SUITE_P(NetworkAndDispatchModes, MluPeerRecoveryTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    if (argc == 5 && std::strcmp(argv[1], "--mlu-peer") == 0)
        return mooncake::tent::peerMain(
            {argv[2][0] == '1', argv[3][0] == '1', argv[4][0] == '1'});
    signal(SIGPIPE, SIG_IGN);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
