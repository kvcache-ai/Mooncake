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

// A host-only process initiates transfers into and out of TPU memory that
// another process owns. Only the owner can copy between its HBM and host
// memory, so the initiator must stage the TPU side remotely, without a TPU
// transport or PJRT adapter of its own.
//
// The platform is chosen at build time and shared by the whole process, so
// the two roles run in separate processes, and the peer always comes from a
// USE_TPU build:
//   * The peer (--tpu-peer) owns a TPU buffer behind the mock PJRT adapter.
//     Pipes carry only test control messages; all payload bytes pass through
//     TENT.
//   * The initiator is the test itself. Built without USE_TPU it is a
//     genuinely host-only process: configure that build with
//     -DTENT_TPU_MOCK_PEER=<this binary from a USE_TPU build>. A USE_TPU
//     build starts its own binary as the peer, and its initiator loads no
//     adapter and disables its TPU transport.

#include <dlfcn.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

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
    static bool hasTransport(tent::TransferEngineImpl& engine,
                             tent::TransportType type) {
        return engine.transport_list_[type] != nullptr;
    }
};

namespace tent {
namespace {

using Params = std::tuple<bool, bool>;  // HP TCP instead of TCP, legacy mode
constexpr size_t kChunk = 1UL << 20;    // smallStagePool() slot, both sides
// Longer than the four-slot staging pool, and not chunk aligned.
constexpr size_t kLength = 4 * kChunk + 4096 + 3;
constexpr size_t kOffset = 4096 + 32;
constexpr size_t kSize = kOffset + kLength + 4096;
constexpr uint8_t kGuard = 0xEE;
constexpr int kFill = 0, kVerify = 1;  // Peer commands.

std::shared_ptr<Config> config(Params params, bool tpu) {
    auto c = std::make_shared<Config>();
    c->set("metadata_type", "p2p");
    c->set("metadata_servers", "");
    c->set("rpc_server_hostname", "127.0.0.1");
    c->set("rpc_server_port", "0");
    c->set("log_level", "warning");
    c->set("transports/tcp/enable", !std::get<0>(params));
    c->set("transports/hp_tcp/enable", std::get<0>(params));
    c->set("transports/rdma/enable", false);
    c->set("transports/shm/enable", false);
    c->set("transports/io_uring/enable", false);
    c->set("transports/mpcomm/enable", false);
    c->set("transports/tpu/enable", tpu);
    c->set("use_legacy_transport_selection", std::get<1>(params));
    return c;
}

std::vector<uint8_t> pattern(size_t size, int seed) {
    std::vector<uint8_t> result(size);
    for (size_t i = 0; i < size; ++i) result[i] = (i * 13 + seed) % 251;
    return result;
}

void fill(uint8_t* bytes, int seed, size_t offset, size_t length) {
    std::memset(bytes, kGuard, kSize);
    auto data = pattern(length, seed);
    std::memcpy(bytes + offset, data.data(), length);
}

bool matches(const uint8_t* bytes, int seed, size_t offset, size_t length) {
    auto data = pattern(length, seed);
    if (std::memcmp(bytes + offset, data.data(), length) != 0) return false;
    for (size_t i = 0; i < kSize; ++i) {
        if ((i < offset || i >= offset + length) && bytes[i] != kGuard)
            return false;
    }
    return true;
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

struct Hello {
    char segment[256]{};
    uint64_t tpu = 0;
};
struct Command {
    int op = -1;  // kFill, kVerify, or -1 to clean up and exit.
    int seed = 0;
    size_t offset = 0;
    size_t length = 0;
};

int peerMain(Params params) {
#ifndef MOCK_TPU_PJRT_LIB
    (void)params;
    fprintf(stderr, "--tpu-peer needs a USE_TPU build\n");
    return 10;
#else
    ::setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, /*overwrite=*/1);
    void* mock = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
    if (!mock) return 11;
    auto register_device = reinterpret_cast<void* (*)(size_t, int)>(
        dlsym(mock, "mock_tpu_pjrt_register_device"));
    auto device_data = reinterpret_cast<void* (*)(const void*)>(
        dlsym(mock, "mock_tpu_pjrt_device_data"));
    if (!register_device || !device_data) return 12;
    auto conf = config(params, /*tpu=*/true);
    if (Platform::getLoader(conf).type() != "tpu") return 13;
    void* tpu = register_device(kSize, /*index=*/0);
    if (!tpu) return 14;
    TransferEngineImpl engine(conf);
    if (!engine.available()) return 1;
    TransferEngineImplTestPeer::smallStagePool(engine);
    MemoryOptions tpu_opts;  // Location left to TENT: "tpu:0".
    if (!engine.registerLocalMemory({tpu}, {kSize}, tpu_opts).ok()) return 2;
    auto* target = static_cast<uint8_t*>(device_data(tpu));
    Hello hello;
    auto name = engine.getSegmentName();
    if (name.size() >= sizeof(hello.segment)) return 3;
    std::memcpy(hello.segment, name.c_str(), name.size() + 1);
    hello.tpu = reinterpret_cast<uint64_t>(tpu);
    if (!io(STDOUT_FILENO, &hello, sizeof(hello), true)) return 4;
    Command command;
    while (io(STDIN_FILENO, &command, sizeof(command), false)) {
        if (command.op == -1) {
            bool ok = TransferEngineImplTestPeer::stopStaging(engine).ok();
            ok &= engine.unregisterLocalMemory(tpu).ok();
            return ok ? 0 : 8;
        }
        if (command.offset > kSize || command.length > kSize - command.offset)
            return 5;
        int result = 0;
        if (command.op == kFill)
            fill(target, command.seed, command.offset, command.length);
        else if (!matches(target, command.seed, command.offset, command.length))
            result = 1;
        if (!io(STDOUT_FILENO, &result, sizeof(result), true)) return 6;
    }
    return 7;
#endif
}

class Peer {
   public:
    ~Peer() {
        if (input_ >= 0) close(input_);
        if (output_ >= 0) close(output_);
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            waitpid(pid_, nullptr, 0);
        }
    }
    bool start(const std::string& executable, Params params) {
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
        std::string path = executable, option = "--tpu-peer";
        std::string hp_tcp = std::get<0>(params) ? "1" : "0";
        std::string legacy = std::get<1>(params) ? "1" : "0";
        char* argv[] = {path.data(), option.data(), hp_tcp.data(),
                        legacy.data(), nullptr};
        int error =
            posix_spawn(&pid_, path.c_str(), &actions, nullptr, argv, environ);
        posix_spawn_file_actions_destroy(&actions);
        close(in[0]);
        close(out[1]);
        input_ = in[1];
        output_ = out[0];
        return error == 0 && io(output_, &hello, sizeof(hello), false);
    }
    bool started() const { return pid_ > 0; }
    bool command(int op, int seed, size_t offset = 0, size_t length = 0) {
        Command cmd{op, seed, offset, length};
        int result = -1;
        return io(input_, &cmd, sizeof(cmd), true) &&
               io(output_, &result, sizeof(result), false) && result == 0;
    }
    int finish() {
        Command cmd;
        if (!io(input_, &cmd, sizeof(cmd), true)) return -1;
        int status = -1;
        if (waitpid(pid_, &status, 0) < 0) return -1;
        pid_ = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
    Hello hello;

   private:
    int input_ = -1, output_ = -1;
    pid_t pid_ = -1;
};

std::string peerExecutable() {
    if (const char* path = std::getenv("TENT_TPU_MOCK_PEER")) return path;
#ifdef MOCK_TPU_PJRT_LIB
    return "/proc/self/exe";
#else
    return "";
#endif
}

class TpuHostInitiatorTest : public ::testing::TestWithParam<Params> {
   protected:
    void SetUp() override {
        auto executable = peerExecutable();
        if (executable.empty())
            GTEST_SKIP() << "Set TENT_TPU_MOCK_PEER to "
                            "tent_tpu_host_initiator_test of a USE_TPU build";
        ASSERT_TRUE(peer_.start(executable, GetParam()));
        engine_ = std::make_unique<TransferEngineImpl>(
            config(GetParam(), /*tpu=*/false));
        ASSERT_TRUE(engine_->available());
        // Host-only: no TPU transport, and nothing local is TPU memory.
        ASSERT_FALSE(TransferEngineImplTestPeer::hasTransport(*engine_, TPU));
        ASSERT_NE(Platform::getLoader().getMemoryType(peerAddress()),
                  MTYPE_TPU);
        TransferEngineImplTestPeer::smallStagePool(*engine_);
        MemoryOptions opts;
        opts.location = "cpu:0";
        ASSERT_TRUE(engine_->allocateLocalMemory(&local_, kSize, opts).ok());
        ASSERT_TRUE(engine_->registerLocalMemory({local_}, {kSize}, opts).ok());
        ASSERT_TRUE(engine_->openSegment(remote_, peer_.hello.segment).ok());
    }

    void TearDown() override {
        if (engine_) {
            EXPECT_TRUE(TransferEngineImplTestPeer::stopStaging(*engine_).ok());
            if (local_) {
                EXPECT_TRUE(engine_->unregisterLocalMemory(local_).ok());
                EXPECT_TRUE(engine_->freeLocalMemory(local_).ok());
            }
            engine_.reset();
        }
        if (peer_.started()) {
            EXPECT_EQ(peer_.finish(), 0);
        }
    }

    TransportType network() const {
        return std::get<0>(GetParam()) ? HP_TCP : TCP;
    }
    void* peerAddress() const {
        return reinterpret_cast<void*>(peer_.hello.tpu);
    }

    Request request(Request::OpCode opcode) {
        Request r{};
        r.opcode = opcode;
        r.source = static_cast<uint8_t*>(local_) + kOffset;
        r.target_id = remote_;
        r.target_offset = peer_.hello.tpu + kOffset;
        r.length = kLength;
        return r;
    }

    // Copies kLength bytes at kOffset between local host memory and the
    // peer's buffer. Source and destination start out different, and the
    // destination is checked against an independently computed pattern, with
    // every byte outside the range left untouched.
    void transfer(Request::OpCode opcode) {
        const int seed = ++seed_;
        auto* local = static_cast<uint8_t*>(local_);
        if (opcode == Request::WRITE) {
            fill(local, seed, kOffset, kLength);
            ASSERT_TRUE(peer_.command(kFill, seed));  // All guard.
        } else {
            fill(local, seed, 0, 0);
            ASSERT_TRUE(peer_.command(kFill, seed, kOffset, kLength));
        }
        auto batch = engine_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        ASSERT_TRUE(engine_->submitTransfer(batch, {request(opcode)}).ok());
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(20);
        do {
            auto s = engine_->progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        EXPECT_TRUE(engine_->freeBatch(batch).ok());
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
        if (opcode == Request::WRITE) {
            EXPECT_TRUE(peer_.command(kVerify, seed, kOffset, kLength));
        } else {
            EXPECT_TRUE(matches(local, seed, kOffset, kLength));
        }
    }

    // The peer's TPU side is staged remotely; this side needs no stage.
    void expectRemoteStagingOnly(Request::OpCode opcode) {
        auto route =
            TransferEngineImplTestPeer::route(*engine_, request(opcode));
        ASSERT_EQ(route.transport, network());
        ASSERT_EQ(route.staging_params.size(), 3u);
        EXPECT_FALSE(route.staging_params[0].empty());
        EXPECT_TRUE(route.staging_params[1].empty());
        EXPECT_FALSE(route.staging_params[2].empty());
    }

    Peer peer_;
    std::unique_ptr<TransferEngineImpl> engine_;
    void* local_ = nullptr;
    SegmentID remote_ = LOCAL_SEGMENT_ID;
    int seed_ = 0;
};

TEST_P(TpuHostInitiatorTest, WritesIntoRemoteTpuMemory) {
    ASSERT_NO_FATAL_FAILURE(expectRemoteStagingOnly(Request::WRITE));
    transfer(Request::WRITE);
}

TEST_P(TpuHostInitiatorTest, ReadsFromRemoteTpuMemory) {
    ASSERT_NO_FATAL_FAILURE(expectRemoteStagingOnly(Request::READ));
    transfer(Request::READ);
}

INSTANTIATE_TEST_SUITE_P(
    NetworkAndSelectionModes, TpuHostInitiatorTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<Params>& info) {
        return std::string(std::get<0>(info.param) ? "HpTcp" : "Tcp") +
               (std::get<1>(info.param) ? "Legacy" : "Selector");
    });

}  // namespace
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    if (argc == 4 && std::strcmp(argv[1], "--tpu-peer") == 0)
        return mooncake::tent::peerMain({argv[2][0] == '1', argv[3][0] == '1'});
    // The initiator must not load a PJRT adapter, whatever the environment
    // names. The peer sets its own adapter path.
    ::setenv("MC_TPU_PJRT_LIB", "/nonexistent/libmooncake_tpu_pjrt.so",
             /*overwrite=*/1);
    signal(SIGPIPE, SIG_IGN);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
