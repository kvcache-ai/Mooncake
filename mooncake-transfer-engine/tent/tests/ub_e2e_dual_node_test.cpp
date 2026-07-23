// Copyright 2025 KVCache.AI
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
//
// UB + TENT dual-node end-to-end test (requires real Kunpeng URMA hardware).
//
// Usage (two machines, shared etcd):
//
//   # Machine A — server (holds and exposes data)
//   ./tent_ub_e2e_dual_node_test --role=server --segment_name=node_a \
//       --data_size=1048576
//
//   # Machine B — client (reads / writes remote data)
//   ./tent_ub_e2e_dual_node_test --role=client --remote_segment=node_a \
//       --data_size=1048576 --operation=write   # or --operation=read
//
// The test writes a known pattern, reads it back, and verifies data integrity.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

DEFINE_string(role, "",
              "Role: 'server' (holds data and waits) or 'client' (initiates "
              "transfer and verifies)");
DEFINE_string(segment_name, "", "Local segment name (used by the server)");
DEFINE_string(remote_segment, "", "Remote segment name to open as client");
DEFINE_string(transport_config, "",
              "Path to a TENT JSON config file (optional).  When omitted a "
              "default config with UB enabled is built automatically.");
DEFINE_int32(data_size, 1 * 1024 * 1024,
             "Data size in bytes for the test buffer (default 1 MiB)");
DEFINE_string(operation, "write",
              "Operation for the client: 'write' or 'read'");

namespace {

std::atomic<bool> running{true};

void signalHandler(int /*signum*/) { running.store(false); }

void setupSignalHandler() {
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

std::shared_ptr<mooncake::tent::Config> buildDefaultUbConfig() {
    auto conf = std::make_shared<mooncake::tent::Config>();
    std::string cfg_json = R"({
        "transports": {
            "tcp": { "enable": false },
            "rdma": { "enable": false },
            "ub": { "enable": true }
        }
    })";
    auto status = conf->load(cfg_json);
    LOG_ASSERT(status.ok())
        << "Failed to load built-in UB config: " << status.ToString();
    return conf;
}

int runServer() {
    setupSignalHandler();

    std::shared_ptr<mooncake::tent::Config> config;
    if (!FLAGS_transport_config.empty()) {
        config = std::make_shared<mooncake::tent::Config>();
        auto s = config->loadFile(FLAGS_transport_config);
        LOG_ASSERT(s.ok()) << "Failed to load config: " << s.ToString();
    } else {
        config = buildDefaultUbConfig();
    }

    auto engine = std::make_unique<mooncake::tent::TransferEngine>(config);
    LOG_ASSERT(engine->available())
        << "TENT TransferEngine not available (check config / transports)";

    std::string seg_name = FLAGS_segment_name;
    LOG_ASSERT(!seg_name.empty())
        << "Server requires --segment_name (e.g. --segment_name=node_a)";

    // Allocate page-aligned buffer for UB URMA registration.
    const size_t buf_size = static_cast<size_t>(FLAGS_data_size);
    void* buf = nullptr;
    LOG_ASSERT(posix_memalign(&buf, 4096, buf_size) == 0)
        << "Failed to allocate page-aligned buffer (" << buf_size << " bytes)";
    std::memset(buf, 0xAB, buf_size);
    LOG(INFO) << "Server: allocated " << buf_size << " bytes at " << buf;

    auto reg_s = engine->registerLocalMemory(buf, buf_size,
                                             mooncake::tent::kGlobalReadWrite);
    LOG_ASSERT(reg_s.ok()) << "Server: registerLocalMemory failed: "
                           << reg_s.ToString();
    LOG(INFO) << "Server: registered local memory";

    LOG(INFO) << "Server: segment '" << seg_name
              << "' is ready.  Press Ctrl-C to stop.";
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    engine->unregisterLocalMemory(buf, buf_size);
    free(buf);
    LOG(INFO) << "Server: shutdown complete";
    return 0;
}

int runClient() {
    std::shared_ptr<mooncake::tent::Config> config;
    if (!FLAGS_transport_config.empty()) {
        config = std::make_shared<mooncake::tent::Config>();
        auto s = config->loadFile(FLAGS_transport_config);
        LOG_ASSERT(s.ok()) << "Failed to load config: " << s.ToString();
    } else {
        config = buildDefaultUbConfig();
    }

    auto engine = std::make_unique<mooncake::tent::TransferEngine>(config);
    LOG_ASSERT(engine->available())
        << "TENT TransferEngine not available (check config / transports)";

    std::string remote = FLAGS_remote_segment;
    LOG_ASSERT(!remote.empty())
        << "Client requires --remote_segment (e.g. --remote_segment=node_a)";

    // Allocate page-aligned buffer.
    const size_t buf_size = static_cast<size_t>(FLAGS_data_size);
    void* buf = nullptr;
    LOG_ASSERT(posix_memalign(&buf, 4096, buf_size) == 0)
        << "Failed to allocate page-aligned buffer (" << buf_size << " bytes)";
    std::memset(buf, 0x00, buf_size);
    LOG(INFO) << "Client: allocated " << buf_size << " bytes at " << buf;

    auto reg_s = engine->registerLocalMemory(buf, buf_size,
                                             mooncake::tent::kGlobalReadWrite);
    LOG_ASSERT(reg_s.ok()) << "Client: registerLocalMemory failed: "
                           << reg_s.ToString();
    LOG(INFO) << "Client: registered local memory";

    // Open the remote segment.
    mooncake::tent::SegmentID seg_id = 0;
    auto open_s = engine->openSegment(seg_id, remote);
    LOG_ASSERT(open_s.ok()) << "Client: openSegment('" << remote
                            << "') failed: " << open_s.ToString();
    LOG(INFO) << "Client: opened remote segment '" << remote
              << "' → segment_id " << seg_id;

    // Fetch segment info to get remote base address (buffer at index 0).
    mooncake::tent::SegmentInfo info;
    auto info_s = engine->getSegmentInfo(seg_id, info);
    LOG_ASSERT(info_s.ok())
        << "Client: getSegmentInfo failed: " << info_s.ToString();
    LOG(INFO) << "Client: remote segment has " << info.buffers.size()
              << " buffer(s)";
    if (info.buffers.empty()) {
        LOG(ERROR) << "Client: remote segment has no buffers";
        engine->unregisterLocalMemory(buf, buf_size);
        free(buf);
        return 1;
    }
    uint64_t remote_addr = info.buffers[0].base;
    LOG(INFO) << "Client: remote buffer base = " << remote_addr;

    // --- WRITE test ---
    std::string op = FLAGS_operation;
    if (op == "write") {
        // Fill local buffer with a known pattern.
        for (size_t i = 0; i < buf_size; ++i)
            static_cast<uint8_t*>(buf)[i] = static_cast<uint8_t>(i & 0xFF);

        auto batch_id = engine->allocateBatch(1);
        LOG_ASSERT(batch_id != 0) << "Client: allocateBatch failed";

        mooncake::tent::Request req;
        req.opcode = mooncake::tent::Request::WRITE;
        req.source = buf;
        req.target_id = seg_id;
        req.target_offset = remote_addr;
        req.length = buf_size;

        auto sub_s = engine->submitTransfer(batch_id, {req});
        LOG_ASSERT(sub_s.ok())
            << "Client: submitTransfer(WRITE) failed: " << sub_s.ToString();

        // Poll until completed.
        mooncake::tent::TransferStatus ts;
        bool done = false;
        for (int i = 0; i < 1000 && !done; ++i) {
            auto gs = engine->getTransferStatus(batch_id, ts);
            LOG_ASSERT(gs.ok())
                << "Client: getTransferStatus failed: " << gs.ToString();
            if (ts.s == mooncake::tent::COMPLETED) {
                done = true;
            } else if (ts.s == mooncake::tent::FAILED ||
                       ts.s == mooncake::tent::TIMEOUT) {
                LOG(ERROR) << "Client: WRITE transfer failed, status="
                           << static_cast<int>(ts.s);
                engine->freeBatch(batch_id);
                engine->unregisterLocalMemory(buf, buf_size);
                free(buf);
                return 1;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        LOG_ASSERT(done) << "Client: WRITE did not complete within 10s";

        auto free_s = engine->freeBatch(batch_id);
        LOG_ASSERT(free_s.ok())
            << "Client: freeBatch failed: " << free_s.ToString();
        LOG(INFO) << "Client: WRITE " << buf_size << " bytes → COMPLETED ✓";
    }

    // --- READ-back-and-verify test ---
    {
        std::memset(buf, 0x00, buf_size);
        auto batch_id = engine->allocateBatch(1);
        LOG_ASSERT(batch_id != 0) << "Client: allocateBatch(READ) failed";

        mooncake::tent::Request req;
        req.opcode = mooncake::tent::Request::READ;
        req.source = buf;
        req.target_id = seg_id;
        req.target_offset = remote_addr;
        req.length = buf_size;

        auto sub_s = engine->submitTransfer(batch_id, {req});
        LOG_ASSERT(sub_s.ok())
            << "Client: submitTransfer(READ) failed: " << sub_s.ToString();

        mooncake::tent::TransferStatus ts;
        bool done = false;
        for (int i = 0; i < 1000 && !done; ++i) {
            auto gs = engine->getTransferStatus(batch_id, ts);
            LOG_ASSERT(gs.ok())
                << "Client: getTransferStatus(READ) failed: " << gs.ToString();
            if (ts.s == mooncake::tent::COMPLETED)
                done = true;
            else if (ts.s == mooncake::tent::FAILED ||
                     ts.s == mooncake::tent::TIMEOUT) {
                LOG(ERROR) << "Client: READ transfer failed, status="
                           << static_cast<int>(ts.s);
                engine->freeBatch(batch_id);
                engine->unregisterLocalMemory(buf, buf_size);
                free(buf);
                return 1;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        LOG_ASSERT(done) << "Client: READ did not complete within 10s";

        engine->freeBatch(batch_id);
        LOG(INFO) << "Client: READ " << buf_size << " bytes → COMPLETED ✓";
    }

    // Data integrity: if we wrote first, verify the content.
    if (op == "write") {
        bool ok = true;
        size_t first_mismatch = 0;
        uint8_t expected = 0, got = 0;
        for (size_t i = 0; i < buf_size && ok; ++i) {
            expected = static_cast<uint8_t>(i & 0xFF);
            got = static_cast<uint8_t*>(buf)[i];
            if (got != expected) {
                ok = false;
                first_mismatch = i;
            }
        }
        if (!ok) {
            LOG(ERROR) << "Client: data MISMATCH at offset " << first_mismatch
                       << " (expected 0x" << std::hex
                       << static_cast<int>(expected) << ", got 0x"
                       << static_cast<int>(got) << std::dec << ")";
            engine->unregisterLocalMemory(buf, buf_size);
            free(buf);
            return 1;
        }
        LOG(INFO) << "Client: data integrity VERIFIED ✓ (all " << buf_size
                  << " bytes match)";
    }

    engine->unregisterLocalMemory(buf, buf_size);
    free(buf);
    LOG(INFO) << "Client: test PASSED";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "UB + TENT dual-node e2e test.\n\n"
        "Server:  --role=server  --segment_name=<name>\n"
        "Client:  --role=client  --remote_segment=<name>\n\n"
        "Both nodes must share a TENT metadata backend (etcd or built-in "
        "RPC).  Use --transport_config to point at a JSON file, or omit it "
        "to use the built-in UB-only config.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    if (FLAGS_role == "server" || FLAGS_role.empty()) {
        return runServer();
    } else if (FLAGS_role == "client") {
        return runClient();
    }

    LOG(ERROR) << "Unknown --role '" << FLAGS_role
               << "'.  Use 'server' or 'client'.";
    return 1;
}
