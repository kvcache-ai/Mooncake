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

#include <gtest/gtest.h>

#include <asio.hpp>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "error.h"
#include "tent/common/config.h"
#include "transfer_engine.h"
#include "transfer_engine_c.h"

namespace mooncake {
namespace {

class EnvVarGuard {
   public:
    EnvVarGuard(const char* name, const std::string& value) : name_(name) {
        const char* old = std::getenv(name);
        if (old) {
            old_value_ = old;
            had_value_ = true;
        }
        setenv(name, value.c_str(), 1);
    }

    ~EnvVarGuard() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string old_value_;
    bool had_value_ = false;
};

class TempConfigFile {
   public:
    explicit TempConfigFile(const std::string& content) {
        auto unique_name =
            "te-tent-compat-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".json";
        path_ = std::filesystem::temp_directory_path() / unique_name;
        std::ofstream ofs(path_);
        ofs << content;
    }

    ~TempConfigFile() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    std::string path() const { return path_.string(); }

   private:
    std::filesystem::path path_;
};

std::optional<uint16_t> reserveUnusedTcpPort() {
    try {
        asio::io_context io_context;
        asio::ip::tcp::acceptor acceptor(
            io_context,
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
        return acceptor.local_endpoint().port();
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

TEST(TransferEngineTentCompatTest,
     LegacyP2pHandshakeSelectsRpcIdentity) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto socket_probe = reserveUnusedTcpPort();
    if (!socket_probe) GTEST_SKIP() << "Loopback TCP sockets are not available";
    TransferEngine engine(/*auto_discover=*/false);

    ASSERT_TRUE(engine.isUsingTent());
    int init_rc = 1;
    try {
        init_rc = engine.init(P2PHANDSHAKE, "127.0.0.1:1",
                              "ignored.example.invalid", 1);
    } catch (const std::exception& e) {
        GTEST_SKIP() << "Loopback RPC server is not available: " << e.what();
    }
    ASSERT_EQ(init_rc, 0);
    EXPECT_NE(engine.getRpcPort(), 1);
    EXPECT_EQ(engine.getLocalIpAndPort(),
              "127.0.0.1:" + std::to_string(engine.getRpcPort()));
    EXPECT_EQ(engine.installTransport("rdma", nullptr), nullptr);
    EXPECT_EQ(engine.installTransport("definitely_unknown", nullptr),
              nullptr);
    EXPECT_EQ(engine.uninstallTransport("definitely_unknown"), 0);
}

TEST(TransferEngineTentCompatTest,
     LegacyConstructorFilterIsRetainedByTentFacade) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "topology": {"rdma_whitelist": ["mlx5_from_config"]},
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false,
                          {"mlx5_from_ctor", "mlx5_second"});
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    auto links = mooncake::tent::json::parse(engine.showLinks(true));
    ASSERT_EQ(links["mode"], "tent");
    ASSERT_EQ(links["rdma_whitelist"],
              std::vector<std::string>({"mlx5_from_ctor", "mlx5_second"}));
}

TEST(TransferEngineTentCompatTest,
     LegacySetterFilterAppliesBeforeTentInit) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    engine.setWhitelistFilters({"mlx5_from_setter"});
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    auto links = mooncake::tent::json::parse(engine.showLinks(true));
    ASSERT_EQ(links["rdma_whitelist"],
              std::vector<std::string>({"mlx5_from_setter"}));
}

TEST(TransferEngineTentCompatTest,
     LegacyMemoryRegistrationOptionsMapToTent) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    std::vector<uint8_t> buffer(4096);
    ASSERT_EQ(engine.registerLocalMemory(
                  buffer.data(), buffer.size(), "cpu:0",
                  /*remote_accessible=*/false, /*update_metadata=*/false),
              0);
    EXPECT_TRUE(engine.checkOverlap(buffer.data(), buffer.size()));
    EXPECT_TRUE(engine.checkOverlap(buffer.data() + 128, 16));

    uint64_t base = 0;
    EXPECT_TRUE(engine.getSegmentBufferBase(LOCAL_SEGMENT_ID, 0, base)
                    .IsInvalidArgument());

    ASSERT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
    EXPECT_FALSE(engine.checkOverlap(buffer.data(), buffer.size()));
}

TEST(TransferEngineTentCompatTest,
     LegacyMetadataAndTopologyApisFailSoftlyInTentMode) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    EXPECT_EQ(engine.getMetadata(), nullptr);
    EXPECT_NE(engine.getLocalTopology(), nullptr);
}

TEST(TransferEngineTentCompatTest,
     LegacyMemoryRegistrationRejectsInvalidAndOverlappingRanges) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    std::vector<uint8_t> buffer(4096);
    EXPECT_EQ(engine.registerLocalMemory(buffer.data(), 0, "cpu:0"),
              ERR_INVALID_ARGUMENT);
    ASSERT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(),
                                         "cpu:0"),
              0);
    EXPECT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(),
                                         "cpu:0"),
              ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(engine.registerLocalMemory(buffer.data() + 128, 16, "cpu:0"),
              ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data() + 1),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST(TransferEngineTentCompatTest, LegacyBatchRegistrationRejectsOverlap) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    std::vector<uint8_t> buffer(4096);
    std::vector<BufferEntry> entries = {
        {buffer.data(), 1024},
        {buffer.data() + 512, 1024},
    };
    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"),
              ERR_ADDRESS_OVERLAPPED);
}

TEST(TransferEngineTentCompatTest,
     LegacyApiFailsCleanlyBeforeTentInitialization) {
    EnvVarGuard use_tent("MC_USE_TENT", "1");

    TransferEngine engine(/*auto_discover=*/false);
    std::vector<uint8_t> buffer(4096);
    EXPECT_TRUE(engine.isUsingTent());
    EXPECT_EQ(engine.getRpcPort(), 0);
    EXPECT_TRUE(engine.getLocalIpAndPort().empty());
    EXPECT_EQ(engine.openSegment("missing"),
              static_cast<SegmentHandle>(-1));
    EXPECT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(),
                                         "cpu:0"),
              ERR_CONTEXT);
    EXPECT_EQ(engine.allocateBatchID(1), INVALID_BATCH_ID);

    TransferStatus transfer_status{Transport::COMPLETED, 123};
    auto status =
        engine.getTransferStatus(INVALID_BATCH_ID, 0, transfer_status);
    EXPECT_TRUE(status.IsContext());
    EXPECT_EQ(transfer_status.s, Transport::COMPLETED);
    EXPECT_EQ(transfer_status.transferred_bytes, 123);
}

TEST(TransferEngineTentCompatTest,
     LegacyStatusDoesNotMutateOutputForInvalidTentBatch) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto port = reserveUnusedTcpPort();
    if (!port) GTEST_SKIP() << "Loopback TCP sockets are not available";

    TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:1", "127.0.0.1",
                          *port),
              0);

    TransferStatus transfer_status{Transport::COMPLETED, 123};
    auto status =
        engine.getTransferStatus(INVALID_BATCH_ID, 0, transfer_status);
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_EQ(transfer_status.s, Transport::COMPLETED);
    EXPECT_EQ(transfer_status.transferred_bytes, 123);
}

TEST(TransferEngineTentCompatTest,
     LegacyApiCanReadTentSegmentBufferBase) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto target_port = reserveUnusedTcpPort();
    const auto initiator_port = reserveUnusedTcpPort();
    if (!target_port || !initiator_port) {
        GTEST_SKIP() << "Loopback TCP sockets are not available";
    }
    const std::string target_seed =
        "127.0.0.1:" + std::to_string(*target_port);
    const std::string initiator_seed =
        "127.0.0.1:" + std::to_string(*initiator_port);

    TransferEngine target(/*auto_discover=*/false);
    TransferEngine initiator(/*auto_discover=*/false);

    int target_init_rc = 1;
    int initiator_init_rc = 1;
    try {
        target_init_rc = target.init(P2PHANDSHAKE, target_seed, "127.0.0.1",
                                     *target_port);
        initiator_init_rc = initiator.init(
            P2PHANDSHAKE, initiator_seed, "127.0.0.1", *initiator_port);
    } catch (const std::exception& e) {
        GTEST_SKIP() << "Loopback RPC server is not available: " << e.what();
    }
    ASSERT_EQ(target_init_rc, 0);
    ASSERT_EQ(initiator_init_rc, 0);
    const auto target_name = target.getLocalIpAndPort();

    std::vector<uint8_t> target_buffer(4096);
    ASSERT_EQ(target.registerLocalMemory(target_buffer.data(),
                                         target_buffer.size(), "cpu:0"),
              0);

    auto handle = initiator.openSegment(target_name);
    ASSERT_NE(handle, static_cast<SegmentHandle>(-1));

    uint64_t remote_base = 0;
    auto status = initiator.getSegmentBufferBase(handle, 0, remote_base);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(remote_base, reinterpret_cast<uint64_t>(target_buffer.data()));

    EXPECT_TRUE(initiator.getSegmentBufferBase(handle, 1, remote_base)
                    .IsInvalidArgument());
}

TEST(TransferEngineTentCompatTest, CApiCanUseTentForLoopbackTransfer) {
    TempConfigFile conf_file(R"({
        "log_level": "warning",
        "transports": {
            "tcp": {"enable": true},
            "rdma": {"enable": false},
            "shm": {"enable": false},
            "io_uring": {"enable": false},
            "nvlink": {"enable": false},
            "mnnvl": {"enable": false},
            "gds": {"enable": false},
            "ascend_direct": {"enable": false}
        }
    })");
    EnvVarGuard use_tent("MC_USE_TENT", "1");
    EnvVarGuard conf("MC_TENT_CONF", conf_file.path());

    const auto target_port = reserveUnusedTcpPort();
    const auto initiator_port = reserveUnusedTcpPort();
    if (!target_port || !initiator_port) {
        GTEST_SKIP() << "Loopback TCP sockets are not available";
    }
    const std::string target_seed =
        "127.0.0.1:" + std::to_string(*target_port);
    const std::string initiator_seed =
        "127.0.0.1:" + std::to_string(*initiator_port);

    auto* target = ::createTransferEngine(P2PHANDSHAKE, target_seed.c_str(),
                                          "127.0.0.1", *target_port, 0);
    auto* initiator = ::createTransferEngine(
        P2PHANDSHAKE, initiator_seed.c_str(), "127.0.0.1", *initiator_port, 0);
    ASSERT_NE(target, nullptr);
    ASSERT_NE(initiator, nullptr);
    EXPECT_EQ(::discoverTopology(initiator), 0);

    char target_name_buf[128] = {};
    ASSERT_EQ(::getLocalIpAndPort(target, target_name_buf,
                                  sizeof(target_name_buf)),
              0);
    const std::string target_name = target_name_buf;
    auto c_segment = ::openSegment(initiator, target_name.c_str());
    ASSERT_GE(c_segment, 0);
    EXPECT_EQ(::closeSegment(initiator, c_segment), 0);

    std::vector<uint8_t> source(4096, 0x5a);
    std::vector<uint8_t> target_buffer(4096, 0);
    ASSERT_EQ(::registerLocalMemory(target, target_buffer.data(),
                                    target_buffer.size(), "cpu:0", 1),
              0);
    ASSERT_EQ(::registerLocalMemory(initiator, source.data(), source.size(),
                                    "cpu:0", 1),
              0);

    auto segment = ::openSegment(initiator, target_name.c_str());
    ASSERT_NE(segment, static_cast<segment_id_t>(-1));

    auto batch = ::allocateBatchID(initiator, 1);
    ASSERT_NE(batch, INVALID_BATCH);
    transfer_request_t request{};
    request.opcode = OPCODE_WRITE;
    request.source = source.data();
    request.target_id = segment;
    request.target_offset = reinterpret_cast<uint64_t>(target_buffer.data());
    request.length = source.size();
    ASSERT_EQ(::submitTransfer(initiator, batch, &request, 1), 0);

    transfer_status_t status{};
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(5);
    do {
        ASSERT_EQ(::getTransferStatus(initiator, batch, 0, &status), 0);
        if (status.status == STATUS_COMPLETED) break;
        ASSERT_NE(status.status, STATUS_FAILED);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);

    EXPECT_EQ(status.status, STATUS_COMPLETED);
    EXPECT_EQ(target_buffer, source);

    EXPECT_EQ(::freeBatchID(initiator, batch), 0);
    EXPECT_EQ(::closeSegment(initiator, segment), 0);
    EXPECT_EQ(::unregisterLocalMemory(initiator, source.data()), 0);
    EXPECT_EQ(::unregisterLocalMemory(target, target_buffer.data()), 0);
    ::destroyTransferEngine(initiator);
    ::destroyTransferEngine(target);
}

}  // namespace
}  // namespace mooncake
