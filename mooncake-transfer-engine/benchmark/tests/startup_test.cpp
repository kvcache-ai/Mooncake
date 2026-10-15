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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "te_backend.h"
#include "tent/common/types.h"

namespace mooncake::tent {
namespace {

class TEBenchStartupTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        ASSERT_EQ(setenv("MC_FORCE_TCP", "1", 1), 0);
        ASSERT_EQ(unsetenv("MC_USE_TENT"), 0);
        ASSERT_EQ(unsetenv("MC_USE_TEV1"), 0);
    }

    void SetUp() override {
        XferBenchConfig::loadFromFlags();
        XferBenchConfig::seg_name = "127.0.0.1";
        XferBenchConfig::seg_type = "DRAM";
        XferBenchConfig::metadata_type = "p2p";
        XferBenchConfig::xport_type = "tcp";
        XferBenchConfig::total_buffer_size = buffer_.size();

        reserved_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(reserved_fd_, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT_EQ(bind(reserved_fd_, reinterpret_cast<sockaddr*>(&address),
                       sizeof(address)),
                  0);
        socklen_t length = sizeof(address);
        ASSERT_EQ(getsockname(reserved_fd_,
                              reinterpret_cast<sockaddr*>(&address), &length),
                  0);
        missing_target_ =
            "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
    }

    void TearDown() override {
        target_.reset();
        if (reserved_fd_ >= 0) close(reserved_fd_);
    }

    void createTarget(bool register_buffer) {
        target_ = std::make_unique<mooncake::TransferEngine>(false);
        ASSERT_EQ(target_->init("P2PHANDSHAKE", "127.0.0.1"), 0);
        if (register_buffer) {
            ASSERT_EQ(target_->registerLocalMemory(buffer_.data(),
                                                   buffer_.size(), "cpu:0"),
                      0);
        }
        XferBenchConfig::target_seg_name = target_->getLocalIpAndPort();
    }

    void expectStartFailure(TEBenchRunner& runner) {
        const int rc = runner.startInitiator(1);
        if (rc == 0) {
            EXPECT_EQ(runner.stopInitiator(), 0);
        }
        EXPECT_EQ(rc, -1);
    }

    std::vector<char> buffer_ = std::vector<char>(65536);
    std::unique_ptr<mooncake::TransferEngine> target_;
    int reserved_fd_ = -1;
    std::string missing_target_;
};

TEST_F(TEBenchStartupTest, UnavailableTargetReturnsError) {
    XferBenchConfig::target_seg_name = missing_target_;
    TEBenchRunner runner;
    expectStartFailure(runner);
}

TEST_F(TEBenchStartupTest, EmptyTargetBuffersReturnError) {
    ASSERT_NO_FATAL_FAILURE(createTarget(false));
    TEBenchRunner runner;
    expectStartFailure(runner);
}

TEST_F(TEBenchStartupTest, LaterUnavailableTargetReturnsErrorAndCanRetry) {
    ASSERT_NO_FATAL_FAILURE(createTarget(true));
    const auto valid_target = XferBenchConfig::target_seg_name;
    XferBenchConfig::target_seg_name += "," + missing_target_;
    TEBenchRunner runner;
    expectStartFailure(runner);

    XferBenchConfig::target_seg_name = valid_target;
    ASSERT_EQ(runner.startInitiator(1), 0);
    EXPECT_EQ(runner.getTargetCount(), 1u);
    EXPECT_EQ(runner.stopInitiator(), 0);
}

TEST_F(TEBenchStartupTest, ValidTargetSupportsWriteAndRead) {
    ASSERT_NO_FATAL_FAILURE(createTarget(true));
    TEBenchRunner runner;
    ASSERT_EQ(runner.startInitiator(1), 0);
    const auto local = runner.getLocalBufferBase(0, 4096, 1);
    const auto remote = runner.getTargetBufferBase(0, 4096, 1);
    const auto segment = runner.getTargetSegmentId(0);
    std::vector<char> expected(4096);
    for (size_t i = 0; i < expected.size(); ++i)
        expected[i] = static_cast<char>(i * 17 + 3);
    std::memcpy(reinterpret_cast<void*>(local), expected.data(),
                expected.size());
    EXPECT_GE(runner.runSingleTransfer(local, segment, remote, 4096, 1, WRITE,
                                       0, IntentType::INTENT_UNSPEC),
              0);
    std::memset(reinterpret_cast<void*>(local), 0, expected.size());
    EXPECT_GE(runner.runSingleTransfer(local, segment, remote, 4096, 1, READ, 0,
                                       IntentType::INTENT_UNSPEC),
              0);
    EXPECT_EQ(std::memcmp(reinterpret_cast<void*>(local), expected.data(),
                          expected.size()),
              0);
    EXPECT_EQ(runner.stopInitiator(), 0);
}

}  // namespace
}  // namespace mooncake::tent
