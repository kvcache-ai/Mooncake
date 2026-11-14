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

#include "tent_backend.h"
#include "tent_transfer_result.h"

#include <cstdlib>
#include <fstream>
#include <unistd.h>

#include <gtest/gtest.h>

namespace mooncake::tent {

std::shared_ptr<Config> loadConfig();

namespace {

class TENTConfigTest : public testing::Test {
   protected:
    void SetUp() override {
        if (const char* env = std::getenv("MC_TENT_CONF")) saved_env_ = env;
        unsetenv("MC_TENT_CONF");
        saved_aware_ = XferBenchConfig::deadline_aware;
        saved_theta_ = XferBenchConfig::mlu_local_threshold;
        saved_arbitration_ = XferBenchConfig::deadline_bw_arbitration;
        saved_metadata_ = XferBenchConfig::metadata_type;
        saved_port_ = XferBenchConfig::rpc_server_port;
        XferBenchConfig::deadline_aware = false;
        XferBenchConfig::mlu_local_threshold = 0;
        XferBenchConfig::deadline_bw_arbitration = false;
        XferBenchConfig::rpc_server_port = 0;
        saved_transport_ = XferBenchConfig::xport_type;
        saved_segment_ = XferBenchConfig::seg_type;
        saved_mix_ = XferBenchConfig::seg_type_mix;
        XferBenchConfig::seg_type_mix.clear();
    }

    void TearDown() override {
        if (saved_env_)
            setenv("MC_TENT_CONF", saved_env_->c_str(), 1);
        else
            unsetenv("MC_TENT_CONF");
        if (!path_.empty()) std::remove(path_.c_str());
        XferBenchConfig::deadline_aware = saved_aware_;
        XferBenchConfig::mlu_local_threshold = saved_theta_;
        XferBenchConfig::deadline_bw_arbitration = saved_arbitration_;
        XferBenchConfig::rpc_server_port = saved_port_;
        XferBenchConfig::metadata_type = saved_metadata_;
        XferBenchConfig::xport_type = saved_transport_;
        XferBenchConfig::seg_type = saved_segment_;
        XferBenchConfig::seg_type_mix = saved_mix_;
    }

    void useConfigFile(bool aware, double theta, bool arbitration = false) {
        path_ = testing::TempDir() + "tebench-deadline-config-XXXXXX";
        const int fd = mkstemp(path_.data());
        ASSERT_GE(fd, 0);
        close(fd);
        json document = {
            {"enable_runtime_queue", aware},
            {"rpc_server_hostname", "127.0.0.1"},
            {"metadata_type", "p2p"},
            {"runtime_queue",
             {{"deadline_aware", aware},
              {"mlu_local_threshold", theta},
              {"max_dispatch_owners", 3}}},
            {"transports",
             {{"rdma",
               {{"enable", false}, {"deadline_bw_arbitration", arbitration}}},
              {"tcp", {{"enable", false}}},
              {"hp_tcp", {{"enable", false}}},
              {"shm", {{"enable", false}}},
              {"io_uring", {{"enable", false}}}}}};
        std::ofstream(path_) << document;
        ASSERT_EQ(setenv("MC_TENT_CONF", path_.c_str(), 1), 0);
        XferBenchConfig::xport_type.clear();
        XferBenchConfig::metadata_type = "p2p";
    }

    gflags::FlagSaver flag_saver_;
    std::optional<std::string> saved_env_;
    bool saved_aware_, saved_arbitration_;
    double saved_theta_;
    int saved_port_;
    std::string path_, saved_metadata_;
    std::string saved_transport_, saved_segment_, saved_mix_;
};

TEST_F(TENTConfigTest, NetworkSelectionRetainsMluHostCopies) {
    for (const auto* transport : {"tcp", "hp_tcp"}) {
        SCOPED_TRACE(transport);
        XferBenchConfig::xport_type = transport;
        for (const auto* mix : {"", " DRAM, VRAM "}) {
            SCOPED_TRACE(mix);
            XferBenchConfig::seg_type = "VRAM";
            XferBenchConfig::seg_type_mix = mix;
            auto config = loadConfig();
            EXPECT_TRUE(config->get(
                std::string("transports/") + transport + "/enable", false));
            EXPECT_FALSE(config->get("transports/rdma/enable", true));
#ifdef USE_MLU
            EXPECT_TRUE(config->get("transports/mlu/enable", false));
#else
            EXPECT_FALSE(config->contains("transports/mlu/enable"));
#endif
        }
    }
}

TEST_F(TENTConfigTest, DramOnlyDoesNotEnableMluHostCopies) {
    XferBenchConfig::xport_type = "tcp";
    XferBenchConfig::seg_type = "DRAM";
    EXPECT_FALSE(loadConfig()->contains("transports/mlu/enable"));
    XferBenchConfig::seg_type = "VRAM";
    XferBenchConfig::seg_type_mix = "dram";
    EXPECT_FALSE(loadConfig()->contains("transports/mlu/enable"));
}

TEST_F(TENTConfigTest, AutomaticSelectionLeavesTransportsUnset) {
    XferBenchConfig::xport_type.clear();
    XferBenchConfig::seg_type = "VRAM";
    XferBenchConfig::seg_type_mix = "dram,vram";
    auto config = loadConfig();
    EXPECT_FALSE(config->contains("transports/mlu/enable"));
    EXPECT_FALSE(config->contains("transports/tcp/enable"));
    EXPECT_FALSE(config->contains("transports/hp_tcp/enable"));
}

TEST_F(TENTConfigTest, DeadlineFlagsSurviveEngineConfigReload) {
    useConfigFile(false, 1.5);
    XferBenchConfig::deadline_aware = true;
    XferBenchConfig::mlu_local_threshold = 1.25;
    XferBenchConfig::deadline_bw_arbitration = true;
    auto config = loadConfig();
    TransferEngine engine(config);
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(config->get("enable_runtime_queue", false));
    EXPECT_TRUE(config->get("runtime_queue/deadline_aware", false));
    EXPECT_DOUBLE_EQ(config->get("runtime_queue/mlu_local_threshold", 0.0),
                     1.25);
    EXPECT_TRUE(config->get("transports/rdma/deadline_bw_arbitration", false));
    EXPECT_EQ(config->get("runtime_queue/max_dispatch_owners", 0), 3);
}

TEST_F(TENTConfigTest, DefaultFlagsRetainFileDeadlineSettings) {
    useConfigFile(true, 1.5, true);
    auto config = loadConfig();
    TransferEngine engine(config);
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(config->get("enable_runtime_queue", false));
    EXPECT_TRUE(config->get("runtime_queue/deadline_aware", false));
    EXPECT_DOUBLE_EQ(config->get("runtime_queue/mlu_local_threshold", 0.0),
                     1.5);
    EXPECT_TRUE(config->get("transports/rdma/deadline_bw_arbitration", false));
}

TEST_F(TENTConfigTest, ExplicitFalseDisablesFileDeadlineSettings) {
    useConfigFile(true, 1.5, true);
    ASSERT_FALSE(
        gflags::SetCommandLineOption("deadline_aware", "false").empty());
    ASSERT_FALSE(
        gflags::SetCommandLineOption("deadline_bw_arbitration", "false")
            .empty());
    auto config = loadConfig();
    TransferEngine engine(config);
    ASSERT_TRUE(engine.available());
    // Disabling deadline ordering leaves the file's general admission queue
    // and unrelated limits intact, but makes its positive threshold inactive.
    EXPECT_TRUE(config->get("enable_runtime_queue", false));
    EXPECT_FALSE(config->get("runtime_queue/deadline_aware", true));
    EXPECT_FALSE(config->get("transports/rdma/deadline_bw_arbitration", true));
    EXPECT_EQ(config->get("runtime_queue/max_dispatch_owners", 0), 3);
}

TEST_F(TENTConfigTest, ExplicitFalseOnlyOverridesTheSelectedFlag) {
    useConfigFile(true, 1.5, true);
    for (const std::string flag :
         {"deadline_aware", "deadline_bw_arbitration"}) {
        SCOPED_TRACE(flag);
        gflags::FlagSaver saved_flags;
        ASSERT_FALSE(
            gflags::SetCommandLineOption(flag.c_str(), "false").empty());
        auto config = loadConfig();
        TransferEngine engine(config);
        ASSERT_TRUE(engine.available());
        EXPECT_EQ(config->get("runtime_queue/deadline_aware", false),
                  flag != "deadline_aware");
        EXPECT_EQ(config->get("transports/rdma/deadline_bw_arbitration", false),
                  flag != "deadline_bw_arbitration");
    }
}

TEST_F(TENTConfigTest, ExplicitZeroDisablesFileThreshold) {
    useConfigFile(true, 1.5);
    ASSERT_FALSE(
        gflags::SetCommandLineOption("mlu_local_threshold", "0").empty());
    XferBenchConfig::deadline_aware = true;
    auto config = loadConfig();
    TransferEngine engine(config);
    ASSERT_TRUE(engine.available());
    EXPECT_DOUBLE_EQ(config->get("runtime_queue/mlu_local_threshold", -1.0),
                     0.0);
}

TEST(DeadlineTransferResultTest, RetainsCompletedTasksInCanceledBatch) {
    const std::vector<TransferStatusEnum> states = {
        TransferStatusEnum::COMPLETED, TransferStatusEnum::CANCELED,
        TransferStatusEnum::COMPLETED, TransferStatusEnum::CANCELED};
    const auto result = summarizeCanceledTransfer(
        states.size(), 4096, 25, [&](size_t index, TransferStatus& status) {
            status.s = states[index];
            return true;
        });
    EXPECT_EQ(result.completed, 2u);
    EXPECT_EQ(result.dropped, 2u);
    EXPECT_EQ(result.transferred_bytes, 8192u);
    EXPECT_EQ(result.completed_indices, (std::vector<size_t>{0, 2}));
    EXPECT_DOUBLE_EQ(result.duration_us, 25);
}

TEST(DeadlineTransferResultTest, FullyCanceledBatchHasNoLatencySample) {
    const auto result = summarizeCanceledTransfer(
        3, 4096, 25, [](size_t, TransferStatus& status) {
            status.s = TransferStatusEnum::CANCELED;
            return true;
        });
    EXPECT_EQ(result.completed, 0u);
    EXPECT_EQ(result.dropped, 3u);
    EXPECT_EQ(result.transferred_bytes, 0u);
    EXPECT_DOUBLE_EQ(result.duration_us, kTransferDropped);
}

TEST(DeadlineTransferResultTest, FailureAndPollErrorRemainFatal) {
    for (auto state :
         {TransferStatusEnum::FAILED, TransferStatusEnum::TIMEOUT,
          TransferStatusEnum::INVALID, TransferStatusEnum::PENDING}) {
        const auto result = summarizeCanceledTransfer(
            2, 4096, 25, [state](size_t index, TransferStatus& status) {
                status.s = index == 0 ? TransferStatusEnum::COMPLETED : state;
                return true;
            });
        EXPECT_EQ(result.duration_us, -1);
        EXPECT_EQ(result.completed, 0u);
        EXPECT_EQ(result.dropped, 0u);
    }
    EXPECT_EQ(summarizeCanceledTransfer(
                  2, 4096, 25, [](size_t, TransferStatus&) { return false; })
                  .duration_us,
              -1);
}

TEST(DeadlineTransferResultTest, ThresholdPrecisionSurvivesStatsFormatting) {
    XferBenchStats stats;
    stats.completed_tasks = 2;
    stats.dropped = 1;
    const auto flags = std::cout.flags();
    const auto precision = std::cout.precision();
    testing::internal::CaptureStdout();
    std::cout << std::fixed << std::setprecision(1);
    printDeadlineDropStats(1.25, stats);
    const auto output = testing::internal::GetCapturedStdout();
    std::cout.flags(flags);
    std::cout.precision(precision);
    EXPECT_NE(output.find("mlu_local_threshold=1.25 completed=2 dropped=1"),
              std::string::npos);
}

}  // namespace
}  // namespace mooncake::tent
