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
#include <unistd.h>

#include <csignal>  // coro_http_client uses SIGPIPE
#include <ylt/coro_http/coro_http_client.hpp>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "te_backend.h"
#include "tent/common/types.h"
#include "transfer_engine_metrics.h"
#include "transfer_metadata_plugin.h"

namespace {

using mooncake::findAvailableTcpPort;
using mooncake::TransferEngineMetrics;
using mooncake::tent::IntentType;
using mooncake::tent::TEBenchRunner;
using mooncake::tent::XferBenchConfig;

class BenchMetricsIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        setenv("MC_FORCE_TCP", "1", 1);
        setenv("MC_TCP_BIND_ADDRESS", "127.0.0.1", 1);
        setenv("MC_TE_METRIC", "1", 1);
        setenv("MC_TE_METRIC_HTTP_HOST", "127.0.0.1", 1);
        int fd = -1;
        const auto port = findAvailableTcpPort(fd);
        ASSERT_GT(port, 0);
        close(fd);
        setenv("MC_TE_METRIC_HTTP_PORT", std::to_string(port).c_str(), 1);
        url_ = "http://127.0.0.1:" + std::to_string(port) + "/metrics";

        XferBenchConfig::metadata_type = "p2p";
        XferBenchConfig::seg_name = "127.0.0.1";
        XferBenchConfig::seg_type = "DRAM";
        XferBenchConfig::xport_type = "tcp";
        XferBenchConfig::total_buffer_size = 1024 * 1024;
        target_ = std::make_unique<TEBenchRunner>();
        XferBenchConfig::target_seg_name = target_->getSegmentName();
        initiator_ = std::make_unique<TEBenchRunner>();
        ASSERT_EQ(initiator_->startInitiator(1), 0);
        started_ = true;
        ASSERT_EQ(TransferEngineMetrics::instance().httpPort(), port);
    }

    void TearDown() override {
        if (started_) initiator_->stopInitiator();
        initiator_.reset();
        target_.reset();
        TransferEngineMetrics::instance().shutdown();
    }

    void checkMetrics(uint64_t read_bytes, uint64_t write_bytes,
                      size_t block_size) {
        constexpr auto startup_timeout = std::chrono::seconds(1);
        constexpr auto retry_interval = std::chrono::milliseconds(25);
        coro_http::coro_http_client client;
        // The metrics HTTP server starts asynchronously; wait for it to accept.
        const auto deadline =
            std::chrono::steady_clock::now() + startup_timeout;
        auto response = client.get(url_);
        while (response.status != 200 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(retry_interval);
            response = client.get(url_);
        }
        ASSERT_EQ(response.status, 200) << url_;
        std::cout << "\n/metrics (expected read bytes: " << read_bytes
                  << ", write bytes: " << write_bytes << ")\n"
                  << response.resp_body << std::endl;
        SCOPED_TRACE(std::string(response.resp_body));
        for (const auto& [direction, transferred] :
             {std::pair{"read", read_bytes}, std::pair{"write", write_bytes}}) {
            const uint64_t requests = transferred / block_size;
            for (const auto& [suffix, expected] :
                 {std::pair{"bytes_total", transferred},
                  std::pair{"requests_total", requests},
                  std::pair{"failures_total", uint64_t{0}},
                  std::pair{"latency_us_count", requests},
                  std::pair{"size_bytes_count", requests}}) {
                // Histograms are exported only after their first observation.
                if (requests == 0 &&
                    (std::strcmp(suffix, "latency_us_count") == 0 ||
                     std::strcmp(suffix, "size_bytes_count") == 0))
                    continue;
                const std::string metric =
                    std::string("mooncake_te_") + direction + "_" + suffix;
                std::istringstream lines{std::string(response.resp_body)};
                std::string line;
                double exported = -1;
                while (std::getline(lines, line)) {
                    std::istringstream sample(line);
                    std::string name;
                    if (sample >> name; name == metric) {
                        ASSERT_TRUE(static_cast<bool>(sample >> exported));
                        break;
                    }
                }
                ASSERT_GE(exported, 0) << "Missing " << metric;
                EXPECT_EQ(exported, expected) << metric;
            }
        }
    }

    std::unique_ptr<TEBenchRunner> target_, initiator_;
    std::string url_;
    bool started_ = false;
};

TEST_F(BenchMetricsIntegrationTest, ExportedMetricsMatchCompletedTransfers) {
    constexpr size_t block_size = 4093, batch_size = 3;
    constexpr size_t bytes = block_size * batch_size;
    const auto local =
        initiator_->getLocalBufferBase(0, block_size, batch_size);
    const auto remote =
        initiator_->getTargetBufferBase(0, block_size, batch_size);
    const auto segment = initiator_->getTargetSegmentId(0);
    auto* source = reinterpret_cast<void*>(local);
    auto* destination = reinterpret_cast<void*>(remote);
    std::string payload(bytes, 'x');
    std::memcpy(source, payload.data(), bytes);
    std::memset(destination, 0, bytes);
    uint64_t read_bytes = 0, write_bytes = 0;
    ASSERT_NO_FATAL_FAILURE(checkMetrics(read_bytes, write_bytes, block_size));

    for (auto opcode :
         {mooncake::tent::WRITE, mooncake::tent::READ, mooncake::tent::READ}) {
        if (opcode == mooncake::tent::READ) std::memset(source, 0, bytes);
        ASSERT_GE(initiator_->runSingleTransfer(local, segment, remote,
                                                block_size, batch_size, opcode,
                                                0, IntentType::INTENT_UNSPEC),
                  0);
        EXPECT_EQ(
            std::memcmp(opcode == mooncake::tent::WRITE ? destination : source,
                        payload.data(), bytes),
            0);
        (opcode == mooncake::tent::READ ? read_bytes : write_bytes) += bytes;
        ASSERT_NO_FATAL_FAILURE(
            checkMetrics(read_bytes, write_bytes, block_size));
    }
}

}  // namespace
