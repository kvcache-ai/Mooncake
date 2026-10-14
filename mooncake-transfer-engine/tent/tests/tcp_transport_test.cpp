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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport_selector.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
    }
    static uint64_t droppedRecvBacklog(const TcpTransport& transport) {
        return transport.notify_backlog_dropped_.load();
    }
};

namespace {

std::shared_ptr<ControlService> makeP2PMetadata() {
    return std::make_shared<ControlService>("p2p", "", nullptr);
}

TEST(TcpTransportConfigTest, InstallWithoutConfigKeepsStructDefaults) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-default";
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    const TcpParams defaults;
    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, defaults.max_retry_count);
    EXPECT_EQ(params.retry_base_delay_ms, defaults.retry_base_delay_ms);
    EXPECT_EQ(params.retry_max_delay_ms, defaults.retry_max_delay_ms);
    EXPECT_EQ(params.max_concurrent_tasks, defaults.max_concurrent_tasks);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, InstallReadsConfigKeys) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-override";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, 5u);
    EXPECT_EQ(params.retry_base_delay_ms, 200ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 4000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 32u);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, RejectsZeroWorkersAndAllowsRetry) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 0);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-zero-workers";
    auto status = transport.install(name, metadata, nullptr, conf);
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.ToString().find("transports/tcp/max_concurrent_tasks"),
              std::string::npos);

    conf->set("transports/tcp/max_concurrent_tasks", 1);
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    EXPECT_EQ(TcpTransportTestPeer::params(transport).max_concurrent_tasks, 1u);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportCapabilityTest, CpuPlatformCanSelectPeerGpu) {
    if (Platform::getLoader().type() != "cpu") {
        GTEST_SKIP() << "Requires a CPU-only build";
    }

    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 1);
    auto transport = std::make_shared<TcpTransport>();
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-cpu-peer-gpu";
    ASSERT_TRUE(transport->install(name, metadata, nullptr, conf).ok());

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[TCP] = transport;
    const std::vector<TransportType> tcp_buffer{TCP};
    const std::vector<TransportType> no_tcp_buffer{};

    struct Case {
        const char* name;
        MemoryType local;
        MemoryType peer;
        bool peer_exports_tcp;
        TransportType expected;
    };
    const Case cases[] = {
        {"host_peer_host", MTYPE_CPU, MTYPE_CPU, true, TCP},
        {"host_peer_gpu", MTYPE_CPU, MTYPE_CUDA, true, TCP},
        {"host_peer_gpu_without_tcp", MTYPE_CPU, MTYPE_CUDA, false, UNSPEC},
        {"local_gpu_peer_host", MTYPE_CUDA, MTYPE_CPU, true, UNSPEC},
        {"local_gpu_peer_gpu", MTYPE_CUDA, MTYPE_CUDA, true, UNSPEC},
    };
    // These are selection metadata, not device allocations or GPU transfers.
    // READ and WRITE keep the same local/peer roles in SelectionContext.
    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        SelectionContext context{};
        context.segment_type = SegmentType::Memory;
        context.local_memory_type = test.local;
        context.remote_memory_type = test.peer;
        context.buffer_transports =
            test.peer_exports_tcp ? &tcp_buffer : &no_tcp_buffer;
        context.transfer_size = 1;
        EXPECT_EQ(selector.select(context, transports, 0, TCP).transport,
                  test.expected);
    }

    EXPECT_TRUE(transport->uninstall().ok());
}

TEST(TcpTransportNotifyTest, RecvBacklogIsBoundedAndDropsTheOldest) {
    auto conf = std::make_shared<Config>();
    conf->set("notification/recv_backlog_max", 4);
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-recv-backlog";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    uint16_t port = 0;
    ASSERT_TRUE(metadata->start(port).ok());
    const std::string addr = "127.0.0.1:" + std::to_string(port);

    // 1000 drops: LOG_EVERY_N(1000) logs one of them, whatever ran before.
    const auto warnings = google::LogMessage::num_messages(google::WARNING);
    for (int i = 0; i < 1004; ++i)
        ASSERT_TRUE(ControlClient::notify(addr, {"n", std::to_string(i)}).ok());
    std::vector<Notification> received;
    ASSERT_TRUE(transport.receiveNotification(received).ok());
    std::vector<std::string> msgs;
    for (const auto& notifi : received) msgs.push_back(notifi.msg);
    EXPECT_EQ(msgs, (std::vector<std::string>{"1000", "1001", "1002", "1003"}));
    EXPECT_EQ(TcpTransportTestPeer::droppedRecvBacklog(transport), 1000u);
    EXPECT_GE(google::LogMessage::num_messages(google::WARNING) - warnings, 1);
    ASSERT_TRUE(transport.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // allocateSubBatch reserves task_list to max_size so submitTransferTasks
    // can take stable TcpTask* after emplace.
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = nextTcpRetryDelay(delay, cap);
    }

    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
