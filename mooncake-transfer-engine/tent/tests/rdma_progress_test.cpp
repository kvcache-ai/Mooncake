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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

bool waitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED)
            return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

TEST(RdmaTransportIntegrationTest, CompletionNotificationWithoutCallerPolling) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/rdma/enable", true);
    config->set("transports/rdma/num_lanes", 1);
    config->set("transports/rdma/endpoint/max_qp_wr", 1);
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", 0);
    config->set("enable_progress_worker", true);
    config->set("runtime_queue/progress_fallback_interval_us", 0UL);
    if (const char* nic = std::getenv("MC_TENT_TEST_NIC"))
        config->set("topology/rdma_whitelist", std::vector<std::string>{nic});
    if (const char* gid = std::getenv("MC_TENT_TEST_GID_INDEX"))
        config->set("transports/rdma/device/gid_index", std::stoi(gid));

    // A verbs device can exist without any usable port. Probe with the same
    // configuration as the engines before starting this hardware-only test.
    Topology topology;
    ASSERT_TRUE(topology.discover({&Platform::getLoader(config)}).ok());
    if (topology.getNicCount(Topology::NIC_RDMA) == 0)
        GTEST_SKIP() << "no usable RDMA port with a GID table";

    constexpr size_t kLength = 1 << 20;
    for (bool runtime_queue : {false, true}) {
        SCOPED_TRACE(runtime_queue);
        // Buffers must outlive both engines, including on an assertion failure.
        std::vector<uint8_t> source(kLength, 0x5A), destination(kLength, 0);
        config->set("enable_runtime_queue", runtime_queue);
        TransferEngine server(config), client(config);
        ASSERT_TRUE(server.available());
        ASSERT_TRUE(client.available());
        ASSERT_TRUE(
            server.registerLocalMemory(destination.data(), kLength).ok());
        ASSERT_TRUE(client.registerLocalMemory(source.data(), kLength).ok());
        SegmentID segment = 0;
        ASSERT_TRUE(client.openSegment(segment, server.getSegmentName()).ok());

        Request request{};
        request.opcode = Request::WRITE;
        request.source = source.data();
        request.target_id = segment;
        request.target_offset = reinterpret_cast<uint64_t>(destination.data());
        request.length = kLength;
        request.transport_hint = RDMA;
        const auto batch = client.allocateBatch(2);
        ASSERT_NE(batch, 0u);
        for (int round = 0; round < 2; ++round) {
            SCOPED_TRACE(round);
            std::fill(source.begin(), source.end(), 0x5A + round);
            Notification notification;
            notification.name = "rdma-progress";
            notification.msg = std::to_string(round);
            ASSERT_TRUE(
                client.submitTransfer(batch, {request}, notification).ok());
            bool received = false;
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (!received && std::chrono::steady_clock::now() < deadline) {
                std::vector<Notification> notifications;
                ASSERT_TRUE(server.receiveNotification(notifications).ok());
                for (const auto& item : notifications)
                    received |= item.name == notification.name &&
                                item.msg == notification.msg;
                if (!received)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            EXPECT_TRUE(received) << "completion requires caller polling";
            // Drain only after observing notification delivery. The second
            // round appends to a batch whose first submission has completed.
            ASSERT_TRUE(waitBatchDone(client, batch));
            EXPECT_EQ(destination, source);
        }
        EXPECT_TRUE(client.freeBatch(batch).ok());
        EXPECT_TRUE(client.closeSegment(segment).ok());
        EXPECT_TRUE(client.unregisterLocalMemory(source.data(), kLength).ok());
        EXPECT_TRUE(
            server.unregisterLocalMemory(destination.data(), kLength).ok());
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
