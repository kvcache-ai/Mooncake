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

// Self-targeted (intra-agent) notifications must be delivered in-process.
//
// The data plane short-circuits LOCAL_SEGMENT_ID transfers to the local
// path, but notifications used to be routed unconditionally through the
// first transport that supports them; over RDMA the local pseudo-endpoint
// has no notification QP, so the send failed and a receiver polling for the
// notification hung forever. These tests pin the in-process delivery
// contract without requiring any NIC or a metadata server.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

namespace {

constexpr char kSegmentName[] = "local-notification-test-segment";

std::shared_ptr<Config> makeConfig(bool enable_tcp) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("local_segment_name", kSegmentName);
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("transports/tcp/enable", enable_tcp);
    config->set("transports/shm/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/nvlink/enable", false);
    config->set("transports/mnnvl/enable", false);
    config->set("transports/gds/enable", false);
    config->set("transports/ascend_direct/enable", false);
    return config;
}

}  // namespace

// Opening the local segment by the engine's own advertised name (or the
// empty name) must resolve to LOCAL_SEGMENT_ID - that is the id the delivery
// below keys on. Note: in p2p mode the advertised name is the normalized
// "ip:port", not the configured local_segment_name.
TEST(LocalNotificationTest, OwnNameResolvesToLocalSegmentId) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    SegmentID handle = ~0ull;
    ASSERT_TRUE(engine.openSegment(handle, engine.getSegmentName()).ok());
    EXPECT_EQ(handle, LOCAL_SEGMENT_ID);

    handle = ~0ull;
    ASSERT_TRUE(engine.openSegment(handle, "").ok());
    EXPECT_EQ(handle, LOCAL_SEGMENT_ID);
}

// Notifications sent to LOCAL_SEGMENT_ID come back from the next
// receiveNotification() poll, in order, exactly once.
TEST(LocalNotificationTest, SelfNotificationIsDeliveredInProcess) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    const int kCount = 8;
    for (int i = 0; i < kCount; ++i) {
        Notification notifi;
        notifi.name = kSegmentName;
        notifi.msg = "payload-" + std::to_string(i);
        ASSERT_TRUE(engine.sendNotification(LOCAL_SEGMENT_ID, notifi).ok());
    }

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), static_cast<size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        EXPECT_EQ(received[i].name, kSegmentName);
        EXPECT_EQ(received[i].msg, "payload-" + std::to_string(i));
    }

    // Delivered exactly once: a second poll returns nothing new.
    std::vector<Notification> again;
    ASSERT_TRUE(engine.receiveNotification(again).ok());
    EXPECT_TRUE(again.empty());
}

// Self-delivery must not depend on any transport advertising notification
// support: with every transport disabled the local queue still works, while
// an empty poll keeps reporting the original "not supported" error.
TEST(LocalNotificationTest, WorksWithoutAnyNotificationTransport) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/false));
    ASSERT_TRUE(engine.available());

    std::vector<Notification> received;
    EXPECT_FALSE(engine.receiveNotification(received).ok());

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "no-transport-payload";
    ASSERT_TRUE(engine.sendNotification(LOCAL_SEGMENT_ID, notifi).ok());

    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "no-transport-payload");
}

}  // namespace tent
}  // namespace mooncake
