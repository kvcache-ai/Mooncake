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

#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/shm/shm_transport.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
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

// ---------------------------------------------------------------------------
// Sub-batch type mismatch regression test (#3129)
// ---------------------------------------------------------------------------
// getTransferStatus is a public Transport API. Handing it a sub-batch that
// was allocated by a different transport used to dereference the result of a
// failed dynamic_cast (UB). Neither allocateSubBatch nor getTransferStatus's
// type check requires install(), so default-constructed transports suffice.

TEST(TcpTransportSubBatchTest, RejectsForeignSubBatchInGetTransferStatus) {
    TcpTransport tcp;
    ShmTransport shm;

    Transport::SubBatchRef shm_batch = nullptr;
    ASSERT_TRUE(shm.allocateSubBatch(shm_batch, 8).ok());
    ASSERT_NE(shm_batch, nullptr);

    // An ShmSubBatch handed to TcpTransport must be rejected, not cast
    // blindly. Without the null check this dereferences nullptr.
    TransferStatus status;
    EXPECT_TRUE(
        tcp.getTransferStatus(shm_batch, 0, status).IsInvalidArgument());

    // Same guard in the opposite direction.
    Transport::SubBatchRef tcp_batch = nullptr;
    ASSERT_TRUE(tcp.allocateSubBatch(tcp_batch, 8).ok());
    ASSERT_NE(tcp_batch, nullptr);
    EXPECT_TRUE(
        shm.getTransferStatus(tcp_batch, 0, status).IsInvalidArgument());

    // A matching sub-batch must still pass: the guard discriminates by
    // type rather than rejecting everything.
    auto* typed_tcp_batch = dynamic_cast<TcpSubBatch*>(tcp_batch);
    ASSERT_NE(typed_tcp_batch, nullptr);
    typed_tcp_batch->task_list.emplace_back();
    ASSERT_TRUE(tcp.getTransferStatus(tcp_batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);

    EXPECT_TRUE(shm.freeSubBatch(shm_batch).ok());
    EXPECT_TRUE(tcp.freeSubBatch(tcp_batch).ok());
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
