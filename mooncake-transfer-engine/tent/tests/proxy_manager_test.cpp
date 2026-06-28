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

#include <cstdint>
#include <string>
#include <vector>

#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {
namespace {

Request makeRequest(int priority) {
    Request request;
    request.opcode = Request::WRITE;
    request.source = reinterpret_cast<void*>(0x1000);
    request.target_id = 7;
    request.target_offset = 0x2000;
    request.length = 4096;
    request.priority = priority;
    return request;
}

TEST(ProxyManagerTest, SubmitReturnsTooManyRequestsWhenShardQueueIsFull) {
    auto proxy = ProxyManager::createForTest(nullptr, 1);

    TaskInfo first;
    TaskInfo second;
    std::vector<std::string> params{"server", "local", "remote"};

    auto status = proxy->submit(&first, 1, params);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(first.staging_status, TransferStatusEnum::PENDING);

    status = proxy->submit(&second, 2, params);
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
}

TEST(ProxyManagerTest, InternalStageRequestsInheritPriority) {
    auto request = makeRequest(PRIO_LOW);

    auto local_stage =
        ProxyManager::makeLocalStageRequest(request, 0x3000, 512, 128);
    EXPECT_EQ(local_stage.opcode, request.opcode);
    EXPECT_EQ(local_stage.source,
              reinterpret_cast<uint8_t*>(request.source) + 128);
    EXPECT_EQ(local_stage.target_id, LOCAL_SEGMENT_ID);
    EXPECT_EQ(local_stage.target_offset, 0x3000);
    EXPECT_EQ(local_stage.length, 512);
    EXPECT_EQ(local_stage.priority, PRIO_LOW);

    auto cross_stage =
        ProxyManager::makeCrossStageRequest(request, 0x3000, 0x4000, 512);
    EXPECT_EQ(cross_stage.opcode, request.opcode);
    EXPECT_EQ(cross_stage.source, reinterpret_cast<void*>(0x3000));
    EXPECT_EQ(cross_stage.target_id, request.target_id);
    EXPECT_EQ(cross_stage.target_offset, 0x4000);
    EXPECT_EQ(cross_stage.length, 512);
    EXPECT_EQ(cross_stage.priority, PRIO_LOW);

    auto remote_stage =
        ProxyManager::makeRemoteStageRequest(request, 0x4000, 512, 128);
    EXPECT_EQ(remote_stage.opcode, request.opcode);
    EXPECT_EQ(remote_stage.source, reinterpret_cast<void*>(0x4000));
    EXPECT_EQ(remote_stage.target_id, LOCAL_SEGMENT_ID);
    EXPECT_EQ(remote_stage.target_offset, request.target_offset + 128);
    EXPECT_EQ(remote_stage.length, 512);
    EXPECT_EQ(remote_stage.priority, PRIO_LOW);
}

TEST(ProxyManagerTest, UnboundedShardQueueKeepsLegacyAcceptBehavior) {
    auto proxy = ProxyManager::createForTest(nullptr, 0);
    std::vector<std::string> params{"server", "local", "remote"};

    TaskInfo first;
    TaskInfo second;
    EXPECT_TRUE(proxy->submit(&first, 1, params).ok());
    EXPECT_TRUE(proxy->submit(&second, 2, params).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
