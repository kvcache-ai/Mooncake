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

#include "tent/common/types.h"
#include "tent/transport/fault_proxy/fault_proxy_transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// Minimal FakeTransport: always succeeds, tracks call counts.
// ---------------------------------------------------------------------------

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }
    size_t task_count = 0;
    // Store status per task for getTransferStatus
    std::vector<TransferStatus> statuses;
};

class FakeTransport : public Transport {
   public:
    int install_calls = 0;
    int submit_calls = 0;
    int status_calls = 0;

    Status install(std::string& /*local_segment_name*/,
                   std::shared_ptr<ControlService> /*metadata*/,
                   std::shared_ptr<Topology> /*local_topology*/,
                   std::shared_ptr<Config> /*conf*/ = nullptr) override {
        ++install_calls;
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t /*max_size*/) override {
        batch = new FakeSubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete batch;
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        ++submit_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        for (size_t i = 0; i < request_list.size(); ++i) {
            fb->statuses.push_back(
                {TransferStatusEnum::COMPLETED, request_list[i].length});
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        ++status_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status = fb->statuses[task_id];
        return Status::OK();
    }

    const char* getName() const override { return "<fake>"; }
};

// ---------------------------------------------------------------------------
// Helper: create a Request for testing (no real memory needed)
// ---------------------------------------------------------------------------

static Request makeRequest(size_t length = 4096) {
    Request req;
    req.opcode = Request::WRITE;
    req.source = nullptr;
    req.target_id = 1;
    req.target_offset = 0;
    req.length = length;
    return req;
}

// ===========================================================================
// Group 1: Unit — FaultProxyTransport basics
// ===========================================================================

TEST(FaultProxyTest, DelegatesToReal) {
    auto fake = std::make_shared<FakeTransport>();
    FaultPolicy policy;  // all defaults — no faults
    auto proxy = std::make_shared<FaultProxyTransport>(fake, policy);

    // install
    std::string seg_name = "test";
    ASSERT_TRUE(proxy->install(seg_name, nullptr, nullptr).ok());
    EXPECT_EQ(fake->install_calls, 1);

    // allocateSubBatch + submitTransferTasks
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(proxy->allocateSubBatch(batch, 16).ok());
    ASSERT_NE(batch, nullptr);

    auto req = makeRequest();
    ASSERT_TRUE(proxy->submitTransferTasks(batch, {req}).ok());
    EXPECT_EQ(fake->submit_calls, 1);
    EXPECT_EQ(proxy->submitCount(), 1);

    // getTransferStatus
    TransferStatus ts{};
    ASSERT_TRUE(proxy->getTransferStatus(batch, 0, ts).ok());
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(ts.transferred_bytes, 4096u);
    EXPECT_EQ(fake->status_calls, 1);

    proxy->freeSubBatch(batch);
}

TEST(FaultProxyTest, FailsInstall) {
    auto fake = std::make_shared<FakeTransport>();
    FaultPolicy policy;
    policy.fail_install = true;
    auto proxy = std::make_shared<FaultProxyTransport>(fake, policy);

    std::string seg_name = "test";
    auto status = proxy->install(seg_name, nullptr, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(fake->install_calls, 0);  // never reached the real transport
}

TEST(FaultProxyTest, FailsSubmitDeterministic) {
    auto fake = std::make_shared<FakeTransport>();
    FaultPolicy policy;
    policy.fail_after_n_submits = 2;  // succeed twice, then fail
    auto proxy = std::make_shared<FaultProxyTransport>(fake, policy);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(proxy->allocateSubBatch(batch, 16).ok());
    auto req = makeRequest();

    // First two submits succeed
    EXPECT_TRUE(proxy->submitTransferTasks(batch, {req}).ok());
    EXPECT_TRUE(proxy->submitTransferTasks(batch, {req}).ok());
    EXPECT_EQ(fake->submit_calls, 2);

    // Third submit fails
    auto s = proxy->submitTransferTasks(batch, {req});
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(fake->submit_calls, 2);  // real transport not called

    // Fourth also fails
    EXPECT_FALSE(proxy->submitTransferTasks(batch, {req}).ok());

    proxy->freeSubBatch(batch);
}

TEST(FaultProxyTest, CorruptsStatus) {
    auto fake = std::make_shared<FakeTransport>();
    FaultPolicy policy;
    policy.status_corrupt_rate = 1.0;  // always corrupt
    auto proxy = std::make_shared<FaultProxyTransport>(fake, policy);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(proxy->allocateSubBatch(batch, 16).ok());
    auto req = makeRequest();
    ASSERT_TRUE(proxy->submitTransferTasks(batch, {req}).ok());

    TransferStatus ts{};
    ASSERT_TRUE(proxy->getTransferStatus(batch, 0, ts).ok());
    // Real transport returned COMPLETED, but proxy flipped it to FAILED
    EXPECT_EQ(ts.s, TransferStatusEnum::FAILED);

    proxy->freeSubBatch(batch);
}

// ===========================================================================
// Group 2: Policy mutation
// ===========================================================================

TEST(FaultProxyTest, ResetPolicyMidRun) {
    auto fake = std::make_shared<FakeTransport>();
    FaultPolicy fail_policy;
    fail_policy.submit_fail_rate = 1.0;
    auto proxy = std::make_shared<FaultProxyTransport>(fake, fail_policy);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(proxy->allocateSubBatch(batch, 16).ok());
    auto req = makeRequest();

    // Should fail
    EXPECT_FALSE(proxy->submitTransferTasks(batch, {req}).ok());
    EXPECT_EQ(fake->submit_calls, 0);

    // Reset to clean policy
    FaultPolicy clean_policy;
    proxy->resetPolicy(clean_policy);

    // Should succeed now
    EXPECT_TRUE(proxy->submitTransferTasks(batch, {req}).ok());
    EXPECT_EQ(fake->submit_calls, 1);
    EXPECT_EQ(proxy->submitCount(), 1);  // counter was reset too

    proxy->freeSubBatch(batch);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
