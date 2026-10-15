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

// Regression tests for partial-failure semantics of
// RdmaTransport::unregisterLocalMemory: the metadata entry is removed BEFORE
// the per-context MR deregistration, so a failed ibv_dereg_mr leaves a live MR
// behind a missing metadata entry. A retry must re-attempt the MR cleanup and
// must only report ERR_ADDRESS_NOT_REGISTERED once nothing remains.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <memory>

#include <sys/mman.h>

#include "error.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

// ibv_dereg_mr is an inline forwarding to mr->context->ops.dereg_mr, so a
// fake ibv_context ops table lets us script deregistration failures without
// RDMA hardware.
int g_fake_dereg_failures_remaining = 0;
int g_fake_dereg_calls = 0;

int fakeDeregMr(ibv_mr *) {
    ++g_fake_dereg_calls;
    if (g_fake_dereg_failures_remaining > 0) {
        --g_fake_dereg_failures_remaining;
        return EIO;
    }
    return 0;
}

struct FakeMr {
    ibv_context verbs_ctx = {};
    ibv_mr mr = {};

    FakeMr(void *addr, size_t length) {
        verbs_ctx.ops.dereg_mr = &fakeDeregMr;
        mr.context = &verbs_ctx;
        mr.addr = addr;
        mr.length = length;
    }
};

class RdmaUnregisterRetryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        buffer_ = mmap(nullptr, kLength, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(buffer_, MAP_FAILED);

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        local_desc->name = "local-rdma-segment";
        local_desc->protocol = "rdma";
        ASSERT_EQ(metadata_->addLocalSegment(LOCAL_SEGMENT_ID,
                                             "local-rdma-segment",
                                             std::move(local_desc)),
                  0);

        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "local-rdma-segment");
        context_ =
            std::make_shared<RdmaContext>(*transport_, "nonexistent-device");
        RdmaTransportTestPeer::addContext(*transport_, context_);

        g_fake_dereg_failures_remaining = 0;
        g_fake_dereg_calls = 0;
    }

    void TearDown() override {
        // Destroy the transport (and its contexts) before unmapping so no
        // deferred cleanup can touch the buffer.
        transport_.reset();
        context_.reset();
        if (buffer_ != MAP_FAILED) munmap(buffer_, kLength);
    }

    void addMetadataEntry() {
        TransferMetadata::BufferDesc desc{};
        desc.name = "local-rdma-segment";
        desc.addr = reinterpret_cast<uint64_t>(buffer_);
        desc.length = kLength;
        ASSERT_EQ(metadata_->addLocalMemoryBuffer(desc, false), 0);
    }

    static constexpr size_t kLength = 64 * 1024;
    void *buffer_ = MAP_FAILED;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;
};

// A failed ibv_dereg_mr after the metadata entry was removed must surface the
// real error (not NOT_REGISTERED), and the retry must reach the MR cleanup
// behind the missing metadata entry instead of short-circuiting.
TEST_F(RdmaUnregisterRetryTest, RetryReachesLiveMrBehindMissingMetadata) {
    FakeMr fake(buffer_, kLength);
    RdmaContextTestPeer::seedMemoryRegion(*context_, buffer_, kLength,
                                          &fake.mr);
    addMetadataEntry();

    // First attempt: metadata is removed, then the MR deregistration fails.
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_))
        << "failed deregistration must keep the MR so a retry can find it";
    EXPECT_EQ(g_fake_dereg_calls, 1);

    // Retry: the metadata entry is already gone, but the cleanup must still
    // reach the live MR. NOT_REGISTERED is now truthful: nothing remains.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
    EXPECT_EQ(g_fake_dereg_calls, 2);

    // A clean address keeps reporting NOT_REGISTERED without touching verbs.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(g_fake_dereg_calls, 2);
}

// While an MR survives, the retry must keep failing: NOT_REGISTERED may only
// be reported once the per-context deregistration has actually succeeded.
TEST_F(RdmaUnregisterRetryTest, RetryKeepsFailingWhileMrSurvives) {
    FakeMr fake(buffer_, kLength);
    RdmaContextTestPeer::seedMemoryRegion(*context_, buffer_, kLength,
                                          &fake.mr);
    addMetadataEntry();

    g_fake_dereg_failures_remaining = 2;
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    // The second call must not be classified as "already unregistered" just
    // because the metadata entry is gone.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));

    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
    EXPECT_EQ(g_fake_dereg_calls, 3);
}

}  // namespace
