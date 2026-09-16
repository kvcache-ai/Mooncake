// Copyright 2024 KVCache.AI
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

// Exercise the real chunking, MR guard, metadata and rollback with fake verbs.
// No RDMA device, pinned memory or firmware resources are used.
#include <gtest/gtest.h>
#include <sys/mman.h>

#include <cerrno>
#include <memory>
#include <unordered_set>
#include <vector>

#include "config.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {
std::vector<size_t> registrations;
std::unordered_set<ibv_mr *> live_mrs;
size_t fail_at = 0;
uint32_t next_key = 1;

ibv_mr *registerMock(ibv_pd *pd, void *addr, size_t length) {
    registrations.push_back(length);
    if (fail_at && registrations.size() == fail_at) {
        errno = ENOMEM;
        return nullptr;
    }
    auto *mr = new ibv_mr{};
    mr->pd = pd;
    mr->addr = addr;
    mr->length = length;
    mr->lkey = mr->rkey = next_key++;
    live_mrs.insert(mr);
    return mr;
}
}  // namespace

extern "C" ibv_mr *__wrap_ibv_reg_mr(ibv_pd *pd, void *addr, size_t length,
                                     int) {
    return registerMock(pd, addr, length);
}
extern "C" ibv_mr *__wrap_ibv_reg_mr_iova2(ibv_pd *pd, void *addr,
                                           size_t length, uint64_t,
                                           unsigned int) {
    return registerMock(pd, addr, length);
}
extern "C" int __wrap_ibv_dereg_mr(ibv_mr *mr) {
    live_mrs.erase(mr);
    delete mr;
    return 0;
}

namespace {
class RdmaMrOverrideTest : public ::testing::Test {
   protected:
    static constexpr size_t kAdvertised = 64 * 1024;
    static constexpr size_t kOverride = 128 * 1024;
    static constexpr size_t kSize = 2 * kOverride;
    GlobalConfig saved_;
    ibv_pd pd_{};
    void *buffer_ = MAP_FAILED;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;

    void SetUp() override {
        saved_ = globalConfig();
        globalConfig().max_mr_size = kAdvertised;
        globalConfig().rdma_max_mr_size_override.reset();
        globalConfig().parallel_reg_mr = 0;
        registrations.clear();
        fail_at = 0;
        buffer_ = mmap(nullptr, kSize, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(buffer_, MAP_FAILED);
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "mr-override-test:1234");
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = "mr-override-test:1234";
        desc->protocol = "rdma";
        metadata_->addLocalSegment(LOCAL_SEGMENT_ID, "mr-override-test:1234",
                                   std::move(desc));
        context_ = std::make_shared<RdmaContext>(*transport_, "mock_rdma");
        RdmaContextTestPeer::setProtectionDomain(*context_, &pd_);
        RdmaTransportTestPeer::addContext(*transport_, context_);
    }

    void TearDown() override {
        if (transport_ && buffer_ != MAP_FAILED &&
            !metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty()) {
            EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), 0);
        }
        EXPECT_TRUE(live_mrs.empty());
        if (context_)
            RdmaContextTestPeer::setProtectionDomain(*context_, nullptr);
        transport_.reset();
        context_.reset();
        if (buffer_ != MAP_FAILED) munmap(buffer_, kSize);
        globalConfig() = saved_;
    }

    int registerBuffer(size_t length = kSize) {
        return transport_->registerLocalMemory(buffer_, length, "cpu:0", true,
                                               false);
    }
};

TEST_F(RdmaMrOverrideTest, DefaultRegistersFourDeviceSizedChunks) {
    ASSERT_EQ(registerBuffer(), 0);
    EXPECT_EQ(registrations, (std::vector<size_t>(4, kAdvertised)));
    EXPECT_EQ(metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.size(),
              4u);
}

TEST_F(RdmaMrOverrideTest,
       OverrideRegistersFullLengthAndPublishesDistinctKeys) {
    globalConfig().rdma_max_mr_size_override = kOverride;
    ASSERT_EQ(registerBuffer(), 0);
    EXPECT_EQ(registrations, (std::vector<size_t>{kOverride, kOverride}));
    const auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 2u);
    EXPECT_EQ(desc->buffers[0].addr, reinterpret_cast<uint64_t>(buffer_));
    EXPECT_EQ(desc->buffers[1].addr,
              reinterpret_cast<uint64_t>(buffer_) + kOverride);
    EXPECT_EQ(desc->buffers[0].length, kOverride);
    EXPECT_EQ(desc->buffers[1].length, kOverride);
    ASSERT_EQ(desc->buffers[0].rkey.size(), 1u);
    ASSERT_EQ(desc->buffers[1].rkey.size(), 1u);
    EXPECT_NE(desc->buffers[0].rkey[0], desc->buffers[1].rkey[0]);
    EXPECT_EQ(context_->rkey(static_cast<char *>(buffer_) + kOverride),
              desc->buffers[1].rkey[0]);
    EXPECT_EQ(globalConfig().max_mr_size, kAdvertised);
    ASSERT_EQ(transport_->unregisterLocalMemory(buffer_, false), 0);
    EXPECT_TRUE(live_mrs.empty());
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

TEST_F(RdmaMrOverrideTest, DriverFailureRollsBackWithoutSmallerRetry) {
    globalConfig().rdma_max_mr_size_override = kOverride;
    fail_at = 2;
    EXPECT_NE(registerBuffer(), 0);
    EXPECT_EQ(registrations, (std::vector<size_t>{kOverride, kOverride}));
    EXPECT_TRUE(live_mrs.empty());
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

TEST_F(RdmaMrOverrideTest, ContextRejectsRequestsAboveEffectiveLimit) {
    globalConfig().rdma_max_mr_size_override = kOverride;
    DmabufExport exp;
    EXPECT_NE(context_->registerMemoryRegion(buffer_, kOverride + 1,
                                             IBV_ACCESS_LOCAL_WRITE, exp),
              0);
    EXPECT_TRUE(registrations.empty());
}

TEST_F(RdmaMrOverrideTest, PreTouchUsesTheSameOverrideAsRegistration) {
    globalConfig().rdma_max_mr_size_override = kOverride;
    ASSERT_EQ(context_->preTouchMemory(buffer_, kOverride), 0);
    EXPECT_EQ(registrations, (std::vector<size_t>{kOverride}));
    EXPECT_TRUE(live_mrs.empty());
}
}  // namespace
