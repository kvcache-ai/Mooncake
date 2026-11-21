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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "error.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/tcp_transport/tcp_transport.h"

using namespace mooncake;

namespace {

constexpr const char *kSegmentName = "local-tcp-segment";

class FakeMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string &key, Json::Value &value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = store_.find(key);
        if (it == store_.end()) return false;
        value = it->second;
        return true;
    }

    bool set(const std::string &key, const Json::Value &value) override {
        ++set_calls_;
        int remaining = fail_sets_remaining_.load();
        while (remaining > 0 && !fail_sets_remaining_.compare_exchange_weak(
                                    remaining, remaining - 1)) {
        }
        if (remaining > 0) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        store_[key] = value;
        return true;
    }

    bool remove(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        store_.erase(key);
        return true;
    }

    void failNextSet(int n) { fail_sets_remaining_ = n; }
    int setCalls() const { return set_calls_.load(); }

   private:
    std::mutex mutex_;
    std::map<std::string, Json::Value> store_;
    std::atomic<int> fail_sets_remaining_{0};
    std::atomic<int> set_calls_{0};
};

class TestableTransferMetadata : public TransferMetadata {
   public:
    explicit TestableTransferMetadata(
        std::shared_ptr<MetadataStoragePlugin> storage_plugin)
        : TransferMetadata(std::move(storage_plugin)) {}
};

}  // namespace

namespace mooncake {

class TcpUnregisterRepublishTestPeer {
   public:
    static void bindMetadata(TcpTransport &transport,
                             std::shared_ptr<TransferMetadata> metadata,
                             const std::string &segment_name) {
        transport.metadata_ = std::move(metadata);
        transport.local_server_name_ = segment_name;
    }

    static int registerMemory(TcpTransport &transport, void *addr,
                              size_t length, bool update_metadata) {
        return transport.registerLocalMemory(addr, length, "cpu:0", true,
                                             update_metadata);
    }

    static int unregisterMemory(TcpTransport &transport, void *addr,
                                bool update_metadata) {
        return transport.unregisterLocalMemory(addr, update_metadata);
    }

    static int unregisterBatch(TcpTransport &transport,
                               const std::vector<void *> &addrs) {
        return transport.unregisterLocalMemoryBatch(addrs);
    }
};

}  // namespace mooncake

namespace {

class TcpUnregisterRepublishTest : public ::testing::Test {
   protected:
    void SetUp() override {
        plugin_ = std::make_shared<FakeMetadataStoragePlugin>();
        metadata_ = std::make_shared<TestableTransferMetadata>(plugin_);
        auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        local_desc->name = kSegmentName;
        local_desc->protocol = "tcp";
        ASSERT_EQ(metadata_->addLocalSegment(LOCAL_SEGMENT_ID, kSegmentName,
                                             std::move(local_desc)),
                  0);
        transport_ = std::make_unique<TcpTransport>();
        TcpUnregisterRepublishTestPeer::bindMetadata(*transport_, metadata_,
                                                     kSegmentName);
    }

    int registerMemory(void *addr, size_t length, bool update_metadata) {
        return TcpUnregisterRepublishTestPeer::registerMemory(
            *transport_, addr, length, update_metadata);
    }

    int unregisterMemory(void *addr, bool update_metadata) {
        return TcpUnregisterRepublishTestPeer::unregisterMemory(
            *transport_, addr, update_metadata);
    }

    int unregisterBatch(const std::vector<void *> &addrs) {
        return TcpUnregisterRepublishTestPeer::unregisterBatch(*transport_,
                                                               addrs);
    }

    bool hasLocalBuffer(void *addr) {
        auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!desc) return false;
        for (const auto &buffer : desc->buffers) {
            if (buffer.addr == reinterpret_cast<uint64_t>(addr)) return true;
        }
        return false;
    }

    std::shared_ptr<FakeMetadataStoragePlugin> plugin_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<TcpTransport> transport_;
    std::vector<char> first_ = std::vector<char>(4096);
    std::vector<char> second_ = std::vector<char>(4096);
};

TEST_F(TcpUnregisterRepublishTest, SingleRetryCompletesPublication) {
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);
    plugin_->failNextSet(1);
    EXPECT_EQ(unregisterMemory(first_.data(), true), ERR_METADATA);
    EXPECT_FALSE(hasLocalBuffer(first_.data()));
    EXPECT_EQ(plugin_->setCalls(), 1);

    EXPECT_EQ(unregisterMemory(first_.data(), true), 0);
    EXPECT_EQ(plugin_->setCalls(), 2);
    EXPECT_EQ(unregisterMemory(first_.data(), true),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(plugin_->setCalls(), 2);
}

TEST_F(TcpUnregisterRepublishTest, ReregistrationSupersedesPendingRepublish) {
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);
    plugin_->failNextSet(1);
    ASSERT_EQ(unregisterMemory(first_.data(), true), ERR_METADATA);
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);

    EXPECT_EQ(unregisterMemory(first_.data(), true), 0);
    EXPECT_FALSE(hasLocalBuffer(first_.data()));
    EXPECT_EQ(plugin_->setCalls(), 2);
}

TEST_F(TcpUnregisterRepublishTest,
       NonPublishingRetryStillCompletesPublication) {
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);
    plugin_->failNextSet(1);
    ASSERT_EQ(unregisterMemory(first_.data(), true), ERR_METADATA);

    EXPECT_EQ(unregisterMemory(first_.data(), false), 0);
    EXPECT_EQ(plugin_->setCalls(), 2);
    EXPECT_EQ(unregisterMemory(first_.data(), true),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST_F(TcpUnregisterRepublishTest, BatchRetryCompletesCollectivePublication) {
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);
    ASSERT_EQ(registerMemory(second_.data(), second_.size(), false), 0);
    plugin_->failNextSet(1);
    EXPECT_EQ(unregisterBatch({first_.data(), second_.data()}), ERR_METADATA);
    EXPECT_FALSE(hasLocalBuffer(first_.data()));
    EXPECT_FALSE(hasLocalBuffer(second_.data()));

    EXPECT_EQ(unregisterBatch({first_.data(), second_.data()}), 0);
    EXPECT_EQ(plugin_->setCalls(), 2);
    EXPECT_EQ(unregisterBatch({first_.data(), second_.data()}),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST_F(TcpUnregisterRepublishTest, MixedBatchReportsUnknownAfterKnownRemoval) {
    ASSERT_EQ(registerMemory(first_.data(), first_.size(), false), 0);

    EXPECT_EQ(unregisterBatch({first_.data(), second_.data()}),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_FALSE(hasLocalBuffer(first_.data()));
    EXPECT_EQ(plugin_->setCalls(), 1);
    EXPECT_EQ(unregisterBatch({first_.data(), second_.data()}),
              ERR_ADDRESS_NOT_REGISTERED);
}

}  // namespace
