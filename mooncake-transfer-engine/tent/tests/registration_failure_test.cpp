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
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/segment_registry.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void replaceRegistry(
        tent::TransferEngineImpl& engine,
        std::unique_ptr<tent::SegmentRegistry> registry) {
        engine.metadata_->segmentManager().registry_ = std::move(registry);
    }
};

namespace tent {
namespace {

// Inject only registry I/O failures, retaining the real local publication and
// synchronizeLocal() path. Replacement occurs before any test operation.
class FailingRegistry : public SegmentRegistry {
   public:
    Status getSegmentDesc(SegmentDescRef& desc, const std::string&) override {
        if (!published) return Status::InvalidArgument("no published segment");
        desc = published;
        return Status::OK();
    }

    Status putSegmentDesc(SegmentDescRef& desc) override {
        ++put_calls;
        if (fail_puts > 0) {
            --fail_puts;
            return Status::InternalError(
                "injected metadata publication failure");
        }
        published = desc;
        return Status::OK();
    }

    Status deleteSegmentDesc(const std::string&) override {
        published.reset();
        return Status::OK();
    }

    int fail_puts{0};
    int put_calls{0};
    SegmentDescRef published;
};

// Each successful add creates a distinct generation. A failed removal leaves
// it live, and an overlapping add is rejected, as by the native HP_TCP
// registry.
class FailingRegistrationTransport : public Transport {
   public:
    explicit FailingRegistrationTransport(TransportType type = HP_TCP)
        : type_(type) {}

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        ++add_attempts;
        if (fail_adds > 0) {
            --fail_adds;
            return Status::InternalError(
                "injected transport registration failure");
        }
        if (live.count(desc.addr))
            return Status::InvalidArgument("overlapping test registration");
        const auto generation = std::to_string(++successful_adds);
        live.emplace(desc.addr, Entry{desc.length, generation});
        desc.transports.push_back(type_);
        desc.transport_attrs[type_] = generation;
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc& desc) override {
        ++remove_attempts;
        auto it = live.find(desc.addr);
        if (it == live.end() || it->second.length != desc.length)
            return Status::InvalidArgument("removing absent test registration");
        auto attr = desc.transport_attrs.find(type_);
        if (attr == desc.transport_attrs.end() ||
            attr->second != it->second.generation)
            return Status::InvalidArgument("removing wrong test generation");
        if (fail_removals > 0 &&
            (!fail_address || *fail_address == desc.addr)) {
            --fail_removals;
            return Status::InternalError(
                "injected registration cleanup failure");
        }
        live.erase(it);
        ++successful_removes;
        ++removed[desc.addr];
        desc.transports.erase(
            std::remove(desc.transports.begin(), desc.transports.end(), type_),
            desc.transports.end());
        desc.transport_attrs.erase(type_);
        return Status::OK();
    }

    bool tracksLocalBuffer(const BufferDesc& desc) const override {
        auto it = live.find(desc.addr);
        return it != live.end() && it->second.length == desc.length;
    }

    const char* getName() const override { return "failing-registration"; }

    struct Entry {
        uint64_t length;
        std::string generation;
    };
    std::map<uint64_t, Entry> live;
    std::map<uint64_t, int> removed;
    int fail_adds{0};
    int fail_removals{0};
    std::optional<uint64_t> fail_address;
    int add_attempts{0};
    int remove_attempts{0};
    int successful_adds{0};
    int successful_removes{0};

   private:
    TransportType type_;
};

class RegistrationFailureTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("metadata_servers", "");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", "0");
        config->set("log_level", "warning");
        for (const char* name : {"tcp", "hp_tcp", "shm", "rdma", "io_uring",
                                 "nvlink", "mnnvl", "gds", "ascend_direct"}) {
            config->set(std::string("transports/") + name + "/enable", false);
        }
        engine = std::make_unique<TransferEngineImpl>(config);
        ASSERT_TRUE(engine->available());
        auto replacement = std::make_unique<FailingRegistry>();
        registry = replacement.get();
        TransferEngineImplTestPeer::replaceRegistry(*engine,
                                                    std::move(replacement));
        transport = std::make_shared<FailingRegistrationTransport>();
        engine->swapTransportForTest(HP_TCP, transport);
        options.type = HP_TCP;
        options.perm = kGlobalReadWrite;
    }

    void TearDown() override {
        if (registry) registry->fail_puts = 0;
        if (transport) transport->fail_removals = 0;
        engine.reset();
    }

    size_t visibleCount() {
        SegmentInfo info;
        EXPECT_TRUE(engine->getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        return info.buffers.size();
    }

    Status add(std::vector<char>& buffer) {
        return engine->registerLocalMemory({buffer.data()}, {buffer.size()},
                                           options);
    }

    Status remove(std::vector<char>& buffer) {
        return engine->unregisterLocalMemory(buffer.data(), buffer.size());
    }

    static uint64_t address(const std::vector<char>& buffer) {
        return reinterpret_cast<uint64_t>(buffer.data());
    }

    void useTcpTransport() {
        engine->swapTransportForTest(HP_TCP, nullptr);
        transport = std::make_shared<FailingRegistrationTransport>(TCP);
        engine->swapTransportForTest(TCP, transport);
        options.type = TCP;
    }

    // Keep backing memory alive until the engine and its transports are gone.
    std::vector<char> old_buffer = std::vector<char>(4096);
    std::vector<char> new_buffer = std::vector<char>(4096);
    std::vector<char> tail_buffer = std::vector<char>(4096);
    std::unique_ptr<TransferEngineImpl> engine;
    std::shared_ptr<FailingRegistrationTransport> transport;
    FailingRegistry* registry{nullptr};
    MemoryOptions options;
};

TEST_F(RegistrationFailureTest, RegisterSyncFailureRollsBackWholeMixedBatch) {
    ASSERT_TRUE(add(old_buffer).ok());
    std::vector<void*> addresses{old_buffer.data(), new_buffer.data(),
                                 new_buffer.data(), old_buffer.data()};
    std::vector<size_t> lengths(addresses.size(), old_buffer.size());
    registry->fail_puts = 1;
    EXPECT_FALSE(engine->registerLocalMemory(addresses, lengths, options).ok());
    EXPECT_EQ(visibleCount(), 1u);
    EXPECT_EQ(transport->successful_adds, 2);
    EXPECT_EQ(transport->removed[address(new_buffer)], 1);
    EXPECT_EQ(transport->removed[address(old_buffer)], 0);

    ASSERT_TRUE(engine->registerLocalMemory(addresses, lengths, options).ok());
    EXPECT_EQ(transport->successful_adds, 3);
    EXPECT_EQ(visibleCount(), 2u);
    ASSERT_TRUE(remove(old_buffer).ok());
    ASSERT_TRUE(remove(old_buffer).ok());
    EXPECT_EQ(transport->removed[address(old_buffer)], 0);
    ASSERT_TRUE(remove(old_buffer).ok());
    EXPECT_EQ(transport->removed[address(old_buffer)], 1);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->removed[address(new_buffer)], 1);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->removed[address(new_buffer)], 2);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, ZeroLengthIsRejectedBeforeAnyRegistrationWork) {
    ASSERT_TRUE(add(old_buffer).ok());
    const int publications = registry->put_calls;
    registry->fail_puts = 1;
    auto status =
        engine->registerLocalMemory({old_buffer.data()}, {0}, options);
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_EQ(transport->add_attempts, 1);
    EXPECT_EQ(transport->remove_attempts, 0);
    EXPECT_EQ(registry->put_calls, publications);
    EXPECT_EQ(visibleCount(), 1u);

    // A zero length must never reach rollback, where length zero means the
    // wildcard unregister operation and could remove the existing range.
    registry->fail_puts = 0;
    ASSERT_TRUE(remove(old_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, ExplicitNonHpTcpFailureGrantsNoReference) {
    useTcpTransport();
    transport->fail_adds = 1;
    const int publications = registry->put_calls;
    EXPECT_FALSE(add(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->add_attempts, 1);
    EXPECT_EQ(transport->successful_adds, 0);
    EXPECT_EQ(transport->remove_attempts, 0);
    EXPECT_EQ(registry->put_calls, publications);

    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_adds, 1);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, UnspecifiedSoleTransportFailureCanBeRetried) {
    useTcpTransport();
    options.type = UNSPEC;
    transport->fail_adds = 1;
    EXPECT_FALSE(add(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->successful_adds, 0);
    ASSERT_TRUE(add(new_buffer).ok());
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_adds, 1);
    EXPECT_EQ(transport->successful_removes, 1);

    // With multiple transports, an optional backend failure still permits a
    // successful fallback, preserving UNSPEC's existing best-effort behavior.
    auto fallback = std::make_shared<FailingRegistrationTransport>(HP_TCP);
    engine->swapTransportForTest(HP_TCP, fallback);
    transport->fail_adds = 1;
    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_adds, 1);
    EXPECT_EQ(fallback->successful_adds, 1);
    EXPECT_EQ(visibleCount(), 1u);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->remove_attempts, 1);
    EXPECT_EQ(fallback->successful_removes, 1);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, FailedCompensatingSyncLeavesNoLocalReference) {
    // Both the initial publication and a compensating publication may fail.
    registry->fail_puts = 2;
    EXPECT_FALSE(add(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_TRUE(transport->live.empty());
    EXPECT_EQ(transport->successful_removes, 1);

    registry->fail_puts = 0;
    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_adds, 2);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 2);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest,
       FailedUnregisterRequiresCleanupRetryBeforeNewGeneration) {
    ASSERT_TRUE(add(new_buffer).ok());
    transport->fail_removals = 2;
    EXPECT_FALSE(remove(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->live.size(), 1u);

    // Registration cannot consume another operation's failed unregister or
    // let that operation's subsequent retry remove a newly registered range.
    EXPECT_TRUE(add(new_buffer).IsTooManyRequests());
    EXPECT_EQ(transport->add_attempts, 1);
    EXPECT_EQ(transport->remove_attempts, 1);
    EXPECT_EQ(transport->successful_removes, 0);
    EXPECT_EQ(visibleCount(), 0u);

    EXPECT_FALSE(remove(new_buffer).ok());
    EXPECT_EQ(transport->remove_attempts, 2);
    EXPECT_TRUE(add(new_buffer).IsTooManyRequests());
    EXPECT_EQ(transport->remove_attempts, 2);
    EXPECT_EQ(transport->add_attempts, 1);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_TRUE(transport->live.empty());

    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_EQ(transport->successful_adds, 2);
    EXPECT_EQ(transport->live.size(), 1u);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 2);
    EXPECT_TRUE(transport->live.empty());
}

TEST_F(RegistrationFailureTest,
       FailedRegistrationCleanupCanBeRetriedByRegister) {
    registry->fail_puts = 1;
    transport->fail_removals = 2;
    EXPECT_FALSE(add(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->live.size(), 1u);
    EXPECT_EQ(transport->remove_attempts, 1);

    // This residue belongs to a failed registration, so a registration retry
    // may clean it up. It still must not create a new generation too early.
    EXPECT_FALSE(add(new_buffer).ok());
    EXPECT_EQ(transport->remove_attempts, 2);
    EXPECT_EQ(transport->add_attempts, 1);
    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_EQ(transport->successful_adds, 2);
    EXPECT_EQ(visibleCount(), 1u);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 2);
    EXPECT_TRUE(transport->live.empty());
}

TEST_F(RegistrationFailureTest, UnregisterRetryFinishesRetiredCleanup) {
    ASSERT_TRUE(add(new_buffer).ok());
    transport->fail_removals = 1;
    EXPECT_FALSE(remove(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->successful_removes, 0);

    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 1);
    EXPECT_TRUE(transport->live.empty());
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->remove_attempts, 2);
}

TEST_F(RegistrationFailureTest, UnregisterSyncFailureRetriesPublicationOnly) {
    ASSERT_TRUE(engine
                    ->registerLocalMemory(
                        {new_buffer.data(), new_buffer.data()},
                        {new_buffer.size(), new_buffer.size()}, options)
                    .ok());
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 0);

    registry->fail_puts = 1;
    EXPECT_FALSE(remove(new_buffer).ok());
    EXPECT_EQ(visibleCount(), 0u);
    EXPECT_EQ(transport->successful_removes, 1);
    const int publications = registry->put_calls;
    // Even though the physical registration is gone, the failed unregister
    // retains its tombstone until its metadata publication is retried.
    EXPECT_TRUE(add(new_buffer).IsTooManyRequests());
    EXPECT_EQ(transport->add_attempts, 1);
    EXPECT_EQ(transport->remove_attempts, 1);
    EXPECT_EQ(registry->put_calls, publications);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_GT(registry->put_calls, publications);
    EXPECT_EQ(transport->remove_attempts, 1);
    ASSERT_TRUE(registry->published);
    EXPECT_TRUE(std::get<MemorySegmentDesc>(registry->published->detail)
                    .buffers.empty());
    ASSERT_TRUE(add(new_buffer).ok());
    EXPECT_EQ(transport->successful_adds, 2);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->successful_removes, 2);
}

TEST_F(RegistrationFailureTest, BatchPublishesFinalRemovalsOnce) {
    ASSERT_TRUE(add(old_buffer).ok());
    ASSERT_TRUE(add(new_buffer).ok());
    const int publications = registry->put_calls;
    ASSERT_TRUE(
        engine
            ->unregisterLocalMemory({old_buffer.data(), new_buffer.data()},
                                    {old_buffer.size(), new_buffer.size()})
            .ok());
    EXPECT_EQ(registry->put_calls, publications + 1);
    EXPECT_EQ(transport->successful_removes, 2);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, FailedBatchPublicationLeavesSuffixUntouched) {
    ASSERT_TRUE(add(old_buffer).ok());
    ASSERT_TRUE(add(new_buffer).ok());
    ASSERT_TRUE(add(new_buffer).ok());
    registry->fail_puts = 1;
    auto status =
        engine->unregisterLocalMemory({old_buffer.data(), new_buffer.data()},
                                      {old_buffer.size(), new_buffer.size()});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("index 0"), std::string::npos);
    EXPECT_EQ(transport->removed[address(old_buffer)], 1);
    EXPECT_EQ(transport->removed[address(new_buffer)], 0);

    // Retry starts at the reported failed index. The suffix must not have
    // consumed a reference before the metadata failure was returned.
    ASSERT_TRUE(
        engine
            ->unregisterLocalMemory({old_buffer.data(), new_buffer.data()},
                                    {old_buffer.size(), new_buffer.size()})
            .ok());
    EXPECT_EQ(transport->removed[address(old_buffer)], 1);
    EXPECT_EQ(transport->removed[address(new_buffer)], 0);
    EXPECT_EQ(visibleCount(), 1u);
    ASSERT_TRUE(remove(new_buffer).ok());
    EXPECT_EQ(transport->removed[address(new_buffer)], 1);
    EXPECT_EQ(visibleCount(), 0u);
}

TEST_F(RegistrationFailureTest, EarlyBatchCleanupFailureCommitsOnlyItsPrefix) {
    ASSERT_TRUE(add(old_buffer).ok());
    ASSERT_TRUE(add(old_buffer).ok());
    ASSERT_TRUE(add(new_buffer).ok());
    ASSERT_TRUE(add(tail_buffer).ok());
    const int publications = registry->put_calls;
    transport->fail_address = address(new_buffer);
    transport->fail_removals = 1;
    auto status =
        engine->unregisterLocalMemory({old_buffer.data(), old_buffer.data(),
                                       new_buffer.data(), tail_buffer.data()},
                                      {old_buffer.size(), old_buffer.size(),
                                       new_buffer.size(), tail_buffer.size()});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("index 2"), std::string::npos);
    EXPECT_EQ(registry->put_calls, publications + 1);
    EXPECT_EQ(transport->removed[address(old_buffer)], 1);
    EXPECT_EQ(transport->removed[address(new_buffer)], 0);
    EXPECT_EQ(transport->removed[address(tail_buffer)], 0);
    EXPECT_EQ(visibleCount(), 1u);

    // The completed prefix is not replayed; retry the failed range and suffix.
    ASSERT_TRUE(
        engine
            ->unregisterLocalMemory({new_buffer.data(), tail_buffer.data()},
                                    {new_buffer.size(), tail_buffer.size()})
            .ok());
    EXPECT_EQ(registry->put_calls, publications + 2);
    EXPECT_EQ(transport->removed[address(old_buffer)], 1);
    EXPECT_EQ(transport->removed[address(new_buffer)], 1);
    EXPECT_EQ(transport->removed[address(tail_buffer)], 1);
    EXPECT_EQ(transport->remove_attempts, 4);
    EXPECT_EQ(visibleCount(), 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
