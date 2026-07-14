#include "master_service.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "client_buffer.h"
#include "store_warmup_internal.h"
#include "types.h"

namespace mooncake::test {
namespace {

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, std::optional<const char*> value)
        : name_(name) {
        if (const char* old_value = std::getenv(name)) old_value_ = old_value;
        if (value) {
            setenv(name, *value, 1);
        } else {
            unsetenv(name);
        }
    }

    ~ScopedEnvVar() {
        if (old_value_) {
            setenv(name_.c_str(), old_value_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};

}  // namespace

class StoreWarmupTest : public ::testing::Test {
   protected:
    static Segment MakeSegment(std::string name, uintptr_t base,
                               std::string protocol,
                               std::string te_endpoint = "") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = 1024 * 1024;
        segment.protocol = std::move(protocol);
        segment.te_endpoint = std::move(te_endpoint);
        return segment;
    }

    static void Mount(MasterService& service, const Segment& segment,
                      const UUID& client_id) {
        auto result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(result.has_value());
    }

    static std::unordered_set<std::string> TargetNames(
        const std::vector<WarmupTarget>& targets) {
        std::unordered_set<std::string> names;
        for (const auto& target : targets) {
            names.insert(target.segment_name);
        }
        return names;
    }

    static void ExpectSequentialPriority(
        const std::vector<WarmupTarget>& targets) {
        for (size_t i = 0; i < targets.size(); ++i) {
            EXPECT_EQ(targets[i].priority, i);
        }
    }
};

TEST_F(StoreWarmupTest, SingleNodeReturnsNoWarmupTargets) {
    MasterService service;
    const UUID requester{1, 1};

    Mount(service,
          MakeSegment("local-segment-1", 0x10000000, "tcp", "127.0.0.1:18000"),
          requester);
    Mount(service, MakeSegment("127.0.0.1:18001", 0x20000000, "tcp"),
          requester);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    EXPECT_TRUE(targets->empty());

    targets = service.ListWarmupTargets(requester, 1, {});
    ASSERT_TRUE(targets.has_value());
    EXPECT_TRUE(targets->empty());
}

TEST_F(StoreWarmupTest, ListWarmupTargetsUsesExactProtocolTokens) {
    MasterService service;
    const UUID requester{5, 1};
    const UUID remote_client_1{6, 1};
    const UUID remote_client_2{7, 1};
    const UUID remote_client_3{8, 1};

    Mount(service,
          MakeSegment("remote-token-list", 0x40000000, "tcp,rdma",
                      "127.0.0.1:18003"),
          remote_client_1);
    Mount(service,
          MakeSegment("remote-substring", 0x50000000, "tcp_like",
                      "127.0.0.1:18004"),
          remote_client_2);
    Mount(service,
          MakeSegment("remote-rdma", 0x60000000, "rdma", "127.0.0.1:18005"),
          remote_client_3);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 1);
    EXPECT_EQ(targets->at(0).segment_name, "127.0.0.1:18003");
    EXPECT_EQ(targets->at(0).protocol, "tcp,rdma");
}

TEST_F(StoreWarmupTest, DeduplicatesWarmupTargetsByEndpoint) {
    MasterService service;
    const UUID requester{11, 1};
    const UUID remote_client{12, 1};

    Mount(service,
          MakeSegment("remote-dup-a", 0xB0000000, "tcp", "127.0.0.1:18009"),
          remote_client);
    Mount(service,
          MakeSegment("remote-dup-b", 0xC0000000, "tcp", "127.0.0.1:18009"),
          remote_client);
    Mount(service,
          MakeSegment("remote-distinct", 0xD0000000, "tcp", "127.0.0.1:18010"),
          remote_client);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 2);
    const auto target_names = TargetNames(*targets);
    EXPECT_TRUE(target_names.contains("127.0.0.1:18009"));
    EXPECT_TRUE(target_names.contains("127.0.0.1:18010"));
    ExpectSequentialPriority(*targets);
}

TEST_F(StoreWarmupTest, WarmupMaxTargetsUsesSafeDefaultAndExplicitValues) {
    {
        ScopedEnvVar max_targets("MC_STORE_WARMUP_MAX_TARGETS", std::nullopt);
        EXPECT_EQ(internal::LoadStoreWarmupOptions().max_targets,
                  internal::kDefaultWarmupMaxTargets);
    }
    {
        ScopedEnvVar max_targets("MC_STORE_WARMUP_MAX_TARGETS", "3");
        EXPECT_EQ(internal::LoadStoreWarmupOptions().max_targets, 3);
    }
    {
        ScopedEnvVar max_targets("MC_STORE_WARMUP_MAX_TARGETS", "0");
        EXPECT_EQ(internal::LoadStoreWarmupOptions().max_targets, 0);
    }
}

TEST_F(StoreWarmupTest, PendingTransferRetainsBufferUntilTerminal) {
    alignas(64) std::array<std::byte, 64> memory{};
    auto allocator =
        ClientBufferAllocator::create(memory.data(), memory.size(), "tcp");
    auto buffer = allocator->allocate(memory.size());
    ASSERT_TRUE(buffer.has_value());

    std::atomic<bool> terminal{false};
    std::atomic<size_t> release_calls{0};
    internal::WarmupBatchCleanup cleanup(
        [&](Transport::BatchID) {
            return terminal.load() ? internal::WarmupBatchState::kTerminal
                                   : internal::WarmupBatchState::kPending;
        },
        [&](Transport::BatchID) {
            release_calls.fetch_add(1);
            return Status::OK();
        });

    cleanup.TrackPendingTransfer(
        {1, "test-segment", std::move(*buffer), /*transfer_terminal=*/false});
    EXPECT_FALSE(cleanup.DrainFor(std::chrono::milliseconds(20)));
    EXPECT_FALSE(allocator->allocate(memory.size()).has_value());
    EXPECT_EQ(release_calls.load(), 0);

    terminal.store(true);
    ASSERT_TRUE(cleanup.DrainFor(std::chrono::seconds(1)));
    EXPECT_EQ(release_calls.load(), 1);
    EXPECT_TRUE(allocator->allocate(memory.size()).has_value());
    EXPECT_TRUE(cleanup.ShutdownFor(std::chrono::seconds(1)));
}

TEST_F(StoreWarmupTest, TerminalTransferReleasesBufferWhileBatchFreeRetries) {
    alignas(64) std::array<std::byte, 64> memory{};
    auto allocator =
        ClientBufferAllocator::create(memory.data(), memory.size(), "tcp");
    auto buffer = allocator->allocate(memory.size());
    ASSERT_TRUE(buffer.has_value());

    std::atomic<bool> allow_release{false};
    std::atomic<size_t> release_calls{0};
    internal::WarmupBatchCleanup cleanup(
        [](Transport::BatchID) {
            return internal::WarmupBatchState::kTerminal;
        },
        [&](Transport::BatchID) {
            release_calls.fetch_add(1);
            if (!allow_release.load()) {
                return Status::BatchBusy("injected first release failure");
            }
            return Status::OK();
        });

    internal::PendingWarmupBatch batch{2, "test-segment", std::move(*buffer),
                                       /*transfer_terminal=*/true};
    EXPECT_FALSE(cleanup.ReleaseOrTrack(std::move(batch)));
    EXPECT_TRUE(allocator->allocate(memory.size()).has_value());
    EXPECT_EQ(cleanup.PendingTransferCount(), 0);
    EXPECT_EQ(cleanup.PendingBatchIDReleaseCount(), 1);
    EXPECT_GE(release_calls.load(), 1);

    allow_release.store(true);
    ASSERT_TRUE(cleanup.DrainFor(std::chrono::seconds(1)));
    const size_t calls_after_release = release_calls.load();
    EXPECT_TRUE(cleanup.ShutdownFor(std::chrono::seconds(1)));
    EXPECT_EQ(release_calls.load(), calls_after_release);
}

TEST_F(StoreWarmupTest, ShutdownOccursBeforeMemoryUnregistration) {
    std::byte memory{};
    std::atomic<int> sequence{0};
    std::atomic<int> buffer_release_order{0};
    internal::WarmupBatchCleanup cleanup(
        [](Transport::BatchID) {
            return internal::WarmupBatchState::kTerminal;
        },
        [](Transport::BatchID) { return Status::OK(); });

    cleanup.TrackPendingTransfer(
        {3, "test-segment",
         BufferHandle(
             &memory, sizeof(memory),
             [&]() { buffer_release_order.store(sequence.fetch_add(1) + 1); }),
         /*transfer_terminal=*/false});

    ASSERT_TRUE(cleanup.ShutdownFor(std::chrono::seconds(1)));
    const int unregister_order = sequence.fetch_add(1) + 1;
    EXPECT_GT(buffer_release_order.load(), 0);
    EXPECT_LT(buffer_release_order.load(), unregister_order);
    EXPECT_EQ(cleanup.PendingTransferCount(), 0);
}

}  // namespace mooncake::test
