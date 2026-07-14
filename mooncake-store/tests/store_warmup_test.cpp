#include "master_service.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
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

TEST_F(StoreWarmupTest, FiltersUnavailableSegmentsAndDeduplicatesEndpoints) {
    MasterService service;
    const UUID requester{11, 1};
    const UUID remote_client{12, 1};

    auto local = MakeSegment("local", 0xA0000000, "tcp", "127.0.0.1:18008");
    auto available =
        MakeSegment("available", 0xB0000000, "tcp", "127.0.0.1:18009");
    auto draining =
        MakeSegment("draining", 0xC0000000, "tcp", "127.0.0.1:18010");
    auto unmounted =
        MakeSegment("unmounted", 0xD0000000, "tcp", "127.0.0.1:18011");
    auto duplicate_a =
        MakeSegment("duplicate-a", 0xE0000000, "tcp", "127.0.0.1:18012");
    auto duplicate_b =
        MakeSegment("duplicate-b", 0xF0000000, "tcp", "127.0.0.1:18012");

    Mount(service, local, requester);
    Mount(service, available, remote_client);
    Mount(service, draining, remote_client);
    Mount(service, unmounted, remote_client);
    Mount(service, duplicate_a, remote_client);
    Mount(service, duplicate_b, remote_client);
    ASSERT_TRUE(
        service.GracefulUnmountSegment(draining.id, remote_client, 60 * 1000)
            .has_value());
    ASSERT_TRUE(
        service.UnmountSegment(unmounted.id, remote_client).has_value());

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 2);
    const auto target_names = TargetNames(*targets);
    EXPECT_EQ(target_names.size(), 2);
    EXPECT_TRUE(target_names.contains("127.0.0.1:18009"));
    EXPECT_TRUE(target_names.contains("127.0.0.1:18012"));
    EXPECT_FALSE(target_names.contains("127.0.0.1:18008"));
    EXPECT_FALSE(target_names.contains("127.0.0.1:18010"));
    EXPECT_FALSE(target_names.contains("127.0.0.1:18011"));
}

TEST_F(StoreWarmupTest, MaxTargetsIsStableForRequester) {
    MasterService service;
    const UUID requester{13, 1};
    const UUID remote_client{14, 1};

    Mount(service,
          MakeSegment("remote-a", 0x110000000, "tcp", "127.0.0.1:18013"),
          remote_client);
    Mount(service,
          MakeSegment("remote-b", 0x120000000, "tcp", "127.0.0.1:18014"),
          remote_client);
    Mount(service,
          MakeSegment("remote-c", 0x130000000, "tcp", "127.0.0.1:18015"),
          remote_client);
    Mount(service,
          MakeSegment("remote-d", 0x140000000, "tcp", "127.0.0.1:18016"),
          remote_client);

    auto first = service.ListWarmupTargets(requester, 2, {"tcp"});
    auto second = service.ListWarmupTargets(requester, 2, {"tcp"});
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(first->size(), 2);
    ASSERT_EQ(second->size(), 2);
    EXPECT_EQ(TargetNames(*first).size(), 2);
    EXPECT_EQ(TargetNames(*first), TargetNames(*second));
    ExpectSequentialPriority(*first);
    ExpectSequentialPriority(*second);
    for (size_t i = 0; i < first->size(); ++i) {
        EXPECT_EQ(first->at(i).segment_name, second->at(i).segment_name);
    }
}

TEST_F(StoreWarmupTest, ServerCapsUnlimitedAndOversizedRequests) {
    EXPECT_GT(internal::kMaxWarmupTargetsPerRequest,
              internal::kDefaultWarmupMaxTargets);
    EXPECT_EQ(internal::NormalizeWarmupMaxTargets(0),
              internal::kMaxWarmupTargetsPerRequest);
    EXPECT_EQ(internal::NormalizeWarmupMaxTargets(
                  internal::kMaxWarmupTargetsPerRequest + 1),
              internal::kMaxWarmupTargetsPerRequest);
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

TEST_F(StoreWarmupTest, PendingTransferDoesNotStarveBatchIdRelease) {
    alignas(64) std::array<std::byte, 64> memory{};
    auto allocator =
        ClientBufferAllocator::create(memory.data(), memory.size(), "tcp");
    auto buffer = allocator->allocate(memory.size());
    ASSERT_TRUE(buffer.has_value());

    std::atomic<bool> transfer_terminal{false};
    std::atomic<size_t> batch_release_attempts{0};
    std::atomic<bool> batch_released{false};
    std::mutex released_mutex;
    std::condition_variable released_cv;
    internal::WarmupBatchCleanup cleanup(
        [&](Transport::BatchID) {
            return transfer_terminal.load()
                       ? internal::WarmupBatchState::kTerminal
                       : internal::WarmupBatchState::kPending;
        },
        [&](Transport::BatchID batch_id) {
            if (batch_id != 2) return Status::OK();
            const size_t attempt = batch_release_attempts.fetch_add(1) + 1;
            if (attempt == 1) {
                return Status::BatchBusy("injected first release failure");
            }
            batch_released.store(true);
            released_cv.notify_all();
            return Status::OK();
        });

    cleanup.TrackPendingTransfer(
        {1, "pending-segment", std::move(*buffer), false});
    EXPECT_FALSE(
        cleanup.ReleaseOrTrack({2, "terminal-segment", std::nullopt, true}));

    {
        std::unique_lock<std::mutex> lock(released_mutex);
        EXPECT_TRUE(released_cv.wait_for(lock, std::chrono::seconds(1), [&]() {
            return batch_released.load();
        }));
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (cleanup.PendingBatchIDReleaseCount() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(cleanup.PendingBatchIDReleaseCount(), 0);
    EXPECT_EQ(cleanup.PendingTransferCount(), 1);
    EXPECT_FALSE(allocator->allocate(memory.size()).has_value());

    transfer_terminal.store(true);
    EXPECT_TRUE(cleanup.DrainFor(std::chrono::seconds(1)));
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
