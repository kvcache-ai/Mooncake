// Component tests for EvictEngine (design doc sections 5.10, 6.7, 9.3).
//
// The invariant this file exists to protect: detaching an entry from an index
// is a *logical* removal, and the physical bytes only come back when the last
// snapshot of them is released. A round therefore reports two numbers, and the
// second one is read from the pool rather than summed from the victim list --
// reporting a still-referenced block as reclaimed would tell a caller to
// expect an allocation to succeed when nothing was freed.
//
// The engine is driven directly here: rounds, the deadline and the try_evict
// switch belong to AllocateWithPolicy, so every ReclaimOneRound below is one
// call by one caller, never a loop.
//
// Time is injected. The reclaim deadline moves because ManualClock was
// advanced, never because a thread waited.

#include "p2p/client/v2/evict_engine.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/frequency_tracker.h"
#include "p2p/client/v2/local_copy_engine.h"
#include "p2p/client/v2/migration_engine.h"
#include "p2p/client/v2/eviction_index.h"
#include "p2p/client/v2/tiler_manager.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

constexpr size_t kBlockSize = 4096;
constexpr size_t kFastCapacity = 4u << 20;
constexpr size_t kSlowCapacity = 4u << 20;
constexpr UUID kUnknownTiler{0xdead, 0xbeef};

/**
 * @class ManualClock
 * @brief The only time source in this file. Its origin is unrelated to the
 *        real steady clock, so a test that accidentally depended on wall time
 *        would fail rather than pass slowly.
 */
class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        return now_.load(std::memory_order_acquire);
    }

    void Advance(std::chrono::milliseconds delta) {
        now_.store(now_.load(std::memory_order_acquire) + delta,
                   std::memory_order_release);
    }

   private:
    std::atomic<time_point> now_{Clock::time_point{} + 24h};
};

/**
 * @class AcceptingSink
 * @brief Swallows the facts the tilers publish.
 *
 * Nothing here needs them any more: victim ordering lives in each tiler's own
 * eviction index and is updated synchronously by TilerManager, in the same
 * mutation as the BlockIndex. This exists only so the tilers have somewhere to
 * publish to. A test that wants a genuinely stale candidate injects one
 * directly -- see InjectStaleToken.
 */
class AcceptingSink final : public EventSink {
   public:
    QueuePushResult Publish(BlockEvent) override {
        return QueuePushResult::kEnqueued;
    }
};

std::vector<uint8_t> Pattern(size_t size, uint8_t seed) {
    std::vector<uint8_t> out(size);
    for (size_t i = 0; i < size; ++i) {
        out[i] = static_cast<uint8_t>((i * 31 + seed) & 0xff);
    }
    return out;
}

std::span<const std::byte> AsBytes(const std::vector<uint8_t>& data) {
    return {reinterpret_cast<const std::byte*>(data.data()), data.size()};
}

std::span<std::byte> AsWritableBytes(std::vector<uint8_t>& data) {
    return {reinterpret_cast<std::byte*>(data.data()), data.size()};
}

}  // namespace

class EvictEngineTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("EvictEngineTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_evict_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);

        clock_ = std::make_shared<ManualClock>();
        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});
        sink_ = std::make_shared<AcceptingSink>();

        auto fast = MakeDramTiler(/*priority=*/100, kFastCapacity);
        auto slow = MakeSsdTiler(/*priority=*/10, kSlowCapacity);
        ASSERT_NE(fast, nullptr);
        ASSERT_NE(slow, nullptr);
        fast_ = fast.get();
        slow_ = slow.get();
        tilers_.by_priority.push_back(std::move(fast));
        tilers_.by_priority.push_back(std::move(slow));
        tilers_.Rebuild();

        tracker_ = std::make_shared<FrequencyTracker>();

        for (auto* tiler : tilers_.All()) {
            tiler->SetEventPublisher(EventPublisher(sink_));
        }

        callbacks_.remove_replica =
            [this](std::string_view key,
                   const UUID& tier_id) -> tl::expected<void, ErrorCode> {
            std::lock_guard<std::mutex> lock(removed_mu_);
            removed_.emplace_back(std::string(key), tier_id);
            return {};
        };

        migration_ = std::make_unique<MigrationEngine>(
            &tilers_, &registry_, &copier_, &callbacks_,
            [this](const UUID& tiler_id, size_t size, size_t alignment,
                   AllocationSource) -> tl::expected<MutableBlock, ErrorCode> {
                TilerManager* tiler = tilers_.Find(tiler_id);
                if (tiler == nullptr) {
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                return tiler->Allocate(size, alignment);
            });
        evict_ = std::make_unique<EvictEngine>(
            &tilers_, &registry_, &callbacks_, clock_,
            [this](const std::string& key) {
                std::lock_guard<std::mutex> lock(removed_mu_);
                orphaned_.push_back(key);
            });
    }

    void TearDown() override {
        evict_.reset();
        migration_.reset();
        tilers_ = MultiTiler{};
        std::filesystem::remove_all(storage_dir_);
    }

    std::unique_ptr<TilerManager> MakeDramTiler(int32_t priority,
                                                size_t capacity) {
        DramArenaConfig arena;
        arena.capacity_bytes = capacity;
        // Alignment 1 makes Usage() exactly the sum of the block sizes, so the
        // free_bytes bracket can be asserted as equalities rather than bounds.
        arena.alignment = 1;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);

        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(DRAM) failed: "
                          << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::DRAM;
        logical.priority = priority;
        logical.tags = {"fast"};
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    std::unique_ptr<TilerManager> MakeSsdTiler(int32_t priority,
                                               size_t capacity) {
        SSDDeviceConfig device;
        device.file_path = (storage_dir_ / "slow.data").string();
        device.capacity_bytes = capacity;
        SSDBlockPoolConfig pool_config;
        pool_config.devices.push_back(device);

        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(SSD) failed: "
                          << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::NVME;
        logical.priority = priority;
        logical.tags = {"slow"};
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    /** Allocate, fill and register `key`, returning the caller's snapshot. */
    ImmutableBlock Commit(TilerManager& tiler, std::string_view key,
                          uint8_t seed) {
        const std::vector<uint8_t> payload = Pattern(kBlockSize, seed);
        auto allocated = tiler.Allocate(payload.size());
        CHECK(allocated.has_value()) << "test setup: Allocate failed";
        CHECK(allocated->Write(0, AsBytes(payload)).has_value());
        auto completed =
            std::move(allocated.value()).Complete(std::string(key));
        CHECK(completed.has_value()) << "test setup: Complete failed";
        auto registered = tiler.Register(key, std::move(completed.value()));
        CHECK(registered.has_value()) << "test setup: Register failed";
        return std::move(registered.value());
    }

    /** Drops the snapshot, so the index is the only holder of the bytes. */
    void CommitAndForget(TilerManager& tiler, std::string_view key,
                         uint8_t seed) {
        Commit(tiler, key, seed);
    }

    BlockRegistrationHandle HandleFor(std::string_view key) {
        auto handle = registry_.Match(key);
        CHECK(handle.has_value()) << "test setup: no live registration";
        return handle.value();
    }

    bool Present(TilerManager& tiler, std::string_view key) {
        auto handle = registry_.Match(key);
        if (!handle.has_value()) return false;
        return tiler.Match(handle.value()).has_value();
    }

    /**
     * @brief A read of the key, the way the data plane reports one.
     *
     * Through TilerManager::NotifyAccess, which is what DataManagerV2::Get
     * calls: it updates the tier's eviction ordering and publishes the fact.
     * Feeding the policy directly would leave the ordering that actually
     * chooses victims untouched, so the test would assert nothing.
     */
    void RecordAccess(TilerManager& tiler, std::string_view key) {
        auto handle = registry_.Match(key);
        CHECK(handle.has_value()) << "test setup: no live registration";
        auto block = tiler.Match(handle.value());
        CHECK(block.has_value()) << "test setup: no replica to read";
        tiler.NotifyAccess(handle.value(), block.value());
    }

    /**
     * @brief Make the tier's ordering name `block_id` for `key`.
     *
     * The ordering is updated synchronously with the index now, so staleness
     * can no longer be produced by hiding an event -- but it is still
     * reachable: an update that could not be applied leaves the shard marked
     * needs_reconcile, and a reconcile pass can race a commit. This injects
     * that state directly, which is the honest way to test the engine's
     * response to it.
     */
    void InjectStaleToken(TilerManager& tiler,
                          const BlockRegistrationHandle& handle,
                          const BlockId& block_id, size_t size_bytes) {
        BlockToken token;
        token.key = handle.Key();
        token.registration_id = handle.Id();
        token.registration = handle.Downgrade();
        token.tiler_id = tiler.Id();
        token.block_id = block_id;
        token.size_bytes = size_bytes;
        tiler.Eviction()->OnCommit(token);
    }

    ReclaimRequest RequestFor(size_t target_bytes) const {
        ReclaimRequest request;
        request.tiler_id = fast_->Id();
        request.source = AllocationSource::kPut;
        request.allocation_size = kBlockSize;
        request.reclaim_target_bytes = target_bytes;
        request.round = 0;
        request.deadline = clock_->Now() + 10s;
        return request;
    }

    BlockToken EvictToken(const BlockRegistrationHandle& registration,
                          const UUID& source_tiler,
                          const BlockId& block_id) const {
        BlockToken token;
        token.key = registration.Key();
        token.registration_id = registration.Id();
        token.registration = registration.Downgrade();
        token.tiler_id = source_tiler;
        token.block_id = block_id;
        return token;
    }

    /** Keys whose last replica went with an eviction. */
    std::vector<std::string> Orphaned() const {
        std::lock_guard<std::mutex> lock(removed_mu_);
        return orphaned_;
    }

    std::vector<std::pair<std::string, UUID>> RemovedReplicas() const {
        std::lock_guard<std::mutex> lock(removed_mu_);
        return removed_;
    }

    std::filesystem::path storage_dir_;
    std::shared_ptr<ManualClock> clock_;
    BlockRegistry registry_;
    std::shared_ptr<FrequencyTracker> tracker_;
    std::shared_ptr<AcceptingSink> sink_;
    MultiTiler tilers_;
    TilerManager* fast_ = nullptr;
    TilerManager* slow_ = nullptr;
    LocalCopyEngine copier_{LocalTransferConfig{}};
    mutable std::mutex removed_mu_;
    std::vector<std::string> orphaned_;
    std::vector<std::pair<std::string, UUID>> removed_;
    MetadataCallbacks callbacks_;
    std::unique_ptr<MigrationEngine> migration_;
    std::unique_ptr<EvictEngine> evict_;
};

// A round is sized by the caller's target, not by the tier: reclaiming more
// than was asked for would throw away hot data the caller never needed freed.
TEST_F(EvictEngineTest, ColdestVictimsGoFirstAndTheRoundStopsAtTheTarget) {
    for (int i = 0; i < 6; ++i) {
        CommitAndForget(*fast_, "evict/cold/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }
    ASSERT_EQ(fast_->IndexStats().entry_count, 6U);

    auto result = evict_->ReclaimOneRound(RequestFor(2 * kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->candidates_examined, 2U);
    EXPECT_EQ(result->logically_detached_bytes, 2 * kBlockSize);
    EXPECT_EQ(result->physically_reclaimed_bytes, 2 * kBlockSize);
    EXPECT_FALSE(result->deadline_reached);

    // The two oldest committed keys went; the other four stayed.
    EXPECT_FALSE(Present(*fast_, "evict/cold/0"));
    EXPECT_FALSE(Present(*fast_, "evict/cold/1"));
    for (int i = 2; i < 6; ++i) {
        EXPECT_TRUE(Present(*fast_, "evict/cold/" + std::to_string(i)))
            << "round drained past its target at index " << i;
    }
    EXPECT_EQ(fast_->IndexStats().entry_count, 4U);

    const auto removed = RemovedReplicas();
    ASSERT_EQ(removed.size(), 2U);
    EXPECT_EQ(removed[0].first, "evict/cold/0");
    EXPECT_EQ(removed[0].second, fast_->Id());
    EXPECT_EQ(removed[1].first, "evict/cold/1");
}

// Recency and frequency, not commit order, decide who goes first: a key that
// has been read is banded warmer and must outlive the untouched ones.
TEST_F(EvictEngineTest, AReadKeyIsNotTheFirstVictimDespiteBeingOldest) {
    for (int i = 0; i < 4; ++i) {
        CommitAndForget(*fast_, "evict/warm/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }
    for (int i = 0; i < 3; ++i) RecordAccess(*fast_, "evict/warm/0");

    auto result = evict_->ReclaimOneRound(RequestFor(2 * kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(result->logically_detached_bytes, 2 * kBlockSize);

    EXPECT_TRUE(Present(*fast_, "evict/warm/0"));
    EXPECT_FALSE(Present(*fast_, "evict/warm/1"));
    EXPECT_FALSE(Present(*fast_, "evict/warm/2"));
    EXPECT_TRUE(Present(*fast_, "evict/warm/3"));
}

// The central distinction. A reader's snapshot keeps the bytes alive, so the
// round detached them logically but freed nothing; reporting them as
// reclaimed would tell AllocateWithPolicy to retry an allocation that has no
// more room than before.
TEST_F(EvictEngineTest, ADetachedButHeldVictimIsNotReportedAsReclaimed) {
    ImmutableBlock held = Commit(*fast_, "evict/held", 0x11);
    ASSERT_TRUE(static_cast<bool>(held));
    CommitAndForget(*fast_, "evict/held/other", 0x12);
    const size_t usage_before = fast_->Usage();

    auto result = evict_->ReclaimOneRound(RequestFor(kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->candidates_examined, 1U);
    EXPECT_EQ(result->logically_detached_bytes, kBlockSize);
    EXPECT_EQ(result->physically_reclaimed_bytes, 0U);
    EXPECT_EQ(result->free_bytes_after, result->free_bytes_before);
    EXPECT_EQ(fast_->Usage(), usage_before);

    // Logically gone for every new lookup, physically still there.
    EXPECT_FALSE(Present(*fast_, "evict/held"));
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(held.Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, Pattern(kBlockSize, 0x11));

    // Only the last reader going away gives the space back.
    held = ImmutableBlock();
    EXPECT_EQ(fast_->Usage(), usage_before - kBlockSize);
}

// The bracket is what the pool reported before and after, not an accumulator
// over the victims: an allocation nobody indexed still occupies space, and a
// caller that trusted a victim-derived number would over-count its room.
TEST_F(EvictEngineTest, FreeBytesAreReadFromThePoolAroundTheRound) {
    for (int i = 0; i < 4; ++i) {
        CommitAndForget(*fast_, "evict/bracket/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }
    // Allocated but never completed or registered: invisible to the index and
    // to the policy, yet very much charged to the pool.
    auto reserved = fast_->Allocate(kBlockSize);
    ASSERT_TRUE(reserved.has_value()) << toString(reserved.error());

    const size_t free_before = fast_->FreeBytes();
    ASSERT_EQ(free_before, kFastCapacity - 5 * kBlockSize);

    auto result = evict_->ReclaimOneRound(RequestFor(2 * kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->free_bytes_before, free_before);
    EXPECT_EQ(result->free_bytes_after, fast_->FreeBytes());
    EXPECT_EQ(result->free_bytes_after, kFastCapacity - 3 * kBlockSize);
    EXPECT_EQ(result->physically_reclaimed_bytes,
              result->free_bytes_after - result->free_bytes_before);
}

// A token names a block, not a key. The block under this key was replaced
// while the policy still remembered the old one; honouring the token would
// drop the live replacement.
TEST_F(EvictEngineTest, AReplacedBlockIsSkippedAndTheLiveOneSurvives) {
    const std::string key = "evict/replaced";
    ImmutableBlock first = Commit(*fast_, key, 0x21);
    ASSERT_TRUE(static_cast<bool>(first));
    const BlockRegistrationHandle registration = HandleFor(key);

    const BlockId first_id = first.Id();
    ASSERT_TRUE(fast_->Delete(registration, first.Id()).has_value());
    first = ImmutableBlock();
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x22);
    auto allocated = fast_->Allocate(payload.size());
    ASSERT_TRUE(allocated.has_value());
    ASSERT_TRUE(allocated->Write(0, AsBytes(payload)).has_value());
    auto completed = std::move(allocated.value()).Complete(key);
    ASSERT_TRUE(completed.has_value());
    auto second =
        fast_->RegisterWithHandle(std::move(completed.value()), registration);
    ASSERT_TRUE(second.has_value()) << toString(second.error());

    // Put the ordering back to naming the block that is already gone.
    InjectStaleToken(*fast_, registration, first_id, kBlockSize);

    auto result = evict_->ReclaimOneRound(RequestFor(kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->candidates_examined, 1U);
    EXPECT_EQ(result->logically_detached_bytes, 0U);
    EXPECT_EQ(result->physically_reclaimed_bytes, 0U);
    EXPECT_EQ(evict_->Stats().victims_stale, 1U);
    EXPECT_EQ(evict_->Stats().victims_detached, 0U);

    auto live = fast_->Match(registration);
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(live->Id(), second->Id());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(live->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
    EXPECT_TRUE(RemovedReplicas().empty());

    // And the ordering has been repaired rather than merely skipped: the live
    // block is offered next time. Without that, a block whose token went stale
    // once would never be a candidate again and its bytes would be stranded.
    const auto repaired = fast_->Eviction()->SelectVictims(kBlockSize);
    ASSERT_EQ(repaired.size(), 1U);
    EXPECT_EQ(repaired[0].block_id, second->Id());
}

// Delete-then-recreate gives the key a new identity. The old token upgrades
// and even matches a block, so only the canonical check stands between it and
// the data a caller has just written under the same name.
TEST_F(EvictEngineTest, ARetiredRegistrationIsSkippedAndTheNewKeySurvives) {
    const std::string key = "evict/recreated";
    ImmutableBlock first = Commit(*fast_, key, 0x31);
    ASSERT_TRUE(static_cast<bool>(first));
    // Held on purpose: the weak handle in the victim must still upgrade, so
    // the skip is decided by retirement and canonicality, not by a dead
    // pointer.
    const BlockRegistrationHandle retired = HandleFor(key);

    {
        auto guard = retired.LockMutation();
        retired.Retire(guard);
    }
    ImmutableBlock recreated = Commit(*fast_, key, 0x32);
    ASSERT_TRUE(static_cast<bool>(recreated));
    ASSERT_NE(recreated.Registration(), retired.Id());

    auto result = evict_->ReclaimOneRound(RequestFor(kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->candidates_examined, 1U);
    EXPECT_EQ(result->logically_detached_bytes, 0U);
    EXPECT_EQ(evict_->Stats().victims_stale, 1U);

    // The key a caller can see today is untouched.
    EXPECT_TRUE(Present(*fast_, key));
    auto live = fast_->Match(HandleFor(key));
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(live->Id(), recreated.Id());
    EXPECT_TRUE(RemovedReplicas().empty());
}

// A request thread waits on this round. Overrunning the deadline would turn a
// full tier into a latency cliff, so an expired deadline means no candidate is
// even examined -- and the round still returns instead of retrying.
TEST_F(EvictEngineTest, AnExpiredDeadlineExaminesNothingAndRemovesNothing) {
    for (int i = 0; i < 6; ++i) {
        CommitAndForget(*fast_, "evict/deadline/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }
    const size_t usage_before = fast_->Usage();

    ReclaimRequest request = RequestFor(3 * kBlockSize);
    request.deadline = clock_->Now() + 10ms;
    clock_->Advance(20ms);

    auto result = evict_->ReclaimOneRound(request);
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_TRUE(result->deadline_reached);
    EXPECT_EQ(result->candidates_examined, 0U);
    EXPECT_EQ(result->logically_detached_bytes, 0U);
    EXPECT_EQ(result->physically_reclaimed_bytes, 0U);
    EXPECT_EQ(fast_->IndexStats().entry_count, 6U);
    EXPECT_EQ(fast_->Usage(), usage_before);
    EXPECT_TRUE(RemovedReplicas().empty());

    const EvictStats stats = evict_->Stats();
    EXPECT_EQ(stats.rounds, 1U);
    EXPECT_EQ(stats.deadline_reached, 1U);
    EXPECT_EQ(stats.victims_detached, 0U);
}

// An unknown tier is a caller bug, not an empty tier: it must not be reported
// as a round that found nothing to reclaim.
TEST_F(EvictEngineTest, AnUnknownTilerIsRejectedAndCountsNoRound) {
    CommitAndForget(*fast_, "evict/unknown", 0x41);

    ReclaimRequest request = RequestFor(kBlockSize);
    request.tiler_id = kUnknownTiler;
    auto result = evict_->ReclaimOneRound(request);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(evict_->Stats().rounds, 0U);
    EXPECT_TRUE(Present(*fast_, "evict/unknown"));
}

// A watermark loop that has nothing to reclaim still calls in; asking for zero
// bytes must not cost the tier a block.
TEST_F(EvictEngineTest, AZeroTargetReclaimsNothing) {
    for (int i = 0; i < 3; ++i) {
        CommitAndForget(*fast_, "evict/zero/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }
    const size_t usage_before = fast_->Usage();

    auto result = evict_->ReclaimOneRound(RequestFor(0));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    EXPECT_EQ(result->candidates_examined, 0U);
    EXPECT_EQ(result->logically_detached_bytes, 0U);
    EXPECT_EQ(result->physically_reclaimed_bytes, 0U);
    EXPECT_FALSE(result->deadline_reached);
    EXPECT_EQ(result->free_bytes_before, result->free_bytes_after);
    EXPECT_EQ(fast_->IndexStats().entry_count, 3U);
    EXPECT_EQ(fast_->Usage(), usage_before);
    EXPECT_TRUE(RemovedReplicas().empty());
    EXPECT_EQ(evict_->Stats().rounds, 1U);
    EXPECT_EQ(evict_->Stats().victims_detached, 0U);
}

// An evict command names one replica of one identity. The other tier's copy is
// a different block under the same registration, and Master must be told about
// exactly the replica that went.
TEST_F(EvictEngineTest, ExecuteRemovesTheNamedReplicaAndLeavesTheOtherTier) {
    const std::string key = "evict/execute";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x51);
    ImmutableBlock fast_block = Commit(*fast_, key, 0x51);
    ASSERT_TRUE(static_cast<bool>(fast_block));
    const BlockRegistrationHandle registration = HandleFor(key);

    auto slow_allocation = slow_->Allocate(payload.size());
    ASSERT_TRUE(slow_allocation.has_value());
    ASSERT_TRUE(copier_.Copy(fast_block, slow_allocation.value()).has_value());
    auto slow_completed = std::move(slow_allocation.value()).Complete(key);
    ASSERT_TRUE(slow_completed.has_value());
    auto slow_replica = slow_->RegisterWithHandle(
        std::move(slow_completed.value()), registration);
    ASSERT_TRUE(slow_replica.has_value()) << toString(slow_replica.error());

    const BlockId evicted_id = fast_block.Id();
    fast_block = ImmutableBlock();
    auto executed =
        evict_->EvictOne(EvictToken(registration, fast_->Id(), evicted_id));
    ASSERT_TRUE(executed.has_value()) << toString(executed.error());

    EXPECT_FALSE(fast_->Match(registration).has_value());
    EXPECT_EQ(fast_->Usage(), 0U);

    // The registration outlives the evicted replica because another one
    // exists, and that copy is still readable.
    ASSERT_TRUE(registry_.Match(key).has_value());
    auto surviving = slow_->Match(registration);
    ASSERT_TRUE(surviving.has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(surviving->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);

    const auto removed = RemovedReplicas();
    ASSERT_EQ(removed.size(), 1U);
    EXPECT_EQ(removed[0].first, key);
    EXPECT_EQ(removed[0].second, fast_->Id());
    EXPECT_EQ(evict_->Stats().victims_detached, 1U);
}

// ---------------------------------------------------------------------------
// Reclamation is tier-local (design section 4.1)
// ---------------------------------------------------------------------------

// The decided trade-off, asserted rather than left implicit: a replica that
// exists nowhere else IS destroyed. Reclamation does not consult other tiers,
// does not demote and does not wait for an offload -- keeping the object alive
// is the offload pipeline's job, and if it has not run yet the object is gone.
//
// This test exists so the loss is a stated property with a counter behind it.
// The previous behaviour (demote instead of drop) is deliberately gone.
TEST_F(EvictEngineTest, AnUnreadSoleReplicaIsDestroyedAndCounted) {
    const std::string key = "evict/sole";
    ImmutableBlock original = Commit(*fast_, key, 0x71);
    ASSERT_TRUE(static_cast<bool>(original));
    original = ImmutableBlock();  // release the snapshot so the space can go

    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    ASSERT_FALSE(slow_->Match(*registration).has_value())
        << "the fixture must start with a single replica";

    auto result = evict_->ReclaimOneRound(RequestFor(kBlockSize));
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(result->logically_detached_bytes, kBlockSize);
    EXPECT_EQ(result->physically_reclaimed_bytes, kBlockSize)
        << "the point of dropping it was to get the bytes back";

    EXPECT_FALSE(Present(*fast_, key)) << "the fast tier was not reclaimed";
    EXPECT_FALSE(slow_->Match(*registration).has_value())
        << "nothing may be copied to another tier on the reclaim path";

    EXPECT_EQ(evict_->Stats().victims_detached, 1U);
    EXPECT_EQ(evict_->Stats().victims_sole_replica, 1U)
        << "the accepted data loss has to be countable, or it is "
           "indistinguishable from a bug";
}

// Master must be told even when the object is gone entirely. Skipping the
// report for a destroyed object would leave Master routing readers to a
// replica that no longer exists, turning accepted loss into an unexplained
// read failure somewhere else.
TEST_F(EvictEngineTest, DestroyingTheLastReplicaIsStillReportedToMaster) {
    const std::string key = "evict/sole/reported";
    ImmutableBlock original = Commit(*fast_, key, 0x72);
    ASSERT_TRUE(static_cast<bool>(original));
    original = ImmutableBlock();

    ASSERT_TRUE(evict_->ReclaimOneRound(RequestFor(kBlockSize)).has_value());

    const auto removed = RemovedReplicas();
    ASSERT_EQ(removed.size(), 1U);
    EXPECT_EQ(removed[0].first, key);
    EXPECT_EQ(removed[0].second, fast_->Id());
}

// And the access tracker has to stop tracking it: the key exists nowhere, so
// leaving it in the tracker reports a hot key that cannot be read.
TEST_F(EvictEngineTest, LosingTheLastReplicaForgetsTheKey) {
    const std::string key = "evict/sole/forgotten";
    ImmutableBlock original = Commit(*fast_, key, 0x73);
    ASSERT_TRUE(static_cast<bool>(original));
    original = ImmutableBlock();

    ASSERT_TRUE(evict_->ReclaimOneRound(RequestFor(kBlockSize)).has_value());
    EXPECT_EQ(Orphaned(), std::vector<std::string>{key});
}

// The other half of the same rule: a replica that DOES exist elsewhere is
// dropped just as unconditionally, and the surviving copy is untouched. What
// changed is that the engine never had to ask which case it was in.
TEST_F(EvictEngineTest, ARedundantReplicaIsDroppedAndTheOtherOneIsIntact) {
    const std::string key = "evict/redundant";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x74);
    ImmutableBlock fast_block = Commit(*fast_, key, 0x74);
    ASSERT_TRUE(static_cast<bool>(fast_block));
    const BlockRegistrationHandle registration = HandleFor(key);

    // A second replica on the slow tier, sharing the one registration.
    auto allocated = slow_->Allocate(payload.size());
    ASSERT_TRUE(allocated.has_value());
    ASSERT_TRUE(allocated->Write(0, AsBytes(payload)).has_value());
    auto completed = std::move(allocated.value()).Complete(key);
    ASSERT_TRUE(completed.has_value());
    ASSERT_TRUE(
        slow_->RegisterWithHandle(std::move(completed.value()), registration)
            .has_value());
    fast_block = ImmutableBlock();

    ASSERT_TRUE(evict_->ReclaimOneRound(RequestFor(kBlockSize)).has_value());

    EXPECT_FALSE(fast_->Match(registration).has_value());
    EXPECT_EQ(evict_->Stats().victims_sole_replica, 0U)
        << "a redundant drop is not data loss and must not be counted as it";
    EXPECT_TRUE(Orphaned().empty())
        << "the key still exists, so the tracker must keep it";

    auto survivor = slow_->Match(registration);
    ASSERT_TRUE(survivor.has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(survivor->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(EvictEngineTest, ExecuteOnAStaleCommandReportsNotFoundAndKeepsTheBlock) {
    const std::string key = "evict/execute/stale";
    ImmutableBlock block = Commit(*fast_, key, 0x61);
    ASSERT_TRUE(static_cast<bool>(block));
    const BlockRegistrationHandle registration = HandleFor(key);

    // An unknown source tier is not a stale victim: nothing was examined.
    auto unknown_tier =
        evict_->EvictOne(EvictToken(registration, kUnknownTiler, block.Id()));
    ASSERT_FALSE(unknown_tier.has_value());
    EXPECT_EQ(unknown_tier.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(evict_->Stats().victims_stale, 0U);

    BlockId wrong = block.Id();
    wrong.local_id += 1;
    auto stale = evict_->EvictOne(EvictToken(registration, fast_->Id(), wrong));
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(evict_->Stats().victims_stale, 1U);
    EXPECT_EQ(evict_->Stats().victims_detached, 0U);
    EXPECT_TRUE(fast_->Match(registration).has_value());
    EXPECT_TRUE(RemovedReplicas().empty());

    // The same command with the right block id is accepted, which proves the
    // refusal above was about the block and not about the key.
    const BlockId correct = block.Id();
    block = ImmutableBlock();
    ASSERT_TRUE(evict_->EvictOne(EvictToken(registration, fast_->Id(), correct))
                    .has_value());
    EXPECT_FALSE(fast_->Match(registration).has_value());
    EXPECT_EQ(RemovedReplicas().size(), 1U);
}

// The counters have to add up across rounds, because the caller uses them to
// decide whether reclaiming is working at all. Every examined candidate ends
// up in exactly one of detached or stale, and one call is one round.
TEST_F(EvictEngineTest, StatsCountRoundsVictimsAndDeadlinesConsistently) {
    // The stale record is built first so it sits at the cold end and is the
    // first candidate the round meets. Re-inserting it after the others would
    // put it at the warm end, where no round of this size would reach it and
    // the test would silently stop exercising the stale path.
    CommitAndForget(*fast_, "evict/stats/0", 0);
    const BlockRegistrationHandle stale_handle = HandleFor("evict/stats/0");
    auto stale_block = fast_->Match(stale_handle);
    ASSERT_TRUE(stale_block.has_value());
    const BlockId stale_id = stale_block->Id();
    stale_block.value() = ImmutableBlock();
    ASSERT_TRUE(fast_->Delete(stale_handle).has_value());
    InjectStaleToken(*fast_, stale_handle, stale_id, kBlockSize);

    for (int i = 1; i < 4; ++i) {
        CommitAndForget(*fast_, "evict/stats/" + std::to_string(i),
                        static_cast<uint8_t>(i));
    }

    auto first = evict_->ReclaimOneRound(RequestFor(2 * kBlockSize));
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    EXPECT_EQ(first->candidates_examined, 2U);
    // The shortfall is left to the caller: the round does not go back for
    // another victim to make up for the stale one.
    EXPECT_EQ(first->logically_detached_bytes, kBlockSize);

    ReclaimRequest expired = RequestFor(2 * kBlockSize);
    expired.deadline = clock_->Now();
    auto second = evict_->ReclaimOneRound(expired);
    ASSERT_TRUE(second.has_value()) << toString(second.error());
    EXPECT_TRUE(second->deadline_reached);

    auto third = evict_->ReclaimOneRound(RequestFor(0));
    ASSERT_TRUE(third.has_value()) << toString(third.error());

    const EvictStats stats = evict_->Stats();
    EXPECT_EQ(stats.rounds, 3U);
    EXPECT_EQ(stats.victims_detached, 1U);
    EXPECT_EQ(stats.victims_stale, 1U);
    EXPECT_EQ(stats.deadline_reached, 1U);
    EXPECT_EQ(stats.victims_detached + stats.victims_stale,
              first->candidates_examined + second->candidates_examined +
                  third->candidates_examined);
}

}  // namespace mooncake::v2
