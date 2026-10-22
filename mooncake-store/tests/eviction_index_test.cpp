#include "metadata/lease_eviction_index.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "allocator.h"
#include "metadata/namespace.h"
#include "object_test_helpers.h"

namespace mooncake {
namespace {

using Clock = std::chrono::system_clock;
using metadata::EvictionIndex;
using std::chrono::seconds;

// A namespace whose writes the lease eviction index follows, and an allocator
// that hands out the memory replicas its objects hold.
class LeaseEvictionIndexTest : public ::testing::Test {
   protected:
    // Publishes `key` holding one memory replica in `status`, with its lease
    // ending at `deadline`.
    void Publish(const std::string& key, Clock::time_point deadline,
                 ReplicaStatus status = ReplicaStatus::COMPLETE,
                 bool hard_pinned = false) {
        auto guard = ns_.objects.WriteKey(key);
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128), status);
        (void)guard.Publish(/*part_count=*/1, TenantId(), key, std::string(),
                            ObjectDataType::UNKNOWN, hard_pinned, std::nullopt);
        auto begin_write = guard.metadata().BeginWrite(
            /*index=*/0, /*length=*/128, std::move(replicas), UUID{1, 2},
            /*session_id=*/"", Clock::time_point{});
        ASSERT_TRUE(begin_write.has_value());
        SetDeadline(guard.metadata(), deadline);
    }

    static void SetDeadline(const ObjectMetadata& metadata,
                            Clock::time_point deadline) {
        SpinLocker locker(&metadata.lock);
        metadata.lease_->SetDeadline(deadline);
    }

    // Every key a walk at `now` hands out, in order.
    std::vector<EvictionIndex::Entry> Walk(Clock::time_point now) const {
        auto cursor = ns_.eviction_index().Scan(now);
        std::vector<EvictionIndex::Entry> entries;
        while (auto entry = cursor->Next()) {
            entries.push_back(std::move(*entry));
        }
        return entries;
    }

    std::vector<std::string> DueKeys(Clock::time_point now) const {
        std::vector<std::string> keys;
        for (const auto& entry : Walk(now)) {
            keys.push_back(entry.key);
        }
        return keys;
    }

    size_t Filed() const { return ns_.eviction_index().Scan(t0_)->Total(); }

    const Clock::time_point t0_{seconds(1000)};
    // Declared before the namespace, so it outlives the replicas it gave out.
    std::shared_ptr<OffsetBufferAllocator> allocator_ =
        std::make_shared<OffsetBufferAllocator>("segment", 0x100000000ULL,
                                                64 * 1024 * 1024, "segment");
    metadata::Namespace ns_;
};

TEST_F(LeaseEvictionIndexTest, FilesOnlyObjectsEvictionCanTakeMemoryFrom) {
    Publish("complete", t0_);
    Publish("processing", t0_, ReplicaStatus::PROCESSING);
    Publish("hard_pinned", t0_, ReplicaStatus::COMPLETE, /*hard_pinned=*/true);
    {
        // No replica at all.
        auto guard = ns_.objects.WriteKey("empty");
        (void)test::PublishEnvelope(guard);
    }

    EXPECT_EQ(Filed(), 1u);
    EXPECT_EQ(DueKeys(t0_ + seconds(1)), std::vector<std::string>{"complete"});
}

TEST_F(LeaseEvictionIndexTest, WalksKeysOldestLeaseFirstUpToNow) {
    Publish("late", t0_ + seconds(5));
    Publish("middle", t0_ + seconds(1));
    Publish("early", t0_);

    // A bucket is due once it has begun, so "late" is not yet.
    EXPECT_EQ(DueKeys(t0_ + seconds(2)),
              (std::vector<std::string>{"early", "middle"}));
    EXPECT_EQ(DueKeys(t0_ + seconds(6)),
              (std::vector<std::string>{"early", "middle", "late"}));
}

TEST_F(LeaseEvictionIndexTest, KeysInOneBucketShareARank) {
    Publish("a", t0_);
    Publish("b", t0_);
    Publish("c", t0_ + seconds(1));

    const auto entries = Walk(t0_ + seconds(5));
    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].rank, entries[1].rank);
    EXPECT_EQ(entries[2].key, "c");
    EXPECT_LT(entries[1].rank, entries[2].rank);
}

TEST_F(LeaseEvictionIndexTest, AReadLeavesTheKeyAndStillAtCatchesIt) {
    Publish("k", t0_);
    Publish("unmoved", t0_);

    // A read extends the lease without a write lock, so the key stays filed
    // at its old deadline.
    {
        auto guard = ns_.objects.Read("k");
        ASSERT_TRUE(guard.has_value());
        SetDeadline(guard->metadata(), t0_ + seconds(10));
    }
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    std::vector<EvictionIndex::Entry> entries;
    while (auto entry = cursor->Next()) {
        entries.push_back(std::move(*entry));
    }
    ASSERT_EQ(entries.size(), 2u);

    // Under the write lock the cursor sees "k" moved on; releasing that lock
    // files it at its real deadline.
    for (const auto& entry : entries) {
        auto guard = ns_.objects.Write(entry.key);
        ASSERT_TRUE(guard.has_value());
        EXPECT_EQ(cursor->StillAt(*guard, entry), entry.key == "unmoved");
    }
    EXPECT_EQ(DueKeys(t0_ + seconds(1)), std::vector<std::string>{"unmoved"});
    EXPECT_EQ(DueKeys(t0_ + seconds(11)),
              (std::vector<std::string>{"unmoved", "k"}));
}

TEST_F(LeaseEvictionIndexTest, KeyLeavesTheIndexWithItsObjectOrItsLastMemory) {
    Publish("torn_down", t0_);
    Publish("evicted", t0_);
    ASSERT_EQ(Filed(), 2u);

    {
        auto guard = ns_.objects.Write("torn_down");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    {
        auto guard = ns_.objects.Write("evicted");
        ASSERT_TRUE(guard.has_value());
        (void)guard->metadata().PopReplicas();
    }

    EXPECT_EQ(Filed(), 0u);
    EXPECT_TRUE(DueKeys(t0_ + seconds(1)).empty());
}

// An index that counts what the namespace tells it.
class CountingIndex final : public EvictionIndex {
   public:
    CountingIndex(bool observes_reads, int* reads, int* writes)
        : EvictionIndex(observes_reads), reads_(reads), writes_(writes) {}

    void OnRead(const route::ReadGuard&) override { ++*reads_; }
    void OnWrite(const route::WriteGuard&) override { ++*writes_; }
    std::unique_ptr<Cursor> Scan(Clock::time_point) const override {
        return nullptr;
    }

   private:
    int* reads_;
    int* writes_;
};

TEST(EvictionIndexWiringTest, ReadsReachOnlyAnIndexThatObservesThem) {
    for (const bool observes_reads : {true, false}) {
        int reads = 0;
        int writes = 0;
        metadata::Namespace ns(
            TenantId(), nullptr, nullptr,
            std::make_unique<CountingIndex>(observes_reads, &reads, &writes));
        ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);
        EXPECT_EQ(writes, 1);

        ASSERT_TRUE(ns.objects.Read("k").has_value());
        EXPECT_EQ(reads, observes_reads ? 1 : 0);
        ASSERT_TRUE(ns.objects.Write("k").has_value());
        EXPECT_EQ(writes, 2);
    }
}

}  // namespace
}  // namespace mooncake
