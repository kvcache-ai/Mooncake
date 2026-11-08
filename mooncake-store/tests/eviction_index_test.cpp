#include "metadata/lease_eviction_index.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "allocator.h"
#include "metadata/namespace.h"
#include "object_test_helpers.h"

namespace allocation_test {
struct Failure : std::bad_alloc {
    explicit Failure(size_t attempt) : attempt(attempt) {}
    size_t attempt;
};

// Track only this thread and only allocations made inside the probe. No heap
// storage is used by the probe or by the replacement allocation functions.
class Probe;
thread_local Probe* active = nullptr;

class Probe {
   public:
    Probe() : previous_(active) { active = this; }
    ~Probe() { Stop(); }
    Probe(const Probe&) = delete;
    Probe& operator=(const Probe&) = delete;

    void Stop() {
        if (active == this) active = previous_;
    }
    void FailAfter(size_t successful_attempts) {
        fail_at_ = attempts + successful_attempts;
    }
    bool Allow(size_t size) {
        last_request = size;
        if (attempts++ >= fail_at_) {
            ++failures;
            return false;
        }
        return true;
    }
    void Allocated(void* ptr, size_t size) {
        for (auto& allocation : allocations_) {
            if (allocation.ptr == nullptr) {
                allocation = {ptr, size};
                ++live_blocks;
                live_bytes += size;
                return;
            }
        }
        overflow = true;
    }
    void Freed(void* ptr) {
        for (auto& allocation : allocations_) {
            if (ptr != nullptr && allocation.ptr == ptr) {
                --live_blocks;
                live_bytes -= allocation.size;
                allocation = {};
                return;
            }
        }
    }

    size_t attempts = 0;
    size_t failures = 0;
    size_t last_request = 0;
    size_t live_blocks = 0;
    size_t live_bytes = 0;
    bool overflow = false;

   private:
    struct Allocation {
        void* ptr = nullptr;
        size_t size = 0;
    };
    std::array<Allocation, 32> allocations_{};
    Probe* previous_;
    size_t fail_at_ = std::numeric_limits<size_t>::max();
};

void* Allocate(size_t size, size_t alignment = alignof(std::max_align_t)) {
    if (active != nullptr && !active->Allow(size)) {
        throw Failure(active->attempts - 1);
    }
    const size_t requested = size;
    size = size == 0 ? 1 : size;
    if (alignment > alignof(std::max_align_t)) {
        const size_t padding = (alignment - size % alignment) % alignment;
        if (size > std::numeric_limits<size_t>::max() - padding) {
            throw std::bad_alloc();
        }
        size += padding;
    }
    for (;;) {
        void* ptr = alignment > alignof(std::max_align_t)
                        ? std::aligned_alloc(alignment, size)
                        : std::malloc(size);
        if (ptr != nullptr) {
            if (active != nullptr) active->Allocated(ptr, requested);
            return ptr;
        }
        const auto handler = std::get_new_handler();
        if (handler == nullptr) throw std::bad_alloc();
        handler();
    }
}

void Free(void* ptr) noexcept {
    if (active != nullptr) active->Freed(ptr);
    std::free(ptr);
}
}  // namespace allocation_test

// Cover ordinary/aligned, scalar/array and nothrow forms with matching frees.
// Mixing ASan's default new with our free would hide the intended test failure
// behind an allocation/deallocation mismatch (gtest also uses nothrow new).
void* operator new(size_t size) { return allocation_test::Allocate(size); }
void* operator new[](size_t size) { return ::operator new(size); }
void* operator new(size_t size, std::align_val_t alignment) {
    return allocation_test::Allocate(size, static_cast<size_t>(alignment));
}
void* operator new[](size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
    return ::operator new(size, tag);
}
void* operator new(size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size, alignment);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t size, std::align_val_t alignment,
                     const std::nothrow_t& tag) noexcept {
    return ::operator new(size, alignment, tag);
}
void operator delete(void* ptr) noexcept { allocation_test::Free(ptr); }
void operator delete[](void* ptr) noexcept { allocation_test::Free(ptr); }
void operator delete(void* ptr, size_t) noexcept { allocation_test::Free(ptr); }
void operator delete[](void* ptr, size_t) noexcept {
    allocation_test::Free(ptr);
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
    allocation_test::Free(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
    allocation_test::Free(ptr);
}
void operator delete(void* ptr, std::align_val_t) noexcept {
    allocation_test::Free(ptr);
}
void operator delete[](void* ptr, std::align_val_t) noexcept {
    allocation_test::Free(ptr);
}
void operator delete(void* ptr, size_t, std::align_val_t) noexcept {
    allocation_test::Free(ptr);
}
void operator delete[](void* ptr, size_t, std::align_val_t) noexcept {
    allocation_test::Free(ptr);
}
void operator delete(void* ptr, std::align_val_t,
                     const std::nothrow_t&) noexcept {
    allocation_test::Free(ptr);
}
void operator delete[](void* ptr, std::align_val_t,
                       const std::nothrow_t&) noexcept {
    allocation_test::Free(ptr);
}

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
        PublishIn(guard, key, std::string(), status, hard_pinned);
        SetDeadline(guard.metadata(), deadline);
    }

    // Publishes `key` in `group_id`, setting the lease its members share.
    void PublishInGroup(const std::string& key, const std::string& group_id,
                        Clock::time_point deadline) {
        auto guard = ns_.objects.WriteKey(key);
        PublishIn(guard, key, group_id);
        SetDeadline(guard.metadata(), deadline);
    }

    // Publishes under `guard`, which holds `key` and no object.
    void PublishIn(const route::WriteGuard& guard, const std::string& key,
                   const std::string& group_id,
                   ReplicaStatus status = ReplicaStatus::COMPLETE,
                   bool hard_pinned = false) {
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128), status);
        (void)guard.Publish(UUID{1, 2}, Clock::time_point{}, 128,
                            std::move(replicas), std::nullopt, hard_pinned,
                            ObjectDataType::UNKNOWN, group_id, TenantId(), key);
    }

    static void SetDeadline(const ObjectMetadata& metadata,
                            Clock::time_point deadline) {
        SpinLocker locker(&metadata.lock);
        metadata.lease_->SetDeadline(deadline);
    }

    // An entry a walk handed out, with its keys copied off the cursor.
    struct WalkedEntry {
        std::vector<std::string> keys;
        EvictionIndex::Rank rank;
    };

    // Every entry a walk at `now` hands out, in order.
    std::vector<WalkedEntry> Walk(Clock::time_point now) const {
        auto cursor = ns_.eviction_index().Scan(now);
        std::vector<WalkedEntry> entries;
        while (auto entry = cursor->Next()) {
            entries.push_back(WalkedEntry{
                {entry->keys.begin(), entry->keys.end()}, entry->rank});
        }
        return entries;
    }

    // The keys a walk at `now` hands out, in order, one entry's together.
    std::vector<std::string> DueKeys(Clock::time_point now) const {
        std::vector<std::string> keys;
        for (const auto& entry : Walk(now)) {
            keys.insert(keys.end(), entry.keys.begin(), entry.keys.end());
        }
        return keys;
    }

    // The entries a walk at `now` hands out, each as the set of its keys.
    std::vector<std::set<std::string>> DueEntries(Clock::time_point now) const {
        std::vector<std::set<std::string>> entries;
        for (const auto& entry : Walk(now)) {
            entries.emplace_back(entry.keys.begin(), entry.keys.end());
        }
        return entries;
    }

    size_t Filed() const { return ns_.eviction_index().Scan(t0_)->Total(); }

    std::vector<std::string> CleanupKeys(bool grouped, bool refill) {
        std::vector<std::string> keys;
        if (refill) {
            keys.emplace_back("prefix");
            Publish(keys.back(), t0_);
        }
        for (size_t i = 0; i < 33; ++i) {
            keys.push_back(std::string(512, 'k') + std::to_string(i));
            const auto deadline = t0_ + seconds(refill ? 1 : 0);
            if (grouped) {
                PublishInGroup(keys.back(), "cleanup", deadline);
            } else {
                Publish(keys.back(), deadline);
            }
        }
        return keys;
    }

    void CheckAllocationFailures(bool grouped, bool refill) {
        const auto keys = CleanupKeys(grouped, refill);
        auto calibration = ns_.eviction_index().Scan(t0_ + seconds(2));
        allocation_test::Probe count;
        if (refill) (void)calibration->Next();
        const size_t start = count.attempts;
        const auto copied = calibration->Next();
        const size_t allocations = count.attempts - start;
        count.Stop();
        ASSERT_TRUE(copied.has_value());
        ASSERT_GT(allocations, 1u);
        // Seal is the last allocating phase of ReadBucket. Its keys_ starts
        // empty, or has only the prefix's capacity. The final growth occurs
        // after earlier string_views have already been appended.
        ASSERT_GE(count.last_request, 33 * sizeof(std::string_view));
        ASSERT_FALSE(count.overflow);
        calibration.reset();
        RecordProperty("allocation_faults", static_cast<int>(allocations));

        for (size_t ordinal = 0; ordinal < allocations; ++ordinal) {
            SCOPED_TRACE(ordinal);
            auto peer = ns_.eviction_index().Scan(t0_ + seconds(2));
            auto peer_entry = peer->Next();
            auto cursor = ns_.eviction_index().Scan(t0_ + seconds(2));
            allocation_test::Probe probe;
            if (refill) (void)cursor->Next();
            const size_t before = probe.attempts;
            probe.FailAfter(ordinal);
            bool caught = false;
            size_t thrown_attempt = 0;
            try {
                (void)cursor->Next();
            } catch (const allocation_test::Failure& failure) {
                caught = true;
                thrown_attempt = failure.attempt;
            }
            const size_t retained = probe.live_blocks;
#if MOONCAKE_ENABLE_CUSTOM_EVICTION
            bool terminal = true;
            // On an unfixed baseline, do not ask an unsealed snapshot for
            // an entry. Report retained buffers and safely destroy it instead.
            if (caught && retained == 0) {
                try {
                    terminal = !cursor->Next() && !cursor->Next();
                } catch (...) {
                    terminal = false;
                }
            }
#endif
            cursor.reset();
            probe.Stop();
            EXPECT_TRUE(caught);
            EXPECT_EQ(thrown_attempt, before + ordinal);
            EXPECT_EQ(probe.failures, 1u);
            EXPECT_EQ(probe.attempts, before + ordinal + 1);
#if MOONCAKE_ENABLE_CUSTOM_EVICTION
            EXPECT_EQ(retained, 0u);
            EXPECT_TRUE(terminal);
#else
            // Main retains any buffers allocated before the failure. Do not
            // retry a partial, unsealed copy; destruction must still free it.
            if (ordinal > 0 || refill) {
                EXPECT_GT(retained, 0u);
            } else {
                EXPECT_EQ(retained, 0u);
            }
#endif
            EXPECT_EQ(probe.live_blocks, 0u);
            EXPECT_EQ(probe.live_bytes, 0u);
            EXPECT_FALSE(probe.overflow);

            size_t seen = 0;
            while (peer_entry) {
                for (const auto key : peer_entry->keys) {
                    ASSERT_LT(seen, keys.size());
                    EXPECT_EQ(key, keys[seen++]);
                }
                peer_entry = peer->Next();
            }
            EXPECT_EQ(seen, keys.size());
        }
    }

    void CheckExhaustion(bool grouped, bool refill) {
        const auto keys = CleanupKeys(grouped, refill);
        auto cursor = ns_.eviction_index().Scan(t0_ + seconds(2));
        allocation_test::Probe probe;
        bool correct = true;
        size_t seen = 0;
        while (seen < keys.size()) {
            const auto entry = cursor->Next();
            if (!entry) {
                correct = false;
                break;
            }
            for (const auto key : entry->keys) {
                if (seen >= keys.size() || key != keys[seen]) correct = false;
                ++seen;
            }
        }
        // The last entry remains readable until the next Next call. All four
        // buffers are still owned here, even though the final entry was
        // returned.
        const size_t live_before = probe.live_blocks;
        const size_t bytes_before = probe.live_bytes;
        const size_t attempts_before = probe.attempts;
        probe.FailAfter(0);
        const bool terminal = !cursor->Next() && !cursor->Next();
        const size_t live_after = probe.live_blocks;
        const size_t bytes_after = probe.live_bytes;
        cursor.reset();
        probe.Stop();
        EXPECT_TRUE(correct);
        EXPECT_EQ(seen, keys.size());
        EXPECT_EQ(live_before, 4u);
        EXPECT_GT(bytes_before, 0u);
#if MOONCAKE_ENABLE_CUSTOM_EVICTION
        EXPECT_EQ(live_after, 0u);
        EXPECT_EQ(bytes_after, 0u);
#else
        // Default OFF preserves main's capacity retention until destruction.
        EXPECT_EQ(live_after, live_before);
        EXPECT_EQ(bytes_after, bytes_before);
#endif
        EXPECT_EQ(probe.live_blocks, 0u);
        EXPECT_EQ(probe.attempts, attempts_before);
        EXPECT_EQ(probe.failures, 0u);
        EXPECT_FALSE(probe.overflow);
        EXPECT_TRUE(terminal);
        RecordProperty("snapshot_requested_bytes",
                       static_cast<int>(bytes_before));
        RecordProperty("terminal_requested_bytes",
                       static_cast<int>(bytes_after));
    }

    const Clock::time_point t0_{seconds(1000)};
    // Declared before the namespace, so it outlives the replicas it gave out.
    std::shared_ptr<OffsetBufferAllocator> allocator_ =
        std::make_shared<OffsetBufferAllocator>("segment", 0x100000000ULL,
                                                64 * 1024 * 1024, "segment");
    metadata::Namespace ns_;
};

TEST(LeaseCursorAllocationTest, CoversAllAllocationForms) {
    const auto aligned = std::align_val_t{64};
    allocation_test::Probe probe;
    std::array<void*, 8> pointers{::operator new(17),
                                  ::operator new[](19),
                                  ::operator new(23, std::nothrow),
                                  ::operator new[](29, std::nothrow),
                                  ::operator new(31, aligned),
                                  ::operator new[](37, aligned),
                                  ::operator new(41, aligned, std::nothrow),
                                  ::operator new[](43, aligned, std::nothrow)};
    const size_t blocks = probe.live_blocks;
    ::operator delete(pointers[0]);
    ::operator delete[](pointers[1], size_t{19});
    ::operator delete(pointers[2], std::nothrow);
    ::operator delete[](pointers[3], std::nothrow);
    ::operator delete(pointers[4], aligned);
    ::operator delete[](pointers[5], size_t{37}, aligned);
    ::operator delete(pointers[6], aligned, std::nothrow);
    ::operator delete[](pointers[7], aligned, std::nothrow);
    probe.FailAfter(0);
    void* ordinary_failure = ::operator new(11, std::nothrow);
    void* aligned_failure = ::operator new[](13, aligned, std::nothrow);
    probe.Stop();
    EXPECT_EQ(blocks, 8u);
    EXPECT_EQ(probe.attempts, 10u);
    EXPECT_EQ(probe.failures, 2u);
    EXPECT_EQ(probe.live_blocks, 0u);
    EXPECT_EQ(probe.live_bytes, 0u);
    EXPECT_FALSE(probe.overflow);
    EXPECT_EQ(ordinary_failure, nullptr);
    EXPECT_EQ(aligned_failure, nullptr);
}

TEST_F(LeaseEvictionIndexTest, ExhaustionRespectsConfiguredBufferLifetime) {
    CheckExhaustion(false, false);
}
TEST_F(LeaseEvictionIndexTest,
       GroupExhaustionRespectsConfiguredBufferLifetime) {
    CheckExhaustion(true, false);
}
TEST_F(LeaseEvictionIndexTest,
       MultipleBucketExhaustionRespectsConfiguredBufferLifetime) {
    CheckExhaustion(false, true);
}
TEST_F(LeaseEvictionIndexTest,
       GroupRefillExhaustionRespectsConfiguredBufferLifetime) {
    CheckExhaustion(true, true);
}

TEST_F(LeaseEvictionIndexTest, EmptyCursorRemainsTerminalAfterPublication) {
    auto cursor = ns_.eviction_index().Scan(t0_);
    EXPECT_FALSE(cursor->Next());
    Publish("new", t0_);
    allocation_test::Probe probe;
    probe.FailAfter(0);
    const bool terminal = !cursor->Next() && !cursor->Next();
    cursor.reset();
    probe.Stop();
    EXPECT_TRUE(terminal);
    EXPECT_EQ(probe.attempts, 0u);
    EXPECT_EQ(probe.live_blocks, 0u);
    EXPECT_EQ(DueKeys(t0_), (std::vector<std::string>{"new"}));
}

TEST_F(LeaseEvictionIndexTest, CancellationReleasesSnapshotWithoutAllocation) {
    (void)CleanupKeys(false, false);
    auto cursor = ns_.eviction_index().Scan(t0_);
    allocation_test::Probe probe;
    const bool yielded = cursor->Next().has_value();
    const size_t live = probe.live_blocks;
    const size_t attempts = probe.attempts;
    probe.FailAfter(0);
    cursor.reset();
    probe.Stop();
    EXPECT_TRUE(yielded);
    EXPECT_EQ(live, 4u);
    EXPECT_EQ(probe.live_blocks, 0u);
    EXPECT_EQ(probe.live_bytes, 0u);
    EXPECT_EQ(probe.attempts, attempts);
    EXPECT_EQ(probe.failures, 0u);
    EXPECT_FALSE(probe.overflow);
}

TEST_F(LeaseEvictionIndexTest,
       FirstCopyAllocationFailuresRespectConfiguration) {
    CheckAllocationFailures(false, false);
}
TEST_F(LeaseEvictionIndexTest,
       GroupCopyAllocationFailuresRespectConfiguration) {
    CheckAllocationFailures(true, false);
}
TEST_F(LeaseEvictionIndexTest, RefillAllocationFailuresRespectConfiguration) {
    CheckAllocationFailures(false, true);
}
TEST_F(LeaseEvictionIndexTest,
       GroupRefillAllocationFailuresRespectConfiguration) {
    CheckAllocationFailures(true, true);
}

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
    EXPECT_EQ(entries[2].keys, std::vector<std::string>{"c"});
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
    // Under the write lock the cursor sees "k" moved on; releasing that lock
    // files it at its real deadline.
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    size_t entries = 0;
    while (auto entry = cursor->Next()) {
        ++entries;
        ASSERT_EQ(entry->keys.size(), 1u);
        const std::string key(entry->keys.front());
        auto guard = ns_.objects.Write(key);
        ASSERT_TRUE(guard.has_value());
        EXPECT_EQ(cursor->StillAt(*guard, *entry), key == "unmoved");
    }
    EXPECT_EQ(entries, 2u);
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

// A group's members come in one entry, filed at the lease they share.
TEST_F(LeaseEvictionIndexTest, AGroupIsOneEntryAtItsSharedLease) {
    PublishInGroup("g1", "g", t0_);
    Publish("k", t0_ + seconds(1));
    // Sets the shared lease later, which moves g1 with it.
    PublishInGroup("g2", "g", t0_ + seconds(2));

    EXPECT_EQ(Filed(), 3u);
    EXPECT_EQ(DueEntries(t0_ + seconds(1)),
              (std::vector<std::set<std::string>>{{"k"}}));
    EXPECT_EQ(DueEntries(t0_ + seconds(3)),
              (std::vector<std::set<std::string>>{{"k"}, {"g1", "g2"}}));
}

// A member leaves the group's entry with its object or its last memory, and
// the entry goes with the last member.
TEST_F(LeaseEvictionIndexTest, AGroupEntryGoesWithItsLastMember) {
    PublishInGroup("g1", "g", t0_);
    PublishInGroup("g2", "g", t0_);
    {
        auto guard = ns_.objects.Write("g1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    EXPECT_EQ(Filed(), 1u);
    EXPECT_EQ(DueEntries(t0_ + seconds(1)),
              (std::vector<std::set<std::string>>{{"g2"}}));

    {
        auto guard = ns_.objects.Write("g2");
        ASSERT_TRUE(guard.has_value());
        (void)guard->metadata().PopReplicas();
    }
    EXPECT_EQ(Filed(), 0u);
    EXPECT_TRUE(Walk(t0_ + seconds(1)).empty());

    // A member published afterwards starts a new entry.
    PublishInGroup("g3", "g", t0_);
    EXPECT_EQ(DueEntries(t0_ + seconds(1)),
              (std::vector<std::set<std::string>>{{"g3"}}));
}

// A key published again in another group, or in none, under one write lock
// moves to where its new object is filed.
TEST_F(LeaseEvictionIndexTest, AKeyPublishedAgainMovesToItsNewGroup) {
    PublishInGroup("g1", "g", t0_);
    PublishInGroup("g2", "g", t0_);
    PublishInGroup("h1", "h", t0_ + seconds(1));
    {
        auto guard = ns_.objects.Write("g1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        PublishIn(*guard, "g1", "h");
    }
    {
        auto guard = ns_.objects.Write("g2");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        PublishIn(*guard, "g2", std::string());
        SetDeadline(guard->metadata(), t0_ + seconds(2));
    }

    EXPECT_EQ(Filed(), 3u);
    EXPECT_EQ(DueEntries(t0_ + seconds(3)),
              (std::vector<std::set<std::string>>{{"g1", "h1"}, {"g2"}}));
}

// A key published in another group, or into a group, after the walk copied
// its entry no longer stands there, even at the same rank.
TEST_F(LeaseEvictionIndexTest, AKeyMovedToAnotherGroupIsNoLongerAtItsEntry) {
    PublishInGroup("g1", "g", t0_);
    PublishInGroup("g2", "g", t0_);
    Publish("k", t0_);
    // The walk copies the bucket as it starts.
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    std::optional<EvictionIndex::Entry> entry = cursor->Next();
    ASSERT_TRUE(entry.has_value());

    for (const std::string key : {"g2", "k"}) {
        auto guard = ns_.objects.Write(key);
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        PublishIn(*guard, key, "h");
        SetDeadline(guard->metadata(), t0_);
    }
    size_t entries = 0;
    for (; entry.has_value(); entry = cursor->Next()) {
        ++entries;
        for (const std::string_view key : entry->keys) {
            auto guard = ns_.objects.Write(key);
            ASSERT_TRUE(guard.has_value());
            EXPECT_EQ(cursor->StillAt(*guard, *entry), key == "g1") << key;
        }
    }
    EXPECT_EQ(entries, 2u);
}

// A group whose members were all torn down after the walk copied its entry
// is gone; a key published again under the same id joins a new incarnation,
// which the old entry does not stand for.
TEST_F(LeaseEvictionIndexTest, AKeyInARecreatedGroupIsNoLongerAtItsEntry) {
    PublishInGroup("g1", "g", t0_);
    PublishInGroup("g2", "g", t0_);
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    std::optional<EvictionIndex::Entry> entry = cursor->Next();
    ASSERT_TRUE(entry.has_value());

    for (const std::string key : {"g1", "g2"}) {
        auto guard = ns_.objects.Write(key);
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    PublishInGroup("g1", "g", t0_);

    auto guard = ns_.objects.Write("g1");
    ASSERT_TRUE(guard.has_value());
    EXPECT_FALSE(cursor->StillAt(*guard, *entry));
}

// A group's last member torn down and published again under the same id
// within one write lock starts a new group with a new lease, which an entry
// copied before does not stand for.
TEST_F(LeaseEvictionIndexTest, AGroupRecreatedInPlaceIsANewIncarnation) {
    PublishInGroup("g1", "g", t0_);
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    std::optional<EvictionIndex::Entry> entry = cursor->Next();
    ASSERT_TRUE(entry.has_value());
    {
        auto guard = ns_.objects.Write("g1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        PublishIn(*guard, "g1", "g");
        SetDeadline(guard->metadata(), t0_);
    }

    auto guard = ns_.objects.Write("g1");
    ASSERT_TRUE(guard.has_value());
    EXPECT_FALSE(cursor->StillAt(*guard, *entry));
}

// A member replaced in place while others keep the group stays in it.
TEST_F(LeaseEvictionIndexTest, AMemberReplacedInPlaceStaysInItsGroup) {
    PublishInGroup("g1", "g", t0_);
    PublishInGroup("g2", "g", t0_);
    auto cursor = ns_.eviction_index().Scan(t0_ + seconds(1));
    std::optional<EvictionIndex::Entry> entry = cursor->Next();
    ASSERT_TRUE(entry.has_value());
    {
        auto guard = ns_.objects.Write("g1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        PublishIn(*guard, "g1", "g");
    }

    {
        auto guard = ns_.objects.Write("g1");
        ASSERT_TRUE(guard.has_value());
        EXPECT_TRUE(cursor->StillAt(*guard, *entry));
    }
    EXPECT_EQ(DueEntries(t0_ + seconds(1)),
              (std::vector<std::set<std::string>>{{"g1", "g2"}}));
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
