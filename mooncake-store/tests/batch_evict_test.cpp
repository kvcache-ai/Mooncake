#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "master_service.h"
#include "mutex.h"
#include "types.h"
#include "utils.h"

namespace mooncake::test {

// Deterministic correctness tests for MasterService::BatchEvict.
//
// These tests drive BatchEvict directly instead of filling a segment and
// waiting for the background eviction thread. Lease timestamps are written
// explicitly so that the eviction order, the exact eviction count, the
// soft-pin fallback and the whole-group semantics are all observable without
// sleeps, background threads or timing assumptions.
class BatchEvictTest : public ::testing::Test {
   protected:
    static constexpr const char* kSegmentName = "batch_evict_test_segment";
    static constexpr size_t kSegmentBase = 0x500000000;
    static constexpr size_t kSegmentSize = 256ULL * 1024 * 1024;
    static constexpr uint64_t kObjectSize = 1024;

    void SetUp() override {
        google::InitGoogleLogging("BatchEvictTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Eviction ratio 0 and a 100% high watermark keep the background eviction
    // thread from interfering: every eviction in these tests is the one the
    // test itself requests.
    static MasterServiceConfig MakeConfig(bool allow_soft_pin_eviction) {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_default_kv_lease_ttl(0)
            .set_default_kv_soft_pin_ttl(60 * 60 * 1000)
            .set_allow_evict_soft_pinned_objects(allow_soft_pin_eviction)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .build();
    }

    static Segment MakeSegment() {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = kSegmentName;
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;
        return segment;
    }

    static UUID MountSegment(MasterService& service) {
        const UUID client_id = generate_uuid();
        auto result = service.MountSegment(MakeSegment(), client_id);
        EXPECT_TRUE(result.has_value());
        return client_id;
    }

    static std::string Key(size_t index) {
        return "batch_evict_key_" + std::to_string(index);
    }

    static void PutObject(MasterService& service, const UUID& client_id,
                          const std::string& key, bool with_soft_pin = false,
                          const std::string& group_id = std::string()) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;
        config.with_soft_pin = with_soft_pin;
        if (!group_id.empty()) {
            config.group_ids = std::vector<std::string>{group_id};
        }

        auto put_start = service.PutStart(client_id, key, TenantId::Default(),
                                          kObjectSize, config);
        ASSERT_TRUE(put_start.has_value())
            << "PutStart failed for key=" << key
            << ", error=" << toString(put_start.error());
        ASSERT_TRUE(service
                        .PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
    }

    // Ungrouped objects are sharded by key hash, but grouped objects are
    // routed by their group id, so the direct shard index is not always the
    // one holding the key. Fall back to a shard scan in that case.
    template <typename Fn>
    static bool WithMetadata(MasterService& service, const std::string& key,
                             Fn&& fn) {
        const TenantId tenant = TenantId::Default();

        auto try_shard = [&](size_t shard_idx) -> bool {
            MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
            auto tenant_it = shard->tenants.find(tenant);
            if (tenant_it == shard->tenants.end()) {
                return false;
            }
            auto metadata_it = tenant_it->second.metadata.find(key);
            if (metadata_it == tenant_it->second.metadata.end()) {
                return false;
            }
            fn(metadata_it->second);
            return true;
        };

        if (try_shard(service.getMetadataShardIndex(tenant, key))) {
            return true;
        }
        for (size_t shard_idx = 0; shard_idx < MasterService::kNumShards;
             ++shard_idx) {
            if (try_shard(shard_idx)) {
                return true;
            }
        }
        ADD_FAILURE() << "metadata not found for key=" << key;
        return false;
    }

    static void SetLease(
        MasterService& service, const std::string& key,
        std::chrono::system_clock::time_point lease_timeout,
        std::optional<std::chrono::system_clock::time_point> soft_pin_timeout =
            std::nullopt) {
        WithMetadata(service, key,
                     [&](MasterService::ObjectMetadata& metadata) {
                         SpinLocker locker(&metadata.lock);
                         metadata.lease_timeout = lease_timeout;
                         metadata.soft_pin_timeout = soft_pin_timeout;
                     });
    }

    static bool Exists(MasterService& service, const std::string& key) {
        auto result = service.ExistKey(key, TenantId::Default());
        if (!result.has_value()) {
            ADD_FAILURE() << "ExistKey failed for key=" << key
                          << ", error=" << toString(result.error());
            return false;
        }
        return result.value();
    }

    static void RunBatchEvict(MasterService& service, double target,
                              double lowerbound) {
        service.BatchEvict(target, lowerbound);
    }

    static std::chrono::system_clock::time_point ExpiredBase() {
        return std::chrono::system_clock::now() - std::chrono::hours(1);
    }

    // Populates `count` expired objects whose lease timestamps are strictly
    // increasing, so Key(i) is always older than Key(i + 1) and no timestamp
    // ties exist at the eviction boundary.
    static void PopulateOldestFirst(MasterService& service,
                                    const UUID& client_id, size_t count) {
        const auto base = ExpiredBase();
        for (size_t i = 0; i < count; ++i) {
            PutObject(service, client_id, Key(i));
            SetLease(service, Key(i), base + std::chrono::nanoseconds(i));
        }
    }
    // Builds a population whose oldest `blocked_count` candidates belong to a
    // group that also holds one member under an active lease. Those candidates
    // pass the census — their own lease has expired — but cannot be evicted
    // during execution, because a group is only evictable once every member's
    // lease has expired. That is an execution-stage failure without any hook:
    // it exercises the same recovery path as metadata churn between the census
    // and the eviction pass.
    struct BlockedGroupPopulation {
        size_t blocked_count;
        size_t keeper_index;
        size_t plain_begin;
        size_t plain_count;
        size_t total_objects;
    };

    static BlockedGroupPopulation PopulateBlockedGroup(
        MasterService& service, const UUID& client_id,
        const std::string& group_id, size_t blocked_count,
        size_t plain_count) {
        const auto base = ExpiredBase();
        const auto active_lease =
            std::chrono::system_clock::now() + std::chrono::hours(1);

        // Oldest leases: the blocked group members are always selected first.
        for (size_t i = 0; i < blocked_count; ++i) {
            PutObject(service, client_id, Key(i), /*with_soft_pin=*/false,
                      group_id);
            SetLease(service, Key(i), base + std::chrono::nanoseconds(i));
        }

        // The keeper shares the group but holds an active lease, so no member
        // of the group can be evicted.
        const size_t keeper_index = blocked_count;
        PutObject(service, client_id, Key(keeper_index),
                  /*with_soft_pin=*/false, group_id);
        SetLease(service, Key(keeper_index), active_lease);

        // Plain objects are strictly newer than every blocked member.
        const size_t plain_begin = keeper_index + 1;
        for (size_t i = 0; i < plain_count; ++i) {
            const size_t index = plain_begin + i;
            PutObject(service, client_id, Key(index));
            SetLease(service, Key(index),
                     base + std::chrono::nanoseconds(index));
        }

        return {blocked_count, keeper_index, plain_begin, plain_count,
                plain_begin + plain_count};
    }

    static size_t CountAlive(MasterService& service, size_t begin, size_t end) {
        size_t alive = 0;
        for (size_t i = begin; i < end; ++i) {
            if (Exists(service, Key(i))) {
                ++alive;
            }
        }
        return alive;
    }
};

// Oldest-first: with distinct lease timestamps the evicted set must be exactly
// the oldest ceil(N * ratio) objects, and nothing newer.
TEST_F(BatchEvictTest, EvictsExactOldestObjectsAtLowRatio) {
    constexpr size_t kObjectCount = 400;
    constexpr size_t kExpectedEvicted = 20;  // ceil(400 * 0.05)

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);
    PopulateOldestFirst(service, client_id, kObjectCount);
    ASSERT_EQ(service.GetKeyCount(), kObjectCount);

    RunBatchEvict(service, /*target=*/0.05, /*lowerbound=*/0.05);

    EXPECT_EQ(service.GetKeyCount(), kObjectCount - kExpectedEvicted);
    for (size_t i = 0; i < kObjectCount; ++i) {
        EXPECT_EQ(Exists(service, Key(i)), i >= kExpectedEvicted)
            << "unexpected eviction outcome at index=" << i;
    }
}

// target == lowerbound: the first pass already satisfies the lower bound, so
// the second pass must not evict anything extra.
TEST_F(BatchEvictTest, TargetEqualsLowerBoundEvictsExactCount) {
    constexpr size_t kObjectCount = 250;
    constexpr size_t kExpectedEvicted = 25;  // ceil(250 * 0.10)

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);
    PopulateOldestFirst(service, client_id, kObjectCount);
    ASSERT_EQ(service.GetKeyCount(), kObjectCount);

    RunBatchEvict(service, /*target=*/0.10, /*lowerbound=*/0.10);

    EXPECT_EQ(service.GetKeyCount(), kObjectCount - kExpectedEvicted);
    EXPECT_FALSE(Exists(service, Key(kExpectedEvicted - 1)));
    EXPECT_TRUE(Exists(service, Key(kExpectedEvicted)));
}

// Soft-pin fallback: unpinned objects go first; soft-pinned objects are only
// evicted by the second pass, oldest first, and only up to the lower bound.
TEST_F(BatchEvictTest, SoftPinnedEvictedOnlyAfterUnpinned) {
    constexpr size_t kNoPinCount = 10;
    constexpr size_t kSoftPinCount = 10;
    // ceil(20 * 0.80) == 16; the first pass can only evict the 10 unpinned
    // objects, leaving 6 for the soft-pinned second pass.
    constexpr size_t kExpectedSoftPinEvicted = 6;

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/true));
    const UUID client_id = MountSegment(service);

    const auto base = ExpiredBase();
    const auto active_soft_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);

    for (size_t i = 0; i < kNoPinCount; ++i) {
        PutObject(service, client_id, Key(i));
        SetLease(service, Key(i), base + std::chrono::nanoseconds(i));
    }
    for (size_t i = 0; i < kSoftPinCount; ++i) {
        const size_t index = kNoPinCount + i;
        PutObject(service, client_id, Key(index), /*with_soft_pin=*/true);
        SetLease(service, Key(index), base + std::chrono::nanoseconds(index),
                 active_soft_pin);
    }
    ASSERT_EQ(service.GetKeyCount(), kNoPinCount + kSoftPinCount);

    RunBatchEvict(service, /*target=*/0.80, /*lowerbound=*/0.80);

    for (size_t i = 0; i < kNoPinCount; ++i) {
        EXPECT_FALSE(Exists(service, Key(i)))
            << "unpinned object survived at index=" << i;
    }
    for (size_t i = 0; i < kSoftPinCount; ++i) {
        const size_t index = kNoPinCount + i;
        EXPECT_EQ(Exists(service, Key(index)), i >= kExpectedSoftPinEvicted)
            << "unexpected soft-pin outcome at index=" << index;
    }
    EXPECT_EQ(service.GetKeyCount(), kSoftPinCount - kExpectedSoftPinEvicted);
}

// Whole-group: selecting one group member evicts the entire group as a unit,
// even when the target only calls for a single object.
TEST_F(BatchEvictTest, WholeGroupEvictedTogether) {
    constexpr size_t kObjectCount = 10;
    constexpr size_t kGroupSize = 3;
    const std::string group_id = "batch_evict_test_group";

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);

    const auto base = ExpiredBase();
    for (size_t i = 0; i < kObjectCount; ++i) {
        PutObject(service, client_id, Key(i), /*with_soft_pin=*/false,
                  i < kGroupSize ? group_id : std::string());
        SetLease(service, Key(i), base + std::chrono::nanoseconds(i));
    }

    // ceil(10 * 0.10) == 1 and the oldest candidate is a group member, so the
    // whole group is evicted even though only one object was requested.
    RunBatchEvict(service, /*target=*/0.10, /*lowerbound=*/0.10);

    EXPECT_EQ(service.GetKeyCount(), kObjectCount - kGroupSize);
    for (size_t i = 0; i < kGroupSize; ++i) {
        EXPECT_FALSE(Exists(service, Key(i)))
            << "group member survived at index=" << i;
    }
    for (size_t i = kGroupSize; i < kObjectCount; ++i) {
        EXPECT_TRUE(Exists(service, Key(i)))
            << "non-group object evicted at index=" << i;
    }
}

// Whole-group safety: a single group member under an active lease keeps every
// member of that group resident.
TEST_F(BatchEvictTest, UnexpiredGroupMemberBlocksWholeGroup) {
    constexpr size_t kObjectCount = 10;
    constexpr size_t kGroupSize = 3;
    const std::string group_id = "batch_evict_test_blocked_group";

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);

    const auto base = ExpiredBase();
    const auto unexpired =
        std::chrono::system_clock::now() + std::chrono::hours(1);

    for (size_t i = 0; i < kObjectCount; ++i) {
        PutObject(service, client_id, Key(i), /*with_soft_pin=*/false,
                  i < kGroupSize ? group_id : std::string());
        SetLease(service, Key(i), base + std::chrono::nanoseconds(i));
    }
    // Hold one member of the group under an active lease.
    SetLease(service, Key(kGroupSize - 1), unexpired);

    RunBatchEvict(service, /*target=*/0.10, /*lowerbound=*/0.10);

    for (size_t i = 0; i < kGroupSize; ++i) {
        EXPECT_TRUE(Exists(service, Key(i)))
            << "blocked group member was evicted at index=" << i;
    }
    // The blocked group yields nothing, so exactly one ungrouped object is
    // evicted instead.
    EXPECT_EQ(service.GetKeyCount(), kObjectCount - 1);
}

// High ratio: the same oldest-first and exact-count guarantees must hold when
// the requested ratio covers most of the population.
TEST_F(BatchEvictTest, HighRatioEvictsExactOldestCount) {
    constexpr size_t kObjectCount = 200;
    constexpr size_t kExpectedEvicted = 160;  // ceil(200 * 0.80)

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);
    PopulateOldestFirst(service, client_id, kObjectCount);
    ASSERT_EQ(service.GetKeyCount(), kObjectCount);

    RunBatchEvict(service, /*target=*/0.80, /*lowerbound=*/0.80);

    EXPECT_EQ(service.GetKeyCount(), kObjectCount - kExpectedEvicted);
    for (size_t i = 0; i < kObjectCount; ++i) {
        EXPECT_EQ(Exists(service, Key(i)), i >= kExpectedEvicted)
            << "unexpected eviction outcome at index=" << i;
    }
}

// Reserve: a small number of candidates that pass the census but fail during
// execution is absorbed by the reserve slack, so the requested target is still
// met exactly and no refill scan is needed.
TEST_F(BatchEvictTest, ReserveAbsorbsExecutionFailuresAndMeetsTarget) {
    constexpr size_t kBlockedCount = 12;
    constexpr size_t kPlainCount = 1200;
    // 1213 objects in total, ceil(1213 * 0.05) == 61.
    constexpr size_t kExpectedEvicted = 61;
    // The 61 oldest candidates are the 12 blocked members plus the 49 oldest
    // plain objects, so those 49 are always among the evicted set. The
    // remaining 12 evictions come from the reserve, whose internal order is
    // unspecified, so only the total is asserted for them.
    constexpr size_t kAlwaysEvictedPlain = 49;

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);
    const auto population = PopulateBlockedGroup(
        service, client_id, "batch_evict_reserve_group", kBlockedCount,
        kPlainCount);
    ASSERT_EQ(service.GetKeyCount(), population.total_objects);

    RunBatchEvict(service, /*target=*/0.05, /*lowerbound=*/0.05);

    EXPECT_EQ(service.GetKeyCount(),
              population.total_objects - kExpectedEvicted);
    EXPECT_EQ(CountAlive(service, 0, kBlockedCount), kBlockedCount)
        << "blocked group members must survive";
    EXPECT_TRUE(Exists(service, Key(population.keeper_index)));
    for (size_t i = 0; i < kAlwaysEvictedPlain; ++i) {
        EXPECT_FALSE(Exists(service, Key(population.plain_begin + i)))
            << "oldest plain object survived at offset=" << i;
    }
    EXPECT_EQ(CountAlive(service, population.plain_begin,
                         population.total_objects),
              kPlainCount - kExpectedEvicted);
}

// Refill: when every candidate inside the reserve frontier fails during
// execution the reserve is exhausted, and the refill scan must recover the
// remaining objects so the requested target is still met exactly.
TEST_F(BatchEvictTest, RefillAfterReserveExhaustionStillMeetsTarget) {
    // The reserve frontier spans target + max(1024, 10% of target), so more
    // than 1024 blocked candidates are required to exhaust it.
    constexpr size_t kBlockedCount = 1160;
    constexpr size_t kPlainCount = 200;
    // 1361 objects in total, ceil(1361 * 0.05) == 69.
    constexpr size_t kExpectedEvicted = 69;

    MasterService service(MakeConfig(/*allow_soft_pin_eviction=*/false));
    const UUID client_id = MountSegment(service);
    const auto population = PopulateBlockedGroup(
        service, client_id, "batch_evict_refill_group", kBlockedCount,
        kPlainCount);
    ASSERT_EQ(service.GetKeyCount(), population.total_objects);

    RunBatchEvict(service, /*target=*/0.05, /*lowerbound=*/0.05);

    // Exact target attainment is the property refill exists to protect: the
    // whole frontier yielded nothing, so every eviction came from the refill.
    EXPECT_EQ(service.GetKeyCount(),
              population.total_objects - kExpectedEvicted);
    EXPECT_EQ(CountAlive(service, 0, kBlockedCount), kBlockedCount)
        << "blocked group members must survive";
    EXPECT_TRUE(Exists(service, Key(population.keeper_index)));
    EXPECT_EQ(CountAlive(service, population.plain_begin,
                         population.total_objects),
              kPlainCount - kExpectedEvicted);
}

}  // namespace mooncake::test
