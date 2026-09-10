#include "master_service/dsl/scenario.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <unistd.h>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_types.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake::test {
namespace {

class EvictFakeBatchHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard lock(kvs_mutex_);
        auto it = kvs_.find(std::string(key));
        if (it == kvs_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard lock(kvs_mutex_);
        kvs_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        std::lock_guard lock(kvs_mutex_);
        kvs.clear();
        for (auto it = kvs_.lower_bound(std::string(begin_key));
             it != kvs_.end() && it->first < end_key; ++it) {
            kvs.push_back({.key = it->first, .value = it->second});
            if (limit != 0 && kvs.size() >= limit) {
                break;
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        std::lock_guard lock(kvs_mutex_);
        for (const auto& compare : txn.compares) {
            auto it = kvs_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != kvs_.end()) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == kvs_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            kvs_[put.key] = put.value;
        }
        return ErrorCode::OK;
    }

   private:
    std::mutex kvs_mutex_;
    std::map<std::string, std::string> kvs_;
};

class EvictBlockingBatchHaKvBackend : public EvictFakeBatchHaKvBackend {
   public:
    void BlockTxn() {
        std::lock_guard lock(block_mutex_);
        blocked_ = true;
    }

    void AllowTxn() {
        {
            std::lock_guard lock(block_mutex_);
            blocked_ = false;
        }
        block_cv_.notify_all();
    }

    ErrorCode Txn(const KvTxn& txn) override {
        {
            std::unique_lock lock(block_mutex_);
            block_cv_.wait(lock, [this] { return !blocked_; });
        }
        return EvictFakeBatchHaKvBackend::Txn(txn);
    }

   private:
    std::mutex block_mutex_;
    std::condition_variable block_cv_;
    bool blocked_{false};
};

class EvictFailingBatchHaKvBackend : public EvictFakeBatchHaKvBackend {
   public:
    void FailTransactionsWith(ErrorCode error) {
        std::lock_guard lock(failure_mutex_);
        transaction_error_ = error;
        transaction_calls_ = 0;
    }

    bool WaitForTransactionCalls(
        size_t count,
        std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
        std::unique_lock lock(failure_mutex_);
        return failure_cv_.wait_for(
            lock, timeout, [&] { return transaction_calls_ >= count; });
    }

    ErrorCode Txn(const KvTxn& txn) override {
        ErrorCode error;
        {
            std::lock_guard lock(failure_mutex_);
            ++transaction_calls_;
            error = transaction_error_;
        }
        failure_cv_.notify_all();
        if (error != ErrorCode::OK) {
            return error;
        }
        return EvictFakeBatchHaKvBackend::Txn(txn);
    }

   private:
    std::mutex failure_mutex_;
    std::condition_variable failure_cv_;
    ErrorCode transaction_error_{ErrorCode::OK};
    size_t transaction_calls_{0};
};

class MasterServiceEvictScenarioTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kObjectSize = 1_KB;

    static void SetUpTestSuite() {
        google::InitGoogleLogging("MasterServiceEvictScenarioTest");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code error;
            std::filesystem::remove(path, error);
        }
    }

    MasterServiceConfig EvictConfig(bool allow_soft_pin_eviction = false) {
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

    MasterServiceConfig PressureConfig(bool allow_soft_pin_eviction = false) {
        auto config = EvictConfig(allow_soft_pin_eviction);
        config.eviction_ratio = 0.05;
        return config;
    }

    MasterServiceConfig TenantConfig(
        const std::map<std::string, uint64_t>& quotas) {
        auto config = EvictConfig();
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_type = "file";
        config.tenant_quota_connector_uri = WritePolicyFile(quotas);
        return config;
    }

    MasterServiceConfig HaConfig(
        const std::string& cluster_id,
        const std::map<std::string, uint64_t>& quotas = {}) {
        auto config = EvictConfig();
        config.enable_ha = true;
        config.enable_oplog = true;
        config.cluster_id = cluster_id;
        config.oplog_batch_max_entries = 1;
        if (!quotas.empty()) {
            config.enable_multi_tenants = true;
            config.tenant_quota_connector_type = "file";
            config.tenant_quota_connector_uri = WritePolicyFile(quotas);
        }
        return config;
    }

    static std::string Key(size_t index) {
        return "evict_scenario_key_" + std::to_string(index);
    }

    static std::chrono::system_clock::time_point ExpiredBase() {
        return std::chrono::system_clock::now() - std::chrono::hours(1);
    }

    static ObjectsSpec<> IndexedObjects(size_t begin, size_t end) {
        auto objects = Objects(begin, end);
        objects.NamedBy(Key);
        return objects;
    }

    void ReadBatchEventually(OpLogBatchStorage& storage, uint64_t batch_id,
                             OpLogBatchRecord& batch) {
        ErrorCode error = ErrorCode::ETCD_KEY_NOT_EXIST;
        for (int attempt = 0; attempt < 100; ++attempt) {
            error = storage.ReadBatch(batch_id, batch);
            if (error == ErrorCode::OK) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(error, ErrorCode::OK);
    }

   private:
    std::string WritePolicyFile(const std::map<std::string, uint64_t>& quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = quotas;
        const auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_evict_scenario_quota_" + std::to_string(::getpid()) +
             "_" + std::to_string(policy_files_.size()) + ".yaml");
        std::ofstream output(path);
        output << FormatTenantQuotaPolicyYaml(snapshot);
        output.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    std::vector<std::string> policy_files_;
};

TEST_F(MasterServiceEvictScenarioTest, EvictsExactOldestObjectsAtLowRatio) {
    constexpr size_t kObjectCount = 400;
    constexpr size_t kExpectedEvicted = 20;

    MasterScenario scenario("evict exact oldest objects", EvictConfig());
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(IndexedObjects(0, kObjectCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase()))
        .When(EvictMemory(0.05))
        .Then(ReadableCount(IndexedObjects(0, kObjectCount),
                            kObjectCount - kExpectedEvicted))
        .Then(IndexedObjects(0, kExpectedEvicted).DoNotExist())
        .Then(IndexedObjects(kExpectedEvicted, kObjectCount).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, TargetEqualsLowerBoundEvictsExactCount) {
    constexpr size_t kObjectCount = 250;
    constexpr size_t kExpectedEvicted = 25;

    MasterScenario scenario("equal target and lower bound", EvictConfig());
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(IndexedObjects(0, kObjectCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase()))
        .When(EvictMemory(0.10).ToLowerBound(0.10))
        .Then(ReadableCount(IndexedObjects(0, kObjectCount),
                            kObjectCount - kExpectedEvicted))
        .Then(Object(Key(kExpectedEvicted - 1)).DoesNotExist())
        .Then(Object(Key(kExpectedEvicted)).IsReadable());
}

TEST_F(MasterServiceEvictScenarioTest, SoftPinnedObjectsAreFallbackCandidates) {
    constexpr size_t kUnpinnedCount = 10;
    constexpr size_t kSoftPinnedCount = 10;
    constexpr size_t kExpectedSoftPinnedEvicted = 6;
    const auto base = ExpiredBase();
    const auto active_soft_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);

    MasterScenario scenario("soft pin fallback", EvictConfig(true));
    scenario.Given(MemoryNode("memory"))
        .Given(IndexedObjects(0, kUnpinnedCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base))
        .Given(IndexedObjects(kUnpinnedCount, kUnpinnedCount + kSoftPinnedCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base + std::chrono::nanoseconds(kUnpinnedCount))
                   .SoftPinnedUntil(active_soft_pin))
        .When(EvictMemory(0.80))
        .Then(
            ReadableCount(IndexedObjects(0, kUnpinnedCount + kSoftPinnedCount),
                          kSoftPinnedCount - kExpectedSoftPinnedEvicted))
        .Then(IndexedObjects(0, kUnpinnedCount + kExpectedSoftPinnedEvicted)
                  .DoNotExist())
        .Then(IndexedObjects(kUnpinnedCount + kExpectedSoftPinnedEvicted,
                             kUnpinnedCount + kSoftPinnedCount)
                  .AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, EvictsWholeGroupTogether) {
    constexpr size_t kObjectCount = 10;
    constexpr size_t kGroupSize = 3;
    const auto base = ExpiredBase();

    MasterScenario scenario("whole group eviction", EvictConfig());
    scenario.Given(MemoryNode("memory"))
        .Given(IndexedObjects(0, kGroupSize)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("group")
                   .ExpiredFrom(base))
        .Given(IndexedObjects(kGroupSize, kObjectCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base + std::chrono::nanoseconds(kGroupSize)))
        .When(EvictMemory(0.10))
        .Then(ReadableCount(IndexedObjects(0, kObjectCount),
                            kObjectCount - kGroupSize))
        .Then(IndexedObjects(0, kGroupSize).DoNotExist())
        .Then(IndexedObjects(kGroupSize, kObjectCount).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, ActiveGroupMemberBlocksWholeGroup) {
    constexpr size_t kObjectCount = 10;
    constexpr size_t kGroupSize = 3;
    const auto base = ExpiredBase();

    MasterScenario scenario("active group member blocks eviction",
                            EvictConfig());
    scenario.Given(MemoryNode("memory"))
        .Given(IndexedObjects(0, kGroupSize)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("group")
                   .ExpiredFrom(base))
        .Given(IndexedObjects(kGroupSize, kObjectCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base + std::chrono::nanoseconds(kGroupSize)))
        .When(ExpireAt(Key(kGroupSize - 1), std::chrono::system_clock::now() +
                                                std::chrono::hours(1)))
        .When(EvictMemory(0.10))
        .Then(ReadableCount(IndexedObjects(0, kObjectCount), kObjectCount - 1))
        .Then(IndexedObjects(0, kGroupSize).AreReadable())
        .Then(Object(Key(kGroupSize)).DoesNotExist());
}

TEST_F(MasterServiceEvictScenarioTest, EvictsExactOldestObjectsAtHighRatio) {
    constexpr size_t kObjectCount = 200;
    constexpr size_t kExpectedEvicted = 160;

    MasterScenario scenario("high ratio eviction", EvictConfig());
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(IndexedObjects(0, kObjectCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase()))
        .When(EvictMemory(0.80))
        .Then(ReadableCount(IndexedObjects(0, kObjectCount),
                            kObjectCount - kExpectedEvicted))
        .Then(IndexedObjects(0, kExpectedEvicted).DoNotExist())
        .Then(IndexedObjects(kExpectedEvicted, kObjectCount).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest,
       ReserveAbsorbsBlockedCandidatesAndMeetsTarget) {
    constexpr size_t kBlockedCount = 12;
    constexpr size_t kPlainCount = 1200;
    constexpr size_t kExpectedEvicted = 61;
    constexpr size_t kAlwaysEvictedPlain = 49;
    const auto base = ExpiredBase();

    MasterScenario scenario("reserve absorbs blocked candidates",
                            EvictConfig());
    const size_t keeper = kBlockedCount;
    const size_t plain_begin = keeper + 1;
    const size_t total = plain_begin + kPlainCount;

    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(IndexedObjects(0, kBlockedCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("blocked-group")
                   .ExpiredFrom(base))
        .Given(IndexedObjects(keeper, keeper + 1)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("blocked-group")
                   .ExpiresAt(std::chrono::system_clock::now() +
                              std::chrono::hours(1)))
        .Given(IndexedObjects(plain_begin, total)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base + std::chrono::nanoseconds(plain_begin)))
        .When(EvictMemory(0.05))
        .Then(ReadableCount(IndexedObjects(0, total), total - kExpectedEvicted))
        .Then(IndexedObjects(0, kBlockedCount + 1).AreReadable())
        .Then(IndexedObjects(plain_begin, plain_begin + kAlwaysEvictedPlain)
                  .DoNotExist());
}

TEST_F(MasterServiceEvictScenarioTest,
       RefillAfterReserveExhaustionStillMeetsTarget) {
    constexpr size_t kBlockedCount = 1160;
    constexpr size_t kPlainCount = 200;
    constexpr size_t kExpectedEvicted = 69;
    const auto base = ExpiredBase();

    MasterScenario scenario("refill after reserve exhaustion", EvictConfig());
    const size_t keeper = kBlockedCount;
    const size_t plain_begin = keeper + 1;
    const size_t total = plain_begin + kPlainCount;

    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(IndexedObjects(0, kBlockedCount)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("blocked-group")
                   .ExpiredFrom(base))
        .Given(IndexedObjects(keeper, keeper + 1)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .InGroup("blocked-group")
                   .ExpiresAt(std::chrono::system_clock::now() +
                              std::chrono::hours(1)))
        .Given(IndexedObjects(plain_begin, total)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiredFrom(base + std::chrono::nanoseconds(plain_begin)))
        .When(EvictMemory(0.05))
        .Then(ReadableCount(IndexedObjects(0, total), total - kExpectedEvicted))
        .Then(IndexedObjects(0, kBlockedCount + 1).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, OpLogRecordsEvictedTenantAndKey) {
    const std::string cluster_id = "evict_scenario_oplog";
    auto backend = std::make_shared<EvictFakeBatchHaKvBackend>();
    MasterScenario scenario("eviction writes tenant-scoped oplog",
                            HaConfig(cluster_id, {{"tenant-a", kObjectSize}}),
                            backend);
    scenario.Given(MemoryNode("memory"))
        .Given(Objects({"cold"})
                   .Size(kObjectSize)
                   .ForTenant("tenant-a")
                   .CompleteOn("memory")
                   .ExpiresAt(ExpiredBase()));

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 2, batch);

    scenario.When(EvictMemory(1.0));
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(batch.entries.size(), 1);
    EXPECT_EQ(batch.entries[0].op_type, OpType::REMOVE);
    EXPECT_EQ(batch.entries[0].tenant_id, "tenant-a");
    EXPECT_EQ(batch.entries[0].object_key, "cold");
}

TEST_F(MasterServiceEvictScenarioTest,
       OpLogDurabilityGatesTenantQuotaReclamation) {
    const std::string cluster_id = "evict_scenario_durability";
    const std::string tenant = TenantId::Default().value();
    auto backend = std::make_shared<EvictBlockingBatchHaKvBackend>();
    MasterScenario scenario("durability gates eviction quota reclamation",
                            HaConfig(cluster_id, {{tenant, kObjectSize}}),
                            backend);
    scenario.Given(MemoryNode("memory").Capacity(kObjectSize))
        .Given(Objects({"cold"})
                   .Size(kObjectSize)
                   .ForTenant(tenant)
                   .CompleteOn("memory")
                   .ExpiresAt(ExpiredBase()));

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    scenario.When(EvictMemory(1.0))
        .Then(Object("cold").DoesNotExist())
        .When(PutStart("before-durable", kObjectSize)
                  .ForTenant(tenant)
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED));

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);
    scenario.When(PutStart("after-durable", kObjectSize)
                      .ForTenant(tenant)
                      .ExpectReplicas(1)
                      .Eventually());
}

TEST_F(MasterServiceEvictScenarioTest,
       OpLogReservationFailureLeavesEvictionCandidateReadable) {
    const std::string cluster_id = "evict_scenario_oplog_failure";
    auto backend = std::make_shared<EvictFailingBatchHaKvBackend>();
    MasterScenario scenario("oplog failure keeps eviction candidate intact",
                            HaConfig(cluster_id), backend);
    scenario.Given(MemoryNode("memory"))
        .Given(Objects({"cold"})
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .ExpiresAt(ExpiredBase()));

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 2, batch);

    // Fail a preceding hard-pinned object's PUT_END to put the ordered writer
    // into its terminal failure state. The following eviction cannot reserve
    // an OpLog slot and therefore must not mutate the cold object.
    backend->FailTransactionsWith(ErrorCode::INTERNAL_ERROR);
    scenario
        .When(PutStart("writer-failure", kObjectSize)
                  .OnNode("memory")
                  .WithHardPin())
        .When(PutEnd("writer-failure"));
    ASSERT_TRUE(backend->WaitForTransactionCalls(1));

    scenario.When(WaitForOpLogFailure())
        .When(EvictMemory(1.0))
        .Then(Object("cold").IsReadable());
}

TEST_F(MasterServiceEvictScenarioTest,
       GlobalEvictReclaimsOnlySelectedTenantQuota) {
    MasterScenario scenario(
        "global eviction preserves tenant isolation",
        TenantConfig({{"tenant-a", kObjectSize}, {"tenant-b", kObjectSize}}));
    const auto base = ExpiredBase();
    scenario.Given(MemoryNode("memory"))
        .Given(Objects({"same-key"})
                   .Size(kObjectSize)
                   .ForTenant("tenant-a")
                   .CompleteOn("memory")
                   .ExpiresAt(base))
        .Given(Objects({"same-key"})
                   .Size(kObjectSize)
                   .ForTenant("tenant-b")
                   .WithHardPin()
                   .CompleteOn("memory")
                   .ExpiresAt(base + std::chrono::seconds(1)))
        .When(EvictMemory(0.5))
        .Then(Object("same-key").ForTenant("tenant-a").DoesNotExist())
        .Then(Object("same-key").ForTenant("tenant-b").IsReadable())
        .When(PutStart("tenant-a-probe", kObjectSize).ForTenant("tenant-a"))
        .When(PutStart("tenant-b-probe", 1)
                  .ForTenant("tenant-b")
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED));
}

TEST_F(MasterServiceEvictScenarioTest,
       TenantAdmissionEvictsOnlyThatTenantsExpiredObject) {
    MasterScenario scenario(
        "tenant admission evicts within tenant",
        TenantConfig({{"tenant-a", kObjectSize}, {"tenant-b", kObjectSize}}));
    scenario.Given(MemoryNode("memory"))
        .Given(Objects({"tenant-a-old"})
                   .Size(kObjectSize)
                   .ForTenant("tenant-a")
                   .CompleteOn("memory")
                   .ExpiresAt(ExpiredBase()))
        .Given(Objects({"tenant-b-object"})
                   .Size(kObjectSize)
                   .ForTenant("tenant-b")
                   .WithHardPin()
                   .CompleteOn("memory"))
        .When(PutStart("tenant-a-new", kObjectSize)
                  .ForTenant("tenant-a")
                  .ExpectReplicas(1))
        .Then(Object("tenant-a-old").ForTenant("tenant-a").DoesNotExist())
        .Then(Object("tenant-b-object").ForTenant("tenant-b").IsReadable())
        .When(PutEnd("tenant-a-new").ForTenant("tenant-a"))
        .When(PutStart("tenant-b-overflow", 1)
                  .ForTenant("tenant-b")
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED));
}

// The scenarios below drive the background eviction thread through the
// client-visible pressure path: a failed allocation arms eviction, and the
// only observable outcomes are which writes eventually succeed and which
// objects remain readable. Sizes are chosen so each armed cycle reclaims
// exactly one expired one-megabyte object.

TEST_F(MasterServiceEvictScenarioTest, AllocationPressureEvictsOldestFirst) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    MasterScenario scenario("allocation pressure evicts the oldest objects",
                            PressureConfig());
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase()))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(IndexedObjects(0, 3).DoNotExist())
        .Then(IndexedObjects(3, 15).AreReadable())
        .Then(ReadableCount(IndexedObjects(0, 15), 12));
}

TEST_F(MasterServiceEvictScenarioTest,
       ClientRequestedSoftPinsSurviveAllocationPressure) {
    constexpr uint64_t kObjectSize = 1024 * 1024;
    MasterScenario scenario("client-requested soft pins survive pressure",
                            PressureConfig(true));
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 2)
                   .Size(kObjectSize)
                   .CompleteOn("memory")
                   .WithSoftPin())
        .Given(IndexedObjects(2, 14).Size(kObjectSize).CompleteOn("memory"))
        .When(PutStart(Key(14), 3 * kObjectSize)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(IndexedObjects(0, 2).AreReadable())
        .Then(Object(Key(14)).IsReadable())
        .Then(ReadableCount(IndexedObjects(2, 14), 9));
}

TEST_F(MasterServiceEvictScenarioTest,
       PressureEvictsUnpinnedObjectsBeforeSoftPinned) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    const auto active_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    MasterScenario scenario("pressure spares soft-pinned objects",
                            PressureConfig(true));
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 2)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase())
                   .SoftPinnedUntil(active_pin))
        .Given(IndexedObjects(2, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase() + std::chrono::nanoseconds(2)))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(IndexedObjects(0, 2).AreReadable())
        .Then(IndexedObjects(2, 5).DoNotExist())
        .Then(IndexedObjects(5, 15).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest,
       PressureEvictsSoftPinnedAsFallbackWhenAllowed) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    const auto active_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    MasterScenario scenario("pressure falls back to soft-pinned objects",
                            PressureConfig(true));
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase())
                   .SoftPinnedUntil(active_pin))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .WithSoftPin()
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(IndexedObjects(0, 3).DoNotExist())
        .Then(IndexedObjects(3, 15).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest,
       PressureCannotReclaimSoftPinnedWhenDisallowed) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    const auto active_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    MasterScenario scenario("soft pins block reclamation when disallowed",
                            PressureConfig(false));
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase())
                   .SoftPinnedUntil(active_pin))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .Then(IndexedObjects(0, 14).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, HardPinnedObjectsSurvivePressure) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    MasterScenario scenario("hard pins survive pressure until forced removal",
                            PressureConfig());
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 1)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase())
                   .WithHardPin())
        .Given(IndexedObjects(1, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase() + std::chrono::nanoseconds(1)))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(Object(Key(0)).IsReadable())
        .Then(IndexedObjects(1, 4).DoNotExist())
        .Then(IndexedObjects(4, 15).AreReadable())
        .When(Remove(Key(0)).Force())
        .Then(Object(Key(0)).DoesNotExist());
}

TEST_F(MasterServiceEvictScenarioTest,
       PressureSparesHardPinnedAndSoftPinnedInOrder) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    const auto active_pin =
        std::chrono::system_clock::now() + std::chrono::hours(1);
    MasterScenario scenario("pressure spares hard and soft pins in order",
                            PressureConfig(true));
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024))
        .Given(IndexedObjects(0, 1)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase())
                   .WithHardPin())
        .Given(IndexedObjects(1, 2)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase() + std::chrono::nanoseconds(1))
                   .SoftPinnedUntil(active_pin))
        .Given(IndexedObjects(2, 14)
                   .Size(kLargeObject)
                   .CompleteOn("memory")
                   .ExpiredFrom(ExpiredBase() + std::chrono::nanoseconds(2)))
        .When(PutStart(Key(14), 3 * kLargeObject)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(Key(14)))
        .Then(Object(Key(0)).IsReadable())
        .Then(Object(Key(1)).IsReadable())
        .Then(IndexedObjects(2, 5).DoNotExist())
        .Then(IndexedObjects(5, 15).AreReadable());
}

TEST_F(MasterServiceEvictScenarioTest, PressureEvictionExpandsToWholeGroup) {
    constexpr uint64_t kGroupObject = 2 * 1024 * 1024;
    MasterScenario scenario("pressure eviction expands to the whole group",
                            PressureConfig());
    scenario.Given(MemoryNode("memory").Capacity(4 * 1024 * 1024))
        .Given(Objects({"grouped_evict_a", "grouped_evict_b"})
                   .Size(kGroupObject)
                   .CompleteOn("memory")
                   .InGroup(GroupOnDifferentShard("grouped_evict_a"))
                   .ExpiredFrom(ExpiredBase()))
        .When(PutStart("grouped_evict_trigger", kGroupObject)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd("grouped_evict_trigger"))
        .Then(Object("grouped_evict_a").DoesNotExist())
        .Then(Object("grouped_evict_b").DoesNotExist())
        .Then(Object("grouped_evict_trigger").IsReadable());
}

TEST_F(MasterServiceEvictScenarioTest,
       LeasedGroupMemberShieldsWholeGroupFromPressure) {
    constexpr uint64_t kGroupObject = 2 * 1024 * 1024;
    auto config = PressureConfig();
    config.default_kv_lease_ttl = 60 * 60 * 1000;
    MasterScenario scenario("a leased member shields its group from pressure",
                            config);
    scenario.Given(MemoryNode("memory").Capacity(4 * 1024 * 1024))
        .Given(Objects({"grouped_leased_a", "grouped_leased_b"})
                   .Size(kGroupObject)
                   .CompleteOn("memory")
                   .InGroup(GroupOnDifferentShard("grouped_leased_a"))
                   .ExpiredFrom(ExpiredBase()))
        // Reading one member grants it a fresh lease, which shields the whole
        // group from the eviction armed by the failing writes below.
        .Then(Object("grouped_leased_a").IsReadable())
        .When(PutStart("grouped_leased_trigger", kGroupObject)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(200)))
        .When(PutStart("grouped_leased_trigger", kGroupObject)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .Then(Object("grouped_leased_a").IsReadable())
        .Then(Object("grouped_leased_b").IsReadable());
}

}  // namespace
}  // namespace mooncake::test
