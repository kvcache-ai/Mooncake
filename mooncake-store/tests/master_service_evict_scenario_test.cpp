#include "master_scenario.h"

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
        .Then(KeyCount(kObjectCount - kExpectedEvicted))
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
        .Then(KeyCount(kObjectCount - kExpectedEvicted))
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
        .Then(KeyCount(kSoftPinnedCount - kExpectedSoftPinnedEvicted))
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
        .Then(KeyCount(kObjectCount - kGroupSize))
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
        .Then(KeyCount(kObjectCount - 1))
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
        .Then(KeyCount(kObjectCount - kExpectedEvicted))
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
        .Then(KeyCount(total - kExpectedEvicted))
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
        .Then(KeyCount(total - kExpectedEvicted))
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

    scenario.Then(OpLogUnavailable())
        .When(EvictMemory(1.0))
        .Then(Object("cold").IsReadable())
        .Then(KeyCount(2));
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
                   .CompleteOn("memory")
                   .ExpiresAt(base + std::chrono::seconds(1)))
        .When(EvictMemory(0.5))
        .Then(Object("same-key").ForTenant("tenant-a").DoesNotExist())
        .Then(Object("same-key").ForTenant("tenant-b").IsReadable())
        .Then(TenantQuota("tenant-a").Uses(0).Reserves(0))
        .Then(TenantQuota("tenant-b").Uses(kObjectSize).Reserves(0));
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
                   .CompleteOn("memory"))
        .When(PutStart("tenant-a-new", kObjectSize)
                  .ForTenant("tenant-a")
                  .ExpectReplicas(1))
        .Then(Object("tenant-a-old").ForTenant("tenant-a").DoesNotExist())
        .Then(Object("tenant-b-object").ForTenant("tenant-b").IsReadable())
        .Then(TenantQuota("tenant-a").Uses(0).Reserves(kObjectSize))
        .When(PutEnd("tenant-a-new").ForTenant("tenant-a"))
        .Then(TenantQuota("tenant-a").Uses(kObjectSize).Reserves(0))
        .Then(TenantQuota("tenant-b").Uses(kObjectSize).Reserves(0));
}

}  // namespace
}  // namespace mooncake::test
