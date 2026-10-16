#include "storage/distributed/object_storage_catalog.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <tuple>
#include <ylt/struct_pack.hpp>

#include "common/base64.h"
#include "ha/kv/redis_ha_kv_backend.h"
#include "storage/distributed/object_storage_backend.h"

namespace mooncake {
namespace {

template <class T>
std::string Encode(const T& value) {
    const auto bytes = struct_pack::serialize(value);
    return base64::Encode(std::string(bytes.begin(), bytes.end()));
}

// Shared by catalog instances to model durable state across process restarts.
// Transactions are atomic; fault injection distinguishes rejection from an
// applied transaction whose response (and optionally reconciliation) is lost.
class CatalogKv : public HaKvBackend {
   public:
    std::map<std::string, std::string> data;
    bool supports_txn{true}, lose_reply{false}, reject_txn{false};
    bool fail_range{false}, fail_delete{false};
    int fail_gets{0}, reads_after_lost_reply{0};
    size_t range_calls{0};

    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard lock(mutex_);
        if (fail_gets > 0) {
            --fail_gets;
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        const auto it = data.find(std::string(key));
        if (it == data.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard lock(mutex_);
        data[std::string(key)] = value;
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& result) override {
        std::lock_guard lock(mutex_);
        ++range_calls;
        if (fail_range) return ErrorCode::ETCD_OPERATION_ERROR;
        result.clear();
        for (auto it = data.lower_bound(std::string(begin));
             it != data.end() && it->first < end && result.size() < limit; ++it)
            result.push_back({it->first, it->second});
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        std::lock_guard lock(mutex_);
        if (fail_delete) return ErrorCode::ETCD_OPERATION_ERROR;
        data.erase(data.lower_bound(std::string(begin)),
                   data.lower_bound(std::string(end)));
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return supports_txn; }
    ErrorCode Txn(const KvTxn& txn) override {
        std::lock_guard lock(mutex_);
        if (reject_txn) {
            reject_txn = false;
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        for (const auto& compare : txn.compares) {
            const auto it = data.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != data.end()) return ErrorCode::ETCD_TRANSACTION_FAIL;
            } else if (compare.kind != KvCompareKind::kValueEquals ||
                       it == data.end() || it->second != compare.expected_value)
                return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
        for (const auto& kv : txn.puts) data[kv.key] = kv.value;
        if (lose_reply) {
            lose_reply = false;
            fail_gets = reads_after_lost_reply;
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        return ErrorCode::OK;
    }

   private:
    std::mutex mutex_;
};

class CatalogObjects : public ObjectStorageAdapter {
   public:
    std::map<std::string, std::string> objects;
    std::vector<std::string> deletes;
    std::function<void()> during_delete;
    bool fail_delete{false};
    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> value) override {
        objects[key] = std::string(value.begin(), value.end());
        return {};
    }
    tl::expected<void, ErrorCode> PutV(const std::string&, const iovec*,
                                       int) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<size_t, ErrorCode> Get(const std::string& key, void* buf,
                                        size_t len) override {
        return GetRange(key, buf, len, 0);
    }
    tl::expected<size_t, ErrorCode> GetRange(const std::string& key, void* buf,
                                             size_t len,
                                             off_t offset) override {
        const auto it = objects.find(key);
        if (it == objects.end())
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        if (offset < 0 || static_cast<uint64_t>(offset) > it->second.size() ||
            len > it->second.size() - offset)
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        std::memcpy(buf, it->second.data() + offset, len);
        return len;
    }
    tl::expected<bool, ErrorCode> Exists(const std::string& key) override {
        return objects.contains(key);
    }
    tl::expected<void, ErrorCode> Delete(const std::string& key) override {
        deletes.push_back(key);
        if (during_delete) during_delete();
        if (fail_delete)
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        objects.erase(key);
        return {};
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        return std::vector<KeyInfo>{};
    }
    tl::expected<void, ErrorCode> Init() override { return {}; }
    const char* GetName() const override { return "catalog-test"; }
    bool SupportsSinglePutAttempt() const override { return true; }
};

ObjectStorageCatalogConfig Config() {
    ObjectStorageCatalogConfig config;
    config.storage_namespace = {"oss",    "http://fixture", "bucket",
                                "region", "prefix",         true};
    config.max_object_bytes = 16;
    config.max_pending_bytes = 64;
    config.max_pending_objects = 4;
    config.maintenance_batch_size = 2;
    return config;
}

class ObjectStorageCatalogTest : public ::testing::Test {
   protected:
    ObjectStorageCatalogConfig config = Config();
    std::shared_ptr<CatalogKv> kv = std::make_shared<CatalogKv>();
    std::shared_ptr<CatalogObjects> objects =
        std::make_shared<CatalogObjects>();
    ObjectStorageCatalog catalog{config, kv, "cluster"};
    void SetUp() override { ASSERT_EQ(catalog.Open(), ErrorCode::OK); }
    std::string Prepare(const std::string& key = "key", uint64_t bytes = 4) {
        auto result = catalog.Prepare("tenant", key, bytes);
        EXPECT_TRUE(result);
        return result ? result->object_key : "";
    }
    void Complete(const std::string& key) {
        ASSERT_EQ(catalog.Claim(key), ErrorCode::OK);
        ASSERT_EQ(catalog.Finish(key, ObjectUploadOutcome::COMPLETE),
                  ErrorCode::OK);
        ASSERT_EQ(catalog.Commit(key, 123), ErrorCode::OK);
    }
};

TEST(ObjectStorageCatalogConfigTest,
     InvalidConfigurationAndUnsupportedKvFailClosed) {
    auto kv = std::make_shared<CatalogKv>();
    auto config = Config();
    ObjectStorageCatalog unopened(config, kv, "cluster");
    EXPECT_FALSE(unopened.CanReserve(1));
    EXPECT_FALSE(unopened.Prepare("t", "k", 1));
    EXPECT_EQ(unopened.Claim("key"), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_FALSE(unopened.Records());
    kv->supports_txn = false;
    EXPECT_EQ(unopened.Open(), ErrorCode::INVALID_PARAMS);
    kv->supports_txn = true;
    for (const auto& cluster :
         {std::string{}, std::string("a/b"), std::string("a\0b", 3)}) {
        ObjectStorageCatalog invalid(config, kv, cluster);
        EXPECT_EQ(invalid.Open(), ErrorCode::INVALID_PARAMS);
    }
    config.max_pending_objects = 0;
    ObjectStorageCatalog invalid(config, kv, "cluster");
    EXPECT_EQ(invalid.Open(), ErrorCode::INVALID_PARAMS);
}

TEST(ObjectStorageCatalogConfigTest,
     ConcurrentFenceAcquisitionHasOnlyOneOwner) {
    class RacingKv : public CatalogKv {
       public:
        std::atomic<bool> race{false};
        std::atomic<int> reads{0};
        std::barrier<> rendezvous{2};
        ErrorCode Get(std::string_view key, std::string& value) override {
            const auto error = CatalogKv::Get(key, value);
            if (race.load() && key.ends_with("/fence") &&
                reads.fetch_add(1) < 2)
                rendezvous.arrive_and_wait();
            return error;
        }
        ErrorCode Txn(const KvTxn& txn) override {
            const auto error = CatalogKv::Txn(txn);
            // Even a transport-shaped failure must not let the losing opener
            // reconcile another opener's successful acquisition as its own.
            return error == ErrorCode::ETCD_TRANSACTION_FAIL
                       ? ErrorCode::ETCD_OPERATION_ERROR
                       : error;
        }
    };
    auto kv = std::make_shared<RacingKv>();
    const auto config = Config();
    ObjectStorageCatalog seed(config, kv, "race");
    ASSERT_EQ(seed.Open(1), ErrorCode::OK);
    const auto target = seed.Prepare("t", "k", 1);
    ASSERT_TRUE(target);
    ObjectStorageCatalog first(config, kv, "race"), second(config, kv, "race");
    kv->race = true;
    auto a = std::async(std::launch::async, [&] { return first.Open(2); });
    auto b = std::async(std::launch::async, [&] { return second.Open(2); });
    const auto first_result = a.get(), second_result = b.get();
    EXPECT_EQ(int(first_result == ErrorCode::OK) +
                  int(second_result == ErrorCode::OK),
              1);
    const auto first_claim = first.Claim(target->object_key);
    const auto second_claim = second.Claim(target->object_key);
    EXPECT_EQ(
        int(first_claim == ErrorCode::OK) + int(second_claim == ErrorCode::OK),
        1);
}

TEST(ObjectStorageCatalogConfigTest,
     OpLogOnlyRedisBackendIsExplicitlyUnsupported) {
    ObjectStorageCatalog catalog(Config(), std::make_shared<RedisHaKvBackend>(),
                                 "cluster");
    EXPECT_EQ(catalog.Open(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(catalog.CanReserve(1));
    EXPECT_FALSE(catalog.Records());
}

TEST_F(ObjectStorageCatalogTest, JointIoCommitRestartAndLeaseAwareReclamation) {
    const auto key = Prepare();
    ASSERT_EQ(catalog.Claim(key), ErrorCode::OK);
    EXPECT_EQ(catalog.Claim(key), ErrorCode::INVALID_WRITE);
    ObjectStorageBackend io({16, 16}, objects);
    std::string value = "data", output(2, '!');
    const auto copy = [](void* dst, const void* src, size_t size) {
        std::memcpy(dst, src, size);
        return true;
    };
    ASSERT_EQ(io.Write(key, 4, {{value.data(), 4}}, copy), ErrorCode::OK);
    ASSERT_EQ(catalog.Finish(key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    ASSERT_FALSE(catalog.Head("tenant", "key")->foreground_complete);
    ASSERT_EQ(catalog.Commit(key, 123), ErrorCode::OK);
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    auto head = restarted.Head("tenant", "key");
    ASSERT_TRUE(head);
    EXPECT_EQ(head->object_key, key);
    EXPECT_EQ(head->checksum, 123U);
    EXPECT_TRUE(head->foreground_complete);
    ASSERT_EQ(io.Read(head->object_key, 4, {{output.data(), 2}}, 1, copy),
              ErrorCode::OK);
    EXPECT_EQ(output, "at");
    ASSERT_EQ(restarted.Retire(key, 100), ErrorCode::OK);
    EXPECT_EQ(restarted.Reclaim(*objects, 99), ErrorCode::OK);
    EXPECT_TRUE(objects->deletes.empty());
    EXPECT_EQ(restarted.Reclaim(*objects, 100), ErrorCode::OK);
    EXPECT_FALSE(restarted.Find(key));
    EXPECT_FALSE(objects->objects.contains(key));
    head = restarted.Head("tenant", "key");
    ASSERT_TRUE(head);
    EXPECT_EQ(head->phase, ObjectStoragePhase::DELETED);
    ObjectStorageCatalog recovered(config, kv, "cluster");
    ASSERT_EQ(recovered.Open(), ErrorCode::OK);
    ASSERT_TRUE(recovered.Head("tenant", "key"));
    EXPECT_EQ(recovered.Head("tenant", "key")->phase,
              ObjectStoragePhase::DELETED);
    EXPECT_TRUE(recovered.Records()->empty());
    auto replacement = recovered.Prepare("tenant", "key", 4);
    ASSERT_TRUE(replacement);
    EXPECT_NE(replacement->object_key, key);
    EXPECT_GT(replacement->generation, head->generation);
}

TEST_F(ObjectStorageCatalogTest,
       ForegroundCommitAndUploadCompletionAreIndependent) {
    const auto key = Prepare();
    ASSERT_EQ(catalog.Commit(key, 7), ErrorCode::OK);
    EXPECT_EQ(catalog.Commit(key, 7), ErrorCode::OK);
    EXPECT_EQ(catalog.Commit(key, 8), ErrorCode::INVALID_WRITE);
    ASSERT_TRUE(catalog.Find(key));
    EXPECT_EQ(catalog.Find(key)->phase, ObjectStoragePhase::PREPARED);
    ASSERT_EQ(catalog.Claim(key), ErrorCode::OK);
    ASSERT_EQ(catalog.Finish(key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    EXPECT_EQ(catalog.Finish(key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    EXPECT_EQ(catalog.Finish(key, ObjectUploadOutcome::UNKNOWN),
              ErrorCode::INVALID_WRITE);
    EXPECT_EQ(catalog.Finish(key, ObjectUploadOutcome::NOT_STARTED),
              ErrorCode::INVALID_WRITE);
    EXPECT_EQ(catalog.Head("tenant", "key")->checksum, 7U);
    EXPECT_FALSE(catalog.Prepare("tenant", "key", 4));
}

TEST_F(ObjectStorageCatalogTest, LostRepliesReconcileClaimsCompletionAndOpen) {
    kv->lose_reply = true;
    const auto key = Prepare();
    kv->lose_reply = true;
    ASSERT_EQ(catalog.Claim(key), ErrorCode::OK);
    EXPECT_EQ(catalog.Claim(key), ErrorCode::INVALID_WRITE);
    kv->lose_reply = true;
    ASSERT_EQ(catalog.Finish(key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    kv->lose_reply = true;
    ASSERT_EQ(catalog.Commit(key), ErrorCode::OK);
    kv->lose_reply = true;
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_EQ(restarted.Head("tenant", "key")->phase,
              ObjectStoragePhase::COMPLETE);
}

TEST_F(ObjectStorageCatalogTest,
       FailedReconciliationStopsDispatchUntilFencedReload) {
    const auto key = Prepare();
    kv->lose_reply = true;
    kv->reads_after_lost_reply = 1;
    EXPECT_EQ(catalog.Claim(key), ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_FALSE(catalog.CanReserve(1));
    EXPECT_FALSE(catalog.Find(key));
    EXPECT_EQ(catalog.Reclaim(*objects, 100),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_EQ(catalog.Open(), ErrorCode::OK);
    EXPECT_EQ(catalog.Find(key)->phase, ObjectStoragePhase::UPLOADING);
    EXPECT_EQ(catalog.Claim(key), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(catalog.Finish(key, ObjectUploadOutcome::NOT_STARTED),
              ErrorCode::OK);
}

TEST_F(ObjectStorageCatalogTest, NamespaceAndEpochFenceStaleWriters) {
    const auto key = Prepare();
    ObjectStorageCatalog next(config, kv, "cluster");
    EXPECT_EQ(next.Open(1), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    ASSERT_EQ(next.Open(2), ErrorCode::OK);
    EXPECT_EQ(catalog.Claim(key), ErrorCode::ETCD_TRANSACTION_FAIL);
    EXPECT_TRUE(objects->deletes.empty());
    EXPECT_EQ(next.Claim(key), ErrorCode::OK);
    EXPECT_EQ(next.Finish(key, ObjectUploadOutcome::UNKNOWN), ErrorCode::OK);
    EXPECT_EQ(next.Reclaim(*objects, 1), ErrorCode::OK);
    ObjectStorageCatalog later(config, kv, "cluster");
    ASSERT_EQ(later.Open(3), ErrorCode::OK);
    const auto before = objects->deletes.size();
    EXPECT_EQ(next.Reclaim(*objects, 1), ErrorCode::ETCD_TRANSACTION_FAIL);
    EXPECT_EQ(objects->deletes.size(), before);
    for (int field = 0; field < 6; ++field) {
        auto changed = config;
        auto& ns = changed.storage_namespace;
        switch (field) {
            case 0:
                ns.provider += "x";
                break;
            case 1:
                ns.endpoint += "x";
                break;
            case 2:
                ns.bucket += "x";
                break;
            case 3:
                ns.region += "x";
                break;
            case 4:
                ns.prefix += "x";
                break;
            case 5:
                ns.path_style = !ns.path_style;
                break;
        }
        ObjectStorageCatalog invalid(changed, kv, "cluster");
        EXPECT_EQ(invalid.Open(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(ObjectStorageCatalogTest, NamesIsolateBinaryIdentitiesAndCatalogs) {
    const std::string tenant("t\0", 2), logical("k\0/", 3);
    const auto first = catalog.Prepare(tenant, logical, 1);
    const auto second = catalog.Prepare("t", logical, 1);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_NE(first->object_key, second->object_key);
    ObjectStorageCatalog other(config, kv, "other");
    ASSERT_EQ(other.Open(), ErrorCode::OK);
    const auto third = other.Prepare(tenant, logical, 1);
    ASSERT_TRUE(third);
    EXPECT_NE(first->object_key, third->object_key);
    const auto long_key = catalog.Prepare("tenant", std::string(4096, 'x'), 1);
    ASSERT_TRUE(long_key);
    EXPECT_LT(long_key->object_key.size(), 128U);
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_EQ(restarted.Head(tenant, logical)->object_key, first->object_key);
}

TEST_F(ObjectStorageCatalogTest,
       SingleSlotReplacementIsAtomicAcrossLostReplyAndRestart) {
    for (const bool lose_reconciliation : {false, true}) {
        auto cfg = config;
        cfg.max_pending_objects = 1;
        cfg.max_pending_bytes = 4;
        auto backend = std::make_shared<CatalogKv>();
        ObjectStorageCatalog single(cfg, backend, "single");
        ASSERT_EQ(single.Open(), ErrorCode::OK);
        auto old = single.Prepare("tenant", "key", 4);
        ASSERT_TRUE(old);
        ASSERT_EQ(single.Claim(old->object_key), ErrorCode::OK);
        ASSERT_EQ(
            single.Finish(old->object_key, ObjectUploadOutcome::NOT_STARTED),
            ErrorCode::OK);
        backend->lose_reply = true;
        backend->reads_after_lost_reply = lose_reconciliation ? 1 : 0;
        const auto replacement = single.Prepare("tenant", "key", 4);
        EXPECT_EQ(bool(replacement), !lose_reconciliation);
        ObjectStorageCatalog restarted(cfg, backend, "single");
        ASSERT_EQ(restarted.Open(), ErrorCode::OK);
        auto head = restarted.Head("tenant", "key");
        ASSERT_TRUE(head);
        EXPECT_NE(head->object_key, old->object_key);
        EXPECT_EQ(head->phase, ObjectStoragePhase::PREPARED);
        EXPECT_EQ(restarted.Reclaim(*objects, 0), ErrorCode::OK);
        EXPECT_TRUE(objects->deletes.empty());
        EXPECT_EQ(restarted.Claim(head->object_key), ErrorCode::OK);
        EXPECT_FALSE(restarted.CanReserve(1));
    }
}

TEST_F(ObjectStorageCatalogTest,
       UnknownPutKeepsTombstoneAcrossSuccessfulDeletesAndRestart) {
    const auto key = Prepare();
    ASSERT_EQ(catalog.Claim(key), ErrorCode::OK);
    ASSERT_EQ(catalog.Finish(key, ObjectUploadOutcome::UNKNOWN), ErrorCode::OK);
    EXPECT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_TRUE(catalog.Find(key));
    ASSERT_EQ(objects->deletes.size(), 1U);
    objects->objects[key] =
        "late";  // PUT materializes after successful DELETE.
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_EQ(restarted.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_FALSE(objects->objects.contains(key));
    EXPECT_TRUE(restarted.Find(key));
    EXPECT_EQ(restarted.Finish(key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    EXPECT_EQ(restarted.Commit(key), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(restarted.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_FALSE(restarted.Find(key));
    EXPECT_EQ(objects->deletes.size(), 3U);
}

TEST_F(ObjectStorageCatalogTest, ReplacementCreditsCannotOverflowPendingBytes) {
    auto cfg = config;
    cfg.max_object_bytes = cfg.max_pending_bytes =
        std::numeric_limits<uint64_t>::max();
    cfg.max_pending_objects = 1;
    ObjectStorageCatalog maximum(cfg, kv, "maximum");
    ASSERT_EQ(maximum.Open(), ErrorCode::OK);
    const auto old = maximum.Prepare("t", "k", cfg.max_object_bytes);
    ASSERT_TRUE(old);
    ASSERT_EQ(maximum.Finish(old->object_key, ObjectUploadOutcome::NOT_STARTED),
              ErrorCode::OK);
    const auto replacement = maximum.Prepare("t", "k", cfg.max_object_bytes);
    ASSERT_TRUE(replacement);
    EXPECT_NE(old->object_key, replacement->object_key);
    EXPECT_FALSE(maximum.CanReserve(1));
    ObjectStorageCatalog restarted(cfg, kv, "maximum");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_EQ(restarted.Head("t", "k")->object_key, replacement->object_key);
}

TEST_F(ObjectStorageCatalogTest,
       LateCompletionDuringDeleteRequiresAnotherDelete) {
    const auto old = Prepare();
    ASSERT_EQ(catalog.Claim(old), ErrorCode::OK);
    ASSERT_EQ(catalog.Finish(old, ObjectUploadOutcome::UNKNOWN), ErrorCode::OK);
    const auto replacement = Prepare();
    objects->during_delete = [&] {
        EXPECT_EQ(catalog.Finish(old, ObjectUploadOutcome::COMPLETE),
                  ErrorCode::OK);
    };
    EXPECT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_TRUE(catalog.Find(old));
    EXPECT_EQ(catalog.Head("tenant", "key")->object_key, replacement);
    objects->during_delete = {};
    objects->objects[old] = "late";
    EXPECT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_FALSE(catalog.Find(old));
    EXPECT_FALSE(objects->objects.contains(old));
    EXPECT_EQ(catalog.Head("tenant", "key")->object_key, replacement);
}

TEST_F(ObjectStorageCatalogTest,
       DeleteAndKvCleanupFailuresAreRetainedForRetry) {
    const auto key = Prepare();
    Complete(key);
    EXPECT_EQ(catalog.Retire(key, 0), ErrorCode::OK);
    objects->fail_delete = true;
    EXPECT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    EXPECT_TRUE(catalog.Find(key));
    objects->fail_delete = false;
    kv->fail_delete = true;
    EXPECT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_EQ(catalog.Head("tenant", "key")->phase,
              ObjectStoragePhase::DELETED);
    const auto before = objects->deletes.size();
    kv->fail_delete = false;
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_EQ(restarted.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_EQ(objects->deletes.size(), before);
    EXPECT_FALSE(restarted.Find(key));
}

TEST_F(ObjectStorageCatalogTest,
       DeleteDoesNotHoldCatalogLockAndFreezesLeaseDeadline) {
    using namespace std::chrono_literals;
    const auto key = Prepare();
    Complete(key);
    ASSERT_EQ(catalog.Retire(key, 10), ErrorCode::OK);
    ASSERT_EQ(catalog.Retire(key, 20), ErrorCode::OK);
    std::promise<void> entered, release;
    auto entry = entered.get_future();
    auto released = release.get_future().share();
    objects->during_delete = [&] {
        entered.set_value();
        released.wait();
    };
    auto gc = std::async(std::launch::async,
                         [&] { return catalog.Reclaim(*objects, 20); });
    const bool deleting = entry.wait_for(5s) == std::future_status::ready;
    EXPECT_TRUE(deleting);
    auto query = std::async(std::launch::async,
                            [&] { return catalog.Head("tenant", "key"); });
    const bool unlocked = query.wait_for(1s) == std::future_status::ready;
    EXPECT_TRUE(unlocked);
    if (unlocked && deleting) {
        EXPECT_EQ(catalog.Retire(key, 21),
                  ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        EXPECT_EQ(catalog.Commit(key), ErrorCode::INVALID_WRITE);
    }
    release.set_value();
    EXPECT_EQ(gc.get(), ErrorCode::OK);
    EXPECT_TRUE(query.get());
}

TEST_F(ObjectStorageCatalogTest,
       BackpressureIncludesUnknownAttemptsAndRetirements) {
    auto cfg = config;
    cfg.max_pending_objects = 1;
    cfg.max_pending_bytes = 4;
    ObjectStorageCatalog limited(cfg, kv, "limited");
    ASSERT_EQ(limited.Open(), ErrorCode::OK);
    const auto key = limited.Prepare("t", "k", 4);
    ASSERT_TRUE(key);
    EXPECT_FALSE(limited.Prepare("t", "large", 17));
    EXPECT_FALSE(limited.CanReserve(0));
    ASSERT_EQ(limited.Claim(key->object_key), ErrorCode::OK);
    ASSERT_EQ(limited.Finish(key->object_key, ObjectUploadOutcome::COMPLETE),
              ErrorCode::OK);
    EXPECT_FALSE(
        limited.CanReserve(1));  // Upload alone is not foreground commit.
    ASSERT_EQ(limited.Commit(key->object_key), ErrorCode::OK);
    const auto second = limited.Prepare("t", "other", 4);
    ASSERT_TRUE(second);
    ASSERT_EQ(limited.Retire(key->object_key, 0),
              ErrorCode::OK);  // Obligations may exceed admission limits.
    EXPECT_FALSE(limited.CanReserve(0));
    ASSERT_EQ(limited.Claim(second->object_key), ErrorCode::OK);
    ASSERT_EQ(limited.Finish(second->object_key, ObjectUploadOutcome::UNKNOWN),
              ErrorCode::OK);
    ASSERT_EQ(limited.Reclaim(*objects, 0), ErrorCode::OK);
    EXPECT_FALSE(limited.CanReserve(0));
    ObjectStorageCatalog restarted(cfg, kv, "limited");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_FALSE(restarted.CanReserve(0));
    EXPECT_FALSE(restarted.Prepare("t", "other", 4));
}

TEST_F(ObjectStorageCatalogTest,
       BoundedMaintenanceRotatesPastFutureAndUncertainRecords) {
    auto cfg = config;
    cfg.maintenance_batch_size = 1;
    ObjectStorageCatalog bounded(cfg, kv, "bounded");
    ASSERT_EQ(bounded.Open(), ErrorCode::OK);
    std::vector<std::string> names;
    for (int i = 0; i < 3; ++i) {
        auto record = bounded.Prepare("t", std::to_string(i), 1);
        ASSERT_TRUE(record);
        names.push_back(record->object_key);
        ASSERT_EQ(bounded.Claim(record->object_key), ErrorCode::OK);
        ASSERT_EQ(
            bounded.Finish(record->object_key, ObjectUploadOutcome::COMPLETE),
            ErrorCode::OK);
        ASSERT_EQ(bounded.Commit(record->object_key), ErrorCode::OK);
    }
    std::sort(names.begin(), names.end());
    ASSERT_EQ(bounded.Retire(names[0], 100), ErrorCode::OK);
    ASSERT_EQ(bounded.Retire(names[1], 0), ErrorCode::OK);
    ASSERT_EQ(bounded.Retire(names[2], 0), ErrorCode::OK);
    for (int i = 0; i < 3; ++i) {
        const auto before = objects->deletes.size();
        EXPECT_EQ(bounded.Reclaim(*objects, 0), ErrorCode::OK);
        EXPECT_LE(objects->deletes.size() - before, 1U);
    }
    EXPECT_TRUE(bounded.Find(names[0]));
    EXPECT_FALSE(bounded.Find(names[1]));
    EXPECT_FALSE(bounded.Find(names[2]));
}

TEST_F(ObjectStorageCatalogTest,
       FailedOpenNeverExposesPartiallyRecoveredState) {
    Prepare();
    ObjectStorageCatalog restarted(config, kv, "cluster");
    kv->fail_range = true;
    EXPECT_EQ(restarted.Open(), ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_FALSE(restarted.Head("tenant", "key"));
    EXPECT_FALSE(restarted.Records());
    EXPECT_FALSE(restarted.CanReserve(1));
    kv->fail_range = false;
    EXPECT_EQ(restarted.Open(), ErrorCode::OK);
    EXPECT_TRUE(restarted.Head("tenant", "key"));
}

TEST_F(ObjectStorageCatalogTest, CorruptRecordAndHeadAreRejected) {
    const auto key = Prepare();
    const auto saved = kv->data;
    const auto record_key = "/mooncake/object-storage/cluster/records/" + key;
    for (const std::string& corrupt :
         {std::string("not-base64!"), std::string("AA==")}) {
        kv->data = saved;
        kv->data[record_key] = corrupt;
        ObjectStorageCatalog invalid(config, kv, "cluster");
        EXPECT_EQ(invalid.Open(), ErrorCode::DESERIALIZE_FAIL);
        EXPECT_FALSE(invalid.Records());
    }
    kv->data = saved;
    auto broken = *catalog.Find(key);
    broken.namespace_id += "wrong";
    kv->data[record_key] = Encode(broken);
    ObjectStorageCatalog wrong_ns(config, kv, "cluster");
    EXPECT_EQ(wrong_ns.Open(), ErrorCode::DESERIALIZE_FAIL);
    kv->data = saved;
    broken = *catalog.Find(key);
    ++broken.revision;
    kv->data[record_key] = Encode(broken);
    ObjectStorageCatalog mismatched(config, kv, "cluster");
    EXPECT_EQ(mismatched.Open(), ErrorCode::DESERIALIZE_FAIL);
    kv->data = saved;
    broken = *catalog.Find(key);
    broken.phase = ObjectStoragePhase::DELETED;
    broken.upload_finished = true;
    const auto head_key = "/mooncake/object-storage/cluster/heads/" +
                          Encode(std::make_pair(broken.tenant, broken.key));
    kv->data[head_key] = Encode(broken);
    ObjectStorageCatalog deleted_head(config, kv, "cluster");
    EXPECT_EQ(deleted_head.Open(), ErrorCode::DESERIALIZE_FAIL);
}

TEST_F(ObjectStorageCatalogTest,
       RecoveryLoadsMultiplePagesAndRetainsPendingClaims) {
    auto cfg = config;
    cfg.max_pending_objects = 600;
    cfg.max_pending_bytes = 600;
    ObjectStorageCatalog many(cfg, kv, "many");
    ASSERT_EQ(many.Open(), ErrorCode::OK);
    for (int i = 0; i < 513; ++i)
        ASSERT_TRUE(many.Prepare("t", std::to_string(i), 1));
    auto head = many.Head("t", "0");
    ASSERT_TRUE(head);
    ASSERT_EQ(many.Claim(head->object_key), ErrorCode::OK);
    const auto before = kv->range_calls;
    ObjectStorageCatalog restarted(cfg, kv, "many");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    ASSERT_TRUE(restarted.Records());
    EXPECT_EQ(restarted.Records()->size(), 513U);
    EXPECT_EQ(kv->range_calls - before, 6U);
    EXPECT_EQ(restarted.Claim(head->object_key), ErrorCode::INVALID_WRITE);
}

TEST_F(ObjectStorageCatalogTest, GenerationExhaustionCannotReuseAnOldName) {
    const auto created = catalog.Prepare("t", "k", 1);
    ASSERT_TRUE(created);
    ASSERT_EQ(catalog.Retire(created->object_key, 0), ErrorCode::OK);
    ASSERT_EQ(catalog.Reclaim(*objects, 0), ErrorCode::OK);
    auto head = *catalog.Head("t", "k");
    head.generation = std::numeric_limits<uint64_t>::max();
    head.revision = 1;
    head.namespace_id = Encode(config.storage_namespace);
    head.object_key =
        head.object_key.substr(0, 64) + "/" + std::to_string(head.generation);
    head.phase = ObjectStoragePhase::DELETED;
    head.upload_finished = true;
    kv->data["/mooncake/object-storage/cluster/heads/" +
             Encode(std::make_pair(head.tenant, head.key))] = Encode(head);
    ObjectStorageCatalog restarted(config, kv, "cluster");
    ASSERT_EQ(restarted.Open(), ErrorCode::OK);
    const auto next = restarted.Prepare("t", "k", 1);
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error(), ErrorCode::INVALID_WRITE);
}

}  // namespace
}  // namespace mooncake
