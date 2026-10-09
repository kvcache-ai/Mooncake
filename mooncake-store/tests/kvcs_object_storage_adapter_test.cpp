#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#ifdef MOONCAKE_KVCS_TEST_WRAP
#include <cerrno>

#include <kvcs_capi.h>

namespace {

std::mutex kvcs_forced_put_statuses_mutex;
std::map<std::string, int> kvcs_forced_put_statuses;
std::atomic<size_t> kvcs_wrapped_put_failures{0};
std::atomic<size_t> kvcs_delete_failures_remaining{0};
std::atomic<size_t> kvcs_wrapped_delete_failures{0};

std::optional<int> ForcedKvcsLowLevelPutStatus(std::string_view key) {
    std::lock_guard lock(kvcs_forced_put_statuses_mutex);
    auto it = kvcs_forced_put_statuses.find(std::string(key));
    if (it == kvcs_forced_put_statuses.end()) return std::nullopt;
    return it->second;
}

}  // namespace

extern "C" int __real_kvcs_ll_batch_put(
    kvcs_ll_client_t*, const kvcs_ll_put_item_t*, int, kvcs_put_result_t*, int,
    int64_t, const kvcs_ll_batch_opts_t*) __attribute__((weak));

extern "C" int __wrap_kvcs_ll_batch_put(
    kvcs_ll_client_t* client, const kvcs_ll_put_item_t* items, int count,
    kvcs_put_result_t* results, int capacity, int64_t deadline_ns,
    const kvcs_ll_batch_opts_t* options) {
    if (__real_kvcs_ll_batch_put == nullptr) return -ENOTSUP;
    if (items == nullptr || results == nullptr || count <= 0 ||
        capacity < count) {
        return __real_kvcs_ll_batch_put(client, items, count, results, capacity,
                                        deadline_ns, options);
    }

    bool has_failure = false;
    for (int index = 0; index < count; ++index) {
        if (items[index].key != nullptr &&
            ForcedKvcsLowLevelPutStatus(items[index].key).has_value()) {
            has_failure = true;
            break;
        }
    }
    if (!has_failure) {
        return __real_kvcs_ll_batch_put(client, items, count, results, capacity,
                                        deadline_ns, options);
    }

    for (int index = 0; index < count; ++index) {
        const auto forced_status =
            items[index].key == nullptr
                ? std::nullopt
                : ForcedKvcsLowLevelPutStatus(items[index].key);
        if (forced_status.has_value()) {
            results[index] = {};
            results[index].status = *forced_status;
            kvcs_wrapped_put_failures.fetch_add(1,
                                                std::memory_order_relaxed);
            continue;
        }
        const int status = __real_kvcs_ll_batch_put(
            client, &items[index], 1, &results[index], 1, deadline_ns, options);
        if (status != 0) return status;
    }
    return 0;
}

extern "C" int __real_kvcs_ll_batch_delete(
    kvcs_ll_client_t*, const char* const*, int, int*, int, int64_t,
    const kvcs_ll_batch_opts_t*) __attribute__((weak));

extern "C" int __wrap_kvcs_ll_batch_delete(
    kvcs_ll_client_t* client, const char* const* keys, int count, int* statuses,
    int capacity, int64_t deadline_ns, const kvcs_ll_batch_opts_t* options) {
    if (__real_kvcs_ll_batch_delete == nullptr) return -ENOTSUP;
    size_t remaining =
        kvcs_delete_failures_remaining.load(std::memory_order_relaxed);
    while (remaining != 0 &&
           !kvcs_delete_failures_remaining.compare_exchange_weak(
               remaining, remaining - 1, std::memory_order_relaxed)) {
    }
    if (remaining != 0) {
        kvcs_wrapped_delete_failures.fetch_add(1,
                                               std::memory_order_relaxed);
        return -ETIMEDOUT;
    }
    return __real_kvcs_ll_batch_delete(client, keys, count, statuses, capacity,
                                       deadline_ns, options);
}
#endif

namespace mooncake {
namespace {

#ifdef MOONCAKE_KVCS_TEST_WRAP
class ScopedKvcsLowLevelPutFailures {
   public:
    ScopedKvcsLowLevelPutFailures(
        std::initializer_list<std::pair<const std::string, int>> failures) {
        std::lock_guard lock(kvcs_forced_put_statuses_mutex);
        saved_ = kvcs_forced_put_statuses;
        kvcs_forced_put_statuses =
            std::map<std::string, int>(failures);
        failure_count_before_ =
            kvcs_wrapped_put_failures.load(std::memory_order_relaxed);
    }

    ~ScopedKvcsLowLevelPutFailures() {
        std::lock_guard lock(kvcs_forced_put_statuses_mutex);
        kvcs_forced_put_statuses = std::move(saved_);
    }

    size_t FailureCount() const {
        return kvcs_wrapped_put_failures.load(std::memory_order_relaxed) -
               failure_count_before_;
    }

   private:
    std::map<std::string, int> saved_;
    size_t failure_count_before_ = 0;
};

class ScopedKvcsLowLevelDeleteFailures {
   public:
    explicit ScopedKvcsLowLevelDeleteFailures(size_t count)
        : failure_count_before_(
              kvcs_wrapped_delete_failures.load(std::memory_order_relaxed)) {
        kvcs_delete_failures_remaining.store(count,
                                             std::memory_order_relaxed);
    }

    ~ScopedKvcsLowLevelDeleteFailures() {
        kvcs_delete_failures_remaining.store(0, std::memory_order_relaxed);
    }

    size_t FailureCount() const {
        return kvcs_wrapped_delete_failures.load(std::memory_order_relaxed) -
               failure_count_before_;
    }

   private:
    size_t failure_count_before_ = 0;
};
#endif

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

#ifdef MOONCAKE_KVCS_TEST_NO_SDK
TEST(KvcsDriverTest, MissingSdkReturnsNotSupported) {
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_FALSE(drivers);
    EXPECT_EQ(drivers.error(), ErrorCode::NOT_SUPPORTED);
}
#endif

#ifdef MOONCAKE_KVCS_TEST_SDK
struct KvcsLowLevelEnvironment {
    KvcsLowLevelEnvironment() {
        efc_socket.Set("/mock/unused.sock");
        max_key_size.Set("256");
        max_value_size.Set("32");
    }

    ScopedEnvVar efc_socket{"MOONCAKE_KVCS_EFC_SOCKET"};
    ScopedEnvVar max_key_size{"MOONCAKE_KVCS_MAX_KEY_SIZE"};
    ScopedEnvVar max_value_size{"MOONCAKE_KVCS_MAX_VALUE_SIZE"};
};
#endif

#ifdef MOONCAKE_KVCS_TEST_SDK
TEST(KvcsDriverTest, LowLevelPreservesLongSingleShardKeys) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    ASSERT_EQ(drivers->size(), 1u);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());
    EXPECT_EQ(driver.MaxKeySize(), 256u);

    std::string key(256, 's');
    std::string value = "single-shard";
    const std::array<KvcsShardPutRequest, 1> puts{{
        {.logical_key = key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{value.data(), value.size()}},
         .size = value.size()},
    }};
    auto put_results = driver.BatchPut(puts);
    ASSERT_EQ(put_results.size(), 1u);
    ASSERT_TRUE(put_results[0]);

    const std::array<ObjectKey, 1> keys{key};
    auto queries = driver.BatchQuery(keys);
    ASSERT_EQ(queries.size(), 1u);
    ASSERT_TRUE(queries[0]);
    EXPECT_FALSE(queries[0]->layout_known);

    std::string output(value.size(), '\0');
    const std::array<KvcsShardGetRequest, 1> gets{{
        {.logical_key = key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{output.data(), output.size()}},
         .size = output.size()},
    }};
    auto get_results = driver.BatchGet(gets);
    ASSERT_EQ(get_results.size(), 1u);
    ASSERT_TRUE(get_results[0]);
    EXPECT_EQ(output, value);

    auto deletes = driver.BatchDelete(keys);
    ASSERT_EQ(deletes.size(), 1u);
    EXPECT_TRUE(deletes[0]);
}

TEST(KvcsDriverTest, LowLevelEnforcesMultiShardPhysicalKeyBudget) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    std::string first(32, 'a');
    std::string second = "b";
    const auto make_puts = [&](const ObjectKey& key) {
        return std::array<KvcsShardPutRequest, 2>{{
            {.logical_key = key,
             .total_shard = 2,
             .shard_id = 0,
             .slices = {{first.data(), first.size()}},
             .size = first.size()},
            {.logical_key = key,
             .total_shard = 2,
             .shard_id = 1,
             .slices = {{second.data(), second.size()}},
             .size = second.size()},
        }};
    };

    const ObjectKey accepted_key(247, 'a');
    auto accepted = driver.BatchPut(make_puts(accepted_key));
    ASSERT_EQ(accepted.size(), 2u);
    ASSERT_TRUE(accepted[0]);
    ASSERT_TRUE(accepted[1]);
    const std::array<ObjectKey, 1> accepted_keys{accepted_key};
    auto accepted_query = driver.BatchQuery(accepted_keys);
    ASSERT_EQ(accepted_query.size(), 1u);
    ASSERT_TRUE(accepted_query[0]);
    EXPECT_EQ(accepted_query[0]->total_shard, 2u);
    EXPECT_TRUE(driver.BatchDelete(accepted_keys)[0]);

    const ObjectKey rejected_key(248, 'b');
    auto rejected = driver.BatchPut(make_puts(rejected_key));
    ASSERT_EQ(rejected.size(), 2u);
    ASSERT_FALSE(rejected[0]);
    ASSERT_FALSE(rejected[1]);
    EXPECT_EQ(rejected[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(rejected[1].error(), ErrorCode::INVALID_PARAMS);

    const std::array<ObjectKey, 1> long_missing_key{ObjectKey(256, 'm')};
    auto missing = driver.BatchQuery(long_missing_key);
    ASSERT_EQ(missing.size(), 1u);
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsDriverTest, LowLevelValidatesMaximumPartSuffix) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    // Keep this in sync with the driver's materialized-shard safety bound.
    // The largest supported shard id is 1048575; its part suffix consumes
    // 13 bytes, leaving 243 bytes for the logical key.
    constexpr uint32_t kMaxMaterializedShards = 1U << 20;
    char accepted_output = '\0';
    const std::array<KvcsShardGetRequest, 1> accepted{{
        {.logical_key = ObjectKey(243, 'a'),
         .total_shard = kMaxMaterializedShards,
         .shard_id = kMaxMaterializedShards - 1,
         .slices = {{&accepted_output, 1}},
         .size = 1},
    }};
    const auto accepted_results = driver.BatchGet(accepted);
    ASSERT_EQ(accepted_results.size(), 1u);
    ASSERT_FALSE(accepted_results[0]);
    EXPECT_EQ(accepted_results[0].error(), ErrorCode::OBJECT_NOT_FOUND);

    char rejected_output = '\0';
    const std::array<KvcsShardGetRequest, 1> rejected{{
        {.logical_key = ObjectKey(244, 'b'),
         .total_shard = kMaxMaterializedShards,
         .shard_id = kMaxMaterializedShards - 1,
         .slices = {{&rejected_output, 1}},
         .size = 1},
    }};
    const auto rejected_results = driver.BatchGet(rejected);
    ASSERT_EQ(rejected_results.size(), 1u);
    ASSERT_FALSE(rejected_results[0]);
    EXPECT_EQ(rejected_results[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsDriverTest, LowLevelReusesMatchingInsertOnlyChunks) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "recover-matching-chunks";
    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> stale_chunks{{
        {.logical_key = logical_key + ".part_0",
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key + ".part_1",
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    const auto stale_results = driver.BatchPut(stale_chunks);
    ASSERT_EQ(stale_results.size(), stale_chunks.size());
    ASSERT_TRUE(stale_results[0]);
    ASSERT_TRUE(stale_results[1]);

    const std::array<KvcsShardPutRequest, 2> retry{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    const auto retry_results = driver.BatchPut(retry);
    ASSERT_EQ(retry_results.size(), retry.size());
    ASSERT_TRUE(retry_results[0]);
    ASSERT_TRUE(retry_results[1]);

    const std::array<ObjectKey, 1> keys{logical_key};
    const auto query = driver.BatchQuery(keys);
    ASSERT_EQ(query.size(), 1u);
    ASSERT_TRUE(query[0]);
    EXPECT_EQ(query[0]->total_shard, 2u);

    std::string first_output(first.size(), '\0');
    std::string second_output(second.size(), '\0');
    const std::array<KvcsShardGetRequest, 2> gets{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first_output.data(), first_output.size()}},
         .size = first_output.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second_output.data(), second_output.size()}},
         .size = second_output.size()},
    }};
    const auto get_results = driver.BatchGet(gets);
    ASSERT_EQ(get_results.size(), gets.size());
    ASSERT_TRUE(get_results[0]);
    ASSERT_TRUE(get_results[1]);
    EXPECT_EQ(first_output, first);
    EXPECT_EQ(second_output, second);
    EXPECT_TRUE(driver.BatchDelete(keys)[0]);
}

TEST(KvcsDriverTest, LowLevelRollbackPreservesPreexistingPhysicalKeys) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "rollback-owned-chunks";
    const ObjectKey blocker_key = logical_key + ".part_1";
    std::string blocker = "different";
    const std::array<KvcsShardPutRequest, 1> blocker_put{{
        {.logical_key = blocker_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{blocker.data(), blocker.size()}},
         .size = blocker.size()},
    }};
    ASSERT_TRUE(driver.BatchPut(blocker_put)[0]);

    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> puts{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    const auto put_results = driver.BatchPut(puts);
    ASSERT_EQ(put_results.size(), puts.size());
    ASSERT_FALSE(put_results[0]);
    ASSERT_FALSE(put_results[1]);
    EXPECT_EQ(put_results[0].error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(put_results[1].error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    const std::array<ObjectKey, 2> physical_keys{logical_key + ".part_0",
                                                 blocker_key};
    const auto query = driver.BatchQuery(physical_keys);
    ASSERT_EQ(query.size(), physical_keys.size());
    ASSERT_FALSE(query[0]);
    EXPECT_EQ(query[0].error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_TRUE(query[1]);

    std::string blocker_output(blocker.size(), '\0');
    const std::array<KvcsShardGetRequest, 1> blocker_get{{
        {.logical_key = blocker_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{blocker_output.data(), blocker_output.size()}},
         .size = blocker_output.size()},
    }};
    ASSERT_TRUE(driver.BatchGet(blocker_get)[0]);
    EXPECT_EQ(blocker_output, blocker);
    const std::array<ObjectKey, 1> blocker_keys{blocker_key};
    ASSERT_TRUE(driver.BatchDelete(blocker_keys)[0]);

    const auto retry = driver.BatchPut(puts);
    ASSERT_EQ(retry.size(), puts.size());
    ASSERT_TRUE(retry[0]);
    ASSERT_TRUE(retry[1]);
    const std::array<ObjectKey, 1> logical_keys{logical_key};
    EXPECT_TRUE(driver.BatchDelete(logical_keys)[0]);
}

#ifdef MOONCAKE_KVCS_TEST_WRAP
TEST(KvcsDriverTest, LowLevelAmbiguousPutDoesNotRollbackKnownSibling) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "ambiguous-put-rollback";
    const ObjectKey first_chunk_key = logical_key + ".part_0";
    const ObjectKey ambiguous_chunk_key = logical_key + ".part_1";
    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> puts{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    {
        ScopedKvcsLowLevelPutFailures put_failures{
            {ambiguous_chunk_key, -ENOMEM},
        };
        const auto failed = driver.BatchPut(puts);
        EXPECT_EQ(put_failures.FailureCount(), 1u);
        ASSERT_EQ(failed.size(), puts.size());
        ASSERT_FALSE(failed[0]);
        ASSERT_FALSE(failed[1]);
        EXPECT_EQ(failed[0].error(), ErrorCode::KVCS_INCOMPLETE);
        EXPECT_EQ(failed[1].error(), ErrorCode::KVCS_INCOMPLETE);
    }

    const ObjectKey unrelated_key = "unrelated-after-quarantine";
    std::string unrelated_value = "safe";
    const std::array<KvcsShardPutRequest, 3> retry{{
        puts[0],
        puts[1],
        {.logical_key = unrelated_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{unrelated_value.data(), unrelated_value.size()}},
         .size = unrelated_value.size()},
    }};
    const auto retry_results = driver.BatchPut(retry);
    ASSERT_EQ(retry_results.size(), retry.size());
    ASSERT_FALSE(retry_results[0]);
    ASSERT_FALSE(retry_results[1]);
    EXPECT_EQ(retry_results[0].error(), ErrorCode::KVCS_INCOMPLETE);
    EXPECT_EQ(retry_results[1].error(), ErrorCode::KVCS_INCOMPLETE);
    EXPECT_TRUE(retry_results[2]);

    const std::array<ObjectKey, 2> physical_keys{first_chunk_key,
                                                 ambiguous_chunk_key};
    const auto physical_query = driver.BatchQuery(physical_keys);
    ASSERT_EQ(physical_query.size(), physical_keys.size());
    ASSERT_TRUE(physical_query[0]);
    ASSERT_FALSE(physical_query[1]);
    EXPECT_EQ(physical_query[1].error(), ErrorCode::OBJECT_NOT_FOUND);

    const std::array<ObjectKey, 1> cleanup{first_chunk_key};
    EXPECT_TRUE(driver.BatchDelete(cleanup)[0]);
    const std::array<ObjectKey, 1> unrelated_cleanup{unrelated_key};
    EXPECT_TRUE(driver.BatchDelete(unrelated_cleanup)[0]);
}

TEST(KvcsDriverTest, LowLevelRollbackDeleteTimeoutQuarantinesPhysicalKey) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "rollback-delete-recovery";
    const ObjectKey first_chunk_key = logical_key + ".part_0";
    const ObjectKey blocker_key = logical_key + ".part_1";
    std::string blocker = "different";
    const std::array<KvcsShardPutRequest, 1> blocker_put{{
        {.logical_key = blocker_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{blocker.data(), blocker.size()}},
         .size = blocker.size()},
    }};
    ASSERT_TRUE(driver.BatchPut(blocker_put)[0]);

    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> puts{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    {
        ScopedKvcsLowLevelDeleteFailures delete_failures(1);
        const auto failed = driver.BatchPut(puts);
        EXPECT_EQ(delete_failures.FailureCount(), 1u);
        ASSERT_EQ(failed.size(), puts.size());
        ASSERT_FALSE(failed[0]);
        ASSERT_FALSE(failed[1]);
        EXPECT_EQ(failed[0].error(), ErrorCode::KVCS_INCOMPLETE);
        EXPECT_EQ(failed[1].error(), ErrorCode::KVCS_INCOMPLETE);
    }

    const std::array<ObjectKey, 2> physical_keys{first_chunk_key, blocker_key};
    const auto physical_query = driver.BatchQuery(physical_keys);
    ASSERT_EQ(physical_query.size(), physical_keys.size());
    ASSERT_TRUE(physical_query[0]);
    ASSERT_TRUE(physical_query[1]);

    const std::array<ObjectKey, 1> blocker_keys{blocker_key};
    ASSERT_TRUE(driver.BatchDelete(blocker_keys)[0]);

    const auto retried = driver.BatchPut(puts);
    ASSERT_EQ(retried.size(), puts.size());
    ASSERT_FALSE(retried[0]);
    ASSERT_FALSE(retried[1]);
    EXPECT_EQ(retried[0].error(), ErrorCode::KVCS_INCOMPLETE);
    EXPECT_EQ(retried[1].error(), ErrorCode::KVCS_INCOMPLETE);

    const std::array<ObjectKey, 1> logical_keys{logical_key};
    const auto query = driver.BatchQuery(logical_keys);
    ASSERT_EQ(query.size(), 1u);
    ASSERT_FALSE(query[0]);
    EXPECT_EQ(query[0].error(), ErrorCode::KVCS_INCOMPLETE);

    const std::array<ObjectKey, 1> cleanup{first_chunk_key};
    EXPECT_TRUE(driver.BatchDelete(cleanup)[0]);
}

TEST(KvcsDriverTest, LowLevelAmbiguousManifestPutQuarantinesObject) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "ambiguous-manifest-put";
    const ObjectKey manifest_key = logical_key + ".manifest";
    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> puts{{
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = logical_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    {
        ScopedKvcsLowLevelPutFailures put_failures{
            {manifest_key, -ENOMEM},
        };
        const auto failed = driver.BatchPut(puts);
        EXPECT_EQ(put_failures.FailureCount(), 1u);
        ASSERT_EQ(failed.size(), puts.size());
        ASSERT_FALSE(failed[0]);
        ASSERT_FALSE(failed[1]);
        EXPECT_EQ(failed[0].error(), ErrorCode::KVCS_INCOMPLETE);
        EXPECT_EQ(failed[1].error(), ErrorCode::KVCS_INCOMPLETE);
    }

    const auto retry = driver.BatchPut(puts);
    ASSERT_EQ(retry.size(), puts.size());
    ASSERT_FALSE(retry[0]);
    ASSERT_FALSE(retry[1]);
    EXPECT_EQ(retry[0].error(), ErrorCode::KVCS_INCOMPLETE);
    EXPECT_EQ(retry[1].error(), ErrorCode::KVCS_INCOMPLETE);

    const std::array<ObjectKey, 1> logical_keys{logical_key};
    const auto query = driver.BatchQuery(logical_keys);
    ASSERT_EQ(query.size(), logical_keys.size());
    ASSERT_FALSE(query[0]);
    EXPECT_EQ(query[0].error(), ErrorCode::KVCS_INCOMPLETE);

    const std::array<ObjectKey, 2> cleanup{
        logical_key + ".part_0", logical_key + ".part_1"};
    const auto cleanup_results = driver.BatchDelete(cleanup);
    ASSERT_EQ(cleanup_results.size(), cleanup.size());
    EXPECT_TRUE(cleanup_results[0]);
    EXPECT_TRUE(cleanup_results[1]);
}

TEST(KvcsDriverTest, LowLevelDeleteTimeoutQuarantinesPhysicalKey) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey logical_key = "ambiguous-explicit-delete";
    std::string value = "value";
    const std::array<KvcsShardPutRequest, 1> puts{{
        {.logical_key = logical_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{value.data(), value.size()}},
         .size = value.size()},
    }};
    ASSERT_TRUE(driver.BatchPut(puts)[0]);

    const std::array<ObjectKey, 1> keys{logical_key};
    {
        ScopedKvcsLowLevelDeleteFailures delete_failures(1);
        const auto deleted = driver.BatchDelete(keys);
        EXPECT_EQ(delete_failures.FailureCount(), 1u);
        ASSERT_EQ(deleted.size(), keys.size());
        ASSERT_FALSE(deleted[0]);
        EXPECT_EQ(deleted[0].error(), ErrorCode::RPC_TIMEOUT);
    }

    const auto retry = driver.BatchPut(puts);
    ASSERT_EQ(retry.size(), puts.size());
    ASSERT_FALSE(retry[0]);
    EXPECT_EQ(retry[0].error(), ErrorCode::KVCS_INCOMPLETE);

    const auto query = driver.BatchQuery(keys);
    ASSERT_EQ(query.size(), keys.size());
    ASSERT_FALSE(query[0]);
    EXPECT_EQ(query[0].error(), ErrorCode::KVCS_INCOMPLETE);
    const auto quarantined_delete = driver.BatchDelete(keys);
    ASSERT_EQ(quarantined_delete.size(), keys.size());
    ASSERT_FALSE(quarantined_delete[0]);
    EXPECT_EQ(quarantined_delete[0].error(), ErrorCode::KVCS_INCOMPLETE);
}
#endif

TEST(KvcsDriverTest, LowLevelCompleteObjectsRemainInsertOnly) {
    KvcsLowLevelEnvironment environment;
    const std::array<uint32_t, 1> mountpoints{0};
    auto drivers = CreateKvcsLowLevelDrivers(mountpoints);
    ASSERT_TRUE(drivers);
    auto& driver = *drivers->front();
    ASSERT_TRUE(driver.Init());

    const ObjectKey multi_key = "complete-multi-shard";
    std::string first(32, 'a');
    std::string second = "b";
    const std::array<KvcsShardPutRequest, 2> multi_puts{{
        {.logical_key = multi_key,
         .total_shard = 2,
         .shard_id = 0,
         .slices = {{first.data(), first.size()}},
         .size = first.size()},
        {.logical_key = multi_key,
         .total_shard = 2,
         .shard_id = 1,
         .slices = {{second.data(), second.size()}},
         .size = second.size()},
    }};
    const auto initial_multi = driver.BatchPut(multi_puts);
    ASSERT_TRUE(initial_multi[0]);
    ASSERT_TRUE(initial_multi[1]);
    const auto duplicate_multi = driver.BatchPut(multi_puts);
    ASSERT_FALSE(duplicate_multi[0]);
    ASSERT_FALSE(duplicate_multi[1]);
    EXPECT_EQ(duplicate_multi[0].error(),
              ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(duplicate_multi[1].error(),
              ErrorCode::OBJECT_ALREADY_EXISTS);

    const ObjectKey single_key = "complete-single-shard";
    std::string single = "single";
    const std::array<KvcsShardPutRequest, 1> single_put{{
        {.logical_key = single_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{single.data(), single.size()}},
         .size = single.size()},
    }};
    ASSERT_TRUE(driver.BatchPut(single_put)[0]);
    const auto duplicate_single = driver.BatchPut(single_put);
    ASSERT_FALSE(duplicate_single[0]);
    EXPECT_EQ(duplicate_single[0].error(),
              ErrorCode::OBJECT_ALREADY_EXISTS);

    const std::array<ObjectKey, 2> keys{multi_key, single_key};
    const auto deletes = driver.BatchDelete(keys);
    ASSERT_EQ(deletes.size(), keys.size());
    EXPECT_TRUE(deletes[0]);
    EXPECT_TRUE(deletes[1]);
}

#endif

class InsertOnlyKvcsDriver final : public KvcsDriver {
   public:
    explicit InsertOnlyKvcsDriver(bool existence_only_query = false)
        : existence_only_query_(existence_only_query) {}

    tl::expected<void, ErrorCode> Init() override { return {}; }

    uint64_t MaxValueSize() const override { return 3; }
    uint32_t MaxKeySize() const override {
        return kKvcsDefaultMaxKeySize;
    }

    KvcsPutShardResults BatchPut(
        std::span<const KvcsShardPutRequest> requests) override {
        if (put_delay_.count() != 0) std::this_thread::sleep_for(put_delay_);
        ++put_calls_;
        max_put_batch_items_ = std::max(max_put_batch_items_, requests.size());
        if (fail_put_attempts_ != 0) {
            --fail_put_attempts_;
            KvcsPutShardResults failures;
            failures.reserve(requests.size());
            for (size_t i = 0; i < requests.size(); ++i) {
                failures.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_UNAVAILABLE));
            }
            return failures;
        }
        std::set<ObjectKey> existing_keys;
        for (const auto& request : requests) {
            if (values_.contains(request.logical_key)) {
                existing_keys.insert(request.logical_key);
            }
        }

        KvcsPutShardResults results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            if (existing_keys.contains(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                continue;
            }
            std::string value;
            for (const auto& slice : request.slices) {
                value.append(static_cast<const char*>(slice.ptr), slice.size);
            }
            values_[request.logical_key][request.shard_id] = std::move(value);
            totals_[request.logical_key] = request.total_shard;
            results.emplace_back();
        }
        return results;
    }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> logical_keys) override {
        ++query_calls_;
        if (fail_query_attempts_ != 0) {
            --fail_query_attempts_;
            KvcsDriverQueryResults failures;
            failures.reserve(logical_keys.size());
            for (size_t i = 0; i < logical_keys.size(); ++i) {
                failures.emplace_back(tl::make_unexpected(query_failure_));
            }
            return failures;
        }
        KvcsDriverQueryResults results;
        results.reserve(logical_keys.size());
        for (const auto& key : logical_keys) {
            const auto value_it = values_.find(key);
            if (value_it == values_.end()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            if (existence_only_query_) {
                results.emplace_back(KvcsManifest{
                    .logical_key = corrupt_query_key_ ? key + ".wrong" : key,
                    .total_shard = 0,
                    .total_size = 0,
                    .shards = {},
                    .layout_known = false,
                });
                continue;
            }
            KvcsManifest manifest{
                .logical_key = corrupt_query_key_ ? key + ".wrong" : key,
                .total_shard = totals_.at(key),
                .total_size = 0,
                .shards = {}};
            for (const auto& [shard_id, value] : value_it->second) {
                manifest.total_size += value.size();
                manifest.shards.push_back({
                    .shard_id = shard_id,
                    .size = value.size(),
                });
            }
            results.emplace_back(std::move(manifest));
        }
        return results;
    }

    KvcsGetShardResults BatchGet(
        std::span<const KvcsShardGetRequest> requests) override {
        if (get_delay_.count() != 0) std::this_thread::sleep_for(get_delay_);
        ++get_calls_;
        get_items_ += requests.size();
        max_get_batch_items_ = std::max(max_get_batch_items_, requests.size());
        if (fail_get_attempts_ != 0) {
            --fail_get_attempts_;
            KvcsGetShardResults failures;
            failures.reserve(requests.size());
            for (size_t i = 0; i < requests.size(); ++i) {
                failures.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_UNAVAILABLE));
            }
            return failures;
        }
        KvcsGetShardResults results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            const auto object_it = values_.find(request.logical_key);
            if (object_it == values_.end() ||
                !object_it->second.contains(request.shard_id)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }

            const auto& value = object_it->second.at(request.shard_id);
            size_t offset = 0;
            for (const auto& slice : request.slices) {
                std::memcpy(slice.ptr, value.data() + offset, slice.size);
                offset += slice.size;
            }
            if (offset != value.size()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            } else {
                results.emplace_back();
            }
        }
        return results;
    }

    KvcsDeleteResults BatchDelete(
        std::span<const ObjectKey> logical_keys) override {
        ++delete_calls_;
        KvcsDeleteResults results;
        results.reserve(logical_keys.size());
        if (fail_delete_attempts_ != 0) {
            --fail_delete_attempts_;
            for (size_t i = 0; i < logical_keys.size(); ++i) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_UNAVAILABLE));
            }
            return results;
        }
        for (const auto& key : logical_keys) {
            const auto value = values_.find(key);
            if (delete_offload_target_ != nullptr && value != values_.end()) {
                delete_offload_target_->values_[key] = value->second;
                delete_offload_target_->totals_[key] = totals_.at(key);
            }
            values_.erase(key);
            totals_.erase(key);
            results.emplace_back();
        }
        return results;
    }

    size_t delete_calls() const { return delete_calls_; }
    size_t put_calls() const { return put_calls_; }
    size_t query_calls() const { return query_calls_; }
    size_t get_calls() const { return get_calls_; }
    size_t get_items() const { return get_items_; }
    size_t stored_objects() const { return values_.size(); }
    std::set<ObjectKey> stored_keys() const {
        std::set<ObjectKey> keys;
        for (const auto& item : values_) keys.insert(item.first);
        return keys;
    }
    size_t max_put_batch_items() const { return max_put_batch_items_; }
    size_t max_get_batch_items() const { return max_get_batch_items_; }
    void FailNextPut() { fail_put_attempts_ = 1; }
    void FailPutAttempts(size_t attempts) { fail_put_attempts_ = attempts; }
    void FailQueryAttempts(size_t attempts,
                           ErrorCode error = ErrorCode::KVCS_UNAVAILABLE) {
        fail_query_attempts_ = attempts;
        query_failure_ = error;
    }
    void CorruptQueryKey(bool enabled = true) { corrupt_query_key_ = enabled; }
    void FailNextGet() { fail_get_attempts_ = 1; }
    void FailGetAttempts(size_t attempts) { fail_get_attempts_ = attempts; }
    void FailDeleteAttempts(size_t attempts) {
        fail_delete_attempts_ = attempts;
    }
    void SetPutDelay(std::chrono::milliseconds delay) { put_delay_ = delay; }
    void SetGetDelay(std::chrono::milliseconds delay) { get_delay_ = delay; }
    void SetDeleteOffloadTarget(InsertOnlyKvcsDriver* target) {
        delete_offload_target_ = target;
    }
    void MoveAllTo(InsertOnlyKvcsDriver& destination) {
        for (auto& [key, shards] : values_) {
            destination.values_[key] = std::move(shards);
            destination.totals_[key] = totals_.at(key);
        }
        values_.clear();
        totals_.clear();
    }

   private:
    std::map<ObjectKey, std::map<uint32_t, std::string>> values_;
    std::map<ObjectKey, uint32_t> totals_;
    size_t delete_calls_ = 0;
    size_t put_calls_ = 0;
    size_t query_calls_ = 0;
    size_t get_calls_ = 0;
    size_t get_items_ = 0;
    size_t max_put_batch_items_ = 0;
    size_t max_get_batch_items_ = 0;
    size_t fail_put_attempts_ = 0;
    size_t fail_query_attempts_ = 0;
    size_t fail_get_attempts_ = 0;
    size_t fail_delete_attempts_ = 0;
    ErrorCode query_failure_ = ErrorCode::KVCS_UNAVAILABLE;
    bool existence_only_query_ = false;
    bool corrupt_query_key_ = false;
    std::chrono::milliseconds put_delay_{0};
    std::chrono::milliseconds get_delay_{0};
    InsertOnlyKvcsDriver* delete_offload_target_ = nullptr;
};

class ConcurrencyTrackingKvcsDriver final : public KvcsDriver {
   public:
    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 1024; }
    KvcsPutShardResults BatchPut(
        std::span<const KvcsShardPutRequest> requests) override {
        const size_t active = active_.fetch_add(1) + 1;
        size_t observed = max_active_.load();
        while (active > observed &&
               !max_active_.compare_exchange_weak(observed, active)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        active_.fetch_sub(1);
        return KvcsPutShardResults(requests.size());
    }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> logical_keys) override {
        KvcsDriverQueryResults results;
        results.reserve(logical_keys.size());
        for (size_t i = 0; i < logical_keys.size(); ++i) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
        }
        return results;
    }

    KvcsGetShardResults BatchGet(
        std::span<const KvcsShardGetRequest> requests) override {
        KvcsGetShardResults results;
        results.reserve(requests.size());
        for (size_t i = 0; i < requests.size(); ++i) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
        }
        return results;
    }

    KvcsDeleteResults BatchDelete(
        std::span<const ObjectKey> logical_keys) override {
        return KvcsDeleteResults(logical_keys.size());
    }

    size_t max_active() const { return max_active_.load(); }

   private:
    std::atomic<size_t> active_{0};
    std::atomic<size_t> max_active_{0};
};

TEST(KvcsObjectStorageAdapterTest, BoundsSingleTargetIoConcurrency) {
    FileStorageConfig config;
    auto driver = std::make_unique<ConcurrencyTrackingKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::array<std::thread, 8> callers;
    std::array<bool, 8> succeeded{};
    std::array<std::string, 8> keys;
    std::array<std::string, 8> values;
    for (size_t i = 0; i < callers.size(); ++i) {
        keys[i] = "concurrent-key-" + std::to_string(i);
        values[i] = "value-" + std::to_string(i);
        callers[i] = std::thread([&, i] {
            ready.fetch_add(1);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const ObjectStoragePutRequest request{
                .logical_key = keys[i],
                .slices = {{values[i].data(), values[i].size()}},
            };
            const auto results = adapter.BatchPutV({&request, 1});
            succeeded[i] = results.size() == 1 && results[0].has_value();
        });
    }
    while (ready.load() != callers.size()) std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto& caller : callers) caller.join();

    EXPECT_TRUE(std::all_of(succeeded.begin(), succeeded.end(),
                            [](bool value) { return value; }));
    EXPECT_EQ(driver_ptr->max_active(), 2u);
}

TEST(KvcsObjectStorageAdapterTest,
     RebuildsLowLevelLayoutFromExpectedSizeAfterExistenceQuery) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>(true);
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    std::string value = "abc";
    ObjectStoragePutRequest put{
        .logical_key = "inline-key",
        .slices = {{value.data(), value.size()}},
    };
    auto put_results = adapter.BatchPutV({&put, 1});
    ASSERT_EQ(put_results.size(), 1u);
    ASSERT_TRUE(put_results[0]);
    EXPECT_EQ(driver_ptr->query_calls(), 0u);

    const std::array<ObjectKey, 1> keys{"inline-key"};
    auto query_results = adapter.BatchQueryProvider(keys);
    ASSERT_EQ(query_results.size(), 1u);
    ASSERT_TRUE(query_results[0]);
    EXPECT_FALSE(query_results[0]->layout_known);

    std::string output(value.size(), '\0');
    ObjectStorageGetRequest get{
        .logical_key = "inline-key",
        .slices = {{output.data(), output.size()}},
        .expected_size = value.size(),
    };
    auto get_results = adapter.BatchGetInto({&get, 1});
    ASSERT_EQ(get_results.size(), 1u);
    ASSERT_TRUE(get_results[0]);
    EXPECT_EQ(output, value);
}

TEST(KvcsObjectStorageAdapterTest, RejectsMismatchedDriverQueryKey) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    std::string value = "abc";
    const ObjectStoragePutRequest put{
        .logical_key = "key",
        .slices = {{value.data(), value.size()}},
    };
    const auto put_results = adapter.BatchPutV({&put, 1});
    ASSERT_EQ(put_results.size(), 1u);
    ASSERT_TRUE(put_results[0]);

    driver_ptr->CorruptQueryKey();
    const std::array<ObjectKey, 1> keys{"key"};
    const auto query_results = adapter.BatchQueryProviderUntil(
        keys, std::chrono::steady_clock::now() + std::chrono::seconds(1));
    ASSERT_EQ(query_results.size(), 1u);
    ASSERT_FALSE(query_results[0]);
    EXPECT_EQ(query_results[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, ReplacesOnlyExplicitUpserts) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    EXPECT_STREQ(adapter.GetName(), kKvcsLowLevelAdapterName);
    ASSERT_TRUE(adapter.Init());

    std::string original = "original";
    ObjectStoragePutRequest initial{
        .logical_key = "key",
        .slices = {{original.data(), original.size()}},
    };
    auto initial_result = adapter.BatchPutV({&initial, 1});
    ASSERT_EQ(initial_result.size(), 1u);
    ASSERT_TRUE(initial_result[0]);

    std::string replacement = "replaced-value";
    ObjectStoragePutRequest ordinary_put{
        .logical_key = "key",
        .slices = {{replacement.data(), replacement.size()}},
    };
    auto ordinary_result = adapter.BatchPutV({&ordinary_put, 1});
    ASSERT_EQ(ordinary_result.size(), 1u);
    ASSERT_FALSE(ordinary_result[0]);
    EXPECT_EQ(ordinary_result[0].error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(driver_ptr->delete_calls(), 0u);

    ordinary_put.replace_existing = true;
    auto upsert_result = adapter.BatchPutV({&ordinary_put, 1});
    ASSERT_EQ(upsert_result.size(), 1u);
    ASSERT_TRUE(upsert_result[0]);
    EXPECT_EQ(driver_ptr->delete_calls(), 1u);

    std::string first(replacement.size() / 2, '\0');
    std::string second(replacement.size() - first.size() + 8, '\0');
    ObjectStorageGetRequest get{
        .logical_key = "key",
        .slices = {{first.data(), first.size()},
                   {second.data(), second.size()}},
        .expected_size = replacement.size(),
    };
    auto get_result = adapter.BatchGetInto({&get, 1});
    ASSERT_EQ(get_result.size(), 1u);
    ASSERT_TRUE(get_result[0]);
    EXPECT_EQ(first + second.substr(0, replacement.size() - first.size()),
              replacement);
}

TEST(KvcsObjectStorageAdapterTest, DeleteIsIdempotentAndListingIsDisabled) {
    FileStorageConfig config;
    KvcsObjectStorageAdapter adapter(config,
                                     std::make_unique<InsertOnlyKvcsDriver>());
    EXPECT_STREQ(adapter.GetName(), kKvcsLowLevelAdapterName);
    ASSERT_TRUE(adapter.Init());

    const std::vector<std::string> keys{"missing"};
    auto results = adapter.BatchDelete(keys);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(adapter.ListKeys().error(), ErrorCode::NOT_SUPPORTED);
}

TEST(KvcsObjectStorageAdapterTest, EnforcesTenantQualifiedKeyLimit) {
    FileStorageConfig config;
    KvcsObjectStorageAdapter adapter(config,
                                     std::make_unique<InsertOnlyKvcsDriver>());
    ASSERT_TRUE(adapter.Init());

    std::string value = "value";
    const std::string too_long_key(119, 'k');
    const ObjectStoragePutRequest request{
        .logical_key = too_long_key,
        .slices = {{value.data(), value.size()}},
    };
    const auto results = adapter.BatchPutV({&request, 1});
    ASSERT_EQ(results.size(), 1u);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, RejectsInvalidBatchBeforeDriverWrite) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    std::string value = "value";
    const std::array<ObjectStoragePutRequest, 2> requests{{
        {.logical_key = "valid", .slices = {{value.data(), value.size()}}},
        {.logical_key = "", .slices = {{value.data(), value.size()}}},
    }};
    const auto results = adapter.BatchPutV(requests);
    ASSERT_EQ(results.size(), requests.size());
    EXPECT_TRUE(std::all_of(results.begin(), results.end(), [](const auto& r) {
        return !r && r.error() == ErrorCode::INVALID_PARAMS;
    }));
    EXPECT_EQ(driver_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, TransientWriteErrorsDoNotDisableDriver) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    driver_ptr->FailPutAttempts(3);
    std::string value = "value";
    for (size_t i = 0; i < 3; ++i) {
        const ObjectStoragePutRequest request{
            .logical_key = "failed-" + std::to_string(i),
            .slices = {{value.data(), value.size()}},
        };
        const auto result = adapter.BatchPutV({&request, 1});
        ASSERT_FALSE(result[0]);
        EXPECT_EQ(result[0].error(), ErrorCode::KVCS_UNAVAILABLE);
    }

    const ObjectStoragePutRequest recovered{
        .logical_key = "recovered",
        .slices = {{value.data(), value.size()}},
    };
    const auto result = adapter.BatchPutV({&recovered, 1});
    ASSERT_TRUE(result[0]);
    EXPECT_EQ(driver_ptr->put_calls(), 4u);
}

TEST(KvcsObjectStorageAdapterTest, SpreadsSingleReplicaBatchAcrossTargets) {
    FileStorageConfig config;
    auto first = std::make_unique<InsertOnlyKvcsDriver>();
    auto second = std::make_unique<InsertOnlyKvcsDriver>();
    auto third = std::make_unique<InsertOnlyKvcsDriver>();
    auto* first_ptr = first.get();
    auto* second_ptr = second.get();
    auto* third_ptr = third.get();

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back(
        {.id = "a", .mountpoint_index = 1, .driver = std::move(first)});
    targets.push_back(
        {.id = "b", .mountpoint_index = 2, .driver = std::move(second)});
    targets.push_back(
        {.id = "c", .mountpoint_index = 3, .driver = std::move(third)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    std::string value = "abc";
    std::vector<ObjectStoragePutRequest> puts;
    puts.reserve(12);
    for (size_t i = 0; i < 12; ++i) {
        puts.push_back({.logical_key = "balanced-key-" + std::to_string(i),
                        .slices = {{value.data(), value.size()}}});
    }

    const auto results = adapter.BatchPutV(puts);
    ASSERT_EQ(results.size(), puts.size());
    EXPECT_TRUE(
        std::all_of(results.begin(), results.end(),
                    [](const auto& result) { return result.has_value(); }));
    EXPECT_EQ(first_ptr->stored_objects(), 4u);
    EXPECT_EQ(second_ptr->stored_objects(), 4u);
    EXPECT_EQ(third_ptr->stored_objects(), 4u);
    EXPECT_EQ(first_ptr->put_calls(), 1u);
    EXPECT_EQ(second_ptr->put_calls(), 1u);
    EXPECT_EQ(third_ptr->put_calls(), 1u);
    EXPECT_EQ(first_ptr->max_put_batch_items(), 4u);
    EXPECT_EQ(second_ptr->max_put_batch_items(), 4u);
    EXPECT_EQ(third_ptr->max_put_batch_items(), 4u);
}

TEST(KvcsObjectStorageAdapterTest, StableHashPreservesPerTargetBatches) {
    FileStorageConfig config;
    auto local = std::make_unique<InsertOnlyKvcsDriver>();
    auto remote = std::make_unique<InsertOnlyKvcsDriver>();
    auto* local_ptr = local.get();
    auto* remote_ptr = remote.get();

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back({.id = "local",
                       .mountpoint_index = 0,
                       .route_kind = KvcsEfcRouteKind::kLocal,
                       .driver = std::move(local)});
    targets.push_back({.id = "remote-a",
                       .mountpoint_index = 1,
                       .route_kind = KvcsEfcRouteKind::kKvCacheStore,
                       .driver = std::move(remote)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    std::string value = "abc";
    std::vector<ObjectStoragePutRequest> puts;
    for (size_t i = 0; i < 64; ++i) {
        puts.push_back({.logical_key = "hybrid-key-" + std::to_string(i),
                        .slices = {{value.data(), value.size()}}});
    }
    const auto results = adapter.BatchPutV(puts);
    ASSERT_TRUE(
        std::all_of(results.begin(), results.end(),
                    [](const auto& result) { return result.has_value(); }));
    EXPECT_GT(local_ptr->stored_objects(), 0u);
    EXPECT_GT(remote_ptr->stored_objects(), 0u);
    EXPECT_EQ(local_ptr->stored_objects() + remote_ptr->stored_objects(),
              puts.size());
    EXPECT_EQ(local_ptr->put_calls(), 1u);
    EXPECT_EQ(remote_ptr->put_calls(), 1u);

    auto second_local = std::make_unique<InsertOnlyKvcsDriver>();
    auto second_remote = std::make_unique<InsertOnlyKvcsDriver>();
    auto* second_local_ptr = second_local.get();
    auto* second_remote_ptr = second_remote.get();
    std::vector<KvcsLowLevelTargetSpec> second_targets;
    second_targets.push_back({.id = "local",
                              .mountpoint_index = 0,
                              .route_kind = KvcsEfcRouteKind::kLocal,
                              .driver = std::move(second_local)});
    second_targets.push_back({.id = "remote-a",
                              .mountpoint_index = 1,
                              .route_kind = KvcsEfcRouteKind::kKvCacheStore,
                              .driver = std::move(second_remote)});
    KvcsObjectStorageAdapter second_adapter(config, std::move(second_targets));
    ASSERT_TRUE(second_adapter.Init());
    const auto second_results = second_adapter.BatchPutV(puts);
    ASSERT_TRUE(
        std::all_of(second_results.begin(), second_results.end(),
                    [](const auto& result) { return result.has_value(); }));
    EXPECT_EQ(second_local_ptr->stored_keys(), local_ptr->stored_keys());
    EXPECT_EQ(second_remote_ptr->stored_keys(), remote_ptr->stored_keys());

    const auto local_keys = local_ptr->stored_keys();
    ASSERT_FALSE(local_keys.empty());
    std::string local_logical_key;
    for (const auto& request : puts) {
        if (local_keys.contains(
                EncodeKvcsKey(config.kvcs_tenant_id, request.logical_key))) {
            local_logical_key = request.logical_key;
            break;
        }
    }
    ASSERT_FALSE(local_logical_key.empty());
    const auto encoded_local_key =
        EncodeKvcsKey(config.kvcs_tenant_id, local_logical_key);
    local_ptr->SetDeleteOffloadTarget(remote_ptr);
    ASSERT_TRUE(adapter.Delete(local_logical_key));
    EXPECT_FALSE(local_ptr->stored_keys().contains(encoded_local_key));
    EXPECT_FALSE(remote_ptr->stored_keys().contains(encoded_local_key));
}

TEST(KvcsObjectStorageAdapterTest,
     ColdQueryUsesHashedPrimaryAndFallsBackOnMiss) {
    FileStorageConfig config;
    auto local = std::make_unique<InsertOnlyKvcsDriver>();
    auto remote = std::make_unique<InsertOnlyKvcsDriver>();
    auto* local_ptr = local.get();
    auto* remote_ptr = remote.get();

    std::string value = "abc";
    const ObjectKey logical_key = "cold-query-key";
    const ObjectKey encoded_key =
        EncodeKvcsKey(config.kvcs_tenant_id, logical_key);
    const std::array<KvcsShardPutRequest, 1> seed{{
        {.logical_key = encoded_key,
         .total_shard = 1,
         .shard_id = 0,
         .slices = {{value.data(), value.size()}},
         .size = value.size()},
    }};
    ASSERT_TRUE(local->BatchPut(seed)[0]);
    ASSERT_TRUE(remote->BatchPut(seed)[0]);

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back({.id = "local",
                       .mountpoint_index = 0,
                       .route_kind = KvcsEfcRouteKind::kLocal,
                       .driver = std::move(local)});
    targets.push_back({.id = "remote",
                       .mountpoint_index = 1,
                       .route_kind = KvcsEfcRouteKind::kKvCacheStore,
                       .driver = std::move(remote)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    const std::array<ObjectKey, 1> hit_keys{logical_key};
    const auto hit = adapter.BatchQueryProvider(hit_keys);
    ASSERT_EQ(hit.size(), 1u);
    ASSERT_TRUE(hit[0]);
    EXPECT_EQ(local_ptr->query_calls() + remote_ptr->query_calls(), 1u);

    const std::array<ObjectKey, 1> missing_keys{"cold-query-missing"};
    const auto miss = adapter.BatchQueryProvider(missing_keys);
    ASSERT_EQ(miss.size(), 1u);
    ASSERT_FALSE(miss[0]);
    EXPECT_EQ(miss[0].error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(local_ptr->query_calls() + remote_ptr->query_calls(), 3u);
}

TEST(KvcsObjectStorageAdapterTest, PreservesProviderErrorAcrossMissFallback) {
    FileStorageConfig config;
    auto unavailable = std::make_unique<InsertOnlyKvcsDriver>();
    auto missing = std::make_unique<InsertOnlyKvcsDriver>();
    unavailable->FailQueryAttempts(1000);
    unavailable->FailGetAttempts(1000);

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back({.id = "unavailable",
                       .mountpoint_index = 1,
                       .driver = std::move(unavailable)});
    targets.push_back(
        {.id = "missing", .mountpoint_index = 2, .driver = std::move(missing)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    std::vector<ObjectKey> keys;
    for (size_t i = 0; i < 12; ++i) {
        keys.push_back("provider-error-key-" + std::to_string(i));
    }
    const auto queries = adapter.BatchQueryProvider(keys);
    ASSERT_EQ(queries.size(), keys.size());
    EXPECT_TRUE(
        std::all_of(queries.begin(), queries.end(), [](const auto& item) {
            return !item && item.error() == ErrorCode::KVCS_UNAVAILABLE;
        }));

    std::vector<std::string> outputs(keys.size(), std::string(3, '\0'));
    std::vector<ObjectStorageGetRequest> gets;
    ObjectStorageQueryResults contexts;
    gets.reserve(keys.size());
    contexts.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        gets.push_back({.logical_key = keys[i],
                        .slices = {{outputs[i].data(), outputs[i].size()}},
                        .expected_size = outputs[i].size()});
        contexts.emplace_back(ObjectStorageQueryContext{
            .logical_key = keys[i],
            .total_shard = 1,
            .total_size = outputs[i].size(),
            .shards = {{.shard_id = 0, .size = outputs[i].size()}},
        });
    }
    const auto reads = adapter.BatchGetIntoWithQueryContexts(gets, contexts);
    ASSERT_EQ(reads.size(), gets.size());
    EXPECT_TRUE(std::all_of(reads.begin(), reads.end(), [](const auto& item) {
        return !item && item.error() == ErrorCode::KVCS_UNAVAILABLE;
    }));
}

TEST(KvcsObjectStorageAdapterTest,
     FallsBackFromIncompleteQueryToCompleteTarget) {
    FileStorageConfig config;
    auto incomplete = std::make_unique<InsertOnlyKvcsDriver>();
    auto complete = std::make_unique<InsertOnlyKvcsDriver>();
    auto* incomplete_ptr = incomplete.get();
    auto* complete_ptr = complete.get();
    incomplete->FailQueryAttempts(1000, ErrorCode::KVCS_INCOMPLETE);

    std::string value = "abc";
    std::vector<ObjectKey> keys;
    std::vector<KvcsShardPutRequest> seed;
    for (size_t i = 0; i < 12; ++i) {
        keys.push_back("incomplete-fallback-key-" + std::to_string(i));
        seed.push_back({
            .logical_key = EncodeKvcsKey(config.kvcs_tenant_id, keys.back()),
            .total_shard = 1,
            .shard_id = 0,
            .slices = {{value.data(), value.size()}},
            .size = value.size(),
        });
    }
    const auto seeded = complete->BatchPut(seed);
    ASSERT_TRUE(std::all_of(seeded.begin(), seeded.end(),
                            [](const auto& item) { return item.has_value(); }));

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back({.id = "incomplete",
                       .mountpoint_index = 1,
                       .driver = std::move(incomplete)});
    targets.push_back({.id = "complete",
                       .mountpoint_index = 2,
                       .driver = std::move(complete)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    const auto queries = adapter.BatchQueryProvider(keys);
    ASSERT_EQ(queries.size(), keys.size());
    EXPECT_TRUE(std::all_of(queries.begin(), queries.end(),
                            [](const auto& item) { return item.has_value(); }));
    EXPECT_GT(incomplete_ptr->query_calls(), 0u);
    EXPECT_GT(complete_ptr->query_calls(), 0u);
}

// Placement intentionally follows the stable hash of the tenant-qualified key.
// It does not use capacity, queue depth, or measured bandwidth.

TEST(KvcsObjectStorageAdapterTest, BatchesAcrossTargetsAndReadsByTarget) {
    FileStorageConfig config;
    auto first = std::make_unique<InsertOnlyKvcsDriver>();
    auto second = std::make_unique<InsertOnlyKvcsDriver>();
    auto third = std::make_unique<InsertOnlyKvcsDriver>();

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back(
        {.id = "a", .mountpoint_index = 1, .driver = std::move(first)});
    targets.push_back(
        {.id = "b", .mountpoint_index = 2, .driver = std::move(second)});
    targets.push_back(
        {.id = "c", .mountpoint_index = 3, .driver = std::move(third)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    std::string first_value = "first-value";
    std::string second_value = "second-value";
    const std::array<ObjectStoragePutRequest, 2> puts{{
        {.logical_key = "key-one",
         .slices = {{first_value.data(), first_value.size()}}},
        {.logical_key = "key-two",
         .slices = {{second_value.data(), second_value.size()}}},
    }};
    const auto put_results = adapter.BatchPutV(puts);
    ASSERT_EQ(put_results.size(), puts.size());
    EXPECT_TRUE(put_results[0]);
    EXPECT_TRUE(put_results[1]);

    const std::array<ObjectKey, 2> keys{"key-one", "key-two"};
    auto manifests = adapter.BatchQueryProvider(keys);
    ASSERT_EQ(manifests.size(), keys.size());
    ASSERT_TRUE(manifests[0]);
    ASSERT_TRUE(manifests[1]);

    std::string first_output(first_value.size(), '\0');
    std::string second_output(second_value.size(), '\0');
    const std::array<ObjectStorageGetRequest, 2> gets{{
        {.logical_key = "key-one",
         .slices = {{first_output.data(), first_output.size()}},
         .expected_size = first_output.size()},
        {.logical_key = "key-two",
         .slices = {{second_output.data(), second_output.size()}},
         .expected_size = second_output.size()},
    }};
    const auto get_results =
        adapter.BatchGetIntoWithQueryContexts(gets, manifests);
    ASSERT_EQ(get_results.size(), gets.size());
    EXPECT_TRUE(get_results[0]);
    EXPECT_TRUE(get_results[1]);
    EXPECT_EQ(first_output, first_value);
    EXPECT_EQ(second_output, second_value);
}

TEST(KvcsObjectStorageAdapterTest,
     FallsBackToNextTargetForTransientPutFailure) {
    FileStorageConfig config;
    auto first = std::make_unique<InsertOnlyKvcsDriver>();
    auto second = std::make_unique<InsertOnlyKvcsDriver>();
    auto* first_ptr = first.get();
    auto* second_ptr = second.get();
    first_ptr->FailPutAttempts(1000);

    std::vector<KvcsLowLevelTargetSpec> targets;
    targets.push_back(
        {.id = "a", .mountpoint_index = 1, .driver = std::move(first)});
    targets.push_back(
        {.id = "b", .mountpoint_index = 2, .driver = std::move(second)});
    KvcsObjectStorageAdapter adapter(config, std::move(targets));
    ASSERT_TRUE(adapter.Init());

    std::string value = "value";
    std::vector<ObjectStoragePutRequest> puts;
    for (size_t i = 0; i < 12; ++i) {
        puts.push_back({.logical_key = "fallback-key-" + std::to_string(i),
                        .slices = {{value.data(), value.size()}}});
    }
    const auto results = adapter.BatchPutV(puts);
    ASSERT_EQ(results.size(), puts.size());
    EXPECT_TRUE(
        std::all_of(results.begin(), results.end(),
                    [](const auto& result) { return result.has_value(); }));
    EXPECT_EQ(first_ptr->stored_objects(), 0u);
    EXPECT_EQ(second_ptr->stored_objects(), puts.size());
    EXPECT_GT(first_ptr->put_calls(), 0u);
    EXPECT_GT(second_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, UsesQueryForHealthCheck) {
    FileStorageConfig config;
    auto driver = std::make_unique<InsertOnlyKvcsDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config,
                                     std::move(driver));
    ASSERT_TRUE(adapter.Init());

    EXPECT_TRUE(adapter.CheckHealth());
    driver_ptr->FailQueryAttempts(1);
    const auto failed = adapter.CheckHealth();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::KVCS_UNAVAILABLE);
    EXPECT_EQ(driver_ptr->query_calls(), 2u);
}

}  // namespace
}  // namespace mooncake
