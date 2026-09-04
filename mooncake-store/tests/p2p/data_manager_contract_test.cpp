// Contract tests for the DataManager interface.
//
// Every case here is written against `p2p/client/data_manager.h` and the
// factory only: no concrete implementation type, no private member access.
// The suite is parameterized over DataManagerVersion so the same assertions
// run against V1 and (from Phase 2) V2. It is the executable form of the
// contract table in remake_kvbm/new_data_manager.md section 4.2, and the
// baseline the differential replay of section 9.2 compares against.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include "client_buffer.hpp"
#include "p2p/client/data_manager.h"
#include "p2p/client/data_manager_factory.h"
#include "p2p/client/data_manager_test_hook.h"
#include "p2p/client/data_manager_types.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace {

// syncAwait() only accepts a Lazy, so wrap the Future in a trivial one.
tl::expected<void, ErrorCode> AwaitVoidFuture(
    async_simple::Future<tl::expected<void, ErrorCode>> future) {
    return async_simple::coro::syncAwait(
        [](async_simple::Future<tl::expected<void, ErrorCode>> f)
            -> async_simple::coro::Lazy<tl::expected<void, ErrorCode>> {
            co_return co_await std::move(f);
        }(std::move(future)));
}

Json::Value ParseJson(const std::string& text) {
    Json::Value value;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    EXPECT_TRUE(
        reader->parse(text.data(), text.data() + text.size(), &value, &errs))
        << errs;
    return value;
}

constexpr const char* kDramOnlyTiers = R"({
    "tiers": [
        {
            "type": "DRAM",
            "capacity": 268435456,
            "priority": 10,
            "tags": ["fast", "local"],
            "allocator_type": "OFFSET"
        }
    ]
})";

// A small DRAM tier next to a large storage tier. Used to show that the local
// write path never spills onto the slow tier when DRAM is full.
constexpr const char* kSmallDramPlusStorageTiers = R"({
    "tiers": [
        {
            "type": "DRAM",
            "capacity": 8388608,
            "priority": 100,
            "tags": ["dram"],
            "allocator_type": "OFFSET"
        },
        {
            "type": "STORAGE",
            "capacity": 1073741824,
            "priority": 10,
            "tags": ["ssd"]
        }
    ]
})";

// A fast tier small enough that reclaim is the only way a further write can
// succeed, next to a slow tier with room to spare.
constexpr const char* kTinyDramPlusStorageTiers = R"({
    "tiers": [
        {
            "type": "DRAM",
            "capacity": 4194304,
            "priority": 100,
            "tags": ["dram"],
            "allocator_type": "OFFSET"
        },
        {
            "type": "STORAGE",
            "capacity": 268435456,
            "priority": 10,
            "tags": ["ssd"]
        }
    ],
    "v2": {"allocation_failure": {"try_evict": true, "max_evict_rounds": 2,
                                  "evict_timeout_ms": 500}}
})";

// The same topology with reclaim switched off, so exhaustion stays exhaustion.
constexpr const char* kTinyDramNoEvictTiers = R"({
    "tiers": [
        {
            "type": "DRAM",
            "capacity": 4194304,
            "priority": 100,
            "tags": ["dram"],
            "allocator_type": "OFFSET"
        },
        {
            "type": "STORAGE",
            "capacity": 268435456,
            "priority": 10,
            "tags": ["ssd"]
        }
    ],
    "v2": {"allocation_failure": {"try_evict": false}}
})";

/**
 * @brief Records the metadata callbacks a DataManager fires, so a test can
 *        assert on the logical sequence without caring about threading.
 */
struct CallbackRecorder {
    struct AddEvent {
        std::string key;
        UUID tier_id;
        size_t size;
    };
    struct RemoveEvent {
        std::string key;
        UUID tier_id;
    };

    mutable std::mutex mu;
    std::vector<AddEvent> adds;
    std::vector<RemoveEvent> removes;
    std::vector<std::pair<std::string, std::optional<UUID>>> rectifies;

    size_t AddCount() const {
        std::lock_guard<std::mutex> lock(mu);
        return adds.size();
    }
    size_t RemoveCount() const {
        std::lock_guard<std::mutex> lock(mu);
        return removes.size();
    }
    size_t RectifyCount() const {
        std::lock_guard<std::mutex> lock(mu);
        return rectifies.size();
    }
    bool SawAdd(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mu);
        for (const auto& e : adds) {
            if (e.key == key) return true;
        }
        return false;
    }
    bool SawRemove(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mu);
        for (const auto& e : removes) {
            if (e.key == key) return true;
        }
        return false;
    }
};

/**
 * @brief One DataManager plus everything needed to keep it alive.
 */
struct ManagerEnv {
    std::shared_ptr<TransferEngine> transfer_engine;
    std::shared_ptr<CallbackRecorder> recorder;
    std::unique_ptr<DataManager> manager;

    DataManager& operator*() const { return *manager; }
    DataManager* operator->() const { return manager.get(); }

    ~ManagerEnv() {
        if (manager) {
            manager->Stop();
            manager->Destroy();
        }
    }
};

std::unique_ptr<ManagerEnv> MakeManager(DataManagerVersion version,
                                        const char* tiers_json,
                                        LocalTransferMode mode,
                                        uint32_t lease_duration_ms = 0) {
    auto env = std::make_unique<ManagerEnv>();
    env->transfer_engine = std::make_shared<TransferEngine>(false);
    env->recorder = std::make_shared<CallbackRecorder>();

    DataManagerConfig config;
    config.version = version;
    config.tier_config = ParseJson(tiers_json);
    config.v1_lock_shard_count = 64;
    config.local_transfer.mode = mode;
    // Deterministic: run the copy on the Wait() caller thread.
    config.local_transfer.local_memcpy_async_worker_num = 0;
    config.local_transfer.te_async_poll_worker_num = 0;
    config.key_lease.duration_ms = lease_duration_ms;
    // The TransferEngine below is constructed but never init()ed, which is all
    // the local Put/Get/lease paths need. Registering tier memory against an
    // un-initialized engine would dereference an absent transport.
    config.register_tiers_with_transfer_engine = false;
    // Keep the suite fast. The bound itself is what UnwaitedPutHandle... and
    // WaitAfterStop... assert; five real seconds per case would only measure
    // the default.
    config.stop_drain_timeout = std::chrono::milliseconds(300);

    auto recorder = env->recorder;
    MetadataCallbacks callbacks;
    callbacks.add_replica = [recorder](
                                std::string_view key, const UUID& tier_id,
                                size_t size) -> tl::expected<void, ErrorCode> {
        std::lock_guard<std::mutex> lock(recorder->mu);
        recorder->adds.push_back({std::string(key), tier_id, size});
        return {};
    };
    callbacks.remove_replica =
        [recorder](std::string_view key,
                   const UUID& tier_id) -> tl::expected<void, ErrorCode> {
        std::lock_guard<std::mutex> lock(recorder->mu);
        recorder->removes.push_back({std::string(key), tier_id});
        return {};
    };
    callbacks.segment_sync = [](const Segment&,
                                bool) -> tl::expected<void, ErrorCode> {
        return {};
    };
    callbacks.rectify_route = [recorder](std::string_view key,
                                         std::optional<UUID> tier_id) {
        std::lock_guard<std::mutex> lock(recorder->mu);
        recorder->rectifies.emplace_back(std::string(key), tier_id);
    };

    auto created = CreateDataManager(config, env->transfer_engine,
                                     std::move(callbacks), {});
    if (!created) {
        ADD_FAILURE() << "CreateDataManager failed: "
                      << toString(created.error());
        return nullptr;
    }
    env->manager = std::move(created.value());
    return env;
}

std::string VersionSuffix(DataManagerVersion version) {
    return ToString(version);
}

/**
 * @struct VersionCapabilities
 * @brief What a version implements today.
 *
 * The contract is written once and runs against every version; a version that
 * has not reached a given surface yet skips those cases explicitly rather than
 * having them deleted or weakened. Each `false` here is a phase still to land,
 * and flipping it to `true` is what makes the corresponding contract binding.
 */
struct VersionCapabilities {
    bool forward_protocol = true;  // PreWrite / Commit / Revoke / Pin / UnPin
    bool remote_io = true;  // Read/WriteRemoteDataAsync, TransferDataAsync
    bool slow_tier = true;  // a storage tier can be configured at all

    /**
     * Reclaiming space never destroys an object that exists on only one tier.
     *
     * Unlike the fields above, this one is NOT a phase still to land -- it is a
     * decided difference. V1 demotes such a replica to a slower tier. V2's
     * reclaim path is tier-local by design (redesign section 4.1): it does not
     * look at other tiers, does not demote and does not wait for an offload,
     * and durability is delegated entirely to the offload pipeline. If offload
     * has not copied the block down when the tier fills, the object is lost.
     *
     * It is a capability rather than a deleted assertion so that the
     * difference stays in the contract where a reader will find it. V2 counts
     * every such loss in EvictStats::victims_sole_replica.
     */
    bool reclaim_preserves_sole_replica = true;
};

VersionCapabilities CapabilitiesOf(DataManagerVersion version) {
    switch (version) {
        case DataManagerVersion::kV1:
            return VersionCapabilities{};
        case DataManagerVersion::kV2: {
            VersionCapabilities capabilities;
            capabilities.reclaim_preserves_sole_replica = false;
            return capabilities;
        }
    }
    return VersionCapabilities{};
}

}  // namespace

// GTEST_SKIP() expands to a return, so these have to be macros used directly
// in a test body: wrapping them in a helper would mark the test skipped and
// then keep running it.
#define SKIP_UNLESS_FORWARD_PROTOCOL()                              \
    do {                                                            \
        if (!CapabilitiesOf(GetParam()).forward_protocol) {         \
            GTEST_SKIP() << "forward protocol not implemented for " \
                         << VersionSuffix(GetParam()) << " yet";    \
        }                                                           \
    } while (0)

#define SKIP_UNLESS_REMOTE_IO()                                  \
    do {                                                         \
        if (!CapabilitiesOf(GetParam()).remote_io) {             \
            GTEST_SKIP() << "remote IO not implemented for "     \
                         << VersionSuffix(GetParam()) << " yet"; \
        }                                                        \
    } while (0)

#define SKIP_UNLESS_SLOW_TIER()                                    \
    do {                                                           \
        if (!CapabilitiesOf(GetParam()).slow_tier) {               \
            GTEST_SKIP() << "slow-tier pools not implemented for " \
                         << VersionSuffix(GetParam()) << " yet";   \
        }                                                          \
    } while (0)

// ===========================================================================
// Fixture
// ===========================================================================

class DataManagerContractTest
    : public ::testing::TestWithParam<DataManagerVersion> {
   protected:
    static void SetUpTestSuite() {
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("DataManagerContractTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_path_ = "/tmp/mooncake_dm_contract_" +
                        VersionSuffix(GetParam()) + "_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::remove_all(storage_path_);
        std::filesystem::create_directories(storage_path_);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", storage_path_.c_str(), 1);

        env_ =
            MakeManager(GetParam(), kDramOnlyTiers, LocalTransferMode::MEMCPY);
        ASSERT_NE(env_, nullptr);
    }

    void TearDown() override {
        env_.reset();
        std::filesystem::remove_all(storage_path_);
    }

    DataManager& dm() { return *env_->manager; }
    CallbackRecorder& recorder() { return *env_->recorder; }

    // Background work is asynchronous in V2, so a comparison taken right after
    // the last API call would race it. The hook is reached by dynamic_cast
    // precisely so the production interface carries no test-only method.
    static void Drain(DataManager& manager) {
        if (auto* hook = dynamic_cast<DataManagerTestHook*>(&manager)) {
            hook->DrainForTest();
        }
    }

    // --- helpers -----------------------------------------------------------

    static std::string MakePayload(size_t size, char seed = 'a') {
        std::string out(size, seed);
        for (size_t i = 0; i < size; ++i) {
            out[i] = static_cast<char>('a' + ((i + seed) % 26));
        }
        return out;
    }

    tl::expected<void, ErrorCode> Put(DataManager& target,
                                      const std::string& key,
                                      const std::string& payload) {
        std::vector<Slice> slices = {
            {const_cast<char*>(payload.data()), payload.size()}};
        auto handle = target.Put(key, slices);
        if (!handle) return tl::make_unexpected(handle.error());
        return handle.value()->Wait();
    }

    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      const std::string& payload) {
        return Put(dm(), key, payload);
    }

    tl::expected<std::string, ErrorCode> Get(const std::string& key,
                                             size_t size) {
        std::string out(size, '\0');
        std::vector<Slice> slices = {{out.data(), out.size()}};
        auto handle = dm().Get(key, slices);
        if (!handle) return tl::make_unexpected(handle.error());
        auto waited = handle.value().task_handle->Wait();
        if (!waited) return tl::make_unexpected(waited.error());
        return out;
    }

    std::string storage_path_;
    std::unique_ptr<ManagerEnv> env_;
};

// ===========================================================================
// Put / Get: happy paths
// ===========================================================================

TEST_P(DataManagerContractTest, PutThenGetSingleSlice) {
    const std::string key = "contract_single";
    const std::string payload = MakePayload(4096);

    ASSERT_TRUE(Put(key, payload).has_value());
    EXPECT_TRUE(dm().Exist(key));

    auto got = Get(key, payload.size());
    ASSERT_TRUE(got.has_value()) << toString(got.error());
    EXPECT_EQ(*got, payload);
}

TEST_P(DataManagerContractTest, GetScattersIntoMultipleSlices) {
    const std::string key = "contract_multi_slice";
    const std::string payload = MakePayload(3000);
    ASSERT_TRUE(Put(key, payload).has_value());

    std::string part_a(1000, '\0');
    std::string part_b(1000, '\0');
    std::string part_c(1000, '\0');
    std::vector<Slice> slices = {{part_a.data(), part_a.size()},
                                 {part_b.data(), part_b.size()},
                                 {part_c.data(), part_c.size()}};
    auto handle = dm().Get(key, slices);
    ASSERT_TRUE(handle.has_value()) << toString(handle.error());
    ASSERT_TRUE(handle.value().task_handle->Wait().has_value());
    EXPECT_EQ(part_a + part_b + part_c, payload);
    EXPECT_EQ(handle.value().data_size, static_cast<int64_t>(payload.size()));
}

TEST_P(DataManagerContractTest, GetWithAllocatorReturnsExactSizedBuffer) {
    const std::string key = "contract_allocator";
    const std::string payload = MakePayload(2048);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto allocator = ClientBufferAllocator::create(64ULL * 1024 * 1024);
    ASSERT_NE(allocator, nullptr);
    auto handle = dm().Get(key, allocator);
    ASSERT_TRUE(handle.has_value()) << toString(handle.error());
    ASSERT_TRUE(handle.value().task_handle->Wait().has_value());
    ASSERT_NE(handle.value().read_buf, nullptr);
    ASSERT_EQ(handle.value().read_buf->size(), payload.size());
    EXPECT_EQ(
        std::string(static_cast<const char*>(handle.value().read_buf->ptr()),
                    payload.size()),
        payload);
}

// ===========================================================================
// Put / Get: argument validation
// ===========================================================================

TEST_P(DataManagerContractTest, PutRejectsEmptyKey) {
    const std::string payload = MakePayload(16);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};
    auto handle = dm().Put("", slices);
    if (handle.has_value()) {
        auto waited = handle.value()->Wait();
        ASSERT_FALSE(waited.has_value()) << "empty key must not commit";
        EXPECT_EQ(waited.error(), ErrorCode::INVALID_PARAMS);
    } else {
        EXPECT_EQ(handle.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_FALSE(dm().Exist(""));
}

TEST_P(DataManagerContractTest, PutRejectsZeroLengthPayload) {
    const std::string key = "contract_zero_len";
    char dummy = 0;
    std::vector<Slice> slices = {{&dummy, 0}};
    auto handle = dm().Put(key, slices);
    if (handle.has_value()) {
        auto waited = handle.value()->Wait();
        ASSERT_FALSE(waited.has_value());
        EXPECT_EQ(waited.error(), ErrorCode::INVALID_PARAMS);
    } else {
        EXPECT_EQ(handle.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_FALSE(dm().Exist(key));
}

TEST_P(DataManagerContractTest, PutRejectsAllocationLargerThanCapacity) {
    const std::string key = "contract_too_big";
    const std::string payload = MakePayload(1024);
    // Far beyond the configured 256MiB DRAM tier.
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), 4ULL * 1024 * 1024 * 1024}};
    auto handle = dm().Put(key, slices);
    ASSERT_FALSE(handle.has_value());
    EXPECT_EQ(handle.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(dm().Exist(key));
}

TEST_P(DataManagerContractTest, GetRejectsTooSmallDestination) {
    const std::string key = "contract_small_dest";
    const std::string payload = MakePayload(4096);
    ASSERT_TRUE(Put(key, payload).has_value());

    std::string too_small(16, '\0');
    std::vector<Slice> slices = {{too_small.data(), too_small.size()}};
    auto handle = dm().Get(key, slices);
    if (handle.has_value()) {
        auto waited = handle.value().task_handle->Wait();
        ASSERT_FALSE(waited.has_value());
        EXPECT_EQ(waited.error(), ErrorCode::INVALID_PARAMS);
    } else {
        EXPECT_EQ(handle.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_P(DataManagerContractTest, GetMissingKeyReturnsObjectNotFound) {
    std::string out(16, '\0');
    std::vector<Slice> slices = {{out.data(), out.size()}};
    auto handle = dm().Get("contract_absent", slices);
    ASSERT_FALSE(handle.has_value());
    EXPECT_EQ(handle.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ===========================================================================
// Put: duplicates and concurrency
// ===========================================================================

TEST_P(DataManagerContractTest, DuplicatePutIsRejected) {
    const std::string key = "contract_dup";
    const std::string payload = MakePayload(512);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto second = Put(key, payload);
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // The original value survives.
    auto got = Get(key, payload.size());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, payload);
}

TEST_P(DataManagerContractTest, ConcurrentSameKeyPutHasExactlyOneWinner) {
    const std::string key = "contract_race";
    const std::string payload = MakePayload(1024);
    constexpr int kThreads = 8;

    std::atomic<int> winners{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            if (Put(key, payload).has_value()) {
                winners.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(winners.load(), 1);
    EXPECT_TRUE(dm().Exist(key));
    auto got = Get(key, payload.size());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, payload);
}

// The write claim must span the whole Put, not just its synchronous prologue.
// If it were released before the copy, a second writer would find neither a
// claim nor a committed replica and would allocate a second full-size block
// for an object that is about to exist -- and on a tight tier those wasted
// allocations are what turn write contention into an eviction storm.
TEST_P(DataManagerContractTest, WriteClaimIsHeldUntilThePutTaskCompletes) {
    const std::string key = "contract_claim_span";
    const std::string payload = MakePayload(4096);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};

    auto first = dm().Put(key, slices);
    ASSERT_TRUE(first.has_value());

    // The first Put has returned but has not run yet: the key is claimed and
    // not yet visible, and a second writer must be told so.
    EXPECT_FALSE(dm().Exist(key));
    auto second = dm().Put(key, slices);
    ASSERT_FALSE(second.has_value())
        << "the claim must still exclude a second writer";
    EXPECT_EQ(second.error(), ErrorCode::REPLICA_IS_PROCESSING);

    ASSERT_TRUE(first.value()->Wait().has_value());
    EXPECT_TRUE(dm().Exist(key));

    // Once committed the key is rejected for a different reason, and that
    // reason is permanent rather than retryable.
    auto third = Put(key, payload);
    ASSERT_FALSE(third.has_value());
    EXPECT_EQ(third.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
}

// The window that actually mattered is inside Wait(): the claim used to be
// released before the copy started, so a second writer could slip in during
// the memcpy and take its own full-size allocation. The payload here is large
// enough that the copy dominates, and the assertion can never false-fail --
// a correct implementation simply never hands out a second handle.
TEST_P(DataManagerContractTest, NoSecondWriterSlipsInWhileAPutIsCopying) {
    const std::string key = "contract_claim_during_copy";
    const std::string payload = MakePayload(32ULL * 1024 * 1024);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};

    auto first = dm().Put(key, slices);
    ASSERT_TRUE(first.has_value());

    std::atomic<bool> writing{true};
    std::atomic<int> intruders{0};
    std::atomic<int> attempts{0};
    std::thread intruder([&] {
        std::vector<Slice> other = {
            {const_cast<char*>(payload.data()), payload.size()}};
        while (writing.load(std::memory_order_acquire)) {
            attempts.fetch_add(1, std::memory_order_relaxed);
            auto handle = dm().Put(key, other);
            if (handle.has_value()) {
                // Getting a handle here means the claim was already gone while
                // the first writer was still copying: this attempt has taken a
                // second full-size allocation for the same key.
                intruders.fetch_add(1, std::memory_order_relaxed);
                (void)handle.value()->Wait();
            }
        }
    });

    auto waited = first.value()->Wait();
    writing.store(false, std::memory_order_release);
    intruder.join();

    ASSERT_TRUE(waited.has_value()) << toString(waited.error());
    EXPECT_GT(attempts.load(), 0);
    EXPECT_EQ(intruders.load(), 0)
        << "a second writer allocated while the first was still copying";
    auto got = Get(key, payload.size());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, payload);
}

TEST_P(DataManagerContractTest, ConcurrentDistinctKeyPutAllSucceed) {
    constexpr int kThreads = 8;
    const std::string payload = MakePayload(1024);

    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            if (Put("contract_par_" + std::to_string(i), payload).has_value()) {
                ok.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(ok.load(), kThreads);
    for (int i = 0; i < kThreads; ++i) {
        EXPECT_TRUE(dm().Exist("contract_par_" + std::to_string(i)));
    }
}

// ===========================================================================
// Key lifetime: implementations must copy the key, not borrow it
// ===========================================================================

TEST_P(DataManagerContractTest, PutTaskOutlivesCallerKeyStorage) {
    const std::string key_value(64, 'k');
    const std::string payload = MakePayload(1024);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};

    auto key_storage = std::make_unique<std::string>(key_value);
    auto handle = dm().Put(*key_storage, slices);
    ASSERT_TRUE(handle.has_value());
    key_storage.reset();

    ASSERT_TRUE(handle.value()->Wait().has_value());
    EXPECT_TRUE(dm().Exist(key_value));
    auto got = Get(key_value, payload.size());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, payload);
}

TEST_P(DataManagerContractTest, GetTaskOutlivesCallerKeyStorage) {
    const std::string key_value(64, 'g');
    const std::string payload = MakePayload(1024);
    ASSERT_TRUE(Put(key_value, payload).has_value());

    std::string out(payload.size(), '\0');
    std::vector<Slice> slices = {{out.data(), out.size()}};
    auto key_storage = std::make_unique<std::string>(key_value);
    auto handle = dm().Get(*key_storage, slices);
    ASSERT_TRUE(handle.has_value());
    key_storage.reset();

    ASSERT_TRUE(handle.value().task_handle->Wait().has_value());
    EXPECT_EQ(out, payload);
}

// ===========================================================================
// Metadata queries
// ===========================================================================

TEST_P(DataManagerContractTest, QueryReturnsTierAndSize) {
    const std::string key = "contract_query";
    const std::string payload = MakePayload(777);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto tier_views = dm().GetTierViews();
    ASSERT_FALSE(tier_views.empty());

    auto query = dm().Query(key);
    ASSERT_TRUE(query.has_value()) << toString(query.error());
    EXPECT_EQ(query->second, payload.size());
    bool known_tier = false;
    for (const auto& view : tier_views) {
        if (view.id == query->first) known_tier = true;
    }
    EXPECT_TRUE(known_tier) << "Query must name a tier from GetTierViews()";

    auto missing = dm().Query("contract_query_absent");
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_P(DataManagerContractTest, QueryObjectSizeMatchesPayload) {
    const std::string key = "contract_size";
    const std::string payload = MakePayload(1234);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto size = dm().QueryObjectSize(key);
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, payload.size());

    auto missing = dm().QueryObjectSize("contract_size_absent");
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_P(DataManagerContractTest, ExistHonoursTierFilter) {
    const std::string key = "contract_exist";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());

    EXPECT_TRUE(dm().Exist(key));
    EXPECT_FALSE(dm().Exist("contract_exist_absent"));

    auto tier_views = dm().GetTierViews();
    ASSERT_EQ(tier_views.size(), 1U);
    EXPECT_TRUE(dm().Exist(key, tier_views[0].id));
    EXPECT_FALSE(dm().Exist(key, UUID{0xdead, 0xbeef}));
}

TEST_P(DataManagerContractTest, GetReplicaTierIdsListsExactReplicas) {
    const std::string key = "contract_replicas";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());

    auto tier_ids = dm().GetReplicaTierIds(key);
    ASSERT_EQ(tier_ids.size(), 1U);
    EXPECT_EQ(tier_ids[0], dm().GetTierViews()[0].id);

    // Absent key: empty vector, never an error.
    EXPECT_TRUE(dm().GetReplicaTierIds("contract_replicas_absent").empty());
}

TEST_P(DataManagerContractTest, GetTierViewsReportsConfiguredTopology) {
    auto views = dm().GetTierViews();
    ASSERT_EQ(views.size(), 1U);
    EXPECT_EQ(views[0].type, MemoryType::DRAM);
    EXPECT_EQ(views[0].capacity, 268435456U);
    EXPECT_EQ(views[0].priority, 10);
    EXPECT_EQ(views[0].GetName(), MakeTierSegmentName(views[0].id));
    EXPECT_LE(views[0].usage, views[0].capacity);
}

// ===========================================================================
// Delete / RemoveAll
// ===========================================================================

TEST_P(DataManagerContractTest, DeleteRemovesKeyAndNotifiesMaster) {
    const std::string key = "contract_delete";
    ASSERT_TRUE(Put(key, MakePayload(128)).has_value());
    ASSERT_TRUE(dm().Exist(key));

    ASSERT_TRUE(dm().Delete(key).has_value());
    EXPECT_FALSE(dm().Exist(key));
    EXPECT_TRUE(dm().GetReplicaTierIds(key).empty());
    EXPECT_TRUE(recorder().SawRemove(key));
}

TEST_P(DataManagerContractTest, DeleteWithNotifyMasterFalseSkipsCallback) {
    const std::string key = "contract_delete_silent";
    ASSERT_TRUE(Put(key, MakePayload(128)).has_value());
    const size_t before = recorder().RemoveCount();

    ASSERT_TRUE(
        dm().Delete(key, std::nullopt, /*notify_master=*/false).has_value());
    EXPECT_FALSE(dm().Exist(key));
    EXPECT_EQ(recorder().RemoveCount(), before);
}

TEST_P(DataManagerContractTest, DeleteMissingKeyReturnsObjectNotFound) {
    auto result = dm().Delete("contract_delete_absent");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_P(DataManagerContractTest, DeleteOnWrongTierLeavesReplicaIntact) {
    const std::string key = "contract_delete_wrong_tier";
    ASSERT_TRUE(Put(key, MakePayload(128)).has_value());

    auto result = dm().Delete(key, UUID{0xdead, 0xbeef});
    ASSERT_FALSE(result.has_value());
    // Distinct from OBJECT_NOT_FOUND on purpose: a caller deleting one
    // specific replica has to be able to tell "no such tier" from "no such
    // key".
    EXPECT_EQ(result.error(), ErrorCode::TIER_NOT_FOUND);
    EXPECT_TRUE(dm().Exist(key)) << "a wrong tier id must not drop the replica";
}

TEST_P(DataManagerContractTest, RemoveAllReturnsDistinctKeyCount) {
    constexpr int kKeys = 5;
    for (int i = 0; i < kKeys; ++i) {
        ASSERT_TRUE(
            Put("contract_removeall_" + std::to_string(i), MakePayload(64))
                .has_value());
    }

    auto removed = dm().RemoveAll();
    ASSERT_TRUE(removed.has_value()) << toString(removed.error());
    EXPECT_EQ(*removed, kKeys);
    for (int i = 0; i < kKeys; ++i) {
        EXPECT_FALSE(dm().Exist("contract_removeall_" + std::to_string(i)));
    }

    // Idempotent: nothing left to remove.
    auto again = dm().RemoveAll();
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(*again, 0);
}

TEST_P(DataManagerContractTest, InFlightGetSurvivesConcurrentDelete) {
    const std::string key = "contract_delete_race";
    const std::string payload = MakePayload(8192);
    ASSERT_TRUE(Put(key, payload).has_value());

    std::string out(payload.size(), '\0');
    std::vector<Slice> slices = {{out.data(), out.size()}};
    auto handle = dm().Get(key, slices);
    ASSERT_TRUE(handle.has_value());

    // Delete while the read task has been handed out but not run yet.
    ASSERT_TRUE(dm().Delete(key).has_value());
    EXPECT_FALSE(dm().Exist(key));

    // The already-issued read still completes with the right bytes.
    ASSERT_TRUE(handle.value().task_handle->Wait().has_value());
    EXPECT_EQ(out, payload);

    // A read issued after the delete misses.
    std::string out2(payload.size(), '\0');
    std::vector<Slice> slices2 = {{out2.data(), out2.size()}};
    auto after = dm().Get(key, slices2);
    ASSERT_FALSE(after.has_value());
    EXPECT_EQ(after.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ===========================================================================
// ForEachKeyBatch
// ===========================================================================

TEST_P(DataManagerContractTest, ForEachKeyBatchEmitsOneEntryPerReplica) {
    constexpr int kKeys = 6;
    for (int i = 0; i < kKeys; ++i) {
        ASSERT_TRUE(Put("contract_walk_" + std::to_string(i), MakePayload(100))
                        .has_value());
    }

    std::vector<ReplicaLocation> seen;
    dm().ForEachKeyBatch([&](std::vector<ReplicaLocation>&& batch) {
        for (auto& entry : batch) seen.push_back(std::move(entry));
        return true;
    });

    // One DRAM tier is configured, so exactly one replica per key.
    EXPECT_EQ(seen.size(), static_cast<size_t>(kKeys));
    const UUID tier_id = dm().GetTierViews()[0].id;
    for (const auto& entry : seen) {
        EXPECT_EQ(entry.tier_id, tier_id);
        EXPECT_EQ(entry.size, 100U);
        EXPECT_EQ(entry.key.rfind("contract_walk_", 0), 0U);
    }
}

TEST_P(DataManagerContractTest, ForEachKeyBatchStopsWhenCallbackReturnsFalse) {
    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(
            Put("contract_stopwalk_" + std::to_string(i), MakePayload(64))
                .has_value());
    }

    int batches = 0;
    dm().ForEachKeyBatch([&](std::vector<ReplicaLocation>&&) {
        ++batches;
        return false;
    });
    EXPECT_EQ(batches, 1) << "returning false must stop the walk immediately";
}

TEST_P(DataManagerContractTest, ForEachKeyBatchOnEmptyStoreEmitsNothing) {
    size_t entries = 0;
    dm().ForEachKeyBatch([&](std::vector<ReplicaLocation>&& batch) {
        entries += batch.size();
        return true;
    });
    EXPECT_EQ(entries, 0U);
}

// ===========================================================================
// Hot key statistics
// ===========================================================================

TEST_P(DataManagerContractTest, SuccessfulQueryCountsTowardsHotKeyStats) {
    const std::string key = "contract_hot";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());

    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(dm().Query(key).has_value());
    }
    // Failed queries must not be recorded.
    for (int i = 0; i < 20; ++i) {
        ASSERT_FALSE(dm().Query("contract_hot_absent").has_value());
    }

    auto stats = dm().GetHotKeyStats(/*hot_key_num=*/0);
    bool saw_key = false;
    bool saw_absent = false;
    for (const auto& entry : stats.hot_keys) {
        if (entry.key == key) saw_key = true;
        if (entry.key == "contract_hot_absent") saw_absent = true;
    }
    EXPECT_TRUE(saw_key) << "a successful Query must count as an access";
    EXPECT_FALSE(saw_absent) << "a failed Query must not create a hot key";
}

// Committing counts as an access. Without it a freshly written key would be
// invisible to HARecoveryManager's hot-key phase until someone read it.
//
// The assertion is on the score of a key that was committed and then read
// once: two accesses if the commit counted, one if it did not. Asserting mere
// presence after a bare commit would be knife-edge on V1, whose decaying
// collector reports exactly the tracking threshold for a single access and
// prunes it on an unlucky rounding.
TEST_P(DataManagerContractTest, CommitCountsAsAnAccess) {
    const std::string key = "contract_warm_on_commit";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());
    ASSERT_TRUE(dm().Query(key).has_value());

    auto stats = dm().GetHotKeyStats(/*hot_key_num=*/0);
    const AccessStatEntry* found = nullptr;
    for (const auto& entry : stats.hot_keys) {
        if (entry.key == key) found = &entry;
    }
    ASSERT_NE(found, nullptr) << "a committed and read key must be tracked";
    // The discriminator is one access versus two, so the threshold sits
    // between them: V1 scores are floating point and land a few ulps under an
    // exact 2.0.
    EXPECT_GT(found->recent_heat_score, 1.5)
        << "the commit itself must have counted as an access";
}

TEST_P(DataManagerContractTest, SuccessfulQueryObjectSizeCountsAsAnAccess) {
    const std::string present = "contract_size_heat_present";
    const std::string absent = "contract_size_heat_absent";
    ASSERT_TRUE(Put(present, MakePayload(64)).has_value());

    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(dm().QueryObjectSize(present).has_value());
        ASSERT_FALSE(dm().QueryObjectSize(absent).has_value());
    }

    auto stats = dm().GetHotKeyStats(/*hot_key_num=*/0);
    bool saw_absent = false;
    for (const auto& entry : stats.hot_keys) {
        if (entry.key == absent) saw_absent = true;
    }
    EXPECT_FALSE(saw_absent) << "a failed size query must not create a hot key";

    // The present key outranks a key that was only written, never queried.
    const std::string cold = "contract_size_heat_cold";
    ASSERT_TRUE(Put(cold, MakePayload(64)).has_value());
    auto top = dm().GetHotKeyStats(/*hot_key_num=*/1);
    ASSERT_EQ(top.hot_keys.size(), 1U);
    EXPECT_EQ(top.hot_keys[0].key, present);
}

TEST_P(DataManagerContractTest, HotKeyStatsRespectRequestedLimit) {
    for (int i = 0; i < 10; ++i) {
        const std::string key = "contract_hotlimit_" + std::to_string(i);
        ASSERT_TRUE(Put(key, MakePayload(64)).has_value());
        for (int j = 0; j <= i; ++j) {
            ASSERT_TRUE(dm().Query(key).has_value());
        }
    }

    auto limited = dm().GetHotKeyStats(/*hot_key_num=*/3);
    EXPECT_LE(limited.hot_keys.size(), 3U);

    auto all = dm().GetHotKeyStats(/*hot_key_num=*/0);
    EXPECT_GE(all.hot_keys.size(), limited.hot_keys.size());
}

// ===========================================================================
// PreWrite / WriteCommit / WriteRevoke
// ===========================================================================

TEST_P(DataManagerContractTest, PreWriteCommitMakesObjectVisible) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_prewrite";
    const std::string payload = MakePayload(256);

    auto pre = dm().PreWrite(key, payload.size());
    ASSERT_TRUE(pre.has_value()) << toString(pre.error());
    EXPECT_NE(pre->remote_buffer.addr, 0U);
    EXPECT_EQ(pre->remote_buffer.size, payload.size());
    EXPECT_FALSE(IsZeroUUID(pre->write_operation_id));
    // Not visible before commit.
    EXPECT_FALSE(dm().Exist(key));

    std::memcpy(reinterpret_cast<void*>(pre->remote_buffer.addr),
                payload.data(), payload.size());
    ASSERT_TRUE(dm().WriteCommit(key, pre->write_operation_id).has_value());

    EXPECT_TRUE(dm().Exist(key));
    auto got = Get(key, payload.size());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, payload);
    EXPECT_TRUE(recorder().SawAdd(key));
}

TEST_P(DataManagerContractTest, PreWriteRejectsInvalidArguments) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto empty_key = dm().PreWrite("", 128);
    ASSERT_FALSE(empty_key.has_value());
    EXPECT_EQ(empty_key.error(), ErrorCode::INVALID_PARAMS);

    auto zero_size = dm().PreWrite("contract_prewrite_zero", 0);
    ASSERT_FALSE(zero_size.has_value());
    EXPECT_EQ(zero_size.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, ConcurrentPreWriteOnSameKeyHasOneWinner) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_prewrite_race";
    auto first = dm().PreWrite(key, 128);
    ASSERT_TRUE(first.has_value());

    auto second = dm().PreWrite(key, 128);
    ASSERT_FALSE(second.has_value())
        << "a live write lease must exclude a second PreWrite";
    // A live lease and an already-committed object are distinguishable: the
    // former is retryable once the lease clears, the latter never is.
    EXPECT_EQ(second.error(), ErrorCode::REPLICA_IS_PROCESSING);

    ASSERT_TRUE(dm().WriteRevoke(key, first->write_operation_id).has_value());
}

TEST_P(DataManagerContractTest, PreWriteOnCommittedKeyIsRejected) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_prewrite_exists";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());

    auto pre = dm().PreWrite(key, 64);
    ASSERT_FALSE(pre.has_value());
    EXPECT_EQ(pre.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
}

TEST_P(DataManagerContractTest, WriteCommitRejectsWrongToken) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_commit_token";
    auto pre = dm().PreWrite(key, 128);
    ASSERT_TRUE(pre.has_value());

    auto wrong = dm().WriteCommit(key, generate_uuid());
    ASSERT_FALSE(wrong.has_value());
    EXPECT_EQ(wrong.error(), ErrorCode::INVALID_WRITE);
    EXPECT_FALSE(dm().Exist(key));

    // The real token still works afterwards.
    ASSERT_TRUE(dm().WriteCommit(key, pre->write_operation_id).has_value());
    EXPECT_TRUE(dm().Exist(key));
}

TEST_P(DataManagerContractTest, WriteCommitRejectsInvalidArguments) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto empty_key = dm().WriteCommit("", generate_uuid());
    ASSERT_FALSE(empty_key.has_value());
    EXPECT_EQ(empty_key.error(), ErrorCode::INVALID_PARAMS);

    auto zero_token = dm().WriteCommit("contract_commit_zero", UUID{0, 0});
    ASSERT_FALSE(zero_token.has_value());
    EXPECT_EQ(zero_token.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, WriteCommitWithoutPreWriteFails) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto result = dm().WriteCommit("contract_commit_orphan", generate_uuid());
    ASSERT_FALSE(result.has_value());
    EXPECT_FALSE(dm().Exist("contract_commit_orphan"));
}

TEST_P(DataManagerContractTest, WriteRevokeReleasesTheLease) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_revoke";
    auto pre = dm().PreWrite(key, 128);
    ASSERT_TRUE(pre.has_value());

    ASSERT_TRUE(dm().WriteRevoke(key, pre->write_operation_id).has_value());
    EXPECT_FALSE(dm().Exist(key));

    // The key is writable again.
    auto again = dm().PreWrite(key, 128);
    ASSERT_TRUE(again.has_value());
    ASSERT_TRUE(dm().WriteRevoke(key, again->write_operation_id).has_value());
}

TEST_P(DataManagerContractTest, WriteRevokeIsIdempotentForUnknownKey) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto result = dm().WriteRevoke("contract_revoke_absent", generate_uuid());
    EXPECT_TRUE(result.has_value())
        << "revoking a lease that does not exist must be a no-op success";
}

TEST_P(DataManagerContractTest, WriteRevokeRejectsForeignToken) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_revoke_token";
    auto pre = dm().PreWrite(key, 128);
    ASSERT_TRUE(pre.has_value());

    auto wrong = dm().WriteRevoke(key, generate_uuid());
    ASSERT_FALSE(wrong.has_value());
    EXPECT_EQ(wrong.error(), ErrorCode::INVALID_WRITE);

    // The original lease is untouched, so its own commit still works.
    ASSERT_TRUE(dm().WriteCommit(key, pre->write_operation_id).has_value());
}

// ===========================================================================
// PinKey / UnPinKey
// ===========================================================================

TEST_P(DataManagerContractTest, PinKeyReturnsUsableAddressAndRefCounts) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_pin";
    const std::string payload = MakePayload(512);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto first = dm().PinKey(key);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    EXPECT_NE(first->remote_buffer.addr, 0U);
    EXPECT_EQ(first->remote_buffer.size, payload.size());
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(first->remote_buffer.addr),
                    payload.size()),
        payload);

    // Second pin on the same replica reuses the token and bumps the refcount.
    auto second = dm().PinKey(key);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->read_operation_id, first->read_operation_id);

    // Two unpins are needed to release it.
    ASSERT_TRUE(dm().UnPinKey(key, first->read_operation_id).has_value());
    ASSERT_TRUE(dm().UnPinKey(key, first->read_operation_id).has_value());
}

TEST_P(DataManagerContractTest, PinKeyOnMissingKeyReturnsObjectNotFound) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto pin = dm().PinKey("contract_pin_absent");
    ASSERT_FALSE(pin.has_value());
    EXPECT_EQ(pin.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_P(DataManagerContractTest, PinKeyRejectsEmptyKey) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto pin = dm().PinKey("");
    ASSERT_FALSE(pin.has_value());
    EXPECT_EQ(pin.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, UnPinKeyRejectsForeignToken) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_unpin_token";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());
    auto pin = dm().PinKey(key);
    ASSERT_TRUE(pin.has_value());

    auto wrong = dm().UnPinKey(key, generate_uuid());
    ASSERT_FALSE(wrong.has_value());
    EXPECT_EQ(wrong.error(), ErrorCode::INVALID_READ);

    ASSERT_TRUE(dm().UnPinKey(key, pin->read_operation_id).has_value());
}

TEST_P(DataManagerContractTest, UnPinKeyIsIdempotentWithoutLease) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_unpin_absent";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());
    // Never pinned: unpinning is a no-op success.
    EXPECT_TRUE(dm().UnPinKey(key, generate_uuid()).has_value());
}

TEST_P(DataManagerContractTest, UnPinKeyRejectsInvalidArguments) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    auto empty_key = dm().UnPinKey("", generate_uuid());
    ASSERT_FALSE(empty_key.has_value());
    EXPECT_EQ(empty_key.error(), ErrorCode::INVALID_PARAMS);

    auto zero_token = dm().UnPinKey("contract_unpin_zero", UUID{0, 0});
    ASSERT_FALSE(zero_token.has_value());
    EXPECT_EQ(zero_token.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, PinnedDataStaysReadableAfterDelete) {
    SKIP_UNLESS_FORWARD_PROTOCOL();
    const std::string key = "contract_pin_delete";
    const std::string payload = MakePayload(1024);
    ASSERT_TRUE(Put(key, payload).has_value());

    auto pin = dm().PinKey(key);
    ASSERT_TRUE(pin.has_value());

    ASSERT_TRUE(dm().Delete(key).has_value());
    EXPECT_FALSE(dm().Exist(key));

    // The lease keeps the physical resource alive.
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(pin->remote_buffer.addr),
                    payload.size()),
        payload);
    EXPECT_TRUE(dm().UnPinKey(key, pin->read_operation_id).has_value());
}

// ===========================================================================
// Remote IO (no RDMA required for these cases)
// ===========================================================================

TEST_P(DataManagerContractTest, ReadRemoteDataAsyncMissingKeyIsNotFound) {
    SKIP_UNLESS_REMOTE_IO();
    std::vector<RemoteBufferDesc> dest;
    RemoteBufferDesc desc;
    desc.segment_endpoint = "127.0.0.1:1";
    desc.addr = 0x1000;
    desc.size = 64;
    dest.push_back(desc);

    auto result = async_simple::coro::syncAwait(
        dm().ReadRemoteDataAsync("contract_remote_absent", dest));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_P(DataManagerContractTest, ReadRemoteDataAsyncRejectsInvalidBuffers) {
    SKIP_UNLESS_REMOTE_IO();
    const std::string key = "contract_remote_invalid";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());

    std::vector<RemoteBufferDesc> dest;
    RemoteBufferDesc desc;
    desc.segment_endpoint = "";  // invalid
    desc.addr = 0;               // invalid
    desc.size = 0;               // invalid
    dest.push_back(desc);

    auto result =
        async_simple::coro::syncAwait(dm().ReadRemoteDataAsync(key, dest));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, TransferDataAsyncRejectsInvalidPeerBuffers) {
    SKIP_UNLESS_REMOTE_IO();
    std::vector<char> local(128, 0);
    std::vector<RemoteBufferDesc> peers;
    RemoteBufferDesc desc;
    desc.segment_endpoint = "";
    desc.addr = 0;
    desc.size = 0;
    peers.push_back(desc);

    auto result = AwaitVoidFuture(dm().TransferDataAsync(
        local.data(), local.size(), peers, Transport::TransferRequest::WRITE));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_P(DataManagerContractTest, GetCoroExecutorIsNeverNull) {
    EXPECT_NE(dm().GetCoroExecutor(), nullptr);
    dm().Stop();
    EXPECT_NE(dm().GetCoroExecutor(), nullptr)
        << "GetCoroExecutor must stay callable after Stop()";
}

// ===========================================================================
// Route rectification
// ===========================================================================

TEST_P(DataManagerContractTest, RectifyReadRouteFiresOnlyOnLocalMiss) {
    const std::string present = "contract_rectify_present";
    ASSERT_TRUE(Put(present, MakePayload(64)).has_value());

    dm().RectifyReadRoute(present);
    EXPECT_EQ(recorder().RectifyCount(), 0U)
        << "a key that exists locally must not be rectified";

    dm().RectifyReadRoute("contract_rectify_absent");
    EXPECT_EQ(recorder().RectifyCount(), 1U);
}

TEST_P(DataManagerContractTest, SetRectifyCallbackReplacesAndDisables) {
    std::atomic<int> calls{0};
    dm().SetRectifyCallback(
        [&](std::string_view, std::optional<UUID>) { calls.fetch_add(1); });
    dm().RectifyReadRoute("contract_rectify_a");
    EXPECT_EQ(calls.load(), 1);
    // The original recorder callback has been replaced, not chained.
    EXPECT_EQ(recorder().RectifyCount(), 0U);

    // An empty function disables rectification without crashing.
    dm().SetRectifyCallback({});
    dm().RectifyReadRoute("contract_rectify_b");
    EXPECT_EQ(calls.load(), 1);
}

// ===========================================================================
// Lifecycle
// ===========================================================================

TEST_P(DataManagerContractTest, StopAndDestroyAreIdempotent) {
    ASSERT_TRUE(Put("contract_stop", MakePayload(64)).has_value());
    dm().Stop();
    dm().Stop();
    dm().Destroy();
    dm().Destroy();
    // The ManagerEnv destructor calls both again; reaching TearDown without a
    // crash or a hang is the assertion.
}

TEST_P(DataManagerContractTest, MutatingApisFailAfterStop) {
    const std::string key = "contract_after_stop";
    ASSERT_TRUE(Put(key, MakePayload(64)).has_value());
    dm().Stop();

    const std::string payload = MakePayload(64);
    auto put = Put("contract_after_stop_new", payload);
    EXPECT_FALSE(put.has_value()) << "Put must not succeed after Stop()";

    auto pre = dm().PreWrite("contract_after_stop_pre", 64);
    EXPECT_FALSE(pre.has_value()) << "PreWrite must not succeed after Stop()";

    auto remove_all = dm().RemoveAll();
    EXPECT_FALSE(remove_all.has_value())
        << "RemoveAll must report shutdown after Stop()";
    EXPECT_EQ(remove_all.error(), ErrorCode::SHUTTING_DOWN);
}

// Stop() rejects new work; it does not make committed data disappear. Exist
// returns a plain bool and cannot say "shutting down", so answering false for
// an intact replica would be a wrong answer, not a rejection -- and
// RectifyReadRoute would then ask Master to drop a perfectly good replica.
TEST_P(DataManagerContractTest, CommittedDataStaysVisibleAfterStop) {
    const std::string key = "contract_visible_after_stop";
    ASSERT_TRUE(Put(key, MakePayload(256)).has_value());
    const auto tier_ids_before = dm().GetReplicaTierIds(key);
    ASSERT_EQ(tier_ids_before.size(), 1U);

    dm().Stop();

    EXPECT_TRUE(dm().Exist(key));
    EXPECT_EQ(dm().GetReplicaTierIds(key), tier_ids_before);

    const size_t rectifies_before = recorder().RectifyCount();
    dm().RectifyReadRoute(key);
    EXPECT_EQ(recorder().RectifyCount(), rectifies_before)
        << "shutting down must not be mistaken for a local miss";
}

TEST_P(DataManagerContractTest, UnwaitedPutHandleDestructionDoesNotBlockStop) {
    const std::string payload = MakePayload(4096);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};

    {
        auto handle = dm().Put("contract_unwaited", slices);
        ASSERT_TRUE(handle.has_value());
        // Deliberately dropped without Wait().
    }

    const auto start = std::chrono::steady_clock::now();
    dm().Stop();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(10))
        << "Stop() must not wait forever for a handle the caller never ran";
}

TEST_P(DataManagerContractTest, WaitAfterStopCompletesWithAnError) {
    const std::string payload = MakePayload(4096);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};
    auto handle = dm().Put("contract_wait_after_stop", slices);
    ASSERT_TRUE(handle.has_value());

    dm().Stop();

    // Must terminate (not hang) and must not report success: the object was
    // never committed.
    auto waited = handle.value()->Wait();
    EXPECT_FALSE(waited.has_value());
    EXPECT_FALSE(dm().Exist("contract_wait_after_stop"));
}

// ===========================================================================
// Placement: the local write path never spills to a slower tier
// ===========================================================================

class DataManagerTieredContractTest : public DataManagerContractTest {
   protected:
    void SetUp() override {
        SKIP_UNLESS_SLOW_TIER();
        storage_path_ = "/tmp/mooncake_dm_contract_tiered_" +
                        VersionSuffix(GetParam()) + "_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::remove_all(storage_path_);
        std::filesystem::create_directories(storage_path_);
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", storage_path_.c_str(), 1);

        env_ = MakeManager(GetParam(), kSmallDramPlusStorageTiers,
                           LocalTransferMode::MEMCPY);
        ASSERT_NE(env_, nullptr);
    }

    UUID DramTierId() {
        for (const auto& view : env_->manager->GetTierViews()) {
            if (view.type == MemoryType::DRAM) return view.id;
        }
        ADD_FAILURE() << "no DRAM tier configured";
        return UUID{0, 0};
    }

    UUID StorageTierId() {
        for (const auto& view : env_->manager->GetTierViews()) {
            if (view.type == MemoryType::NVME) return view.id;
        }
        ADD_FAILURE() << "no storage tier configured";
        return UUID{0, 0};
    }
};

TEST_P(DataManagerTieredContractTest, TierViewsCoverEveryConfiguredTier) {
    auto views = env_->manager->GetTierViews();
    ASSERT_EQ(views.size(), 2U);
    EXPECT_NE(DramTierId(), StorageTierId());
    for (const auto& view : views) {
        EXPECT_EQ(view.GetName(), MakeTierSegmentName(view.id));
    }
}

TEST_P(DataManagerTieredContractTest, SlowTierIsReportedAsItsOwnMedium) {
    UUID dram = DramTierId();
    UUID storage = StorageTierId();
    for (const auto& view : env_->manager->GetTierViews()) {
        if (view.id == dram) {
            EXPECT_EQ(view.type, MemoryType::DRAM);
            EXPECT_EQ(view.capacity, 8388608U);
            EXPECT_EQ(view.priority, 100);
        } else if (view.id == storage) {
            EXPECT_EQ(view.type, MemoryType::NVME);
            EXPECT_EQ(view.capacity, 1073741824U);
            EXPECT_EQ(view.priority, 10);
        } else {
            ADD_FAILURE() << "unexpected tier " << view.GetName();
        }
    }
}

// A local write lands on the fast tier and is visible only there, even though
// a much larger slow tier is configured and free.
TEST_P(DataManagerTieredContractTest, LocalWritesLandOnTheFastTierOnly) {
    const std::string key = "contract_tiered_placement";
    const std::string payload = MakePayload(4096);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};
    auto handle = env_->manager->Put(key, slices);
    ASSERT_TRUE(handle.has_value()) << toString(handle.error());
    ASSERT_TRUE(handle.value()->Wait().has_value());

    const UUID dram = DramTierId();
    const UUID storage = StorageTierId();
    EXPECT_TRUE(env_->manager->Exist(key, dram));
    EXPECT_FALSE(env_->manager->Exist(key, storage));

    auto tier_ids = env_->manager->GetReplicaTierIds(key);
    ASSERT_EQ(tier_ids.size(), 1U);
    EXPECT_EQ(tier_ids[0], dram);

    auto query = env_->manager->Query(key);
    ASSERT_TRUE(query.has_value());
    EXPECT_EQ(query->first, dram);

    // Deleting on the tier that does not hold it must not drop the replica.
    auto wrong_tier = env_->manager->Delete(key, storage);
    EXPECT_FALSE(wrong_tier.has_value());
    EXPECT_TRUE(env_->manager->Exist(key));

    ASSERT_TRUE(env_->manager->Delete(key, dram).has_value());
    EXPECT_FALSE(env_->manager->Exist(key));
}

TEST_P(DataManagerTieredContractTest, ForEachKeyBatchReportsTheHoldingTier) {
    const std::string payload = MakePayload(1024);
    for (int i = 0; i < 4; ++i) {
        const std::string key = "contract_tiered_walk_" + std::to_string(i);
        std::vector<Slice> slices = {
            {const_cast<char*>(payload.data()), payload.size()}};
        auto handle = env_->manager->Put(key, slices);
        ASSERT_TRUE(handle.has_value());
        ASSERT_TRUE(handle.value()->Wait().has_value());
    }

    std::vector<ReplicaLocation> seen;
    env_->manager->ForEachKeyBatch([&](std::vector<ReplicaLocation>&& batch) {
        for (auto& entry : batch) seen.push_back(std::move(entry));
        return true;
    });

    // One replica each, all on the fast tier: the walk names the tier that
    // actually holds the bytes, not merely the key.
    EXPECT_EQ(seen.size(), 4U);
    for (const auto& entry : seen) {
        EXPECT_EQ(entry.tier_id, DramTierId());
        EXPECT_EQ(entry.size, payload.size());
    }
}

// An intentional divergence, recorded in executable form (exemption 3 of the
// plan's differential list). Asked to pre-write onto a tier that cannot expose
// an address, V1 silently redirects to DRAM and hands back an address for
// storage the caller did not ask for; V2 refuses.
TEST_P(DataManagerTieredContractTest,
       PreWriteOntoTheSlowTierDivergesByVersion) {
    const std::string key = "contract_tiered_prewrite";
    auto result = env_->manager->PreWrite(key, 4096, StorageTierId());

    if (GetParam() == DataManagerVersion::kV2) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
        EXPECT_FALSE(env_->manager->Exist(key));
    } else {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
        EXPECT_NE(result->remote_buffer.addr, 0U);
        ASSERT_TRUE(env_->manager->WriteRevoke(key, result->write_operation_id)
                        .has_value());
    }
}

TEST_P(DataManagerTieredContractTest, PutNeverSpillsOntoTheSlowTier) {
    // The DRAM tier is 8MiB; ask for more than that while the storage tier has
    // a whole gigabyte free.
    const size_t oversized = 32ULL * 1024 * 1024;
    std::vector<char> buffer(4096, 'x');
    std::vector<Slice> slices = {{buffer.data(), oversized}};

    auto handle = env_->manager->Put("contract_no_spill", slices);
    ASSERT_FALSE(handle.has_value())
        << "a local Put must not fall back to the slow tier";
    EXPECT_EQ(handle.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(env_->manager->Exist("contract_no_spill"));
}

// ===========================================================================
// Reclaim on allocation failure
// ===========================================================================

/**
 * @class DataManagerEvictionContractTest
 * @brief A fast tier small enough to exhaust, so the reclaim path is reached
 *        by writing rather than by reaching into the implementation.
 */
class DataManagerEvictionContractTest : public DataManagerContractTest {
   protected:
    void SetUpWith(const char* tiers_json) {
        storage_path_ = "/tmp/mooncake_dm_contract_evict_" +
                        VersionSuffix(GetParam()) + "_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::remove_all(storage_path_);
        std::filesystem::create_directories(storage_path_);
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", storage_path_.c_str(), 1);

        env_ = MakeManager(GetParam(), tiers_json, LocalTransferMode::MEMCPY);
        ASSERT_NE(env_, nullptr);
    }

    void SetUp() override { SetUpWith(kTinyDramPlusStorageTiers); }

    UUID DramTierId() {
        for (const auto& view : env_->manager->GetTierViews()) {
            if (view.type == MemoryType::DRAM) return view.id;
        }
        ADD_FAILURE() << "no DRAM tier configured";
        return UUID{0, 0};
    }

    UUID StorageTierId() {
        for (const auto& view : env_->manager->GetTierViews()) {
            if (view.type == MemoryType::NVME) return view.id;
        }
        ADD_FAILURE() << "no storage tier configured";
        return UUID{0, 0};
    }

    /** Write `count` blocks of `size` bytes, stopping at the first failure. */
    size_t FillFastTier(size_t size, size_t count, const std::string& prefix) {
        size_t written = 0;
        for (size_t i = 0; i < count; ++i) {
            const std::string payload(size, static_cast<char>('a' + (i % 26)));
            if (!Put(prefix + std::to_string(i), payload).has_value()) break;
            ++written;
        }
        Drain(dm());
        return written;
    }
};

// What a full fast tier does is a policy decision, and the two versions decide
// differently. V2 reclaims: AllocateWithPolicy drives its eviction engine and
// the write goes through. V1 asks its scheduler, and the legacy scheduler
// configured here declines, so the write fails. Neither spills to the slow
// tier -- that part is the shared contract, pinned by
// PutNeverSpillsOntoTheSlowTier.
TEST_P(DataManagerEvictionContractTest, FullFastTierReclaimsOnlyUnderV2) {
    constexpr size_t kBlock = 256 * 1024;
    const size_t written = FillFastTier(kBlock, 24, "evict_fill_");
    ASSERT_GT(written, 4U) << "the tier should have taken several blocks";

    const std::string payload(kBlock, 'z');
    auto extra = Put("evict_new_key", payload);

    if (GetParam() == DataManagerVersion::kV2) {
        ASSERT_TRUE(extra.has_value())
            << "a full fast tier must reclaim rather than refuse: "
            << toString(extra.error());
        EXPECT_TRUE(dm().Exist("evict_new_key"));
        auto got = Get("evict_new_key", payload.size());
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, payload);
    } else {
        ASSERT_FALSE(extra.has_value());
        EXPECT_EQ(extra.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    }
    // Either way the new key must not have landed on the slow tier.
    EXPECT_FALSE(dm().Exist("evict_new_key", StorageTierId()));
}

// Reclaim chooses what to drop; it must never damage what it keeps. Every key
// still visible after a run of reclaiming writes has to read back byte-exact.
TEST_P(DataManagerEvictionContractTest, SurvivorsRemainReadableAfterReclaim) {
    constexpr size_t kBlock = 256 * 1024;
    const size_t written = FillFastTier(kBlock, 24, "evict_survivor_");
    ASSERT_GT(written, 4U);

    size_t rounds_written = 0;
    for (size_t round = 0; round < 8; ++round) {
        const std::string payload(kBlock, static_cast<char>('A' + round));
        if (!Put("evict_round_" + std::to_string(round), payload).has_value()) {
            break;
        }
        ++rounds_written;
    }
    Drain(dm());
    if (rounds_written == 0) {
        GTEST_SKIP() << "this version does not reclaim in this configuration, "
                        "so there is no reclaim to check";
    }

    for (size_t i = 0; i < written; ++i) {
        const std::string key = "evict_survivor_" + std::to_string(i);
        if (!dm().Exist(key)) continue;  // reclaimed, which is allowed
        const std::string expected(kBlock, static_cast<char>('a' + (i % 26)));
        auto got = Get(key, kBlock);
        ASSERT_TRUE(got.has_value()) << key << " is visible but unreadable";
        EXPECT_EQ(*got, expected) << key << " was corrupted by reclaim";
    }
    for (size_t round = 0; round < rounds_written; ++round) {
        const std::string key = "evict_round_" + std::to_string(round);
        if (!dm().Exist(key)) continue;
        const std::string expected(kBlock, static_cast<char>('A' + round));
        auto got = Get(key, kBlock);
        ASSERT_TRUE(got.has_value()) << key;
        EXPECT_EQ(*got, expected) << key << " was corrupted by reclaim";
    }
}

// Reads have to reach the placement policy, not just the statistics counter.
// The read key is written FIRST on purpose: if reads were invisible, the LRU
// would only know commit order and would move the older -- read -- key off the
// fast tier first, which is exactly the failure this pins.
TEST_P(DataManagerEvictionContractTest, ReadKeysOutliveUnreadOnesUnderReclaim) {
    constexpr size_t kBlock = 256 * 1024;
    const std::string warm_payload = MakePayload(kBlock, 'w');
    const std::string cold_payload = MakePayload(kBlock, 'c');
    ASSERT_TRUE(Put("evict_warm", warm_payload).has_value());
    ASSERT_TRUE(Put("evict_cold", cold_payload).has_value());

    for (int i = 0; i < 12; ++i) {
        auto got = Get("evict_warm", kBlock);
        ASSERT_TRUE(got.has_value()) << toString(got.error());
    }
    Drain(dm());

    // Just enough pressure to force a handful of demotions. Filling harder
    // would push both keys off the fast tier and the comparison would say
    // nothing.
    const size_t written = FillFastTier(kBlock, 14, "evict_pressure_");
    if (written == 0) {
        GTEST_SKIP() << "this version does not reclaim in this configuration";
    }
    Drain(dm());

    // The property being pinned is the ORDER reclamation chose, and it holds
    // for both versions: the key that was read has to outlive the one that was
    // not. What differs is what happens to the loser.
    const UUID fast = DramTierId();
    EXPECT_TRUE(dm().Exist("evict_warm", fast))
        << "the read key left the fast tier, so reads are not reaching the "
           "placement policy -- its LRU only knows commit order";
    ASSERT_FALSE(dm().GetReplicaTierIds("evict_warm").empty())
        << "the key that was read 12 times was reclaimed";
    auto warm = Get("evict_warm", kBlock);
    ASSERT_TRUE(warm.has_value());
    EXPECT_EQ(*warm, warm_payload);

    if (CapabilitiesOf(GetParam()).reclaim_preserves_sole_replica) {
        // V1: the sole replica was moved down, not dropped, so it is still
        // readable and its bytes are unchanged.
        ASSERT_FALSE(dm().GetReplicaTierIds("evict_cold").empty())
            << "the cold key was lost rather than demoted";
        auto cold = Get("evict_cold", kBlock);
        ASSERT_TRUE(cold.has_value());
        EXPECT_EQ(*cold, cold_payload);
        return;
    }

    // V2: the cold key may be gone entirely -- that is the accepted cost of a
    // tier-local reclaim path. What is NOT acceptable is a half-state, so the
    // two observables have to agree: either the key still has a replica and
    // reads intact, or it has none and reads report a clean miss. A key that
    // Master still points at but nobody can read would be a bug on top of the
    // trade-off.
    const bool cold_survives = !dm().GetReplicaTierIds("evict_cold").empty();
    auto cold = Get("evict_cold", kBlock);
    if (cold_survives) {
        ASSERT_TRUE(cold.has_value()) << toString(cold.error());
        EXPECT_EQ(*cold, cold_payload);
    } else {
        ASSERT_FALSE(cold.has_value())
            << "the key has no replica left, so a read must miss cleanly "
               "rather than return data or a surprising error";
        EXPECT_EQ(cold.error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

/**
 * @class DataManagerNoEvictContractTest
 * @brief The same topology with reclaim switched off.
 */
class DataManagerNoEvictContractTest : public DataManagerEvictionContractTest {
   protected:
    void SetUp() override {
        if (GetParam() != DataManagerVersion::kV2) {
            // try_evict is a V2 configuration knob; V1 always evicts on a
            // strict DRAM allocation and offers no way to turn that off.
            GTEST_SKIP() << "try_evict=false is not configurable for "
                         << VersionSuffix(GetParam());
        }
        SetUpWith(kTinyDramNoEvictTiers);
    }
};

// With reclaim disabled, a full fast tier is simply full: no victim is chosen,
// nothing is evicted, and the write fails rather than silently landing on the
// slow tier.
TEST_P(DataManagerNoEvictContractTest, ExhaustionIsReportedWhenEvictIsOff) {
    constexpr size_t kBlock = 256 * 1024;
    const size_t written = FillFastTier(kBlock, 64, "noevict_fill_");
    ASSERT_GT(written, 4U);

    const std::string payload(kBlock, 'z');
    auto extra = Put("noevict_new_key", payload);
    ASSERT_FALSE(extra.has_value())
        << "reclaim is disabled, so exhaustion must be reported";
    EXPECT_EQ(extra.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // And nothing was thrown away to make room.
    for (size_t i = 0; i < written; ++i) {
        EXPECT_TRUE(dm().Exist("noevict_fill_" + std::to_string(i)))
            << "block " << i << " was evicted even though try_evict is false";
    }
    // Nor did it quietly land on the slow tier.
    EXPECT_FALSE(dm().Exist("noevict_new_key"));
}

INSTANTIATE_TEST_SUITE_P(
    Versions, DataManagerEvictionContractTest,
    ::testing::Values(DataManagerVersion::kV1, DataManagerVersion::kV2),
    [](const ::testing::TestParamInfo<DataManagerVersion>& info) {
        return VersionSuffix(info.param);
    });

INSTANTIATE_TEST_SUITE_P(
    Versions, DataManagerNoEvictContractTest,
    ::testing::Values(DataManagerVersion::kV1, DataManagerVersion::kV2),
    [](const ::testing::TestParamInfo<DataManagerVersion>& info) {
        return VersionSuffix(info.param);
    });

INSTANTIATE_TEST_SUITE_P(
    Versions, DataManagerContractTest,
    ::testing::Values(DataManagerVersion::kV1, DataManagerVersion::kV2),
    [](const ::testing::TestParamInfo<DataManagerVersion>& info) {
        return VersionSuffix(info.param);
    });

INSTANTIATE_TEST_SUITE_P(
    Versions, DataManagerTieredContractTest,
    ::testing::Values(DataManagerVersion::kV1, DataManagerVersion::kV2),
    [](const ::testing::TestParamInfo<DataManagerVersion>& info) {
        return VersionSuffix(info.param);
    });

}  // namespace mooncake
