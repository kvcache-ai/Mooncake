#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_capacity_provider.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"
#include "storage/distributed/distributed_storage_backend.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#ifdef __linux__
#include <cerrno>
#include <dlfcn.h>
#include <pthread.h>

namespace {
// Inject a real partial std::thread construction failure without adding a
// thread factory to the production pool. Tests arm this only around Init().
std::atomic<int> threads_before_failure{-1};
std::atomic<size_t> created_threads{0}, joined_threads{0};
}  // namespace

extern "C" int pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                              void* (*start)(void*), void* arg) noexcept {
    static const auto create = reinterpret_cast<decltype(&pthread_create)>(
        dlsym(RTLD_NEXT, "pthread_create"));
    int remaining = threads_before_failure.load();
    while (remaining >= 0) {
        if (threads_before_failure.compare_exchange_weak(
                remaining, remaining == 0 ? -1 : remaining - 1)) {
            if (remaining == 0) return EAGAIN;
            break;
        }
    }
    const int result = create(thread, attr, start, arg);
    if (result == 0) ++created_threads;
    return result;
}

extern "C" int pthread_join(pthread_t thread, void** result) {
    static const auto join = reinterpret_cast<decltype(&pthread_join)>(
        dlsym(RTLD_NEXT, "pthread_join"));
    const int status = join(thread, result);
    if (status == 0) ++joined_threads;
    return status;
}
#endif

namespace mooncake {
namespace {

class MemoryDriver final : public KvcsDriver {
   public:
    explicit MemoryDriver(std::vector<KvcsMountpointStat> stats = {})
        : stats_(std::move(stats)) {}

    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 8; }
    tl::expected<std::vector<KvcsMountpointStat>, ErrorCode>
    GetMountpointStats() override {
        return stats_;
    }

    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        ++put_calls_;
        KvcsIoResults results;
        for (const auto& request : requests) {
            if (values_.contains(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                continue;
            }
            std::string value;
            for (const auto& slice : request.slices)
                value.append(static_cast<const char*>(slice.ptr), slice.size);
            values_[request.logical_key] = std::move(value);
            results.emplace_back();
        }
        return results;
    }

    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        KvcsIoResults results;
        for (const auto& request : requests) {
            auto it = values_.find(request.logical_key);
            if (it == values_.end()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            size_t offset = 0;
            if (request.size != it->second.size()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
                continue;
            }
            for (const auto& slice : request.slices) {
                std::memcpy(slice.ptr, it->second.data() + offset, slice.size);
                offset += slice.size;
            }
            if (offset == it->second.size())
                results.emplace_back();
            else
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        KvcsIoResults results;
        for (const auto& key : keys) {
            deleted_keys_.push_back(key);
            values_.erase(key);
            results.emplace_back();
        }
        return results;
    }

    size_t put_calls() const { return put_calls_; }
    const std::vector<ObjectKey>& deleted_keys() const { return deleted_keys_; }

   private:
    size_t put_calls_ = 0;
    std::map<ObjectKey, std::string> values_;
    std::vector<KvcsMountpointStat> stats_;
    std::vector<ObjectKey> deleted_keys_;
};

// Each key has an independent gate; no device timing is used for completion
// ordering or provider read counts.
class GatedReadDriver final : public KvcsDriver {
   public:
    explicit GatedReadDriver(size_t max_batch_keys = 64, size_t workers = 2)
        : max_batch_keys_(max_batch_keys), workers_(workers) {}
    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 1024; }
    size_t MaxBatchKeys() const override { return max_batch_keys_; }
    size_t ReadConcurrency() const override { return workers_; }
    int64_t ReadDeadlineNs() const override {
        std::lock_guard lock(mutex_);
        ++deadline_calls_;
        changed_.notify_all();
        return deadline_ns_;
    }
    void SetDeadline(int64_t deadline_ns) {
        std::lock_guard lock(mutex_);
        deadline_ns_ = deadline_ns;
    }
    bool WaitForDeadlines(size_t count) {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return deadline_calls_ >= count; });
    }
    int64_t DeadlineFor(const std::string& key) {
        std::lock_guard lock(mutex_);
        return deadlines_.at(EncodeKvcsKey(key));
    }
    KvcsIoResults BatchGetUntil(std::span<const KvcsIoRequest> requests,
                                int64_t deadline_ns) override {
        {
            std::lock_guard lock(mutex_);
            for (const auto& request : requests)
                deadlines_[request.logical_key] = deadline_ns;
        }
        return BatchGet(requests);
    }
    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        return KvcsIoResults(requests.size());
    }
    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        return KvcsIoResults(keys.size());
    }
    void Add(const std::string& key, std::string value, bool hold = true,
             bool fail = false) {
        entries_.emplace(EncodeKvcsKey(key),
                         Entry{std::move(value), hold, fail});
    }
    bool Wait(const std::string& key, size_t reads = 1) {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5), [&] {
            return entries_.at(EncodeKvcsKey(key)).reads >= reads;
        });
    }
    bool WaitCopied(const std::string& key) {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5), [&] {
            return entries_.at(EncodeKvcsKey(key)).copied;
        });
    }
    void Release(const std::string& key) {
        std::lock_guard lock(mutex_);
        entries_.at(EncodeKvcsKey(key)).released = true;
        changed_.notify_all();
    }
    void ReleaseAll() {
        std::lock_guard lock(mutex_);
        for (auto& [key, entry] : entries_) entry.released = true;
        changed_.notify_all();
    }
    size_t Reads(const std::string& key) {
        std::lock_guard lock(mutex_);
        return entries_.at(EncodeKvcsKey(key)).reads;
    }
    size_t MaxActive() {
        std::lock_guard lock(mutex_);
        return max_active_;
    }
    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        struct Finish {
            GatedReadDriver& owner;
            ~Finish() {
                std::lock_guard lock(owner.mutex_);
                --owner.active_;
            }
        } finish{*this};
        {
            std::lock_guard lock(mutex_);
            max_active_ = std::max(max_active_, ++active_);
        }
        KvcsIoResults results(requests.size());
        for (size_t i = 0; i < requests.size(); ++i) {
            const auto& request = requests[i];
            std::unique_lock lock(mutex_);
            auto& entry = entries_.at(request.logical_key);
            ++entry.reads;
            changed_.notify_all();
            changed_.wait(lock, [&] { return !entry.hold || entry.released; });
            if (entry.fail) throw std::runtime_error("source failure");
            const auto value = entry.value;
            lock.unlock();
            if (request.size != value.size()) {
                results[i] = tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            size_t offset = 0;
            for (const auto& slice : request.slices) {
                std::memcpy(slice.ptr, value.data() + offset, slice.size);
                offset += slice.size;
            }
            lock.lock();
            entry.copied = true;
            changed_.notify_all();
        }
        return results;
    }

   private:
    struct Entry {
        std::string value;
        bool hold, fail;
        bool released = false;
        size_t reads = 0;
        bool copied = false;
    };
    mutable std::mutex mutex_;
    mutable std::condition_variable changed_;
    std::map<ObjectKey, Entry> entries_;
    std::map<ObjectKey, int64_t> deadlines_;
    int64_t deadline_ns_ = INT64_MAX;
    mutable size_t deadline_calls_ = 0;
    size_t active_ = 0, max_active_ = 0;
    const size_t max_batch_keys_, workers_;
};

struct ReleaseGatedReads {
    GatedReadDriver* driver;
    ~ReleaseGatedReads() { driver->ReleaseAll(); }
};

class ReadCompletions {
   public:
    void Complete(size_t index, const tl::expected<size_t, ErrorCode>& result) {
        std::lock_guard lock(mutex_);
        EXPECT_TRUE(results_.emplace(index, result).second);
        changed_.notify_all();
    }
    bool Wait(size_t index) {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return results_.contains(index); });
    }
    tl::expected<size_t, ErrorCode> Result(size_t index) {
        std::lock_guard lock(mutex_);
        return results_.at(index);
    }

   private:
    std::mutex mutex_;
    std::condition_variable changed_;
    std::map<size_t, tl::expected<size_t, ErrorCode>> results_;
};

TEST(KvcsObjectStorageAdapterTest, SmallBatchCompletesBeforeUnrelatedSlowKey) {
    auto driver = std::make_unique<GatedReadDriver>();
    auto* source = driver.get();
    source->Add("fast", "fast");
    source->Add("slow", "slow");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<char, 4> fast{}, slow{};
    ReadCompletions completed;
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion(
            {{"fast", fast.data(), 4}, {"slow", slow.data(), 4}},
            [&](size_t index, const auto& result) {
                completed.Complete(index, result);
            });
    });
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    source->Release("fast");
    ASSERT_TRUE(completed.Wait(0));
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_EQ(completed.Result(0).value(), 4u);
    EXPECT_EQ(std::string(fast.data(), fast.size()), "fast");
    source->ReleaseAll();
    read.get();
    ASSERT_TRUE(completed.Result(1));
    EXPECT_EQ(source->Reads("fast"), 1u);
    EXPECT_EQ(source->Reads("slow"), 1u);
    EXPECT_LE(source->MaxActive(), 2u);
}

TEST(KvcsObjectStorageAdapterTest, QueuedSingleKeyExpiresWithoutDriverRead) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"first", "second", "queued"})
        source->Add(key, "data");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<char, 4> first{}, second{}, queued{'x', 'x', 'x', 'x'};
    ReadCompletions completed;
    std::future<void> active, pending;
    ReleaseGatedReads release{source};
    active = std::async(std::launch::async, [&] {
        adapter.GetBatch(
            {{"first", first.data(), 4}, {"second", second.data(), 4}});
    });
    ASSERT_TRUE(source->Wait("first"));
    ASSERT_TRUE(source->Wait("second"));
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    source->SetDeadline(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            deadline.time_since_epoch())
                            .count());
    pending = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion({{"queued", queued.data(), 4}},
                                       [&](size_t i, const auto& result) {
                                           completed.Complete(i, result);
                                       });
    });
    ASSERT_TRUE(source->WaitForDeadlines(2));
    std::this_thread::sleep_until(deadline);
    source->ReleaseAll();
    active.get();
    pending.get();
    const auto result = completed.Result(0);
    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_EQ(result.error(), ErrorCode::RPC_TIMEOUT);
    }
    EXPECT_EQ(source->Reads("queued"), 0u);
    EXPECT_EQ(std::string(queued.data(), 4), "xxxx");
}

TEST(KvcsObjectStorageAdapterTest, QueuedReadKeepsOriginalDeadline) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"first", "second", "queued"})
        source->Add(key, "data");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<char, 4> first{}, second{}, queued{};
    std::future<void> active, pending;
    ReleaseGatedReads release{source};
    active = std::async(std::launch::async, [&] {
        adapter.GetBatch(
            {{"first", first.data(), 4}, {"second", second.data(), 4}});
    });
    ASSERT_TRUE(source->Wait("first"));
    ASSERT_TRUE(source->Wait("second"));
    const int64_t deadline =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (std::chrono::steady_clock::now() + std::chrono::seconds(30))
                .time_since_epoch())
            .count();
    source->SetDeadline(deadline);
    pending = std::async(std::launch::async, [&] {
        EXPECT_TRUE(adapter.Get("queued", queued.data(), 4));
    });
    ASSERT_TRUE(source->WaitForDeadlines(2));
    source->ReleaseAll();
    active.get();
    pending.get();
    EXPECT_EQ(source->DeadlineFor("queued"), deadline);
    EXPECT_EQ(source->Reads("queued"), 1u);
    EXPECT_EQ(std::string(queued.data(), 4), "data");
}

TEST(KvcsObjectStorageAdapterTest, RefillsBeforeBlockedCompletionCallback) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"fast", "slow", "next"}) source->Add(key, "data");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 4>, 3> output{};
    std::promise<void> callback_entered, release_callback;
    auto callback_gate = release_callback.get_future();
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion({{"fast", output[0].data(), 4},
                                        {"slow", output[1].data(), 4},
                                        {"next", output[2].data(), 4}},
                                       [&](size_t i, const auto&) {
                                           if (i == 0) {
                                               callback_entered.set_value();
                                               callback_gate.wait();
                                           }
                                       });
    });
    // Release the callback before any assertion can unwind the read future.
    const bool started = source->Wait("fast") && source->Wait("slow");
    source->Release("fast");
    const bool entered =
        callback_entered.get_future().wait_for(std::chrono::seconds(5)) ==
        std::future_status::ready;
    const bool refilled = source->Wait("next");
    release_callback.set_value();
    EXPECT_TRUE(started);
    EXPECT_TRUE(entered);
    EXPECT_TRUE(refilled);
    source->ReleaseAll();
    read.get();
}

TEST(KvcsObjectStorageAdapterTest, RefilledChunkUsesOriginalBatchDeadline) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"fast", "slow", "next"}) source->Add(key, "data");
    const int64_t deadline =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (std::chrono::steady_clock::now() + std::chrono::seconds(30))
                .time_since_epoch())
            .count();
    source->SetDeadline(deadline);
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 4>, 3> output{};
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatch({{"fast", output[0].data(), 4},
                          {"slow", output[1].data(), 4},
                          {"next", output[2].data(), 4}});
    });
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    source->SetDeadline(0);
    source->Release("fast");
    ASSERT_TRUE(source->Wait("next"));
    for (const auto* key : {"fast", "slow", "next"})
        EXPECT_EQ(source->DeadlineFor(key), deadline);
    source->ReleaseAll();
    read.get();
}

TEST(KvcsObjectStorageAdapterTest, RefilledChunkExpiresWithBatchDeadline) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"fast", "slow", "next"}) source->Add(key, "data");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    source->SetDeadline(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            deadline.time_since_epoch())
                            .count());
    std::array<std::array<char, 4>, 3> output{};
    ReadCompletions completed;
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion({{"fast", output[0].data(), 4},
                                        {"slow", output[1].data(), 4},
                                        {"next", output[2].data(), 4}},
                                       [&](size_t i, const auto& result) {
                                           completed.Complete(i, result);
                                       });
    });
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    std::this_thread::sleep_until(deadline);
    source->Release("fast");
    ASSERT_TRUE(completed.Wait(2));
    ASSERT_FALSE(completed.Result(2));
    EXPECT_EQ(completed.Result(2).error(), ErrorCode::RPC_TIMEOUT);
    EXPECT_EQ(source->Reads("next"), 0u);
    EXPECT_EQ(output[2], (std::array<char, 4>{}));
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    source->ReleaseAll();
    read.get();
    EXPECT_TRUE(completed.Result(0));
    EXPECT_TRUE(completed.Result(1));
}

TEST(KvcsObjectStorageAdapterTest, CallbackExceptionDrainsRefilledRead) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    for (const auto* key : {"fast", "slow", "next"}) source->Add(key, "data");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 4>, 3> output{};
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion(
            {{"fast", output[0].data(), 4},
             {"slow", output[1].data(), 4},
             {"next", output[2].data(), 4}},
            [&](size_t i, const auto&) {
                if (i == 0) throw std::runtime_error("consumer failure");
            });
    });
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    source->Release("fast");
    ASSERT_TRUE(source->Wait("next"));
    source->Release("slow");
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    source->Release("next");
    EXPECT_THROW(read.get(), std::runtime_error);
    EXPECT_EQ(std::string(output[2].data(), 4), "data");
}

#ifdef __linux__
TEST(KvcsObjectStorageAdapterTest,
     PartialWorkerCreationFailureReturnsErrorAndRetries) {
    auto driver = std::make_unique<GatedReadDriver>(1, 4);
    KvcsObjectStorageAdapter adapter(std::move(driver));
    const auto created = created_threads.load(), joined = joined_threads.load();
    struct Reset {
        ~Reset() { threads_before_failure = -1; }
    } reset;
    threads_before_failure = 2;
    auto result = adapter.Init();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(created_threads.load() - created, 2u);
    EXPECT_EQ(joined_threads.load() - joined, 2u);
    ASSERT_TRUE(adapter.Init());
}
#endif

TEST(KvcsObjectStorageAdapterTest, CompletedWindowSlotRefillsAndErrorsDrain) {
    auto driver = std::make_unique<GatedReadDriver>(1);
    auto* source = driver.get();
    source->Add("failed", "fail", true, true);
    source->Add("slow", "slow");
    source->Add("next", "next", false);
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 4>, 3> output{};
    ReadCompletions completed;
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion({{"failed", output[0].data(), 4},
                                        {"slow", output[1].data(), 4},
                                        {"next", output[2].data(), 4}},
                                       [&](size_t index, const auto& result) {
                                           completed.Complete(index, result);
                                       });
    });
    ASSERT_TRUE(source->Wait("failed"));
    ASSERT_TRUE(source->Wait("slow"));
    source->Release("failed");
    ASSERT_TRUE(completed.Wait(0));
    ASSERT_FALSE(completed.Result(0));
    EXPECT_EQ(completed.Result(0).error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_TRUE(source->Wait("next"));
    ASSERT_TRUE(completed.Wait(2));
    EXPECT_EQ(std::string(output[2].data(), 4), "next");
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    source->ReleaseAll();
    read.get();
    EXPECT_TRUE(completed.Result(1));
    for (const auto* key : {"failed", "slow", "next"})
        EXPECT_EQ(source->Reads(key), 1u);
    EXPECT_LE(source->MaxActive(), 2u);
}

TEST(KvcsObjectStorageAdapterTest, CallbackExceptionDrainsOtherSourceReads) {
    auto driver = std::make_unique<GatedReadDriver>();
    auto* source = driver.get();
    source->Add("fast", "fast");
    source->Add("slow", "slow");
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::array<char, 4> fast{}, slow{};
    ReadCompletions completed;
    std::future<void> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        adapter.GetBatchWithCompletion(
            {{"fast", fast.data(), 4}, {"slow", slow.data(), 4}},
            [&](size_t index, const auto& result) {
                completed.Complete(index, result);
                if (index == 0) throw std::runtime_error("consumer failure");
            });
    });
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    source->Release("fast");
    ASSERT_TRUE(completed.Wait(0));
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    source->ReleaseAll();
    EXPECT_THROW(read.get(), std::runtime_error);
    EXPECT_EQ(std::string(slow.data(), 4), "slow");
}

// Observe the DFS-to-adapter handoff without changing KVCS source I/O. The
// follower's unrelated key reaches this handoff only after it joined fast.
class ObservedKvcsAdapter final : public ObjectStorageAdapter {
   public:
    explicit ObservedKvcsAdapter(std::unique_ptr<KvcsDriver> driver)
        : adapter_(std::move(driver)) {}
    bool WaitForAdmission() {
        return admitted_.wait_for(std::chrono::seconds(5)) ==
               std::future_status::ready;
    }
    tl::expected<void, ErrorCode> Init() override { return adapter_.Init(); }
    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> data) override {
        return adapter_.Put(key, data);
    }
    tl::expected<void, ErrorCode> PutV(const std::string& key, const iovec* iov,
                                       int count) override {
        return adapter_.PutV(key, iov, count);
    }
    tl::expected<size_t, ErrorCode> Get(const std::string& key, void* data,
                                        size_t size) override {
        return adapter_.Get(key, data, size);
    }
    size_t NativeBatchCalls() const { return native_calls_.load(); }
    size_t CompletionBatchCalls() const { return completion_calls_.load(); }
    bool WaitForFastCallback() {
        return fast_completed_.wait_for(std::chrono::seconds(5)) ==
               std::future_status::ready;
    }
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override {
        ++native_calls_;
        return adapter_.GetBatch(requests);
    }
    std::vector<tl::expected<size_t, ErrorCode>> GetBatchWithCompletion(
        const std::vector<ObjectGetRequest>& requests,
        const ReadCompletion& completed) override {
        ++completion_calls_;
        if (requests.size() == 1 && requests.front().logical_key == "next")
            admission_.set_value();
        return adapter_.GetBatchWithCompletion(
            requests, [&](size_t index, const auto& result) {
                if (completed) completed(index, result);
                if (index == 1 && requests[index].logical_key == "fast")
                    fast_completion_.set_value();
            });
    }
    tl::expected<bool, ErrorCode> Exists(const std::string& key) override {
        return adapter_.Exists(key);
    }
    tl::expected<void, ErrorCode> Delete(const std::string& key) override {
        return adapter_.Delete(key);
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        return adapter_.ListKeys();
    }
    const char* GetName() const override { return adapter_.GetName(); }

   private:
    KvcsObjectStorageAdapter adapter_;
    std::promise<void> admission_;
    std::future<void> admitted_{admission_.get_future()};
    std::promise<void> fast_completion_;
    std::future<void> fast_completed_{fast_completion_.get_future()};
    std::atomic<size_t> native_calls_{0}, completion_calls_{0};
};

TEST(KvcsObjectStorageAdapterTest, DfsPublishesValidatedScatterBeforeSlowKey) {
    auto driver = std::make_unique<GatedReadDriver>();
    auto* source = driver.get();
    source->Add("fast", "fast");
    source->Add("slow", "slow");
    source->Add("next", "next", false);
    DistributedStorageConfig distributed;
    distributed.enable_health_check = false;
    auto adapter = std::make_unique<ObservedKvcsAdapter>(std::move(driver));
    auto* observed = adapter.get();
    DistributedStorageBackend backend(FileStorageConfig{}, distributed, nullptr,
                                      std::move(adapter));
    ASSERT_TRUE(backend.Init());
    const DistributedFSDescriptor descriptor{"", 0, 4, 4, 0};
    std::array<char, 4> first{}, slow{}, follower{}, next{};
    auto request = [&](const char* key, std::vector<Slice> slices,
                       ReplicaID id) {
        return DfsReadRequest{.key = key,
                              .descriptor = descriptor,
                              .slices = std::move(slices),
                              .source_replica_id = id};
    };
    std::vector<std::future<ObjectStorageIoResults>> reads;
    ReleaseGatedReads release{source};
    reads.push_back(std::async(std::launch::async, [&] {
        return backend.BatchRead(
            {request("fast", {{first.data(), 2}, {first.data() + 2, 2}}, 41),
             request("slow", {{slow.data(), 4}}, 42)});
    }));
    ASSERT_TRUE(source->Wait("fast"));
    ASSERT_TRUE(source->Wait("slow"));
    reads.push_back(std::async(std::launch::async, [&] {
        return backend.BatchRead(
            {request("fast", {{follower.data(), 1}, {follower.data() + 1, 3}},
                     41),
             request("next", {{next.data(), 4}}, 43)});
    }));
    // This handoff proves the caller joined fast before releasing that source.
    ASSERT_TRUE(observed->WaitForAdmission());
    source->Release("fast");
    ASSERT_EQ(reads[1].wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(std::string(follower.data(), 4), "fast");
    EXPECT_EQ(std::string(next.data(), 4), "next");
    EXPECT_EQ(reads[0].wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    source->ReleaseAll();
    for (auto& read : reads)
        for (const auto& result : read.get()) EXPECT_TRUE(result);
    EXPECT_EQ(source->Reads("fast"), 1u);
    EXPECT_EQ(source->Reads("slow"), 1u);
    EXPECT_EQ(source->Reads("next"), 1u);
}

TEST(KvcsObjectStorageAdapterTest, DfsScatterAliasKeepsNativeBatchCopyOrder) {
    auto driver = std::make_unique<GatedReadDriver>();
    auto* source = driver.get();
    source->Add("slow", "aaaa");
    source->Add("fast", "bbbb", false);
    auto adapter = std::make_unique<ObservedKvcsAdapter>(std::move(driver));
    auto* observed = adapter.get();
    DistributedStorageConfig distributed;
    distributed.enable_health_check = false;
    DistributedStorageBackend backend(FileStorageConfig{}, distributed, nullptr,
                                      std::move(adapter));
    ASSERT_TRUE(backend.Init());
    const DistributedFSDescriptor descriptor{"", 0, 4, 4, 0};
    std::array<char, 4> shared{'i', 'n', 'i', 't'};
    std::future<ObjectStorageIoResults> read;
    ReleaseGatedReads release{source};
    read = std::async(std::launch::async, [&] {
        return backend.BatchRead(
            {{"slow", descriptor, {{shared.data(), 4}}, 0, false, 41},
             {"fast",
              descriptor,
              {{shared.data(), 2}, {shared.data() + 2, 2}},
              0,
              false,
              42}});
    });
    ASSERT_TRUE(source->Wait("slow"));
    ASSERT_TRUE(source->WaitCopied("fast"));
    // The provider sees shared/staging as disjoint. DFS must use its original
    // destinations to choose the whole-batch path before staging hides aliases.
    if (observed->CompletionBatchCalls() != 0) {
        // Synchronize the broken early-copy path too, so this assertion reads
        // X after B's scatter callback rather than racing that callback.
        ASSERT_TRUE(observed->WaitForFastCallback());
    }
    EXPECT_EQ(observed->NativeBatchCalls(), 1u);
    EXPECT_EQ(observed->CompletionBatchCalls(), 0u);
    EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_EQ(std::string(shared.data(), 4), "init");
    source->ReleaseAll();
    for (const auto& result : read.get()) EXPECT_TRUE(result);
    EXPECT_EQ(std::string(shared.data(), 4), "bbbb");
    EXPECT_EQ(source->Reads("slow"), 1u);
    EXPECT_EQ(source->Reads("fast"), 1u);
}

class ScopedEfcEnvironment {
   public:
    ScopedEfcEnvironment() {
        for (const char* name : {"KVCS_MOUNTPOINTS_JSON"}) {
            const char* value = std::getenv(name);
            saved_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            unsetenv(name);
        }
    }
    ~ScopedEfcEnvironment() {
        for (const auto& [name, value] : saved_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

   private:
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

KvcsObjectStorageAdapter MakeAdapter() {
    return KvcsObjectStorageAdapter(std::make_unique<MemoryDriver>());
}

TEST(KvcsCapacityProviderTest, SelectsConfiguredMountpoint) {
    std::vector<KvcsMountpointStat> stats{
        {.index = 1, .id = "other", .total_bytes = 200, .used_bytes = 50},
        {.index = 2, .id = "target-mp", .total_bytes = 100, .used_bytes = 25}};
    KvcsCapacityProvider provider(
        2, "target-mp", std::make_unique<MemoryDriver>(std::move(stats)));
    auto capacity = provider.GetCapacity();
    ASSERT_TRUE(capacity.has_value()) << capacity.error();
    EXPECT_TRUE(capacity->enabled);
    EXPECT_EQ(capacity->used_bytes, 25u);
    EXPECT_EQ(capacity->capacity_bytes, 100u);
}

TEST(KvcsCapacityProviderTest, RejectsUnknownOrInconsistentCapacity) {
    auto get_capacity = [](std::vector<KvcsMountpointStat> stats) {
        KvcsCapacityProvider provider(
            2, "target-mp", std::make_unique<MemoryDriver>(std::move(stats)));
        return provider.GetCapacity();
    };
    EXPECT_FALSE(get_capacity({}).has_value());
    EXPECT_FALSE(
        get_capacity(
            {{.index = 2, .id = "other", .total_bytes = 100, .used_bytes = 25}})
            .has_value());
    EXPECT_FALSE(get_capacity({{.index = 2,
                                .id = "target-mp",
                                .total_bytes = 0,
                                .used_bytes = 0}})
                     .has_value());
    EXPECT_FALSE(get_capacity({{.index = 2,
                                .id = "target-mp",
                                .total_bytes = 100,
                                .used_bytes = 101}})
                     .has_value());
}

TEST(KvcsCapacityProviderTest, DeletesLogicalKeyWithSharedEncoding) {
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsCapacityProvider provider(1, "target-mp", std::move(driver));
    EXPECT_TRUE(provider.DeleteObject("key"));
    auto invalid = provider.DeleteObject(std::string(300, 'x'));
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    ASSERT_EQ(driver_ptr->deleted_keys().size(), 1u);
    EXPECT_EQ(driver_ptr->deleted_keys()[0], EncodeKvcsKey("key"));
}

TEST(KvcsObjectStorageAdapterTest, PutGetAndRemove) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());

    std::string value = "12345678";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);

    std::string output(value.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, value);

    ASSERT_TRUE(adapter.Delete("key"));
    auto missing = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, RejectsOversizeBeforeProviderWrite) {
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::string value(9, 'x');
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"large", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, DeleteKeepsPerKeyErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"valid", &put_iov, 1}})[0]);

    const std::array<std::string, 2> keys{"valid", std::string(300, 'x')};
    auto removed = adapter.DeleteBatch(keys);
    ASSERT_EQ(removed.size(), keys.size());
    EXPECT_TRUE(removed[0]);
    ASSERT_FALSE(removed[1]);
    EXPECT_EQ(removed[1].error(), ErrorCode::INVALID_PARAMS);
    std::string output(value.size(), '\0');
    auto missing = adapter.GetBatch({{"valid", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, GetRejectsMasterSizeMismatch) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);
    std::string output(value.size() - 1, '\0');
    auto result = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST(KvcsObjectStorageAdapterTest, PutOverwritesAndKeepsErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string first = "one";
    iovec first_iov{first.data(), first.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &first_iov, 1}})[0]);
    std::string replacement = "new";
    iovec replacement_iov{replacement.data(), replacement.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &replacement_iov, 1}})[0]);
    std::string output(replacement.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, replacement);

    std::string second = "two";
    iovec invalid_iov{nullptr, second.size()};
    iovec second_iov{second.data(), second.size()};
    const std::array<ObjectPutRequest, 2> requests{
        ObjectPutRequest{"second", &invalid_iov, 1},
        ObjectPutRequest{"second", &second_iov, 1}};
    auto invalid = adapter.PutBatch(
        std::vector<ObjectPutRequest>(requests.begin(), requests.end()));
    ASSERT_FALSE(invalid[0]);
    EXPECT_EQ(invalid[0].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(invalid[1]);
}

TEST(KvcsObjectStorageAdapterTest, RejectsInvalidRequests) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "x";
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, DefaultEfcTargetRejectsMultipleMountpoints) {
    ScopedEfcEnvironment environment;
    auto target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_index, 1u);
    EXPECT_EQ(target->mountpoint_id, "kvcachestore-default");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one","subpath":"/tenant"}]})",
           1);
    target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_id, "one");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one"},{"mountPointID":"two"}]})",
           1);
    auto multiple = LoadKvcsEfcTarget("");
    ASSERT_FALSE(multiple);
    EXPECT_EQ(multiple.error(), ErrorCode::INVALID_PARAMS);
}

#ifdef MOONCAKE_KVCS_TEST_NO_SDK
TEST(KvcsObjectStorageAdapterTest, MissingSdkIsExplicitlyUnsupported) {
    ScopedEfcEnvironment environment;
    KvcsObjectStorageAdapter adapter;
    auto result = adapter.Init();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
}
#endif

#ifdef MOONCAKE_KVCS_TEST_SDK
TEST(KvcsObjectStorageAdapterTest, OfficialMockSingleValueRoundTrip) {
    if (!std::getenv("MOONCAKE_KVCS_TEST_MOCK")) GTEST_SKIP();
    auto created = CreateKvcsLowLevelDriver(1);
    ASSERT_TRUE(created);
    auto& driver = **created;
    ASSERT_TRUE(driver.Init());
    auto stats = driver.GetMountpointStats();
    ASSERT_TRUE(stats);
    ASSERT_FALSE(stats->empty());
    EXPECT_GT(stats->front().total_bytes, 0u);
    std::string value = "value";
    const KvcsIoRequest put{.logical_key = "pr1-mock-roundtrip",
                            .slices = {{value.data(), value.size()}},
                            .size = value.size()};
    ASSERT_TRUE(driver.BatchPut({&put, 1})[0]);

    std::string output(value.size(), '\0');
    const KvcsIoRequest get{.logical_key = put.logical_key,
                            .slices = {{output.data(), output.size()}},
                            .size = output.size()};
    ASSERT_TRUE(driver.BatchGet({&get, 1})[0]);
    EXPECT_EQ(output, value);
    const std::array<ObjectKey, 1> keys{put.logical_key};
    ASSERT_TRUE(driver.BatchDelete(keys)[0]);
    auto after_delete = driver.BatchGet({&get, 1});
    ASSERT_FALSE(after_delete[0]);
    EXPECT_EQ(after_delete[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}
#endif

}  // namespace
}  // namespace mooncake
