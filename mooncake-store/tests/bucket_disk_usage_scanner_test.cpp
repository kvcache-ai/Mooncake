#include "storage_backend.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <cstdlib>
#include <fstream>
#include <future>
#include <stdexcept>

namespace mooncake::test {

class BucketStorageBackendTestPeer {
   public:
    static void SetScanHook(BucketStorageBackend& backend,
                            std::function<void()> hook) {
        {
            std::lock_guard<std::mutex> lock(backend.disk_scan_mutex_);
            backend.disk_scan_hook_for_test_ = std::move(hook);
            backend.disk_scan_requested_ = true;
        }
        backend.disk_scan_cv_.notify_all();
    }

    static int64_t Usage(BucketStorageBackend& backend) {
        SharedMutexLocker lock(&backend.mutex_, shared_lock);
        return backend.ActualDiskBytesUsedLocked();
    }

    static bool WaitForUsage(BucketStorageBackend& backend,
                             const std::function<bool(int64_t)>& predicate) {
        std::unique_lock<std::mutex> lock(backend.disk_scan_mutex_);
        return backend.disk_scan_cv_.wait_for(
            lock, std::chrono::seconds(3),
            [&] { return predicate(Usage(backend)); });
    }

    static bool HasScanner(BucketStorageBackend& backend) {
        return backend.disk_scan_thread_.joinable();
    }
};

namespace {
struct ReleaseScanOnExit {
    std::promise<void>& promise;
    bool released = false;
    void Release() {
        if (!released) {
            released = true;
            promise.set_value();
        }
    }
    ~ReleaseScanOnExit() { Release(); }
};
}  // namespace

class BucketDiskUsageScannerTest : public ::testing::Test {
   protected:
    std::string path;
    FileStorageConfig file_config;
    BucketBackendConfig bucket_config;

    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() /
                        "mooncake-disk-scan-XXXXXX")
                           .string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        path = pattern;
        file_config.storage_filepath = path;
        file_config.use_uring = false;
        bucket_config.eviction_policy = BucketEvictionPolicy::FIFO;
        bucket_config.max_total_size = 64 * 1024 * 1024;
        bucket_config.max_physical_bytes = 64 * 1024 * 1024;
        bucket_config.disk_scan_cache_ms = 20;
    }

    void TearDown() override { std::filesystem::remove_all(path); }

    static bool Put(BucketStorageBackend& backend, const std::string& key,
                    std::string& value) {
        auto result = backend.BatchOffload(
            {{key, {{value.data(), value.size()}}}},
            [](const auto&, auto&) { return ErrorCode::OK; });
        return result.has_value();
    }

    int64_t DiskBytes() const {
        int64_t total = 0;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(path)) {
            struct stat st;
            if (::stat(entry.path().c_str(), &st) == 0)
                total += st.st_blocks * 512;
        }
        return total;
    }
};

TEST_F(BucketDiskUsageScannerTest, BlockedScanDoesNotBlockReadOrWrite) {
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<bool> once{false};
    BucketStorageBackend backend(file_config, bucket_config);
    ReleaseScanOnExit release_on_exit{release};
    ASSERT_TRUE(backend.Init());
    std::string initial(4096, 'a');
    ASSERT_TRUE(Put(backend, "initial", initial));
    BucketStorageBackendTestPeer::SetScanHook(backend, [&] {
        if (!once.exchange(true)) entered.set_value();
        released.wait();
    });
    ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);

    std::string added(1024 * 1024, 'b');
    auto foreground = std::async(std::launch::async, [&] {
        std::string restored(initial.size(), '\0');
        std::unordered_map<std::string, Slice> objects{
            {"initial", {restored.data(), restored.size()}}};
        auto read = backend.BatchLoad(objects);
        return read.has_value() && restored == initial &&
               Put(backend, "added", added);
    });
    const bool completed = foreground.wait_for(std::chrono::seconds(2)) ==
                           std::future_status::ready;
    if (completed) {
        // The published snapshot still predates this write: admission must
        // already cover its allocated blocks while traversal is suspended.
        EXPECT_GE(BucketStorageBackendTestPeer::Usage(backend), DiskBytes());
    }
    release_on_exit.Release();
    EXPECT_TRUE(completed) << "Directory scan held a foreground metadata lock";
    EXPECT_TRUE(foreground.get());
    BucketStorageBackendTestPeer::SetScanHook(backend, {});
    // The scan began before the new write. Its publication must not discard
    // that admission, regardless of whether traversal saw the new files.
    EXPECT_GE(BucketStorageBackendTestPeer::Usage(backend), DiskBytes());
}

TEST_F(BucketDiskUsageScannerTest, InitialScanIncludesNestedExternalFiles) {
    std::filesystem::create_directory(path + "/external");
    std::ofstream(path + "/external/data") << std::string(16384, 'x');
    bucket_config.max_physical_bytes = 1024;
    BucketStorageBackend backend(file_config, bucket_config);
    ASSERT_TRUE(backend.Init());
    EXPECT_GE(BucketStorageBackendTestPeer::Usage(backend), 16384);
    std::string value(1024, 'v');
    EXPECT_FALSE(Put(backend, "over-cap", value));
}

TEST_F(BucketDiskUsageScannerTest, ScanFailureFailsClosedAndRecovers) {
    BucketStorageBackend backend(file_config, bucket_config);
    ASSERT_TRUE(backend.Init());
    BucketStorageBackendTestPeer::SetScanHook(
        backend, [] { throw std::runtime_error("injected scan failure"); });
    ASSERT_TRUE(BucketStorageBackendTestPeer::WaitForUsage(
        backend,
        [&](auto n) { return n == bucket_config.max_physical_bytes; }));
    std::string value(1024, 'v');
    EXPECT_FALSE(Put(backend, "failed-scan", value));
    BucketStorageBackendTestPeer::SetScanHook(backend, {});
    ASSERT_TRUE(BucketStorageBackendTestPeer::WaitForUsage(
        backend, [&](auto n) { return n < bucket_config.max_physical_bytes; }));
    EXPECT_TRUE(Put(backend, "recovered", value));
}

TEST_F(BucketDiskUsageScannerTest, DisabledPhysicalCapNeedsNoScanner) {
    bucket_config.max_physical_bytes = 0;
    BucketStorageBackend backend(file_config, bucket_config);
    ASSERT_TRUE(backend.Init());
    EXPECT_FALSE(BucketStorageBackendTestPeer::HasScanner(backend));
}

}  // namespace mooncake::test
