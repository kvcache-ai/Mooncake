#include "http_metadata_cleanup_worker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "types.h"

namespace mooncake::testing {

TEST(HttpMetadataCleanupWorkerTest,
     AttemptsBothMetadataKeysAndIsolatesFailures) {
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_set<std::string> attempted_keys;

    HttpMetadataCleanupWorker worker(
        [&](const std::string& key) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                attempted_keys.insert(key);
            }
            cv.notify_one();
            if (key.find("/ram/") != std::string::npos) {
                throw std::runtime_error("injected removal failure");
            }
            return true;
        },
        "mooncake/cleanup-test/");
    ASSERT_TRUE(worker.Start());
    worker.Enqueue({generate_uuid(), {"segment-a"}});

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                                [&] { return attempted_keys.size() == 2; }));
    }
    EXPECT_EQ(attempted_keys, (std::unordered_set<std::string>{
                                  "mooncake/cleanup-test/ram/segment-a",
                                  "mooncake/cleanup-test/rpc_meta/segment-a"}));
    worker.Stop();
}

TEST(HttpMetadataCleanupWorkerTest, CreateReportsMissingBackend) {
    auto worker = HttpMetadataCleanupWorker::Create(nullptr);
    ASSERT_FALSE(worker.has_value());
    EXPECT_NE(worker.error().find("no local or remote"), std::string::npos);
}

TEST(HttpMetadataCleanupWorkerTest, CreateRejectsNonHttpRemoteBackend) {
    auto worker =
        HttpMetadataCleanupWorker::Create(nullptr, "redis://metadata:6379");
    ASSERT_FALSE(worker.has_value());
    EXPECT_NE(worker.error().find("not an HTTP endpoint"), std::string::npos);
}

}  // namespace mooncake::testing
