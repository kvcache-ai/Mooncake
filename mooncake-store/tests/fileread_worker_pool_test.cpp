#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>

namespace mooncake {
namespace {

class ScopedFilereadWorkersEnv {
   public:
    explicit ScopedFilereadWorkersEnv(const char* value) {
        if (const char* old = std::getenv("MC_FILEREAD_WORKERS")) {
            original_ = old;
        }
        EXPECT_EQ(setenv("MC_FILEREAD_WORKERS", value, 1), 0);
    }

    ~ScopedFilereadWorkersEnv() {
        if (original_) {
            EXPECT_EQ(setenv("MC_FILEREAD_WORKERS", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_FILEREAD_WORKERS"), 0);
        }
    }

   private:
    std::optional<std::string> original_;
};

#ifdef __linux__
size_t ProcessThreadCount() {
    size_t count = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator("/proc/self/task")) {
        (void)entry;
        ++count;
    }
    return count;
}

bool WaitForProcessThreadCount(size_t expected) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        if (ProcessThreadCount() == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

size_t StableProcessThreadCount() {
    size_t baseline = ProcessThreadCount();
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const size_t now = ProcessThreadCount();
        if (now == baseline) {
            return baseline;
        }
        baseline = now;
    }
    return baseline;
}

TEST(FilereadWorkerPoolTest, AcceptsTypedTrailingWhitespaceAndCaches) {
    google::InitGoogleLogging("FilereadWorkerPoolTest");
    ScopedFilereadWorkersEnv env("2 ");
    std::shared_ptr<StorageBackend> backend;
    // glog can spawn helper threads after InitGoogleLogging; settle first.
    const size_t baseline = StableProcessThreadCount();
    {
        FilereadWorkerPool pool(backend);
        ASSERT_TRUE(WaitForProcessThreadCount(baseline + 2));
    }
    ASSERT_TRUE(WaitForProcessThreadCount(baseline));
    ASSERT_EQ(setenv("MC_FILEREAD_WORKERS", "3", 1), 0);
    {
        FilereadWorkerPool pool(backend);
        // worker_count is cached on first construction; still 2, not 3.
        ASSERT_TRUE(WaitForProcessThreadCount(baseline + 2));
    }
    EXPECT_TRUE(WaitForProcessThreadCount(baseline));
    google::ShutdownGoogleLogging();
}
#endif

}  // namespace
}  // namespace mooncake
