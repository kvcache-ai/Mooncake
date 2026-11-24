#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <optional>
#include <string>

namespace mooncake {

class FilereadWorkerPoolTestPeer {
   public:
    static size_t WorkerCount(const FilereadWorkerPool& pool) {
        return pool.workers_.size();
    }
};

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

TEST(FilereadWorkerPoolTest, AcceptsTypedTrailingWhitespaceAndCaches) {
    google::InitGoogleLogging("FilereadWorkerPoolTest");
    ScopedFilereadWorkersEnv env("2 ");
    std::shared_ptr<StorageBackend> backend;
    // Inspect only this pool: linked runtimes (e.g. Go via etcd) can create
    // unrelated process threads asynchronously, including during destruction.
    {
        FilereadWorkerPool pool(backend);
        EXPECT_EQ(FilereadWorkerPoolTestPeer::WorkerCount(pool), 2u);
    }
    ASSERT_EQ(setenv("MC_FILEREAD_WORKERS", "3", 1), 0);
    {
        FilereadWorkerPool pool(backend);
        EXPECT_EQ(FilereadWorkerPoolTestPeer::WorkerCount(pool), 2u);
    }
    google::ShutdownGoogleLogging();
}

}  // namespace
}  // namespace mooncake
