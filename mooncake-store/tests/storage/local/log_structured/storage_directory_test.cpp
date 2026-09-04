#include "storage/local/log_structured/storage_directory.h"

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class DirectoryTempPath {
   public:
    DirectoryTempPath() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake-log-directory-test-" + std::to_string(getpid()) +
                 "-" + std::to_string(id));
    }

    ~DirectoryTempPath() { std::filesystem::remove_all(path_); }

    const std::filesystem::path& path() const { return path_; }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

TEST(LogStructuredDirectoryTest, CreatesAndReusesStableIdentity) {
    DirectoryTempPath temp;
    StorageIdentity identity;
    {
        auto directory = StorageDirectory::Open(temp.path().string());
        ASSERT_TRUE(directory.has_value());
        identity = (*directory)->identity();
        EXPECT_NE(identity, StorageIdentity{});
        EXPECT_TRUE(std::filesystem::exists(temp.path() / "IDENTITY"));
    }
    auto reopened = StorageDirectory::Open(temp.path().string());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ((*reopened)->identity(), identity);
}

TEST(LogStructuredDirectoryTest, RejectsConcurrentMount) {
    DirectoryTempPath temp;
    auto first = StorageDirectory::Open(temp.path().string());
    ASSERT_TRUE(first.has_value());
    auto second = StorageDirectory::Open(temp.path().string());
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), StorageDirectoryError::kAlreadyMounted);
}

TEST(LogStructuredDirectoryTest, RejectsUnknownNonEmptyDirectory) {
    DirectoryTempPath temp;
    std::filesystem::create_directories(temp.path());
    std::ofstream(temp.path() / "foreign.data") << "foreign";

    auto directory = StorageDirectory::Open(temp.path().string());
    ASSERT_FALSE(directory.has_value());
    EXPECT_EQ(directory.error(), StorageDirectoryError::kUnrecognizedFormat);
}

TEST(LogStructuredDirectoryTest, RejectsCorruptIdentity) {
    DirectoryTempPath temp;
    std::filesystem::create_directories(temp.path());
    std::ofstream(temp.path() / "IDENTITY", std::ios::binary) << "bad";

    auto directory = StorageDirectory::Open(temp.path().string());
    ASSERT_FALSE(directory.has_value());
    EXPECT_EQ(directory.error(), StorageDirectoryError::kCorruptIdentity);
}

}  // namespace
}  // namespace mooncake::logstructured
