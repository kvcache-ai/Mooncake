#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "config/distributed_storage_config.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage_backend.h"

namespace mooncake {
namespace {

class TempDir {
   public:
    TempDir() {
        static std::atomic<uint64_t> sequence{0};
        path_ =
            std::filesystem::temp_directory_path() /
            ("mooncake-bucket-backend-" +
             std::to_string(sequence.fetch_add(1)) + "-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }
    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }
    std::string string() const { return path_.string(); }

   private:
    std::filesystem::path path_;
};

DistributedStorageConfig MakeConfig(const TempDir& dir) {
    DistributedStorageConfig config;
    config.fsdir = dir.string();
    config.fs_adapter_type = "posix";
    config.allocator_type = "bucket";
    config.alignment = 4096;
    config.bucket_capacity = 8192;
    config.max_bucket_count = 4;
    return config;
}

class CountingPosixAdapter : public PosixFsAdapter {
   public:
    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override {
        ++opens;
        return PosixFsAdapter::OpenExistingFile(path);
    }
    tl::expected<void, ErrorCode> CloseFile(int fd) override {
        ++closes;
        return PosixFsAdapter::CloseFile(fd);
    }

    int opens = 0;
    int closes = 0;
};

class ShortIoPosixAdapter : public CountingPosixAdapter {
   public:
    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        if (iovcnt <= 0) return size_t{0};
        iovec chunk = iov[0];
        chunk.iov_len = std::min<size_t>(chunk.iov_len, 3);
        return PosixFsAdapter::WriteAt(fd, &chunk, 1, offset);
    }

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override {
        if (iovcnt <= 0) return size_t{0};
        iovec chunk = iov[0];
        chunk.iov_len = std::min<size_t>(chunk.iov_len, 5);
        return PosixFsAdapter::ReadAt(fd, &chunk, 1, offset);
    }
};

class ZeroProgressPosixAdapter : public CountingPosixAdapter {
   public:
    tl::expected<size_t, ErrorCode> WriteAt(int, const iovec*, int,
                                            int64_t) override {
        return size_t{0};
    }
};

TEST(ImmutableBucketBackendTest, BufferedScatterIoUsesRequestScopedHandles) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto descriptor = allocator.Allocate("key", 11);
    ASSERT_TRUE(descriptor);

    auto adapter = std::make_unique<ShortIoPosixAdapter>();
    auto* counts = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());

    std::string first = "hello ";
    std::string second = "world";
    auto writes = backend.BatchWrite(
        {{"key",
          *descriptor,
          {{first.data(), first.size()}, {second.data(), second.size()}}}});
    ASSERT_EQ(writes.size(), 1u);
    ASSERT_TRUE(writes[0]);

    std::string output(11, '\0');
    auto reads = backend.BatchRead(
        {{"key",
          *descriptor,
          {{output.data(), 4}, {output.data() + 4, output.size() - 4}}}});
    ASSERT_EQ(reads.size(), 1u);
    ASSERT_TRUE(reads[0]);
    EXPECT_EQ(output, "hello world");
    EXPECT_EQ(counts->opens, 2);
    EXPECT_EQ(counts->closes, 2);
}

TEST(ImmutableBucketBackendTest, RejectsEscapesMismatchAndMissingFile) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto descriptor = allocator.Allocate("key", 4);
    ASSERT_TRUE(descriptor);

    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config,
                                      std::make_unique<PosixFsAdapter>());
    ASSERT_TRUE(backend.Init());
    std::string value = "data";

    auto escaped = *descriptor;
    escaped.file_path = dir.string() + "/../bucket_000000.data";
    auto result = backend.BatchWrite({{"key", escaped, {{value.data(), 4}}}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);

    auto mismatched = *descriptor;
    mismatched.shard_idx = 1;
    result = backend.BatchWrite({{"key", mismatched, {{value.data(), 4}}}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);

    ASSERT_TRUE(std::filesystem::remove(descriptor->file_path));
    result = backend.BatchWrite({{"key", *descriptor, {{value.data(), 4}}}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_NOT_FOUND);
    EXPECT_FALSE(std::filesystem::exists(descriptor->file_path));
}

TEST(ImmutableBucketBackendTest, ZeroProgressIsReportedAsWriteFailure) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto descriptor = allocator.Allocate("key", 4);
    ASSERT_TRUE(descriptor);

    FileStorageConfig file_config;
    DistributedStorageBackend backend(
        file_config, config, std::make_unique<ZeroProgressPosixAdapter>());
    ASSERT_TRUE(backend.Init());
    std::string value = "data";
    auto result =
        backend.BatchWrite({{"key", *descriptor, {{value.data(), 4}}}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_WRITE_FAIL);
}

}  // namespace
}  // namespace mooncake
