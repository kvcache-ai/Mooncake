#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
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
    explicit ShortIoPosixAdapter(bool batch_io) : batch_io_(batch_io) {}
    bool SupportsBatchIo() const override { return batch_io_; }

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

   private:
    bool batch_io_;
};

class RecordingPosixAdapter : public CountingPosixAdapter {
   public:
    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override {
        auto fd = CountingPosixAdapter::OpenExistingFile(path);
        if (fd) peak_open = std::max(peak_open, ++open_now);
        return fd;
    }
    tl::expected<void, ErrorCode> CloseFile(int fd) override {
        --open_now;
        return CountingPosixAdapter::CloseFile(fd);
    }
    std::vector<tl::expected<size_t, ErrorCode>> BatchWriteAt(
        std::span<const FdIoRequest> requests) override {
        write_batches.push_back(requests.size());
        return CountingPosixAdapter::BatchWriteAt(requests);
    }
    std::vector<tl::expected<size_t, ErrorCode>> BatchReadAt(
        std::span<const FdIoRequest> requests) override {
        read_batches.push_back(requests.size());
        return CountingPosixAdapter::BatchReadAt(requests);
    }

    std::vector<size_t> write_batches;
    std::vector<size_t> read_batches;
    int open_now = 0;
    int peak_open = 0;
};

// Exercise the batching capability without requiring a 3FS mount.
class BatchRecordingPosixAdapter : public RecordingPosixAdapter {
   public:
    bool SupportsBatchIo() const override { return true; }
};

class ObservedReadPosixAdapter : public RecordingPosixAdapter {
   public:
    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override {
        if (fail_next_open) {
            fail_next_open = false;
            ++opens;
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        return RecordingPosixAdapter::OpenExistingFile(path);
    }
    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int count,
                                           int64_t offset) override {
        ++reads;
        if (count == 0 || reads == fail_read_call) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        auto result = PosixFsAdapter::ReadAt(fd, iov, count, offset);
        if (reads == 1 && after_first_read) after_first_read();
        return result;
    }
    tl::expected<void, ErrorCode> CloseFile(int fd) override {
        auto result = RecordingPosixAdapter::CloseFile(fd);
        if (fail_next_close) {
            fail_next_close = false;
            return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
        }
        return result;
    }

    int reads = 0;
    int fail_read_call = -1;
    bool fail_next_open = false;
    bool fail_next_close = false;
    std::function<void()> after_first_read;
};

class ZeroProgressPosixAdapter : public CountingPosixAdapter {
   public:
    tl::expected<size_t, ErrorCode> WriteAt(int, const iovec*, int,
                                            int64_t) override {
        return size_t{0};
    }
};

class ImmutableBucketShortIoTest : public ::testing::TestWithParam<bool> {};
class ImmutableBucketBatchIoTest : public ::testing::TestWithParam<bool> {};

TEST_P(ImmutableBucketShortIoTest, BufferedScatterIoUsesRequestScopedHandles) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto descriptor = allocator.Allocate("key", 11);
    ASSERT_TRUE(descriptor);

    auto adapter = std::make_unique<ShortIoPosixAdapter>(GetParam());
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

INSTANTIATE_TEST_SUITE_P(ScalarAndBatch, ImmutableBucketShortIoTest,
                         ::testing::Bool());

TEST(ImmutableBucketBackendTest, PosixReadBatchReusesBucketHandle) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto first = allocator.Allocate("first", 4);
    auto second = allocator.Allocate("second", 4);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_EQ(first->file_path, second->file_path);

    auto adapter = std::make_unique<RecordingPosixAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());

    std::string first_value = "abcd";
    std::string second_value = "wxyz";
    auto writes =
        backend.BatchWrite({{"first", *first, {{first_value.data(), 4}}},
                            {"second", *second, {{second_value.data(), 4}}}});
    ASSERT_EQ(writes.size(), 2u);
    ASSERT_TRUE(writes[0]);
    ASSERT_TRUE(writes[1]);
    EXPECT_TRUE(recorded->write_batches.empty());
    EXPECT_EQ(recorded->opens, 2);
    EXPECT_EQ(recorded->closes, 2);
    EXPECT_EQ(recorded->peak_open, 1);

    std::string first_output(4, '\0');
    std::string second_output(4, '\0');
    auto reads =
        backend.BatchRead({{"first", *first, {{first_output.data(), 4}}},
                           {"second", *second, {{second_output.data(), 4}}}});
    ASSERT_EQ(reads.size(), 2u);
    ASSERT_TRUE(reads[0]);
    ASSERT_TRUE(reads[1]);
    EXPECT_EQ(first_output, first_value);
    EXPECT_EQ(second_output, second_value);
    EXPECT_TRUE(recorded->read_batches.empty());
    EXPECT_EQ(recorded->opens, 3);
    EXPECT_EQ(recorded->closes, 3);
    EXPECT_EQ(recorded->peak_open, 1);
    EXPECT_EQ(recorded->open_now, 0);

    const int opens_before = recorded->opens;
    reads =
        backend.BatchRead({{"first", *first, {{first_output.data(), 4}}},
                           {"invalid", *first, {{nullptr, 4}}},
                           {"second", *second, {{second_output.data(), 4}}}});
    ASSERT_EQ(reads.size(), 3u);
    EXPECT_TRUE(reads[0]);
    ASSERT_FALSE(reads[1]);
    EXPECT_EQ(reads[1].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(reads[2]);
    EXPECT_EQ(recorded->opens - opens_before, 2);
    EXPECT_EQ(recorded->opens, recorded->closes);
}

TEST(ImmutableBucketBackendTest, FailedOpenDoesNotFailTheNextRequest) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto first = allocator.Allocate("first", 4);
    auto second = allocator.Allocate("second", 4);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_EQ(first->file_path, second->file_path);
    auto adapter = std::make_unique<ObservedReadPosixAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());
    std::string value = "data";
    for (const auto& result :
         backend.BatchWrite({{"first", *first, {{value.data(), 4}}},
                             {"second", *second, {{value.data(), 4}}}})) {
        ASSERT_TRUE(result);
    }
    recorded->fail_next_open = true;
    std::string output(4, '\0');
    auto results =
        backend.BatchRead({{"first", *first, {{output.data(), 4}}},
                           {"second", *second, {{output.data(), 4}}}});
    ASSERT_EQ(results.size(), 2u);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_OPEN_FAIL);
    EXPECT_TRUE(results[1]);
    EXPECT_EQ(output, value);
    EXPECT_EQ(recorded->opens, recorded->closes + 1);
    EXPECT_EQ(recorded->open_now, 0);
}

TEST(ImmutableBucketBackendTest, ScalarShortReadsPreserveRequestOrder) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    std::vector<std::string> values{std::string(11, 'A'), std::string(11, 'B'),
                                    std::string(11, 'C')};
    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < values.size(); ++i) {
        auto descriptor =
            allocator.Allocate(std::to_string(i), values[i].size());
        ASSERT_TRUE(descriptor);
        descriptors.push_back(*descriptor);
    }
    ASSERT_EQ(descriptors[0].file_path, descriptors[1].file_path);
    ASSERT_NE(descriptors[0].file_path, descriptors[2].file_path);
    auto adapter = std::make_unique<ShortIoPosixAdapter>(false);
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());
    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < values.size(); ++i) {
        writes.push_back({std::to_string(i),
                          descriptors[i],
                          {{values[i].data(), values[i].size()}}});
    }
    for (const auto& result : backend.BatchWrite(writes)) ASSERT_TRUE(result);
    std::string output(11, '\0');
    auto windows = backend.BatchRead(
        {{"first", descriptors[0], {{output.data(), output.size()}}},
         {"second", descriptors[1], {{output.data() + 7, 4}}, 0, true}});
    ASSERT_EQ(windows.size(), 2u);
    EXPECT_TRUE(windows[0]);
    EXPECT_TRUE(windows[1]);
    EXPECT_EQ(output, std::string(7, 'A') + std::string(4, 'B'));
    const int opens_before = recorded->opens;
    std::vector<DfsReadRequest> reads;
    for (size_t i : {0, 1, 2, 0}) {
        reads.push_back({std::to_string(i),
                         descriptors[i],
                         {{output.data(), output.size()}}});
    }
    auto results = backend.BatchRead(reads);
    ASSERT_EQ(results.size(), reads.size());
    for (const auto& result : results) EXPECT_TRUE(result);
    EXPECT_EQ(output, values[0]);
    EXPECT_EQ(recorded->opens - opens_before, 3);
    EXPECT_EQ(recorded->opens, recorded->closes);
}

TEST(ImmutableBucketBackendTest, SharedClosePreservesSpecificReadFailures) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto adapter = std::make_unique<ObservedReadPosixAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());
    std::string value = "data";
    std::vector<DfsWriteRequest> writes;
    std::vector<DfsReadRequest> reads;
    std::vector<std::string> outputs(3, std::string(4, '\0'));
    for (size_t i = 0; i < outputs.size(); ++i) {
        auto descriptor = allocator.Allocate(std::to_string(i), value.size());
        ASSERT_TRUE(descriptor);
        writes.push_back(
            {std::to_string(i), *descriptor, {{value.data(), value.size()}}});
        reads.push_back({std::to_string(i),
                         *descriptor,
                         {{outputs[i].data(), outputs[i].size()}}});
    }
    for (const auto& result : backend.BatchWrite(writes)) ASSERT_TRUE(result);
    recorded->fail_read_call = 1;
    recorded->fail_next_close = true;
    auto results = backend.BatchRead(reads);
    ASSERT_EQ(results.size(), 3u);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::FILE_INVALID_HANDLE);
    EXPECT_TRUE(results[2]);
    EXPECT_EQ(outputs[2], value);
    EXPECT_EQ(recorded->opens, recorded->closes);
    EXPECT_EQ(recorded->open_now, 0);
    EXPECT_EQ(recorded->peak_open, 1);
}

TEST(ImmutableBucketBackendTest, EmptyWindowDoesNotIssueScalarRead) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto descriptor = allocator.Allocate("key", 4);
    ASSERT_TRUE(descriptor);
    auto adapter = std::make_unique<ObservedReadPosixAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());
    auto results = backend.BatchRead({{"key", *descriptor, {}, 4, true}});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0]);
    EXPECT_EQ(recorded->reads, 0);
    EXPECT_EQ(recorded->opens, 1);
    EXPECT_EQ(recorded->closes, 1);
}

TEST(ImmutableBucketBackendTest, SharedHandleEndsAtReadCallBoundary) {
    for (bool replace : {false, true}) {
        SCOPED_TRACE(replace);
        TempDir dir;
        const auto config = MakeConfig(dir);
        ImmutableBucketAllocator allocator;
        ASSERT_TRUE(allocator.Init(config));
        auto first = allocator.Allocate("first", 4);
        auto second = allocator.Allocate("second", 4);
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
        ASSERT_EQ(first->file_path, second->file_path);
        auto adapter = std::make_unique<ObservedReadPosixAdapter>();
        auto* recorded = adapter.get();
        FileStorageConfig file_config;
        DistributedStorageBackend backend(file_config, config,
                                          std::move(adapter));
        ASSERT_TRUE(backend.Init());
        std::string old_value = "old!";
        std::string new_value = "new!";
        for (const auto& result : backend.BatchWrite(
                 {{"first", *first, {{old_value.data(), 4}}},
                  {"second", *second, {{old_value.data(), 4}}}})) {
            ASSERT_TRUE(result);
        }
        recorded->after_first_read = [&] {
            ASSERT_TRUE(std::filesystem::remove(first->file_path));
            if (!replace) return;
            PosixFsAdapter setup;
            ASSERT_TRUE(setup.PreallocateFile(first->file_path,
                                              config.bucket_capacity));
            auto fd = setup.OpenExistingFile(first->file_path);
            ASSERT_TRUE(fd);
            iovec iov{new_value.data(), new_value.size()};
            ASSERT_TRUE(setup.WriteAt(*fd, &iov, 1, first->offset));
            ASSERT_TRUE(setup.WriteAt(*fd, &iov, 1, second->offset));
            ASSERT_TRUE(setup.CloseFile(*fd));
        };
        std::string first_out(4, '\0'), second_out(4, '\0');
        std::vector<DfsReadRequest> reads{
            {"first", *first, {{first_out.data(), 4}}},
            {"second", *second, {{second_out.data(), 4}}}};
        auto results = backend.BatchRead(reads);
        ASSERT_EQ(results.size(), 2u);
        EXPECT_TRUE(results[0]);
        EXPECT_TRUE(results[1]);
        EXPECT_EQ(first_out, old_value);
        EXPECT_EQ(second_out, old_value);
        results = backend.BatchRead(reads);
        ASSERT_EQ(results.size(), 2u);
        if (replace) {
            EXPECT_TRUE(results[0]);
            EXPECT_TRUE(results[1]);
            EXPECT_EQ(first_out, new_value);
            EXPECT_EQ(second_out, new_value);
        } else {
            for (const auto& result : results) {
                ASSERT_FALSE(result);
                EXPECT_EQ(result.error(), ErrorCode::FILE_NOT_FOUND);
            }
            EXPECT_FALSE(std::filesystem::exists(first->file_path));
        }
        EXPECT_EQ(recorded->opens, recorded->closes);
        EXPECT_EQ(recorded->open_now, 0);
    }
}

TEST(ImmutableBucketBackendTest, BatchOpensEachBucketOnceAndSubmitsTogether) {
    TempDir dir;
    const auto config = MakeConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto first = allocator.Allocate("first", 4);
    auto second = allocator.Allocate("second", 4);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_EQ(first->file_path, second->file_path);

    auto adapter = std::make_unique<BatchRecordingPosixAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());

    std::string first_value = "abcd";
    std::string second_value = "wxyz";
    auto writes =
        backend.BatchWrite({{"first", *first, {{first_value.data(), 4}}},
                            {"second", *second, {{second_value.data(), 4}}}});
    ASSERT_EQ(writes.size(), 2u);
    ASSERT_TRUE(writes[0]);
    ASSERT_TRUE(writes[1]);
    EXPECT_EQ(recorded->opens, 1);
    EXPECT_EQ(recorded->closes, 1);
    EXPECT_EQ(recorded->write_batches, std::vector<size_t>{2});

    std::string first_output(4, '\0');
    std::string second_output(4, '\0');
    auto reads =
        backend.BatchRead({{"first", *first, {{first_output.data(), 4}}},
                           {"second", *second, {{second_output.data(), 4}}}});
    ASSERT_EQ(reads.size(), 2u);
    ASSERT_TRUE(reads[0]);
    ASSERT_TRUE(reads[1]);
    EXPECT_EQ(first_output, first_value);
    EXPECT_EQ(second_output, second_value);
    EXPECT_EQ(recorded->opens, 2);
    EXPECT_EQ(recorded->closes, 2);
    EXPECT_EQ(recorded->read_batches, std::vector<size_t>{2});
}

TEST_P(ImmutableBucketBatchIoTest, BoundsAndClosesOpenBucketHandles) {
    constexpr size_t kMaxOpen =
        DistributedStorageBackend::kMaxOpenBucketsPerBatch;
    constexpr size_t kObjects = kMaxOpen + 1;
    TempDir dir;
    auto config = MakeConfig(dir);
    config.max_bucket_count = kObjects;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    // Each object fills a bucket, so every request targets its own bucket.
    std::vector<std::string> values;
    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < kObjects; ++i) {
        auto descriptor = allocator.Allocate("key" + std::to_string(i),
                                             config.bucket_capacity);
        ASSERT_TRUE(descriptor);
        descriptors.push_back(*descriptor);
        values.emplace_back(config.bucket_capacity,
                            static_cast<char>('a' + i % 26));
    }

    std::unique_ptr<RecordingPosixAdapter> adapter;
    if (GetParam()) {
        adapter = std::make_unique<BatchRecordingPosixAdapter>();
    } else {
        adapter = std::make_unique<RecordingPosixAdapter>();
    }
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());

    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < kObjects; ++i) {
        writes.push_back({"key" + std::to_string(i),
                          descriptors[i],
                          {{values[i].data(), values[i].size()}}});
    }
    auto write_results = backend.BatchWrite(writes);
    ASSERT_EQ(write_results.size(), kObjects);
    for (const auto& result : write_results) EXPECT_TRUE(result);
    EXPECT_EQ(recorded->write_batches, GetParam()
                                           ? (std::vector<size_t>{kMaxOpen, 1})
                                           : std::vector<size_t>{});
    EXPECT_EQ(recorded->opens, static_cast<int>(kObjects));
    EXPECT_EQ(recorded->closes, static_cast<int>(kObjects));
    EXPECT_EQ(recorded->peak_open, GetParam() ? static_cast<int>(kMaxOpen) : 1);

    std::vector<std::string> outputs(kObjects,
                                     std::string(config.bucket_capacity, '\0'));
    std::vector<DfsReadRequest> reads;
    for (size_t i = 0; i < kObjects; ++i) {
        reads.push_back({"key" + std::to_string(i),
                         descriptors[i],
                         {{outputs[i].data(), outputs[i].size()}}});
    }
    auto read_results = backend.BatchRead(reads);
    ASSERT_EQ(read_results.size(), kObjects);
    for (const auto& result : read_results) EXPECT_TRUE(result);
    EXPECT_EQ(outputs, values);
    EXPECT_EQ(recorded->read_batches, GetParam()
                                          ? (std::vector<size_t>{kMaxOpen, 1})
                                          : std::vector<size_t>{});
    EXPECT_EQ(recorded->peak_open, GetParam() ? static_cast<int>(kMaxOpen) : 1);
    EXPECT_EQ(recorded->opens, recorded->closes);
    EXPECT_EQ(recorded->open_now, 0);
}

INSTANTIATE_TEST_SUITE_P(ScalarAndBatch, ImmutableBucketBatchIoTest,
                         ::testing::Bool());

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

    ASSERT_TRUE(
        backend.BatchWrite({{"key", *descriptor, {{value.data(), 4}}}})[0]);
    std::string output(4, '\0');
    result = backend.BatchRead(
        {{"key", *descriptor, {{output.data(), output.size()}}},
         {"escaped", escaped, {{output.data(), output.size()}}},
         {"mismatched", mismatched, {{output.data(), output.size()}}},
         {"key", *descriptor, {{output.data(), output.size()}}}});
    ASSERT_EQ(result.size(), 4u);
    EXPECT_TRUE(result[0]);
    ASSERT_FALSE(result[1]);
    EXPECT_EQ(result[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_FALSE(result[2]);
    EXPECT_EQ(result[2].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(result[3]);
    EXPECT_EQ(output, value);

    ASSERT_TRUE(std::filesystem::remove(descriptor->file_path));
    result = backend.BatchWrite({{"key", *descriptor, {{value.data(), 4}}}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_NOT_FOUND);
    EXPECT_FALSE(std::filesystem::exists(descriptor->file_path));
    result = backend.BatchRead({{"key", *descriptor, {{output.data(), 4}}}});
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
