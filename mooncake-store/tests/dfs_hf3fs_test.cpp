#include <fcntl.h>
#include <gtest/gtest.h>
#include <limits.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "hf3fs/hf3fs.h"
#include "storage/distributed/shard_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/hf3fs_adapter.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage_backend.h"

namespace mooncake::test {
namespace {

constexpr const char* kDefaultRoot = "/mnt/3fs/mooncake_test";

bool IsHf3fsPath(const std::string& path) {
    char mount_point[PATH_MAX] = {};
    int ret = hf3fs_extract_mount_point(mount_point, sizeof(mount_point),
                                        path.c_str());
    return ret > 0 && ret <= static_cast<int>(sizeof(mount_point));
}

std::string DfsRootFromEnv() {
    const char* root = std::getenv("MOONCAKE_DFS_ROOT_DIR");
    return root && root[0] != '\0' ? std::string(root)
                                   : std::string(kDefaultRoot);
}

std::optional<std::string> ExistingAncestor(std::filesystem::path path) {
    while (!path.empty()) {
        if (std::filesystem::exists(path)) return path.string();
        path = path.parent_path();
    }
    return std::nullopt;
}

class Hf3fsTestDir {
   public:
    Hf3fsTestDir() {
        static std::atomic<int64_t> counter{0};
        root_ = DfsRootFromEnv();
        path_ = std::filesystem::path(root_) /
                ("dfs_hf3fs_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(++counter));
        path_str_ = path_.string();
    }

    ~Hf3fsTestDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    void CreateOrSkip() {
        auto ancestor = ExistingAncestor(path_);
        if (!ancestor.has_value()) {
            GTEST_SKIP() << "no existing parent for test root: " << root_;
        }
        if (!IsHf3fsPath(*ancestor)) {
            GTEST_SKIP() << "test root is not under an hf3fs mount: " << root_;
        }

        std::filesystem::create_directories(path_);
        if (!IsHf3fsPath(path_str_)) {
            GTEST_SKIP() << "test directory is not on an hf3fs mount: "
                         << path_str_;
        }
    }

    const std::string& path() const { return path_str_; }

    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

   private:
    std::string root_;
    std::filesystem::path path_;
    std::string path_str_;
};

class Hf3fsAdapterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::make_unique<Hf3fsTestDir>();
        test_dir_->CreateOrSkip();
    }

    void TearDown() override { test_dir_.reset(); }

    std::unique_ptr<Hf3fsTestDir> test_dir_;
};

class RecordingHf3fsAdapter : public Hf3fsAdapter {
   public:
    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override {
        auto fd = Hf3fsAdapter::OpenExistingFile(path);
        if (fd) peak_open = std::max(peak_open, ++open_now);
        return fd;
    }

    tl::expected<void, ErrorCode> CloseFile(int fd) override {
        auto result = Hf3fsAdapter::CloseFile(fd);
        if (result) --open_now;
        return result;
    }

    std::vector<tl::expected<size_t, ErrorCode>> BatchWriteAt(
        std::span<const FdIoRequest> requests) override {
        write_batches.push_back(requests.size());
        return Hf3fsAdapter::BatchWriteAt(requests);
    }

    std::vector<tl::expected<size_t, ErrorCode>> BatchReadAt(
        std::span<const FdIoRequest> requests) override {
        read_batches.push_back(requests.size());
        return Hf3fsAdapter::BatchReadAt(requests);
    }

    std::vector<size_t> write_batches;
    std::vector<size_t> read_batches;
    int open_now = 0;
    int peak_open = 0;
};

}  // namespace

TEST_F(Hf3fsAdapterTest, WriteAtReadAtThroughUsrbio) {
    Hf3fsAdapter adapter;
    ASSERT_TRUE(adapter.Init(test_dir_->path()).has_value());

    const std::string path = test_dir_->file("adapter_smoke.data");
    ASSERT_TRUE(adapter.PreallocateFile(path, 4096).has_value());

    auto fd = adapter.OpenFile(path);
    ASSERT_TRUE(fd.has_value());

    std::array<char, 128> write_buf;
    std::array<char, 128> read_buf{};
    write_buf.fill('Q');

    iovec wiov{write_buf.data(), write_buf.size()};
    auto written = adapter.WriteAt(*fd, &wiov, 1, 100);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, write_buf.size());

    iovec riov{read_buf.data(), read_buf.size()};
    auto read = adapter.ReadAt(*fd, &riov, 1, 100);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, read_buf.size());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), write_buf.size()),
              0);

    EXPECT_TRUE(adapter.CloseFile(*fd).has_value());
    EXPECT_TRUE(adapter.Shutdown().has_value());
}

TEST_F(Hf3fsAdapterTest, BatchIoAcrossRingAndSharedBufferCapacity) {
    Hf3fsAdapter adapter;
    ASSERT_TRUE(adapter.Init(test_dir_->path()));
    const Hf3fsConfig config;
    const size_t count = config.ior_entries + 3;
    std::vector<std::string> values(count);
    std::vector<std::array<iovec, 3>> iovs(count);
    std::vector<FdIoRequest> requests(count);
    std::vector<int> fds;
    // Two files, with multiple positional requests sharing each registered fd.
    for (int i = 0; i < 2; ++i) {
        auto fd =
            adapter.OpenFile(test_dir_->file("batch_" + std::to_string(i)));
        ASSERT_TRUE(fd);
        fds.push_back(*fd);
    }
    int64_t offset = 7;
    for (size_t i = 0; i < count; ++i) {
        values[i].assign(i == 0 ? config.iov_size + 4097 : 4097, 'A' + i % 26);
        iovs[i] = {{{values[i].data(), 31},
                    {nullptr, 0},
                    {values[i].data() + 31, values[i].size() - 31}}};
        requests[i] = {fds[i % fds.size()], iovs[i].data(), 3, offset};
        offset += values[i].size() + 13;
    }
    const auto writes = adapter.BatchWriteAt(requests);
    ASSERT_EQ(writes.size(), count);
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(writes[i]);
        EXPECT_EQ(*writes[i], values[i].size());
        std::fill(values[i].begin(), values[i].end(), '?');
    }
    const auto reads = adapter.BatchReadAt(requests);
    ASSERT_EQ(reads.size(), count);
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(reads[i]);
        EXPECT_EQ(*reads[i], values[i].size());
        EXPECT_EQ(values[i], std::string(values[i].size(), 'A' + i % 26));
    }
    for (int fd : fds) EXPECT_TRUE(adapter.CloseFile(fd));
    EXPECT_TRUE(adapter.Shutdown());
}

TEST_F(Hf3fsAdapterTest, DistributedBackendBatchWriteAndRead) {
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = test_dir_->path();

    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = test_dir_->path();
    distributed_config.fs_adapter_type = "hf3fs";
    distributed_config.shard_count = 2;
    distributed_config.shard_capacity = 1024 * 1024;
    distributed_config.alignment = 4096;
    distributed_config.single_tenant = true;

    // The master prepares shard files before publishing their descriptors.
    ShardAllocator allocator;
    ASSERT_TRUE(allocator.Init(distributed_config).has_value());

    DistributedStorageBackend backend(file_config, distributed_config,
                                      std::make_unique<Hf3fsAdapter>());
    ASSERT_TRUE(backend.Init().has_value());

    constexpr size_t kObjects = 4;
    constexpr size_t kObjectSize = 4096;
    std::vector<std::string> values;
    values.reserve(kObjects);
    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < kObjects; ++i) {
        const auto key = "hf3fs_backend_key_" + std::to_string(i);
        auto descriptor = allocator.Allocate(key, kObjectSize);
        ASSERT_TRUE(descriptor.has_value());
        values.emplace_back(kObjectSize, static_cast<char>('A' + i));
        writes.push_back({key,
                          *descriptor,
                          {{values.back().data(), 1024},
                           {values.back().data() + 1024, kObjectSize - 1024}}});
    }
    auto write_results = backend.BatchWrite(writes);
    ASSERT_EQ(write_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) ASSERT_TRUE(write_results[i]) << i;

    std::vector<std::string> outputs(kObjects, std::string(kObjectSize, '\0'));
    std::vector<DfsReadRequest> reads;
    for (size_t i = 0; i < kObjects; ++i) {
        reads.push_back({writes[i].key,
                         writes[i].descriptor,
                         {{outputs[i].data(), 2048},
                          {outputs[i].data() + 2048, kObjectSize - 2048}}});
    }
    auto read_results = backend.BatchRead(reads);
    ASSERT_EQ(read_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) ASSERT_TRUE(read_results[i]) << i;
    EXPECT_EQ(outputs, values);
}

TEST_F(Hf3fsAdapterTest, BucketBatchSharesHandlesAcrossBoundedSubmissions) {
    constexpr size_t kMaxOpen =
        DistributedStorageBackend::kMaxOpenBucketsPerBatch;
    constexpr size_t kBuckets = kMaxOpen + 1;
    constexpr size_t kObjects = 2 * kBuckets;
    constexpr size_t kObjectSize = 4096;
    DistributedStorageConfig config;
    config.fsdir = test_dir_->path();
    config.fs_adapter_type = "hf3fs";
    config.allocator_type = "bucket";
    config.alignment = 4096;
    config.bucket_capacity = 2 * kObjectSize;
    config.max_bucket_count = kBuckets;

    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    auto adapter = std::make_unique<RecordingHf3fsAdapter>();
    auto* recorded = adapter.get();
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config, std::move(adapter));
    ASSERT_TRUE(backend.Init());

    std::vector<std::string> values;
    values.reserve(kObjects);
    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < kObjects; ++i) {
        const auto key = "bucket_key_" + std::to_string(i);
        auto descriptor = allocator.Allocate(key, kObjectSize);
        ASSERT_TRUE(descriptor) << i;
        values.emplace_back(kObjectSize, static_cast<char>(i + 1));
        writes.push_back(
            {key, *descriptor, {{values.back().data(), kObjectSize}}});
    }
    ASSERT_EQ(allocator.GetFileCount(), kBuckets);

    auto write_results = backend.BatchWrite(writes);
    ASSERT_EQ(write_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) ASSERT_TRUE(write_results[i]) << i;
    EXPECT_EQ(recorded->write_batches, (std::vector<size_t>{2 * kMaxOpen, 2}));
    EXPECT_EQ(recorded->peak_open, static_cast<int>(kMaxOpen));
    EXPECT_EQ(recorded->open_now, 0);

    std::vector<std::string> outputs(kObjects, std::string(kObjectSize, '\0'));
    std::vector<DfsReadRequest> reads;
    for (size_t i = 0; i < kObjects; ++i) {
        reads.push_back({writes[i].key,
                         writes[i].descriptor,
                         {{outputs[i].data(), kObjectSize}}});
    }
    auto read_results = backend.BatchRead(reads);
    ASSERT_EQ(read_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) ASSERT_TRUE(read_results[i]) << i;
    EXPECT_EQ(outputs, values);
    EXPECT_EQ(recorded->read_batches, (std::vector<size_t>{2 * kMaxOpen, 2}));
    EXPECT_EQ(recorded->peak_open, static_cast<int>(kMaxOpen));
    EXPECT_EQ(recorded->open_now, 0);
}

TEST_F(Hf3fsAdapterTest, ConcurrentBatchesUseTheSameShard) {
    constexpr size_t kWorkers = 4;
    constexpr size_t kObjectsPerWorker = 2;
    constexpr size_t kObjectSize = 4096;
    DistributedStorageConfig config;
    config.fsdir = test_dir_->path();
    config.fs_adapter_type = "hf3fs";
    config.shard_count = 1;
    config.shard_capacity = 1024 * 1024;
    config.alignment = 4096;

    ShardAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));
    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < kWorkers * kObjectsPerWorker; ++i) {
        auto descriptor =
            allocator.Allocate("key_" + std::to_string(i), kObjectSize);
        ASSERT_TRUE(descriptor);
        descriptors.push_back(*descriptor);
    }
    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config,
                                      std::make_unique<Hf3fsAdapter>());
    ASSERT_TRUE(backend.Init());

    std::barrier start(static_cast<std::ptrdiff_t>(kWorkers));
    std::vector<std::future<bool>> workers;
    for (size_t worker = 0; worker < kWorkers; ++worker) {
        workers.push_back(std::async(std::launch::async, [&, worker] {
            std::vector<std::string> values(
                kObjectsPerWorker,
                std::string(kObjectSize, static_cast<char>('A' + worker)));
            std::vector<std::string> outputs(kObjectsPerWorker,
                                             std::string(kObjectSize, '\0'));
            std::vector<DfsWriteRequest> writes;
            std::vector<DfsReadRequest> reads;
            for (size_t i = 0; i < kObjectsPerWorker; ++i) {
                const size_t index = worker * kObjectsPerWorker + i;
                const auto key = "key_" + std::to_string(index);
                writes.push_back({key,
                                  descriptors[index],
                                  {{values[i].data(), kObjectSize}}});
                reads.push_back({key,
                                 descriptors[index],
                                 {{outputs[i].data(), kObjectSize}}});
            }
            // Initialize each thread's USRBIO resources before concurrent I/O.
            auto warmup = backend.BatchRead(reads);
            start.arrive_and_wait();
            if (warmup.size() != kObjectsPerWorker) return false;
            for (const auto& result : warmup) {
                if (!result) return false;
            }
            auto written = backend.BatchWrite(writes);
            auto read = backend.BatchRead(reads);
            if (written.size() != kObjectsPerWorker ||
                read.size() != kObjectsPerWorker) {
                return false;
            }
            for (size_t i = 0; i < kObjectsPerWorker; ++i) {
                if (!written[i] || !read[i]) return false;
            }
            return outputs == values;
        }));
    }
    for (auto& worker : workers) EXPECT_TRUE(worker.get());
}

}  // namespace mooncake::test
