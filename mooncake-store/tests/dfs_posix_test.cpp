#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage_backend.h"

namespace mooncake::test {

class TempDir {
   public:
    explicit TempDir(const std::string& prefix) {
        static std::atomic<int64_t> counter{0};
        path_ = std::filesystem::temp_directory_path() /
                (prefix + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(++counter));
        path_str_ = path_.string();
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    const std::string& path() const { return path_str_; }

    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

   private:
    std::filesystem::path path_;
    std::string path_str_;
};

class EnvGuard {
   public:
    EnvGuard() {
        Save("MOONCAKE_DFS_FS_ADAPTER");
        Save("MOONCAKE_DISTRIBUTED_FS_TYPE");
        Save("MOONCAKE_DFS_EVICTION_ENABLED");
        Save("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK");
        Save("MOONCAKE_DFS_EVICTION_LOW_WATERMARK");
        Save("MOONCAKE_DFS_DEFERRED_FREE_SECONDS");
        Save("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL");
        Save("MOONCAKE_DFS_ROOT_DIRS");
        Save("MOONCAKE_DFS_ROOT_DIR");
        Save("MOONCAKE_DFS_SHARD_COUNT");
        Save("MOONCAKE_DFS_SHARD_CAPACITY");
        Save("MOONCAKE_DFS_ALIGNMENT");
        Save("MOONCAKE_DFS_SINGLE_TENANT");
    }

    ~EnvGuard() {
        for (const auto& [key, value] : saved_) {
            if (value.has_value()) {
                ::setenv(key.c_str(), value->c_str(), 1);
            } else {
                ::unsetenv(key.c_str());
            }
        }
    }

    void Set(const char* key, const char* value) { ::setenv(key, value, 1); }
    void Unset(const char* key) { ::unsetenv(key); }

   private:
    void Save(const std::string& key) {
        const char* value = ::getenv(key.c_str());
        if (value) {
            saved_.push_back({key, std::string(value)});
        } else {
            saved_.push_back({key, std::nullopt});
        }
    }

    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

class AlignedBuffer {
   public:
    explicit AlignedBuffer(size_t size, size_t alignment = 4096) : size_(size) {
        void* ptr = nullptr;
        if (::posix_memalign(&ptr, alignment, size) != 0) ptr = nullptr;
        ptr_ = static_cast<char*>(ptr);
    }

    ~AlignedBuffer() { std::free(ptr_); }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    char* data() { return ptr_; }
    const char* data() const { return ptr_; }
    size_t size() const { return size_; }

    void Fill(char value) { std::memset(ptr_, value, size_); }

   private:
    char* ptr_ = nullptr;
    size_t size_ = 0;
};

void ConfigurePosixDfs(EnvGuard& env) {
    env.Unset("MOONCAKE_DFS_ROOT_DIRS");
    env.Set("MOONCAKE_DFS_FS_ADAPTER", "posix");
    env.Set("MOONCAKE_DFS_SINGLE_TENANT", "1");
    env.Set("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    env.Set("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
}

DistributedStorageConfig MakeAllocatorConfig(const std::string& mount_path,
                                             int shard_count,
                                             uint64_t shard_capacity,
                                             uint64_t alignment) {
    auto config = DistributedStorageConfig::FromEnvironment();
    config.fsdir = mount_path;
    config.shard_count = shard_count;
    config.shard_capacity = shard_capacity;
    config.alignment = alignment;
    return config;
}

std::vector<DfsGlobalAllocator::EvictionCandidate>
PrepareAndCommitPreparedEviction(DfsGlobalAllocator& allocator) {
    auto pending = allocator.PrepareEviction();
    auto candidates = pending.Candidates();
    allocator.CommitPreparedEviction(std::move(pending));
    return candidates;
}

class FsAdapterFdTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tmp_ = std::make_unique<TempDir>("dfs_fd_test");
        adapter_ = std::make_unique<PosixFsAdapter>();
        ASSERT_TRUE(adapter_->Init(tmp_->path()).has_value());
    }

    void TearDown() override {
        adapter_.reset();
        tmp_.reset();
    }

    std::unique_ptr<TempDir> tmp_;
    std::unique_ptr<PosixFsAdapter> adapter_;
};

TEST_F(FsAdapterFdTest, OpenClose) {
    auto pre = adapter_->PreallocateFile(tmp_->file("shard0.data"), 4096);
    ASSERT_TRUE(pre.has_value());

    auto fd = adapter_->OpenFile(tmp_->file("shard0.data"));
    ASSERT_TRUE(fd.has_value());
    EXPECT_GE(*fd, 0);

    auto close = adapter_->CloseFile(*fd);
    EXPECT_TRUE(close.has_value());
}

TEST_F(FsAdapterFdTest, WriteAtReadAt) {
    ASSERT_TRUE(
        adapter_->PreallocateFile(tmp_->file("shard0.data"), 4096).has_value());
    auto fd = adapter_->OpenFile(tmp_->file("shard0.data"));
    ASSERT_TRUE(fd.has_value());

    char write_buf[128];
    std::memset(write_buf, 'A', sizeof(write_buf));
    iovec wiov{write_buf, sizeof(write_buf)};
    auto written = adapter_->WriteAt(*fd, &wiov, 1, 100);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, sizeof(write_buf));

    char read_buf[128] = {};
    iovec riov{read_buf, sizeof(read_buf)};
    auto read = adapter_->ReadAt(*fd, &riov, 1, 100);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, sizeof(read_buf));
    EXPECT_EQ(std::memcmp(write_buf, read_buf, sizeof(write_buf)), 0);

    adapter_->CloseFile(*fd);
}

TEST_F(FsAdapterFdTest, MultiIovWriteRead) {
    constexpr size_t total = 8192;
    ASSERT_TRUE(adapter_->PreallocateFile(tmp_->file("shard_multi.data"), total)
                    .has_value());
    auto fd = adapter_->OpenFile(tmp_->file("shard_multi.data"));
    ASSERT_TRUE(fd.has_value());

    char w0[2048], w1[3072], w2[3072];
    std::memset(w0, 'A', sizeof(w0));
    std::memset(w1, 'B', sizeof(w1));
    std::memset(w2, 'C', sizeof(w2));
    iovec wiovs[3] = {
        {w0, sizeof(w0)},
        {w1, sizeof(w1)},
        {w2, sizeof(w2)},
    };
    auto written = adapter_->WriteAt(*fd, wiovs, 3, 0);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, total);

    char r0[2048] = {}, r1[3072] = {}, r2[3072] = {};
    iovec riovs[3] = {
        {r0, sizeof(r0)},
        {r1, sizeof(r1)},
        {r2, sizeof(r2)},
    };
    auto read = adapter_->ReadAt(*fd, riovs, 3, 0);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, total);
    EXPECT_EQ(std::memcmp(w0, r0, sizeof(w0)), 0);
    EXPECT_EQ(std::memcmp(w1, r1, sizeof(w1)), 0);
    EXPECT_EQ(std::memcmp(w2, r2, sizeof(w2)), 0);

    adapter_->CloseFile(*fd);
}

TEST_F(FsAdapterFdTest, MultiIovPartialReadAndUnalignedAccess) {
    constexpr size_t total = 8192;
    ASSERT_TRUE(
        adapter_->PreallocateFile(tmp_->file("shard_partial.data"), total)
            .has_value());
    auto fd = adapter_->OpenFile(tmp_->file("shard_partial.data"));
    ASSERT_TRUE(fd.has_value());

    std::string write_data(total, 'X');
    iovec wiov{write_data.data(), write_data.size()};
    ASSERT_TRUE(adapter_->WriteAt(*fd, &wiov, 1, 0).has_value());

    char read_buf[3072] = {};
    iovec riov{read_buf, sizeof(read_buf)};
    auto read = adapter_->ReadAt(*fd, &riov, 1, 2048);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, sizeof(read_buf));
    EXPECT_EQ(std::memcmp(read_buf, write_data.data() + 2048, sizeof(read_buf)),
              0);

    char wbuf[63];
    std::memset(wbuf, 'Y', sizeof(wbuf));
    iovec unaligned_wiov{wbuf, sizeof(wbuf)};
    auto written = adapter_->WriteAt(*fd, &unaligned_wiov, 1, 101);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, sizeof(wbuf));

    char rbuf[63] = {};
    iovec unaligned_riov{rbuf, sizeof(rbuf)};
    read = adapter_->ReadAt(*fd, &unaligned_riov, 1, 101);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, sizeof(rbuf));
    EXPECT_EQ(std::memcmp(wbuf, rbuf, sizeof(wbuf)), 0);

    char beyond[64] = {};
    iovec beyond_iov{beyond, sizeof(beyond)};
    auto beyond_read = adapter_->ReadAt(*fd, &beyond_iov, 1, 1ULL << 30);
    ASSERT_TRUE(beyond_read.has_value());
    EXPECT_EQ(*beyond_read, 0);

    adapter_->CloseFile(*fd);
}

TEST_F(FsAdapterFdTest, PreallocateLargeSparseFile) {
    constexpr uint64_t size = 4ULL * 1024 * 1024 * 1024;
    ASSERT_TRUE(adapter_->PreallocateFile(tmp_->file("shard_large.data"), size)
                    .has_value());

    struct stat st;
    ASSERT_EQ(::stat(tmp_->file("shard_large.data").c_str(), &st), 0);
    EXPECT_EQ(static_cast<uint64_t>(st.st_size), size);
    EXPECT_LT(static_cast<uint64_t>(st.st_blocks) * 512, size / 1000);

    auto fd = adapter_->OpenFile(tmp_->file("shard_large.data"));
    ASSERT_TRUE(fd.has_value());
    char wbuf[4096];
    std::memset(wbuf, 'L', sizeof(wbuf));
    iovec wiov{wbuf, sizeof(wbuf)};
    auto written = adapter_->WriteAt(*fd, &wiov, 1, 3ULL * 1024 * 1024 * 1024);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, sizeof(wbuf));

    char rbuf[4096] = {};
    iovec riov{rbuf, sizeof(rbuf)};
    auto read = adapter_->ReadAt(*fd, &riov, 1, 3ULL * 1024 * 1024 * 1024);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, sizeof(rbuf));
    EXPECT_EQ(std::memcmp(wbuf, rbuf, sizeof(wbuf)), 0);

    adapter_->CloseFile(*fd);
}

TEST(DfsGlobalAllocatorTest, AllocateFreeAndFormatShardIdx) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_alloc");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 4, 1024 * 1024, 4096)));

    auto desc = alloc.Allocate("key1", 100);
    ASSERT_TRUE(desc.has_value());
    EXPECT_EQ(desc->aligned_size, 4096);
    EXPECT_GE(desc->shard_idx, 0);
    EXPECT_LT(desc->shard_idx, 4);
    EXPECT_EQ(desc->offset % 4096, 0);

    alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx, "key1");
    auto desc2 = alloc.Allocate("key2", 100);
    EXPECT_TRUE(desc2.has_value());

    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(0, 64), "00");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(9, 64), "09");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(10, 64), "10");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(63, 64), "63");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(100, 1000), "100");
}

TEST(DfsGlobalAllocatorTest, PlacesShardsRoundRobinAcrossRoots) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir root0("dfs_alloc_root0");
    TempDir root1("dfs_alloc_root1");

    auto config = MakeAllocatorConfig(root0.path(), 4, 1024 * 1024, 4096);
    config.root_dirs = {root0.path(), root1.path()};

    DfsGlobalAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    for (int shard_idx = 0; shard_idx < config.shard_count; ++shard_idx) {
        const TempDir& expected_root = shard_idx % 2 == 0 ? root0 : root1;
        const std::string file_name =
            "dfs_shard_" +
            DfsGlobalAllocator::FormatShardIdx(shard_idx, config.shard_count) +
            ".data";
        EXPECT_TRUE(std::filesystem::exists(expected_root.file(file_name)))
            << "shard_idx=" << shard_idx;
    }

    auto descriptor = allocator.Allocate("multi-root-key", 100);
    ASSERT_TRUE(descriptor);
    EXPECT_EQ(
        descriptor->file_path,
        (std::filesystem::path(config.RootForShard(descriptor->shard_idx)) /
         ("dfs_shard_" +
          DfsGlobalAllocator::FormatShardIdx(descriptor->shard_idx,
                                             config.shard_count) +
          ".data"))
            .string());
}

TEST(DfsGlobalAllocatorTest, InitReturnsSpecificErrors) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_init_error");

    DistributedStorageConfig invalid_config =
        MakeAllocatorConfig(tmp.path(), 1, 1024 * 1024, 3);
    DfsGlobalAllocator invalid_allocator;
    auto invalid_result = invalid_allocator.Init(invalid_config);
    ASSERT_FALSE(invalid_result);
    EXPECT_EQ(invalid_result.error(), ErrorCode::INVALID_PARAMS);

    const std::string file_path = tmp.file("not_a_directory");
    const int fd = ::open(file_path.c_str(), O_CREAT | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::close(fd), 0);

    DistributedStorageConfig file_error_config =
        MakeAllocatorConfig(file_path, 1, 1024 * 1024, 4096);
    DfsGlobalAllocator file_error_allocator;
    auto file_error_result = file_error_allocator.Init(file_error_config);
    ASSERT_FALSE(file_error_result);
    EXPECT_EQ(file_error_result.error(), ErrorCode::FILE_WRITE_FAIL);
}

TEST(DfsGlobalAllocatorTest, AllocateReservesAlignmentPadding) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_alloc_padding");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 8 * 1024, 4096)));

    auto desc = alloc.Allocate("key1", 100);
    ASSERT_TRUE(desc.has_value());
    EXPECT_EQ(desc->aligned_size, 4096);
    EXPECT_EQ(desc->offset % 4096, 0);

    auto exhausted = alloc.Allocate("key2", 100);
    EXPECT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx, "key1");
    auto after_free = alloc.Allocate("key3", 100);
    EXPECT_TRUE(after_free.has_value());
}

TEST(DfsGlobalAllocatorTest, ExhaustionAndEviction) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    env.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.5");
    env.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.25");
    TempDir tmp("dfs_exhaust");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 32 * 1024, 4096)));

    std::vector<DistributedFSDescriptor> descs;
    for (int i = 0; i < 4; ++i) {
        auto desc = alloc.Allocate("k" + std::to_string(i), 100);
        ASSERT_TRUE(desc.has_value()) << "allocation " << i;
        alloc.UpdateAccess("k" + std::to_string(i), desc->shard_idx,
                           desc->offset);
        descs.push_back(*desc);
    }

    auto exhausted = alloc.Allocate("k_exhausted", 100);
    EXPECT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    auto pending = alloc.PrepareEviction();
    auto evicted = pending.Candidates();
    ASSERT_FALSE(evicted.empty());
    EXPECT_EQ(evicted.front().key, "k0");

    // Prepare only reserves candidates; their extents are not reusable until
    // the master accepts and commits the transaction.
    auto before_commit = alloc.Allocate("k_before_commit", 100);
    EXPECT_FALSE(before_commit.has_value());

    alloc.CommitPreparedEviction(std::move(pending));

    auto after_evict = alloc.Allocate("k_after_evict", 100);
    EXPECT_TRUE(after_evict.has_value());
}

TEST(DfsGlobalAllocatorTest, RestorePreparedEvictionPreservesCandidateOrder) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    env.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.9");
    env.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.7");
    TempDir tmp("dfs_abort_eviction");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 32 * 1024, 4096)));

    for (int i = 0; i < 4; ++i) {
        const std::string key = "k" + std::to_string(i);
        auto desc = alloc.Allocate(key, 100);
        ASSERT_TRUE(desc.has_value()) << "allocation " << i;
        alloc.UpdateAccess(key, desc->shard_idx, desc->offset);
    }

    auto pending = alloc.PrepareEviction();
    ASSERT_EQ(pending.Candidates().size(), 2);
    EXPECT_EQ(pending.Candidates()[0].key, "k0");
    EXPECT_EQ(pending.Candidates()[1].key, "k1");
    alloc.RestorePreparedEviction(std::move(pending));

    auto retry = alloc.PrepareEviction();
    ASSERT_EQ(retry.Candidates().size(), 2);
    EXPECT_EQ(retry.Candidates()[0].key, "k0");
    EXPECT_EQ(retry.Candidates()[1].key, "k1");
    alloc.RestorePreparedEviction(std::move(retry));
}

TEST(DfsGlobalAllocatorTest, PartialResolutionContinuesToLowWatermark) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    env.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.9");
    env.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.7");
    TempDir tmp("dfs_partial_eviction");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 32 * 1024, 4096)));

    std::vector<DistributedFSDescriptor> descs;
    for (int i = 0; i < 4; ++i) {
        const std::string key = "k" + std::to_string(i);
        auto desc = alloc.Allocate(key, 100);
        ASSERT_TRUE(desc.has_value()) << "allocation " << i;
        alloc.UpdateAccess(key, desc->shard_idx, desc->offset);
        descs.push_back(*desc);
    }

    auto pending = alloc.PrepareEviction();
    ASSERT_EQ(pending.Candidates().size(), 2);
    EXPECT_EQ(pending.Candidates()[0].key, "k0");
    EXPECT_EQ(pending.Candidates()[1].key, "k1");

    alloc.ResolvePreparedEviction(std::move(pending), {true, false});
    alloc.UpdateAccess("k1", descs[1].shard_idx, descs[1].offset);

    // Accepting only k0 drops usage below the high watermark but not the low
    // watermark. The same active eviction cycle must therefore continue and
    // reach k2 behind the restored, protected k1.
    auto continuation = alloc.PrepareEviction();
    ASSERT_EQ(continuation.Candidates().size(), 1);
    EXPECT_EQ(continuation.Candidates().front().key, "k2");
    alloc.CommitPreparedEviction(std::move(continuation));

    auto complete = alloc.PrepareEviction();
    EXPECT_TRUE(complete.Empty());
}

TEST(DfsGlobalAllocatorTest, EvictionCountsPendingFreeTowardWatermarks) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    env.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.9");
    env.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.7");
    env.Set("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "30");
    TempDir tmp("dfs_pending_watermark");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 32 * 1024, 4096)));

    for (int i = 0; i < 4; ++i) {
        const std::string key = "k" + std::to_string(i);
        auto desc = alloc.Allocate(key, 100);
        ASSERT_TRUE(desc.has_value()) << "allocation " << i;
        alloc.UpdateAccess(key, desc->shard_idx, desc->offset);
    }

    auto evicted = PrepareAndCommitPreparedEviction(alloc);
    ASSERT_EQ(evicted.size(), 2);
    EXPECT_EQ(evicted[0].key, "k0");
    EXPECT_EQ(evicted[1].key, "k1");

    auto repeated = PrepareAndCommitPreparedEviction(alloc);
    EXPECT_TRUE(repeated.empty());

    auto before_release = alloc.Allocate("k_before_release", 100);
    EXPECT_FALSE(before_release.has_value());
    EXPECT_EQ(before_release.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

TEST(DfsGlobalAllocatorTest, FreeRemovesLruEntryBeforeOffsetReuse) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_free_lru_reuse");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 8 * 1024, 4096)));

    auto desc_a = alloc.Allocate("A", 100);
    ASSERT_TRUE(desc_a.has_value());
    alloc.UpdateAccess("A", desc_a->shard_idx, desc_a->offset);

    alloc.Free(desc_a->offset, desc_a->aligned_size, desc_a->shard_idx, "A");

    auto desc_b = alloc.Allocate("B", 100);
    ASSERT_TRUE(desc_b.has_value());
    ASSERT_EQ(desc_b->offset, desc_a->offset);
    alloc.UpdateAccess("B", desc_b->shard_idx, desc_b->offset);

    auto evicted = PrepareAndCommitPreparedEviction(alloc);
    ASSERT_EQ(evicted.size(), 1);
    EXPECT_EQ(evicted.front().key, "B");
}

TEST(DfsGlobalAllocatorTest, StaleFreeDoesNotReleaseReusedOffset) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_stale_free");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(alloc.Init(MakeAllocatorConfig(tmp.path(), 1, 8 * 1024, 4096)));

    auto desc_a = alloc.Allocate("A", 100);
    ASSERT_TRUE(desc_a.has_value());
    alloc.UpdateAccess("A", desc_a->shard_idx, desc_a->offset);

    alloc.Free(desc_a->offset, desc_a->aligned_size, desc_a->shard_idx, "A");

    auto desc_b = alloc.Allocate("B", 100);
    ASSERT_TRUE(desc_b.has_value());
    ASSERT_EQ(desc_b->offset, desc_a->offset);
    alloc.UpdateAccess("B", desc_b->shard_idx, desc_b->offset);

    alloc.Free(desc_a->offset, desc_a->aligned_size, desc_a->shard_idx, "A");

    auto evicted = PrepareAndCommitPreparedEviction(alloc);
    ASSERT_EQ(evicted.size(), 1);
    EXPECT_EQ(evicted.front().key, "B");
}

TEST(DfsGlobalAllocatorTest, ConcurrentAllocate) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_concurrent");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(
        alloc.Init(MakeAllocatorConfig(tmp.path(), 4, 128 * 1024, 4096)));

    constexpr int kThreadCount = 32;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&alloc, &success_count, &fail_count, i]() {
            std::string key = "key_" + std::to_string(i);
            auto desc = alloc.Allocate(key, 100);
            if (desc.has_value()) {
                success_count++;
                alloc.UpdateAccess(key, desc->shard_idx, desc->offset);
                alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx,
                           key);
            } else {
                fail_count++;
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(success_count.load(), kThreadCount);
    EXPECT_EQ(fail_count.load(), 0);
}

TEST(ReplicaDfsTest, HelpersAndDescriptor) {
    DistributedFSDescriptor desc{"/mnt/3fs/shard0.data", 4096, 100, 4096, 0};
    Replica replica(desc, ReplicaStatus::PROCESSING);

    EXPECT_TRUE(replica.is_dfs_replica());
    EXPECT_FALSE(replica.is_memory_replica());
    EXPECT_FALSE(replica.is_disk_replica());
    EXPECT_FALSE(replica.is_nof_replica());
    EXPECT_EQ(replica.type(), ReplicaType::DFS);
    EXPECT_EQ(replica.get_dfs_descriptor().offset, 4096);

    auto descriptor = replica.get_descriptor();
    EXPECT_TRUE(descriptor.is_dfs_replica());
    EXPECT_FALSE(descriptor.is_memory_replica());
    EXPECT_EQ(descriptor.status, ReplicaStatus::PROCESSING);
    EXPECT_EQ(descriptor.get_dfs_descriptor().object_size, 100);

    EXPECT_TRUE(replica.is_processing());
    replica.mark_complete();
    EXPECT_TRUE(replica.is_completed());
    replica.mark_processing();
    EXPECT_TRUE(replica.is_processing());

    EXPECT_EQ(replica.get_refcnt(), 0);
    replica.inc_refcnt();
    EXPECT_TRUE(replica.is_busy());
    replica.dec_refcnt();
    EXPECT_FALSE(replica.is_busy());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 0;
    config.dfs_replica_num = 1;
    EXPECT_EQ(DetermineReplicaWriteMode(config),
              ReplicaWriteMode::RELIABLE_MULTI_REPLICA);
}

class DfsBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tmp_ = std::make_unique<TempDir>("dfs_backend");
        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = tmp_->path();

        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = tmp_->path();
        distributed_config.fs_adapter_type = "posix";
        distributed_config.shard_count = 4;
        distributed_config.shard_capacity = 64 * 1024 * 1024;
        distributed_config.alignment = 4096;

        backend_ = std::make_unique<DistributedStorageBackend>(
            file_config, distributed_config,
            std::make_unique<PosixFsAdapter>());
        ASSERT_TRUE(backend_->Init().has_value());
    }

    void TearDown() override {
        backend_.reset();
        tmp_.reset();
    }

    std::string ShardPath(int shard_idx) const {
        return tmp_->file("dfs_shard_" +
                          DfsGlobalAllocator::FormatShardIdx(shard_idx, 4) +
                          ".data");
    }

    std::unique_ptr<TempDir> tmp_;
    std::unique_ptr<DistributedStorageBackend> backend_;
};

class ControlledPosixFsAdapter : public PosixFsAdapter {
   public:
    void FailWriteCall(int call) { fail_write_call_ = call; }
    void ShortWriteCall(int call) { short_write_call_ = call; }
    void FailReadCall(int call) { fail_read_call_ = call; }
    void ShortReadCall(int call) { short_read_call_ = call; }
    int WriteCallCount() const { return write_calls_.load(); }
    int ReadCallCount() const { return read_calls_.load(); }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        if (call == fail_write_call_) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (call == short_write_call_) {
            size_t total_size = 0;
            for (int i = 0; i < iovcnt; ++i) {
                total_size += iov[i].iov_len;
            }
            return total_size == 0 ? 0 : total_size - 1;
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override {
        const int call = ++read_calls_;
        if (call == fail_read_call_) {
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        if (call == short_read_call_) {
            size_t total_size = 0;
            for (int i = 0; i < iovcnt; ++i) {
                total_size += iov[i].iov_len;
            }
            return total_size == 0 ? 0 : total_size - 1;
        }
        return PosixFsAdapter::ReadAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> read_calls_{0};
    int fail_write_call_ = -1;
    int short_write_call_ = -1;
    int fail_read_call_ = -1;
    int short_read_call_ = -1;
};

TEST_F(DfsBackendTest, BatchWriteUsesExplicitDescriptors) {
    AlignedBuffer write_buf(4096);
    ASSERT_NE(write_buf.data(), nullptr);
    write_buf.Fill('E');

    std::vector<DfsWriteRequest> requests{
        {"explicit",
         {ShardPath(1), 0, 4096, 4096, 1},
         {{write_buf.data(), write_buf.size()}}},
        {"bad_path",
         {tmp_->file("wrong.data"), 4096, 4096, 4096, 0},
         {{write_buf.data(), write_buf.size()}}},
        {"bad_size",
         {ShardPath(0), 4096, 2048, 4096, 0},
         {{write_buf.data(), write_buf.size()}}},
    };
    auto results = backend_->BatchWrite(requests);
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_FALSE(results[2].has_value());
    EXPECT_EQ(results[2].error(), ErrorCode::INVALID_PARAMS);

    AlignedBuffer read_buf(4096);
    auto read_results =
        backend_->BatchRead({{"explicit",
                              requests[0].descriptor,
                              {{read_buf.data(), read_buf.size()}}}});
    ASSERT_EQ(read_results.size(), 1);
    ASSERT_TRUE(read_results[0].has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), write_buf.size()),
              0);
}

TEST_F(DfsBackendTest, BatchWritePreservesPerKeyWriteErrors) {
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp_->path();

    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = tmp_->path();
    distributed_config.fs_adapter_type = "posix";
    distributed_config.shard_count = 4;
    distributed_config.shard_capacity = 64 * 1024 * 1024;
    distributed_config.alignment = 4096;

    auto adapter = std::make_unique<ControlledPosixFsAdapter>();
    auto* controlled_adapter = adapter.get();
    auto backend = std::make_unique<DistributedStorageBackend>(
        file_config, distributed_config, std::move(adapter));
    ASSERT_TRUE(backend->Init().has_value());
    controlled_adapter->FailWriteCall(2);
    controlled_adapter->ShortWriteCall(3);

    AlignedBuffer write_buf(4096);
    ASSERT_NE(write_buf.data(), nullptr);
    std::vector<DfsWriteRequest> requests{
        {"ok",
         {ShardPath(0), 0, 4096, 4096, 0},
         {{write_buf.data(), write_buf.size()}}},
        {"failed",
         {ShardPath(0), 4096, 4096, 4096, 0},
         {{write_buf.data(), write_buf.size()}}},
        {"short",
         {ShardPath(0), 8192, 4096, 4096, 0},
         {{write_buf.data(), write_buf.size()}}},
    };
    auto results = backend->BatchWrite(requests);
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::FILE_WRITE_FAIL);
    ASSERT_FALSE(results[2].has_value());
    EXPECT_EQ(results[2].error(), ErrorCode::FILE_WRITE_FAIL);
}

TEST_F(DfsBackendTest, BatchReadUsesExplicitDescriptorsAndMultipleSlices) {
    AlignedBuffer first_value(4096), second_value(4096);
    ASSERT_NE(first_value.data(), nullptr);
    ASSERT_NE(second_value.data(), nullptr);
    first_value.Fill('R');
    second_value.Fill('S');

    const DistributedFSDescriptor first_desc{ShardPath(0), 0, 4096, 4096, 0};
    const DistributedFSDescriptor second_desc{ShardPath(0), 4096, 4096, 4096,
                                              0};
    auto write_results = backend_->BatchWrite(
        {{"same_key", first_desc, {{first_value.data(), first_value.size()}}},
         {"other_key",
          second_desc,
          {{second_value.data(), second_value.size()}}}});
    ASSERT_EQ(write_results.size(), 2);
    ASSERT_TRUE(write_results[0].has_value());
    ASSERT_TRUE(write_results[1].has_value());

    AlignedBuffer output_0(1024), output_1(3072), unused(64);
    ASSERT_NE(output_0.data(), nullptr);
    ASSERT_NE(output_1.data(), nullptr);
    ASSERT_NE(unused.data(), nullptr);
    unused.Fill('U');
    std::vector<DfsReadRequest> requests{
        {"same_key",
         first_desc,
         {{output_0.data(), output_0.size()},
          {output_1.data(), output_1.size()},
          {unused.data(), unused.size()}}},
        {"too_small", first_desc, {{output_0.data(), output_0.size()}}},
        {"null_slice", first_desc, {{nullptr, first_desc.object_size}}},
    };
    auto read_results = backend_->BatchRead(requests);
    ASSERT_EQ(read_results.size(), requests.size());
    ASSERT_TRUE(read_results[0].has_value());
    ASSERT_FALSE(read_results[1].has_value());
    EXPECT_EQ(read_results[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_FALSE(read_results[2].has_value());
    EXPECT_EQ(read_results[2].error(), ErrorCode::INVALID_PARAMS);

    EXPECT_EQ(std::memcmp(first_value.data(), output_0.data(), output_0.size()),
              0);
    EXPECT_EQ(std::memcmp(first_value.data() + output_0.size(), output_1.data(),
                          output_1.size()),
              0);
    for (size_t i = 0; i < unused.size(); ++i) {
        EXPECT_EQ(unused.data()[i], 'U');
    }
}

TEST_F(DfsBackendTest, BatchReadPreservesPerKeyErrors) {
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp_->path();

    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = tmp_->path();
    distributed_config.fs_adapter_type = "posix";
    distributed_config.shard_count = 4;
    distributed_config.shard_capacity = 64 * 1024 * 1024;
    distributed_config.alignment = 4096;

    auto adapter = std::make_unique<ControlledPosixFsAdapter>();
    auto* controlled_adapter = adapter.get();
    auto backend = std::make_unique<DistributedStorageBackend>(
        file_config, distributed_config, std::move(adapter));
    ASSERT_TRUE(backend->Init().has_value());

    AlignedBuffer write_buf(4096);
    ASSERT_NE(write_buf.data(), nullptr);
    write_buf.Fill('T');
    std::vector<DfsWriteRequest> writes;
    for (size_t i = 0; i < 4; ++i) {
        writes.push_back({"key_" + std::to_string(i),
                          {ShardPath(0), i * 4096, 4096, 4096, 0},
                          {{write_buf.data(), write_buf.size()}}});
    }
    auto write_results = backend->BatchWrite(writes);
    ASSERT_EQ(write_results.size(), writes.size());
    for (const auto& result : write_results) {
        ASSERT_TRUE(result.has_value());
    }

    controlled_adapter->FailReadCall(2);
    controlled_adapter->ShortReadCall(3);
    AlignedBuffer out0(4096), out1(4096), out2(4096), out3(4096);
    std::vector<DfsReadRequest> reads{
        {"ok_0", writes[0].descriptor, {{out0.data(), out0.size()}}},
        {"failed", writes[1].descriptor, {{out1.data(), out1.size()}}},
        {"short", writes[2].descriptor, {{out2.data(), out2.size()}}},
        {"ok_3", writes[3].descriptor, {{out3.data(), out3.size()}}},
    };
    auto read_results = backend->BatchRead(reads);
    ASSERT_EQ(read_results.size(), reads.size());
    EXPECT_TRUE(read_results[0].has_value());
    ASSERT_FALSE(read_results[1].has_value());
    EXPECT_EQ(read_results[1].error(), ErrorCode::FILE_OPEN_FAIL);
    ASSERT_FALSE(read_results[2].has_value());
    EXPECT_EQ(read_results[2].error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_TRUE(read_results[3].has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), out3.data(), write_buf.size()), 0);
}

TEST_F(DfsBackendTest, RejectsInvalidDescriptorRangesBeforeIo) {
    constexpr uint64_t kShardCapacity = 64 * 1024 * 1024;

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp_->path();

    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = tmp_->path();
    distributed_config.fs_adapter_type = "posix";
    distributed_config.shard_count = 4;
    distributed_config.shard_capacity = kShardCapacity;
    distributed_config.alignment = 4096;

    auto adapter = std::make_unique<ControlledPosixFsAdapter>();
    auto* controlled_adapter = adapter.get();
    auto backend = std::make_unique<DistributedStorageBackend>(
        file_config, distributed_config, std::move(adapter));
    ASSERT_TRUE(backend->Init().has_value());

    AlignedBuffer small_buf(4096);
    AlignedBuffer large_buf(8192);
    ASSERT_NE(small_buf.data(), nullptr);
    ASSERT_NE(large_buf.data(), nullptr);

    std::vector<DfsWriteRequest> requests{
        {"past_capacity",
         {ShardPath(0), kShardCapacity, 4096, 4096, 0},
         {{small_buf.data(), small_buf.size()}}},
        {"object_exceeds_allocation",
         {ShardPath(0), 0, 8192, 4096, 0},
         {{large_buf.data(), large_buf.size()}}},
        {"overflow",
         {ShardPath(0), std::numeric_limits<uint64_t>::max() - 4095, 4096, 4096,
          0},
         {{small_buf.data(), small_buf.size()}}},
        {"unaligned_offset",
         {ShardPath(0), 1, 4096, 4096, 0},
         {{small_buf.data(), small_buf.size()}}},
    };

    auto write_results = backend->BatchWrite(requests);
    ASSERT_EQ(write_results.size(), requests.size());
    for (const auto& result : write_results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(controlled_adapter->WriteCallCount(), 0);

    AlignedBuffer read_buf(8192);
    ASSERT_NE(read_buf.data(), nullptr);
    std::vector<DfsReadRequest> read_requests;
    for (const auto& request : requests) {
        read_requests.push_back({request.key,
                                 request.descriptor,
                                 {{read_buf.data(), read_buf.size()}}});
    }
    auto read_results = backend->BatchRead(read_requests);
    ASSERT_EQ(read_results.size(), read_requests.size());
    for (const auto& result : read_results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(controlled_adapter->ReadCallCount(), 0);
}

TEST_F(DfsBackendTest, KeyOnlyStorageBackendOperationsAreNotSupported) {
    std::unordered_map<std::string, std::vector<Slice>> offload_batch;
    auto offload_result = backend_->BatchOffload(
        offload_batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(offload_result.has_value());
    EXPECT_EQ(offload_result.error(), ErrorCode::NOT_SUPPORTED);

    std::unordered_map<std::string, Slice> load_batch;
    auto load_result = backend_->BatchLoad(load_batch);
    ASSERT_FALSE(load_result.has_value());
    EXPECT_EQ(load_result.error(), ErrorCode::NOT_SUPPORTED);

    auto exists_result = backend_->IsExist("key");
    ASSERT_FALSE(exists_result.has_value());
    EXPECT_EQ(exists_result.error(), ErrorCode::NOT_SUPPORTED);
    auto enabled_result = backend_->IsEnableOffloading();
    ASSERT_TRUE(enabled_result.has_value());
    EXPECT_FALSE(*enabled_result);
}

TEST_F(DfsBackendTest, BatchReadAndWriteAcceptUnalignedBuffers) {
    constexpr size_t kObjectSize = 1234;
    AlignedBuffer write_storage(kObjectSize + 1);
    AlignedBuffer read_storage(kObjectSize + 3);
    ASSERT_NE(write_storage.data(), nullptr);
    ASSERT_NE(read_storage.data(), nullptr);

    char* write_ptr = write_storage.data() + 1;
    char* read_ptr = read_storage.data() + 3;
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(write_ptr) % 4096, 0);
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(read_ptr) % 4096, 0);
    for (size_t i = 0; i < kObjectSize; ++i) {
        write_ptr[i] = static_cast<char>('a' + (i % 26));
    }

    const DistributedFSDescriptor descriptor{ShardPath(0), 0, kObjectSize, 4096,
                                             0};
    auto write_results = backend_->BatchWrite(
        {{"unaligned", descriptor, {{write_ptr, kObjectSize}}}});
    ASSERT_EQ(write_results.size(), 1);
    ASSERT_TRUE(write_results[0].has_value());

    auto read_results = backend_->BatchRead(
        {{"unaligned", descriptor, {{read_ptr, kObjectSize}}}});
    ASSERT_EQ(read_results.size(), 1);
    ASSERT_TRUE(read_results[0].has_value());
    EXPECT_EQ(std::memcmp(write_ptr, read_ptr, kObjectSize), 0);
}

TEST_F(DfsBackendTest, MultipleKeysAcrossShards) {
    AlignedBuffer key0(4096), key1(4096), key2(8192);
    ASSERT_NE(key0.data(), nullptr);
    ASSERT_NE(key1.data(), nullptr);
    ASSERT_NE(key2.data(), nullptr);
    key0.Fill('A');
    key1.Fill('B');
    key2.Fill('C');

    const std::vector<DistributedFSDescriptor> descriptors{
        {ShardPath(0), 0, key0.size(), key0.size(), 0},
        {ShardPath(0), 4096, key1.size(), key1.size(), 0},
        {ShardPath(1), 0, key2.size(), key2.size(), 1},
    };
    auto write_results = backend_->BatchWrite(
        {{"key0", descriptors[0], {{key0.data(), key0.size()}}},
         {"key1", descriptors[1], {{key1.data(), key1.size()}}},
         {"key2", descriptors[2], {{key2.data(), key2.size()}}}});
    ASSERT_EQ(write_results.size(), 3);
    EXPECT_TRUE(write_results[0].has_value());
    EXPECT_TRUE(write_results[1].has_value());
    EXPECT_TRUE(write_results[2].has_value());

    AlignedBuffer out0(4096), out1(4096), out2(8192);
    ASSERT_NE(out0.data(), nullptr);
    ASSERT_NE(out1.data(), nullptr);
    ASSERT_NE(out2.data(), nullptr);
    auto read_results = backend_->BatchRead(
        {{"key0", descriptors[0], {{out0.data(), out0.size()}}},
         {"key1", descriptors[1], {{out1.data(), out1.size()}}},
         {"key2", descriptors[2], {{out2.data(), out2.size()}}}});
    ASSERT_EQ(read_results.size(), 3);
    EXPECT_TRUE(read_results[0].has_value());
    EXPECT_TRUE(read_results[1].has_value());
    EXPECT_TRUE(read_results[2].has_value());
    EXPECT_EQ(std::memcmp(key0.data(), out0.data(), key0.size()), 0);
    EXPECT_EQ(std::memcmp(key1.data(), out1.data(), key1.size()), 0);
    EXPECT_EQ(std::memcmp(key2.data(), out2.data(), key2.size()), 0);
}

TEST_F(DfsBackendTest, LargeObject) {
    constexpr size_t kLargeSize = 33 * 1024 * 1024;
    AlignedBuffer write_buf(kLargeSize);
    ASSERT_NE(write_buf.data(), nullptr);
    write_buf.Fill('L');
    const DistributedFSDescriptor descriptor{ShardPath(2), 0, write_buf.size(),
                                             write_buf.size(), 2};
    auto write_results = backend_->BatchWrite(
        {{"large", descriptor, {{write_buf.data(), write_buf.size()}}}});
    ASSERT_EQ(write_results.size(), 1);
    ASSERT_TRUE(write_results[0].has_value());

    AlignedBuffer read_buf(kLargeSize);
    ASSERT_NE(read_buf.data(), nullptr);
    auto read_results = backend_->BatchRead(
        {{"large", descriptor, {{read_buf.data(), read_buf.size()}}}});
    ASSERT_EQ(read_results.size(), 1);
    ASSERT_TRUE(read_results[0].has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), kLargeSize), 0);
}

TEST_F(DfsBackendTest, FailurePaths) {
    PosixFsAdapter adapter;
    ASSERT_TRUE(adapter.Init(tmp_->path()).has_value());
    char buf[64] = {};
    iovec iov{buf, sizeof(buf)};
    auto write = adapter.WriteAt(-1, &iov, 1, 0);
    ASSERT_FALSE(write.has_value());
    EXPECT_EQ(write.error(), ErrorCode::INVALID_PARAMS);

    auto read =
        adapter.ReadFile(tmp_->file("does_not_exist"), buf, sizeof(buf));
    ASSERT_FALSE(read.has_value());
    EXPECT_EQ(read.error(), ErrorCode::FILE_NOT_FOUND);
}

TEST(DfsStorageBackendTest, OpensAndUsesShardsAcrossRoots) {
    TempDir root0("dfs_backend_root0");
    TempDir root1("dfs_backend_root1");

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root0.path();

    DistributedStorageConfig config;
    config.fsdir = root0.path();
    config.root_dirs = {root0.path(), root1.path()};
    config.fs_adapter_type = "posix";
    config.shard_count = 2;
    config.shard_capacity = 1024 * 1024;
    config.alignment = 4096;

    DistributedStorageBackend backend(file_config, config,
                                      std::make_unique<PosixFsAdapter>());
    ASSERT_TRUE(backend.Init());

    const std::string shard0 = root0.file(
        "dfs_shard_" + DfsGlobalAllocator::FormatShardIdx(0, 2) + ".data");
    const std::string shard1 = root1.file(
        "dfs_shard_" + DfsGlobalAllocator::FormatShardIdx(1, 2) + ".data");
    ASSERT_TRUE(std::filesystem::exists(shard0));
    ASSERT_TRUE(std::filesystem::exists(shard1));

    AlignedBuffer input0(4096);
    AlignedBuffer input1(4096);
    ASSERT_NE(input0.data(), nullptr);
    ASSERT_NE(input1.data(), nullptr);
    input0.Fill('A');
    input1.Fill('B');

    const DistributedFSDescriptor descriptor0{shard0, 0, input0.size(),
                                              input0.size(), 0};
    const DistributedFSDescriptor descriptor1{shard1, 0, input1.size(),
                                              input1.size(), 1};
    auto write_results = backend.BatchWrite(
        {{"key0", descriptor0, {{input0.data(), input0.size()}}},
         {"key1", descriptor1, {{input1.data(), input1.size()}}}});
    ASSERT_EQ(write_results.size(), 2);
    ASSERT_TRUE(write_results[0]);
    ASSERT_TRUE(write_results[1]);

    AlignedBuffer output0(4096);
    AlignedBuffer output1(4096);
    ASSERT_NE(output0.data(), nullptr);
    ASSERT_NE(output1.data(), nullptr);
    auto read_results = backend.BatchRead(
        {{"key0", descriptor0, {{output0.data(), output0.size()}}},
         {"key1", descriptor1, {{output1.data(), output1.size()}}}});
    ASSERT_EQ(read_results.size(), 2);
    ASSERT_TRUE(read_results[0]);
    ASSERT_TRUE(read_results[1]);
    EXPECT_EQ(std::memcmp(input0.data(), output0.data(), input0.size()), 0);
    EXPECT_EQ(std::memcmp(input1.data(), output1.data(), input1.size()), 0);
}

TEST(DfsStorageFactoryTest, CreatesDistributedBackendWithPosixAdapter) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_factory");
    env.Set("MOONCAKE_DFS_ROOT_DIR", tmp.path().c_str());
    env.Set("MOONCAKE_DFS_SHARD_COUNT", "2");
    env.Set("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    env.Set("MOONCAKE_DFS_ALIGNMENT", "4096");
    env.Set("MOONCAKE_DFS_SINGLE_TENANT", "true");

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = tmp.path();

    auto backend = CreateStorageBackend(file_config);
    ASSERT_TRUE(backend.has_value());
    ASSERT_NE(*backend, nullptr);
    EXPECT_TRUE((*backend)->Init().has_value());
}

}  // namespace mooncake::test
