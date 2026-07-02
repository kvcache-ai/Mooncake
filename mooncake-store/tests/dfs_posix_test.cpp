#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "storage/distributed/dfs_descriptor_cache.h"
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
    env.Set("MOONCAKE_DFS_FS_ADAPTER", "posix");
    env.Set("MOONCAKE_DFS_EVICTION_ENABLED", "0");
    env.Set("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
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
    ASSERT_TRUE(alloc.Init(tmp.path(), 4, 1024 * 1024, 4096));

    auto desc = alloc.Allocate("key1", 100);
    ASSERT_TRUE(desc.has_value());
    EXPECT_EQ(desc->aligned_size, 4096);
    EXPECT_GE(desc->shard_idx, 0);
    EXPECT_LT(desc->shard_idx, 4);
    EXPECT_EQ(desc->offset % 4096, 0);

    alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx);
    auto desc2 = alloc.Allocate("key2", 100);
    EXPECT_TRUE(desc2.has_value());

    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(0, 64), "00");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(9, 64), "09");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(10, 64), "10");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(63, 64), "63");
    EXPECT_EQ(DfsGlobalAllocator::FormatShardIdx(100, 1000), "100");
}

TEST(DfsGlobalAllocatorTest, AllocateReservesAlignmentPadding) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_alloc_padding");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(alloc.Init(tmp.path(), 1, 8 * 1024, 4096));

    auto desc = alloc.Allocate("key1", 100);
    ASSERT_TRUE(desc.has_value());
    EXPECT_EQ(desc->aligned_size, 4096);
    EXPECT_EQ(desc->offset % 4096, 0);

    auto exhausted = alloc.Allocate("key2", 100);
    EXPECT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx);
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
    ASSERT_TRUE(alloc.Init(tmp.path(), 1, 32 * 1024, 4096));

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

    auto evicted = alloc.EvictIfNeeded();
    ASSERT_FALSE(evicted.empty());
    EXPECT_EQ(evicted.front().key, "k0");

    auto after_evict = alloc.Allocate("k_after_evict", 100);
    EXPECT_TRUE(after_evict.has_value());
}

TEST(DfsGlobalAllocatorTest, ConcurrentAllocate) {
    EnvGuard env;
    ConfigurePosixDfs(env);
    TempDir tmp("dfs_concurrent");

    DfsGlobalAllocator alloc;
    ASSERT_TRUE(alloc.Init(tmp.path(), 4, 128 * 1024, 4096));

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
                alloc.Free(desc->offset, desc->aligned_size, desc->shard_idx);
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

TEST(DfsDescCacheTest, BasicAndConcurrentOperations) {
    DfsDescriptorCache cache;
    DistributedFSDescriptor desc{"/mnt/3fs/shard0.data", 4096, 100, 4096, 0};
    cache.Put("key1", desc);
    auto got = cache.Get("key1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->offset, 4096);

    DistributedFSDescriptor overwritten{"/mnt/3fs/shard1.data", 8192, 200, 4096,
                                        1};
    cache.Put("key1", overwritten);
    got = cache.Get("key1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->offset, 8192);
    EXPECT_EQ(got->shard_idx, 1);

    cache.Remove("key1");
    EXPECT_FALSE(cache.Get("key1").has_value());
    cache.Remove("missing");

    for (int i = 0; i < 50; ++i) {
        cache.Put("key_" + std::to_string(i),
                  {"/mnt/3fs/shard0.data", static_cast<uint64_t>(i * 4096), 100,
                   4096, 0});
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&cache]() {
            for (int i = 0; i < 50; ++i) {
                cache.Get("key_" + std::to_string(i));
            }
        });
    }
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 50; i < 100; ++i) {
                cache.Put("key_" + std::to_string(i + t * 50),
                          {"/mnt/3fs/shard0.data",
                           static_cast<uint64_t>(i * 4096), 100, 4096, 0});
            }
        });
    }
    threads.emplace_back([&cache]() {
        for (int i = 0; i < 25; ++i) {
            cache.Remove("key_" + std::to_string(i));
        }
    });
    for (auto& thread : threads) thread.join();
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

        desc_cache_ = std::make_shared<DfsDescriptorCache>();
        backend_ = std::make_unique<DistributedStorageBackend>(
            file_config, distributed_config,
            std::make_unique<PosixFsAdapter>());
        backend_->SetDescriptorCache(desc_cache_);
        ASSERT_TRUE(backend_->Init().has_value());
    }

    void TearDown() override {
        backend_.reset();
        desc_cache_.reset();
        tmp_.reset();
    }

    std::string ShardPath(int shard_idx) const {
        return tmp_->file("dfs_shard_" +
                          DfsGlobalAllocator::FormatShardIdx(shard_idx, 4) +
                          ".data");
    }

    std::unique_ptr<TempDir> tmp_;
    std::unique_ptr<DistributedStorageBackend> backend_;
    std::shared_ptr<DfsDescriptorCache> desc_cache_;
};

TEST_F(DfsBackendTest, BatchOffloadAndBatchLoad) {
    AlignedBuffer write_buf(4096);
    ASSERT_NE(write_buf.data(), nullptr);
    write_buf.Fill('X');
    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch["key1"] = {{write_buf.data(), write_buf.size()}};

    desc_cache_->Put("key1", {ShardPath(0), 0, 4096, 4096, 0});

    std::vector<std::string> completed_keys;
    auto offload_res =
        backend_->BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                          std::vector<StorageObjectMetadata>&) {
            completed_keys = keys;
            return ErrorCode::OK;
        });
    ASSERT_TRUE(offload_res.has_value());
    EXPECT_EQ(*offload_res, 1);
    ASSERT_EQ(completed_keys.size(), 1);
    EXPECT_EQ(completed_keys[0], "key1");

    AlignedBuffer read_buf(4096);
    ASSERT_NE(read_buf.data(), nullptr);
    std::unordered_map<std::string, Slice> load_batch;
    load_batch["key1"] = {read_buf.data(), read_buf.size()};
    auto load_res = backend_->BatchLoad(load_batch);
    ASSERT_TRUE(load_res.has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), write_buf.size()),
              0);
}

TEST_F(DfsBackendTest, BatchOffloadAndBatchLoadAcceptUnalignedBuffers) {
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

    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch["unaligned"] = {{write_ptr, kObjectSize}};
    desc_cache_->Put("unaligned", {ShardPath(0), 0, kObjectSize, 4096, 0});

    std::vector<std::string> completed_keys;
    auto offload_res =
        backend_->BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                          std::vector<StorageObjectMetadata>&) {
            completed_keys = keys;
            return ErrorCode::OK;
        });
    ASSERT_TRUE(offload_res.has_value());
    EXPECT_EQ(*offload_res, 1);
    ASSERT_EQ(completed_keys.size(), 1);
    EXPECT_EQ(completed_keys[0], "unaligned");

    std::unordered_map<std::string, Slice> load_batch;
    load_batch["unaligned"] = {read_ptr, kObjectSize};
    auto load_res = backend_->BatchLoad(load_batch);
    ASSERT_TRUE(load_res.has_value());
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

    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch["key0"] = {{key0.data(), key0.size()}};
    batch["key1"] = {{key1.data(), key1.size()}};
    batch["key2"] = {{key2.data(), key2.size()}};

    desc_cache_->Put("key0", {ShardPath(0), 0, key0.size(), key0.size(), 0});
    desc_cache_->Put("key1", {ShardPath(0), 4096, key1.size(), key1.size(), 0});
    desc_cache_->Put("key2", {ShardPath(1), 0, key2.size(), key2.size(), 1});

    auto offload_res = backend_->BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_res.has_value());
    EXPECT_EQ(*offload_res, 3);

    AlignedBuffer out0(4096), out1(4096), out2(8192);
    ASSERT_NE(out0.data(), nullptr);
    ASSERT_NE(out1.data(), nullptr);
    ASSERT_NE(out2.data(), nullptr);
    std::unordered_map<std::string, Slice> load_batch;
    load_batch["key0"] = {out0.data(), out0.size()};
    load_batch["key1"] = {out1.data(), out1.size()};
    load_batch["key2"] = {out2.data(), out2.size()};
    auto load_res = backend_->BatchLoad(load_batch);
    ASSERT_TRUE(load_res.has_value());
    EXPECT_EQ(std::memcmp(key0.data(), out0.data(), key0.size()), 0);
    EXPECT_EQ(std::memcmp(key1.data(), out1.data(), key1.size()), 0);
    EXPECT_EQ(std::memcmp(key2.data(), out2.data(), key2.size()), 0);
}

TEST_F(DfsBackendTest, LargeObject) {
    constexpr size_t kLargeSize = 33 * 1024 * 1024;
    AlignedBuffer write_buf(kLargeSize);
    ASSERT_NE(write_buf.data(), nullptr);
    write_buf.Fill('L');
    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch["large"] = {{write_buf.data(), write_buf.size()}};
    desc_cache_->Put("large",
                     {ShardPath(2), 0, write_buf.size(), write_buf.size(), 2});

    auto offload_res = backend_->BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_res.has_value());
    EXPECT_EQ(*offload_res, 1);

    AlignedBuffer read_buf(kLargeSize);
    ASSERT_NE(read_buf.data(), nullptr);
    std::unordered_map<std::string, Slice> load_batch;
    load_batch["large"] = {read_buf.data(), read_buf.size()};
    auto load_res = backend_->BatchLoad(load_batch);
    ASSERT_TRUE(load_res.has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), kLargeSize), 0);
}

TEST_F(DfsBackendTest, FailurePaths) {
    AlignedBuffer read_buf(4096);
    ASSERT_NE(read_buf.data(), nullptr);
    std::unordered_map<std::string, Slice> load_batch;
    load_batch["missing_key"] = {read_buf.data(), read_buf.size()};
    auto load_res = backend_->BatchLoad(load_batch);
    ASSERT_FALSE(load_res.has_value());
    EXPECT_EQ(load_res.error(), ErrorCode::OBJECT_NOT_FOUND);

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
