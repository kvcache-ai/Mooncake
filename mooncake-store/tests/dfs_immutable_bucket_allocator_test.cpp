#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "config/distributed_storage_config.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/immutable_bucket_allocator.h"

namespace mooncake {
namespace {

class TempDir {
   public:
    TempDir() {
        static std::atomic<uint64_t> sequence{0};
        path_ =
            std::filesystem::temp_directory_path() /
            ("mooncake-bucket-allocator-" +
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
    std::filesystem::path file(const std::string& name) const {
        return path_ / name;
    }

   private:
    std::filesystem::path path_;
};

DistributedStorageConfig BucketConfig(const TempDir& dir) {
    DistributedStorageConfig config;
    config.fsdir = dir.string();
    config.fs_adapter_type = "posix";
    config.allocator_type = "bucket";
    config.alignment = 4096;
    config.bucket_capacity = 8192;
    config.max_bucket_count = 8;
    config.eviction_enabled = true;
    config.eviction_high_watermark = 0.01;
    config.eviction_low_watermark = 0.0;
    return config;
}

TEST(BucketEntryLayoutTest, ComputesValueAndPaddingWithOverflowChecks) {
    auto layout = ComputeBucketEntryLayout(1, 4097, 4096);
    ASSERT_TRUE(layout);
    EXPECT_EQ(layout->offset, 4096u);
    EXPECT_EQ(layout->object_size, 4097u);
    EXPECT_EQ(layout->aligned_size, 8192u);
    EXPECT_EQ(layout->end(), 12288u);

    const auto descriptor =
        MakeBucketDescriptor("/dfs/bucket_000007.data", *layout, 7);
    EXPECT_EQ(descriptor.file_path, "/dfs/bucket_000007.data");
    EXPECT_EQ(descriptor.offset, layout->offset);
    EXPECT_EQ(descriptor.object_size, layout->object_size);
    EXPECT_EQ(descriptor.aligned_size, layout->aligned_size);
    EXPECT_EQ(descriptor.shard_idx, 7);

    EXPECT_FALSE(CheckedAlignUp(std::numeric_limits<uint64_t>::max(), 4096));
    EXPECT_FALSE(ComputeBucketEntryLayout(0, 1, 3));
    EXPECT_FALSE(ComputeBucketEntryLayout(0, 0, 4096));
}

TEST(ImmutableBucketAllocatorTest, AppendsAndPacksBatchAcrossBuckets) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    const std::vector<BatchAllocateRequest> requests = {
        {"a", 3000}, {"b", 3000}, {"c", 3000}};
    auto results = allocator.BatchAllocate(requests);
    ASSERT_EQ(results.size(), requests.size());
    ASSERT_TRUE(results[0].success);
    ASSERT_TRUE(results[1].success);
    ASSERT_TRUE(results[2].success);
    EXPECT_EQ(results[0].descriptor.shard_idx, results[1].descriptor.shard_idx);
    EXPECT_NE(results[1].descriptor.shard_idx, results[2].descriptor.shard_idx);
    EXPECT_EQ(results[0].descriptor.offset, 0u);
    EXPECT_EQ(results[1].descriptor.offset, 4096u);
    EXPECT_EQ(results[2].descriptor.offset, 0u);
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
}

TEST(ImmutableBucketAllocatorTest, DuplicateBatchRollsBackWithoutOffsetReuse) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    auto rolled_back = allocator.BatchAllocate({{"same", 100}, {"same", 100}});
    ASSERT_EQ(rolled_back.size(), 2u);
    EXPECT_FALSE(rolled_back[0].success);
    EXPECT_FALSE(rolled_back[1].success);
    EXPECT_TRUE(rolled_back[0].descriptor.file_path.empty());
    EXPECT_FALSE(allocator.GetBucketIdForKey("same"));

    auto replacement = allocator.Allocate("same", 100);
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->offset, 4096u);
    EXPECT_FALSE(allocator.MarkCommitted("same", rolled_back[0].descriptor));
    EXPECT_TRUE(allocator.MarkCommitted("same", *replacement));
}

TEST(ImmutableBucketAllocatorTest, StaleFreeCannotTombstoneReplacement) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    auto first = allocator.Allocate("key", 100);
    ASSERT_TRUE(first);
    allocator.Free("key", *first);
    auto replacement = allocator.Allocate("key", 100);
    ASSERT_TRUE(replacement);
    ASSERT_NE(first->offset, replacement->offset);
    allocator.Free("key", *first);
    EXPECT_TRUE(allocator.MarkCommitted("key", *replacement));
}

TEST(ImmutableBucketAllocatorTest, PendingEntriesProtectSealedBucket) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto pending = allocator.Allocate("pending", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(pending);
    ASSERT_TRUE(active);
    EXPECT_TRUE(allocator.PrepareEvictionForAllocationFailure().Empty());

    ASSERT_TRUE(allocator.MarkCommitted("pending", *pending));
    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    ASSERT_EQ(eviction.Candidates().size(), 1u);
    EXPECT_EQ(eviction.Candidates()[0].key, "pending");
    allocator.AbortEviction(std::move(eviction));
}

TEST(ImmutableBucketAllocatorTest, RetiresWholeBucketAfterDelete) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto cold = allocator.Allocate("cold", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(cold);
    ASSERT_TRUE(active);
    ASSERT_TRUE(allocator.MarkCommitted("cold", *cold));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));
    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    const auto path = eviction.Candidates().front().descriptor.file_path;
    ASSERT_TRUE(allocator.CommitEviction(std::move(eviction)));
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(allocator.GetBucketCount(), 1u);
    EXPECT_FALSE(allocator.GetBucketIdForKey("cold"));
}

TEST(ImmutableBucketAllocatorTest, RejectsExistingBucketArtifacts) {
    TempDir dir;
    std::ofstream(dir.file("bucket_000000.data")) << "old";
    ImmutableBucketAllocator allocator;
    auto initialized = allocator.Init(BucketConfig(dir));
    ASSERT_FALSE(initialized);
    EXPECT_EQ(initialized.error(), ErrorCode::INVALID_PARAMS);
}

TEST(ImmutableBucketAllocatorTest, ConcurrentAllocationDoesNotOverlap) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096 * 16;
    config.max_bucket_count = 16;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    constexpr int kThreads = 8;
    constexpr int kPerThread = 16;
    std::mutex results_mutex;
    std::atomic<bool> allocation_failed{false};
    std::vector<DistributedFSDescriptor> descriptors;
    std::vector<std::thread> threads;
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key =
                    std::to_string(thread) + ":" + std::to_string(i);
                auto descriptor = allocator.Allocate(key, 1);
                if (!descriptor) {
                    allocation_failed.store(true);
                    return;
                }
                std::lock_guard lock(results_mutex);
                descriptors.push_back(*descriptor);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    ASSERT_FALSE(allocation_failed.load());
    ASSERT_EQ(descriptors.size(), kThreads * kPerThread);
    std::set<std::pair<int, uint64_t>> addresses;
    for (const auto& descriptor : descriptors) {
        EXPECT_TRUE(
            addresses.emplace(descriptor.shard_idx, descriptor.offset).second);
    }
}

}  // namespace
}  // namespace mooncake
