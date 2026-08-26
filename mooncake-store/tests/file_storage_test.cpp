#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>

#include "allocator.h"
#include "client_metric.h"
#include "file_storage.h"
#include "storage_backend.h"
#include "tenant_id.h"
#include "test_server_helpers.h"
#include "utils.h"
#include "utils/common.h"

namespace mooncake {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

void UnsetEnv(const std::string& key) { unsetenv(key.c_str()); }

class FileStorageTest : public ::testing::Test {
   protected:
    std::string data_path;
    void SetUp() override {
        google::InitGoogleLogging("FileStorageTest");
        FLAGS_logtostderr = true;
        UnsetEnv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR");
        UnsetEnv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
        UnsetEnv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES");
        UnsetEnv("MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS");
        UnsetEnv("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS");
        UnsetEnv("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS");
        UnsetEnv("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION");
        UnsetEnv("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO");
        UnsetEnv("MOONCAKE_OFFLOAD_USE_URING");
        UnsetEnv("MOONCAKE_USE_URING");
        data_path = (std::filesystem::current_path() / "file_storage_test_data")
                        .string();
        fs::create_directories(data_path);
        for (const auto& entry : fs::directory_iterator(data_path)) {
            std::error_code ec;
            if (entry.is_regular_file()) {
                fs::remove(entry.path(), ec);
            } else if (entry.is_directory()) {
                fs::remove_all(entry.path(), ec);
            }
        }
    }

    tl::expected<void, ErrorCode> FileStorageBatchOffload(
        FileStorage& fileStorage, std::vector<std::string>& keys,
        std::vector<int64_t>& sizes,
        std::unordered_map<std::string, std::string>& batch_data) {
        std::vector<int64_t> buckets;
        return BatchOffloadUtil(*fileStorage.storage_backend_, keys, sizes,
                                batch_data, buckets);
    }

    tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
    FileStorageAllocateBatch(FileStorage& fileStorage,
                             const std::vector<std::string>& keys,
                             const std::vector<int64_t>& sizes) {
        return fileStorage.AllocateBatch(keys, sizes,
                                         *fileStorage.client_buffer_allocator_);
    }

    void SetPinnedRestoreArena(FileStorage& fileStorage, void* address,
                               size_t size) {
        fileStorage.pinned_restore_arena_allocator_ =
            ClientBufferAllocator::create(address, size);
    }

    tl::expected<void, ErrorCode> FileStorageBatchLoad(
        FileStorage& fileStorage,
        std::unordered_map<std::string, Slice>& batch_object) {
        return fileStorage.BatchLoad(batch_object);
    }

    tl::expected<bool, ErrorCode> FileStorageIsEnableOffloading(
        FileStorage& fileStorage) {
        return fileStorage.IsEnableOffloading();
    }

    tl::expected<void, ErrorCode> FileStorageNotifyEvictedDiskReplicas(
        FileStorage& fileStorage,
        const std::vector<std::string>& evicted_keys) {
        return fileStorage.NotifyEvictedDiskReplicas(evicted_keys);
    }

    tl::expected<void, ErrorCode> FileStorageGroupOffloadingKeysByBucket(
        FileStorage& fileStorage,
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return bucket_backend->AllocateOffloadingBuckets(offloading_objects,
                                                         buckets_keys);
    }

    size_t GetUngroupedOffloadingObjectsSize(FileStorage& fileStorage) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return 0;
        }
        return bucket_backend->UngroupedOffloadingObjectsSize();
    }

    // Static funnel to the private FileStorage::IsPerBucketSoftOffloadError.
    // FileStorageTest is friended; TEST_F-generated subclasses are not.
    static bool CallIsPerBucketSoftOffloadError(ErrorCode error) {
        return FileStorage::IsPerBucketSoftOffloadError(error);
    }

    // Funnel to the private FileStorage::Heartbeat, for the drain tests.
    static tl::expected<void, ErrorCode> FileStorageHeartbeat(
        FileStorage& fileStorage) {
        return fileStorage.Heartbeat();
    }

    // Drives the drain-vs-heartbeat interleaving deterministically: a
    // heartbeat tick that has already passed its entry draining_ check parks
    // on offloading_mutex_, a drain runs, and the parked tick resumes only
    // around the drain's own critical section. The fixture holds the mutex
    // as the scheduler while both threads are launched, so the tick is
    // provably not inside its RPC section when the drain latches. After the
    // release the two race for the lock; the postcondition (segment
    // deregistered, never re-mounted) must hold in both wake orders, so the
    // assertions do not depend on the schedule. Without the drain taking
    // offloading_mutex_, the parked tick would resume after the drain
    // finished, get SEGMENT_NOT_FOUND, and re-mount the segment.
    tl::expected<void, ErrorCode> DrainWhileHeartbeatTickParked(
        FileStorage& fileStorage) {
        // The parked tick may win the lock and run a full pass, which is
        // only supported on an initialized backend (Init() is also what
        // starts the heartbeat thread in production).
        EXPECT_TRUE(fileStorage.storage_backend_->Init());
        tl::expected<void, ErrorCode> drain_result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        std::thread heartbeat_thread;
        std::thread drain_thread;
        {
            MutexLocker scheduler(&fileStorage.offloading_mutex_);
            heartbeat_thread =
                std::thread([&fileStorage] { fileStorage.Heartbeat(); });
            // Let the tick pass the entry check and park on the mutex we
            // hold. The assertions do not depend on it parking -- a tick
            // that has not reached the lock is caught by the entry check
            // instead -- the pause just pins the adversarial schedule.
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            drain_thread = std::thread([&fileStorage, &drain_result] {
                drain_result = fileStorage.DrainLocalDiskSegment(0);
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        heartbeat_thread.join();
        drain_thread.join();
        return drain_result;
    }

    void AssertHeartbeatEvictsAllKeys(
        FileStorage& fileStorage, const std::vector<std::string>& expected_keys,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batch_object) {
        ASSERT_TRUE(fileStorage.storage_backend_->Init());
        {
            MutexLocker locker(&fileStorage.offloading_mutex_);
            fileStorage.enable_offloading_ = true;
        }

        auto offload_result = fileStorage.storage_backend_->BatchOffload(
            batch_object,
            [&fileStorage](const std::vector<std::string>& keys,
                           std::vector<StorageObjectMetadata>& metadatas) {
                for (auto& metadata : metadatas) {
                    metadata.transport_endpoint = fileStorage.local_rpc_addr_;
                }
                auto result =
                    fileStorage.client_->NotifyOffloadSuccess(keys, metadatas);
                if (!result) {
                    return result.error();
                }
                return ErrorCode::OK;
            });
        ASSERT_TRUE(offload_result.has_value());
        ASSERT_EQ(offload_result.value(),
                  static_cast<int64_t>(expected_keys.size()));

        for (const auto& key : expected_keys) {
            auto query_result = fileStorage.client_->Query(key);
            ASSERT_TRUE(query_result.has_value());
            bool has_local_disk_replica = false;
            for (const auto& replica : query_result->replicas) {
                has_local_disk_replica |= replica.is_local_disk_replica();
            }
            EXPECT_TRUE(has_local_disk_replica);
        }

        auto heartbeat_result = fileStorage.Heartbeat();
        ASSERT_TRUE(heartbeat_result.has_value());

        for (const auto& key : expected_keys) {
            auto exists = fileStorage.storage_backend_->IsExist(key);
            ASSERT_TRUE(exists.has_value());
            EXPECT_FALSE(exists.value());

            auto query_result = fileStorage.client_->Query(key);
            ASSERT_FALSE(query_result.has_value());
            EXPECT_EQ(query_result.error(), ErrorCode::OBJECT_NOT_FOUND);
        }
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        LOG(INFO) << "Clear test data...";
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
    }
};

TEST_F(FileStorageTest, IsEnableOffloading) {
    std::unordered_map<std::string, std::string> all_object;
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FileStorage fileStorage1(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage1, keys, sizes, batch_data));
    auto enable_offloading_result1 =
        FileStorageIsEnableOffloading(fileStorage1);
    ASSERT_TRUE(enable_offloading_result1 && enable_offloading_result1.value());

    // bucket_keys_limit/bucket_size_limit moved to BucketBackendConfig.
    // With current semantics, backend prevents offloading once it would exceed
    // limits, so we validate IsEnableOffloading directly under tight limits.

    // Case 2: total_keys_limit < bucket_keys_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    file_storage_config.total_keys_limit = 9;
    FileStorage fileStorage2(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result2 =
        FileStorageIsEnableOffloading(fileStorage2);
    ASSERT_TRUE(enable_offloading_result2 &&
                !enable_offloading_result2.value());

    // Case 3: total_size_limit < bucket_size_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "969");
    file_storage_config.total_keys_limit = 10'000'000;
    file_storage_config.total_size_limit = 100;
    FileStorage fileStorage3(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result3 =
        FileStorageIsEnableOffloading(fileStorage3);
    ASSERT_TRUE(enable_offloading_result3 &&
                !enable_offloading_result3.value());
}

TEST_F(FileStorageTest, BatchGetUsesPinnedArenaAndFallsBackWhenFull) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));

    size_t payload_size = 0;
    for (const auto size : sizes) {
        payload_size += static_cast<size_t>(size);
    }
    const size_t arena_size = payload_size * 3 / 2;
    std::vector<char> restore_arena(arena_size + 4096);
    const auto arena_begin =
        (reinterpret_cast<uintptr_t>(restore_arena.data()) + 4095) & ~4095ULL;
    SetPinnedRestoreArena(fileStorage, reinterpret_cast<void*>(arena_begin),
                          arena_size);

    auto pinned_result = fileStorage.BatchGetLocal(keys, sizes);
    ASSERT_TRUE(pinned_result);
    ASSERT_EQ(pinned_result->pointers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_GE(pinned_result->pointers[i], arena_begin);
        EXPECT_LT(pinned_result->pointers[i], arena_begin + arena_size);
        EXPECT_EQ(
            std::string(reinterpret_cast<char*>(pinned_result->pointers[i]),
                        sizes[i]),
            batch_data.at(keys[i]));
    }

    auto fallback_result = fileStorage.BatchGetLocal(keys, sizes);
    ASSERT_TRUE(fallback_result);
    ASSERT_EQ(fallback_result->pointers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_TRUE(fallback_result->pointers[i] < arena_begin ||
                    fallback_result->pointers[i] >= arena_begin + arena_size);
        EXPECT_EQ(
            std::string(reinterpret_cast<char*>(fallback_result->pointers[i]),
                        sizes[i]),
            batch_data.at(keys[i]));
    }
}

TEST_F(FileStorageTest, AllocateBatchAvoidsDirectIoPaddingForPosixReads) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 64 * 1024;
    file_storage_config.use_uring = false;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    for (size_t i = 0; i < 16; ++i) {
        keys.emplace_back("key" + std::to_string(i));
        sizes.emplace_back(4 * 1024);
    }

    auto allocate_result = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_result)
        << "POSIX reads should not reserve O_DIRECT padding";
    EXPECT_EQ(allocate_result.value()->total_size,
              file_storage_config.local_buffer_size);
    ASSERT_EQ(allocate_result.value()->handles.size(), keys.size());
    for (const auto& handle : allocate_result.value()->handles) {
        EXPECT_EQ(handle.size(), 4 * 1024);
    }
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.scanmeta_iterator_keys_limit = 969;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_deduplicates_carryover) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    offloading_objects.emplace("duplicate", 1);

    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_TRUE(buckets_keys.empty());
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 1);

    for (size_t i = 0; i < 9; ++i) {
        offloading_objects.emplace("new" + std::to_string(i), 1);
    }
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));

    ASSERT_EQ(buckets_keys.size(), 1);
    ASSERT_EQ(buckets_keys.front().size(), 10);
    std::unordered_set<std::string> unique_keys(buckets_keys.front().begin(),
                                                buckets_keys.front().end());
    EXPECT_EQ(unique_keys.size(), 10);
    EXPECT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_size_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_bucket_size_limit_and_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 500; i++) {
        offloading_objects.emplace("test" + std::to_string(i), i);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "9");
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "496");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < buckets_keys.size(); i++) {
        auto bucket_keys = buckets_keys.at(i);
        ASSERT_TRUE(bucket_keys.size() <= 9);
        size_t total_size = 0;
        std::string keys;
        for (const auto& bucket_key : bucket_keys) {
            total_size += offloading_objects.at(bucket_key);
            keys += bucket_key + ",";
        }
        ASSERT_TRUE(total_size <= 496);
    }
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_ungrouped_offloading_objects) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 1; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    offloading_objects.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < 7; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
}

TEST_F(FileStorageTest, ReadBucketBackendValues) {
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1000");
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "536870912");

    const auto config = BucketBackendConfig::FromEnvironment();
    EXPECT_EQ(config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.bucket_size_limit, 512 * 1024 * 1024);
}

TEST_F(FileStorageTest, HeartbeatRunsDiskWatermarkEvictionWithoutOffloadWork) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "heartbeat_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    auto mount_result = client.value()->MountLocalDiskSegment(true);
    ASSERT_TRUE(mount_result.has_value())
        << "MountLocalDiskSegment failed: " << toString(mount_result.error());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/heartbeat_watermark";
    config.local_buffer_size = 4 * 1024 * 1024;
    config.disk_eviction_high_watermark_ratio = 1e-12;
    config.disk_eviction_low_watermark_ratio = 0.5e-12;
    fs::create_directories(config.storage_filepath);

    FileStorage file_storage(config, client.value(), local_rpc_addr);

    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<std::unique_ptr<char[]>> buffers;
    std::vector<std::string> expected_keys = {
        "heartbeat_key_1", "heartbeat_key_2", "heartbeat_key_3"};
    for (size_t i = 0; i < expected_keys.size(); ++i) {
        auto buffer = std::make_unique<char[]>(512);
        std::memset(buffer.get(), static_cast<int>('a' + i), 512);
        batch_object.emplace(expected_keys[i],
                             std::vector<Slice>{Slice{buffer.get(), 512}});
        buffers.push_back(std::move(buffer));
    }

    AssertHeartbeatEvictsAllKeys(file_storage, expected_keys, batch_object);
}

// A drain deregisters the disk tier; the heartbeat re-mounts the segment
// whenever the master answers SEGMENT_NOT_FOUND. These pin the interleavings
// where the two overlap: the drain must win in every schedule.

TEST_F(FileStorageTest, HeartbeatAfterDrainDoesNotRemount) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "drain_hb_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/drain_hb";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    ASSERT_TRUE(file_storage.DrainLocalDiskSegment(0).has_value());

    // A tick after the drain is a no-op: it must not take the
    // SEGMENT_NOT_FOUND recovery branch and re-mount the segment.
    ASSERT_TRUE(FileStorageHeartbeat(file_storage).has_value());

    std::vector<OffloadTaskItem> offload_items;
    auto heartbeat_rpc =
        client.value()->OffloadObjectHeartbeat(true, offload_items);
    ASSERT_FALSE(heartbeat_rpc.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, heartbeat_rpc.error());
}

TEST_F(FileStorageTest, DrainSurvivesParkedHeartbeatTick) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "drain_race_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/drain_race";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    // A tick that passed its entry draining_ check is parked on
    // offloading_mutex_ while the drain runs (see the fixture helper). It
    // must not resume into the SEGMENT_NOT_FOUND recovery branch and
    // re-mount the segment the drain deregistered.
    auto drain_result = DrainWhileHeartbeatTickParked(file_storage);
    ASSERT_TRUE(drain_result.has_value());

    std::vector<OffloadTaskItem> offload_items;
    auto heartbeat_rpc =
        client.value()->OffloadObjectHeartbeat(true, offload_items);
    ASSERT_FALSE(heartbeat_rpc.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, heartbeat_rpc.error());
}

TEST_F(FileStorageTest, NotifyEvictedDiskReplicasUsesTenantScopedKeys) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "tenant_notify_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    auto mount_result = client.value()->MountLocalDiskSegment(true);
    ASSERT_TRUE(mount_result.has_value())
        << "MountLocalDiskSegment failed: " << toString(mount_result.error());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/tenant_notify";
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);

    const std::string key = "shared_key";
    std::vector<OffloadTaskItem> tasks = {
        {.tenant_id = "tenant_a", .key = key, .size = 128},
        {.tenant_id = "tenant_b", .key = key, .size = 128},
    };
    std::vector<StorageObjectMetadata> metadatas;
    metadatas.reserve(tasks.size());
    for (const auto& task : tasks) {
        metadatas.push_back(StorageObjectMetadata{
            .bucket_id = 0,
            .offset = 0,
            .key_size = static_cast<int64_t>(task.key.size()),
            .data_size = task.size,
            .transport_endpoint = local_rpc_addr,
        });
    }
    ASSERT_TRUE(client.value()->NotifyOffloadSuccess(tasks, metadatas));

    for (const auto& task : tasks) {
        auto before = client.value()->BatchQuery({key}, task.tenant_id);
        ASSERT_EQ(before.size(), 1);
        ASSERT_TRUE(before[0].has_value());
        bool has_local_disk_replica = false;
        for (const auto& replica : before[0]->replicas) {
            has_local_disk_replica |= replica.is_local_disk_replica();
        }
        ASSERT_TRUE(has_local_disk_replica);
    }

    auto notify_result = FileStorageNotifyEvictedDiskReplicas(
        file_storage, {TenantId("tenant_a").MakeScopedKey(key),
                       TenantId("tenant_b").MakeScopedKey(key)});
    ASSERT_TRUE(notify_result.has_value());

    for (const auto& task : tasks) {
        auto after = client.value()->BatchQuery({key}, task.tenant_id);
        ASSERT_EQ(after.size(), 1);
        ASSERT_FALSE(after[0].has_value());
        EXPECT_EQ(after[0].error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

// Regression test for issue #2827: under concurrent/repeat offload of the same
// keys, the bucket backend rejects a whole bucket atomically with
// OBJECT_ALREADY_EXISTS (see BucketStorageBackend duplicate-key tests). That
// error must be treated as a per-bucket soft failure so OffloadObjects reports
// the keys back to the master and continues, rather than aborting the whole
// offload cycle and leaving master/SSD metadata inconsistent (which surfaced as
// spurious INVALID_KEY on the read path). It stays alongside INVALID_READ,
// while genuinely fatal errors (e.g. KEYS_ULTRA_LIMIT, INTERNAL_ERROR) do not.
TEST_F(FileStorageTest, DuplicateOffloadErrorIsPerBucketSoftError) {
    EXPECT_TRUE(
        CallIsPerBucketSoftOffloadError(ErrorCode::OBJECT_ALREADY_EXISTS));
    EXPECT_TRUE(CallIsPerBucketSoftOffloadError(ErrorCode::INVALID_READ));

    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::KEYS_ULTRA_LIMIT));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::INTERNAL_ERROR));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::INVALID_KEY));
    EXPECT_FALSE(CallIsPerBucketSoftOffloadError(ErrorCode::OK));
}

TEST_F(FileStorageTest, BatchLoad_WithStorageBackendAdaptor) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_backend_type = StorageBackendType::kFilePerKey;
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "FileStorageTestDir";

    auto total_path = fs::path(data_path) / file_per_key_config.fsdir;
    fs::create_directories(total_path);

    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    auto offload_res =
        FileStorageBatchOffload(fileStorage, keys, sizes, batch_data);
    ASSERT_TRUE(offload_res) << "FileStorageBatchOffload failed";

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res) << "FileStorageAllocateBatch failed";

    auto batch = std::move(allocate_res.value());

    auto load_res = FileStorageBatchLoad(fileStorage, batch->slices);
    ASSERT_TRUE(load_res) << "FileStorageBatchLoad failed";

    for (const auto& it : batch->slices) {
        const std::string& key = it.first;
        const Slice& slice = it.second;
        std::string data(static_cast<char*>(slice.ptr), slice.size);

        auto found = batch_data.find(key);
        ASSERT_TRUE(found != batch_data.end())
            << "key not found in batch_data: " << key;
        EXPECT_EQ(data, found->second);
    }
}

TEST_F(FileStorageTest, BatchLoadRecordsSsdMetrics) {
    // Setup: write data to storage backend via BatchOffloadUtil
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // Create FileStorage WITH SsdMetric
    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Write test data to disk
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    // Allocate buffers and call BatchLoad (read path)
    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);

    // Verify SSD read metrics were recorded
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(),
              static_cast<int64_t>(allocate_res.value()->slices.size()));

    // Verify bytes: sum of all slice sizes
    int64_t expected_bytes = 0;
    for (const auto& [key, slice] : allocate_res.value()->slices) {
        expected_bytes += slice.size;
    }
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), expected_bytes);
    EXPECT_GT(expected_bytes, 0);

    // Verify latency histogram has exactly 1 observation (one BatchLoad call)
    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 1);

    // Write metrics should remain 0 (BatchOffloadUtil bypasses FileStorage)
    EXPECT_EQ(ssd_metric.ssd_write_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_write_bytes.value(), 0);

    LOG(INFO) << "SSD read metrics after BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, BatchLoadFailureDoesNotRecordSsdMetrics) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Init storage backend so we can call BatchLoad
    // But load with keys that don't exist on disk -> should fail
    std::unordered_map<std::string, Slice> batch_object;
    char dummy_buf[4096] = {};
    batch_object["nonexistent_key_1"] = Slice{dummy_buf, 4096};
    batch_object["nonexistent_key_2"] = Slice{dummy_buf, 8192};

    auto result = FileStorageBatchLoad(fileStorage, batch_object);
    // Expect failure (keys don't exist on disk)
    EXPECT_FALSE(result);

    // Metrics should remain 0 - failed operations are not counted
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), 0);

    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 0);

    LOG(INFO) << "SSD metrics after failed BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, NullSsdMetricDoesNotCrash) {
    // FileStorage with nullptr SsdMetric should work fine (no metrics recorded)
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // nullptr SsdMetric (default)
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);
    // No crash = success. No metrics pointer, so nothing to verify.
}

// Issue #3709: a RemoveAll-style physical wipe of the SSD files while master
// still tracks the key's LOCAL_DISK replica leaves a dangling entry. A Put
// for the same key must not report success while storing nothing: the client
// evicts the dangling replica and retries PutStart, so the new data is
// actually written.
TEST_F(FileStorageTest, PutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "heal_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir(master_root.string())
                             .build();
    ASSERT_TRUE(master.Start(master_config));

    std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    // Memory segment and registered buffers, so the retried PutStart can
    // allocate a memory replica and Get can read it back.
    constexpr size_t kSegSize = 64 * 1024 * 1024;
    void* seg_ptr = allocate_buffer_allocator_memory(kSegSize);
    ASSERT_NE(seg_ptr, nullptr);
    ASSERT_TRUE(
        client.value()->MountSegment(seg_ptr, kSegSize, "tcp").has_value());
    SimpleAllocator allocator(16 * 1024 * 1024);
    ASSERT_TRUE(client.value()
                    ->RegisterLocalMemory(allocator.getBase(), 16 * 1024 * 1024,
                                          "cpu:0", false, false)
                    .has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/heal_ssd";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);
    ASSERT_TRUE(file_storage.storage_backend_->Init());
    {
        MutexLocker locker(&file_storage.offloading_mutex_);
        file_storage.enable_offloading_ = true;
    }

    // Offload one key so master tracks a LOCAL_DISK replica backed by a real
    // file on the client SSD.
    const std::string key = "heal_key";
    std::string original(512, 'x');
    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    batch_object.emplace(key, std::vector<Slice>{Slice{original.data(), 512}});
    auto offload_result = file_storage.storage_backend_->BatchOffload(
        batch_object,
        [&file_storage](const std::vector<std::string>& keys,
                        std::vector<StorageObjectMetadata>& metadatas) {
            for (auto& metadata : metadatas) {
                metadata.transport_endpoint = file_storage.local_rpc_addr_;
            }
            auto result =
                file_storage.client_->NotifyOffloadSuccess(keys, metadatas);
            return result ? ErrorCode::OK : result.error();
        });
    ASSERT_TRUE(offload_result.has_value());

    auto query = client.value()->Query(key);
    ASSERT_TRUE(query.has_value());
    bool has_local_disk = false;
    for (const auto& replica : query->replicas) {
        has_local_disk |= replica.is_local_disk_replica();
    }
    ASSERT_TRUE(has_local_disk);

    // Simulate the #3709 divergence: the SSD file is wiped physically while
    // master keeps the metadata.
    file_storage.storage_backend_->RemoveAll();
    auto exists = file_storage.storage_backend_->IsExist(key);
    ASSERT_TRUE(exists.has_value());
    ASSERT_FALSE(exists.value());
    query = client.value()->Query(key);
    ASSERT_TRUE(query.has_value());

    // Put the same key again: the dangling replica must be evicted and the
    // Put must really store new data.
    void* buf = allocator.allocate(512);
    ASSERT_NE(buf, nullptr);
    std::string updated(512, 'y');
    std::memcpy(buf, updated.data(), 512);
    std::vector<Slice> slices{Slice{buf, 512}};
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto put = client.value()->Put(key, slices, cfg);
    ASSERT_TRUE(put.has_value())
        << "Put over a dangling replica failed: " << toString(put.error());
    allocator.deallocate(buf, 512);

    // The new data is readable through the normal Get path.
    void* read_buf = allocator.allocate(512);
    ASSERT_NE(read_buf, nullptr);
    std::vector<Slice> read_slices{Slice{read_buf, 512}};
    auto get = client.value()->Get(key, read_slices);
    ASSERT_TRUE(get.has_value())
        << "Get after self-heal failed: " << toString(get.error());
    EXPECT_EQ(std::memcmp(read_slices[0].ptr, updated.data(), 512), 0);
    allocator.deallocate(read_buf, 512);

    auto unmount = client.value()->UnmountSegment(seg_ptr, kSegSize);
    EXPECT_TRUE(unmount.has_value());
    std::free(seg_ptr);
}

}  // namespace mooncake
