#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <unordered_set>
#include <string>
#include <thread>

#include "allocator.h"
#include "client_metric.h"
#include "common/network.h"
#include "file_storage.h"
#include "storage_backend.h"
#include "tenant_id.h"
#include "test_server_helpers.h"
#include "common/client_buffer_allocation.h"
#include "utils/common.h"

namespace mooncake {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

void UnsetEnv(const std::string& key) { unsetenv(key.c_str()); }

// Commit another offload after grouping but before this batch reaches the
// backend. This deterministically exercises the duplicate race from #4097
// without relying on a second thread winning a filesystem/RPC race.
class ConcurrentOffloadBackend : public BucketStorageBackend {
   public:
    ConcurrentOffloadBackend(const FileStorageConfig& config,
                             const BucketBackendConfig& bucket_config,
                             std::unordered_set<std::string> duplicate_keys,
                             bool fail_write)
        : BucketStorageBackend(config, bucket_config),
          duplicate_keys_(std::move(duplicate_keys)),
          fail_write_(fail_write) {}

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>
            complete,
        EvictionHandler eviction = nullptr) override {
        std::unordered_map<std::string, std::vector<Slice>> duplicates;
        for (const auto& key : duplicate_keys_) {
            duplicates.emplace(key, batch.at(key));
        }
        if (!duplicates.empty()) {
            // The local commit survives even if its Master notification was
            // lost. The retry must release its own task without claiming a
            // new disk replica for the skipped write.
            auto result =
                BucketStorageBackend::BatchOffload(duplicates, nullptr);
            if (!result) return result;
        }
        if (fail_write_) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        return BucketStorageBackend::BatchOffload(batch, std::move(complete),
                                                  std::move(eviction));
    }

   private:
    std::unordered_set<std::string> duplicate_keys_;
    bool fail_write_;
};

class FileStorageTest : public ::testing::Test {
   protected:
    std::string data_path;

    void RunDeferredOffloadFailureScenario(bool capacity_limit) {
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build()));
        constexpr size_t kSegmentSize = 16 * 1024 * 1024;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(kSegmentSize), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(kSegmentSize);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client_result =
            Client::Create(endpoint, master.metadata_url(), "tcp", std::nullopt,
                           master.master_address());
        ASSERT_TRUE(client_result.has_value());
        auto client = client_result.value();
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));

        ReplicateConfig replicate;
        replicate.replica_num = 1;
        constexpr size_t kValueSize = 128;
        void* buffer = allocator.allocate(kValueSize);
        ASSERT_NE(buffer, nullptr);
        std::memset(buffer, 'a', kValueSize);
        std::vector<Slice> slices{{buffer, kValueSize}};
        for (size_t i = 0; i < 5; ++i) {
            ASSERT_TRUE(client->Put("deferred_" + std::to_string(i), slices,
                                    replicate));
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), 5u);

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kBucket;
        config.storage_filepath = data_path + "/deferred_failure";
        config.local_buffer_size = 1024 * 1024;
        // Two-key buckets: the first fits, the second exceeds capacity, and
        // the final key is deferred before either write starts.
        config.total_keys_limit = 2;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 2;
        bucket_config.flush_partial_buckets = false;
        std::shared_ptr<BucketStorageBackend> backend;
        if (capacity_limit) {
            backend =
                std::make_shared<BucketStorageBackend>(config, bucket_config);
        } else {
            backend = std::make_shared<ConcurrentOffloadBackend>(
                config, bucket_config, std::unordered_set<std::string>{}, true);
        }
        file_storage.storage_backend_ = backend;
        file_storage.enable_offloading_ = true;
        ASSERT_TRUE(backend->Init());

        // Carry one real master task across a heartbeat boundary first.
        ASSERT_TRUE(file_storage.OffloadObjects({tasks.front()}));
        ASSERT_EQ(file_storage.deferred_task_by_storage_key_.size(), 1u);
        ASSERT_EQ(backend->UngroupedOffloadingObjectsSize(), 1u);
        auto blocked = client->Upsert(tasks.front().key, slices, replicate);
        ASSERT_FALSE(blocked.has_value());
        EXPECT_EQ(blocked.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);

        auto result = file_storage.OffloadObjects(
            std::vector<OffloadTaskItem>(tasks.begin() + 1, tasks.end()));
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), capacity_limit ? ErrorCode::KEYS_ULTRA_LIMIT
                                                 : ErrorCode::FILE_WRITE_FAIL);
        EXPECT_EQ(file_storage.enable_offloading_, !capacity_limit);
        EXPECT_EQ(file_storage.deferred_task_by_storage_key_.size(),
                  capacity_limit ? 0u : 1u);
        EXPECT_EQ(backend->UngroupedOffloadingObjectsSize(),
                  capacity_limit ? 0u : 1u);

        size_t persisted_count = 0;
        for (const auto& task : tasks) {
            const auto storage_key =
                TenantId(task.tenant_id).MakeScopedKey(task.key);
            auto exists = backend->IsExist(storage_key);
            ASSERT_TRUE(exists.has_value());
            persisted_count += exists.value();
            // A transient write error must keep the tail pinned for retry;
            // a capacity shutdown must release every task immediately.
            auto overwritten = client->Upsert(task.key, slices, replicate);
            if (file_storage.deferred_task_by_storage_key_.count(storage_key)) {
                ASSERT_FALSE(overwritten.has_value());
                EXPECT_EQ(overwritten.error(),
                          ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            } else {
                EXPECT_TRUE(overwritten.has_value()) << task.key;
            }
        }
        EXPECT_EQ(persisted_count, capacity_limit ? 2u : 0u);
        allocator.deallocate(buffer, kValueSize);
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunSkippedOffloadScenario(
        size_t duplicate_count, bool fail_write,
        StorageBackendType backend_type = StorageBackendType::kBucket) {
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build()));
        constexpr size_t kSegmentSize = 16 * 1024 * 1024;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(kSegmentSize), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(kSegmentSize);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client_result =
            Client::Create(endpoint, master.metadata_url(), "tcp", std::nullopt,
                           master.master_address());
        ASSERT_TRUE(client_result.has_value());
        auto client = client_result.value();
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));

        const std::vector<std::string> keys = {"duplicate_key", "other_key"};
        std::vector<std::string> values = {std::string(128, 'a'),
                                           std::string(128, 'b')};
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buffer = allocator.allocate(values[i].size());
            ASSERT_NE(buffer, nullptr);
            std::memcpy(buffer, values[i].data(), values[i].size());
            std::vector<Slice> slices{{buffer, values[i].size()}};
            ASSERT_TRUE(client->Put(keys[i], slices, replicate));
            allocator.deallocate(buffer, values[i].size());
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), keys.size());
        void* overwrite_buffer = allocator.allocate(values.front().size());
        ASSERT_NE(overwrite_buffer, nullptr);
        std::memset(overwrite_buffer, 'c', values.front().size());
        std::vector<Slice> overwrite{{overwrite_buffer, values.front().size()}};
        for (const auto& key : keys) {
            auto blocked = client->Upsert(key, overwrite, replicate);
            ASSERT_FALSE(blocked.has_value());
            ASSERT_EQ(blocked.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
        }

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = backend_type;
        config.storage_filepath = data_path + "/skipped_ssd";
        config.local_buffer_size = 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = keys.size();
        std::unordered_set<std::string> duplicates;
        for (size_t i = 0; i < duplicate_count; ++i) {
            duplicates.insert(TenantId::Default().MakeScopedKey(keys[i]));
        }
        if (backend_type == StorageBackendType::kBucket) {
            file_storage.storage_backend_ =
                std::make_shared<ConcurrentOffloadBackend>(
                    config, bucket_config, std::move(duplicates), fail_write);
        } else {
            // File-per-key also returns success for a partially written batch
            // and omits failed keys from its completion callback.
            file_storage.storage_backend_->SetTestFailurePredicate(
                [duplicates](const std::string& key) {
                    return duplicates.count(key) != 0;
                });
        }
        auto backend = file_storage.storage_backend_;
        ASSERT_TRUE(backend->Init());
        auto result = file_storage.OffloadObjects(tasks);
        if (fail_write) {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
        } else {
            ASSERT_TRUE(result.has_value());
        }

        for (size_t i = 0; i < keys.size(); ++i) {
            const auto storage_key = TenantId::Default().MakeScopedKey(keys[i]);
            const bool persisted = backend_type == StorageBackendType::kBucket
                                       ? !fail_write || i < duplicate_count
                                       : i >= duplicate_count;
            auto exists = backend->IsExist(storage_key);
            ASSERT_TRUE(exists.has_value());
            EXPECT_EQ(exists.value(), persisted);
            if (persisted) {
                std::string actual(values[i].size(), '\0');
                std::unordered_map<std::string, Slice> read{
                    {storage_key, {actual.data(), actual.size()}}};
                ASSERT_TRUE(backend->BatchLoad(read));
                EXPECT_EQ(actual, values[i]);
            }
            auto query = client->Query(keys[i]);
            ASSERT_TRUE(query.has_value());
            size_t disk_replicas = 0;
            for (const auto& replica : query->replicas) {
                disk_replicas += replica.is_local_disk_replica();
            }
            // Only newly committed keys may register a disk replica. A skip
            // is a NACK, never a success with a fabricated bucket descriptor.
            EXPECT_EQ(disk_replicas,
                      (!fail_write && i >= duplicate_count) ? 1u : 0u);
            // Upsert is blocked both by an in-flight offload marker and by a
            // retained source reference. It must become available immediately,
            // without waiting for the offload task timeout.
            auto overwritten = client->Upsert(keys[i], overwrite, replicate);
            EXPECT_TRUE(overwritten.has_value())
                << keys[i] << ": " << toString(overwritten.error());
        }
        allocator.deallocate(overwrite_buffer, values.front().size());
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

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
        UnsetEnv("MOONCAKE_OFFLOAD_FLUSH_PARTIAL_BUCKETS");
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
        UnsetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE");
        UnsetEnv("MOONCAKE_DFS_FS_ADAPTER");
        UnsetEnv("MOONCAKE_DISTRIBUTED_ROOT_DIR");
        UnsetEnv("MOONCAKE_DFS_ROOT_DIR");
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

    bool FileStorageUsesDfsControlPlane(const FileStorage& file_storage) {
        return file_storage.config_.enable_dfs;
    }

    tl::expected<void, ErrorCode> FileStorageNotifyEvictedDiskReplicas(
        FileStorage& fileStorage,
        const std::vector<std::string>& evicted_keys) {
        return fileStorage.NotifyEvictedDiskReplicas(evicted_keys);
    }

    tl::expected<void, ErrorCode> FileStorageGroupOffloadingKeysByBucket(
        FileStorage& fileStorage,
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys,
        std::vector<std::string>* deferred_keys = nullptr) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return bucket_backend->AllocateOffloadingBuckets(
            offloading_objects, buckets_keys, deferred_keys);
    }

    tl::expected<void, ErrorCode> FileStorageOffloadObjects(
        FileStorage& fileStorage,
        const std::vector<OffloadTaskItem>& offloading_objects) {
        return fileStorage.OffloadObjects(offloading_objects);
    }

    std::unordered_map<std::string, OffloadTaskItem>
    GetDeferredTaskByStorageKey(FileStorage& fileStorage) {
        return fileStorage.deferred_task_by_storage_key_;
    }

    static void FileStoragePartitionUnbucketedTasks(
        const std::unordered_map<std::string, OffloadTaskItem>&
            task_by_storage_key,
        const std::unordered_set<std::string>& all_bucket_keys,
        const std::vector<std::string>& deferred_keys,
        const std::unordered_map<std::string, OffloadTaskItem>&
            previously_carried,
        const std::unordered_set<std::string>& redrained_carried_keys,
        std::vector<OffloadTaskItem>& failed_tasks,
        std::unordered_map<std::string, OffloadTaskItem>& carried_tasks) {
        FileStorage::PartitionUnbucketedTasks(
            task_by_storage_key, all_bucket_keys, deferred_keys,
            previously_carried, redrained_carried_keys, failed_tasks,
            carried_tasks);
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
    void RunPutHealWipeScenario(const std::string& data_path,
                                StorageBackendType backend_type,
                                const std::string& suffix) {
        std::filesystem::path master_root =
            std::filesystem::path(data_path) / ("heal_master" + suffix);
        std::filesystem::create_directories(master_root);

        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_root_fs_dir(master_root.string())
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
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
                        ->RegisterLocalMemory(allocator.getBase(),
                                              16 * 1024 * 1024, "cpu:0", false,
                                              false)
                        .has_value());

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = backend_type;
        config.storage_filepath = data_path + "/heal_ssd" + suffix;
        config.local_buffer_size = 4 * 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client.value(), local_rpc_addr);
        ASSERT_TRUE(file_storage.storage_backend_->Init());
        {
            MutexLocker locker(&file_storage.offloading_mutex_);
            file_storage.enable_offloading_ = true;
        }

        // Offload one key so master tracks a LOCAL_DISK replica backed by a
        // real file on the client SSD.
        const std::string key = "heal_key" + suffix;
        std::string original(512, 'x');
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(key,
                             std::vector<Slice>{Slice{original.data(), 512}});
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

        // Wire the probe the way RealClient does in production: existence
        // against this process's offload files.
        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists = file_storage.Exists(k);
                if (!exists) {
                    return std::nullopt;
                }
                return !*exists;
            });

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
        auto exists = file_storage.Exists(key);
        ASSERT_TRUE(exists.has_value());
        ASSERT_FALSE(exists.value());
        // QueryResult is not assignable, so re-query into a fresh variable.
        auto query_after_wipe = client.value()->Query(key);
        ASSERT_TRUE(query_after_wipe.has_value());

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
    void RunBatchPutHealWipeScenario(const std::string& data_path,
                                     bool with_key_metadata = false) {
        std::filesystem::path master_root =
            std::filesystem::path(data_path) / "heal_master_batch";
        std::filesystem::create_directories(master_root);

        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_root_fs_dir(master_root.string())
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client.has_value());
        ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

        constexpr size_t kSegSize = 64 * 1024 * 1024;
        void* seg_ptr = allocate_buffer_allocator_memory(kSegSize);
        ASSERT_NE(seg_ptr, nullptr);
        ASSERT_TRUE(
            client.value()->MountSegment(seg_ptr, kSegSize, "tcp").has_value());
        SimpleAllocator allocator(16 * 1024 * 1024);
        ASSERT_TRUE(client.value()
                        ->RegisterLocalMemory(allocator.getBase(),
                                              16 * 1024 * 1024, "cpu:0", false,
                                              false)
                        .has_value());

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kFilePerKey;
        config.storage_filepath = data_path + "/heal_ssd_batch";
        config.local_buffer_size = 4 * 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage file_storage(config, client.value(), local_rpc_addr);
        ASSERT_TRUE(file_storage.storage_backend_->Init());
        {
            MutexLocker locker(&file_storage.offloading_mutex_);
            file_storage.enable_offloading_ = true;
        }

        // Offload two keys, then wipe the backing files to recreate the #3709
        // divergence on both.
        const std::vector<std::string> wiped_keys = {"heal_batch_a",
                                                     "heal_batch_b"};
        std::string original(512, 'x');
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        for (const auto& key : wiped_keys) {
            batch_object.emplace(
                key, std::vector<Slice>{Slice{original.data(), 512}});
        }
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

        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists = file_storage.Exists(k);
                if (!exists) {
                    return std::nullopt;
                }
                return !*exists;
            });

        file_storage.storage_backend_->RemoveAll();
        auto exists = file_storage.Exists(wiped_keys[0]);
        ASSERT_TRUE(exists.has_value());
        ASSERT_FALSE(exists.value());

        // One batch with both wiped keys and a fresh key.
        const std::string fresh_key = "heal_batch_fresh";
        const std::string updated(512, 'y');
        std::vector<ObjectKey> keys = {fresh_key, wiped_keys[0], wiped_keys[1]};
        std::vector<std::vector<Slice>> batched_slices;
        std::vector<void*> buffers;
        for (size_t i = 0; i < keys.size(); ++i) {
            void* buf = allocator.allocate(512);
            ASSERT_NE(buf, nullptr);
            std::memcpy(buf, updated.data(), 512);
            buffers.push_back(buf);
            batched_slices.push_back({Slice{buf, 512}});
        }
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        if (with_key_metadata) {
            cfg.session_ids =
                std::vector<std::string>{"fresh", "wiped-a", "wiped-b"};
            cfg.group_ids =
                std::vector<std::string>{"fresh-group", "a-group", "b-group"};
            cfg.per_key_preferred_hca_names =
                std::vector<std::vector<std::string>>{
                    {"mlx5_0"}, {}, {"mlx5_1"}};
        }
        auto results = client.value()->BatchPut(keys, batched_slices, cfg);
        ASSERT_EQ(results.size(), keys.size());
        for (size_t i = 0; i < results.size(); ++i) {
            ASSERT_TRUE(results[i].has_value())
                << "BatchPut over a dangling replica failed for " << keys[i]
                << ": " << toString(results[i].error());
        }

        // Every key, healed or fresh, reads the new data back.
        for (const auto& key : keys) {
            void* read_buf = allocator.allocate(512);
            ASSERT_NE(read_buf, nullptr);
            std::vector<Slice> read_slices{Slice{read_buf, 512}};
            auto get = client.value()->Get(key, read_slices);
            ASSERT_TRUE(get.has_value())
                << "Get after batch self-heal failed for " << key << ": "
                << toString(get.error());
            EXPECT_EQ(std::memcmp(read_slices[0].ptr, updated.data(), 512), 0);
            allocator.deallocate(read_buf, 512);
        }
        for (void* buf : buffers) {
            allocator.deallocate(buf, 512);
        }

        auto unmount = client.value()->UnmountSegment(seg_ptr, kSegSize);
        EXPECT_TRUE(unmount.has_value());
        std::free(seg_ptr);
    }
};

TEST_F(FileStorageTest, SkippedOffloadAllDuplicatesReleaseSourceTasks) {
    RunSkippedOffloadScenario(2, false);
}

TEST_F(FileStorageTest, SkippedOffloadMixedBatchReleasesSourceTasks) {
    RunSkippedOffloadScenario(1, false);
}

TEST_F(FileStorageTest, SkippedOffloadFreshBatchKeepsSuccessNotifications) {
    RunSkippedOffloadScenario(0, false);
}

TEST_F(FileStorageTest, SkippedOffloadPartialFileWriteReleasesSourceTasks) {
    RunSkippedOffloadScenario(1, false, StorageBackendType::kFilePerKey);
}

TEST_F(FileStorageTest, SkippedOffloadAllFileWritesFailReleasesSourceTasks) {
    RunSkippedOffloadScenario(2, false, StorageBackendType::kFilePerKey);
}

TEST_F(FileStorageTest, SkippedOffloadWriteFailureReleasesAllSourceTasks) {
    RunSkippedOffloadScenario(1, true);
}

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

TEST_F(FileStorageTest, DistributedBackendSelectsControlPlaneFromStorageMode) {
    SetEnv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "distributed_storage_backend");
    SetEnv("MOONCAKE_DISTRIBUTED_ROOT_DIR", data_path + "/distributed");

    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = 4 * 1024 * 1024;
    EXPECT_FALSE(config.enable_dfs);

    SetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE", "posix");
    {
        FileStorage file_storage(config, nullptr, "localhost:9003");
        EXPECT_TRUE(FileStorageUsesDfsControlPlane(file_storage));
    }

#ifdef HAVE_OSS_ADAPTER
    SetEnv("MOONCAKE_DISTRIBUTED_FS_TYPE", "oss");
    FileStorage file_storage(config, nullptr, "localhost:9003");
    EXPECT_FALSE(FileStorageUsesDfsControlPlane(file_storage));

    auto enable_offloading = FileStorageIsEnableOffloading(file_storage);
    ASSERT_TRUE(enable_offloading);
    EXPECT_TRUE(*enable_offloading);
#endif
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

TEST_F(FileStorageTest, OffloadObjects_capacity_limit_releases_deferred_tasks) {
    RunDeferredOffloadFailureScenario(true);
}

TEST_F(FileStorageTest, OffloadObjects_write_failure_preserves_deferred_tasks) {
    RunDeferredOffloadFailureScenario(false);
}

// Regression (upstream #3006): keys deferred to the ungrouped pool must be
// reported to the caller so their offload tasks can be carried across the
// deferral. Before this, deferred keys were NACKed on deferral and then
// silently dropped on re-emission (no task) — never written to SSD.
TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_reports_deferred_keys) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    std::vector<std::string> deferred_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    // 35 keys / limit 10: 3 full buckets, the 5-key tail is deferred.
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys, &deferred_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    ASSERT_EQ(deferred_keys.size(), 5);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    // Deferred keys are exactly the keys in no bucket.
    std::unordered_set<std::string> bucketed;
    for (const auto& bucket : buckets_keys) {
        bucketed.insert(bucket.begin(), bucket.end());
    }
    for (const auto& key : deferred_keys) {
        EXPECT_EQ(bucketed.count(key), 0u) << key;
        EXPECT_EQ(offloading_objects.count(key), 1u) << key;
    }

    // Empty input: the grouping loop never runs, so the pool is untouched —
    // nothing is emitted, nothing re-reported. The caller must therefore
    // RETAIN its carried tasks rather than rebuild from deferred_keys alone.
    buckets_keys.clear();
    deferred_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, {}, buckets_keys, &deferred_keys));
    ASSERT_EQ(buckets_keys.size(), 0);
    ASSERT_EQ(deferred_keys.size(), 0);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);

    // Input too small to fill the bucket: pool + new input are all
    // RE-deferred, and all must be RE-reported.
    offloading_objects.clear();
    for (size_t i = 100; i < 102; i++) {
        offloading_objects.emplace("late" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    deferred_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys, &deferred_keys));
    ASSERT_EQ(buckets_keys.size(), 0);
    ASSERT_EQ(deferred_keys.size(), 7);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 7);

    // Enough new input to fill one exact bucket: pool + new input are
    // emitted; the pool empties and nothing is deferred.
    offloading_objects.clear();
    for (size_t i = 200; i < 203; i++) {
        offloading_objects.emplace("fill" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    deferred_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys, &deferred_keys));
    ASSERT_EQ(buckets_keys.size(), 1);
    ASSERT_EQ(buckets_keys[0].size(), 10);
    ASSERT_EQ(deferred_keys.size(), 0);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

// Regression (upstream #3006): the NACK sweep must exempt deferred keys —
// carrying their tasks to the next cycle — while still NACKing keys the
// backend deliberately skipped.
TEST_F(FileStorageTest, PartitionUnbucketedTasks_carries_deferred_not_nacked) {
    auto make_task = [](const std::string& key) {
        return OffloadTaskItem{.tenant_id = "default", .key = key, .size = 1};
    };
    std::unordered_map<std::string, OffloadTaskItem> task_by_storage_key{
        {"bucketed", make_task("bucketed")},
        {"deferred", make_task("deferred")},
        {"skipped_a", make_task("skipped_a")},
        {"skipped_b", make_task("skipped_b")},
    };
    // "pooled_waiting" was carried from an earlier cycle; this cycle the
    // backend neither emitted nor re-reported it (empty effective input
    // leaves the pool untouched), so it must be RETAINED, not NACKed.
    task_by_storage_key.emplace("pooled_waiting", make_task("pooled_waiting"));
    const std::unordered_map<std::string, OffloadTaskItem> previously_carried{
        {"pooled_waiting", make_task("pooled_waiting")}};
    const std::unordered_set<std::string> all_bucket_keys{"bucketed"};
    const std::vector<std::string> deferred_keys{"deferred"};

    std::vector<OffloadTaskItem> failed_tasks;
    std::unordered_map<std::string, OffloadTaskItem> carried_tasks;
    FileStoragePartitionUnbucketedTasks(task_by_storage_key, all_bucket_keys,
                                        deferred_keys, previously_carried, {},
                                        failed_tasks, carried_tasks);

    // The deferred key is carried with its original task; the un-emitted
    // previously carried key is retained. Neither is NACKed.
    ASSERT_EQ(carried_tasks.size(), 2u);
    ASSERT_EQ(carried_tasks.count("deferred"), 1u);
    ASSERT_EQ(carried_tasks.count("pooled_waiting"), 1u);
    EXPECT_EQ(carried_tasks.at("deferred").key, "deferred");
    for (const auto& task : failed_tasks) {
        EXPECT_NE(task.key, "deferred");
        EXPECT_NE(task.key, "pooled_waiting");
        EXPECT_NE(task.key, "bucketed");
    }
    // Deliberately skipped keys are still NACKed.
    ASSERT_EQ(failed_tasks.size(), 2u);
}

// Regression (review follow-up on the re-drain collision path): a carried key
// re-drained this cycle and then skipped outright by the backend — re-PUT at a
// size over bucket_size_limit, which GroupOffloadingKeysByBucket drops without
// bucketing OR deferring — must NACK the FRESH task and must not retain the
// stale one. Retention here would be doubly wrong: the stale task is stranded
// forever (its pooled copy was dropped on collision, so the pool can never
// re-emit it) and its presence in carried_tasks suppresses the fresh task's
// NACK, losing the task and leaking its source-replica refcount.
TEST_F(FileStorageTest, PartitionUnbucketedTasks_redrained_then_skipped) {
    OffloadTaskItem stale{.tenant_id = "default", .key = "x", .size = 1};
    OffloadTaskItem fresh{.tenant_id = "default", .key = "x", .size = 5};
    // The carry merge already resolved the collision: the fresh task won the
    // task map, the pooled copy was removed, and "x" was recorded as
    // re-drained.
    const std::unordered_map<std::string, OffloadTaskItem> task_by_storage_key{
        {"x", fresh}};
    const std::unordered_map<std::string, OffloadTaskItem> previously_carried{
        {"x", stale}};
    const std::unordered_set<std::string> redrained_carried_keys{"x"};
    // Over the size limit: neither bucketed nor deferred.
    const std::unordered_set<std::string> all_bucket_keys{};
    const std::vector<std::string> deferred_keys{};

    std::vector<OffloadTaskItem> failed_tasks;
    std::unordered_map<std::string, OffloadTaskItem> carried_tasks;
    FileStoragePartitionUnbucketedTasks(
        task_by_storage_key, all_bucket_keys, deferred_keys, previously_carried,
        redrained_carried_keys, failed_tasks, carried_tasks);

    EXPECT_TRUE(carried_tasks.empty())
        << "a re-drained key has no pooled copy left to wait for; retaining "
           "the stale task strands it and suppresses the fresh NACK";
    ASSERT_EQ(failed_tasks.size(), 1u);
    EXPECT_EQ(failed_tasks[0].key, "x");
    EXPECT_EQ(failed_tasks[0].size, 5)
        << "the fresh task must be the one NACKed";
}

// The exclusion must be keyed on "re-drained this cycle", NOT on "present in
// task_by_storage_key": the carry merge inserts every non-collision carried
// task into that map, so testing membership there would void retention for
// every carried key and re-introduce the task-less drop this PR fixes. This
// pins the distinction — same shape as above but WITHOUT the collision.
TEST_F(FileStorageTest, PartitionUnbucketedTasks_merged_carry_still_retained) {
    OffloadTaskItem carried{.tenant_id = "default", .key = "y", .size = 1};
    // Post-merge state for a non-collision carried key: it IS in
    // task_by_storage_key, but it was not re-drained.
    const std::unordered_map<std::string, OffloadTaskItem> task_by_storage_key{
        {"y", carried}};
    const std::unordered_map<std::string, OffloadTaskItem> previously_carried{
        {"y", carried}};
    const std::unordered_set<std::string> all_bucket_keys{};
    const std::vector<std::string> deferred_keys{};

    std::vector<OffloadTaskItem> failed_tasks;
    std::unordered_map<std::string, OffloadTaskItem> carried_tasks;
    FileStoragePartitionUnbucketedTasks(task_by_storage_key, all_bucket_keys,
                                        deferred_keys, previously_carried, {},
                                        failed_tasks, carried_tasks);

    ASSERT_EQ(carried_tasks.size(), 1u);
    EXPECT_EQ(carried_tasks.count("y"), 1u);
    EXPECT_TRUE(failed_tasks.empty())
        << "the pool still holds this key; NACKing it re-introduces #3006";
}

// Regression (upstream #3006, integration): drive the deferral carry through
// OffloadObjects itself. A cycle in which every key defers emits no bucket,
// so nothing touches the master client — which makes the integrated path
// (carry merge, deferral report plumbing, partition, carry assignment)
// testable without a client. Phases 3 and 4 exercise re-drained keys: their
// pooled copies are removed and the fresh tasks and sizes are re-deferred.
TEST_F(FileStorageTest, OffloadObjects_deferred_tail_tasks_carried) {
    auto make_task = [](const std::string& key) {
        return OffloadTaskItem{.tenant_id = "default", .key = key, .size = 1};
    };
    auto carried_user_keys =
        [](const std::unordered_map<std::string, OffloadTaskItem>& carry) {
            std::unordered_set<std::string> keys;
            for (const auto& [_, task] : carry) keys.insert(task.key);
            return keys;
        };

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    // Phase 1: two tasks, far below bucket capacity -> the whole drain is
    // the tail: no bucket emitted, both tasks carried, none NACKed (no
    // client call happens -- client_ is null, so reaching it would crash).
    ASSERT_TRUE(FileStorageOffloadObjects(fileStorage,
                                          {make_task("d1"), make_task("d2")}));
    auto carry = GetDeferredTaskByStorageKey(fileStorage);
    ASSERT_EQ(carry.size(), 2u);
    EXPECT_EQ(carried_user_keys(carry),
              (std::unordered_set<std::string>{"d1", "d2"}));
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 2);

    // Phase 2: one more task; pool + new input still cannot fill a bucket,
    // so all three are re-deferred and re-carried.
    ASSERT_TRUE(FileStorageOffloadObjects(fileStorage, {make_task("d3")}));
    carry = GetDeferredTaskByStorageKey(fileStorage);
    ASSERT_EQ(carry.size(), 3u);
    EXPECT_EQ(carried_user_keys(carry),
              (std::unordered_set<std::string>{"d1", "d2", "d3"}));
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 3);

    // Phase 3: re-drain a carried key. The fresh entry replaces its pooled
    // copy and is grouped with the other carried keys. All are re-deferred,
    // so every task survives without a NACK.
    ASSERT_TRUE(FileStorageOffloadObjects(fileStorage, {make_task("d1")}));
    carry = GetDeferredTaskByStorageKey(fileStorage);
    ASSERT_EQ(carry.size(), 3u);
    EXPECT_EQ(carried_user_keys(carry),
              (std::unordered_set<std::string>{"d1", "d2", "d3"}));
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 3);

    // Phase 4 (re-drain collision semantics): a carried key re-PUT with a
    // DIFFERENT size while deferred. The fresh drain must win entirely —
    // pooled copy (stale size) removed, fresh task carried — rather than
    // the fresh task winning the task map while the stale size wins the
    // packing accounting.
    OffloadTaskItem d2_fresh{.tenant_id = "default", .key = "d2", .size = 2};
    ASSERT_TRUE(FileStorageOffloadObjects(fileStorage, {d2_fresh}));
    carry = GetDeferredTaskByStorageKey(fileStorage);
    ASSERT_EQ(carry.size(), 3u);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 3);
    for (const auto& [_, task] : carry) {
        if (task.key == "d2") {
            EXPECT_EQ(task.size, 2) << "fresh drained task must supersede "
                                       "the stale carried/pooled copy";
        }
    }

    // The remaining collision corner — a re-drained key the backend then skips
    // outright (re-PUT over bucket_size_limit) — cannot be driven here: by
    // construction it produces a NACK, and the NACK path dereferences the null
    // client_ this fixture relies on. It is covered at unit level by
    // PartitionUnbucketedTasks_redrained_then_skipped.
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
    // The per-key file backend reports the wiped file as OBJECT_NOT_FOUND /
    // FILE_OPEN_FAIL, the original proof set.
    RunPutHealWipeScenario(data_path, StorageBackendType::kFilePerKey,
                           "_per_key");
}

TEST_F(FileStorageTest,
       PutAfterPhysicalWipeHealsDanglingLocalDiskReplicaOnBucket) {
    // The bucket backend's BatchLoad reports a missing key as INVALID_KEY
    // (fanganpai's production trace in #3709); the heal must still fire.
    RunPutHealWipeScenario(data_path, StorageBackendType::kBucket, "_bucket");
}

TEST_F(FileStorageTest,
       BatchPutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    // The BatchPut twin of the scenario above: OBJECT_ALREADY_EXISTS results
    // on the batch path must not be converted to success for keys whose
    // backing file is gone. Two wiped keys and one fresh key go through one
    // batch; every key must really store.
    RunBatchPutHealWipeScenario(data_path);
}

TEST_F(FileStorageTest,
       SessionAndHcaBatchPutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    RunBatchPutHealWipeScenario(data_path, true);
}

}  // namespace mooncake
