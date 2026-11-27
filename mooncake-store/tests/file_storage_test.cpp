#include "offload_storage_key.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <map>
#include <mutex>
#include <cstdio>
#include <set>
#include <cstring>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <fstream>
#include <memory>
#include <optional>
#include <unordered_set>
#include <future>
#include <variant>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_metric.h"
#include "common/network.h"
#include "file_storage.h"
#include "offload_storage_key.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "master_service/master_service_test_peer.h"
#include "real_client.h"
#include "transfer_engine.h"
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

// Fails every read the way BucketStorageBackend does when preadv fails:
// the errno goes to the disk-error observer, FILE_READ_FAIL to the caller.
class FailingReadBackend : public BucketStorageBackend {
   public:
    FailingReadBackend(const FileStorageConfig& config,
                       const BucketBackendConfig& bucket_config, int read_errno)
        : BucketStorageBackend(config, bucket_config),
          read_errno_(read_errno) {}

    void SetDiskErrorObserver(DiskErrorObserver observer) override {
        observer_ = std::move(observer);
    }

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>&) override {
        ++loads;
        observer_(read_errno_, "preadv");
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    tl::expected<void, ErrorCode> BatchLoadSpans(
        std::unordered_map<std::string, StorageValueSpan>&) override {
        ++span_loads;
        observer_(read_errno_, "preadv");
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>
            complete,
        EvictionHandler eviction = nullptr) override {
        ++offloads;
        return BucketStorageBackend::BatchOffload(batch, std::move(complete),
                                                  std::move(eviction));
    }

    int loads = 0;
    int span_loads = 0;
    int offloads = 0;

   private:
    int read_errno_;
    DiskErrorObserver observer_;
};

class DelayedReadBackend : public BucketStorageBackend {
   public:
    using BucketStorageBackend::BucketStorageBackend;

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& slices) override {
        ++loads;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return BucketStorageBackend::BatchLoad(slices);
    }

    std::atomic<size_t> loads{0};
};

class BlockingCarryoverBackend : public BucketStorageBackend {
   public:
    BlockingCarryoverBackend(const FileStorageConfig& config,
                             const BucketBackendConfig& bucket_config)
        : BucketStorageBackend(config, bucket_config),
          released_(release.get_future().share()) {}

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>
            complete,
        EvictionHandler eviction = nullptr) override {
        entered.set_value();
        released_.wait();
        return BucketStorageBackend::BatchOffload(batch, std::move(complete),
                                                  std::move(eviction));
    }

    std::promise<void> entered, release;

   private:
    std::shared_future<void> released_;
};

// Real RPC/SSD integration below needs an in-memory HA persistence target,
// while only the ordered writer's admission is faulted. This exercises partial
// publication without depending on timing a backend persistence error.
class NotificationHaBackend final : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard lock(mutex_);
        const auto it = kvs_.find(std::string(key));
        if (it == kvs_.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard lock(mutex_);
        kvs_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& kvs) override {
        std::lock_guard lock(mutex_);
        kvs.clear();
        for (auto it = kvs_.lower_bound(std::string(begin));
             it != kvs_.end() && it->first < end; ++it) {
            kvs.push_back({it->first, it->second});
            if (limit && kvs.size() == limit) break;
        }
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn& txn) override {
        std::lock_guard lock(mutex_);
        for (const auto& compare : txn.compares) {
            const auto it = kvs_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != kvs_.end()) return ErrorCode::ETCD_TRANSACTION_FAIL;
            } else if (it == kvs_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) kvs_[put.key] = put.value;
        return ErrorCode::OK;
    }

   private:
    std::mutex mutex_;
    std::map<std::string, std::string> kvs_;
};

class NotificationRejectingWriter final : public OrderedOpLogWriter {
   public:
    NotificationRejectingWriter(OrderedOpLogWriterConfig config,
                                WriteBatchFn write_batch)
        : OrderedOpLogWriter(std::move(config), std::move(write_batch)) {}

    // Once the nth PUT_END is rejected, keep rejecting until explicit recovery.
    void RejectFromPutEnd(size_t nth) { remaining_.store(nth); }
    void AllowCommits() { remaining_.store(-1); }

    tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry,
        DurableCallback callback) override {
        if (entry.op_type == OpType::PUT_END) {
            const auto remaining = remaining_.load();
            if (remaining == 0 ||
                (remaining > 0 && remaining_.fetch_sub(1) == 1)) {
                return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
            }
        }
        return OrderedOpLogWriter::Commit(
            std::move(reservation), std::move(entry), std::move(callback));
    }

   private:
    std::atomic<int64_t> remaining_{-1};
};

class NotificationCountingBucket final : public BucketStorageBackend {
   public:
    NotificationCountingBucket(const FileStorageConfig& config,
                               const BucketBackendConfig& bucket_config)
        : BucketStorageBackend(config, bucket_config) {}

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>
            complete,
        EvictionHandler eviction = nullptr) override {
        ++payload_writes;
        return BucketStorageBackend::BatchOffload(batch, std::move(complete),
                                                  std::move(eviction));
    }
    tl::expected<std::vector<std::string>, ErrorCode> EvictAboveDiskWatermark(
        double high, double low, EvictionHandler eviction = nullptr) override {
        ++watermark_passes;
        return BucketStorageBackend::EvictAboveDiskWatermark(
            high, low, std::move(eviction));
    }

    size_t payload_writes{0};
    size_t watermark_passes{0};
};

enum class ReclaimCase {
    kRemoved,
    kRePut,
    kRetriedTask,
    kSegmentLost,
    kSegmentRemounted,
    kQueryFails,
    kExistingData,
    kOffloadFails
};

class FileStorageTest : public ::testing::Test {
   protected:
    std::string data_path;

    static uintmax_t StoredFileBytes(const std::string& path) {
        uintmax_t bytes = 0;
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (!entry.is_regular_file()) continue;
            struct stat info{};
            EXPECT_EQ(0, ::stat(entry.path().c_str(), &info));
            bytes += static_cast<uintmax_t>(info.st_blocks) * 512;
        }
        return bytes;
    }

    // Fails every write while `fail` is set, like a full disk.
    class FailingWriteBackend : public BucketStorageBackend {
       public:
        using BucketStorageBackend::BucketStorageBackend;
        tl::expected<int64_t, ErrorCode> BatchOffload(
            const std::unordered_map<std::string, std::vector<Slice>>& batch,
            std::function<ErrorCode(const std::vector<std::string>&,
                                    std::vector<StorageObjectMetadata>&)>
                complete,
            EvictionHandler eviction = nullptr) override {
            if (fail.load()) {
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
            return BucketStorageBackend::BatchOffload(
                batch, std::move(complete), std::move(eviction));
        }
        std::atomic<bool> fail{false};
    };

    void RunReclaimScenario(ReclaimCase reclaim_case) {
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build()));
        constexpr size_t kSegmentSize = 16 * 1024 * 1024;
        constexpr size_t kSize = 128;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(kSegmentSize), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(kSegmentSize);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));

        const std::string key = "reclaim-key";
        auto task_storage_key = [](const OffloadTaskItem& task) {
            const std::string generation = task.object_generation.value_or("");
            const std::string physical =
                generation.empty()
                    ? task.key
                    : MakeOffloadStorageKey(task.key, generation);
            return TenantId::Default().MakeScopedKey(physical);
        };
        auto put = [&](char fill, const std::string& put_key = "reclaim-key") {
            auto* buffer = allocator.allocate(kSize);
            ASSERT_NE(buffer, nullptr);
            std::memset(buffer, fill, kSize);
            ReplicateConfig replicate;
            replicate.replica_num = 1;
            std::vector<Slice> slices{{buffer, kSize}};
            ASSERT_TRUE(client->Put(put_key, slices, replicate));
            allocator.deallocate(buffer, kSize);
        };
        auto drain_tasks = [&] {
            std::vector<OffloadTaskItem> tasks;
            EXPECT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
            return tasks;
        };

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_filepath = data_path + "/reclaim";
        config.local_buffer_size = 1024 * 1024;
        config.reclaim_interval_seconds = 1;
        config.reclaim_cycle_seconds = 1;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 1;
        auto backend =
            std::make_shared<FailingWriteBackend>(config, bucket_config);
        storage.storage_backend_ = backend;
        ASSERT_TRUE(backend->Init());
        const auto baseline = StoredFileBytes(config.storage_filepath);

        auto bucket_of =
            [&](const std::string& storage_key) -> std::optional<int64_t> {
            std::unordered_map<std::string, StorageObjectMetadata> metadata;
            if (!backend->GetObjectDataSize(storage_key) ||
                !backend->BatchQuery({storage_key}, metadata)) {
                return std::nullopt;
            }
            return metadata.at(storage_key).bucket_id;
        };
        auto local_value = [&](const std::string& storage_key) -> std::string {
            std::string actual(kSize, '\0');
            std::unordered_map<std::string, Slice> read{
                {storage_key, {actual.data(), actual.size()}}};
            return backend->BatchLoad(read) ? actual : std::string();
        };
        auto master_has_our_replica = [&] {
            auto query = client->Query(key);
            if (!query) return false;
            return std::any_of(query->replicas.begin(), query->replicas.end(),
                               [&](const Replica::Descriptor& replica) {
                                   const auto* disk =
                                       std::get_if<LocalDiskDescriptor>(
                                           &replica.descriptor_variant);
                                   return disk != nullptr &&
                                          disk->transport_endpoint == endpoint;
                               });
        };
        auto reclaim_twice = [&] {
            for (int i = 0; i < 2; ++i) {
                storage.next_reclaim_at_ = {};
                storage.ReclaimRemovedObjects();
            }
        };

        put('o');
        auto tasks = drain_tasks();
        ASSERT_EQ(1, tasks.size());
        const auto first_storage_key = task_storage_key(tasks[0]);
        ASSERT_TRUE(storage.OffloadObjects(tasks));
        const auto first_bucket = bucket_of(first_storage_key);
        ASSERT_TRUE(first_bucket);
        ASSERT_TRUE(master_has_our_replica());
        const auto populated = StoredFileBytes(config.storage_filepath);
        ASSERT_GT(populated, baseline);

        switch (reclaim_case) {
            case ReclaimCase::kRemoved:
                ASSERT_TRUE(client->Remove(key, true));
                // The first tick records the verdict, the next one acts.
                ASSERT_TRUE(storage.Heartbeat());
                EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));
                storage.next_reclaim_at_ = {};
                ASSERT_TRUE(storage.Heartbeat());
                EXPECT_FALSE(bucket_of(first_storage_key));
                EXPECT_EQ(baseline, StoredFileBytes(config.storage_filepath));
                break;
            case ReclaimCase::kRePut: {
                ASSERT_TRUE(client->Remove(key, true));
                put('n');
                tasks = drain_tasks();
                ASSERT_EQ(1, tasks.size());
                const auto second_storage_key = task_storage_key(tasks[0]);
                ASSERT_TRUE(storage.OffloadObjects(tasks));
                const auto second_bucket = bucket_of(second_storage_key);
                ASSERT_TRUE(second_bucket);
                EXPECT_NE(*first_bucket, *second_bucket);
                EXPECT_EQ(std::string(kSize, 'n'),
                          local_value(second_storage_key));
                EXPECT_TRUE(master_has_our_replica());
                // The stale bucket goes like any bucket with no live key.
                reclaim_twice();
                EXPECT_FALSE(bucket_of(first_storage_key));
                EXPECT_EQ(second_bucket, bucket_of(second_storage_key));
                EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));
                break;
            }
            case ReclaimCase::kRetriedTask:
                std::ignore = storage.OffloadObjects(tasks);
                EXPECT_EQ(first_bucket, bucket_of(first_storage_key));
                EXPECT_EQ(std::string(kSize, 'o'),
                          local_value(first_storage_key));
                EXPECT_TRUE(master_has_our_replica());
                EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));
                break;
            case ReclaimCase::kSegmentLost:
                ASSERT_TRUE(client->Remove(key, true));
                storage.segment_lost_epoch_.fetch_add(1);
                reclaim_twice();
                EXPECT_EQ(first_bucket, bucket_of(first_storage_key));
                storage.rescanned_epoch_.store(
                    storage.segment_lost_epoch_.load());
                reclaim_twice();
                EXPECT_FALSE(bucket_of(first_storage_key));
                EXPECT_EQ(baseline, StoredFileBytes(config.storage_filepath));
                break;
            case ReclaimCase::kSegmentRemounted:
                storage.enable_offloading_ = true;
                ASSERT_TRUE(client->UnmountLocalDiskSegment());
                ASSERT_FALSE(master_has_our_replica());
                ASSERT_TRUE(storage.Heartbeat());
                EXPECT_EQ(1, storage.segment_lost_epoch_.load());
                EXPECT_FALSE(storage.MasterKnowsOurObjects());
                ASSERT_TRUE(storage.rescan_future_.valid());
                storage.rescan_future_.wait();
                ASSERT_TRUE(storage.Heartbeat());
                EXPECT_TRUE(storage.MasterKnowsOurObjects());
                EXPECT_TRUE(master_has_our_replica());
                EXPECT_EQ(first_bucket, bucket_of(first_storage_key));
                break;
            case ReclaimCase::kQueryFails:
                ASSERT_TRUE(client->Remove(key, true));
                storage.ReclaimRemovedObjects();
                ASSERT_EQ(1,
                          storage.reclaim_dead_keys_.at(*first_bucket).size());
                master.Stop();
                storage.next_reclaim_at_ = {};
                storage.ReclaimRemovedObjects();
                EXPECT_EQ(first_bucket, bucket_of(first_storage_key));
                EXPECT_TRUE(storage.reclaim_dead_keys_.empty());
                return;
            case ReclaimCase::kExistingData: {
                storage.storage_backend_.reset();
                backend.reset();
                FileStorage restarted(config, client, endpoint);
                ASSERT_TRUE(restarted.Init());
                ASSERT_TRUE(restarted.Shutdown());
                EXPECT_TRUE(restarted.reclaim_disabled_);
                ASSERT_TRUE(client->Remove(key, true));
                for (int i = 0; i < 2; ++i) {
                    restarted.next_reclaim_at_ = {};
                    restarted.ReclaimRemovedObjects();
                }
                EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));
                break;
            }
            case ReclaimCase::kOffloadFails:
                storage.enable_offloading_ = true;
                ASSERT_TRUE(client->Remove(key, true));
                ASSERT_TRUE(storage.Heartbeat());
                put('x', "other-key");
                backend->fail.store(true);
                storage.next_reclaim_at_ = {};
                EXPECT_FALSE(storage.Heartbeat());
                EXPECT_FALSE(bucket_of(first_storage_key));
                EXPECT_EQ(baseline, StoredFileBytes(config.storage_filepath));
                break;
        }
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunOrdinaryRemoveAllReclaimScenario(StorageBackendType backend_type) {
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
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));
        auto* buffer = allocator.allocate(128);
        ASSERT_NE(buffer, nullptr);
        std::memset(buffer, 'o', 128);
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        std::vector<Slice> slices{{buffer, 128}};
        ASSERT_TRUE(client->Put("ordinary-reclaim", slices, replicate));
        allocator.deallocate(buffer, 128);
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(1, tasks.size());

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_filepath = data_path + "/ordinary_reclaim";
        config.local_buffer_size = 1024 * 1024;
        config.storage_backend_type = backend_type;
        config.disk_eviction_high_watermark_ratio = 0.99;
        config.disk_eviction_low_watermark_ratio = 0.98;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        if (backend_type == StorageBackendType::kBucket) {
            BucketBackendConfig bucket_config;
            bucket_config.bucket_keys_limit = 1;
            bucket_config.max_total_size = 1024 * 1024;
            bucket_config.eviction_policy = BucketEvictionPolicy::FIFO;
            storage.storage_backend_ =
                std::make_shared<BucketStorageBackend>(config, bucket_config);
        }
        ASSERT_TRUE(storage.storage_backend_->Init());
        const auto baseline = StoredFileBytes(config.storage_filepath);
        ASSERT_TRUE(storage.OffloadObjects(tasks));
        const auto populated = StoredFileBytes(config.storage_filepath);
        ASSERT_GT(populated, baseline);
        auto removed = client->RemoveAll(true);
        ASSERT_TRUE(removed);
        ASSERT_EQ(1, *removed);
        EXPECT_FALSE(client->Query("ordinary-reclaim"));
        ASSERT_TRUE(storage.Heartbeat());
        EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));

        // Physical reclamation follows configured pressure, even without
        // another write; metadata removal alone does not wipe the backend.
        storage.config_.disk_eviction_high_watermark_ratio = 1e-12;
        storage.config_.disk_eviction_low_watermark_ratio = 0.5e-12;
        ASSERT_TRUE(storage.Heartbeat());
        EXPECT_EQ(baseline, StoredFileBytes(config.storage_filepath));
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunManagedRemoveAllScenario(bool file_per_key = false,
                                     bool protected_prefix = false) {
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
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));

        const WeightRevisionIdentity identity{.tenant_id = "default",
                                              .name_space = "production",
                                              .resource_id = "ssd-weight",
                                              .revision = "v1",
                                              .weight_generation = 1};
        auto begin = client->BeginWeightImport({
            .identity = identity,
            .payload_group_id = {},
            .expected_payload_count = 1,
            .expected_logical_bytes = 128,
            .policy =
                WeightStoragePolicy{
                    .preferred_residency = WeightResidencyState::HOT,
                    .migration_mode = WeightMigrationMode::MANUAL},
            .affinity_summary =
                {.affinity_count = 1,
                 .affinity_digest = ComputeWeightAffinityMappingSha256(
                     {{"managed-payload", 128, "payload"}})},
        });
        ASSERT_TRUE(begin && *begin);
        const auto manifest_key = MakeWeightManifestKey(identity);
        const std::vector<std::string> keys{"ordinary-ssd", "managed-payload",
                                            manifest_key};
        const std::vector<std::string> values{std::string(128, 'o'),
                                              std::string(128, 'w'),
                                              std::string(128, 'm')};
        for (size_t i = 0; i < keys.size(); ++i) {
            auto* buffer = allocator.allocate(values[i].size());
            ASSERT_NE(buffer, nullptr);
            std::memcpy(buffer, values[i].data(), values[i].size());
            ReplicateConfig config;
            config.replica_num = 1;
            if (i != 0) {
                config.group_ids = {(*begin)->manifest.payload_group_id};
                config.data_type =
                    i == 1 ? ObjectDataType::WEIGHT : ObjectDataType::METADATA;
                if (i == 1) {
                    config.residency_affinity_ids =
                        std::vector<std::string>{"payload"};
                }
            }
            std::vector<Slice> slices{{buffer, values[i].size()}};
            ASSERT_TRUE(client->Put(keys[i], slices, config));
            allocator.deallocate(buffer, values[i].size());
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(keys.size(), tasks.size());
        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_filepath = data_path + "/managed_ssd";
        config.local_buffer_size = 1024 * 1024;
        config.storage_backend_type = file_per_key
                                          ? StorageBackendType::kFilePerKey
                                          : StorageBackendType::kBucket;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = keys.size();
        bucket_config.max_total_size = 1024 * 1024;
        bucket_config.eviction_policy = BucketEvictionPolicy::FIFO;
        if (!file_per_key) {
            storage.storage_backend_ =
                std::make_shared<BucketStorageBackend>(config, bucket_config);
        }
        ASSERT_TRUE(storage.storage_backend_->Init());
        const auto baseline = StoredFileBytes(config.storage_filepath);
        if (protected_prefix) {
            for (const auto index : {1, 2, 0}) {
                for (const auto& task : tasks) {
                    if (task.key != keys[index]) continue;
                    std::vector<OffloadTaskItem> single{task};
                    ASSERT_TRUE(storage.OffloadObjects(single));
                }
            }
        } else {
            ASSERT_TRUE(storage.OffloadObjects(tasks));
        }
        const auto populated = StoredFileBytes(config.storage_filepath);
        ASSERT_GT(populated, baseline);
        auto ready = client->CommitWeightImport(
            {.identity = identity,
             .expected_metadata_generation = (*begin)->metadata_generation,
             .manifest = {
                 .manifest_key = manifest_key,
                 .manifest_sha256 = std::string(64, 'a'),
                 .payload_group_id = (*begin)->manifest.payload_group_id,
                 .payload_keys_sha256 =
                     ComputeWeightPayloadKeysSha256({keys[1]}),
                 .payload_count = 1,
                 .logical_bytes = 128}});
        ASSERT_TRUE(ready && *ready);

        auto check_payload = [&] {
            std::string actual(values[1].size(), '\0');
            std::unordered_map<std::string, Slice> read{
                {TenantId::Default().MakeScopedKey(MakeOffloadStorageKey(
                     keys[1], std::find_if(tasks.begin(), tasks.end(),
                                           [&](const auto& task) {
                                               return task.key == keys[1];
                                           })
                                  ->object_generation.value_or(""))),
                 {actual.data(), actual.size()}}};
            ASSERT_TRUE(storage.storage_backend_->BatchLoad(read));
            EXPECT_EQ(values[1], actual);
        };
        ASSERT_NO_FATAL_FAILURE(check_payload());
        auto removed = client->RemoveAll(true);
        ASSERT_TRUE(removed);
        EXPECT_EQ(1, *removed);
        ASSERT_TRUE(storage.Heartbeat());
        EXPECT_TRUE(client->Query(keys[1]));
        EXPECT_TRUE(client->Query(manifest_key));
        ASSERT_NO_FATAL_FAILURE(check_payload());
        storage.config_.disk_eviction_high_watermark_ratio = 1e-12;
        storage.config_.disk_eviction_low_watermark_ratio = 0.5e-12;
        if (protected_prefix) {
            uintmax_t logical_bytes = 0;
            for (const auto& entry :
                 fs::recursive_directory_iterator(config.storage_filepath)) {
                if (entry.is_regular_file()) logical_bytes += entry.file_size();
            }
            const auto capacity = static_cast<uint64_t>(
                fs::space(config.storage_filepath).capacity * 0.9);
            // One oldest protected file suffices for the projected target.
            // Repeated heartbeats must still reach the later ordinary orphan.
            storage.config_.disk_eviction_high_watermark_ratio =
                static_cast<double>(logical_bytes - 32) / capacity;
            storage.config_.disk_eviction_low_watermark_ratio =
                static_cast<double>(logical_bytes - 64) / capacity;
        }
        for (int tick = 0; tick < (protected_prefix ? 4 : 1); ++tick) {
            ASSERT_TRUE(storage.Heartbeat());
        }
        if (file_per_key) {
            EXPECT_LT(StoredFileBytes(config.storage_filepath), populated);
            auto ordinary_exists = storage.storage_backend_->IsExist(
                TenantId::Default().MakeScopedKey(keys[0]));
            ASSERT_TRUE(ordinary_exists);
            EXPECT_FALSE(*ordinary_exists);
        } else {
            EXPECT_EQ(populated, StoredFileBytes(config.storage_filepath));
        }
        ASSERT_NO_FATAL_FAILURE(check_payload());

        // A mixed bucket must remain intact while managed objects are live.
        // Once the revision is deleted, all its bytes become reclaimable.
        auto deleted = client->DeleteWeightRevision(
            {.identity = identity,
             .expected_metadata_generation = (*ready)->metadata_generation});
        ASSERT_TRUE(deleted && *deleted);
        EXPECT_FALSE(client->Query(keys[1]));
        EXPECT_FALSE(client->Query(manifest_key));
        storage.config_.disk_eviction_high_watermark_ratio = 1e-12;
        storage.config_.disk_eviction_low_watermark_ratio = 0.5e-12;
        ASSERT_TRUE(storage.Heartbeat());
        EXPECT_EQ(baseline, StoredFileBytes(config.storage_filepath));
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunDeferredHeartbeatTailScenario() {
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
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = created.value();
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));
        void* buffer = allocator.allocate(128);
        ASSERT_NE(buffer, nullptr);
        std::memset(buffer, 't', 128);
        std::vector<Slice> slices{{buffer, 128}};
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        ASSERT_TRUE(client->Put("heartbeat-tail", slices, replicate));

        auto config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kBucket;
        config.storage_filepath = data_path + "/heartbeat_tail";
        config.local_buffer_size = 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 10;
        bucket_config.flush_partial_buckets = false;
        auto backend =
            std::make_shared<BucketStorageBackend>(config, bucket_config);
        storage.storage_backend_ = backend;
        ASSERT_TRUE(backend->Init());
        ASSERT_TRUE(storage.RegisterLocalMemory());
        storage.enable_offloading_ = true;
        ASSERT_TRUE(storage.Heartbeat());
        ASSERT_EQ(1u, storage.pending_offload_tasks_.size());
        EXPECT_EQ(1u, backend->UngroupedOffloadingObjectsSize());
        std::vector<OffloadTaskItem> fresh;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, fresh));
        ASSERT_TRUE(fresh.empty());

        ASSERT_TRUE(storage.Heartbeat());
        EXPECT_TRUE(storage.pending_offload_tasks_.empty());
        EXPECT_EQ(0u, backend->UngroupedOffloadingObjectsSize());
        auto query = client->Query("heartbeat-tail");
        ASSERT_TRUE(query);
        EXPECT_EQ(1u,
                  std::count_if(query->replicas.begin(), query->replicas.end(),
                                [](const auto& replica) {
                                    return replica.is_local_disk_replica();
                                }));
        std::vector<OffloadTaskItem> remaining;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, remaining));
        EXPECT_TRUE(remaining.empty());
        EXPECT_TRUE(client->Upsert("heartbeat-tail", slices, replicate));
    }

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
        ASSERT_EQ(file_storage.pending_offload_tasks_.size(), 1u);
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
        EXPECT_EQ(file_storage.pending_offload_tasks_.size(),
                  capacity_limit ? 0u : 1u);
        EXPECT_EQ(backend->UngroupedOffloadingObjectsSize(),
                  capacity_limit ? 0u : 1u);

        size_t persisted_count = 0;
        for (const auto& task : tasks) {
            const auto storage_key =
                TenantId(task.tenant_id)
                    .MakeScopedKey(MakeOffloadStorageKey(
                        task.key, task.object_generation.value_or("")));
            auto exists = backend->IsExist(storage_key);
            ASSERT_TRUE(exists.has_value());
            persisted_count += exists.value();
            // A transient write error must keep the tail pinned for retry;
            // a capacity shutdown must release every task immediately.
            auto overwritten = client->Upsert(task.key, slices, replicate);
            if (file_storage.pending_offload_tasks_.count(storage_key)) {
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
        std::unordered_map<std::string, std::string> physical_keys;
        for (const auto& task : tasks)
            physical_keys[task.key] =
                TenantId(task.tenant_id)
                    .MakeScopedKey(MakeOffloadStorageKey(
                        task.key, task.object_generation.value_or("")));
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
            duplicates.insert(physical_keys.at(keys[i]));
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
            const auto storage_key = physical_keys.at(keys[i]);
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

    void RunCarryoverScenario(const std::vector<size_t>& batches,
                              const std::string& terminal = "",
                              bool cross_tenant = false) {
        const auto quota_path = data_path + "/carryover-quota.yaml";
        {
            std::ofstream quota(quota_path);
            quota << "version: 1\ntenants:\n"
                     "  - name: default\n    quota: 1073741824\n"
                     "  - name: tenant-b\n    quota: 1073741824\n";
        }
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_rpc_port(0)
                                     .set_http_metrics_port(0)
                                     .set_root_fs_dir("")
                                     .build(),
                                 [&](WrappedMasterServiceConfig& config) {
                                     config.enable_multi_tenants = cross_tenant;
                                     config.tenant_quota_connector_uri =
                                         quota_path;
                                 }));
        constexpr size_t memory = 16 * 1024 * 1024, bytes = 512;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(memory), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(memory);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), memory, "tcp"));
        // Publish a complete snapshot before another client writes. Scalar
        // registrations can coalesce metadata asynchronously on this branch.
        if (cross_tenant) {
            ASSERT_TRUE(client->RegisterLocalMemory(allocator.getBase(), memory,
                                                    "cpu:0", false, false));
        } else {
            ASSERT_TRUE(client->RegisterLocalMemory(allocator.getBase(), memory,
                                                    "cpu:0", false, false));
        }
        ASSERT_TRUE(client->MountLocalDiskSegment(true));
        std::shared_ptr<Client> other = client;
        if (cross_tenant) {
            auto second = Client::Create(
                "127.0.0.1:" + std::to_string(getFreeTcpPort()),
                master.metadata_url(), "tcp", std::nullopt,
                master.master_address(), nullptr, {}, "tenant-b");
            ASSERT_TRUE(second);
            other = *second;
            ASSERT_TRUE(other->RegisterLocalMemory(allocator.getBase(), memory,
                                                   "cpu:0", false, false));
        }
        FileStorageConfig cfg = FileStorageConfig::FromEnvironment();
        cfg.storage_backend_type = StorageBackendType::kBucket;
        cfg.storage_filepath = data_path + "/carryover";
        cfg.local_buffer_size = 1024 * 1024;
        fs::create_directories(cfg.storage_filepath);
        auto storage = std::make_unique<FileStorage>(cfg, client, endpoint);
        BucketBackendConfig bc;
        bc.bucket_keys_limit = 32;
        std::shared_ptr<BucketStorageBackend> backend;
        std::shared_ptr<BlockingCarryoverBackend> blocked_backend;
        if (terminal == "drain_race") {
            blocked_backend =
                std::make_shared<BlockingCarryoverBackend>(cfg, bc);
            backend = blocked_backend;
        } else {
            backend = std::make_shared<ConcurrentOffloadBackend>(
                cfg, bc, std::unordered_set<std::string>{},
                terminal == "write_fail");
        }
        storage->storage_backend_ = backend;
        storage->enable_offloading_ = true;
        ASSERT_TRUE(backend->Init());
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        replicate.preferred_segment = endpoint;
        std::vector<std::string> keys, tenants;
        std::vector<std::shared_ptr<Client>> callers;
        std::vector<OffloadTaskItem> first_tasks;
        std::unordered_map<std::string, std::string> physical_keys;
        size_t ordinal = 0;
        for (size_t batch = 0; batch < batches.size(); ++batch) {
            for (size_t n = 0; n < batches[batch]; ++n, ++ordinal) {
                // The two tenants deliberately use the same user keys.
                const auto caller = cross_tenant && batch % 2 ? other : client;
                const auto key =
                    "carryover-" + std::to_string(cross_tenant ? n : ordinal);
                keys.push_back(key);
                tenants.push_back(caller->tenant_id());
                callers.push_back(caller);
                void* buffer = allocator.allocate(bytes);
                ASSERT_NE(buffer, nullptr);
                std::memset(buffer, static_cast<int>(ordinal + 1), bytes);
                std::vector<Slice> slices{{buffer, bytes}};
                ASSERT_TRUE(caller->Put(key, slices, replicate));
                allocator.deallocate(buffer, bytes);
            }
            std::vector<OffloadTaskItem> tasks;
            ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
            ASSERT_EQ(tasks.size(), batches[batch]);
            for (const auto& task : tasks)
                physical_keys[TenantId(task.tenant_id)
                                  .MakeScopedKey(task.key)] =
                    TenantId(task.tenant_id)
                        .MakeScopedKey(MakeOffloadStorageKey(
                            task.key, task.object_generation.value_or("")));
            if (batch == 0) first_tasks = tasks;
            tl::expected<void, ErrorCode> result;
            if (terminal == "drain_race" && batch + 1 == batches.size()) {
                auto entered = blocked_backend->entered.get_future();
                auto writing = std::async(std::launch::async, [&] {
                    return storage->OffloadObjects(tasks);
                });
                const bool reached =
                    entered.wait_for(std::chrono::seconds(5)) ==
                    std::future_status::ready;
                if (!reached) blocked_backend->release.set_value();
                ASSERT_TRUE(reached);
                auto draining = std::async(std::launch::async, [&] {
                    return storage->DrainLocalDiskSegment(0);
                });
                const auto deadline =
                    std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (!storage->draining_.load() &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::yield();
                }
                {
                    // Drain sets the latch and unmounts under this mutex.
                    // Taking it after observing the latch proves unmount is
                    // finished before the blocked write is released.
                    MutexLocker locker(&storage->offloading_mutex_);
                    EXPECT_TRUE(storage->draining_.load());
                }
                blocked_backend->release.set_value();
                result = writing.get();
                EXPECT_TRUE(draining.get());
            } else {
                result = storage->OffloadObjects(tasks);
            }
            if (terminal == "write_fail" && batch + 1 == batches.size()) {
                ASSERT_FALSE(result);
                EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
            } else if (terminal == "drain_race" &&
                       batch + 1 == batches.size()) {
                ASSERT_FALSE(result);
                // The local commit is preserved for every uncertain aggregate
                // notification result, even when drain made the mount vanish.
                EXPECT_EQ(result.error(),
                          ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            } else {
                ASSERT_TRUE(result);
            }
            if (batch == 0 && batches[0] < 32) {
                EXPECT_EQ(storage->pending_offload_tasks_.size(), batches[0]);
                EXPECT_EQ(backend->UngroupedOffloadingObjectsSize(),
                          batches[0]);
                void* buffer = allocator.allocate(bytes);
                ASSERT_NE(buffer, nullptr);
                std::memset(buffer, 99, bytes);
                std::vector<Slice> overwrite{{buffer, bytes}};
                for (size_t i = 0; i < keys.size(); ++i) {
                    const auto blocked =
                        callers[i]->Upsert(keys[i], overwrite, replicate);
                    EXPECT_FALSE(blocked);
                    if (!blocked) {
                        EXPECT_EQ(blocked.error(),
                                  ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                    }
                }
                allocator.deallocate(buffer, bytes);
                if (terminal == "remove_all") storage->RemoveAll();
                if (terminal == "drain") {
                    ASSERT_TRUE(storage->DrainLocalDiskSegment(0));
                    // Models a tick that fetched before drain but reached
                    // OffloadObjects only after drain returned.
                    ASSERT_TRUE(storage->OffloadObjects(first_tasks));
                    EXPECT_TRUE(storage->pending_offload_tasks_.empty());
                }
                if (terminal == "destruct") storage.reset();
                if (terminal == "delete_keys") {
                    // Local pending tasks can be cancelled before deleting
                    // metadata whose sources they still borrow.
                    ASSERT_TRUE(storage->DrainLocalDiskSegment(0));
                    for (size_t i = 0; i < keys.size(); ++i) {
                        ASSERT_TRUE(callers[i]->Remove(keys[i], true));
                    }
                }
                if (terminal == "duplicate_pending") {
                    ASSERT_TRUE(storage->OffloadObjects(first_tasks));
                }
            }
        }
        const bool discarded = terminal == "remove_all" ||
                               terminal == "drain" || terminal == "destruct" ||
                               terminal == "write_fail" ||
                               terminal == "drain_race";
        if (!discarded) {
            // An empty real heartbeat must advance the final incomplete
            // bucket; no sleeps or extended Master task TTL are needed.
            ASSERT_TRUE(storage->Heartbeat());
        }
        if (storage) EXPECT_TRUE(storage->pending_offload_tasks_.empty());
        if (terminal == "drain_race") {
            EXPECT_TRUE(storage->pending_notification_tasks_.empty());
            EXPECT_TRUE(storage->pending_notification_metadatas_.empty());
            ASSERT_TRUE(storage->OffloadObjects({}));
            ASSERT_TRUE(storage->Heartbeat());
            EXPECT_TRUE(storage->pending_notification_tasks_.empty());
        }
        EXPECT_EQ(backend->UngroupedOffloadingObjectsSize(), 0u);
        if (terminal == "old_task_replay") {
            ASSERT_TRUE(storage->OffloadObjects(first_tasks));
            EXPECT_TRUE(storage->pending_offload_tasks_.empty());
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            auto query = callers[i]->Query(keys[i]);
            if (terminal == "delete_keys") {
                EXPECT_FALSE(query);
                auto exists = backend->IsExist(physical_keys.at(
                    TenantId(tenants[i]).MakeScopedKey(keys[i])));
                ASSERT_TRUE(exists);
                EXPECT_FALSE(*exists);
                continue;
            }
            ASSERT_TRUE(query);
            size_t disk = 0;
            for (const auto& replica : query->replicas) {
                disk += replica.is_local_disk_replica();
            }
            EXPECT_EQ(disk, discarded ? 0u : 1u);
            const auto scoped =
                physical_keys.at(TenantId(tenants[i]).MakeScopedKey(keys[i]));
            auto exists = backend->IsExist(scoped);
            ASSERT_TRUE(exists);
            // Drain fences Master publication and cancels its tasks; it does
            // not delete an already committed bucket whose RPC reply failed.
            const bool locally_retained =
                !discarded || terminal == "drain_race";
            EXPECT_EQ(*exists, locally_retained);
            if (terminal == "drain_race") {
                const auto state =
                    test::MasterServiceTestPeer(*master.service())
                        .InspectOffloadForTesting(TenantId(tenants[i]),
                                                  keys[i]);
                EXPECT_EQ(state.task_count, 0u);
                EXPECT_EQ(state.memory_refs, 0u);
            }
            if (locally_retained) {
                std::string actual(bytes, '\0');
                std::unordered_map<std::string, Slice> read{
                    {scoped, {actual.data(), actual.size()}}};
                ASSERT_TRUE(backend->BatchLoad(read));
                EXPECT_EQ(actual, std::string(bytes, static_cast<char>(i + 1)));
            }
            // A terminal ACK or NACK must release the retained source/task,
            // without waiting for the Master's 600-second task reaper.
            void* buffer = allocator.allocate(bytes);
            ASSERT_NE(buffer, nullptr);
            std::memset(buffer, 77, bytes);
            std::vector<Slice> overwrite{{buffer, bytes}};
            const auto unblocked =
                callers[i]->Upsert(keys[i], overwrite, replicate);
            EXPECT_TRUE(unblocked) << keys[i];
            allocator.deallocate(buffer, bytes);
        }
        storage.reset();
        if (other != client) {
            EXPECT_TRUE(other->unregisterLocalMemory(allocator.getBase()));
            other.reset();
        }
        EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
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
        UnsetEnv("MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ");
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

    size_t ClientBufferBatchCount(FileStorage& fileStorage) {
        size_t total = 0;
        for (auto& shard : fileStorage.client_buffer_shards_) {
            MutexLocker locker(&shard.mutex);
            total += shard.batches.size();
        }
        return total;
    }

    std::shared_ptr<FileStorage::AllocatedBatch> ClientBufferBatch(
        FileStorage& fileStorage, uint64_t batch_id) {
        auto& shard = fileStorage.ShardFor(batch_id);
        MutexLocker locker(&shard.mutex);
        auto it = shard.batches.find(batch_id);
        return it == shard.batches.end() ? nullptr : it->second.batch;
    }

    bool ExpireClientBufferBatch(FileStorage& fileStorage, uint64_t batch_id) {
        auto& shard = fileStorage.ShardFor(batch_id);
        MutexLocker locker(&shard.mutex);
        auto it = shard.batches.find(batch_id);
        if (it == shard.batches.end()) return false;
        it->second.lease_timeout = std::chrono::steady_clock::now();
        return true;
    }

    size_t ClientBufferShardIndex(uint64_t batch_id) {
        return batch_id % FileStorage::kClientBufferShards;
    }

    FileStorage::BatchGetResult FileStorageShareStagingBuffer(
        FileStorage& fileStorage,
        const std::shared_ptr<FileStorage::AllocatedBatch>& staging) {
        return fileStorage.ShareStagingBuffer(staging, staging->pointers);
    }

    void SetPinnedRestoreArena(FileStorage& fileStorage, void* address,
                               size_t size) {
        fileStorage.pinned_restore_arena_allocator_ =
            ClientBufferAllocator::create(address, size);
    }

    std::shared_ptr<DelayedReadBackend> InstallDelayedReadBackend(
        FileStorage& storage, const FileStorageConfig& config) {
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 10;
        auto backend =
            std::make_shared<DelayedReadBackend>(config, bucket_config);
        storage.storage_backend_ = backend;
        return backend;
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
            offloading_objects, buckets_keys, false, deferred_keys);
    }

    tl::expected<void, ErrorCode> FileStorageOffloadObjects(
        FileStorage& fileStorage,
        const std::vector<OffloadTaskItem>& offloading_objects) {
        return fileStorage.OffloadObjects(offloading_objects);
    }

    std::unordered_map<std::string, OffloadTaskItem>
    GetDeferredTaskByStorageKey(FileStorage& fileStorage) {
        return fileStorage.pending_offload_tasks_;
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

    static bool IsDraining(FileStorage& fileStorage) {
        return fileStorage.draining_.load();
    }

    static void InjectDiskError(FileStorage& fileStorage, int err,
                                const char* op) {
        fileStorage.OnDiskError(err, op);
    }

    static bool IsDiskFenced(FileStorage& fileStorage) {
        return fileStorage.disk_fenced_.load();
    }

    static int CheckFilesystemOf(FileStorage& fileStorage, bool sync_first) {
        return fileStorage.CheckFilesystem(sync_first);
    }

    static std::vector<int> DiskRootFds(FileStorage& fileStorage) {
        std::vector<int> fds;
        for (const auto& root : fileStorage.disk_roots_) {
            fds.push_back(root.fd);
        }
        return fds;
    }

    static tl::expected<void, ErrorCode> InitStorageBackend(
        FileStorage& fileStorage) {
        return fileStorage.storage_backend_->Init();
    }

    // Reads values of several sizes back from the offset allocator backend
    // through FileStorage, whole and in windows, into the staging buffers
    // FileStorage allocates for them.
    void RunOffsetBackendLocalReadScenario(bool use_uring) {
        auto config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kOffsetAllocator;
        config.storage_filepath = data_path;
        config.total_size_limit = 64 * 1024 * 1024;
        config.local_buffer_size = 128 * 1024 * 1024;
        config.use_uring = use_uring;
        FileStorage fileStorage(config, nullptr, "localhost:9003");
        ASSERT_TRUE(InitStorageBackend(fileStorage));

        // 200 bytes is about what BatchOffloadUtil writes per key, 56960 is
        // the KV page size reported in issue #5272 and 131072 the default of
        // storage_backend_bench. The extents of the small values are not
        // multiples of 4 KiB, so the records after them do not start on a
        // page boundary.
        constexpr size_t kSizes[] = {200,  131072, 1000, 56960,
                                     1500, 11000,  777};
        constexpr size_t kKeyCount = 28;
        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<int64_t> sizes;
        for (size_t i = 0; i < kKeyCount; ++i) {
            std::string value(kSizes[i % std::size(kSizes)], '\0');
            for (size_t j = 0; j < value.size(); ++j) {
                value[j] =
                    static_cast<char>((j * 31 + (j >> 12) + i * 131) & 0xff);
            }
            keys.push_back("fs_offset_key_" + std::to_string(i));
            sizes.push_back(static_cast<int64_t>(value.size()));
            values.push_back(std::move(value));

            std::unordered_map<std::string, std::vector<Slice>> batch;
            batch.emplace(keys.back(),
                          std::vector<Slice>{Slice{values.back().data(),
                                                   values.back().size()}});
            auto stored = fileStorage.storage_backend_->BatchOffload(
                batch, [](const std::vector<std::string>&,
                          std::vector<StorageObjectMetadata>&) {
                    return ErrorCode::OK;
                });
            ASSERT_TRUE(stored.has_value()) << stored.error();
            ASSERT_EQ(stored.value(), 1);
        }

        // Both results stay alive, so the second batch gets other staging
        // buffers than the first.
        std::vector<FileStorage::LocalBatchResult> alive;
        for (int pass = 0; pass < 2; ++pass) {
            auto result = fileStorage.BatchGetLocal(keys, sizes);
            ASSERT_TRUE(result) << result.error();
            ASSERT_EQ(result->pointers.size(), keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                EXPECT_EQ(std::memcmp(reinterpret_cast<const char*>(
                                          result->pointers[i]),
                                      values[i].data(), values[i].size()),
                          0)
                    << "pass " << pass << " key " << keys[i];
            }
            alive.push_back(std::move(result.value()));
        }

        std::vector<int64_t> offsets;
        std::vector<int64_t> lengths;
        for (const auto size : sizes) {
            offsets.push_back(size / 3);
            lengths.push_back(std::min<int64_t>(size - size / 3, 5000));
        }
        auto ranges =
            fileStorage.BatchGetLocalRanges(keys, offsets, lengths, sizes);
        ASSERT_TRUE(ranges) << ranges.error();
        ASSERT_EQ(ranges->pointers.size(), keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            EXPECT_EQ(
                std::memcmp(reinterpret_cast<const char*>(ranges->pointers[i]),
                            values[i].data() + offsets[i],
                            static_cast<size_t>(lengths[i])),
                0)
                << "range of key " << keys[i];
        }
    }

    struct DiskFenceOutcome {
        bool fenced = false;
        int filesystem_checks = 0;
        int synced_checks = 0;          // checks that ran syncfs first
        int backend_loads = 0;          // after two reads
        bool gauge_exported = false;    // before any disk error
        bool segment_mounted = true;    // after two heartbeats
        bool offload_released = false;  // queued offload task, after them
        int64_t fenced_gauge = -1;
        bool fenced_offload_reached_backend = false;
        bool fenced_span_load_reached_backend = false;
        size_t carried_before_fence = 0;  // carry_before_fence only
        size_t carried_after_fence = 0;   // carried tasks + pooled keys
    };

    static constexpr int kRealFilesystemCheck = -1;

    // True if a serialized metric text has a sample (not only # HELP/# TYPE)
    // for the metric.
    static bool HasSample(const std::string& text, const std::string& name) {
        return text.rfind(name, 0) == 0 ||
               text.find("\n" + name) != std::string::npos;
    }

    // A store with a memory segment and a mounted LOCAL_DISK segment whose
    // disk reads fail with read_errno while the filesystem check answers
    // check_errno (kRealFilesystemCheck: the real check on the live test
    // directory). One put leaves an offload task queued at the master for
    // this store. Runs two reads, then two heartbeats, and records what
    // happened. With carry_before_fence, one heartbeat first takes the task
    // and defers its key to the backend's ungrouped pool (#3006 carry).
    void RunDiskErrorScenario(const std::string& name, int read_errno,
                              int check_errno, DiskFenceOutcome& out,
                              bool carry_before_fence = false) {
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
        const std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto client_result =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client_result.has_value());
        auto client = client_result.value();
        ASSERT_TRUE(client->MountSegment(segment.get(), kSegmentSize, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(
            allocator.getBase(), kSegmentSize, "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true).has_value());

        // The put queues an offload task for this store that holds a
        // reference on the memory replica. Once the store is no longer
        // registered, Upsert is refused until the master releases the task
        // (while the task is still queued, Upsert would cancel it itself).
        const std::string offload_key = "offload_key";
        const std::string value(64, 'v');
        void* buffer = allocator.allocate(value.size());
        ASSERT_NE(buffer, nullptr);
        std::memcpy(buffer, value.data(), value.size());
        std::vector<Slice> slices{{buffer, value.size()}};
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        ASSERT_TRUE(client->Put(offload_key, slices, replicate));

        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_filepath = data_path + "/" + name;
        config.local_buffer_size = 4 * 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        SsdMetric metric;
        FileStorage file_storage(config, client, local_rpc_addr, &metric);
        std::string metrics_text;
        metric.serialize(metrics_text);
        out.gauge_exported =
            HasSample(metrics_text, "mooncake_ssd_disk_fenced");
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = carry_before_fence ? 2 : 1;
        auto backend = std::make_shared<FailingReadBackend>(
            config, bucket_config, read_errno);
        file_storage.storage_backend_ = backend;
        backend->SetDiskErrorObserver([&file_storage](int err, const char* op) {
            file_storage.OnDiskError(err, op);
        });
        ASSERT_TRUE(backend->Init());
        if (check_errno != kRealFilesystemCheck) {
            file_storage.test_filesystem_check_ = [&out,
                                                   check_errno](bool sync) {
                ++out.filesystem_checks;
                out.synced_checks += sync ? 1 : 0;
                return check_errno;
            };
        }
        {
            MutexLocker locker(&file_storage.offloading_mutex_);
            file_storage.enable_offloading_ = true;
        }
        if (carry_before_fence) {
            (void)file_storage.Heartbeat();
            out.carried_before_fence =
                file_storage.pending_offload_tasks_.size();
        }

        for (int i = 0; i < 2; ++i) {
            std::string read_buffer(64, '\0');
            std::unordered_map<std::string, Slice> batch{
                {"key", Slice{read_buffer.data(), read_buffer.size()}}};
            EXPECT_FALSE(file_storage.BatchLoad(batch).has_value());
        }
        for (int i = 0; i < 2; ++i) {
            (void)file_storage.Heartbeat();
        }

        out.fenced = file_storage.disk_fenced_.load();
        out.backend_loads = backend->loads;
        out.carried_after_fence = file_storage.pending_offload_tasks_.size() +
                                  backend->UngroupedOffloadingObjectsSize();
        std::vector<OffloadTaskItem> items;
        out.segment_mounted =
            client->OffloadObjectHeartbeat(true, items).has_value();
        out.offload_released =
            client->Upsert(offload_key, slices, replicate).has_value();
        out.fenced_gauge = metric.ssd_disk_fenced.value();
        if (out.fenced && !carry_before_fence) {
            // Offload work already in hand must not reach a fenced disk. The
            // key has a memory replica on this store, so without the fence
            // this offload would reach the backend.
            auto offload = file_storage.OffloadObjects(
                {OffloadTaskItem{.tenant_id = TenantId::Default().value(),
                                 .key = offload_key,
                                 .size = static_cast<int64_t>(value.size())}});
            EXPECT_TRUE(offload.has_value());
            out.fenced_offload_reached_backend = backend->offloads > 0;
            // Neither may a byte-window read.
            std::string span_buffer(64, '\0');
            std::unordered_map<std::string, StorageValueSpan> spans{
                {"key", StorageValueSpan{
                            0, span_buffer.size(),
                            Slice{span_buffer.data(), span_buffer.size()}}}};
            EXPECT_FALSE(file_storage.BatchLoadSpans(spans).has_value());
            out.fenced_span_load_reached_backend = backend->span_loads > 0;
        }
        allocator.deallocate(buffer, value.size());
        EXPECT_TRUE(client->UnmountSegment(segment.get(), kSegmentSize));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    // An EIO from second_op arrives while the check for an EIO from first_op
    // is running (simulated by re-entering from inside the check). Check i
    // answers check_errnos[i], or 0 past the end; synced records whether
    // each check ran syncfs. Returns whether the disk tier is fenced.
    static bool FailDuringFilesystemCheck(FileStorage& fileStorage,
                                          const char* first_op,
                                          const char* second_op,
                                          std::vector<int> check_errnos,
                                          std::vector<bool>& synced) {
        fileStorage.test_filesystem_check_ =
            [&fileStorage, second_op, check_errnos, &synced](bool sync) {
                synced.push_back(sync);
                if (synced.size() == 1) fileStorage.OnDiskError(EIO, second_op);
                return synced.size() <= check_errnos.size()
                           ? check_errnos[synced.size() - 1]
                           : 0;
            };
        fileStorage.OnDiskError(EIO, first_op);
        return fileStorage.disk_fenced_.load();
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

        constexpr size_t memory = 16 * 1024 * 1024;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(memory), &std::free);
        ASSERT_NE(segment, nullptr);
        ASSERT_TRUE(
            fileStorage.client_->MountSegment(segment.get(), memory, "tcp"));
        SimpleAllocator input(memory);
        ASSERT_TRUE(fileStorage.client_->RegisterLocalMemory(
            input.getBase(), memory, "cpu:0", false, false));
        for (const auto& key : expected_keys) {
            const auto& original = batch_object.at(key).front();
            void* source = input.allocate(original.size);
            ASSERT_NE(source, nullptr);
            std::memcpy(source, original.ptr, original.size);
            std::vector<Slice> slices{{source, original.size}};
            ASSERT_TRUE(fileStorage.client_->Put(key, slices, {}));
            input.deallocate(source, original.size);
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(fileStorage.client_->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), expected_keys.size());
        ASSERT_TRUE(fileStorage.OffloadObjects(tasks));
        ASSERT_TRUE(fileStorage.OffloadObjects({}));
        auto cleared = fileStorage.client_->BatchReplicaClear(
            expected_keys, fileStorage.client_->getClientId(),
            fileStorage.local_rpc_addr_);
        ASSERT_TRUE(cleared);
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
        EXPECT_TRUE(fileStorage.client_->UnmountSegment(segment.get(), memory));
        EXPECT_TRUE(
            fileStorage.client_->unregisterLocalMemory(input.getBase()));
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
        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_default_kv_lease_ttl(1000)
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        std::unique_ptr<void, decltype(&std::free)> segment(nullptr,
                                                            &std::free);
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client.has_value());
        ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

        // Memory segment and registered buffers, so the retried PutStart can
        // allocate a memory replica and Get can read it back.
        constexpr size_t kSegSize = 64 * 1024 * 1024;
        segment.reset(allocate_buffer_allocator_memory(kSegSize));
        void* seg_ptr = segment.get();
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
        void* initial = allocator.allocate(original.size());
        ASSERT_NE(initial, nullptr);
        std::memcpy(initial, original.data(), original.size());
        std::vector<Slice> initial_slices{{initial, original.size()}};
        ASSERT_TRUE(client.value()->Put(key, initial_slices, {}));
        allocator.deallocate(initial, original.size());
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client.value()->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), 1u);
        ASSERT_TRUE(file_storage.OffloadObjects(tasks));
        ASSERT_TRUE(file_storage.OffloadObjects({}));
        std::unordered_set<std::string> cleared_keys;
        const auto clear_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (cleared_keys.size() < 1u &&
               std::chrono::steady_clock::now() < clear_deadline) {
            auto cleared = client.value()->BatchReplicaClear(
                {key}, client.value()->getClientId(), local_rpc_addr);
            ASSERT_TRUE(cleared);
            cleared_keys.insert(cleared->begin(), cleared->end());
            if (cleared_keys.size() < 1u)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(cleared_keys.size(), 1u);

        // Wire the probe the way RealClient does in production: existence
        // against this process's offload files.
        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists =
                    file_storage.Exists(TenantId::Default().MakeScopedKey(k));
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
        segment.reset();
    }
    void RunBatchPutHealWipeScenario(const std::string& data_path,
                                     bool with_key_metadata = false,
                                     bool with_affinity_only = false) {
        testing::InProcMaster master;
        auto master_config = InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_default_kv_lease_ttl(1000)
                                 .build();
        ASSERT_TRUE(master.Start(master_config));

        std::string local_rpc_addr =
            "127.0.0.1:" + std::to_string(getFreeTcpPort());
        std::unique_ptr<void, decltype(&std::free)> segment(nullptr,
                                                            &std::free);
        auto client =
            Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                           std::nullopt, master.master_address());
        ASSERT_TRUE(client.has_value());
        ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

        constexpr size_t kSegSize = 64 * 1024 * 1024;
        segment.reset(allocate_buffer_allocator_memory(kSegSize));
        void* seg_ptr = segment.get();
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
        for (const auto& key : wiped_keys) {
            void* initial = allocator.allocate(original.size());
            ASSERT_NE(initial, nullptr);
            std::memcpy(initial, original.data(), original.size());
            std::vector<Slice> initial_slices{{initial, original.size()}};
            ASSERT_TRUE(client.value()->Put(key, initial_slices, {}));
            allocator.deallocate(initial, original.size());
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client.value()->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), wiped_keys.size());
        ASSERT_TRUE(file_storage.OffloadObjects(tasks));
        ASSERT_TRUE(file_storage.OffloadObjects({}));
        std::unordered_set<std::string> cleared_keys;
        const auto clear_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (cleared_keys.size() < wiped_keys.size() &&
               std::chrono::steady_clock::now() < clear_deadline) {
            auto cleared = client.value()->BatchReplicaClear(
                wiped_keys, client.value()->getClientId(), local_rpc_addr);
            ASSERT_TRUE(cleared);
            cleared_keys.insert(cleared->begin(), cleared->end());
            if (cleared_keys.size() < wiped_keys.size())
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(cleared_keys.size(), wiped_keys.size());

        client.value()->SetLocalDiskProbe(
            [&file_storage](const std::string& k) -> std::optional<bool> {
                auto exists =
                    file_storage.Exists(TenantId::Default().MakeScopedKey(k));
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
        if (with_affinity_only) {
            cfg.residency_affinity_ids =
                std::vector<std::string>{"fresh-unit", "unit-a", "unit-b"};
        }
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
        segment.reset();
    }
    void RunUncertainNotificationScenario(const std::string& terminal = "") {
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build(),
                                 [](WrappedMasterServiceConfig& config) {
                                     config.enable_ha = true;
                                     config.enable_oplog = true;
                                     config.ha_backend_connstring.clear();
                                     config.ha_backend_type = "etcd";
                                 }));
        test::MasterServiceTestPeer peer(*master.service());
        NotificationRejectingWriter* writer = nullptr;
        peer.SetBatchOpLogWriterFactoryForTesting(
            [&writer](OrderedOpLogWriterConfig config,
                      OrderedOpLogWriter::WriteBatchFn write_batch) {
                auto created = std::make_unique<NotificationRejectingWriter>(
                    std::move(config), std::move(write_batch));
                writer = created.get();
                return created;
            });
        ASSERT_EQ(peer.SetBatchOpLogBackendForTesting(
                      std::make_shared<NotificationHaBackend>()),
                  ErrorCode::OK);
        ASSERT_NE(writer, nullptr);

        constexpr size_t memory = 16 * 1024 * 1024, bytes = 128;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(memory), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(memory);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                      std::nullopt, master.master_address());
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), memory, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(allocator.getBase(), memory,
                                                "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        auto put = [&](const std::string& key, char fill) {
            void* buffer = allocator.allocate(bytes);
            if (!buffer) return false;
            std::memset(buffer, fill, bytes);
            std::vector<Slice> slices{{buffer, bytes}};
            auto result = client->Put(key, slices, replicate);
            allocator.deallocate(buffer, bytes);
            return result.has_value();
        };
        std::map<std::string, std::string> values;
        // Four keys prove the first uncertain bucket stops the same cycle.
        // Terminal cases use just its two real offload tasks.
        const bool cancelling = terminal == "drain" || terminal == "remove_all";
        const size_t key_count = cancelling ? 2 : 4;
        for (size_t i = 0; i < key_count; ++i) {
            const auto key = "uncertain-" + std::to_string(i);
            values[key] = std::string(bytes, 'a' + i);
            ASSERT_TRUE(put(key, 'a' + i));
        }
        std::vector<OffloadTaskItem> tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, tasks));
        ASSERT_EQ(tasks.size(), key_count);

        std::unordered_map<std::string, std::string> physical_keys;
        for (const auto& task : tasks)
            physical_keys[task.key] =
                TenantId(task.tenant_id)
                    .MakeScopedKey(MakeOffloadStorageKey(
                        task.key, task.object_generation.value_or("")));
        auto physical_key = [&](const std::string& key) {
            return physical_keys.at(key);
        };
        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kBucket;
        config.storage_filepath = data_path + "/uncertain-notification";
        config.local_buffer_size = 1024 * 1024;
        config.enable_disk_watermark_eviction = true;
        config.disk_eviction_high_watermark_ratio = 1e-12;
        config.disk_eviction_low_watermark_ratio = 0.5e-12;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 2;
        auto backend =
            std::make_shared<NotificationCountingBucket>(config, bucket_config);
        storage.storage_backend_ = backend;
        ASSERT_TRUE(backend->Init());
        writer->RejectFromPutEnd(2);
        auto result = storage.OffloadObjects(tasks);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        ASSERT_EQ(storage.pending_notification_tasks_.size(), 2u);
        ASSERT_EQ(storage.pending_notification_metadatas_.size(), 2u);
        EXPECT_EQ(backend->payload_writes, 1u);
        std::set<std::string> committed;
        for (const auto& task : storage.pending_notification_tasks_)
            committed.insert(task.key);
        auto assert_readable = [&] {
            for (const auto& key : committed) {
                std::string read(bytes, '\0');
                std::unordered_map<std::string, Slice> reads{
                    {physical_key(key), {read.data(), read.size()}}};
                EXPECT_TRUE(backend->BatchLoad(reads));
                EXPECT_EQ(read, values.at(key));
            }
        };
        assert_readable();
        size_t published = 0, still_pending = 0;
        for (const auto& [key, value] : values) {
            auto query = client->Query(key);
            ASSERT_TRUE(query);
            size_t disks = 0;
            for (const auto& replica : query->replicas)
                disks += replica.is_local_disk_replica();
            const auto state =
                peer.InspectOffloadForTesting(TenantId::Default(), key);
            if (committed.contains(key)) {
                published += disks;
                still_pending += state.task_count;
                EXPECT_EQ(state.memory_refs, state.task_count);
            } else {
                EXPECT_EQ(disks, 0u);
                EXPECT_EQ(state.task_count, 0u);
                EXPECT_EQ(state.memory_refs, 0u);
                EXPECT_FALSE(*backend->IsExist(physical_key(key)));
            }
        }
        EXPECT_EQ(published, 1u);
        EXPECT_EQ(still_pending, 1u);
        EXPECT_EQ(peer.GetLocalDiskUsedBytesForTesting(client->getClientId()),
                  bytes);
        ASSERT_TRUE(storage.RunDiskWatermarkEviction());
        EXPECT_EQ(backend->watermark_passes, 0u);
        assert_readable();

        if (terminal == "generation_reissue") {
            writer->AllowCommits();
            auto reissued = storage.pending_notification_tasks_.front();
            for (const auto& task : storage.pending_notification_tasks_)
                if (peer.InspectOffloadForTesting(TenantId::Default(), task.key)
                        .task_count)
                    reissued = task;
            reissued.task_id = reissued.task_id.value_or("") + "-reissued";
            ASSERT_TRUE(peer.ReissueOffloadTaskForTesting(reissued));
            std::vector<StorageObjectMetadata> nacks(tasks.size(),
                                                     {-1, 0, 0, -1, ""});
            ASSERT_TRUE(client->NotifyOffloadSuccess(tasks, nacks));
            auto before = peer.InspectOffloadForTesting(TenantId::Default(),
                                                        reissued.key);
            EXPECT_EQ(before.task_count, 1u);
            EXPECT_EQ(before.memory_refs, 1u);
            ASSERT_TRUE(storage.OffloadObjects({reissued}));
            EXPECT_TRUE(storage.pending_notification_tasks_.empty());
            EXPECT_EQ(backend->payload_writes, 1u);
            auto after = peer.InspectOffloadForTesting(TenantId::Default(),
                                                       reissued.key);
            EXPECT_EQ(after.task_count, 0u);
            EXPECT_EQ(after.memory_refs, 0u);
            auto query = client->Query(reissued.key);
            ASSERT_TRUE(query);
            EXPECT_TRUE(std::any_of(query->replicas.begin(),
                                    query->replicas.end(),
                                    [](const auto& replica) {
                                        return replica.is_local_disk_replica();
                                    }));
            assert_readable();
            EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
            EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
            return;
        }
        if (terminal.starts_with("generation_")) {
            writer->AllowCommits();
            std::string victim;
            for (const auto& key : committed) {
                if (peer.InspectOffloadForTesting(TenantId::Default(), key)
                        .task_count == 0) {
                    victim = key;
                    break;
                }
            }
            ASSERT_FALSE(victim.empty());
            const bool rewrite = terminal.find("rewrite") != std::string::npos;
            const bool force = terminal.find("force") != std::string::npos;
            const auto old_generation =
                client->Query(victim)->object_generation;
            const auto old_physical = physical_key(victim);
            ASSERT_TRUE(client->Remove(victim, force));
            bool absent = false;
            for (int n = 0; n < 100; ++n) {
                if (!client->Query(victim)) {
                    absent = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            ASSERT_TRUE(absent);
            std::vector<OffloadTaskItem> fresh;
            if (rewrite) {
                ASSERT_TRUE(put(victim, 'z'));
                auto query = client->Query(victim);
                ASSERT_TRUE(query);
                ASSERT_FALSE(query->object_generation.empty());
                EXPECT_NE(query->object_generation, old_generation);
                ASSERT_TRUE(client->OffloadObjectHeartbeat(true, fresh));
                ASSERT_EQ(fresh.size(), 1u);
            }
            // Both previously vulnerable publication paths must reject old
            // generations, without releasing a replacement task's pin.
            ASSERT_TRUE(storage.ReRegisterOffloadedObjects());
            ASSERT_TRUE(storage.OffloadObjects({}));
            if (!rewrite) {
                EXPECT_FALSE(client->Query(victim));
            } else {
                auto state =
                    peer.InspectOffloadForTesting(TenantId::Default(), victim);
                EXPECT_EQ(state.task_count, 1u);
                EXPECT_EQ(state.memory_refs, 1u);
                std::vector<StorageObjectMetadata> nacks(tasks.size(),
                                                         {-1, 0, 0, -1, ""});
                ASSERT_TRUE(client->NotifyOffloadSuccess(tasks, nacks));
                state =
                    peer.InspectOffloadForTesting(TenantId::Default(), victim);
                EXPECT_EQ(state.task_count, 1u);
                EXPECT_EQ(state.memory_refs, 1u);
                auto query = client->Query(victim);
                ASSERT_TRUE(query);
                for (const auto& replica : query->replicas)
                    EXPECT_FALSE(replica.is_local_disk_replica());
                ASSERT_TRUE(storage.OffloadObjects(fresh));
                ASSERT_TRUE(storage.OffloadObjects({}));
                auto after_offload = client->Query(victim);
                ASSERT_TRUE(after_offload);
                std::string new_physical;
                for (const auto& replica : after_offload->replicas)
                    if (replica.is_local_disk_replica()) {
                        const auto& disk = replica.get_local_disk_descriptor();
                        EXPECT_EQ(disk.object_generation.value_or(""),
                                  after_offload->object_generation);
                        new_physical = TenantId::Default().MakeScopedKey(
                            disk.storage_key.value_or(""));
                    }
                ASSERT_FALSE(new_physical.empty());
                EXPECT_NE(new_physical, old_physical);
                std::string old_bytes(bytes, '\0'), new_bytes(bytes, '\0');
                std::unordered_map<std::string, Slice> reads{
                    {old_physical, {old_bytes.data(), bytes}},
                    {new_physical, {new_bytes.data(), bytes}}};
                ASSERT_TRUE(backend->BatchLoad(reads));
                EXPECT_EQ(old_bytes, values.at(victim));
                EXPECT_EQ(new_bytes, std::string(bytes, 'z'));
                // An old bucket's eviction notice cannot remove the current
                // generation's descriptor from the same Owner.
                const auto old_inner =
                    TenantId::ParseScopedKey(old_physical).second;
                ASSERT_TRUE(client->EvictDiskReplica(
                    old_inner, ReplicaType::LOCAL_DISK, true));
                auto after_old_eviction = client->Query(victim);
                ASSERT_TRUE(after_old_eviction);
                EXPECT_TRUE(
                    std::any_of(after_old_eviction->replicas.begin(),
                                after_old_eviction->replicas.end(),
                                [](const auto& replica) {
                                    return replica.is_local_disk_replica();
                                }));
                // Matching live metadata can restore its exact local record.
                const auto new_inner =
                    TenantId::ParseScopedKey(new_physical).second;
                ASSERT_TRUE(client->EvictDiskReplica(
                    new_inner, ReplicaType::LOCAL_DISK, true));
                ASSERT_TRUE(storage.ReRegisterOffloadedObjects());
                auto after_restore = client->Query(victim);
                ASSERT_TRUE(after_restore);
                EXPECT_TRUE(std::any_of(
                    after_restore->replicas.begin(),
                    after_restore->replicas.end(), [&](const auto& replica) {
                        return replica.is_local_disk_replica() &&
                               replica.get_local_disk_descriptor()
                                       .storage_key == new_inner;
                    }));
            }
            EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
            EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
            return;
        }
        if (!cancelling) {
            // Admit new tasks while the writer is healthy, then keep the
            // uncertain retry rejected. New work must be NACKed without
            // overwriting/deleting the protected bucket; same-key replay is
            // excluded from the NACK sweep.
            writer->AllowCommits();
            ASSERT_TRUE(put("late-0", 'x'));
            ASSERT_TRUE(put("late-1", 'y'));
            std::vector<OffloadTaskItem> later;
            ASSERT_TRUE(client->OffloadObjectHeartbeat(true, later));
            ASSERT_EQ(later.size(), 2u);
            for (const auto& task : later)
                physical_keys[task.key] =
                    TenantId(task.tenant_id)
                        .MakeScopedKey(MakeOffloadStorageKey(
                            task.key, task.object_generation.value_or("")));
            for (const auto& task : storage.pending_notification_tasks_) {
                if (peer.InspectOffloadForTesting(TenantId::Default(), task.key)
                        .task_count) {
                    later.push_back(task);
                    break;
                }
            }
            ASSERT_EQ(later.size(), 3u);
            writer->RejectFromPutEnd(1);
            auto blocked = storage.OffloadObjects(later);
            EXPECT_FALSE(blocked);
            EXPECT_EQ(backend->payload_writes, 1u);
            EXPECT_EQ(storage.pending_notification_tasks_.size(), 2u);
            for (const auto& key : {"late-0", "late-1"}) {
                const auto state =
                    peer.InspectOffloadForTesting(TenantId::Default(), key);
                EXPECT_EQ(state.task_count, 0u);
                EXPECT_EQ(state.memory_refs, 0u);
                EXPECT_FALSE(*backend->IsExist(physical_key(key)));
            }
            size_t remaining = 0;
            for (const auto& key : committed)
                remaining +=
                    peer.InspectOffloadForTesting(TenantId::Default(), key)
                        .task_count;
            EXPECT_EQ(remaining, 1u);
            assert_readable();
            writer->AllowCommits();
            if (terminal == "lost_reply") {
                // Construct the state after a successful Master notification
                // whose reply the Owner did not observe: real RPC completes
                // every task, while FileStorage deliberately retains its
                // uncertain batch. This is not a network-fault injection.
                ASSERT_TRUE(client->NotifyOffloadSuccess(
                    storage.pending_notification_tasks_,
                    storage.pending_notification_metadatas_));
                ASSERT_EQ(storage.pending_notification_tasks_.size(), 2u);
                EXPECT_EQ(
                    peer.GetLocalDiskUsedBytesForTesting(client->getClientId()),
                    2 * bytes);
                for (const auto& key : committed) {
                    const auto state =
                        peer.InspectOffloadForTesting(TenantId::Default(), key);
                    EXPECT_EQ(state.task_count, 0u);
                    EXPECT_EQ(state.memory_refs, 0u);
                }
                assert_readable();
            }
            ASSERT_TRUE(storage.OffloadObjects({}));
            EXPECT_TRUE(storage.pending_notification_tasks_.empty());
            EXPECT_TRUE(storage.pending_notification_metadatas_.empty());
            EXPECT_EQ(backend->payload_writes, 1u);
            EXPECT_EQ(
                peer.GetLocalDiskUsedBytesForTesting(client->getClientId()),
                2 * bytes);
            ASSERT_TRUE(storage.OffloadObjects({}));
            EXPECT_EQ(
                peer.GetLocalDiskUsedBytesForTesting(client->getClientId()),
                2 * bytes);
            for (const auto& key : committed) {
                auto query = client->Query(key);
                ASSERT_TRUE(query);
                size_t disks = 0;
                for (const auto& replica : query->replicas)
                    disks += replica.is_local_disk_replica();
                EXPECT_EQ(disks, 1u);
                const auto state =
                    peer.InspectOffloadForTesting(TenantId::Default(), key);
                EXPECT_EQ(state.task_count, 0u);
                EXPECT_EQ(state.memory_refs, 0u);
            }
            assert_readable();
        } else {
            writer->AllowCommits();
            if (terminal == "drain") {
                ASSERT_TRUE(storage.DrainLocalDiskSegment(0));
            } else {
                // Match the heartbeat's Master-remove-all -> physical wipe
                // order rather than leaving advertised replicas behind.
                EXPECT_GE(master.service()->RemoveAll(true), 0);
                storage.RemoveAll();
            }
            EXPECT_TRUE(storage.pending_notification_tasks_.empty());
            EXPECT_TRUE(storage.pending_notification_metadatas_.empty());
            ASSERT_TRUE(storage.OffloadObjects({}));
            ASSERT_TRUE(storage.Heartbeat());
            EXPECT_EQ(backend->payload_writes, 1u);
            for (const auto& key : committed) {
                const auto state =
                    peer.InspectOffloadForTesting(TenantId::Default(), key);
                EXPECT_EQ(state.task_count, 0u);
                EXPECT_EQ(state.memory_refs, 0u);
                auto query = client->Query(key);
                if (query) {
                    for (const auto& replica : query->replicas)
                        EXPECT_FALSE(replica.is_local_disk_replica());
                }
            }
            if (terminal == "drain")
                assert_readable();
            else {
                for (const auto& key : committed)
                    EXPECT_FALSE(*backend->IsExist(physical_key(key)));
            }
        }
        EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
    }

    void RunGenerationCarryoverScenario(bool tenant_scope,
                                        bool blocked_source = false) {
        const auto policy_path = data_path + "/generation-tenant.yaml";
        {
            std::ofstream policy(policy_path);
            policy << "version: 1\ntenants:\n  - name: default\n    quota: "
                      "1073741824\n  - name: tenant-b\n    quota: 1073741824\n";
        }
        testing::InProcMaster master;
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_default_kv_lease_ttl(0)
                                     .set_root_fs_dir("")
                                     .build(),
                                 [&](WrappedMasterServiceConfig& config) {
                                     config.enable_multi_tenants = tenant_scope;
                                     config.tenant_quota_connector_uri =
                                         policy_path;
                                 }));
        constexpr size_t memory = 16 * 1024 * 1024, bytes = 128;
        std::unique_ptr<void, decltype(&std::free)> segment(
            allocate_buffer_allocator_memory(memory), &std::free);
        ASSERT_NE(segment, nullptr);
        SimpleAllocator allocator(memory);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        auto created =
            Client::Create(endpoint, master.metadata_url(), "tcp", std::nullopt,
                           master.master_address(), nullptr, {},
                           tenant_scope ? "tenant-b" : "default");
        ASSERT_TRUE(created);
        auto client = *created;
        ASSERT_TRUE(client->MountSegment(segment.get(), memory, "tcp"));
        ASSERT_TRUE(client->RegisterLocalMemory(allocator.getBase(), memory,
                                                "cpu:0", false, false));
        ASSERT_TRUE(client->MountLocalDiskSegment(true));
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        auto put = [&](char byte) {
            void* buffer = allocator.allocate(bytes);
            if (!buffer) return false;
            std::memset(buffer, byte, bytes);
            std::vector<Slice> slices{{buffer, bytes}};
            auto result = client->Put("same-carryover-key", slices, replicate);
            allocator.deallocate(buffer, bytes);
            return bool(result);
        };
        ASSERT_TRUE(put('a'));
        std::vector<OffloadTaskItem> old_tasks;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, old_tasks));
        ASSERT_EQ(old_tasks.size(), 1u);
        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_backend_type = StorageBackendType::kBucket;
        config.storage_filepath = data_path + "/generation-carryover";
        config.local_buffer_size = 1024 * 1024;
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, endpoint);
        BucketBackendConfig bucket;
        bucket.bucket_keys_limit = 2;
        if (blocked_source) {
            bucket.bucket_keys_limit = 1;
            auto backend =
                std::make_shared<BlockingCarryoverBackend>(config, bucket);
            storage.storage_backend_ = backend;
            ASSERT_TRUE(backend->Init());
            auto entered = backend->entered.get_future();
            auto writing = std::async(std::launch::async, [&] {
                return storage.OffloadObjects(old_tasks);
            });
            const auto reached = entered.wait_for(std::chrono::seconds(5));
            if (reached != std::future_status::ready)
                backend->release.set_value();
            ASSERT_EQ(reached, std::future_status::ready);
            test::MasterServiceTestPeer peer(*master.service());
            ASSERT_TRUE(peer.AgeOffloadTaskForTesting(
                TenantId(client->tenant_id()), old_tasks.front().key));
            peer.RunBatchEvictForTesting(1.0, 1.0);
            auto pinned = peer.InspectOffloadForTesting(
                TenantId(client->tenant_id()), old_tasks.front().key);
            EXPECT_EQ(pinned.task_count, 1u);
            EXPECT_EQ(pinned.memory_refs, 1u);
            void* overwrite = allocator.allocate(bytes);
            ASSERT_NE(overwrite, nullptr);
            std::memset(overwrite, 'z', bytes);
            std::vector<Slice> overwrite_slices{{overwrite, bytes}};
            EXPECT_FALSE(client->Upsert("same-carryover-key", overwrite_slices,
                                        replicate));
            allocator.deallocate(overwrite, bytes);
            backend->release.set_value();
            ASSERT_TRUE(writing.get());
            auto settled = peer.InspectOffloadForTesting(
                TenantId(client->tenant_id()), old_tasks.front().key);
            EXPECT_EQ(settled.task_count, 0u);
            EXPECT_EQ(settled.memory_refs, 0u);
            const auto physical =
                TenantId(client->tenant_id())
                    .MakeScopedKey(MakeOffloadStorageKey(
                        old_tasks.front().key,
                        old_tasks.front().object_generation.value_or("")));
            std::string actual(bytes, '\0');
            std::unordered_map<std::string, Slice> read{
                {physical, {actual.data(), bytes}}};
            ASSERT_TRUE(backend->BatchLoad(read));
            EXPECT_EQ(actual, std::string(bytes, 'a'));
            EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
            EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
            return;
        }
        auto backend =
            std::make_shared<NotificationCountingBucket>(config, bucket);
        storage.storage_backend_ = backend;
        ASSERT_TRUE(backend->Init());
        ASSERT_TRUE(storage.OffloadObjects(old_tasks));
        EXPECT_EQ(storage.pending_offload_tasks_.size(), 1u);
        {
            MutexLocker task_lock(&storage.offload_tasks_mutex_);
            storage.CancelPendingOffloadsLocked();
        }
        ASSERT_TRUE(client->Remove("same-carryover-key", true));
        ASSERT_TRUE(put('z'));
        std::vector<OffloadTaskItem> fresh;
        ASSERT_TRUE(client->OffloadObjectHeartbeat(true, fresh));
        ASSERT_EQ(fresh.size(), 1u);
        ASSERT_NE(fresh.front().object_generation,
                  old_tasks.front().object_generation);
        ASSERT_TRUE(storage.OffloadObjects(fresh));
        ASSERT_TRUE(storage.OffloadObjects({}));
        EXPECT_TRUE(storage.pending_offload_tasks_.empty());
        const auto old_physical =
            TenantId(client->tenant_id())
                .MakeScopedKey(MakeOffloadStorageKey(
                    old_tasks.front().key,
                    old_tasks.front().object_generation.value_or("")));
        const auto new_physical =
            TenantId(client->tenant_id())
                .MakeScopedKey(MakeOffloadStorageKey(
                    fresh.front().key,
                    fresh.front().object_generation.value_or("")));
        EXPECT_FALSE(*backend->IsExist(old_physical));
        EXPECT_TRUE(*backend->IsExist(new_physical));
        EXPECT_EQ(backend->payload_writes, 1u);
        std::string actual(bytes, '\0');
        std::unordered_map<std::string, Slice> reads{
            {new_physical, {actual.data(), bytes}}};
        ASSERT_TRUE(backend->BatchLoad(reads));
        EXPECT_EQ(actual, std::string(bytes, 'z'));
        auto state = test::MasterServiceTestPeer(*master.service())
                         .InspectOffloadForTesting(
                             TenantId(client->tenant_id()), fresh.front().key);
        EXPECT_EQ(state.task_count, 0u);
        EXPECT_EQ(state.memory_refs, 0u);
        EXPECT_TRUE(client->UnmountSegment(segment.get(), memory));
        EXPECT_TRUE(client->unregisterLocalMemory(allocator.getBase()));
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

#ifdef USE_URING
// Pages this process has pinned, in KiB (VmPin).
static long PinnedKib() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmPin:", 0) == 0) return std::stol(line.substr(6));
    }
    return -1;
}

// With io_uring, FileStorage pins its staging buffer as the fixed buffer. The
// destructor must unpin it before the buffer is freed.
TEST_F(FileStorageTest, UringStagingBufferUnpinnedOnDestruction) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.use_uring = true;
    const long pinned_before = PinnedKib();
    long pinned_alive = -1;
    {
        FileStorage file_storage(file_storage_config, nullptr,
                                 "localhost:9003");
        pinned_alive = PinnedKib();
    }
    if (pinned_alive <= pinned_before) {
        GTEST_SKIP() << "io_uring did not pin the staging buffer here "
                        "(no buffer clone support or RLIMIT_MEMLOCK too small)";
    }
    EXPECT_LE(PinnedKib(), pinned_before);
}

TEST_F(FileStorageTest, OffsetBackendLocalReadsWithUring) {
    RunOffsetBackendLocalReadScenario(/*use_uring=*/true);
}
#endif

TEST_F(FileStorageTest, OffsetBackendLocalReads) {
    RunOffsetBackendLocalReadScenario(/*use_uring=*/false);
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

    SetEnv("MOONCAKE_DFS_FS_ADAPTER", "kvcs-lowlevel");
    {
        FileStorage file_storage(config, nullptr, "localhost:9003");
        EXPECT_TRUE(FileStorageUsesDfsControlPlane(file_storage));
    }
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
    // Default grouping defers the short tail until the next heartbeat.
    ASSERT_EQ(buckets_keys.size(), 3);
    size_t grouped = 0;
    std::unordered_set<std::string> grouped_keys;
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
        grouped += bucket_keys.size();
        grouped_keys.insert(bucket_keys.begin(), bucket_keys.end());
    }
    EXPECT_EQ(grouped, 30);
    EXPECT_EQ(grouped_keys.size(), 30);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    grouped_keys.clear();
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
        grouped_keys.insert(bucket_keys.begin(), bucket_keys.end());
    }
    EXPECT_EQ(grouped_keys.size(), 40);
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
    buckets_keys.clear();

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
    EXPECT_EQ(unique_keys.count("duplicate"), 1u);
    EXPECT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_flushes_replayed_tail) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    offloading_objects.emplace("tail-0", 1);
    offloading_objects.emplace("tail-1", 1);

    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_TRUE(buckets_keys.empty());
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 2);

    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 1);
    EXPECT_EQ(buckets_keys.front().size(), 2);
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
    // Default grouping defers the short tail until the next heartbeat.
    ASSERT_EQ(buckets_keys.size(), 3);
    size_t grouped = 0;
    std::unordered_set<std::string> grouped_keys;
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
        grouped += bucket_keys.size();
        grouped_keys.insert(bucket_keys.begin(), bucket_keys.end());
    }
    EXPECT_EQ(grouped, 30);
    EXPECT_EQ(grouped_keys.size(), 30);
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    for (size_t i = 35; i < 40; ++i) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    grouped_keys.clear();
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
        grouped_keys.insert(bucket_keys.begin(), bucket_keys.end());
    }
    EXPECT_EQ(grouped_keys.size(), 40);
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

TEST_F(FileStorageTest, EmptyHeartbeatFlushesDeferredTail) {
    RunDeferredHeartbeatTailScenario();
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
// Regression (review follow-up on the re-drain collision path): a carried key
// re-drained this cycle and then skipped outright by the backend — re-PUT at a
// size over bucket_size_limit, which GroupOffloadingKeysByBucket drops without
// bucketing OR deferring — must NACK the FRESH task and must not retain the
// stale one. Retention here would be doubly wrong: the stale task is stranded
// forever (its pooled copy was dropped on collision, so the pool can never
// re-emit it) and its presence in carried_tasks suppresses the fresh task's
// NACK, losing the task and leaking its source-replica refcount.
// The exclusion must be keyed on "re-drained this cycle", NOT on "present in
// task_by_storage_key": the carry merge inserts every non-collision carried
// task into that map, so testing membership there would void retention for
// every carried key and re-introduce the task-less drop this PR fixes. This
// pins the distinction — same shape as above but WITHOUT the collision.
// Regression (upstream #3006, integration): drive the deferral carry through
// OffloadObjects itself. A cycle in which every key defers emits no bucket,
// so nothing touches the master client — which makes the integrated path
// (carry merge, deferral report plumbing, partition, carry assignment)
// testable without a client. Phases 3 and 4 exercise re-drained keys: their
// pooled copies are removed and the fresh tasks and sizes are re-deferred.
TEST_F(FileStorageTest, OffloadObjects_deferred_tail_tasks_carried) {
    RunCarryoverScenario({2, 1, 1});
}

TEST_F(FileStorageTest, HeartbeatRunsDiskWatermarkEvictionWithoutOffloadWork) {
    std::filesystem::path master_root =
        std::filesystem::path(data_path) / "heartbeat_master";
    std::filesystem::create_directories(master_root);

    testing::InProcMaster master;
    auto master_config = InProcMasterConfigBuilder()
                             .set_enable_offload(true)
                             .set_root_fs_dir("")
                             .set_default_kv_lease_ttl(0)
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
    const auto policy_path = data_path + "/notify-tenants.yaml";
    {
        std::ofstream policy(policy_path);
        policy << "version: 1\ntenants:\n  - name: default\n    quota: "
                  "1073741824\n  - name: tenant_a\n    quota: 1073741824\n  - "
                  "name: tenant_b\n    quota: 1073741824\n";
    }
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                 .set_enable_offload(true)
                                 .set_default_kv_lease_ttl(0)
                                 .set_root_fs_dir("")
                                 .build(),
                             [&](WrappedMasterServiceConfig& config) {
                                 config.enable_multi_tenants = true;
                                 config.tenant_quota_connector_uri =
                                     policy_path;
                             }));
    constexpr size_t memory = 16 * 1024 * 1024;
    std::unique_ptr<void, decltype(&std::free)> segment(
        allocate_buffer_allocator_memory(memory), &std::free);
    ASSERT_NE(segment, nullptr);
    SimpleAllocator payload(memory);
    const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto owner_created = Client::Create(endpoint, master.metadata_url(), "tcp",
                                        std::nullopt, master.master_address());
    ASSERT_TRUE(owner_created);
    auto owner = *owner_created;
    ASSERT_TRUE(owner->MountSegment(segment.get(), memory, "tcp"));
    ASSERT_TRUE(owner->MountLocalDiskSegment(true));
    // Publish the complete source Segment snapshot before a different
    // tenant client performs a remote transfer into it.
    ASSERT_TRUE(owner->RegisterLocalMemory(payload.getBase(), memory, "cpu:0",
                                           false, false));
    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/tenant_notify";
    config.local_buffer_size = 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage storage(config, owner, endpoint);
    ASSERT_TRUE(InitStorageBackend(storage));
    std::vector<std::shared_ptr<Client>> writers;
    const std::string key = "shared_key";
    for (const auto& tenant : {"tenant_a", "tenant_b"}) {
        auto created =
            Client::Create("127.0.0.1:" + std::to_string(getFreeTcpPort()),
                           master.metadata_url(), "tcp", std::nullopt,
                           master.master_address(), nullptr, {}, tenant);
        ASSERT_TRUE(created);
        auto writer = *created;
        ASSERT_TRUE(writer->RegisterLocalMemory(payload.getBase(), memory,
                                                "cpu:0", false, false));
        void* source = payload.allocate(128);
        ASSERT_NE(source, nullptr);
        std::memset(source, tenant[7], 128);
        std::vector<Slice> slices{{source, 128}};
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        replicate.preferred_segments = {endpoint};
        ASSERT_TRUE(writer->Put(key, slices, replicate));
        payload.deallocate(source, 128);
        writers.push_back(writer);
    }
    std::vector<OffloadTaskItem> tasks;
    ASSERT_TRUE(owner->OffloadObjectHeartbeat(true, tasks));
    ASSERT_EQ(tasks.size(), 2u);
    ASSERT_TRUE(FileStorageOffloadObjects(storage, tasks));
    ASSERT_TRUE(FileStorageOffloadObjects(storage, {}));
    for (const auto& writer : writers) {
        auto cleared =
            writer->BatchReplicaClear({key}, writer->getClientId(), endpoint);
        ASSERT_TRUE(cleared);
        ASSERT_EQ(cleared->size(), 1u);
        auto before = writer->Query(key);
        ASSERT_TRUE(before);
        for (const auto& replica : before->replicas)
            ASSERT_TRUE(replica.is_local_disk_replica());
    }
    // Pre-upgrade legacy (non-@mcssd2:) eviction keys must NOT wedge disk
    // eviction with INVALID_PARAMS and must NOT evict newer fenced replicas.
    std::vector<std::string> legacy_keys{
        TenantId("tenant_a").MakeScopedKey(key),
        TenantId("tenant_b").MakeScopedKey("absent_legacy_key")};
    ASSERT_TRUE(FileStorageNotifyEvictedDiskReplicas(storage, legacy_keys));
    for (const auto& writer : writers) {
        auto still_present = writer->Query(key);
        ASSERT_TRUE(still_present);
    }

    std::vector<std::string> physical;
    for (const auto& task : tasks)
        physical.push_back(
            TenantId(task.tenant_id)
                .MakeScopedKey(MakeOffloadStorageKey(
                    task.key, task.object_generation.value_or(""))));
    ASSERT_TRUE(FileStorageNotifyEvictedDiskReplicas(storage, physical));
    for (const auto& writer : writers) EXPECT_FALSE(writer->Query(key));
    for (const auto& writer : writers)
        EXPECT_TRUE(writer->unregisterLocalMemory(payload.getBase()));
    EXPECT_TRUE(owner->unregisterLocalMemory(payload.getBase()));
    EXPECT_TRUE(owner->UnmountSegment(segment.get(), memory));
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

TEST_F(FileStorageTest, RemoveAllPreservesManagedSsdPayload) {
    RunManagedRemoveAllScenario();
}

TEST_F(FileStorageTest, RemoveAllOrdinarySsdReclaimedAtWatermark) {
    RunOrdinaryRemoveAllReclaimScenario(StorageBackendType::kFilePerKey);
}

TEST_F(FileStorageTest, RemoveAllOrdinaryBucketReclaimedAtWatermark) {
    RunOrdinaryRemoveAllReclaimScenario(StorageBackendType::kBucket);
}

TEST_F(FileStorageTest, RemoveAllReclaimsOrdinaryFileBesideManagedFiles) {
    RunManagedRemoveAllScenario(true);
}

TEST_F(FileStorageTest, RemoveAllReclaimsOrdinaryFileAfterProtectedPrefix) {
    RunManagedRemoveAllScenario(true, true);
}

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

TEST_F(FileStorageTest,
       AffinityOnlyBatchPutAfterPhysicalWipeHealsDanglingLocalDiskReplica) {
    RunBatchPutHealWipeScenario(data_path, false, true);
}

TEST_F(FileStorageTest, ClassifyDiskErrno) {
    using Action = FileStorage::DiskErrorAction;
    for (int err : {ENODEV, ENXIO, EROFS, ESHUTDOWN}) {
        EXPECT_EQ(FileStorage::ClassifyDiskErrno(err), Action::kFence) << err;
    }
    for (int err : {EIO, EUCLEAN, EBADMSG, ENODATA, EREMOTEIO, EILSEQ,
                    ETIMEDOUT, ENOLINK}) {
        EXPECT_EQ(FileStorage::ClassifyDiskErrno(err), Action::kCheckFilesystem)
            << err;
    }
    // 0 is a logical failure (short read/write) with no syscall error.
    for (int err : {0, ENOSPC, EDQUOT, EFBIG, ENOENT, EEXIST, EACCES, EMFILE,
                    ENFILE, ENOMEM, EBADF, EISDIR, EINTR}) {
        EXPECT_EQ(FileStorage::ClassifyDiskErrno(err), Action::kIgnore) << err;
    }
}

// EIO on a filesystem that is shut down: the first failure fences the disk
// tier. Reads stop reaching the disk at once, offload stops, the heartbeat
// hands the queued offload task back to the master (releasing the memory
// replica it pinned), unmounts the segment and never re-mounts it.
TEST_F(FileStorageTest, DiskFenceEioOnDeadFilesystemFencesDiskTier) {
    DiskFenceOutcome out;
    RunDiskErrorScenario("fence_eio_dead", EIO, EIO, out);
    EXPECT_TRUE(out.fenced);
    EXPECT_EQ(out.filesystem_checks, 1);
    EXPECT_EQ(out.synced_checks, 0);  // a read failure checks without syncfs
    EXPECT_EQ(out.backend_loads, 1);
    EXPECT_FALSE(out.segment_mounted);
    EXPECT_TRUE(out.offload_released);
    EXPECT_EQ(out.fenced_gauge, 1);
    EXPECT_FALSE(out.fenced_offload_reached_backend);
    EXPECT_FALSE(out.fenced_span_load_reached_backend);
}

// EIO while the filesystem is alive (real fstat on the live directory): one
// bad file, not a dead disk. Nothing is fenced.
TEST_F(FileStorageTest, DiskFenceEioOnLiveFilesystemDoesNotFence) {
    DiskFenceOutcome out;
    RunDiskErrorScenario("fence_eio_live", EIO, kRealFilesystemCheck, out);
    EXPECT_FALSE(out.fenced);
    EXPECT_EQ(out.backend_loads, 2);
    EXPECT_TRUE(out.segment_mounted);
    EXPECT_TRUE(out.gauge_exported);
    EXPECT_EQ(out.fenced_gauge, 0);
}

// ENOSPC never fences and never consults the filesystem check, even when
// that check would fail: eviction can free space.
TEST_F(FileStorageTest, DiskFenceEnospcIsIgnored) {
    DiskFenceOutcome out;
    RunDiskErrorScenario("fence_enospc", ENOSPC, EIO, out);
    EXPECT_FALSE(out.fenced);
    EXPECT_EQ(out.filesystem_checks, 0);
    EXPECT_EQ(out.backend_loads, 2);
    EXPECT_TRUE(out.segment_mounted);
}

// ENODEV fences without a filesystem check, even when that check passes.
TEST_F(FileStorageTest, DiskFenceEnodevFencesWithoutFilesystemCheck) {
    DiskFenceOutcome out;
    RunDiskErrorScenario("fence_enodev", ENODEV, 0, out);
    EXPECT_TRUE(out.fenced);
    EXPECT_EQ(out.filesystem_checks, 0);
    EXPECT_EQ(out.backend_loads, 1);
    EXPECT_FALSE(out.segment_mounted);
    EXPECT_TRUE(out.offload_released);
}

// With several disks in storage_filepath, the filesystem check opens and
// checks every root, instead of stat-ing the whole list as one path (ENOENT,
// which would let a dead disk go unfenced).
TEST_F(FileStorageTest, DiskFenceChecksEveryOffloadRoot) {
    const auto root_a = fs::path(data_path) / "fence_root_a";
    const auto root_b = fs::path(data_path) / "fence_root_b";
    fs::create_directories(root_a);
    fs::create_directories(root_b);
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = root_a.string() + "," + root_b.string();
    FileStorage file_storage(config, nullptr, "localhost:9004");
    const std::vector<int> fds = DiskRootFds(file_storage);
    ASSERT_EQ(fds.size(), 2u);
    EXPECT_GE(fds[0], 0);
    EXPECT_GE(fds[1], 0);
    EXPECT_EQ(CheckFilesystemOf(file_storage, /*sync_first=*/false), 0);
    EXPECT_EQ(CheckFilesystemOf(file_storage, /*sync_first=*/true), 0);
}

// A task carried across a bucket deferral (#3006) is NACKed by the fence:
// nothing re-emits the pool afterwards, and the task would pin its source
// replica until the master's TTL reaper.
TEST_F(FileStorageTest, DiskFenceNacksCarriedOffloadTasks) {
    DiskFenceOutcome out;
    RunDiskErrorScenario("fence_carry", EIO, EIO, out,
                         /*carry_before_fence=*/true);
    EXPECT_EQ(out.carried_before_fence, 1u);
    EXPECT_TRUE(out.fenced);
    EXPECT_FALSE(out.segment_mounted);
    EXPECT_EQ(out.carried_after_fence, 0u);
    EXPECT_TRUE(out.offload_released);
}

// Concurrent failures do not run parallel filesystem checks: one that finds
// a check running is queued behind it. Once the running check fences, the
// queued failure needs no check. A write failure's check runs syncfs first.
TEST_F(FileStorageTest, DiskFenceRunsOneCheckAtATime) {
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    FileStorage file_storage(config, nullptr, "localhost:9003");
    std::vector<bool> synced;
    EXPECT_TRUE(FailDuringFilesystemCheck(file_storage, "pwritev", "preadv",
                                          {EIO}, synced));
    EXPECT_EQ(synced, std::vector<bool>{true});
}

// A write failure that arrives during a read failure's fstat-only check is
// checked after it, with syncfs: XFS is usually still up until syncfs forces
// the log write, so only the write's check can find the dead device.
TEST_F(FileStorageTest, DiskFenceChecksWriteFailureQueuedBehindReadCheck) {
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    FileStorage file_storage(config, nullptr, "localhost:9003");
    std::vector<bool> synced;
    EXPECT_TRUE(FailDuringFilesystemCheck(file_storage, "preadv", "pwritev",
                                          {0, EIO}, synced));
    EXPECT_EQ(synced, (std::vector<bool>{false, true}));
}

// When the queued check passes too, the checking slot is released: a later
// failure runs its own check.
TEST_F(FileStorageTest, DiskFenceQueuedCheckReleasesTheCheckingSlot) {
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    FileStorage file_storage(config, nullptr, "localhost:9003");
    std::vector<bool> synced;
    EXPECT_FALSE(FailDuringFilesystemCheck(file_storage, "preadv", "pwritev",
                                           {0, 0}, synced));
    EXPECT_EQ(synced, (std::vector<bool>{false, true}));
    InjectDiskError(file_storage, EIO, "read");
    EXPECT_EQ(synced, (std::vector<bool>{false, true, false}));
    EXPECT_FALSE(IsDiskFenced(file_storage));
}

// A drain whose unmount RPC fails keeps its latch: the heartbeat must
// not re-mount, and once the master answers again the heartbeat retries the
// unmount. The master is restarted and the segment re-mounted behind the
// store's back, standing in for a master that kept the registration after
// the store's RPC failed (e.g. timed out during the master's sweep).
TEST_F(FileStorageTest, DrainUnmountFailureKeepsLatchAndRetries) {
    auto master_root = fs::path(data_path) / "drain_retry_master";
    fs::create_directories(master_root);
    auto master_config = [&](int rpc, int metadata, int metrics) {
        auto builder = InProcMasterConfigBuilder()
                           .set_enable_offload(true)
                           .set_root_fs_dir(master_root.string());
        if (rpc > 0) {
            builder.set_rpc_port(rpc)
                .set_http_metadata_port(metadata)
                .set_http_metrics_port(metrics);
        }
        return builder.build();
    };
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(master_config(0, 0, 0)));
    const int rpc_port = master.rpc_port();
    const int metadata_port = master.http_metadata_port();
    const int metrics_port = master.http_metrics_port();

    const std::string local_rpc_addr =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = Client::Create(local_rpc_addr, master.metadata_url(), "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(client.value()->MountLocalDiskSegment(true).has_value());

    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kFilePerKey;
    config.storage_filepath = data_path + "/drain_retry";
    config.local_buffer_size = 4 * 1024 * 1024;
    fs::create_directories(config.storage_filepath);
    FileStorage file_storage(config, client.value(), local_rpc_addr);
    // A heartbeat that is not skipped runs a full pass, which needs an
    // initialized backend (Init() does this in production).
    ASSERT_TRUE(InitStorageBackend(file_storage).has_value());

    master.Stop();
    ASSERT_FALSE(file_storage.DrainLocalDiskSegment(0).has_value());
    EXPECT_TRUE(IsDraining(file_storage));
    (void)FileStorageHeartbeat(file_storage);  // master down: no-op
    EXPECT_TRUE(IsDraining(file_storage));

    ASSERT_TRUE(
        master.Start(master_config(rpc_port, metadata_port, metrics_port)));
    bool remounted = false;
    for (int i = 0; i < 100 && !remounted; ++i) {
        remounted = client.value()->MountLocalDiskSegment(true).has_value();
        if (!remounted) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    ASSERT_TRUE(remounted) << "client did not reach the restarted master";

    for (int i = 0; i < 2; ++i) {
        (void)FileStorageHeartbeat(file_storage);
    }
    EXPECT_TRUE(IsDraining(file_storage));
    std::vector<OffloadTaskItem> offload_items;
    auto heartbeat_rpc =
        client.value()->OffloadObjectHeartbeat(true, offload_items);
    ASSERT_FALSE(heartbeat_rpc.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, heartbeat_rpc.error());
}

TEST_F(FileStorageTest, BatchGetRanges_RegistersReleasableStaging) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = 128 * 1024 * 1024;
    config.total_keys_limit = 1000;
    FileStorage storage(config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(storage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());
    ASSERT_GT(sizes.front(), 2);

    const int64_t offset = 1;
    const int64_t length = sizes.front() - 2;
    auto result = storage.BatchGetRanges({keys.front()}, {offset}, {length},
                                         {sizes.front()});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->pointers.size(), 1u);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(result->pointers[0]), length),
        batch_data.at(keys.front()).substr(offset, length));
    EXPECT_EQ(ClientBufferBatchCount(storage), 1u);
    EXPECT_NE(ClientBufferBatch(storage, result->batch_id), nullptr);
    EXPECT_TRUE(ExpireClientBufferBatch(storage, result->batch_id));
    EXPECT_TRUE(storage.ReleaseBuffer(result->batch_id));
    EXPECT_EQ(ClientBufferBatchCount(storage), 0u);
}

TEST_F(FileStorageTest, BatchGetSingleflight_SerializesSameKey) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const std::string target_key = keys.front();
    const int64_t target_size = sizes.front();
    constexpr int kConcurrency = 8;

    std::atomic<int> success_count{0};
    std::atomic<int> started{0};
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&] {
            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet({target_key}, {target_size});
            if (result) {
                fileStorage.ReleaseBuffer(result->batch_id);
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kConcurrency)
        << "All concurrent BatchGet calls should succeed";
}

TEST_F(FileStorageTest, BatchGetSingleflight_DifferentKeysRunConcurrently) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_GE(keys.size(), 2u) << "Need at least 2 keys for this test";

    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (size_t i = 0; i < std::min(keys.size(), size_t(4)); ++i) {
        threads.emplace_back([&, i] {
            auto result = fileStorage.BatchGet({keys[i]}, {sizes[i]});
            if (result) {
                fileStorage.ReleaseBuffer(result->batch_id);
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(),
              static_cast<int>(std::min(keys.size(), size_t(4))))
        << "All different-key BatchGet calls should succeed";
}

TEST_F(FileStorageTest, BatchGetSingleflight_MultiKeyBatchSucceeds) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_GE(keys.size(), 2u);

    auto result = fileStorage.BatchGet(keys, sizes);
    ASSERT_TRUE(result) << "Multi-key BatchGet should succeed";
    fileStorage.ReleaseBuffer(result->batch_id);
}

TEST_F(FileStorageTest, BatchGetLocalSharesOwnerAndPreservesPointerOrder) {
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.use_uring = false;
    FileStorage storage(config, nullptr, "localhost:9003");
    auto backend = InstallDelayedReadBackend(storage, config);
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> data;
    ASSERT_TRUE(FileStorageBatchOffload(storage, keys, sizes, data));
    ASSERT_GE(keys.size(), 3u);
    keys.resize(3);
    sizes.resize(3);

    constexpr size_t kReaders = 8;
    std::barrier start(static_cast<std::ptrdiff_t>(kReaders));
    std::vector<std::optional<FileStorage::LocalBatchResult>> results(kReaders);
    std::vector<std::thread> readers;
    for (size_t i = 0; i < kReaders; ++i) {
        readers.emplace_back([&, i] {
            auto requested_keys = keys;
            auto requested_sizes = sizes;
            if (i % 2) {
                std::reverse(requested_keys.begin(), requested_keys.end());
                std::reverse(requested_sizes.begin(), requested_sizes.end());
            }
            start.arrive_and_wait();
            auto result =
                storage.BatchGetLocal(requested_keys, requested_sizes);
            if (result) results[i] = std::move(*result);
        });
    }
    for (auto& reader : readers) reader.join();

    EXPECT_EQ(backend->loads.load(), 1u);
    ASSERT_TRUE(results[0]);
    for (size_t reader = 0; reader < kReaders; ++reader) {
        ASSERT_TRUE(results[reader]);
        ASSERT_EQ(results[reader]->pointers.size(), keys.size());
        for (size_t index = 0; index < keys.size(); ++index) {
            const size_t key_index =
                reader % 2 ? keys.size() - index - 1 : index;
            const auto pointer = results[reader]->pointers[index];
            EXPECT_EQ(pointer, results[0]->pointers[key_index]);
            EXPECT_EQ(std::string(reinterpret_cast<const char*>(pointer),
                                  sizes[key_index]),
                      data.at(keys[key_index]));
        }
    }
    // Retaining consumers keeps their bytes alive, but does not cache reads.
    auto later = storage.BatchGetLocal(keys, sizes);
    ASSERT_TRUE(later);
    EXPECT_EQ(backend->loads.load(), 2u);
    EXPECT_NE(later->pointers.front(), results[0]->pointers.front());
}

TEST_F(FileStorageTest, LocalAndRpcReadsKeepSeparateStagingContracts) {
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = 8 * 1024 * 1024;
    config.use_uring = false;
    std::vector<char> arena(64 * 1024 + 4096);
    const auto arena_begin =
        (reinterpret_cast<uintptr_t>(arena.data()) + 4095) & ~4095ULL;
    FileStorage storage(config, nullptr, "localhost:9003");
    auto backend = InstallDelayedReadBackend(storage, config);
    SetPinnedRestoreArena(storage, reinterpret_cast<void*>(arena_begin),
                          64 * 1024);
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> data;
    ASSERT_TRUE(FileStorageBatchOffload(storage, keys, sizes, data));
    ASSERT_GE(keys.size(), 3u);
    keys.resize(3);
    sizes.resize(3);
    std::optional<FileStorage::LocalBatchResult> local;
    std::optional<FileStorage::BatchGetResult> rpc;
    std::barrier start(2);
    std::thread local_reader([&] {
        start.arrive_and_wait();
        auto result = storage.BatchGetLocal(keys, sizes);
        if (result) local = std::move(*result);
    });
    std::thread rpc_reader([&] {
        start.arrive_and_wait();
        auto result = storage.BatchGet(keys, sizes);
        if (result) rpc = std::move(*result);
    });
    local_reader.join();
    rpc_reader.join();
    ASSERT_TRUE(local);
    ASSERT_TRUE(rpc);
    EXPECT_EQ(backend->loads.load(), 2u);
    ASSERT_EQ(local->pointers.size(), keys.size());
    ASSERT_EQ(rpc->pointers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_GE(local->pointers[i], arena_begin);
        EXPECT_LT(local->pointers[i], arena_begin + 64 * 1024);
        EXPECT_TRUE(rpc->pointers[i] < arena_begin ||
                    rpc->pointers[i] >= arena_begin + 64 * 1024);
        for (const auto pointer : {local->pointers[i], rpc->pointers[i]}) {
            EXPECT_EQ(
                std::string(reinterpret_cast<const char*>(pointer), sizes[i]),
                data.at(keys[i]));
        }
    }
    EXPECT_TRUE(storage.ReleaseBuffer(rpc->batch_id));
}

TEST_F(FileStorageTest, BatchGetSingleflight_SameMultiKeyBatchShared) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_GE(keys.size(), 3u);

    keys.resize(3);
    sizes.resize(3);
    constexpr int kConcurrency = 16;
    std::atomic<int> started{0};
    std::atomic<int> success_count{0};
    std::vector<uint64_t> first_key_pointers(kConcurrency, 0);
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    for (int reader = 0; reader < kConcurrency; ++reader) {
        threads.emplace_back([&, reader] {
            std::vector<std::string> reader_keys;
            std::vector<int64_t> reader_sizes;
            for (size_t offset = 0; offset < keys.size(); ++offset) {
                const size_t index = (reader + offset) % keys.size();
                reader_keys.push_back(keys[index]);
                reader_sizes.push_back(sizes[index]);
            }

            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet(reader_keys, reader_sizes);
            if (!result) return;
            ASSERT_EQ(result->pointers.size(), reader_keys.size());
            for (size_t i = 0; i < reader_keys.size(); ++i) {
                const auto* data =
                    reinterpret_cast<const char*>(result->pointers[i]);
                EXPECT_EQ(std::string(data, reader_sizes[i]),
                          batch_data.at(reader_keys[i]));
                if (reader_keys[i] == keys.front()) {
                    first_key_pointers[reader] = result->pointers[i];
                }
            }
            fileStorage.ReleaseBuffer(result->batch_id);
            success_count.fetch_add(1);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(success_count.load(), kConcurrency);
    size_t largest_shared_group = 0;
    for (int i = 0; i < kConcurrency; ++i) {
        size_t shared_group = 0;
        for (int j = 0; j < kConcurrency; ++j) {
            if (first_key_pointers[j] == first_key_pointers[i]) {
                ++shared_group;
            }
        }
        largest_shared_group = std::max(largest_shared_group, shared_group);
    }
    EXPECT_GT(largest_shared_group, 1u)
        << "At least 2 reordered readers should share one staging batch";
}

TEST_F(FileStorageTest, BatchGetSingleflight_StagingSharingVerification) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const std::string target_key = keys.front();
    const int64_t target_size = sizes.front();
    const std::string& expected_data = batch_data.at(target_key);
    constexpr int kConcurrency = 16;

    std::atomic<int> success_count{0};
    std::atomic<int> started{0};
    std::vector<uint64_t> batch_ids(kConcurrency, 0);
    std::vector<uint64_t> staging_ptrs(kConcurrency, 0);
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&, i] {
            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet({target_key}, {target_size});
            if (!result) return;
            ASSERT_EQ(result->pointers.size(), 1u);
            batch_ids[i] = result->batch_id;
            staging_ptrs[i] = result->pointers[0];

            // Verify data correctness: read from staging pointer. The reader
            // still holds its own batch id here, so the staging buffer cannot
            // be recycled underneath it even if the other readers are done.
            auto* data_ptr = reinterpret_cast<const char*>(result->pointers[0]);
            std::string actual(data_ptr, target_size);
            EXPECT_EQ(actual, expected_data)
                << "Data mismatch for batch_id=" << result->batch_id;

            fileStorage.ReleaseBuffer(result->batch_id);
            success_count.fetch_add(1);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kConcurrency)
        << "All concurrent BatchGet calls should succeed";

    // Staging sharing now shows up as several batch ids backed by one staging
    // buffer: every reader releases its own id, and the memory returns to the
    // pool when the last id goes away.
    std::unordered_set<uint64_t> distinct_ids(batch_ids.begin(),
                                              batch_ids.end());
    EXPECT_EQ(distinct_ids.size(), static_cast<size_t>(kConcurrency))
        << "Every reader should get its own batch id";

    size_t largest_group = 0;
    for (int i = 0; i < kConcurrency; ++i) {
        size_t group = 0;
        for (int j = 0; j < kConcurrency; ++j) {
            if (staging_ptrs[j] == staging_ptrs[i]) group++;
        }
        largest_group = std::max(largest_group, group);
    }
    LOG(INFO) << "Staging sharing results: " << kConcurrency << " readers used "
              << distinct_ids.size() << " batch ids, " << largest_group
              << " of them shared one staging buffer";
    EXPECT_GT(largest_group, 1u)
        << "At least 2 readers should share the same staging buffer";
}

TEST_F(FileStorageTest, BatchGetSingleflight_LastReleaseFreesSharedStaging) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    constexpr int64_t kBufferSize = 1024 * 1024;
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = kBufferSize;
    config.total_keys_limit = 1000;
    config.use_uring = false;
    FileStorage storage(config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(storage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const auto& key = keys.front();
    const auto size = sizes.front();
    auto first = storage.BatchGet({key}, {size});
    ASSERT_TRUE(first);
    ASSERT_EQ(first->pointers.size(), 1u);
    auto staging = ClientBufferBatch(storage, first->batch_id);
    ASSERT_NE(staging, nullptr);

    // Register a second lease on the same staging buffer, then let the
    // registered batch IDs own the buffer for the rest of the test.
    auto last = FileStorageShareStagingBuffer(storage, staging);
    staging.reset();
    ASSERT_NE(first->batch_id, last.batch_id);
    ASSERT_EQ(first->pointers, last.pointers);
    ASSERT_EQ(ClientBufferBatchCount(storage), 2u);

    ASSERT_TRUE(storage.ReleaseBuffer(first->batch_id));
    EXPECT_EQ(ClientBufferBatchCount(storage), 1u);

    const std::vector<std::string> probe_keys = {key};
    const std::vector<int64_t> probe_sizes = {kBufferSize};
    auto probe = FileStorageAllocateBatch(storage, probe_keys, probe_sizes);
    ASSERT_FALSE(probe);
    EXPECT_EQ(probe.error(), ErrorCode::BUFFER_OVERFLOW);
    const auto* data = reinterpret_cast<const char*>(last.pointers.front());
    EXPECT_EQ(std::string(data, size), batch_data.at(key));

    ASSERT_TRUE(storage.ReleaseBuffer(last.batch_id));
    EXPECT_FALSE(storage.ReleaseBuffer(last.batch_id));
    EXPECT_EQ(ClientBufferBatchCount(storage), 0u);
    EXPECT_TRUE(FileStorageAllocateBatch(storage, probe_keys, probe_sizes))
        << "The last release should return staging memory to the pool";
}

TEST_F(FileStorageTest, BatchGetSingleflight_AliasLeasesAreIndependent) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    constexpr int64_t kBufferSize = 1024 * 1024;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = kBufferSize;
    file_storage_config.total_keys_limit = 1000;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    auto leader = fileStorage.BatchGet({keys.front()}, {sizes.front()});
    ASSERT_TRUE(leader);
    auto staging = ClientBufferBatch(fileStorage, leader->batch_id);
    ASSERT_NE(staging, nullptr);

    auto alias = FileStorageShareStagingBuffer(fileStorage, staging);
    ASSERT_NE(ClientBufferShardIndex(leader->batch_id),
              ClientBufferShardIndex(alias.batch_id));
    ASSERT_EQ(ClientBufferBatchCount(fileStorage), 2u);
    staging.reset();

    // Expiring the leader must not expire an alias in another shard. The
    // alias started its own lease window when it joined the shared staging
    // buffer, and its shard mutex cannot protect a deadline stored on the
    // shared AllocatedBatch.
    ASSERT_TRUE(ExpireClientBufferBatch(fileStorage, leader->batch_id));
    const std::vector<std::string> probe_key = {keys.front()};
    const std::vector<int64_t> probe_size = {kBufferSize};
    auto probe = FileStorageAllocateBatch(fileStorage, probe_key, probe_size);
    ASSERT_FALSE(probe);
    EXPECT_EQ(probe.error(), ErrorCode::BUFFER_OVERFLOW);

    EXPECT_FALSE(fileStorage.ReleaseBuffer(leader->batch_id))
        << "The expired leader entry should have been collected";
    EXPECT_TRUE(fileStorage.ReleaseBuffer(alias.batch_id))
        << "The alias has an independent, unexpired lease";
    EXPECT_EQ(ClientBufferBatchCount(fileStorage), 0u);
}

TEST_F(FileStorageTest,
       BatchGetSingleflight_AbandonedReaderDoesNotLeakStaging) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    constexpr int64_t kBufferSize = 1024 * 1024;
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_filepath = data_path;
    config.local_buffer_size = kBufferSize;
    config.total_keys_limit = 1000;
    config.use_uring = false;
    FileStorage storage(config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(storage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const auto& key = keys.front();
    auto first = storage.BatchGet({key}, {sizes.front()});
    ASSERT_TRUE(first);
    auto staging = ClientBufferBatch(storage, first->batch_id);
    ASSERT_NE(staging, nullptr);
    auto second = FileStorageShareStagingBuffer(storage, staging);
    staging.reset();
    ASSERT_NE(first->batch_id, second.batch_id);
    ASSERT_EQ(first->pointers, second.pointers);
    ASSERT_EQ(ClientBufferBatchCount(storage), 2u);

    // A whole-pool allocation drives eager GC while the abandoned leases
    // still own the shared staging buffer.
    const std::vector<std::string> probe_keys = {key};
    const std::vector<int64_t> probe_sizes = {kBufferSize};
    auto probe = FileStorageAllocateBatch(storage, probe_keys, probe_sizes);
    ASSERT_FALSE(probe);
    EXPECT_EQ(probe.error(), ErrorCode::BUFFER_OVERFLOW);
    EXPECT_EQ(ClientBufferBatchCount(storage), 2u);

    ASSERT_TRUE(ExpireClientBufferBatch(storage, first->batch_id));
    ASSERT_TRUE(ExpireClientBufferBatch(storage, second.batch_id));
    EXPECT_TRUE(FileStorageAllocateBatch(storage, probe_keys, probe_sizes))
        << "Expired leases should return staging memory to the pool";
    EXPECT_EQ(ClientBufferBatchCount(storage), 0u);
}

class FileStorageReclaimTest
    : public FileStorageTest,
      public ::testing::WithParamInterface<ReclaimCase> {};

TEST_P(FileStorageReclaimTest, Scenario) { RunReclaimScenario(GetParam()); }

std::string ReclaimCaseName(const ::testing::TestParamInfo<ReclaimCase>& info) {
    constexpr const char* kNames[] = {
        "DeletesBucketOfRemovedObject", "OffloadsRePutKeyAfresh",
        "KeepsLiveCopyOnRetriedTask",   "WaitsForRescanAfterSegmentLoss",
        "ReopensAfterRemountRescan",    "PassFailsWhenQueryFails",
        "DisabledOnNonEmptyDirectory",  "RunsWhenOffloadFails"};
    return kNames[static_cast<int>(info.param)];
}

INSTANTIATE_TEST_SUITE_P(
    Reclaim, FileStorageReclaimTest,
    ::testing::Values(ReclaimCase::kRemoved, ReclaimCase::kRePut,
                      ReclaimCase::kRetriedTask, ReclaimCase::kSegmentLost,
                      ReclaimCase::kSegmentRemounted, ReclaimCase::kQueryFails,
                      ReclaimCase::kExistingData, ReclaimCase::kOffloadFails),
    ReclaimCaseName);

TEST_F(FileStorageTest, OffloadCarryover5Then27) {
    RunCarryoverScenario({5, 27});
}
TEST_F(FileStorageTest, OffloadCarryover1Then31) {
    RunCarryoverScenario({1, 31});
}
TEST_F(FileStorageTest, OffloadCarryover31Then1) {
    RunCarryoverScenario({31, 1});
}
TEST_F(FileStorageTest, OffloadCarryoverContinuousBatches) {
    RunCarryoverScenario({5, 27, 7, 25, 1, 31});
}
TEST_F(FileStorageTest, OffloadCarryoverNoCarryoverControl) {
    RunCarryoverScenario({32});
}
TEST_F(FileStorageTest, OffloadCarryoverEmptyHeartbeatFlushesPartial) {
    RunCarryoverScenario({5});
}
TEST_F(FileStorageTest, OffloadCarryoverWriteFailureReleasesAllSources) {
    RunCarryoverScenario({5, 27}, "write_fail");
}
TEST_F(FileStorageTest, OffloadCarryoverRemoveAllCancelsPending) {
    RunCarryoverScenario({5}, "remove_all");
}
TEST_F(FileStorageTest, OffloadCarryoverDrainCancelsPending) {
    RunCarryoverScenario({5}, "drain");
}
TEST_F(FileStorageTest, OffloadCarryoverDestructorCancelsPending) {
    RunCarryoverScenario({5}, "destruct");
}
TEST_F(FileStorageTest, OffloadCarryoverDeletedKeysAreNotResurrected) {
    RunCarryoverScenario({5}, "delete_keys");
}
TEST_F(FileStorageTest, OffloadCarryoverDuplicatePendingTasks) {
    RunCarryoverScenario({5, 27}, "duplicate_pending");
}
TEST_F(FileStorageTest, OffloadCarryoverCompletedTaskReplay) {
    RunCarryoverScenario({32}, "old_task_replay");
}
TEST_F(FileStorageTest, OffloadCarryoverSameUserKeysAcrossTenants) {
    RunCarryoverScenario({16, 16}, "", true);
}
TEST_F(FileStorageTest, OffloadCarryoverDrainRacesInFlightWrite) {
    RunCarryoverScenario({5, 27}, "drain_race");
}

TEST_F(FileStorageTest,
       PartialHaNotificationRetainsBucketUntilEmptyHeartbeatRetry) {
    RunUncertainNotificationScenario();
}

TEST_F(FileStorageTest,
       CompletedHaNotificationWithUnobservedReplyRetriesIdempotently) {
    RunUncertainNotificationScenario("lost_reply");
}

TEST_F(FileStorageTest,
       DrainCancelsUncertainHaNotificationWithoutResurrection) {
    RunUncertainNotificationScenario("drain");
}

TEST_F(FileStorageTest,
       RemoveAllCancelsUncertainHaNotificationWithoutResurrection) {
    RunUncertainNotificationScenario("remove_all");
}

TEST_F(FileStorageTest,
       DeliveredOffloadKeepsBorrowedSourcePinnedAfterMasterTimeout) {
    RunGenerationCarryoverScenario(false, true);
}

TEST_F(FileStorageTest,
       CarryoverAndReplacementOfSameKeyBorrowOnlyMatchingGeneration) {
    RunGenerationCarryoverScenario(false);
}

TEST_F(FileStorageTest,
       TenantCarryoverAndReplacementKeepPhysicalGenerationIsolated) {
    RunGenerationCarryoverScenario(true);
}

TEST_F(FileStorageTest,
       ReissuedTaskAdoptsCommittedGenerationWithoutRewritingPayload) {
    RunUncertainNotificationScenario("generation_reissue");
}

TEST_F(FileStorageTest, RetriedHaNotificationCannotResurrectOrdinaryDelete) {
    RunUncertainNotificationScenario("generation_delete");
}

TEST_F(FileStorageTest, RetriedHaNotificationCannotResurrectForcedDelete) {
    RunUncertainNotificationScenario("generation_delete_force");
}

TEST_F(FileStorageTest, OldHaNotificationAndScanCannotConsumeReplacementPin) {
    RunUncertainNotificationScenario("generation_rewrite");
}

TEST_F(FileStorageTest,
       OldHaNotificationAfterForcedDeleteCannotPublishOldBytes) {
    RunUncertainNotificationScenario("generation_rewrite_force");
}

}  // namespace mooncake

namespace mooncake {}  // namespace mooncake

#if defined(__linux__) && defined(USE_URING)
namespace mooncake {}  // namespace mooncake
#endif
