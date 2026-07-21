#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <unistd.h>

#include "hot_standby_service.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/mock_metadata_store.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "types.h"

namespace mooncake::test {

class FakeBatchHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = kvs_.find(std::string(key));
        if (it == kvs_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        kvs_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        kvs.clear();
        for (auto it = kvs_.lower_bound(std::string(begin_key));
             it != kvs_.end() && it->first < end_key; ++it) {
            kvs.push_back({.key = it->first, .value = it->second});
            if (limit != 0 && kvs.size() >= limit) {
                break;
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        for (const auto& compare : txn.compares) {
            auto it = kvs_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != kvs_.end()) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == kvs_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            kvs_[put.key] = put.value;
        }
        return ErrorCode::OK;
    }

   private:
    std::map<std::string, std::string> kvs_;
};

class BlockingBatchHaKvBackend : public FakeBatchHaKvBackend {
   public:
    void BlockTxn() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = true;
    }

    void AllowTxn() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            blocked_ = false;
        }
        cv_.notify_all();
    }

    ErrorCode Txn(const KvTxn& txn) override {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !blocked_; });
        }
        return FakeBatchHaKvBackend::Txn(txn);
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool blocked_{false};
};

class FailingBatchHaKvBackend : public FakeBatchHaKvBackend {
   public:
    void SetTxnError(ErrorCode error) {
        std::lock_guard<std::mutex> lock(mutex_);
        txn_error_ = error;
        txn_calls_ = 0;
    }

    bool WaitForTxnCalls(size_t count, std::chrono::milliseconds timeout =
                                           std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return txn_calls_ >= count; });
    }

    ErrorCode Txn(const KvTxn& txn) override {
        ErrorCode txn_error;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++txn_calls_;
            txn_error = txn_error_;
        }
        cv_.notify_all();
        if (txn_error != ErrorCode::OK) {
            return txn_error;
        }
        return FakeBatchHaKvBackend::Txn(txn);
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    ErrorCode txn_error_{ErrorCode::OK};
    size_t txn_calls_{0};
};

class MasterServiceHATest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("MasterServiceHATest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;
    static constexpr uint64_t kStrictTenantQuotaBytes = 4 * 1024 * 1024;
    static constexpr const char* kDefaultTenant = "default";

    void SetUp() override {
        ::setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", LegacyOpLogRootDir().c_str(),
                 1);
    }

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        policy_files_.clear();
        std::error_code ec;
        std::filesystem::remove_all(LegacyOpLogRootDir(), ec);
        ::unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH");
    }

    std::string LegacyOpLogRootDir() const {
        return (std::filesystem::temp_directory_path() /
                ("mooncake_master_service_ha_oplog_" +
                 std::to_string(::getpid())))
            .string();
    }

    std::string WriteTenantPolicyFile(
        const std::map<std::string, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = tenant_quotas;
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_master_service_ha_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    MasterServiceConfig MakeStrictHAConfig(
        const std::vector<std::string>& tenants = {kDefaultTenant,
                                                   "tenant_a"}) {
        std::map<std::string, uint64_t> tenant_quotas;
        for (const auto& tenant : tenants) {
            tenant_quotas.emplace(tenant, kStrictTenantQuotaBytes);
        }
        return MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id("test_cluster")
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile(tenant_quotas))
            .build();
    }

    static bool HasOpLogWriter(const MasterService& service) {
        return service.ordered_oplog_writer_ != nullptr;
    }

    static bool IsOpLogEnabled(const MasterService& service) {
        return service.enable_oplog_;
    }

    static bool HasBatchOpLogStorage(const MasterService& service) {
        return service.batch_oplog_storage_ != nullptr;
    }

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

#ifdef USE_NOF
    NoFSegment MakeNoFSegment(
        std::string name = "test_nof_segment",
        std::string endpoint = "test_nof_segment_endpoint",
        size_t base = kDefaultSegmentBase + kDefaultSegmentSize,
        size_t size = kDefaultSegmentSize) const {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = std::move(endpoint);
        return segment;
    }
#endif

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase,
        size_t size = kDefaultSegmentSize) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    std::string PutObject(MasterService& service, const UUID& client_id,
                          const std::string& key,
                          size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start = service.PutStart(client_id, key, kDefaultTenant,
                                          slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, kDefaultTenant, ReplicaType::MEMORY)
                .has_value());
        return key;
    }

    std::string PutObjectOnSegment(MasterService& service,
                                   const UUID& client_id,
                                   const std::string& key,
                                   const std::string& segment_name,
                                   size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};
        auto put_start = service.PutStart(client_id, key, kDefaultTenant,
                                          slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, kDefaultTenant, ReplicaType::MEMORY)
                .has_value());
        return key;
    }

    void ReadBatchEventually(OpLogBatchStorage& storage, uint64_t batch_id,
                             OpLogBatchRecord& batch) const {
        ErrorCode read_err = ErrorCode::ETCD_KEY_NOT_EXIST;
        for (int i = 0; i < 50; ++i) {
            read_err = storage.ReadBatch(batch_id, batch);
            if (read_err == ErrorCode::OK) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_EQ(ErrorCode::OK, read_err);
    }

    void ReadRemoveBatchEventually(OpLogBatchStorage& storage,
                                   uint64_t first_batch_id,
                                   const std::string& key,
                                   OpLogBatchRecord& batch) const {
        bool saw_remove = false;
        for (int attempt = 0; attempt < 50 && !saw_remove; ++attempt) {
            for (uint64_t batch_id = first_batch_id;
                 batch_id < first_batch_id + 8 && !saw_remove; ++batch_id) {
                if (storage.ReadBatch(batch_id, batch) != ErrorCode::OK) {
                    continue;
                }
                for (const auto& entry : batch.entries) {
                    if (entry.op_type == OpType::REMOVE &&
                        entry.object_key == key) {
                        saw_remove = true;
                        break;
                    }
                }
            }
            if (!saw_remove) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        ASSERT_TRUE(saw_remove);
    }

    std::string PutObjectWithTenant(MasterService& service,
                                    const UUID& client_id,
                                    const std::string& key,
                                    const std::string& tenant_id,
                                    size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, tenant_id, slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY)
                .has_value());
        return key;
    }

    std::string PutObjectOnSegmentWithTenant(MasterService& service,
                                             const UUID& client_id,
                                             const std::string& key,
                                             const std::string& segment_name,
                                             const std::string& tenant_id,
                                             size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};
        auto put_start =
            service.PutStart(client_id, key, tenant_id, slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY)
                .has_value());
        return key;
    }

    Replica::Descriptor MakeStandbyMemoryReplica(const std::string& endpoint,
                                                 size_t size = 1024) const {
        Replica::Descriptor replica;
        replica.id = 1;
        replica.status = ReplicaStatus::COMPLETE;

        MemoryDescriptor mem_desc;
        mem_desc.buffer_descriptor.transport_endpoint_ = endpoint;
        mem_desc.buffer_descriptor.buffer_address_ = 0;
        mem_desc.buffer_descriptor.size_ = size;
        replica.descriptor_variant = std::move(mem_desc);
        return replica;
    }

    StandbyObjectEntry MakeStandbyObject(const std::string& key,
                                         const std::string& endpoint,
                                         size_t size = 1024) const {
        StandbyObjectMetadata metadata;
        metadata.client_id = generate_uuid();
        metadata.size = size;
        metadata.last_sequence_id = 1;
        metadata.replicas.push_back(MakeStandbyMemoryReplica(endpoint, size));
        return StandbyObjectEntry{"default", key, std::move(metadata)};
    }

    StandbySegmentInfo MakeStandbyMemorySegment(
        const std::string& endpoint,
        size_t capacity = kDefaultSegmentSize) const {
        StandbySegmentInfo segment;
        segment.segment_name = endpoint;
        segment.transport_endpoint = endpoint;
        segment.capacity = capacity;
        segment.is_memory_segment = true;
        return segment;
    }

    // Friend access to MasterService::metadata_shards_ and
    // getMetadataShardIndex, which are otherwise private.
    // MasterServiceHATest is friended; TEST_F-generated subclasses are not,
    // hence this static funnel. Seeds an in-flight PromotionTask for a
    // given (tenant, key) so NotifyPromotionSuccess can proceed without
    // going through the on-hit admission gate (which is currently
    // restricted to the "default" tenant). Used only by the non-default
    // tenant promotion tests.
    static void SeedPromotionTaskForTesting(MasterService* service,
                                            const std::string& tenant,
                                            const std::string& key,
                                            const UUID& holder_id,
                                            ReplicaID alloc_id,
                                            uint64_t object_size) {
        const size_t shard_idx = service->getMetadataShardIndex(tenant, key);
        auto shard_access =
            MasterService::MetadataShardAccessorRW(service, shard_idx);
        auto& tenant_state = shard_access->tenants[tenant];
        tenant_state.promotion_tasks.emplace(
            key, MasterService::PromotionTask{
                     .source_id = 0,
                     .alloc_id = alloc_id,
                     .object_size = object_size,
                     .start_time = std::chrono::system_clock::now(),
                     .holder_id = holder_id});
    }

    static bool SnapshotManagerCreatedForTesting(const MasterService& service) {
        return service.snapshot_manager_ != nullptr;
    }

    static tl::expected<uint64_t, ErrorCode> AppendVisibleForTesting(
        MasterService& service, OpType type, const std::string& tenant_id,
        const std::string& key, const std::string& payload) {
        return service.AppendOpLogVisibleBeforeDurable(type, tenant_id, key,
                                                       payload);
    }

    static tl::expected<OpLogEntry, ErrorCode> AppendFinalizeForTesting(
        MasterService& service, OpType type, const std::string& tenant_id,
        const std::string& key, const std::string& payload,
        MasterService::DurableFinalizeCallback callback) {
        return service.AppendOpLogWithDurableFinalize(
            type, tenant_id, key, payload, std::move(callback));
    }

    static tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
    ReserveBatchSlotForTesting(MasterService& service) {
        if (!service.ordered_oplog_writer_) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return service.ordered_oplog_writer_->Reserve();
    }

    static void ClearInvalidHandlesForTesting(
        MasterService& service,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) {
        service.ClearInvalidHandles(alive_clients);
    }

    static size_t ReplicaCountForTesting(MasterService& service,
                                         const std::string& tenant_id,
                                         const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        return accessor.Exists() ? accessor.Get().CountReplicas() : 0;
    }

    static std::vector<Replica::Descriptor> ReplicaDescriptorsForTesting(
        MasterService& service, const std::string& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        if (!accessor.Exists()) {
            return {};
        }
        std::vector<Replica::Descriptor> descriptors;
        for (const auto& replica : accessor.Get().GetAllReplicas()) {
            descriptors.push_back(replica.get_descriptor());
        }
        return descriptors;
    }

    static bool HasInvalidMemoryHandleForTesting(MasterService& service,
                                                 const std::string& tenant_id,
                                                 const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        if (!accessor.Exists()) {
            return true;
        }
        for (const auto& replica : accessor.Get().GetAllReplicas()) {
            if (replica.has_invalid_mem_handle()) {
                return true;
            }
        }
        return false;
    }

    static size_t SegmentAllocatedSizeForTesting(MasterService& service,
                                                 const std::string& name) {
        auto access = service.segment_manager_.getAllocatorAccess();
        const auto* allocators =
            access.getAllocatorManager().getAllocators(name);
        EXPECT_NE(allocators, nullptr);
        EXPECT_EQ(allocators == nullptr ? 0 : allocators->size(), 1);
        return allocators == nullptr || allocators->empty()
                   ? 0
                   : allocators->front()->size();
    }

    static void EraseObjectForTesting(MasterService& service,
                                      const std::string& tenant_id,
                                      const std::string& key) {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Erase();
    }

    static void PrepareUnmountSegmentForTesting(MasterService& service,
                                                const UUID& segment_id) {
        auto segment_access = service.segment_manager_.getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(ErrorCode::OK, segment_access.PrepareUnmountSegment(
                                     segment_id, metrics_dec_capacity));
    }

    static std::vector<ReplicaID> MarkCompletedReplicasRemovedForTesting(
        MasterService& service, const std::string& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        if (!accessor.Exists()) {
            return {};
        }
        std::vector<ReplicaID> ids;
        accessor.Get().VisitReplicas(&Replica::fn_is_completed,
                                     [&ids](Replica& replica) {
                                         ids.push_back(replica.id());
                                         replica.mark_removed();
                                     });
        return ids;
    }

    static void FinalizeRemovedReplicasForTesting(
        MasterService& service, const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids) {
        service.FinalizeRemovedReplicasAfterDurable(
            durable_entry, replica_ids, MasterService::QuotaEraseMode::kFull);
    }

    static void SetLocalDiskUsedBytesForTesting(MasterService& service,
                                                const UUID& client_id,
                                                int64_t used_bytes) {
        auto access = service.segment_manager_.getLocalDiskSegmentAccess();
        access.getClientLocalDiskSegment().at(client_id)->ssd_used_bytes.store(
            used_bytes, std::memory_order_relaxed);
    }

    static int64_t GetLocalDiskUsedBytesForTesting(
        MasterService& service, const std::string& segment_name) {
        auto access = service.segment_manager_.getLocalDiskSegmentAccess();
        return access.getSsdUsedBytes(segment_name);
    }

    std::vector<std::string> policy_files_;
    int next_policy_file_{0};
};

class MasterServiceBatchRecordE2ETest : public MasterServiceHATest {};

TEST_F(MasterServiceHATest, RestoreFromStandbyPreservesMemoryBufferDescriptor) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string endpoint = "standby_restore_segment";
    const size_t size = 4096;
    const uintptr_t address = 0x12345000;
    auto object = MakeStandbyObject("standby_restore_key", endpoint, size);
    auto& descriptor = object.metadata.replicas.front()
                           .get_memory_descriptor()
                           .buffer_descriptor;
    descriptor.buffer_address_ = address;
    descriptor.protocol_ = "tcp";

    service.RestoreFromStandbySnapshot({object}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});

    auto replicas = ReplicaDescriptorsForTesting(service, kDefaultTenant,
                                                 "standby_restore_key");
    ASSERT_EQ(replicas.size(), 1);
    ASSERT_TRUE(replicas.front().is_memory_replica());
    const auto& restored =
        replicas.front().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(restored.size_, size);
    EXPECT_EQ(restored.buffer_address_, address);
    EXPECT_EQ(restored.protocol_, "tcp");
    EXPECT_EQ(restored.transport_endpoint_, endpoint);
}

TEST_F(MasterServiceHATest, RemountMakesRestoredMemoryReplicaReady) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string endpoint = "standby_remount_segment";
    const auto metric_before =
        MasterMetricManager::instance().get_allocated_mem_size();
    const std::string first_key = "standby_remount_first_key";
    const std::string second_key = "standby_remount_second_key";
    auto first_object = MakeStandbyObject(first_key, endpoint);
    first_object.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase;
    auto second_object = MakeStandbyObject(second_key, endpoint);
    second_object.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase + 4096;
    service.RestoreFromStandbySnapshot({first_object, second_object}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              2048);

    auto before = service.GetReplicaList(first_key, kDefaultTenant);
    ASSERT_FALSE(before.has_value());
    EXPECT_EQ(before.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto batch_before =
        service.BatchGetReplicaList({first_key, second_key}, kDefaultTenant);
    ASSERT_EQ(batch_before.size(), 2);
    for (const auto& result : batch_before) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::REPLICA_IS_NOT_READY);
    }

    Segment segment = MakeSegment(endpoint);
    ASSERT_TRUE(service.ReMountSegment({segment}, generate_uuid()).has_value());

    auto after = service.GetReplicaList(first_key, kDefaultTenant);
    ASSERT_TRUE(after.has_value()) << toString(after.error());
    ASSERT_EQ(after->replicas.size(), 1);
    EXPECT_TRUE(after->replicas.front().is_memory_replica());
    EXPECT_FALSE(
        HasInvalidMemoryHandleForTesting(service, kDefaultTenant, first_key));
    EXPECT_FALSE(
        HasInvalidMemoryHandleForTesting(service, kDefaultTenant, second_key));
    auto batch_after =
        service.BatchGetReplicaList({first_key, second_key}, kDefaultTenant);
    ASSERT_EQ(batch_after.size(), 2);
    ASSERT_TRUE(batch_after[0].has_value());
    ASSERT_TRUE(batch_after[1].has_value());
    EXPECT_EQ(batch_after[0]
                  ->replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              kDefaultSegmentBase);
    EXPECT_EQ(batch_after[1]
                  ->replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              kDefaultSegmentBase + 4096);
    EXPECT_EQ(SegmentAllocatedSizeForTesting(service, endpoint), 5120);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              5120);

    const std::string new_key = "post_remount_allocation";
    PutObjectOnSegment(service, generate_uuid(), new_key, endpoint);
    auto new_replicas =
        ReplicaDescriptorsForTesting(service, kDefaultTenant, new_key);
    ASSERT_EQ(new_replicas.size(), 1);
    EXPECT_GE(new_replicas.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              kDefaultSegmentBase + 1024);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              6144);

    EraseObjectForTesting(service, kDefaultTenant, first_key);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              5120);
    const std::string replacement_key = "post_remount_replacement";
    PutObjectOnSegment(service, generate_uuid(), replacement_key, endpoint);
    auto replacement =
        ReplicaDescriptorsForTesting(service, kDefaultTenant, replacement_key);
    ASSERT_EQ(replacement.size(), 1);
    EXPECT_EQ(replacement.front()
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              kDefaultSegmentBase);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              6144);
}

TEST_F(MasterServiceHATest, RemountRestoresCachelibMemoryReplica) {
    MasterService service(
        MasterServiceConfig::builder()
            .set_enable_ha(false)
            .set_memory_allocator(BufferAllocatorType::CACHELIB)
            .build());

    const std::string endpoint = "standby_cachelib_remount_segment";
    const auto metric_before =
        MasterMetricManager::instance().get_allocated_mem_size();
    const std::string key = "standby_cachelib_remount_key";
    auto object = MakeStandbyObject(key, endpoint, 64);
    object.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase;
    service.RestoreFromStandbySnapshot({object}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              64);

    auto single_before = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_FALSE(single_before.has_value());
    EXPECT_EQ(single_before.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto batch_before = service.BatchGetReplicaList({key}, kDefaultTenant);
    ASSERT_EQ(batch_before.size(), 1);
    ASSERT_FALSE(batch_before[0].has_value());
    EXPECT_EQ(batch_before[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
    Segment segment = MakeSegment(endpoint);
    ASSERT_TRUE(service.ReMountSegment({segment}, generate_uuid()).has_value());
    ASSERT_TRUE(service.GetReplicaList(key, kDefaultTenant).has_value());
    auto batch_after = service.BatchGetReplicaList({key}, kDefaultTenant);
    ASSERT_EQ(batch_after.size(), 1);
    ASSERT_TRUE(batch_after[0].has_value());
    EXPECT_FALSE(
        HasInvalidMemoryHandleForTesting(service, kDefaultTenant, key));
    EXPECT_EQ(SegmentAllocatedSizeForTesting(service, endpoint), 64);
    EXPECT_EQ(MasterMetricManager::instance().get_allocated_mem_size() -
                  metric_before,
              64);

    PutObjectOnSegment(service, generate_uuid(), "cachelib_after_remount",
                       endpoint, 64);
    const auto old_descriptor =
        ReplicaDescriptorsForTesting(service, kDefaultTenant, key)[0]
            .get_memory_descriptor()
            .buffer_descriptor;
    const auto new_descriptor =
        ReplicaDescriptorsForTesting(service, kDefaultTenant,
                                     "cachelib_after_remount")[0]
            .get_memory_descriptor()
            .buffer_descriptor;
    EXPECT_NE(new_descriptor.buffer_address_, old_descriptor.buffer_address_);
}

TEST_F(MasterServiceHATest, FailedRemountKeepsReplicaInvalidAndCanBeRetried) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string endpoint = "standby_retry_remount_segment";
    auto first = MakeStandbyObject("standby_retry_first", endpoint);
    auto conflicting = MakeStandbyObject("standby_retry_conflict", endpoint);
    first.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase;
    conflicting.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase;
    service.RestoreFromStandbySnapshot({first, conflicting}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});

    Segment segment = MakeSegment(endpoint);
    auto failed = service.ReMountSegment({segment}, generate_uuid());
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::INVALID_PARAMS);
    auto get_after_failure =
        service.GetReplicaList("standby_retry_first", kDefaultTenant);
    ASSERT_FALSE(get_after_failure.has_value());
    EXPECT_EQ(get_after_failure.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto batch_after_failure =
        service.BatchGetReplicaList({"standby_retry_first"}, kDefaultTenant);
    ASSERT_EQ(batch_after_failure.size(), 1);
    ASSERT_FALSE(batch_after_failure[0].has_value());
    EXPECT_EQ(batch_after_failure[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {endpoint};
    auto allocation = service.PutStart(generate_uuid(), "must_not_allocate",
                                       kDefaultTenant, 1024, config);
    EXPECT_FALSE(allocation.has_value());

    EraseObjectForTesting(service, kDefaultTenant, "standby_retry_conflict");
    ASSERT_TRUE(service.ReMountSegment({segment}, generate_uuid()).has_value());
    EXPECT_FALSE(HasInvalidMemoryHandleForTesting(service, kDefaultTenant,
                                                  "standby_retry_first"));
    EXPECT_TRUE(service.GetReplicaList("standby_retry_first", kDefaultTenant)
                    .has_value());
}

TEST_F(MasterServiceHATest, MultiSegmentRemountFailurePublishesNeitherSegment) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string good_endpoint = "standby_atomic_good_segment";
    const std::string bad_endpoint = "standby_atomic_bad_segment";
    auto good = MakeStandbyObject("standby_atomic_good", good_endpoint);
    good.metadata.replicas.front()
        .get_memory_descriptor()
        .buffer_descriptor.buffer_address_ = kDefaultSegmentBase;
    auto bad_first =
        MakeStandbyObject("standby_atomic_bad_first", bad_endpoint);
    auto bad_second =
        MakeStandbyObject("standby_atomic_bad_second", bad_endpoint);
    for (auto* object : {&bad_first, &bad_second}) {
        object->metadata.replicas.front()
            .get_memory_descriptor()
            .buffer_descriptor.buffer_address_ =
            kDefaultSegmentBase + kDefaultSegmentSize;
    }
    service.RestoreFromStandbySnapshot(
        {good, bad_first, bad_second}, 7,
        {MakeStandbyMemorySegment(good_endpoint),
         MakeStandbyMemorySegment(bad_endpoint)});

    Segment good_segment = MakeSegment(good_endpoint);
    Segment bad_segment =
        MakeSegment(bad_endpoint, kDefaultSegmentBase + kDefaultSegmentSize);
    auto remount =
        service.ReMountSegment({good_segment, bad_segment}, generate_uuid());
    ASSERT_FALSE(remount.has_value());
    auto good_get =
        service.GetReplicaList("standby_atomic_good", kDefaultTenant);
    ASSERT_FALSE(good_get.has_value());
    EXPECT_EQ(good_get.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto bad_batch = service.BatchGetReplicaList({"standby_atomic_bad_first"},
                                                 kDefaultTenant);
    ASSERT_EQ(bad_batch.size(), 1);
    ASSERT_FALSE(bad_batch[0].has_value());
    EXPECT_EQ(bad_batch[0].error(), ErrorCode::REPLICA_IS_NOT_READY);

    for (const auto& endpoint : {good_endpoint, bad_endpoint}) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {endpoint};
        auto allocation =
            service.PutStart(generate_uuid(), "must_not_allocate_" + endpoint,
                             kDefaultTenant, 1024, config);
        EXPECT_FALSE(allocation.has_value());
    }
}

TEST_F(MasterServiceHATest, EmptyStandbySegmentCanRemount) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    const std::string endpoint = "standby_empty_segment";
    service.RestoreFromStandbySnapshot({}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});

    Segment segment = MakeSegment(endpoint);
    ASSERT_TRUE(service.ReMountSegment({segment}, generate_uuid()).has_value());
    PutObjectOnSegment(service, generate_uuid(), "empty_segment_new_object",
                       endpoint);
    EXPECT_EQ(SegmentAllocatedSizeForTesting(service, endpoint), 1024);
}

TEST_F(MasterServiceHATest, RemountRejectsStandbySegmentNameMismatch) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    const std::string name = "standby_identity_name";
    const std::string endpoint = "standby_identity_endpoint";
    StandbySegmentInfo standby = MakeStandbyMemorySegment(endpoint);
    standby.segment_name = name;
    service.RestoreFromStandbySnapshot({}, 7, {standby});

    Segment mismatched = MakeSegment("wrong_name");
    mismatched.te_endpoint = endpoint;
    auto failed = service.ReMountSegment({mismatched}, generate_uuid());
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::INVALID_PARAMS);

    Segment correct = MakeSegment(name);
    correct.te_endpoint = endpoint;
    ASSERT_TRUE(service.ReMountSegment({correct}, generate_uuid()).has_value());
}

TEST_F(MasterServiceHATest, RemountRejectsStandbySegmentEndpointMismatch) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    const std::string name = "standby_endpoint_name";
    const std::string endpoint = "standby_endpoint_value";
    StandbySegmentInfo standby = MakeStandbyMemorySegment(endpoint);
    standby.segment_name = name;
    service.RestoreFromStandbySnapshot({}, 7, {standby});

    Segment mismatched = MakeSegment(name);
    mismatched.te_endpoint = "wrong_endpoint";
    auto failed = service.ReMountSegment({mismatched}, generate_uuid());
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::INVALID_PARAMS);

    Segment correct = MakeSegment(name);
    correct.te_endpoint = endpoint;
    ASSERT_TRUE(service.ReMountSegment({correct}, generate_uuid()).has_value());
}

TEST_F(MasterServiceHATest, RemountRejectsCxlForStandbyMemorySegment) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    const std::string endpoint = "standby_protocol_segment";
    service.RestoreFromStandbySnapshot({}, 7,
                                       {MakeStandbyMemorySegment(endpoint)});

    Segment mismatched = MakeSegment(endpoint);
    mismatched.protocol = "cxl";
    auto failed = service.ReMountSegment({mismatched}, generate_uuid());
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);

    Segment correct = MakeSegment(endpoint);
    correct.protocol = "tcp";
    ASSERT_TRUE(service.ReMountSegment({correct}, generate_uuid()).has_value());
}

TEST_F(MasterServiceHATest, RestoreFromStandbyPreservesCxlBufferDescriptor) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string segment_name = "standby_restore_cxl_segment";
    const std::string transport_endpoint = "standby_restore_tcp_endpoint";
    const size_t size = 4096;
    const uintptr_t address = 0x12345000;
    auto object =
        MakeStandbyObject("standby_restore_cxl_key", segment_name, size);
    auto& descriptor = object.metadata.replicas.front()
                           .get_memory_descriptor()
                           .buffer_descriptor;
    descriptor.buffer_address_ = address;
    descriptor.protocol_ = "cxl";

    StandbySegmentInfo segment = MakeStandbyMemorySegment(transport_endpoint);
    segment.segment_name = segment_name;
    service.RestoreFromStandbySnapshot({object}, 7, {segment});

    auto replicas = ReplicaDescriptorsForTesting(service, kDefaultTenant,
                                                 "standby_restore_cxl_key");
    ASSERT_EQ(replicas.size(), 1);
    ASSERT_TRUE(replicas.front().is_memory_replica());
    const auto& restored =
        replicas.front().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(restored.size_, size);
    EXPECT_EQ(restored.buffer_address_, address);
    EXPECT_EQ(restored.protocol_, "cxl");
    EXPECT_EQ(restored.transport_endpoint_, segment_name);

    auto public_replicas =
        service.GetReplicaList("standby_restore_cxl_key", kDefaultTenant);
    ASSERT_FALSE(public_replicas.has_value());
    EXPECT_EQ(public_replicas.error(), ErrorCode::REPLICA_IS_NOT_READY);

    Segment remount = MakeSegment(segment_name);
    remount.te_endpoint = transport_endpoint;
    auto remount_result = service.ReMountSegment({remount}, generate_uuid());
    ASSERT_FALSE(remount_result.has_value());
    EXPECT_EQ(remount_result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    auto single_after =
        service.GetReplicaList("standby_restore_cxl_key", kDefaultTenant);
    ASSERT_FALSE(single_after.has_value());
    EXPECT_EQ(single_after.error(), ErrorCode::REPLICA_IS_NOT_READY);
    auto batch_after = service.BatchGetReplicaList({"standby_restore_cxl_key"},
                                                   kDefaultTenant);
    ASSERT_EQ(batch_after.size(), 1);
    ASSERT_FALSE(batch_after[0].has_value());
    EXPECT_EQ(batch_after[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
}

TEST_F(MasterServiceHATest, RemountRejectsExistingSegmentFromDifferentClient) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    Segment segment = MakeSegment("remount_owner_segment");
    const UUID owner = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, owner).has_value());

    auto result = service.ReMountSegment({segment}, generate_uuid());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(SegmentAllocatedSizeForTesting(service, segment.name), 0);
}

TEST_F(MasterServiceHATest, RemountRejectsMismatchedExistingSegmentIdentity) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    Segment segment = MakeSegment("remount_identity_segment");
    const UUID owner = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, owner).has_value());

    Segment mismatched = segment;
    mismatched.base += 4096;
    auto result = service.ReMountSegment({mismatched}, owner);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(SegmentAllocatedSizeForTesting(service, segment.name), 0);
}

TEST_F(MasterServiceHATest, RestoreFromStandbyPreservesNoFBufferDescriptor) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string endpoint = "standby_restore_nof_endpoint";
    const size_t size = 4096;
    const uintptr_t address = 0x12345000;
    Replica::Descriptor replica;
    replica.id = 1;
    replica.status = ReplicaStatus::COMPLETE;
    NoFDescriptor nof_descriptor;
    nof_descriptor.buffer_descriptor = {static_cast<uint64_t>(size), address,
                                        "nvmeof", endpoint};
    replica.descriptor_variant = std::move(nof_descriptor);
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = size;
    metadata.last_sequence_id = 1;
    metadata.replicas.push_back(std::move(replica));
    StandbyObjectEntry object{kDefaultTenant, "standby_restore_nof_key",
                              std::move(metadata)};

    service.RestoreFromStandbySnapshot({object}, 7, {});

    auto replicas = ReplicaDescriptorsForTesting(service, kDefaultTenant,
                                                 "standby_restore_nof_key");
    ASSERT_EQ(replicas.size(), 1);
    ASSERT_TRUE(replicas.front().is_nof_replica());
    const auto& restored =
        replicas.front().get_nof_descriptor().buffer_descriptor;
    EXPECT_EQ(restored.size_, size);
    EXPECT_EQ(restored.buffer_address_, address);
    EXPECT_EQ(restored.protocol_, "nvmeof");
    EXPECT_EQ(restored.transport_endpoint_, endpoint);

    auto public_replicas =
        service.GetReplicaList("standby_restore_nof_key", kDefaultTenant);
    ASSERT_TRUE(public_replicas.has_value());
    ASSERT_EQ(public_replicas->replicas.size(), 1);
    EXPECT_TRUE(public_replicas->replicas.front().is_nof_replica());
}

TEST_F(MasterServiceHATest, OplogDisabledByDefaultDoesNotCreateWriter) {
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_cluster_id("oplog_disabled_by_default")
                      .build();

    MasterService service(config);
    EXPECT_FALSE(HasOpLogWriter(service));
}

TEST_F(MasterServiceHATest, OplogExplicitEnableCreatesWriter) {
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_enable_oplog(true)
                      .set_cluster_id("oplog_explicit_enable")
                      .build();

    MasterService service(config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));
    EXPECT_TRUE(IsOpLogEnabled(service));
    EXPECT_TRUE(HasOpLogWriter(service));
    EXPECT_TRUE(HasBatchOpLogStorage(service));
}

TEST_F(MasterServiceHATest, OplogDoesNotStartWithUnsupportedHABackend) {
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_enable_oplog(true)
                      .set_ha_backend_type("redis")
                      .set_cluster_id("oplog_unsupported_ha_backend")
                      .build();

    MasterService service(config);
    EXPECT_FALSE(HasOpLogWriter(service));
}

TEST_F(MasterServiceHATest, BatchPrimaryDoesNotStartSnapshotWorker) {
    auto config =
        MasterServiceConfig::builder()
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id("batch_snapshot_gate")
            .set_enable_snapshot(true)
            .set_snapshot_backup_dir(LegacyOpLogRootDir() + "/batch_snapshot")
            .set_snapshot_object_store_type("local")
            .build();

    MasterService service(config);
    EXPECT_FALSE(SnapshotManagerCreatedForTesting(service));
}

TEST_F(MasterServiceBatchRecordE2ETest,
       BatchRecordConstructorThrowsWhenProductionWriterInitFails) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id("test_batch_record_writer_init_fail")
            .set_ha_backend_type("etcd")
            .set_ha_backend_connstring("127.0.0.1:1")
            .build();

    EXPECT_THROW(
        { MasterService service(service_config); }, std::runtime_error);
}

TEST_F(MasterServiceHATest, GetReplicaListClassifiesRemovedReplicaStates) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(false)
                              .build();
    MasterService service(service_config);
    auto mounted = PrepareSimpleSegment(service, "get_readiness_segment");

    const std::string complete_key = "get_readiness_complete_key";
    PutObjectOnSegment(service, mounted.client_id, complete_key,
                       "get_readiness_segment");
    EXPECT_TRUE(
        service.GetReplicaList(complete_key, kDefaultTenant).has_value());

    const std::string removed_key = "get_readiness_removed_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "get_readiness_segment");
    MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant,
                                           removed_key);
    auto removed = service.GetReplicaList(removed_key, kDefaultTenant);
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, removed.error());

    const std::string processing_key = "get_readiness_processing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, processing_key, kDefaultTenant,
                              1024, config)
                    .has_value());
    auto processing = service.GetReplicaList(processing_key, kDefaultTenant);
    ASSERT_FALSE(processing.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, processing.error());

    auto missing =
        service.GetReplicaList("get_readiness_missing_key", kDefaultTenant);
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, missing.error());
}

TEST_F(MasterServiceHATest, BatchGetReplicaListClassifiesRemovedReplicaStates) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(false)
                              .build();
    MasterService service(service_config);
    auto mounted = PrepareSimpleSegment(service, "batch_get_readiness_segment");

    const std::string complete_key = "batch_get_readiness_complete_key";
    PutObjectOnSegment(service, mounted.client_id, complete_key,
                       "batch_get_readiness_segment");

    const std::string removed_key = "batch_get_readiness_removed_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "batch_get_readiness_segment");
    MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant,
                                           removed_key);

    const std::string processing_key = "batch_get_readiness_processing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, processing_key, kDefaultTenant,
                              1024, config)
                    .has_value());

    auto results =
        service.BatchGetReplicaList({complete_key, removed_key, processing_key,
                                     "batch_get_readiness_missing_key"},
                                    kDefaultTenant);
    ASSERT_EQ(4u, results.size());
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, results[1].error());
    ASSERT_FALSE(results[2].has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, results[2].error());
    ASSERT_FALSE(results[3].has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, results[3].error());
}

TEST_F(MasterServiceHATest, ExistKeyRequiresCompletedReplica) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(false)
                              .build();
    MasterService service(service_config);
    auto mounted = PrepareSimpleSegment(service, "exist_readiness_segment");

    const std::string complete_key = "exist_readiness_complete_key";
    PutObjectOnSegment(service, mounted.client_id, complete_key,
                       "exist_readiness_segment");
    auto complete = service.ExistKey(complete_key, kDefaultTenant);
    ASSERT_TRUE(complete.has_value());
    EXPECT_TRUE(complete.value());

    const std::string removed_key = "exist_readiness_removed_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "exist_readiness_segment");
    MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant,
                                           removed_key);
    auto removed = service.ExistKey(removed_key, kDefaultTenant);
    ASSERT_TRUE(removed.has_value());
    EXPECT_FALSE(removed.value());

    const std::string processing_key = "exist_readiness_processing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, processing_key, kDefaultTenant,
                              1024, config)
                    .has_value());
    auto processing = service.ExistKey(processing_key, kDefaultTenant);
    ASSERT_TRUE(processing.has_value());
    EXPECT_FALSE(processing.value());

    auto missing =
        service.ExistKey("exist_readiness_missing_key", kDefaultTenant);
    ASSERT_TRUE(missing.has_value());
    EXPECT_FALSE(missing.value());
}

TEST_F(MasterServiceHATest, BatchExistKeyRequiresCompletedReplica) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(false)
                              .build();
    MasterService service(service_config);
    auto mounted =
        PrepareSimpleSegment(service, "batch_exist_readiness_segment");

    const std::string complete_key = "batch_exist_readiness_complete_key";
    PutObjectOnSegment(service, mounted.client_id, complete_key,
                       "batch_exist_readiness_segment");

    const std::string removed_key = "batch_exist_readiness_removed_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "batch_exist_readiness_segment");
    MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant,
                                           removed_key);

    const std::string processing_key = "batch_exist_readiness_processing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, processing_key, kDefaultTenant,
                              1024, config)
                    .has_value());

    auto results =
        service.BatchExistKey({complete_key, removed_key, processing_key,
                               "batch_exist_readiness_missing_key"},
                              kDefaultTenant);
    ASSERT_EQ(4u, results.size());
    ASSERT_TRUE(results[0].has_value());
    EXPECT_TRUE(results[0].value());
    ASSERT_TRUE(results[1].has_value());
    EXPECT_FALSE(results[1].value());
    ASSERT_TRUE(results[2].has_value());
    EXPECT_FALSE(results[2].value());
    ASSERT_TRUE(results[3].has_value());
    EXPECT_FALSE(results[3].value());
}

TEST_F(MasterServiceHATest, BatchRecordSubmissionHelpersUseOrderedWriter) {
    const std::string cluster_id = "test_batch_record_helpers_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto visible = AppendVisibleForTesting(service, OpType::PUT_END, "tenant",
                                           "visible_key", "visible_payload");
    ASSERT_TRUE(visible.has_value());
    EXPECT_EQ(1u, visible.value());

    std::promise<OpLogEntry> finalized_promise;
    auto finalized_future = finalized_promise.get_future();
    auto finalized = AppendFinalizeForTesting(
        service, OpType::REMOVE, "tenant", "remove_key", {},
        [&finalized_promise](const OpLogEntry& durable_entry) {
            finalized_promise.set_value(durable_entry);
        });
    ASSERT_TRUE(finalized.has_value());
    EXPECT_EQ(2u, finalized->sequence_id);

    ASSERT_EQ(std::future_status::ready,
              finalized_future.wait_for(std::chrono::seconds(5)));
    OpLogEntry finalized_entry = finalized_future.get();
    EXPECT_EQ(2u, finalized_entry.sequence_id);
    EXPECT_EQ(OpType::REMOVE, finalized_entry.op_type);
    EXPECT_EQ("remove_key", finalized_entry.object_key);

    OpLogBatchStorage storage(cluster_id, *backend);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(2u, prefix.last_seq);
}

TEST_F(MasterServiceHATest,
       BatchRecordWriterResolvesEmptyTenantWhenMultiTenantDisabled) {
    const std::string cluster_id = "test_batch_record_default_tenant";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_enable_multi_tenants(false)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto appended = AppendFinalizeForTesting(service, OpType::REMOVE, "",
                                             "default_tenant_key", {}, nullptr);
    ASSERT_TRUE(appended.has_value());

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ("default", batch.entries[0].tenant_id);
}

TEST_F(MasterServiceBatchRecordE2ETest,
       PrimaryWritesBatchRecordAndDurablePrefix) {
    const std::string cluster_id = "test_batch_record_e2e_primary";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_primary_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_MOUNT, batch.entries[0].op_type);
    EXPECT_EQ(1u, batch.entries[0].sequence_id);

    const std::string key = "batch_e2e_primary_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_primary_segment");
    ReadBatchEventually(storage, 2, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(2u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(2u, prefix.last_seq);

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(1u, replicas->replicas.size());
    EXPECT_TRUE(replicas->replicas.front().is_memory_replica());
}

TEST_F(MasterServiceBatchRecordE2ETest, StandbyAppliesPrimaryBatchRecords) {
    const std::string cluster_id = "test_batch_record_e2e_standby_apply";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_standby_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_standby_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_standby_segment");
    ReadBatchEventually(storage, 2, batch);

    MockMetadataStore standby_metadata;
    OpLogApplier applier(&standby_metadata, cluster_id);
    OpLogBatchStandbyReader reader(cluster_id, *backend, applier);

    auto result = reader.PollOnce();
    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(standby_metadata.Exists(kDefaultTenant, key));
}

TEST_F(MasterServiceBatchRecordE2ETest, PromotionCatchesUpToDurablePrefix) {
    const std::string cluster_id = "test_batch_record_e2e_promotion_catchup";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_promotion_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_promotion_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_promotion_segment");
    ReadBatchEventually(storage, 2, batch);

    HotStandbyConfig standby_config;
    standby_config.enable_verification = false;
    standby_config.max_replication_lag_entries = 1000;
    standby_config.enable_oplog_following = true;
    standby_config.enable_snapshot_bootstrap = false;
    standby_config.oplog_poll_interval_ms = 50;

    HotStandbyService standby(standby_config);
    standby.SetCatchUpBatchKvBackendForTesting(backend);
    auto start_err = standby.Start("", "", cluster_id);
    ASSERT_EQ(ErrorCode::OK, start_err);
    ASSERT_EQ(StandbyState::WATCHING, standby.GetState());

    StandbySnapshot snapshot;
    ASSERT_EQ(ErrorCode::OK, standby.PromoteAndExportSnapshot(snapshot));
    EXPECT_EQ(2u, snapshot.oplog_sequence_id);

    bool found_key = false;
    for (const auto& object : snapshot.objects) {
        found_key = found_key || object.key == key;
    }
    EXPECT_TRUE(found_key);
}

TEST_F(MasterServiceBatchRecordE2ETest,
       BackendFailureStopsNewBatchReservations) {
    const std::string cluster_id = "test_batch_record_e2e_backend_failure";
    auto backend = std::make_shared<FailingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    backend->SetTxnError(ErrorCode::PERSISTENT_FAIL);
    auto first = AppendVisibleForTesting(service, OpType::PUT_END,
                                         kDefaultTenant, "failing_key", {});
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(backend->WaitForTxnCalls(1));

    tl::expected<uint64_t, ErrorCode> second;
    for (int i = 0; i < 100; ++i) {
        second = AppendVisibleForTesting(service, OpType::PUT_END,
                                         kDefaultTenant, "rejected_key", {});
        if (!second.has_value()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, second.error());
    backend->SetTxnError(ErrorCode::OK);
}

TEST_F(MasterServiceBatchRecordE2ETest,
       RetryRecoveryRestoresBatchWriterAccepting) {
    const std::string cluster_id = "test_batch_record_e2e_retry_recovery";
    auto backend = std::make_shared<FailingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;

    backend->SetTxnError(ErrorCode::PERSISTENT_FAIL);
    auto first = AppendVisibleForTesting(service, OpType::PUT_END,
                                         kDefaultTenant, "retry_key", {});
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(backend->WaitForTxnCalls(1));

    backend->SetTxnError(ErrorCode::OK);
    ReadBatchEventually(storage, 1, batch);

    auto recovered = AppendVisibleForTesting(
        service, OpType::PUT_END, kDefaultTenant, "recovered_key", {});
    ASSERT_TRUE(recovered.has_value()) << toString(recovered.error());
    ReadBatchEventually(storage, 2, batch);
}

TEST_F(MasterServiceBatchRecordE2ETest,
       RemoveBeforeDurableHidesReplicasFromPrimaryReads) {
    const std::string cluster_id = "test_batch_record_e2e_remove_visibility";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_remove_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_remove_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_remove_segment");
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    auto removed = service.Remove(key, kDefaultTenant, /*force=*/true);
    EXPECT_TRUE(removed.has_value());
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());
    DurablePrefix prefix;
    auto prefix_read = storage.ReadDurablePrefix(prefix);
    EXPECT_EQ(ErrorCode::OK, prefix_read);
    if (prefix_read == ErrorCode::OK) {
        EXPECT_EQ(2u, prefix.batch_id);
        EXPECT_EQ(2u, prefix.last_seq);
    }

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(key, batch.entries[0].object_key);
}

TEST_F(MasterServiceBatchRecordE2ETest, RemoveByRegexWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_remove_regex";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_regex_remove_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string removed_key = "batch_regex_remove_key";
    const std::string kept_key = "batch_regex_keep_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "batch_regex_remove_segment");
    ReadBatchEventually(storage, 2, batch);
    PutObjectOnSegment(service, mounted.client_id, kept_key,
                       "batch_regex_remove_segment");
    ReadBatchEventually(storage, 3, batch);

    auto removed = service.RemoveByRegex("^batch_regex_remove_", kDefaultTenant,
                                         /*force=*/true);
    ASSERT_TRUE(removed.has_value()) << toString(removed.error());
    EXPECT_EQ(1, removed.value());

    ReadBatchEventually(storage, 4, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(removed_key, batch.entries[0].object_key);
    auto removed_exists = service.ExistKey(removed_key, kDefaultTenant);
    ASSERT_TRUE(removed_exists.has_value()) << toString(removed_exists.error());
    EXPECT_FALSE(removed_exists.value());
    auto kept_exists = service.ExistKey(kept_key, kDefaultTenant);
    ASSERT_TRUE(kept_exists.has_value()) << toString(kept_exists.error());
    EXPECT_TRUE(kept_exists.value());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       RemoveFailsBeforeMutationWhenBatchReservationUnavailable) {
    const std::string cluster_id = "test_batch_record_remove_reserve_full";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_remove_reserve_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_reserve_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_reserve_seg");
    ReadBatchEventually(storage, 2, batch);

    auto held_reservation = ReserveBatchSlotForTesting(service);
    ASSERT_TRUE(held_reservation.has_value())
        << toString(held_reservation.error());

    auto removed = service.Remove(key, kDefaultTenant, /*force=*/true);
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED, removed.error());

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    EXPECT_EQ(1u, replicas->replicas.size());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       RemoveDurableCallbackReleasesResources) {
    const std::string cluster_id = "test_batch_record_e2e_remove_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted =
        PrepareSimpleSegment(service, "batch_e2e_remove_finalize_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_remove_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_remove_finalize_segment");
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    ASSERT_TRUE(
        service.Remove(key, kDefaultTenant, /*force=*/true).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    auto before_finalize = service.PutStart(
        mounted.client_id, "batch_e2e_before_remove_finalize_key",
        kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    auto removed = service.ExistKey(key, kDefaultTenant);
    ASSERT_TRUE(removed.has_value());
    EXPECT_FALSE(removed.value());

    auto after_finalize = service.PutStart(
        mounted.client_id, "batch_e2e_after_remove_finalize_key",
        kDefaultTenant, 1024, config);
    EXPECT_TRUE(after_finalize.has_value()) << toString(after_finalize.error());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       ClearInvalidHandlesReleasesResourcesAfterDurable) {
    const std::string cluster_id = "test_batch_record_stale_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_stale_finalize_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    auto spare =
        PrepareSimpleSegment(service, "batch_stale_finalize_spare",
                             kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 2, batch);

    const std::string key = "batch_stale_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_stale_finalize_seg");
    ReadBatchEventually(storage, 3, batch);

    backend->BlockTxn();
    auto unmounted =
        service.UnmountSegment(mounted.segment_id, mounted.client_id);
    ASSERT_TRUE(unmounted.has_value()) << toString(unmounted.error());

    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_stale_finalize_spare"};
    auto before_finalize =
        service.PutStart(spare.client_id, "batch_stale_before_finalize",
                         kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadRemoveBatchEventually(storage, 4, key, batch);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> after_finalize =
        tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
    for (int i = 0; i < 50; ++i) {
        after_finalize =
            service.PutStart(spare.client_id, "batch_stale_after_finalize",
                             kDefaultTenant, 1024, config);
        if (after_finalize.has_value()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(after_finalize.has_value()) << toString(after_finalize.error());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       UpsertStartStaleCleanupReleasesResourcesAfterDurable) {
    const std::string cluster_id = "test_batch_record_upsert_stale_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_upsert_stale_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    auto spare =
        PrepareSimpleSegment(service, "batch_upsert_stale_spare",
                             kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 2, batch);

    const std::string key = "batch_upsert_stale_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_upsert_stale_seg");
    ReadBatchEventually(storage, 3, batch);

    PrepareUnmountSegmentForTesting(service, mounted.segment_id);
    backend->BlockTxn();

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_upsert_stale_spare"};
    auto upsert_result =
        service.UpsertStart(spare.client_id, key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(upsert_result.has_value());

    auto before_finalize =
        service.PutStart(spare.client_id, "batch_upsert_stale_before_finalize",
                         kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadRemoveBatchEventually(storage, 4, key, batch);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> after_finalize =
        tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
    for (int i = 0; i < 50; ++i) {
        after_finalize =
            service.PutStart(spare.client_id, "batch_upsert_stale_after",
                             kDefaultTenant, 1024, config);
        if (after_finalize.has_value()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(after_finalize.has_value()) << toString(after_finalize.error());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       BatchRemoveStaleCleanupReleasesResourcesAfterDurable) {
    const std::string cluster_id =
        "test_batch_record_batch_remove_stale_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted =
        PrepareSimpleSegment(service, "batch_remove_stale_finalize_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    auto spare =
        PrepareSimpleSegment(service, "batch_remove_stale_finalize_spare",
                             kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 2, batch);

    const std::string key = "batch_remove_stale_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_stale_finalize_seg");
    ReadBatchEventually(storage, 3, batch);

    PrepareUnmountSegmentForTesting(service, mounted.segment_id);
    backend->BlockTxn();

    auto remove_result = service.BatchRemove({key}, kDefaultTenant,
                                             /*force=*/true);
    ASSERT_EQ(1u, remove_result.size());
    EXPECT_FALSE(remove_result[0].has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result[0].error());

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_remove_stale_finalize_spare"};
    auto before_finalize =
        service.PutStart(spare.client_id, "batch_remove_stale_before_finalize",
                         kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadRemoveBatchEventually(storage, 4, key, batch);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> after_finalize =
        tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
    for (int i = 0; i < 50; ++i) {
        after_finalize =
            service.PutStart(spare.client_id, "batch_remove_stale_after",
                             kDefaultTenant, 1024, config);
        if (after_finalize.has_value()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(after_finalize.has_value()) << toString(after_finalize.error());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       PartialEvictLeavesRemainingCompleteReplicasReadable) {
    const std::string cluster_id = "test_batch_record_e2e_partial_evict";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_partial_evict_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_partial_evict_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_partial_evict_seg");
    ReadBatchEventually(storage, 2, batch);

    OffloadTaskItem task{.tenant_id = kDefaultTenant, .key = key, .size = 1024};
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = "batch_e2e_partial_evict_disk";
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(mounted.client_id, {task}, {metadata})
            .has_value());
    ReadBatchEventually(storage, 3, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 4, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(key, batch.entries[0].object_key);

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(1u, replicas->replicas.size());
    EXPECT_TRUE(replicas->replicas.front().is_local_disk_replica());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       EvictAllReadableReplicasReturnsObjectNotFound) {
    const std::string cluster_id = "test_batch_record_e2e_evict_all";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_evict_all_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_e2e_evict_all_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_evict_all_seg");
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 3, batch);

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_FALSE(replicas.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, replicas.error());

    auto exists = service.ExistKey(key, kDefaultTenant);
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       ProcessingOnlyObjectExistKeyReturnsFalse) {
    const std::string cluster_id = "test_batch_record_e2e_processing_only";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_processing_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_e2e_processing_seg"};
    const std::string key = "batch_e2e_processing_key";
    ASSERT_TRUE(
        service.PutStart(mounted.client_id, key, kDefaultTenant, 1024, config)
            .has_value());

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_FALSE(replicas.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, replicas.error());

    auto exists = service.ExistKey(key, kDefaultTenant);
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       OffloadAndPromotionSuccessRemainFunctional) {
    const std::string cluster_id = "test_batch_record_e2e_offload_promotion";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted =
        PrepareSimpleSegment(service, "batch_e2e_offload_promotion_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string offload_key = "batch_e2e_offload_key";
    PutObjectOnSegment(service, mounted.client_id, offload_key,
                       "batch_e2e_offload_promotion_seg");
    ReadBatchEventually(storage, 2, batch);

    OffloadTaskItem task{
        .tenant_id = kDefaultTenant, .key = offload_key, .size = 1024};
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = "batch_e2e_offload_endpoint";
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(mounted.client_id, {task}, {metadata})
            .has_value());
    ReadBatchEventually(storage, 3, batch);

    auto offload_replicas = service.GetReplicaList(offload_key, kDefaultTenant);
    ASSERT_TRUE(offload_replicas.has_value())
        << toString(offload_replicas.error());
    bool has_local_disk = false;
    for (const auto& replica : offload_replicas->replicas) {
        has_local_disk = has_local_disk || replica.is_local_disk_replica();
    }
    EXPECT_TRUE(has_local_disk);

    const std::string promotion_key = "batch_e2e_promotion_success_key";
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_e2e_offload_promotion_seg"};
    auto put_start = service.PutStart(mounted.client_id, promotion_key,
                                      kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(1u, put_start->size());
    SeedPromotionTaskForTesting(&service, kDefaultTenant, promotion_key,
                                mounted.client_id, put_start->front().id, 1024);

    ASSERT_TRUE(service
                    .NotifyPromotionSuccess(mounted.client_id, promotion_key,
                                            kDefaultTenant)
                    .has_value());
    ReadBatchEventually(storage, 4, batch);

    auto promotion_replicas =
        service.GetReplicaList(promotion_key, kDefaultTenant);
    ASSERT_TRUE(promotion_replicas.has_value())
        << toString(promotion_replicas.error());
    ASSERT_EQ(1u, promotion_replicas->replicas.size());
    EXPECT_TRUE(promotion_replicas->replicas.front().is_memory_replica());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       SegmentLifecycleEntriesRemainFunctional) {
    const std::string cluster_id = "test_batch_record_e2e_segment_lifecycle";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    const UUID mount_client = generate_uuid();
    Segment mounted = MakeSegment("batch_e2e_lifecycle_mount");
    ASSERT_TRUE(service.MountSegment(mounted, mount_client).has_value());
    ReadBatchEventually(storage, 1, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_MOUNT, batch.entries[0].op_type);

    const UUID remount_client = generate_uuid();
    Segment remounted = MakeSegment("batch_e2e_lifecycle_remount",
                                    kDefaultSegmentBase + kDefaultSegmentSize);
    ASSERT_TRUE(
        service.ReMountSegment({remounted}, remount_client).has_value());
    ReadBatchEventually(storage, 2, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_MOUNT, batch.entries[0].op_type);

    const std::string key = "batch_e2e_lifecycle_key";
    PutObjectOnSegment(service, remount_client, key,
                       "batch_e2e_lifecycle_remount");
    ReadBatchEventually(storage, 3, batch);
    ASSERT_TRUE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ASSERT_TRUE(service.UnmountSegment(mounted.id, mount_client).has_value());
    bool saw_unmount = false;
    for (uint64_t batch_id = 4; batch_id <= 6 && !saw_unmount; ++batch_id) {
        ReadBatchEventually(storage, batch_id, batch);
        ASSERT_EQ(1u, batch.entries.size());
        saw_unmount = batch.entries[0].op_type == OpType::SEGMENT_UNMOUNT;
    }
    EXPECT_TRUE(saw_unmount);
}

TEST_F(MasterServiceHATest, PutEndWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_put_end_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_put_end_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ErrorCode read_err = ErrorCode::ETCD_KEY_NOT_EXIST;
    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(1, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_put_end_segment"};
    const std::string key = "batch_put_end_key";
    auto put_start =
        service.PutStart(mounted.client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_TRUE(
        service
            .PutEnd(mounted.client_id, key, kDefaultTenant, ReplicaType::MEMORY)
            .has_value());

    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(2, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(2u, batch.entries[0].sequence_id);

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(2u, prefix.last_seq);
}

TEST_F(MasterServiceHATest, PutEndVisibleBeforeBatchRecordDurable) {
    const std::string cluster_id = "test_batch_record_put_end_visible";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_put_visible_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_put_visible_segment"};
    const std::string key = "batch_put_visible_key";
    auto put_start =
        service.PutStart(mounted.client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    backend->BlockTxn();
    auto put_end = service.PutEnd(mounted.client_id, key, kDefaultTenant,
                                  ReplicaType::MEMORY);
    EXPECT_TRUE(put_end.has_value());

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    EXPECT_TRUE(replicas.has_value());
    if (replicas.has_value()) {
        EXPECT_EQ(1u, replicas->replicas.size());
        if (!replicas->replicas.empty()) {
            EXPECT_TRUE(replicas->replicas.front().is_memory_replica());
        }
    }
    DurablePrefix prefix;
    auto prefix_read = storage.ReadDurablePrefix(prefix);
    EXPECT_EQ(ErrorCode::OK, prefix_read);
    if (prefix_read == ErrorCode::OK) {
        EXPECT_EQ(1u, prefix.batch_id);
        EXPECT_EQ(1u, prefix.last_seq);
    }

    backend->AllowTxn();
    ReadBatchEventually(storage, 2, batch);
}

TEST_F(MasterServiceHATest, CopyEndVisibleBeforeBatchRecordDurable) {
    const std::string cluster_id = "test_batch_record_copy_end_visible";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto source = PrepareSimpleSegment(service, "batch_copy_visible_src");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_copy_visible_key";
    PutObjectOnSegment(service, source.client_id, key,
                       "batch_copy_visible_src");
    ReadBatchEventually(storage, 2, batch);

    PrepareSimpleSegment(service, "batch_copy_visible_dst",
                         kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_TRUE(service
                    .CopyStart(source.client_id, key, kDefaultTenant,
                               "batch_copy_visible_src",
                               {"batch_copy_visible_dst"})
                    .has_value());

    backend->BlockTxn();
    auto copy_future = std::async(std::launch::async, [&] {
        return service.CopyEnd(source.client_id, key, kDefaultTenant);
    });
    const auto status = copy_future.wait_for(std::chrono::milliseconds(200));
    EXPECT_EQ(std::future_status::ready, status)
        << "CopyEnd must queue batch OpLog and return before durable";
    if (status != std::future_status::ready) {
        backend->AllowTxn();
    }
    auto copy_end = copy_future.get();
    ASSERT_TRUE(copy_end.has_value()) << toString(copy_end.error());

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(3u, prefix.batch_id);
    EXPECT_EQ(3u, prefix.last_seq);

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(2u, replicas->replicas.size());

    backend->AllowTxn();
    ReadBatchEventually(storage, 4, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(key, batch.entries[0].object_key);
}

TEST_F(MasterServiceHATest,
       MoveEndHidesSourceBeforeDurableAndReleasesAfterDurable) {
    const std::string cluster_id = "test_batch_record_move_end_visible";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_eviction_high_watermark_ratio(1.0)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto source = PrepareSimpleSegment(service, "batch_move_visible_src",
                                       kDefaultSegmentBase, 1024);
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_move_visible_key";
    PutObjectOnSegment(service, source.client_id, key, "batch_move_visible_src",
                       1024);
    ReadBatchEventually(storage, 2, batch);

    PrepareSimpleSegment(service, "batch_move_visible_dst",
                         kDefaultSegmentBase + kDefaultSegmentSize, 1024);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_TRUE(service
                    .MoveStart(source.client_id, key, kDefaultTenant,
                               "batch_move_visible_src",
                               "batch_move_visible_dst")
                    .has_value());

    backend->BlockTxn();
    auto move_future = std::async(std::launch::async, [&] {
        return service.MoveEnd(source.client_id, key, kDefaultTenant);
    });
    const auto status = move_future.wait_for(std::chrono::milliseconds(200));
    EXPECT_EQ(std::future_status::ready, status)
        << "MoveEnd must queue batch OpLog and return before durable";
    if (status != std::future_status::ready) {
        backend->AllowTxn();
    }
    auto move_end = move_future.get();
    ASSERT_TRUE(move_end.has_value()) << toString(move_end.error());

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(3u, prefix.batch_id);
    EXPECT_EQ(3u, prefix.last_seq);

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(1u, replicas->replicas.size());

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_move_visible_src"};
    auto before_finalize =
        service.PutStart(source.client_id, "batch_move_before_finalize",
                         kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 4, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(key, batch.entries[0].object_key);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> after_finalize =
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    for (int i = 0; i < 50; ++i) {
        after_finalize =
            service.PutStart(source.client_id, "batch_move_after_finalize",
                             kDefaultTenant, 1024, config);
        if (after_finalize.has_value()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(after_finalize.has_value()) << toString(after_finalize.error());
}

TEST_F(MasterServiceHATest,
       NotifyOffloadSuccessFallbackWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_offload_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_offload_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ErrorCode read_err = ErrorCode::ETCD_KEY_NOT_EXIST;
    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(1, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);

    const std::string key = "batch_offload_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_offload_segment");
    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(2, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);

    OffloadTaskItem task{.tenant_id = kDefaultTenant, .key = key, .size = 1024};
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = "local_disk_endpoint";
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(mounted.client_id, {task}, {metadata})
            .has_value());
    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(replicas.has_value());
    bool has_local_disk = false;
    for (const auto& replica : replicas->replicas) {
        has_local_disk = has_local_disk || replica.is_local_disk_replica();
    }
    EXPECT_TRUE(has_local_disk);

    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(3, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest,
       NotifyOffloadSuccessFallbackVisibleBeforeBatchRecordDurable) {
    const std::string cluster_id = "test_batch_record_offload_visible";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_offload_visible_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_offload_visible_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_offload_visible_seg");
    ReadBatchEventually(storage, 2, batch);

    OffloadTaskItem task{.tenant_id = kDefaultTenant, .key = key, .size = 1024};
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = "local_disk_visible_endpoint";

    backend->BlockTxn();
    auto offload =
        service.NotifyOffloadSuccess(mounted.client_id, {task}, {metadata});
    EXPECT_TRUE(offload.has_value());

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    EXPECT_TRUE(replicas.has_value());
    if (replicas.has_value()) {
        bool has_local_disk = false;
        for (const auto& replica : replicas->replicas) {
            has_local_disk = has_local_disk || replica.is_local_disk_replica();
        }
        EXPECT_TRUE(has_local_disk);
    }
    DurablePrefix prefix;
    auto prefix_read = storage.ReadDurablePrefix(prefix);
    EXPECT_EQ(ErrorCode::OK, prefix_read);
    if (prefix_read == ErrorCode::OK) {
        EXPECT_EQ(2u, prefix.batch_id);
        EXPECT_EQ(2u, prefix.last_seq);
    }

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);
}

TEST_F(MasterServiceHATest, SegmentLifecycleWritesBatchRecordOpLogs) {
    const std::string cluster_id = "test_batch_record_segment_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    const UUID client_id = generate_uuid();
    Segment mounted = MakeSegment("batch_segment_mount");
    ASSERT_TRUE(service.MountSegment(mounted, client_id).has_value());

    const UUID remount_client_id = generate_uuid();
    Segment remounted = MakeSegment("batch_segment_remount",
                                    kDefaultSegmentBase + kDefaultSegmentSize);
    ASSERT_TRUE(
        service.ReMountSegment({remounted}, remount_client_id).has_value());

    ASSERT_TRUE(service.UnmountSegment(mounted.id, client_id).has_value());

    OpLogBatchStorage storage(cluster_id, *backend);
    auto read_batch = [&](uint64_t batch_id) {
        OpLogBatchRecord batch;
        ErrorCode read_err = ErrorCode::ETCD_KEY_NOT_EXIST;
        for (int i = 0; i < 50; ++i) {
            read_err = storage.ReadBatch(batch_id, batch);
            if (read_err == ErrorCode::OK) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        EXPECT_EQ(ErrorCode::OK, read_err);
        return batch;
    };

    auto mount_batch = read_batch(1);
    ASSERT_EQ(1u, mount_batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_MOUNT, mount_batch.entries[0].op_type);
    EXPECT_EQ(1u, mount_batch.entries[0].sequence_id);
    EXPECT_FALSE(mount_batch.entries[0].payload.empty());

    auto remount_batch = read_batch(2);
    ASSERT_EQ(1u, remount_batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_MOUNT, remount_batch.entries[0].op_type);
    EXPECT_EQ(2u, remount_batch.entries[0].sequence_id);
    EXPECT_FALSE(remount_batch.entries[0].payload.empty());

    auto unmount_batch = read_batch(3);
    ASSERT_EQ(1u, unmount_batch.entries.size());
    EXPECT_EQ(OpType::SEGMENT_UNMOUNT, unmount_batch.entries[0].op_type);
    EXPECT_EQ(3u, unmount_batch.entries[0].sequence_id);
    EXPECT_FALSE(unmount_batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest, NotifyPromotionSuccessWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_promotion_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    const auto mounted = PrepareSimpleSegment(service, "batch_promotion_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ErrorCode read_err = ErrorCode::ETCD_KEY_NOT_EXIST;
    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(1, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);

    const std::string key = "batch_promotion_key";
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_promotion_seg"};
    auto put_start =
        service.PutStart(mounted.client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(1u, put_start->size());
    SeedPromotionTaskForTesting(&service, kDefaultTenant, key,
                                mounted.client_id, put_start->front().id, 1024);

    auto res =
        service.NotifyPromotionSuccess(mounted.client_id, key, kDefaultTenant);
    ASSERT_TRUE(res.has_value());

    for (int i = 0; i < 50; ++i) {
        read_err = storage.ReadBatch(2, batch);
        if (read_err == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(ErrorCode::OK, read_err);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(2u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest,
       NotifyPromotionSuccessVisibleBeforeBatchRecordDurable) {
    const std::string cluster_id = "test_batch_record_promotion_visible";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    const auto mounted =
        PrepareSimpleSegment(service, "batch_promotion_visible_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_promotion_visible_key";
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"batch_promotion_visible_seg"};
    auto put_start =
        service.PutStart(mounted.client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(1u, put_start->size());
    SeedPromotionTaskForTesting(&service, kDefaultTenant, key,
                                mounted.client_id, put_start->front().id, 1024);

    backend->BlockTxn();
    auto promotion =
        service.NotifyPromotionSuccess(mounted.client_id, key, kDefaultTenant);
    EXPECT_TRUE(promotion.has_value());

    auto replicas = service.GetReplicaList(key, kDefaultTenant);
    EXPECT_TRUE(replicas.has_value());
    if (replicas.has_value()) {
        EXPECT_EQ(1u, replicas->replicas.size());
        if (!replicas->replicas.empty()) {
            EXPECT_TRUE(replicas->replicas.front().is_memory_replica());
        }
    }
    DurablePrefix prefix;
    auto prefix_read = storage.ReadDurablePrefix(prefix);
    EXPECT_EQ(ErrorCode::OK, prefix_read);
    if (prefix_read == ErrorCode::OK) {
        EXPECT_EQ(1u, prefix.batch_id);
        EXPECT_EQ(1u, prefix.last_seq);
    }

    backend->AllowTxn();
    ReadBatchEventually(storage, 2, batch);
}

TEST_F(MasterServiceHATest, RemoveWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_remove_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_remove_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_key";
    PutObjectOnSegment(service, mounted.client_id, key, "batch_remove_segment");
    ReadBatchEventually(storage, 2, batch);

    ASSERT_TRUE(
        service.Remove(key, kDefaultTenant, /*force=*/true).has_value());
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, RemoveHidesBeforeDurableAndReleasesAfterFinalize) {
    const std::string cluster_id = "test_batch_record_remove_finalize_cluster";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_remove_finalize_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_finalize_seg");
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    ASSERT_TRUE(
        service.Remove(key, kDefaultTenant, /*force=*/true).has_value());
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_remove_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key = "after_remove_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, BatchRemoveWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_batch_remove_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_remove_many_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_many_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_many_segment");
    ReadBatchEventually(storage, 2, batch);

    auto results = service.BatchRemove({key}, kDefaultTenant, /*force=*/true);
    ASSERT_EQ(1u, results.size());
    ASSERT_TRUE(results[0].has_value());
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, BatchRemoveFinalizesEachObjectAfterDurable) {
    const std::string cluster_id = "test_batch_record_batch_remove_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted =
        PrepareSimpleSegment(service, "batch_remove_finalize_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_finalize_segment");
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    auto results = service.BatchRemove({key}, kDefaultTenant, /*force=*/true);
    ASSERT_EQ(1u, results.size());
    ASSERT_TRUE(results[0].has_value());
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_batch_remove_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key =
            "after_batch_remove_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, RemoveAllWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_remove_all_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_remove_all_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_remove_all_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_remove_all_segment");
    ReadBatchEventually(storage, 2, batch);

    EXPECT_EQ(1, service.RemoveAll(kDefaultTenant, /*force=*/true));
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, RemoveAllFinalizesAfterDurable) {
    const std::string cluster_id = "test_batch_record_remove_all_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "remove_all_finalize_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "remove_all_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "remove_all_finalize_segment");
    ReadBatchEventually(storage, 2, batch);

    backend->BlockTxn();
    EXPECT_EQ(1, service.RemoveAll(kDefaultTenant, /*force=*/true));
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_remove_all_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key = "after_remove_all_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, BatchReplicaClearAllWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_clear_all_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_clear_all_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_clear_all_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_clear_all_segment");
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    auto res = service.BatchReplicaClear({key}, mounted.client_id, "");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, BatchReplicaClearSegmentWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_clear_segment_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_clear_seg1");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_clear_segment_key";
    PutObjectOnSegment(service, mounted.client_id, key, "batch_clear_seg1");
    ReadBatchEventually(storage, 2, batch);

    PrepareSimpleSegment(service, "batch_clear_seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 3, batch);

    auto copy_start =
        service.CopyStart(mounted.client_id, key, kDefaultTenant,
                          "batch_clear_seg1", {"batch_clear_seg2"});
    ASSERT_TRUE(copy_start.has_value());
    auto copy_end = service.CopyEnd(mounted.client_id, key, kDefaultTenant);
    ASSERT_TRUE(copy_end.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    auto res =
        service.BatchReplicaClear({key}, mounted.client_id, "batch_clear_seg1");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);
    ReadBatchEventually(storage, 4, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(4u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest, BatchReplicaClearAllReleasesAfterDurable) {
    const std::string cluster_id = "test_batch_record_clear_all_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "clear_all_finalize_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "clear_all_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "clear_all_finalize_segment");
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    backend->BlockTxn();
    auto res = service.BatchReplicaClear({key}, mounted.client_id, "");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_clear_all_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key = "after_clear_all_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, BatchReplicaClearSegmentReleasesAfterDurable) {
    const std::string cluster_id = "test_batch_record_clear_segment_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 2048}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "clear_finalize_seg1");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "clear_segment_finalize_key";
    ReplicateConfig pinned_config;
    pinned_config.replica_num = 1;
    pinned_config.preferred_segments = {"clear_finalize_seg1"};
    pinned_config.with_hard_pin = true;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, key, kDefaultTenant, 1024,
                              pinned_config)
                    .has_value());
    ASSERT_TRUE(
        service
            .PutEnd(mounted.client_id, key, kDefaultTenant, ReplicaType::MEMORY)
            .has_value());
    ReadBatchEventually(storage, 2, batch);

    PrepareSimpleSegment(service, "clear_finalize_seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 3, batch);

    auto copy_start =
        service.CopyStart(mounted.client_id, key, kDefaultTenant,
                          "clear_finalize_seg1", {"clear_finalize_seg2"});
    ASSERT_TRUE(copy_start.has_value());
    ASSERT_TRUE(
        service.CopyEnd(mounted.client_id, key, kDefaultTenant).has_value());
    ReadBatchEventually(storage, 4, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    backend->BlockTxn();
    auto res = service.BatchReplicaClear({key}, mounted.client_id,
                                         "clear_finalize_seg1");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_clear_segment_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 5, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key =
            "after_clear_segment_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, BatchEvictWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_batch_evict_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_evict_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_evict_key";
    PutObjectOnSegment(service, mounted.client_id, key, "batch_evict_segment");
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, BatchEvictReleasesMemoryAfterDurable) {
    const std::string cluster_id = "test_batch_record_batch_evict_finalize";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(50)
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(cluster_id)
            .set_oplog_batch_max_entries(1)
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile({{kDefaultTenant, 1024}}))
            .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_evict_finalize_seg");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_evict_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_evict_finalize_seg");
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    backend->BlockTxn();
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string before_finalize_key = "before_batch_evict_finalize_key";
    auto before_finalize = service.PutStart(
        mounted.client_id, before_finalize_key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 3, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key = "after_batch_evict_finalize_key";
        auto after_finalize =
            service.PutStart(mounted.client_id, after_finalize_key,
                             kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}

TEST_F(MasterServiceHATest, EvictDiskReplicaWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_disk_evict_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_disk_evict_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_disk_evict_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_disk_evict_segment");
    ReadBatchEventually(storage, 2, batch);

    Replica local_disk_replica(mounted.client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service
                    .AddReplica(mounted.client_id, key, kDefaultTenant,
                                local_disk_replica)
                    .has_value());
    ReadBatchEventually(storage, 3, batch);

    ASSERT_TRUE(service
                    .EvictDiskReplica(mounted.client_id, key, kDefaultTenant,
                                      ReplicaType::LOCAL_DISK)
                    .has_value());
    ReadBatchEventually(storage, 4, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(4u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest, EvictDiskReplicaReleasesLocalDiskAfterDurable) {
    const std::string cluster_id =
        "test_batch_record_disk_evict_finalize_cluster";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_enable_offload(true)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    const std::string segment_name = "batch_disk_evict_finalize_segment";
    auto mounted = PrepareSimpleSegment(service, segment_name);
    ASSERT_TRUE(service
                    .MountLocalDiskSegment(mounted.client_id,
                                           /*enable_offloading=*/false)
                    .has_value());
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_disk_evict_finalize_key";
    PutObjectOnSegment(service, mounted.client_id, key, segment_name);
    ReadBatchEventually(storage, 2, batch);

    Replica local_disk_replica(mounted.client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service
                    .AddReplica(mounted.client_id, key, kDefaultTenant,
                                local_disk_replica)
                    .has_value());
    ReadBatchEventually(storage, 3, batch);
    SetLocalDiskUsedBytesForTesting(service, mounted.client_id, 1024);

    backend->BlockTxn();
    ASSERT_TRUE(service
                    .EvictDiskReplica(mounted.client_id, key, kDefaultTenant,
                                      ReplicaType::LOCAL_DISK)
                    .has_value());
    EXPECT_EQ(1024, GetLocalDiskUsedBytesForTesting(service, segment_name));

    auto before_finalize = service.GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(before_finalize.has_value());
    EXPECT_FALSE(std::any_of(before_finalize->replicas.begin(),
                             before_finalize->replicas.end(),
                             [](const Replica::Descriptor& desc) {
                                 return desc.is_local_disk_replica();
                             }));

    backend->AllowTxn();
    ReadBatchEventually(storage, 4, batch);
    EXPECT_EQ(0, GetLocalDiskUsedBytesForTesting(service, segment_name));
}

#ifdef USE_NOF
TEST_F(MasterServiceHATest, NoFBatchEvictWritesBatchRecordOpLog) {
    const std::string cluster_id = "test_batch_record_nof_evict_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    NoFSegment nof_segment =
        MakeNoFSegment("batch_nof_evict_segment", "batch_nof_evict_endpoint");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountNoFSegment(nof_segment, client_id).has_value());

    const std::string key = "batch_nof_evict_key";
    ReplicateConfig config;
    config.nof_replica_num = 1;
    auto put_start =
        service.PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_TRUE(
        service.PutEnd(client_id, key, kDefaultTenant, ReplicaType::NOF_SSD)
            .has_value());

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    service.RunNoFBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                       /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 2, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(2u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest, NoFBatchEvictReleasesNoFSpaceAfterDurable) {
    const std::string cluster_id =
        "test_batch_record_nof_evict_finalize_cluster";
    auto backend = std::make_shared<BlockingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    NoFSegment nof_segment = MakeNoFSegment("batch_nof_evict_finalize_segment",
                                            "batch_nof_evict_finalize_endpoint",
                                            kDefaultSegmentBase, 1024);
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.nof_replica_num = 1;
    const std::string key = "batch_nof_evict_finalize_key";
    ASSERT_TRUE(service.PutStart(client_id, key, kDefaultTenant, 1024, config)
                    .has_value());
    ASSERT_TRUE(
        service.PutEnd(client_id, key, kDefaultTenant, ReplicaType::NOF_SSD)
            .has_value());

    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    backend->BlockTxn();
    service.RunNoFBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                       /*evict_ratio_lowerbound=*/1.0);
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    const std::string before_finalize_key =
        "before_batch_nof_evict_finalize_key";
    auto before_finalize = service.PutStart(client_id, before_finalize_key,
                                            kDefaultTenant, 1024, config);
    EXPECT_FALSE(before_finalize.has_value());

    backend->AllowTxn();
    ReadBatchEventually(storage, 2, batch);

    if (!before_finalize.has_value()) {
        const std::string after_finalize_key =
            "after_batch_nof_evict_finalize_key";
        auto after_finalize = service.PutStart(client_id, after_finalize_key,
                                               kDefaultTenant, 1024, config);
        EXPECT_TRUE(after_finalize.has_value())
            << toString(after_finalize.error());
    }
}
#endif

TEST_F(MasterServiceHATest, PutStartExpiredOverwriteWritesBatchRecordOpLog) {
    const std::string cluster_id =
        "test_batch_record_put_start_cleanup_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    [[maybe_unused]] const auto mounted =
        PrepareSimpleSegment(service, "batch_put_start_cleanup_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string key = "batch_put_start_cleanup_key";
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.PutStart(client_id, key, kDefaultTenant, 1024, config)
                    .has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    const UUID next_client_id = generate_uuid();
    ASSERT_TRUE(
        service.PutStart(next_client_id, key, kDefaultTenant, 1024, config)
            .has_value());
    ReadBatchEventually(storage, 2, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::REMOVE, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(2u, batch.entries[0].sequence_id);
}

TEST_F(MasterServiceHATest,
       DiscardExpiredProcessingReplicasWritesBatchRecordOpLog) {
    const std::string cluster_id =
        "test_batch_record_discard_processing_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    [[maybe_unused]] const auto mounted =
        PrepareSimpleSegment(service, "batch_discard_processing_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    ReplicateConfig config;
    config.replica_num = 1;
    const UUID client_id = generate_uuid();
    const std::string key = "batch_discard_processing_key";
    ASSERT_TRUE(service.PutStart(client_id, key, kDefaultTenant, 1024, config)
                    .has_value());

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    ASSERT_TRUE(
        service.AddReplica(client_id, key, kDefaultTenant, local_disk_replica)
            .has_value());
    ReadBatchEventually(storage, 2, batch);

    std::this_thread::sleep_for(std::chrono::milliseconds(2100));
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(3u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest,
       DiscardExpiredReplicationTaskWritesBatchRecordOpLog) {
    const std::string cluster_id =
        "test_batch_record_discard_replication_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_enable_oplog(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_batch_max_entries(1)
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto src = PrepareSimpleSegment(service, "batch_replication_src");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_discard_replication_key";
    PutObjectOnSegment(service, src.client_id, key, "batch_replication_src");
    ReadBatchEventually(storage, 2, batch);

    [[maybe_unused]] const auto target =
        PrepareSimpleSegment(service, "batch_replication_target",
                             kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_TRUE(service
                    .MoveStart(src.client_id, key, kDefaultTenant,
                               "batch_replication_src",
                               "batch_replication_target")
                    .has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(2100));
    service.RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                    /*evict_ratio_lowerbound=*/1.0);
    ReadBatchEventually(storage, 4, batch);

    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(4u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
