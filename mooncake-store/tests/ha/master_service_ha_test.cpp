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
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <unistd.h>

#include "hot_standby_service.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/mock_oplog_store.h"
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

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        policy_files_.clear();
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
            .set_cluster_id("test_cluster")
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile(tenant_quotas))
            .build();
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

    static uint64_t LastOpLogSequenceForTesting(const MasterService& service) {
        return service.oplog_manager_.GetLastSequenceId();
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

    static tl::expected<void, ErrorCode> ClassifyReplicaReadinessForTesting(
        MasterService& service, const std::string& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{tenant_id, key});
        if (!accessor.Exists()) {
            return service.ClassifyReplicaReadiness(nullptr);
        }
        return service.ClassifyReplicaReadiness(&accessor.Get());
    }

    std::vector<std::string> policy_files_;
    int next_policy_file_{0};
};

class MasterServiceBatchRecordE2ETest : public MasterServiceHATest {};

TEST_F(MasterServiceHATest, ClassifyReplicaReadinessCoversReplicaStates) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(false)
                              .build();
    MasterService service(service_config);
    auto mounted = PrepareSimpleSegment(service, "readiness_segment");

    const std::string complete_key = "readiness_complete_key";
    PutObjectOnSegment(service, mounted.client_id, complete_key,
                       "readiness_segment");
    EXPECT_TRUE(ClassifyReplicaReadinessForTesting(service, kDefaultTenant,
                                                   complete_key)
                    .has_value());

    const std::string removed_key = "readiness_removed_key";
    PutObjectOnSegment(service, mounted.client_id, removed_key,
                       "readiness_segment");
    MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant,
                                           removed_key);
    auto removed = ClassifyReplicaReadinessForTesting(service, kDefaultTenant,
                                                      removed_key);
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, removed.error());

    const std::string processing_key = "readiness_processing_key";
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service
                    .PutStart(mounted.client_id, processing_key, kDefaultTenant,
                              1024, config)
                    .has_value());
    auto processing = ClassifyReplicaReadinessForTesting(
        service, kDefaultTenant, processing_key);
    ASSERT_FALSE(processing.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, processing.error());

    auto missing = ClassifyReplicaReadinessForTesting(service, kDefaultTenant,
                                                      "readiness_missing_key");
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, missing.error());
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

TEST_F(MasterServiceHATest, ExistKeyClassifiesRemovedReplicaStates) {
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
    ASSERT_FALSE(processing.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, processing.error());

    auto missing =
        service.ExistKey("exist_readiness_missing_key", kDefaultTenant);
    ASSERT_TRUE(missing.has_value());
    EXPECT_FALSE(missing.value());
}

TEST_F(MasterServiceHATest, BatchExistKeyClassifiesRemovedReplicaStates) {
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
    ASSERT_FALSE(results[2].has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, results[2].error());
    ASSERT_TRUE(results[3].has_value());
    EXPECT_FALSE(results[3].value());
}

TEST_F(MasterServiceHATest,
       BatchRecordWriterInitMigratesLegacyLatestAndSetsSequence) {
    const std::string cluster_id = "test_batch_record_init_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    ASSERT_EQ(ErrorCode::OK,
              backend->Put("/oplog/" + cluster_id + "/latest", "42"));

    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
                              .set_oplog_batch_max_entries(8)
                              .build();
    MasterService service(service_config);

    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    std::string encoded_prefix;
    ASSERT_EQ(ErrorCode::OK,
              backend->Get(BuildDurablePrefixKey(cluster_id), encoded_prefix));
    DurablePrefix prefix;
    ASSERT_TRUE(DecodeDurablePrefix(encoded_prefix, &prefix));
    EXPECT_EQ(0u, prefix.batch_id);
    EXPECT_EQ(42u, prefix.last_seq);
    EXPECT_EQ(42u, LastOpLogSequenceForTesting(service));
}

TEST_F(MasterServiceHATest, BatchRecordSubmissionHelpersUseOrderedWriter) {
    const std::string cluster_id = "test_batch_record_helpers_cluster";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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

TEST_F(MasterServiceBatchRecordE2ETest,
       PrimaryWritesBatchRecordAndDurablePrefix) {
    const std::string cluster_id = "test_batch_record_e2e_primary";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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

TEST_F(MasterServiceBatchRecordE2ETest,
       BatchRecordWritesDoNotUpdateLegacyLatest) {
    const std::string cluster_id = "test_batch_record_e2e_legacy_latest";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    ASSERT_EQ(ErrorCode::OK,
              backend->Put("/oplog/" + cluster_id + "/latest", "42"));

    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted =
        PrepareSimpleSegment(service, "batch_e2e_legacy_latest_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(43u, batch.entries[0].sequence_id);

    const std::string key = "batch_e2e_legacy_latest_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_legacy_latest_segment");
    ReadBatchEventually(storage, 2, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(44u, batch.entries[0].sequence_id);

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(44u, prefix.last_seq);

    std::string legacy_latest;
    ASSERT_EQ(ErrorCode::OK,
              backend->Get("/oplog/" + cluster_id + "/latest", legacy_latest));
    EXPECT_EQ("42", legacy_latest);
}

TEST_F(MasterServiceBatchRecordE2ETest, StandbyAppliesPrimaryBatchRecords) {
    const std::string cluster_id = "test_batch_record_e2e_standby_apply";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
    EXPECT_FALSE(result.used_legacy_path);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(standby_metadata.Exists(kDefaultTenant, key));
}

TEST_F(MasterServiceBatchRecordE2ETest, LegacyLatestCutoverThenBatchRecords) {
    const std::string cluster_id = "test_batch_record_e2e_legacy_cutover";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    ASSERT_EQ(ErrorCode::OK,
              backend->Put("/oplog/" + cluster_id + "/latest", "42"));

    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto mounted = PrepareSimpleSegment(service, "batch_e2e_cutover_segment");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(43u, batch.entries[0].sequence_id);

    const std::string key = "batch_e2e_cutover_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "batch_e2e_cutover_segment");
    ReadBatchEventually(storage, 2, batch);
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(44u, batch.entries[0].sequence_id);

    MockMetadataStore standby_metadata;
    OpLogApplier applier(&standby_metadata, cluster_id);
    applier.Recover(42);
    OpLogBatchStandbyReader reader(cluster_id, *backend, applier);

    auto result = reader.PollOnce();
    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_FALSE(result.used_legacy_path);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(45u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(standby_metadata.Exists(kDefaultTenant, key));
}

TEST_F(MasterServiceBatchRecordE2ETest, PromotionCatchesUpToDurablePrefix) {
    const std::string cluster_id = "test_batch_record_e2e_promotion_catchup";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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

    const auto standby_dir = std::filesystem::temp_directory_path() /
                             ("mooncake_batch_promotion_" +
                              std::to_string(::getpid()) + "_" + cluster_id);
    std::filesystem::create_directories(standby_dir);

    HotStandbyConfig standby_config;
    standby_config.enable_verification = false;
    standby_config.max_replication_lag_entries = 1000;
    standby_config.oplog_store_type = OpLogStoreType::LOCAL_FS;
    standby_config.oplog_store_root_dir = standby_dir.string();
    standby_config.oplog_poll_interval_ms = 50;
    standby_config.fail_closed_on_incomplete_catch_up = true;
    standby_config.fail_closed_on_unresolved_gaps = true;

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

    std::error_code ec;
    std::filesystem::remove_all(standby_dir, ec);
}

TEST_F(MasterServiceBatchRecordE2ETest,
       BackendFailureStopsNewBatchReservations) {
    const std::string cluster_id = "test_batch_record_e2e_backend_failure";
    auto backend = std::make_shared<FailingBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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

TEST_F(MasterServiceBatchRecordE2ETest,
       RemoveFailsBeforeMutationWhenBatchReservationUnavailable) {
    const std::string cluster_id = "test_batch_record_remove_reserve_full";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
       ProcessingOnlyObjectReturnsReplicaIsNotReady) {
    const std::string cluster_id = "test_batch_record_e2e_processing_only";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
    ASSERT_FALSE(exists.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, exists.error());
}

TEST_F(MasterServiceBatchRecordE2ETest,
       OffloadAndPromotionSuccessRemainFunctional) {
    const std::string cluster_id = "test_batch_record_e2e_offload_promotion";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
            .set_cluster_id(cluster_id)
            .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
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

TEST_F(MasterServiceHATest, LegacySubmissionHelpersDelegateToOpLogStore) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    MasterService service(service_config);
    auto mock_store = std::make_shared<MockOpLogStore>();
    service.SetOpLogStoreForTesting(mock_store);

    auto visible = AppendVisibleForTesting(service, OpType::PUT_END, "tenant",
                                           "visible_key", "visible_payload");
    ASSERT_TRUE(visible.has_value());
    EXPECT_EQ(1u, visible.value());

    bool finalized = false;
    auto finalized_entry = AppendFinalizeForTesting(
        service, OpType::REMOVE, "tenant", "remove_key", {},
        [&finalized](const OpLogEntry& durable_entry) {
            finalized = true;
            EXPECT_EQ(2u, durable_entry.sequence_id);
            EXPECT_EQ("remove_key", durable_entry.object_key);
        });
    ASSERT_TRUE(finalized_entry.has_value());
    EXPECT_TRUE(finalized);
    EXPECT_EQ(2u, finalized_entry->sequence_id);
    EXPECT_EQ(2u, mock_store->EntryCount());
}

TEST_F(MasterServiceHATest, BatchRecordModeRejectsDirectLegacyOpLogWrites) {
    const std::string cluster_id = "test_batch_record_no_legacy_writes";
    auto backend = std::make_shared<FakeBatchHaKvBackend>();
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id(cluster_id)
                              .set_oplog_store_type("etcd_batch_record")
                              .set_oplog_batch_max_entries(1)
                              .build();
    MasterService service(service_config);
    ASSERT_EQ(ErrorCode::OK, service.SetBatchOpLogBackendForTesting(backend));

    auto legacy_store = std::make_shared<MockOpLogStore>();
    service.SetOpLogStoreForTesting(legacy_store);

    auto source = PrepareSimpleSegment(service, "batch_no_legacy_source");
    OpLogBatchStorage storage(cluster_id, *backend);
    OpLogBatchRecord batch;
    ReadBatchEventually(storage, 1, batch);

    const std::string key = "batch_no_legacy_key";
    PutObjectOnSegment(service, source.client_id, key,
                       "batch_no_legacy_source");
    ReadBatchEventually(storage, 2, batch);

    [[maybe_unused]] const auto target =
        PrepareSimpleSegment(service, "batch_no_legacy_target",
                             kDefaultSegmentBase + kDefaultSegmentSize);
    ReadBatchEventually(storage, 3, batch);

    ASSERT_TRUE(service
                    .CopyStart(source.client_id, key, kDefaultTenant,
                               "batch_no_legacy_source",
                               {"batch_no_legacy_target"})
                    .has_value());
    ASSERT_TRUE(
        service.CopyEnd(source.client_id, key, kDefaultTenant).has_value());
    ReadBatchEventually(storage, 4, batch);

    EXPECT_EQ(0u, legacy_store->EntryCount());
    ASSERT_EQ(1u, batch.entries.size());
    EXPECT_EQ(OpType::PUT_END, batch.entries[0].op_type);
    EXPECT_EQ(kDefaultTenant, batch.entries[0].tenant_id);
    EXPECT_EQ(key, batch.entries[0].object_key);
    EXPECT_EQ(4u, batch.entries[0].sequence_id);
    EXPECT_FALSE(batch.entries[0].payload.empty());
}

TEST_F(MasterServiceHATest, RestoreFromStandbySnapshotClearsInvalidEndpoints) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    MasterService service(service_config);
    service.SetOpLogStoreForTesting(std::make_shared<MockOpLogStore>());

    const std::string endpoint = "restored_segment";
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(service, endpoint);

    service.RestoreFromStandbySnapshot(
        {MakeStandbyObject("stale_restore_key", endpoint)}, 1, {});

    service.RestoreFromStandbySnapshot(
        {MakeStandbyObject("valid_restore_key", endpoint)}, 2,
        {MakeStandbyMemorySegment(endpoint)});

    auto valid_result =
        service.GetReplicaList("valid_restore_key", kDefaultTenant);
    ASSERT_TRUE(valid_result.has_value()) << toString(valid_result.error());
    ASSERT_EQ(1u, valid_result->replicas.size());
    EXPECT_EQ(endpoint, valid_result->replicas.front()
                            .get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_);
}

// Test that RemoveByRegex publishes REMOVE OpLog entries for matched keys.
TEST_F(MasterServiceHATest, RemoveByRegexPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back("regex_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire so RemoveByRegex can remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    size_t entries_before = mock_store->EntryCount();
    auto res =
        service->RemoveByRegex("^regex_key_", kDefaultTenant, /*force=*/true);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(5, res.value());

    size_t entries_after = mock_store->EntryCount();
    EXPECT_EQ(entries_before + 5, entries_after);

    // Verify all entries are REMOVE ops for the expected keys.
    std::vector<std::string> removed_keys;
    uint64_t max_seq = 0;
    EXPECT_EQ(ErrorCode::OK, mock_store->GetMaxSequenceId(max_seq));
    for (uint64_t seq = entries_before + 1; seq <= max_seq; ++seq) {
        OpLogEntry entry;
        EXPECT_EQ(ErrorCode::OK, mock_store->ReadOpLog(seq, entry));
        EXPECT_EQ(OpType::REMOVE, entry.op_type);
        removed_keys.push_back(entry.object_key);
    }
    EXPECT_EQ(5u, removed_keys.size());
    for (const auto& key : keys) {
        EXPECT_TRUE(std::find(removed_keys.begin(), removed_keys.end(), key) !=
                    removed_keys.end())
            << "Missing REMOVE OpLog for key=" << key;
    }
}

// Test that BatchRemove skips erase when OpLog persist fails.
TEST_F(MasterServiceHATest, BatchRemovePersistFailureSkipsErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 3; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Make the OpLog store fail on write.
    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto results = service->BatchRemove(keys, kDefaultTenant, /*force=*/true);
    ASSERT_EQ(keys.size(), results.size());

    // All results should report failure because persist failed.
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_FALSE(results[i].has_value())
            << "Expected failure for key=" << keys[i];
    }

    // Keys should still exist in metadata because erase was skipped.
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key, kDefaultTenant);
        ASSERT_TRUE(exist.has_value());
        EXPECT_TRUE(exist.value()) << "Key should still exist: " << key;
    }

    // Restore the store and retry — removal should succeed now.
    mock_store->SetWriteError(ErrorCode::OK);
    results = service->BatchRemove(keys, kDefaultTenant, /*force=*/true);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Retry should succeed for key=" << keys[i];
    }
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key, kDefaultTenant);
        ASSERT_TRUE(exist.has_value());
        EXPECT_FALSE(exist.value()) << "Key should be removed: " << key;
    }
}

// PutRevoke on an object with a PROCESSING MEMORY replica publishes REMOVE
// OpLog.
TEST_F(MasterServiceHATest, PutRevokeSingleReplicaPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_single_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start =
        service->PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    auto res =
        service->PutRevoke(client_id, key, kDefaultTenant, ReplicaType::MEMORY);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
}

TEST_F(MasterServiceHATest, ReplicaMarkRemovedKeepsDescriptorState) {
    Replica replica(generate_uuid(), 1024, "removed_state_endpoint",
                    ReplicaStatus::COMPLETE);

    const ReplicaID id = replica.id();
    const auto descriptor = replica.get_descriptor();
    ASSERT_EQ(ReplicaStatus::COMPLETE, replica.status());

    replica.mark_removed();

    EXPECT_EQ(ReplicaStatus::REMOVED, replica.status());
    EXPECT_EQ(id, replica.id());
    EXPECT_EQ(1024u,
              replica.get_descriptor().get_local_disk_descriptor().object_size);
    EXPECT_EQ(descriptor.get_local_disk_descriptor().client_id,
              replica.get_descriptor().get_local_disk_descriptor().client_id);
}

TEST_F(MasterServiceHATest,
       FinalizeRemovedReplicasAfterDurableErasesRemovedReplicas) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_cluster_id("test_cluster")
                              .build();
    MasterService service(service_config);

    auto mounted = PrepareSimpleSegment(service, "finalize_removed_segment");
    const std::string key = "finalize_removed_key";
    PutObjectOnSegment(service, mounted.client_id, key,
                       "finalize_removed_segment");

    const auto removed_ids =
        MarkCompletedReplicasRemovedForTesting(service, kDefaultTenant, key);
    ASSERT_EQ(1u, removed_ids.size());

    OpLogEntry durable_entry;
    durable_entry.op_type = OpType::REMOVE;
    durable_entry.tenant_id = kDefaultTenant;
    durable_entry.object_key = key;
    durable_entry.sequence_id = 1;

    FinalizeRemovedReplicasForTesting(service, durable_entry, removed_ids);
    EXPECT_FALSE(service.GetReplicaList(key, kDefaultTenant).has_value());

    FinalizeRemovedReplicasForTesting(service, durable_entry, removed_ids);
}

// PutRevoke(MEMORY) on an object with PROCESSING MEMORY + LOCAL_DISK publishes
// PUT_END with the LOCAL_DISK descriptor.
TEST_F(MasterServiceHATest, PutRevokeKeepsLocalDiskPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_mixed_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start =
        service->PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res =
        service->AddReplica(client_id, key, kDefaultTenant, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    auto res =
        service->PutRevoke(client_id, key, kDefaultTenant, ReplicaType::MEMORY);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// EvictDiskReplica(LOCAL_DISK) on an object with MEMORY + LOCAL_DISK publishes
// PUT_END with the MEMORY descriptor.
TEST_F(MasterServiceHATest, EvictDiskReplicaPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "evict_disk_key";
    PutObject(*service, client_id, key);

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res =
        service->AddReplica(client_id, key, kDefaultTenant, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    auto res = service->EvictDiskReplica(client_id, key, kDefaultTenant,
                                         ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// BatchReplicaClear with empty segment_name clears all replicas and publishes
// REMOVE OpLog.
TEST_F(MasterServiceHATest, BatchReplicaClearAllPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "batch_clear_all_key";
    PutObject(*service, client_id, key);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    auto res = service->BatchReplicaClear({key}, client_id, "");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
}

// CopyEnd publishes PUT_END OpLog with the updated replica set.
TEST_F(MasterServiceHATest, CopyEndPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "copy_end_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start =
        service->CopyStart(client_id, key, kDefaultTenant, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    auto res = service->CopyEnd(client_id, key, kDefaultTenant);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// MoveEnd publishes PUT_END OpLog with the updated replica set (source
// removed).
TEST_F(MasterServiceHATest, MoveEndPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "move_end_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto move_start =
        service->MoveStart(client_id, key, kDefaultTenant, "seg1", "seg2");
    ASSERT_TRUE(move_start.has_value());

    auto res = service->MoveEnd(client_id, key, kDefaultTenant);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// BatchReplicaClear on one segment keeps replicas on other segments and
// publishes PUT_END.
TEST_F(MasterServiceHATest,
       BatchReplicaClearSegmentKeepsOtherSegmentsPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "batch_clear_segment_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start =
        service->CopyStart(client_id, key, kDefaultTenant, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    auto copy_end = service->CopyEnd(client_id, key, kDefaultTenant);
    ASSERT_TRUE(copy_end.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    auto res = service->BatchReplicaClear({key}, client_id, "seg1");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// CopyEnd must NOT mark target replicas COMPLETE or erase the task when
// OpLog persist fails.
TEST_F(MasterServiceHATest, CopyEndPersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "copy_end_persist_fail_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start =
        service->CopyStart(client_id, key, kDefaultTenant, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res = service->CopyEnd(client_id, key, kDefaultTenant);
    EXPECT_FALSE(res.has_value())
        << "CopyEnd must return error when OpLog persist fails";

    // Target replicas must remain PROCESSING (not COMPLETE).
    auto list = service->GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(list.has_value());
    // Only the original COMPLETE memory replica should be in the published
    // descriptor list — target replicas are still PROCESSING and excluded.
    EXPECT_EQ(1u, list->replicas.size())
        << "Only the original source replica should be COMPLETE";

    // Retrying after restoring persist should succeed.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry = service->CopyEnd(client_id, key, kDefaultTenant);
    EXPECT_TRUE(retry.has_value())
        << "CopyEnd retry should succeed after persist is restored";
}

// MoveEnd must NOT decrement source refcnt or mark target COMPLETE when
// OpLog persist fails.
TEST_F(MasterServiceHATest, MoveEndPersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "move_end_persist_fail_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto move_start =
        service->MoveStart(client_id, key, kDefaultTenant, "seg1", "seg2");
    ASSERT_TRUE(move_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res = service->MoveEnd(client_id, key, kDefaultTenant);
    EXPECT_FALSE(res.has_value())
        << "MoveEnd must return error when OpLog persist fails";

    // Source must still be COMPLETE and present (PopReplicas was not called),
    // and target must still be PROCESSING (mark_complete was not called).
    // The visible descriptor list contains only COMPLETE replicas; therefore
    // exactly the original source descriptor should appear.
    auto list = service->GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(1u, list->replicas.size())
        << "Source must still be present and target still PROCESSING";

    // Retrying after restoring persist should succeed.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry = service->MoveEnd(client_id, key, kDefaultTenant);
    EXPECT_TRUE(retry.has_value())
        << "MoveEnd retry should succeed after persist is restored";
}

// PutRevoke must NOT mutate metadata when OpLog persist fails (also
// implicitly checks H.1 metric ordering: dec_mem_cache_nums must not run
// when persist fails). The PROCESSING replica should still be revokable
// after we restore persist.
TEST_F(MasterServiceHATest, PutRevokePersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_persist_fail_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start =
        service->PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res =
        service->PutRevoke(client_id, key, kDefaultTenant, ReplicaType::MEMORY);
    EXPECT_FALSE(res.has_value())
        << "PutRevoke must return error when OpLog persist fails";

    // Restore persist; PutRevoke must succeed because the PROCESSING replica
    // was NOT erased on the failed attempt.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry =
        service->PutRevoke(client_id, key, kDefaultTenant, ReplicaType::MEMORY);
    EXPECT_TRUE(retry.has_value())
        << "PutRevoke retry must succeed because the PROCESSING replica "
           "was preserved after persist failure";
}

// ===== Step 2: strong-consistency eviction =====

// BatchEvict must NOT erase MEMORY replicas when OpLog persist fails.
TEST_F(MasterServiceHATest, BatchEvictPersistFailureSkipsMemReplicaErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back("evict_persist_fail_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }
    // Wait for leases to expire so they become evictable.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Aggressive ratios so all eligible objects are picked.
    service->RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                     /*evict_ratio_lowerbound=*/1.0);

    // Memory replicas must remain because persist failed.
    for (const auto& key : keys) {
        auto list = service->GetReplicaList(key, kDefaultTenant);
        ASSERT_TRUE(list.has_value())
            << "Key must still be retrievable: " << key;
        bool has_memory = false;
        for (const auto& desc : list->replicas) {
            if (desc.is_memory_replica()) {
                has_memory = true;
                break;
            }
        }
        EXPECT_TRUE(has_memory)
            << "Memory replica must remain after persist failure: " << key;
    }
}

// ===== Step 3: direct-erase + stale-cleanup + plan/apply =====

// PutStart against an existing PROCESSING-only key triggers the overwrite-
// REMOVE path once put_start_discard_timeout_sec elapses. With persist
// failing, the path must NOT erase the metadata or pop replicas; the next
// PutStart attempt (after the same timeout) must still see the old key.
TEST_F(MasterServiceHATest, PutStartOverwritePersistFailureSkipsErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_start_overwrite_persist_fail_key";

    // PutStart but never PutEnd — leaves a PROCESSING-only metadata.
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start =
        service->PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    // Wait for put_start_discard_timeout to elapse.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Second PutStart triggers the overwrite-REMOVE branch. With persist
    // failing it must return error (not silently drop the old metadata).
    const UUID client_id2 = generate_uuid();
    auto retry_start =
        service->PutStart(client_id2, key, kDefaultTenant, 1024, config);
    EXPECT_FALSE(retry_start.has_value())
        << "PutStart overwrite must return error when REMOVE persist fails";

    // Restore persist; the overwrite must now succeed and the new
    // PutStart must allocate a fresh PROCESSING replica for client_id2.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry2 =
        service->PutStart(client_id2, key, kDefaultTenant, 1024, config);
    EXPECT_TRUE(retry2.has_value())
        << "PutStart overwrite must succeed once persist is restored";
}

// DiscardExpiredProcessingReplicas (Part 1) drops PROCESSING replicas of
// PutStart-expired keys. When the key has been published to standby
// (had_complete_replica == true via an existing LOCAL_DISK replica),
// persist failure must NOT pop the PROCESSING memory replica locally.
TEST_F(MasterServiceHATest,
       DiscardExpiredProcessingReplicasPersistFailureSkipsPop) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "discard_expired_persist_fail_key";

    // PutStart but never PutEnd — leaves a PROCESSING memory replica.
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start =
        service->PutStart(client_id, key, kDefaultTenant, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    // AddReplica a COMPLETE LOCAL_DISK so the metadata has both a
    // PROCESSING memory replica AND a COMPLETE local-disk descriptor.
    // had_complete_replica will be true when the reaper runs.
    Replica local_disk_replica(client_id, 1024, "ld_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res =
        service->AddReplica(client_id, key, kDefaultTenant, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    // Wait for put_start_release_timeout to elapse so Part 1 considers
    // the PROCESSING replica expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(2100));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Drive an eviction cycle which calls DiscardExpiredProcessingReplicas
    // up-front. With persist failing, the PROCESSING memory replica must
    // NOT be popped from metadata.
    service->RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                     /*evict_ratio_lowerbound=*/1.0);

    // Restore persist and try PutEnd. If the reaper popped the
    // PROCESSING memory replica, PutEnd will be a no-op and
    // GetReplicaList will only see the LOCAL_DISK descriptor; if the
    // PROCESSING memory replica was preserved, PutEnd marks it COMPLETE
    // and GetReplicaList will return BOTH descriptors.
    mock_store->SetWriteError(ErrorCode::OK);
    auto end_res =
        service->PutEnd(client_id, key, kDefaultTenant, ReplicaType::MEMORY);
    ASSERT_TRUE(end_res.has_value());

    auto after = service->GetReplicaList(key, kDefaultTenant);
    ASSERT_TRUE(after.has_value());
    bool has_memory = false;
    bool has_local_disk = false;
    for (const auto& desc : after->replicas) {
        if (desc.is_memory_replica()) has_memory = true;
        if (desc.is_local_disk_replica()) has_local_disk = true;
    }
    EXPECT_TRUE(has_memory)
        << "Memory replica must still be present (and now COMPLETE) after "
           "PutEnd, proving the reaper preserved the PROCESSING replica on "
           "persist failure";
    EXPECT_TRUE(has_local_disk) << "LOCAL_DISK replica must still be present";
}

// ===== Step 4: SEGMENT_UNMOUNT retry-on-failure =====
//
// The F fix (UnmountSegment / MountSegment / ReMountSegment now use
// PersistSegmentOpForHAOrEnqueue: durable persist up-front, enqueue the
// same OpLogEntry on failure preserving its sequence_id) is verified
// by code review rather than a unit test. A direct test that drives
// UnmountSegment after MountSegment in this fixture reliably triggers
// "Resource deadlock avoided" inside the segment manager's lock graph
// when `enable_ha_=true` (etcd backend init failure path), even when
// glog init/shutdown is moved to SetUpTestSuite/TearDownTestSuite.
// The behaviour is independent of the F changes and pre-dates them; a
// proper test would need an integration harness that exercises the
// retry queue across primary/standby boundaries (covered by
// localfs_hot_standby_integration_test).

// AddReplica publishes OpLog entry with the non-default tenant_id.
TEST_F(MasterServiceHATest, AddReplicaPublishesTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "tenant_add_replica_key";

    // Create the object in tenant_a so the subsequent AddReplica operates
    // on the same tenant.
    PutObjectWithTenant(*service, client_id, key, tenant);

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto res = service->AddReplica(client_id, key, tenant, local_disk_replica);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// PutRevoke(MEMORY) on a tenant_a object publishes REMOVE OpLog with
// non-default tenant_id.
TEST_F(MasterServiceHATest, PutRevokeMemoryPublishesRemoveTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "tenant_put_revoke_key";

    // PutStart only — replica is still in PROCESSING state, which is the
    // pre-condition for PutRevoke(MEMORY) to be accepted.
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service->PutStart(client_id, key, tenant, /*slice_length=*/1024, config)
            .has_value());

    auto res = service->PutRevoke(client_id, key, tenant, ReplicaType::MEMORY);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// EvictDiskReplica with a remaining MEMORY replica in tenant_a publishes
// PUT_END OpLog with non-default tenant_id (not REMOVE — the object stays
// alive in memory).
TEST_F(MasterServiceHATest, EvictDiskReplicaLocalDiskPublishesPutEndTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "tenant_evict_disk_key";

    // PutEnd creates the MEMORY replica in tenant_a; AddReplica adds the
    // LOCAL_DISK replica in tenant_a. After eviction the object remains
    // alive in MEMORY, so the OpLog must be PUT_END, not REMOVE.
    PutObjectWithTenant(*service, client_id, key, tenant);

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service->AddReplica(client_id, key, tenant, local_disk_replica)
                    .has_value());

    auto res = service->EvictDiskReplica(client_id, key, tenant,
                                         ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// CopyEnd within tenant_a publishes PUT_END OpLog with non-default
// tenant_id (not "default" — the bug is that the 3-arg AppendOpLog overload
// publishes under "default", which would cause the standby to apply it
// against the wrong tenant).
TEST_F(MasterServiceHATest, CopyEndPublishesPutEndTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const std::string tenant = "tenant_a";
    const std::string key = "tenant_copy_end_key";
    const UUID client_id = generate_uuid();

    // Create source object in tenant_a on seg1.
    PutObjectOnSegmentWithTenant(*service, client_id, key, "seg1", tenant);

    // Begin cross-segment copy within tenant_a.
    auto copy_start =
        service->CopyStart(client_id, key, tenant, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    // Complete the copy under tenant_a — this is where the bug manifests.
    auto copy_end = service->CopyEnd(client_id, key, tenant);
    ASSERT_TRUE(copy_end.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// MoveEnd within tenant_a publishes PUT_END OpLog with non-default
// tenant_id (not "default" — the bug is that the 3-arg AppendOpLog overload
// publishes under "default", which would cause the standby to apply it
// against the wrong tenant). Structurally identical to the CopyEnd case.
TEST_F(MasterServiceHATest, MoveEndPublishesPutEndTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    const std::string tenant = "tenant_a";
    const std::string key = "tenant_move_end_key";
    const UUID client_id = generate_uuid();

    // Create source object in tenant_a on seg1.
    PutObjectOnSegmentWithTenant(*service, client_id, key, "seg1", tenant);

    // Begin cross-segment move within tenant_a.
    auto move_start =
        service->MoveStart(client_id, key, tenant, "seg1", "seg2");
    ASSERT_TRUE(move_start.has_value());

    // Complete the move under tenant_a — this is where the bug manifests.
    auto move_end = service->MoveEnd(client_id, key, tenant);
    ASSERT_TRUE(move_end.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    // MoveEnd publishes PUT_END.
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// NotifyPromotionSuccess within tenant_a publishes PUT_END OpLog with
// non-default tenant_id. The HA promotion path is exercised when a standby
// promotes to master: the leaseholder replica is flipped COMPLETE and a
// PUT_END is broadcast to other standbys. The 3-arg AppendOpLog overload
// inside NotifyPromotionSuccess would publish under "default", causing
// standbys to apply it against the wrong tenant.
//
// Note on fixture setup: the production on-hit admission path
// (TryPushPromotionQueue) is gated to "default" tenant with a comment
// stating promotion-on-hit does not support non-default tenants. So this
// test seeds the per-tenant promotion_tasks entry directly via friend
// access (MasterServiceHATest is friended), bypassing the admission gate
// to isolate the OpLog-call bug. This mirrors how PromotionOnHitTest
// covers the default-tenant happy path through the public API; here we
// cover the non-default-tenant OpLog bug at the same code path.
TEST_F(MasterServiceHATest, NotifyPromotionSuccessPublishesPutEndTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    const std::string tenant = "tenant_a";
    const std::string key = "promo_tenant_key";
    const UUID client_id = generate_uuid();
    constexpr size_t kObjectSize = 1024;

    // Mount a memory segment so PutStart has somewhere to allocate the
    // PROCESSING MEMORY replica that promotion will then flip COMPLETE.
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service, "test_segment");

    // Stage a PROCESSING MEMORY replica for tenant_a via the tenant-aware
    // PutStart overload (no PutEnd -- keep the replica in PROCESSING).
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {"test_segment"};
    auto put_start =
        service->PutStart(client_id, key, tenant, kObjectSize, config);
    ASSERT_TRUE(put_start.has_value())
        << "PutStart(tenant_a) failed; error=" << put_start.error();
    ASSERT_EQ(1u, put_start->size());
    const ReplicaID staged_replica_id = put_start->front().id;

    // Seed the in-flight PromotionTask directly. Friend access is required
    // because TryPushPromotionQueue is gated to "default" tenants (see
    // master_service.cpp). alloc_id must be non-zero and holder_id must
    // match client_id for NotifyPromotionSuccess to proceed.
    SeedPromotionTaskForTesting(service.get(), tenant, key, client_id,
                                staged_replica_id, kObjectSize);

    // NotifyPromotionSuccess under tenant_a -- this is where the bug
    // manifests. The 3-arg AppendOpLog overload at line ~4364 would
    // publish under "default" instead of the real tenant.
    auto res = service->NotifyPromotionSuccess(client_id, key, tenant);
    ASSERT_TRUE(res.has_value())
        << "NotifyPromotionSuccess should succeed; error=" << res.error();

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// BatchRemove on a tenant_a object publishes REMOVE OpLog with
// non-default tenant_id (exercises line 3702 in master_service.cpp).
TEST_F(MasterServiceHATest, BatchRemoveForcePublishesRemoveTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "tenant_batch_remove_key";

    // Create the object in tenant_a so the subsequent BatchRemove operates
    // on the same tenant.
    PutObjectWithTenant(*service, client_id, key, tenant);

    // Call BatchRemove under tenant_a with force=true. This hits the
    // `PersistRemoveForHA("BatchRemove", key)` site at line 3702.
    std::vector<std::string> keys{key};
    auto results = service->BatchRemove(keys, tenant, /*force=*/true);
    ASSERT_EQ(1u, results.size());
    ASSERT_TRUE(results[0].has_value())
        << "BatchRemove should succeed; error=" << results[0].error();

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

TEST_F(MasterServiceHATest, RemoveAllTenantPublishesRemoveTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "tenant_remove_all_key";

    PutObjectWithTenant(*service, client_id, key, tenant);

    EXPECT_EQ(1, service->RemoveAll(tenant, /*force=*/true));

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

TEST_F(MasterServiceHATest,
       RemoveUsesNormalizedTenantIdWhenMultiTenantDisabled) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "legacy_remove_normalized_tenant_key";

    PutObject(*service, client_id, key);

    auto remove = service->Remove(key, "tenant_a");
    ASSERT_TRUE(remove.has_value()) << toString(remove.error());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(kDefaultTenant, entry.tenant_id);
}

// BatchReplicaClear (new tenant-aware overload) publishes REMOVE OpLog
// with the real (non-default) tenant_id.
TEST_F(MasterServiceHATest, BatchReplicaClearPublishesRemoveTenantId) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";

    // Create the object in tenant_a
    PutObjectWithTenant(*service, client_id, "batch_clear_key", tenant);

    // Call new tenant-aware BatchReplicaClear overload (clear_all=true)
    std::vector<std::string> keys = {"batch_clear_key"};
    auto res = service->BatchReplicaClear(keys, client_id, "", tenant);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK,
              mock_store->FindLatestEntryForKey("batch_clear_key", entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ(tenant, entry.tenant_id);
    EXPECT_NE("default", entry.tenant_id);
}

// ===== Step 5: end-to-end standby convergence regression test =====
//
// The per-call-site tests above prove the publish side (each call site
// publishes the correct tenant_id). This test proves the cumulative effect:
// when those entries are applied by OpLogApplier on the standby, the
// standby's MockMetadataStore converges to the correct tenant.
//
// This is the regression test that would have caught any missed
// call-site in Tasks 1-8: if any fix were missing, the standby would
// apply the entry to the wrong tenant and the assertions below would
// fail.
TEST_F(MasterServiceHATest, NonDefaultTenantStandbyConvergesCorrectly) {
    auto service_config = MakeStrictHAConfig();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_oplog = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_oplog);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string tenant = "tenant_a";
    const std::string key = "e2e_tenant_key";

    // 1. PutStart + PutEnd under tenant_a.
    PutObjectWithTenant(*service, client_id, key, tenant);

    // 2. Capture the PUT_END entry from the master's MockOpLogStore.
    OpLogEntry put_entry;
    ASSERT_EQ(ErrorCode::OK, mock_oplog->FindLatestEntryForKey(key, put_entry));
    ASSERT_EQ(OpType::PUT_END, put_entry.op_type);
    ASSERT_EQ(tenant, put_entry.tenant_id);

    // 3. Simulate the standby replay: create an independent
    //    MockMetadataStore + OpLogApplier and apply the captured entry.
    auto mock_meta = std::make_shared<MockMetadataStore>();
    OpLogApplier applier(mock_meta.get(), "test_cluster");
    // Fast-forward the applier's expected_sequence_id so seq=N (where N is
    // the PUT_END's actual sequence_id) is accepted in-order. In a real
    // standby, prior entries (PutStart, etc.) would already be applied; here
    // we skip them since the goal is to verify tenant_id propagation only.
    applier.Recover(put_entry.sequence_id - 1);
    ASSERT_TRUE(applier.ApplyOpLogEntry(put_entry));

    // 4. Assert standby state: key is in tenant_a, NEVER in "default".
    ASSERT_TRUE(mock_meta->Exists(tenant, key))
        << "Key must be in tenant_a after applying PUT_END with tenant_id="
        << tenant;
    ASSERT_FALSE(mock_meta->Exists("default", key))
        << "Key must never be in 'default' tenant for tenant_a operations";

    // 5. Remove the key under tenant_a via the master.
    auto remove_res = service->Remove(key, tenant);
    ASSERT_TRUE(remove_res.has_value())
        << "Remove failed; error=" << toString(remove_res.error());

    // 6. Capture the REMOVE entry.
    OpLogEntry remove_entry;
    ASSERT_EQ(ErrorCode::OK,
              mock_oplog->FindLatestEntryForKey(key, remove_entry));
    ASSERT_EQ(OpType::REMOVE, remove_entry.op_type);
    ASSERT_EQ(tenant, remove_entry.tenant_id);

    // 7. Apply the REMOVE on the standby.
    ASSERT_TRUE(applier.ApplyOpLogEntry(remove_entry));

    // 8. Assert standby state: key gone from tenant_a, "default" never
    //    existed.
    ASSERT_FALSE(mock_meta->Exists(tenant, key))
        << "Key must be gone from tenant_a after applying REMOVE with "
           "tenant_id="
        << tenant;
    ASSERT_FALSE(mock_meta->Exists("default", key))
        << "'default' tenant must never have been created";
}

// ===== End-to-end promotion failure placeholder =====

TEST_F(MasterServiceHATest, EndToEndOpLogStoreInaccessibleFailsPromotion) {
    GTEST_SKIP() << "End-to-end wiring is deferred; the unit tests in "
                    "hot_standby_service_test.cpp cover the fail-closed "
                    "behavior at the HotStandbyService layer.";
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
