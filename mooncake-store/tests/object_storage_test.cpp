#include <gtest/gtest.h>

#include <cstring>
#include <future>
#include <fstream>
#include <thread>
#include <map>
#include <mutex>
#include <limits>

#include "storage/distributed/object_storage_backend.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage/distributed/object_storage_catalog.h"
#include "master_service/master_service_test_peer.h"
#include "tenant_quota_policy_store.h"
#include "master_service_test_fixture.h"
#include "serialize/serializer.h"
#include "common/base64.h"
#include "ha/snapshot/master_snapshot_codec.h"
#include "ha/oplog/mock_metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/snapshot_test_utils.h"
#include <ylt/struct_pack.hpp>

namespace mooncake::test {
namespace {
class ObjectCatalogKv final : public HaKvBackend {
   public:
    std::map<std::string, std::string> values;
    bool fail = false;
    bool lose_reply = false;
    bool lose_reconciliation = false;
    bool fail_next_get = false;
    bool lose_reply_on_ready = false;
    ErrorCode Get(std::string_view key, std::string& value) override {
        if (fail || std::exchange(fail_next_get, false))
            return ErrorCode::ETCD_OPERATION_ERROR;
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        values[std::string(key)] = value;
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& result) override {
        result.clear();
        for (auto it = values.lower_bound(std::string(begin));
             it != values.end() && it->first < end && result.size() < limit;
             ++it)
            result.push_back({it->first, it->second});
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        values.erase(values.lower_bound(std::string(begin)),
                     values.lower_bound(std::string(end)));
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn& txn) override {
        if (fail) return ErrorCode::ETCD_OPERATION_ERROR;
        for (const auto& compare : txn.compares) {
            auto it = values.find(compare.key);
            if ((compare.kind == KvCompareKind::kKeyNotExists &&
                 it != values.end()) ||
                (compare.kind == KvCompareKind::kValueEquals &&
                 (it == values.end() || it->second != compare.expected_value)))
                return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
        for (const auto& put : txn.puts) {
            values[put.key] = put.value;
            if (lose_reply_on_ready &&
                put.key.find("/records/") != std::string::npos) {
                ObjectStorageRecord record;
                const auto bytes = base64::Decode(put.value);
                if (struct_pack::deserialize_to(record, bytes) ==
                        struct_pack::errc::ok &&
                    record.phase == ObjectStoragePhase::READY)
                    lose_reply = lose_reconciliation = true;
            }
        }
        if (lose_reply && lose_reconciliation) fail_next_get = true;
        return lose_reply ? ErrorCode::RPC_FAIL : ErrorCode::OK;
    }
};

class ObjectMemoryAdapter final : public ObjectStorageAdapter {
   public:
    std::map<std::string, std::string> objects;
    bool fail_delete = false;
    bool short_read = false;
    size_t deletes = 0;
    std::function<void()> before_delete;
    tl::expected<void, ErrorCode> Init() override { return {}; }
    const char* GetName() const override { return "object-storage-test"; }
    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> data) override {
        objects[key] = std::string(data.data(), data.size());
        return {};
    }
    tl::expected<void, ErrorCode> PutV(const std::string& key, const iovec* iov,
                                       int count) override {
        std::string bytes;
        for (int i = 0; i < count; ++i)
            bytes.append(static_cast<char*>(iov[i].iov_base), iov[i].iov_len);
        objects[key] = std::move(bytes);
        return {};
    }
    tl::expected<size_t, ErrorCode> Get(const std::string& key, void* buffer,
                                        size_t size) override {
        return GetRange(key, buffer, size, 0);
    }
    tl::expected<size_t, ErrorCode> GetRange(const std::string& key,
                                             void* buffer, size_t size,
                                             off_t offset) override {
        auto it = objects.find(key);
        if (it == objects.end())
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        if (offset < 0 || static_cast<size_t>(offset) > it->second.size())
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        size = std::min(size, it->second.size() - offset);
        if (short_read && size) --size;
        std::memcpy(buffer, it->second.data() + offset, size);
        return size;
    }
    tl::expected<bool, ErrorCode> Exists(const std::string& key) override {
        return objects.contains(key);
    }
    tl::expected<void, ErrorCode> Delete(const std::string& key) override {
        if (before_delete) before_delete();
        ++deletes;
        if (fail_delete) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        objects.erase(key);
        return {};
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        ADD_FAILURE() << "GC must never list a bucket";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
};

ObjectStorageConfig Config(std::string mode = "sync") {
    ObjectStorageConfig config;
    config.enabled = true;
    config.endpoint = "http://127.0.0.1:1";
    config.bucket = "test";
    config.region = "test";
    config.prefix = "test/";
    config.write_mode = std::move(mode);
    return config;
}
ObjectStorageRecord Record(const ObjectStorageConfig& config,
                           std::string name = "immutable-name") {
    ObjectStorageRecord record;
    record.tenant_id = "default";
    record.key = "logical-key";
    Replica replica(
        ObjectStorageDescriptor{std::move(name), 6, config.WriteMode(),
                                config.NamespaceId()},
        ReplicaStatus::PROCESSING);
    record.target = replica.get_descriptor();
    return record;
}
bool Copy(void* dst, const void* src, size_t size) {
    std::memcpy(dst, src, size);
    return true;
}

TEST(ObjectStorageCatalog, LostCommitReplyFencingAndRestart) {
    auto kv = std::make_shared<ObjectCatalogKv>();
    auto config = Config();
    ObjectStorageCatalog old(config, kv, "test");
    ASSERT_EQ(old.Open(1), ErrorCode::OK);
    auto record = Record(config);
    kv->lose_reply = true;
    ASSERT_EQ(old.Save(record), ErrorCode::OK);
    kv->lose_reply = false;
    ASSERT_EQ(old.Retire(record.descriptor().object_key, 100), ErrorCode::OK);
    ObjectStorageCatalog next(config, kv, "test");
    ASSERT_EQ(next.Open(2), ErrorCode::OK);
    auto restored = next.Find(record.descriptor().object_key);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->phase, ObjectStoragePhase::RETIRED);
    EXPECT_TRUE(restored->object_deleted);
    EXPECT_EQ(old.Save(Record(config, "old-leader-write")),
              ErrorCode::ETCD_TRANSACTION_FAIL);
    auto changed = config;
    changed.bucket = "different";
    ObjectStorageCatalog wrong(changed, kv, "test");
    EXPECT_EQ(wrong.Open(3), ErrorCode::INVALID_PARAMS);
}

TEST(ObjectStorageCatalog, SingleSlotReplacementRestartsAfterLostReply) {
    auto kv = std::make_shared<ObjectCatalogKv>();
    auto config = Config("async");
    config.max_pending_objects = 1;
    config.max_pending_bytes = config.max_object_bytes = 6;
    ObjectStorageCatalog catalog(config, kv, "test");
    ASSERT_EQ(catalog.Open(1), ErrorCode::OK);
    auto old = Record(config, "not-started");
    old.phase = ObjectStoragePhase::RETIRED;
    old.foreground_complete = old.source_released = old.upload_finished = true;
    ASSERT_EQ(catalog.Save(old), ErrorCode::OK);
    auto next = Record(config, "replacement");
    next.phase = ObjectStoragePhase::READY;
    next.foreground_complete = true;
    kv->lose_reply = kv->lose_reconciliation = true;
    EXPECT_EQ(catalog.Save(next), ErrorCode::RPC_FAIL);
    kv->lose_reply = kv->lose_reconciliation = false;
    ObjectStorageCatalog restored(config, kv, "test");
    ASSERT_EQ(restored.Open(2), ErrorCode::OK);
    EXPECT_FALSE(restored.Find(old.descriptor().object_key));
    ASSERT_TRUE(restored.Find(next.descriptor().object_key));
    ASSERT_EQ(restored.Records().size(), 1u);
    ASSERT_EQ(restored.Heads().size(), 1u);
    EXPECT_EQ(restored.Heads().front().target.id, next.target.id);
    EXPECT_FALSE(restored.CanReserve(6));
    auto completed = *restored.Find(next.descriptor().object_key);
    completed.phase = ObjectStoragePhase::COMPLETE;
    ASSERT_EQ(restored.Save(completed), ErrorCode::OK);
    EXPECT_TRUE(restored.CanReserve(6));
}

TEST(ObjectStorageCatalog, LatePutAndDeleteFailurePreserveTombstone) {
    auto kv = std::make_shared<ObjectCatalogKv>();
    auto config = Config();
    ObjectStorageCatalog catalog(config, kv, "test");
    ASSERT_EQ(catalog.Open(1), ErrorCode::OK);
    auto record = Record(config);
    record.upload_started = true;
    record.phase = ObjectStoragePhase::UPLOADING;
    ASSERT_EQ(catalog.Save(record), ErrorCode::OK);
    ASSERT_EQ(catalog.Retire(record.descriptor().object_key, 100),
              ErrorCode::OK);
    ObjectMemoryAdapter adapter;
    ASSERT_EQ(catalog.Reclaim(adapter, 99), ErrorCode::OK);
    EXPECT_EQ(adapter.deletes, 0u);
    ASSERT_EQ(catalog.Reclaim(adapter, 100), ErrorCode::OK);
    EXPECT_TRUE(catalog.Find(record.descriptor().object_key));
    ASSERT_TRUE(adapter.Put(record.descriptor().object_key,
                            std::span<const char>("abcdef", 6)));
    adapter.fail_delete = true;
    EXPECT_EQ(catalog.Reclaim(adapter, 101), ErrorCode::FILE_WRITE_FAIL);
    EXPECT_TRUE(adapter.objects.contains(record.descriptor().object_key));
    adapter.fail_delete = false;
    ASSERT_EQ(catalog.Reclaim(adapter, 102), ErrorCode::OK);
    EXPECT_TRUE(adapter.objects.empty());
    auto finished = *catalog.Find(record.descriptor().object_key);
    finished.upload_finished = true;
    ASSERT_EQ(catalog.Save(finished), ErrorCode::OK);
    ASSERT_EQ(catalog.Reclaim(adapter, 103), ErrorCode::OK);
    EXPECT_FALSE(catalog.Find(record.descriptor().object_key));
    EXPECT_EQ(kv->values.size(),
              1u);  // Only the namespace/leader fence remains.
}

TEST(ObjectStorageCatalog, AdmissionAndMaintenanceAreBounded) {
    auto kv = std::make_shared<ObjectCatalogKv>();
    auto config = Config();
    config.max_pending_objects = 3;
    config.maintenance_batch_size = 1;
    ObjectStorageCatalog catalog(config, kv, "test");
    ASSERT_EQ(catalog.Open(1), ErrorCode::OK);
    for (int i = 0; i < 3; ++i) {
        auto record = Record(config, std::to_string(i));
        ASSERT_EQ(catalog.Save(record), ErrorCode::OK);
        ASSERT_EQ(catalog.Retire(record.descriptor().object_key, 0),
                  ErrorCode::OK);
    }
    EXPECT_EQ(catalog.Save(Record(config, "overflow")),
              ErrorCode::NO_AVAILABLE_HANDLE);
    ObjectMemoryAdapter adapter;
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(catalog.Reclaim(adapter, 1), ErrorCode::OK);
        EXPECT_EQ(adapter.deletes, static_cast<size_t>(i + 1));
    }
    EXPECT_EQ(catalog.Save(Record(config, "admitted")), ErrorCode::OK);
}

TEST(ObjectStorageCatalog, LogicalDeletionSurvivesGcAndOlderAttemptUpdates) {
    auto kv = std::make_shared<ObjectCatalogKv>();
    auto config = Config("async");
    ObjectStorageCatalog catalog(config, kv, "test");
    ASSERT_EQ(catalog.Open(1), ErrorCode::OK);
    auto old = Record(config, "old");
    old.foreground_complete = true;
    old.phase = ObjectStoragePhase::RETIRED;
    old.upload_started = true;
    ASSERT_EQ(catalog.Save(old), ErrorCode::OK);
    auto next = Record(config, "new");
    next.foreground_complete = true;
    next.phase = ObjectStoragePhase::READY;
    ASSERT_EQ(catalog.Save(next), ErrorCode::OK);
    // Crash between publishing the next intent and retiring the old attempt.
    ASSERT_EQ(catalog.Retire("new", 0), ErrorCode::OK);
    ObjectMemoryAdapter adapter;
    ASSERT_EQ(catalog.Reclaim(adapter, 1), ErrorCode::OK);
    EXPECT_FALSE(catalog.Find("new"));
    old = *catalog.Find("old");
    old.upload_finished = true;
    ASSERT_EQ(catalog.Save(old), ErrorCode::OK);
    ObjectStorageCatalog restored(config, kv, "test");
    ASSERT_EQ(restored.Open(2), ErrorCode::OK);
    const auto heads = restored.Heads();
    ASSERT_EQ(heads.size(), 1u);
    EXPECT_EQ(heads.front().target.id, next.target.id);
    EXPECT_TRUE(heads.front().object_deleted);
}

TEST(ObjectStorageBackend, ScatterGatherRangesAndFailureBoundaries) {
    auto config = Config();
    auto adapter = std::make_shared<ObjectMemoryAdapter>();
    ObjectStorageBackend backend(config, adapter);
    auto descriptor = Record(config).descriptor();
    char first[] = "ab", second[] = "cdef";
    bool started = true;
    EXPECT_EQ(
        backend.Write(descriptor, {{first, 2}, {second, 3}}, Copy, &started),
        ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(started);
    ASSERT_EQ(
        backend.Write(descriptor, {{first, 2}, {second, 4}}, Copy, &started),
        ErrorCode::OK);
    EXPECT_TRUE(started);
    char result[7] = {};
    ASSERT_EQ(backend.Read(descriptor, {{result, 2}, {result + 2, 4}}, 0, Copy),
              ErrorCode::OK);
    EXPECT_STREQ(result, "abcdef");
    char range[4] = {};
    ASSERT_EQ(backend.Read(descriptor, {{range, 3}}, 2, Copy), ErrorCode::OK);
    EXPECT_STREQ(range, "cde");
    EXPECT_EQ(backend.Read(descriptor, {{range, 3}}, 4, Copy),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(backend.Read(descriptor, {{range, 3}},
                           std::numeric_limits<uint64_t>::max(), Copy),
              ErrorCode::INVALID_PARAMS);
    adapter->short_read = true;
    EXPECT_EQ(backend.Read(descriptor, {{range, 3}}, 0, Copy),
              ErrorCode::FILE_READ_FAIL);
    auto wrong = descriptor;
    wrong.namespace_id = "wrong";
    EXPECT_EQ(backend.Read(wrong, {{range, 3}}, 0, Copy),
              ErrorCode::INVALID_PARAMS);
}

class ObjectStorageMasterTest : public MasterServiceTest {
   protected:
    std::shared_ptr<ObjectCatalogKv> kv = std::make_shared<ObjectCatalogKv>();
    std::shared_ptr<ObjectMemoryAdapter> adapter =
        std::make_shared<ObjectMemoryAdapter>();
    void Configure(MasterService& service, const std::string& mode,
                   bool single_slot = false, bool recovery_complete = true) {
        MasterServiceTestPeer::EvictionRunning(service) = false;
        MasterServiceTestPeer::EvictionThread(service).join();
        PauseReplicaCleanup(service);
        auto options = Config(mode);
        if (single_slot) {
            options.max_pending_objects = 1;
            options.max_pending_bytes = 6;
            options.max_object_bytes = 6;
        }
        auto catalog =
            std::make_unique<ObjectStorageCatalog>(options, kv, "master-test");
        ASSERT_EQ(catalog->Open(0), ErrorCode::OK);
        MasterServiceTestPeer(service).SetObjectStorageForTesting(
            std::move(catalog), adapter, recovery_complete);
    }
    void ExpectPendingObjectTarget(MasterService& service) {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                "key", TenantId::Default()));
        ASSERT_TRUE(accessor.Exists());
        EXPECT_TRUE(accessor.Get().HasReplica([](const Replica& replica) {
            return replica.is_object_storage_replica() &&
                   replica.is_processing();
        }));
    }
    static Replica::Descriptor Target(
        const std::vector<Replica::Descriptor>& replicas) {
        for (const auto& r : replicas)
            if (r.is_object_storage_replica()) return r;
        ADD_FAILURE() << "Missing object storage target";
        return {};
    }
};

TEST_F(ObjectStorageMasterTest, PendingSnapshotsSurviveBothRecoveryPaths) {
    for (const std::string state : {"ready", "uploading", "deferred"}) {
        SCOPED_TRACE(state);
        kv = std::make_shared<ObjectCatalogKv>();
        const std::string mode =
            state == "deferred" ? "offload_on_evict" : "async";
        ha::MasterSnapshotPayloads payloads;
        ha::MasterSnapshotCodec codec;
        MountedSegmentContext mounted;
        std::string name;
        {
            MasterService service;
            Configure(service, mode);
            mounted = PrepareSimpleSegment(service);
            ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
            ReplicateConfig config;
            config.os_replica_num = 1;
            auto start = service.PutStart(mounted.client_id, "key",
                                          TenantId::Default(), 6, config);
            ASSERT_TRUE(start);
            const auto target = Target(*start);
            name = target.get_object_storage_descriptor().object_key;
            ASSERT_TRUE(service.PutEnd(mounted.client_id,
                                       ObjectMeta{"key", 42, target.id},
                                       TenantId::Default(), ReplicaType::ALL));
            if (state == "uploading") {
                ASSERT_TRUE(
                    service.BeginObjectStorageUpload(mounted.client_id, name));
            }
            // Never Get the key: its read lease is already expired. An
            // unfinished foreground write in the same snapshot must be lost.
            ASSERT_TRUE(service.PutStart(mounted.client_id, "uncommitted",
                                         TenantId::Default(), 6, config));
            ha::MasterSnapshotStateView view(
                service, MasterServiceTestPeer::SegmentManager(service),
                MasterServiceTestPeer::LocalSsdManager(service),
                MasterServiceTestPeer::NofSegmentManager(service),
                MasterServiceTestPeer::TaskManager(service));
            auto encoded = codec.Encode(view);
            ASSERT_TRUE(encoded) << encoded.error().message;
            payloads = std::move(*encoded);
        }
        {
            MasterService restored;
            // Ordinary startup decodes/applies the snapshot before opening
            // the object catalog. Exercise that exact ordering.
            ASSERT_TRUE(codec.Decode(&restored, payloads));
            ASSERT_TRUE(MasterServiceTestPeer(restored).ApplySnapshotState());
            Configure(restored, mode, false, false);
            ASSERT_EQ(MasterServiceTestPeer(restored).RecoverObjectStorage(),
                      ErrorCode::OK);
            auto query = restored.GetReplicaList("key", TenantId::Default());
            ASSERT_TRUE(query);
            EXPECT_EQ(query->object_checksum, 42u);
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_memory_replica());
            ExpectPendingObjectTarget(restored);
            EXPECT_FALSE(
                restored.GetReplicaList("uncommitted", TenantId::Default()));
        }
        const std::string directory =
            MakeSnapshotTestTempDir("object_pending_snapshot_");
        {
            ScopedEnvVar local_path(kSnapshotLocalPathEnv, directory);
            LocalFileSnapshotObjectStore object_store(directory);
            const std::string cluster = "pending-object-test";
            const CatalogBackendParam backend{.name = "Embedded",
                                              .catalog_store_type = "embedded"};
            auto catalog =
                CreateCatalogStoreForTest(backend, &object_store, cluster, "");
            const auto descriptor = MakeTestSnapshotDescriptor(cluster);
            ASSERT_TRUE(PublishSnapshotPayloadBytes(
                object_store, *catalog, descriptor, payloads.metadata));
            ASSERT_TRUE(object_store.UploadBuffer(
                descriptor.object_prefix + "segments", payloads.segments));
            auto provider = CreateCatalogBackedSnapshotProvider(
                MakeSnapshotProviderConfig(backend, cluster, ""));
            ASSERT_TRUE(provider);
            auto snapshot = (*provider)->LoadLatestSnapshot(cluster);
            ASSERT_TRUE(snapshot);
            ASSERT_TRUE(*snapshot);
            ASSERT_EQ(snapshot->value().metadata.size(), 1u);
            EXPECT_EQ(snapshot->value().metadata.front().key, "key");

            MasterService promoted(MasterServiceConfigBuilder()
                                       .set_default_kv_lease_ttl(0)
                                       .build());
            Configure(promoted, mode, false, false);
            // Deliberately run maintenance in the constructor-to-restore gap,
            // with live memory owners but no restored per-key metadata.
            MasterServiceTestPeer(promoted).RunObjectStorageMaintenance();
            auto& objects = MasterServiceTestPeer(promoted)
                                .ObjectStorageCatalogForTesting();
            ASSERT_TRUE(objects.Find(name));
            EXPECT_FALSE(objects.Find(name)->object_deleted);
            EXPECT_EQ(objects.Find(name)->phase,
                      state == "ready"       ? ObjectStoragePhase::READY
                      : state == "uploading" ? ObjectStoragePhase::UPLOADING
                                             : ObjectStoragePhase::DEFERRED);
            Segment segment = MakeSegment();
            segment.id = mounted.segment_id;
            ASSERT_TRUE(promoted.MountSegment(segment, mounted.client_id));
            ASSERT_TRUE(promoted.RestoreFromStandbySnapshot(
                snapshot->value().metadata, 0, snapshot->value().segments));
            MasterServiceTestPeer(promoted).RunObjectStorageMaintenance();
            auto query = promoted.GetReplicaList("key", TenantId::Default());
            ASSERT_TRUE(query);
            ASSERT_EQ(query->replicas.size(), 1u);
            EXPECT_TRUE(query->replicas.front().is_memory_replica());
            ExpectPendingObjectTarget(promoted);
            ASSERT_TRUE(
                promoted.RegisterObjectStorageClient(mounted.client_id));
            if (state == "deferred")
                MasterServiceTestPeer(promoted).RunBatchEvictForTesting(1.0,
                                                                        1.0);
            if (state != "uploading") {
                ASSERT_TRUE(
                    promoted.BeginObjectStorageUpload(mounted.client_id, name));
            }
            ASSERT_TRUE(promoted.FinishObjectStorageUpload(
                mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
            query = promoted.GetReplicaList("key", TenantId::Default());
            ASSERT_TRUE(query);
            EXPECT_EQ(Target(query->replicas).status, ReplicaStatus::COMPLETE);
        }
        std::filesystem::remove_all(directory);
    }
}

TEST_F(ObjectStorageMasterTest, EmptyPromotionEnablesMaintenanceAfterRecovery) {
    MasterService service(
        MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
    Configure(service, "async", false, false);
    auto& catalog =
        MasterServiceTestPeer(service).ObjectStorageCatalogForTesting();
    auto pending = Record(Config("async"), "missing-source");
    pending.phase = ObjectStoragePhase::READY;
    pending.foreground_complete = true;
    ASSERT_EQ(catalog.Save(pending), ErrorCode::OK);
    auto completed = Record(Config("async"), "complete-without-snapshot");
    completed.key = "completed-key";
    completed.phase = ObjectStoragePhase::COMPLETE;
    completed.target.status = ReplicaStatus::COMPLETE;
    completed.foreground_complete = completed.upload_finished =
        completed.source_released = true;
    ASSERT_EQ(catalog.Save(completed), ErrorCode::OK);
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    EXPECT_FALSE(catalog.Find("missing-source")->object_deleted);
    kv->fail = true;
    EXPECT_FALSE(service.RestoreFromStandbySnapshot({}, 0, {}));
    kv->fail = false;
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    EXPECT_FALSE(catalog.Find("missing-source")->object_deleted);
    ASSERT_TRUE(service.RestoreFromStandbySnapshot({}, 0, {}));
    ASSERT_TRUE(catalog.Find("missing-source"));
    EXPECT_TRUE(catalog.Find("missing-source")->object_deleted);
    auto query = service.GetReplicaList("completed-key", TenantId::Default());
    ASSERT_TRUE(query);
    EXPECT_EQ(Target(query->replicas).status, ReplicaStatus::COMPLETE);
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    EXPECT_GT(adapter->deletes, 0u);
}

TEST_F(ObjectStorageMasterTest, TaskCleanupAllowsSnapshotWriterDuringDelete) {
    std::promise<void> delete_entered, allow_delete, writer_entered;
    auto entered = delete_entered.get_future();
    auto release = allow_delete.get_future();
    auto writer_ready = writer_entered.get_future();
    MasterService service;
    Configure(service, "async");
    auto retired = Record(Config("async"), "gc-with-snapshot-writer");
    retired.phase = ObjectStoragePhase::RETIRED;
    retired.object_deleted = retired.upload_finished = true;
    ASSERT_EQ(
        MasterServiceTestPeer(service).ObjectStorageCatalogForTesting().Save(
            retired),
        ErrorCode::OK);
    adapter->before_delete = [&] {
        delete_entered.set_value();
        release.wait();
    };
    // Drive the actual 30-second cleanup thread. The HTTP boundary is held
    // explicitly while a snapshot writer requests exclusive ownership.
    const auto status = entered.wait_for(std::chrono::seconds(35));
    if (status != std::future_status::ready) {
        allow_delete.set_value();
        FAIL() << "Task cleanup did not reach DELETE";
    }
    std::thread writer([&] {
        std::unique_lock lock(MasterServiceTestPeer::SnapshotMutex(service));
        writer_entered.set_value();
    });
    const auto acquired = writer_ready.wait_for(std::chrono::seconds(5));
    allow_delete.set_value();
    writer.join();
    EXPECT_EQ(acquired, std::future_status::ready);
}

TEST_F(ObjectStorageMasterTest,
       SyncRequiresUploadAndDeletionFailureStopsNewLeases) {
    MasterService service;
    Configure(service, "sync");
    const auto mounted = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    auto target = Target(*start);
    ObjectMeta meta{"key", 42, target.id};
    EXPECT_FALSE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                                ReplicaType::ALL));
    const auto& name = target.get_object_storage_descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    EXPECT_FALSE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(adapter->Put(name, std::span<const char>("abcdef", 6)));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    ASSERT_TRUE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                               ReplicaType::ALL));
    ASSERT_TRUE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                               ReplicaType::ALL));
    auto query = service.GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(query);
    EXPECT_EQ(query->object_checksum, 42u);
    kv->fail = true;
    EXPECT_FALSE(service.Remove("key", TenantId::Default(), true));
    // A failed reconciliation cannot prove retirement did not commit. Do not
    // grant leases which might extend beyond its already-durable GC deadline.
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
    kv->fail = false;
    ASSERT_TRUE(service.Remove("key", TenantId::Default(), true));
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
    // GC may not invalidate a descriptor whose lease was already granted.
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    EXPECT_TRUE(adapter->objects.contains(name));
}

TEST_F(ObjectStorageMasterTest, AsyncFailureUsesFreshNameAndCompletesOnce) {
    MasterService service;
    Configure(service, "async");
    const auto mounted = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    auto target = Target(*start);
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"key", 42, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    auto tasks = service.PollObjectStorageTasks(mounted.client_id);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    const auto old_name = tasks->front().descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, old_name));
    auto removing = service.Remove("key", TenantId::Default(), true);
    ASSERT_FALSE(removing);
    EXPECT_EQ(removing.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    EXPECT_TRUE(service.GetReplicaList("key", TenantId::Default()));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, old_name, ObjectUploadOutcome::UNKNOWN));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, old_name, ObjectUploadOutcome::UNKNOWN));
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    tasks = service.PollObjectStorageTasks(mounted.client_id);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    const auto new_name = tasks->front().descriptor().object_key;
    EXPECT_NE(new_name, old_name);
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, new_name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, new_name, ObjectUploadOutcome::COMPLETE));
    auto query = service.GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(query);
    EXPECT_TRUE(Target(query->replicas).status == ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service.Remove("key", TenantId::Default(), true));
}

TEST_F(ObjectStorageMasterTest, SingleSlotRetrySurvivesLostReplacementReply) {
    MasterService service;
    Configure(service, "async", true);
    const auto mounted = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"key", {}, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    auto name = target.get_object_storage_descriptor().object_key;
    for (int failure_mode : {0, 1, 2}) {
        ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
        // Persist NOT_STARTED first, then lose the replacement transaction's
        // reply and first read-back. Maintenance must adopt the committed head.
        kv->lose_reply_on_ready = failure_mode != 0;
        ASSERT_TRUE(service.FinishObjectStorageUpload(
            mounted.client_id, name, ObjectUploadOutcome::NOT_STARTED));
        kv->lose_reply_on_ready = false;
        kv->lose_reply = kv->lose_reconciliation = false;
        if (failure_mode == 2) {
            // The old completion refreshes the catalog but leaves metadata
            // unchanged; Poll -> Begin must also reconcile the replacement.
            ASSERT_TRUE(service.FinishObjectStorageUpload(
                mounted.client_id, name, ObjectUploadOutcome::NOT_STARTED));
        } else {
            for (int i = 0; i < 3; ++i)
                MasterServiceTestPeer(service).RunObjectStorageMaintenance();
        }
        auto tasks = service.PollObjectStorageTasks(mounted.client_id);
        ASSERT_TRUE(tasks);
        ASSERT_EQ(tasks->size(), 1u);
        const auto next_name = tasks->front().descriptor().object_key;
        EXPECT_NE(name, next_name);
        // Replaying the old completion cannot cancel the new claim.
        if (failure_mode != 2) {
            ASSERT_TRUE(service.FinishObjectStorageUpload(
                mounted.client_id, name, ObjectUploadOutcome::NOT_STARTED));
        }
        name = next_name;
    }
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    auto query = service.GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(query);
    EXPECT_EQ(Target(query->replicas).status, ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service.Remove("key", TenantId::Default(), true));
}

TEST_F(ObjectStorageMasterTest, UploadSourceExcludesMoveAndSegmentClear) {
    MasterService service(
        MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
    Configure(service, "async");
    const auto mounted = PrepareSimpleSegment(service, "source");
    PrepareSimpleSegment(service, "target",
                         kDefaultSegmentBase + kDefaultSegmentSize);
    ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
    ReplicateConfig config;
    config.os_replica_num = 1;
    config.preferred_segment = "source";
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"key", {}, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    const auto name = target.get_object_storage_descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    auto move = service.MoveStart(mounted.client_id, "key", TenantId::Default(),
                                  "source", "target");
    ASSERT_FALSE(move);
    EXPECT_EQ(move.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    auto cleared =
        service.BatchReplicaClear({"key"}, mounted.client_id, "source");
    ASSERT_TRUE(cleared);
    EXPECT_TRUE(cleared->empty());
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    cleared = service.BatchReplicaClear({"key"}, mounted.client_id, "source");
    ASSERT_TRUE(cleared);
    EXPECT_EQ(*cleared, std::vector<std::string>{"key"});
    auto query = service.GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_TRUE(query->replicas.front().is_object_storage_replica());
}

TEST_F(ObjectStorageMasterTest, CompletedObjectBypassesLegacyOffloadOnEvict) {
    for (bool tenant_eviction : {false, true}) {
        for (bool grouped : {false, true}) {
            for (const std::string mode : {"async", "offload_on_evict"}) {
                SCOPED_TRACE(tenant_eviction);
                SCOPED_TRACE(grouped);
                SCOPED_TRACE(mode);
                const auto directory =
                    MakeSnapshotTestTempDir("object_legacy_offload_");
                {
                    const auto policy_path = directory + "/quota.yaml";
                    TenantQuotaPolicySnapshot policy;
                    policy.tenant_quotas.emplace("default", 1024);
                    {
                        std::ofstream output(policy_path);
                        ASSERT_TRUE(output.good());
                        output << FormatTenantQuotaPolicyYaml(policy);
                    }
                    kv = std::make_shared<ObjectCatalogKv>();
                    auto options =
                        MasterServiceConfigBuilder()
                            .set_default_kv_lease_ttl(0)
                            .set_enable_offload(true)
                            .set_enable_multi_tenants(true)
                            .set_tenant_quota_connector_type("file")
                            .set_tenant_quota_connector_uri(policy_path)
                            .set_tenant_eviction_high_watermark_ratio(0.0)
                            .build();
                    options.offload_on_evict = true;
                    MasterService service(options);
                    Configure(service, mode);
                    MasterServiceTestPeer peer(service);
                    const auto mounted = PrepareSimpleSegment(service);
                    ASSERT_TRUE(
                        service.RegisterObjectStorageClient(mounted.client_id));
                    ReplicateConfig config;
                    config.os_replica_num = 1;
                    if (grouped)
                        config.group_ids = std::vector<std::string>{"group"};
                    std::vector<std::string> names;
                    for (const std::string key : {"key-1", "key-2"}) {
                        auto start =
                            service.PutStart(mounted.client_id, key,
                                             TenantId::Default(), 6, config);
                        ASSERT_TRUE(start);
                        const auto target = Target(*start);
                        names.push_back(
                            target.get_object_storage_descriptor().object_key);
                        ASSERT_TRUE(service.PutEnd(
                            mounted.client_id, ObjectMeta{key, 42, target.id},
                            TenantId::Default(), ReplicaType::ALL));
                    }
                    // Pending intents still protect memory from both evictors.
                    if (tenant_eviction) {
                        EXPECT_EQ(peer.EvictTenantMemoryForQuota(
                                          TenantId::Default(), 12)
                                      .freed_bytes,
                                  0u);
                    } else {
                        peer.RunBatchEvictForTesting(1.0, 1.0);
                    }
                    auto usage =
                        service.GetTenantQuotaSnapshot(TenantId::Default());
                    ASSERT_TRUE(usage);
                    EXPECT_EQ(usage->charged_bytes, 12u);
                    for (const auto& name : names) {
                        auto record =
                            peer.ObjectStorageCatalogForTesting().Find(name);
                        ASSERT_TRUE(record);
                        ASSERT_EQ(record->phase, ObjectStoragePhase::READY);
                        ASSERT_TRUE(service.BeginObjectStorageUpload(
                            mounted.client_id, name));
                        ASSERT_TRUE(adapter->Put(
                            name, std::span<const char>("abcdef", 6)));
                        ASSERT_TRUE(service.FinishObjectStorageUpload(
                            mounted.client_id, name,
                            ObjectUploadOutcome::COMPLETE));
                    }
                    // There are deliberately no LOCAL_DISK destinations. A
                    // complete OS copy permits reclamation without another
                    // tier.
                    if (tenant_eviction) {
                        EXPECT_EQ(peer.EvictTenantMemoryForQuota(
                                          TenantId::Default(), 12)
                                      .freed_bytes,
                                  12u);
                    } else {
                        peer.RunBatchEvictForTesting(1.0, 1.0);
                    }
                    usage = service.GetTenantQuotaSnapshot(TenantId::Default());
                    ASSERT_TRUE(usage);
                    EXPECT_EQ(usage->charged_bytes, 0u);
                    for (const std::string key : {"key-1", "key-2"}) {
                        auto query =
                            service.GetReplicaList(key, TenantId::Default());
                        ASSERT_TRUE(query);
                        ASSERT_EQ(query->replicas.size(), 1u);
                        EXPECT_TRUE(query->replicas.front()
                                        .is_object_storage_replica());
                        EXPECT_EQ(query->replicas.front().status,
                                  ReplicaStatus::COMPLETE);
                        const auto& name = query->replicas.front()
                                               .get_object_storage_descriptor()
                                               .object_key;
                        EXPECT_EQ(adapter->objects.at(name), "abcdef");
                    }
                }
                std::filesystem::remove_all(directory);
            }
        }
    }
}

TEST_F(ObjectStorageMasterTest, SurvivingDfsReplicaOutlivesLostUploadSource) {
    for (const std::string allocator_type : {"shard", "bucket"}) {
        for (bool claimed : {false, true}) {
            SCOPED_TRACE(allocator_type);
            SCOPED_TRACE(claimed);
            const auto directory =
                MakeSnapshotTestTempDir("object_surviving_dfs_");
            {
                ScopedEnvVar enable("MOONCAKE_ENABLE_DFS", "1");
                ScopedEnvVar fs_type("MOONCAKE_DFS_FS_ADAPTER", "posix");
                ScopedEnvVar root("MOONCAKE_DFS_ROOT_DIR", directory);
                ScopedEnvVar type("MOONCAKE_DFS_ALLOCATOR", allocator_type);
                ScopedEnvVar shards("MOONCAKE_DFS_SHARD_COUNT", "1");
                ScopedEnvVar shard_size("MOONCAKE_DFS_SHARD_CAPACITY", "32768");
                ScopedEnvVar bucket_size("MOONCAKE_DFS_BUCKET_CAPACITY",
                                         "32768");
                ScopedEnvVar buckets("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
                ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
                ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
                ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT",
                                           "true");
                kv = std::make_shared<ObjectCatalogKv>();
                MasterService service(MasterServiceConfigBuilder()
                                          .set_default_kv_lease_ttl(0)
                                          .build());
                Configure(service, "async");
                const auto mounted = PrepareSimpleSegment(service);
                ASSERT_TRUE(
                    service.RegisterObjectStorageClient(mounted.client_id));
                ReplicateConfig config;
                config.os_replica_num = config.dfs_replica_num = 1;
                auto start = service.PutStart(mounted.client_id, "key",
                                              TenantId::Default(), 6, config);
                ASSERT_TRUE(start);
                const auto dfs = std::find_if(
                    start->begin(), start->end(),
                    [](const auto& r) { return r.is_dfs_replica(); });
                ASSERT_NE(dfs, start->end());
                DistributedStorageBackend backend(
                    FileStorageConfig{},
                    DistributedStorageConfig::FromEnvironment(),
                    std::make_unique<PosixFsAdapter>());
                ASSERT_TRUE(backend.Init());
                std::string value = "abcdef";
                auto written =
                    backend.BatchWrite({{"key",
                                         dfs->get_dfs_descriptor(),
                                         {{value.data(), value.size()}}}});
                ASSERT_EQ(written.size(), 1u);
                ASSERT_TRUE(written.front());
                const auto target = Target(*start);
                const auto name =
                    target.get_object_storage_descriptor().object_key;
                ASSERT_TRUE(service.PutEnd(
                    mounted.client_id, ObjectMeta{"key", 42, target.id},
                    TenantId::Default(), ReplicaType::ALL));
                if (claimed) {
                    ASSERT_TRUE(service.BeginObjectStorageUpload(
                        mounted.client_id, name));
                }
                ASSERT_TRUE(service.UnmountSegment(mounted.segment_id,
                                                   mounted.client_id));
                MasterServiceTestPeer peer(service);
                for (int i = 0; i < 3; ++i) peer.RunObjectStorageMaintenance();
                auto query = service.GetReplicaList("key", TenantId::Default());
                ASSERT_TRUE(query);
                auto surviving = std::find_if(
                    query->replicas.begin(), query->replicas.end(),
                    [](const auto& r) { return r.is_dfs_replica(); });
                ASSERT_NE(surviving, query->replicas.end());
                EXPECT_EQ(surviving->status, ReplicaStatus::COMPLETE);
                EXPECT_EQ(query->object_checksum, 42u);
                std::string output(value.size(), '\0');
                auto read =
                    backend.BatchRead({{"key",
                                        surviving->get_dfs_descriptor(),
                                        {{output.data(), output.size()}}}});
                ASSERT_EQ(read.size(), 1u);
                ASSERT_TRUE(read.front());
                EXPECT_EQ(output, value);
                if (auto* bucket = dynamic_cast<ImmutableBucketAllocator*>(
                        &peer.DfsAllocatorForTesting())) {
                    EXPECT_TRUE(bucket->GetBucketIdForKey("key"));
                }
                auto head = peer.ObjectStorageCatalogForTesting().Head(
                    "default", "key");
                ASSERT_TRUE(head);
                EXPECT_FALSE(head->object_deleted);
                EXPECT_EQ(head->phase, ObjectStoragePhase::RETIRED);
            }
            std::filesystem::remove_all(directory);
        }
    }
}

TEST_F(ObjectStorageMasterTest,
       DrainJobMovesDeferredForegroundCompleteObjects) {
    for (const std::string state :
         {"uncommitted", "deferred", "ready", "uploading"}) {
        SCOPED_TRACE(state);
        kv = std::make_shared<ObjectCatalogKv>();
        MasterService service(
            MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
        Configure(service, "offload_on_evict");
        MasterServiceTestPeer peer(service);
        peer.StopJobDispatchForTesting();
        const auto source = PrepareSimpleSegment(service, "source");
        PrepareSimpleSegment(service, "destination",
                             kDefaultSegmentBase + kDefaultSegmentSize);
        ASSERT_TRUE(service.RegisterObjectStorageClient(source.client_id));
        ReplicateConfig config;
        config.os_replica_num = 1;
        config.preferred_segment = "source";
        auto start = service.PutStart(source.client_id, "key",
                                      TenantId::Default(), 6, config);
        ASSERT_TRUE(start);
        const auto target = Target(*start);
        const auto name = target.get_object_storage_descriptor().object_key;
        auto commit = [&] {
            return service.PutEnd(source.client_id,
                                  ObjectMeta{"key", 42, target.id},
                                  TenantId::Default(), ReplicaType::ALL);
        };
        if (state != "uncommitted") {
            ASSERT_TRUE(commit());
        }
        if (state == "ready" || state == "uploading") {
            peer.RunBatchEvictForTesting(1.0, 1.0);
            auto record = peer.ObjectStorageCatalogForTesting().Find(name);
            ASSERT_TRUE(record);
            ASSERT_EQ(record->phase, ObjectStoragePhase::READY);
            if (state == "uploading") {
                ASSERT_TRUE(
                    service.BeginObjectStorageUpload(source.client_id, name));
            }
        }
        CreateDrainJobRequest request;
        request.segments = {"source"};
        request.target_segments = {"destination"};
        request.max_concurrency = 1;
        auto job = service.CreateDrainJob(request);
        ASSERT_TRUE(job);
        peer.ProcessDrainJobsForTesting();
        auto progress = service.QueryDrainJob(*job);
        ASSERT_TRUE(progress);
        if (state != "deferred") {
            EXPECT_EQ(progress->active_units, 0u);
            EXPECT_EQ(progress->blocked_units, 1u);
            auto tasks = service.FetchTasks(source.client_id, 16);
            ASSERT_TRUE(tasks);
            EXPECT_TRUE(tasks->empty());
            if (state == "uncommitted") {
                ASSERT_TRUE(commit());
            } else {
                if (state == "ready") {
                    ASSERT_TRUE(service.BeginObjectStorageUpload(
                        source.client_id, name));
                }
                ASSERT_TRUE(service.FinishObjectStorageUpload(
                    source.client_id, name, ObjectUploadOutcome::COMPLETE));
            }
            peer.ProcessDrainJobsForTesting();
            progress = service.QueryDrainJob(*job);
            ASSERT_TRUE(progress);
        }
        ASSERT_EQ(progress->active_units, 1u);
        ASSERT_TRUE(ExecutePendingMoveTasks(service, source.client_id));
        peer.ProcessDrainJobsForTesting();
        progress = service.QueryDrainJob(*job);
        ASSERT_TRUE(progress);
        EXPECT_EQ(progress->status, JobStatus::SUCCEEDED);
        EXPECT_EQ(progress->succeeded_units, 1u);
        EXPECT_EQ(progress->active_units, 0u);
        auto status = service.QuerySegmentStatus("source");
        ASSERT_TRUE(status);
        EXPECT_EQ(*status, SegmentStatus::DRAINED);
        auto query = service.GetReplicaList("key", TenantId::Default());
        ASSERT_TRUE(query);
        EXPECT_EQ(query->object_checksum, 42u);
        EXPECT_TRUE(std::any_of(
            query->replicas.begin(), query->replicas.end(),
            [](const auto& replica) {
                return replica.is_memory_replica() &&
                       replica.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_ ==
                           "destination";
            }));
        if (state == "uncommitted" || state == "deferred") {
            auto record = peer.ObjectStorageCatalogForTesting().Find(name);
            ASSERT_TRUE(record);
            EXPECT_EQ(record->phase, ObjectStoragePhase::DEFERRED);
            ExpectPendingObjectTarget(service);
            EXPECT_TRUE(adapter->objects.empty());
        }
    }
}

TEST_F(ObjectStorageMasterTest, RemoveAllDoesNotPublishClearedDuringUpload) {
    for (bool global : {false, true}) {
        SCOPED_TRACE(global);
        kv = std::make_shared<ObjectCatalogKv>();
        MasterService service;
        Configure(service, "async");
        MasterServiceTestPeer peer(service);
        peer.SetKvTenantEpochTrackingForTesting(true);
        const auto mounted = PrepareSimpleSegment(service);
        ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
        ReplicateConfig config;
        config.os_replica_num = 1;
        auto start = service.PutStart(mounted.client_id, "blocked",
                                      TenantId::Default(), 6, config);
        ASSERT_TRUE(start);
        auto target = Target(*start);
        ASSERT_TRUE(service.PutEnd(mounted.client_id,
                                   ObjectMeta{"blocked", 42, target.id},
                                   TenantId::Default(), ReplicaType::ALL));
        const auto name = target.get_object_storage_descriptor().object_key;
        ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
        config.os_replica_num = 0;
        ASSERT_TRUE(service.PutStart(mounted.client_id, "ordinary",
                                     TenantId::Default(), 6, config));
        ASSERT_TRUE(service.PutEnd(mounted.client_id, "ordinary",
                                   TenantId::Default(), ReplicaType::ALL));
        auto remove = [&] {
            return global ? service.RemoveAll(true)
                          : service.RemoveAll(TenantId::Default(), true);
        };
        // A partial wipe cannot announce that the tenant is empty.
        EXPECT_EQ(remove(), 1);
        EXPECT_EQ(peer.GetKvClearedPublishedForTesting(), 0u);
        auto query = service.GetReplicaList("blocked", TenantId::Default());
        ASSERT_TRUE(query);
        EXPECT_EQ(query->object_checksum, 42u);
        EXPECT_FALSE(service.GetReplicaList("ordinary", TenantId::Default()));
        ASSERT_TRUE(service.FinishObjectStorageUpload(
            mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
        EXPECT_EQ(remove(), 1);
        EXPECT_EQ(peer.GetKvClearedPublishedForTesting(), 1u);
        EXPECT_FALSE(service.GetReplicaList("blocked", TenantId::Default()));
        EXPECT_EQ(remove(), 0);
        EXPECT_EQ(peer.GetKvClearedPublishedForTesting(), 1u);
    }
}

TEST_F(ObjectStorageMasterTest, BulkObjectDeletionRetriesAfterCatalogFailure) {
    for (int api = 0; api < 4; ++api) {
        SCOPED_TRACE(api);
        kv = std::make_shared<ObjectCatalogKv>();
        MasterService service;
        Configure(service, "async");
        MasterServiceTestPeer peer(service);
        peer.SetKvTenantEpochTrackingForTesting(true);
        const auto mounted = PrepareSimpleSegment(service);
        ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
        ReplicateConfig config;
        config.os_replica_num = 1;
        auto start = service.PutStart(mounted.client_id, "key",
                                      TenantId::Default(), 6, config);
        ASSERT_TRUE(start);
        const auto target = Target(*start);
        ASSERT_TRUE(service.PutEnd(mounted.client_id,
                                   ObjectMeta{"key", 42, target.id},
                                   TenantId::Default(), ReplicaType::ALL));
        auto remove = [&]() -> long {
            if (api == 0) return service.RemoveAll(TenantId::Default(), true);
            if (api == 1) return service.RemoveAll(true);
            if (api == 2) {
                auto result =
                    service.RemoveByRegex("^key$", TenantId::Default(), true);
                EXPECT_TRUE(result);
                return result ? *result : -1;
            }
            auto results =
                service.BatchRemove({"key"}, TenantId::Default(), true);
            EXPECT_EQ(results.size(), 1u);
            return results.at(0) ? 1 : 0;
        };
        kv->fail = true;
        EXPECT_EQ(remove(), 0);
        EXPECT_EQ(peer.GetKvClearedPublishedForTesting(), 0u);
        {
            MasterServiceTestPeer::MetadataAccessorRO accessor(
                &service,
                peer.MakeObjectIdentityForRequest("key", TenantId::Default()));
            ASSERT_TRUE(accessor.Exists());
            EXPECT_TRUE(accessor.Get().AllReplicas([](const Replica& r) {
                return r.status() == ReplicaStatus::REMOVED;
            }));
        }
        kv->fail = false;
        // Retry the same bulk API against the frozen descriptors. No single-key
        // Remove or maintenance pass should be needed to finish the deletion.
        EXPECT_EQ(remove(), 1);
        EXPECT_EQ(peer.GetKvClearedPublishedForTesting(), api < 2 ? 1u : 0u);
        EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
        auto head = peer.ObjectStorageCatalogForTesting().Head(
            TenantId::Default().value(), "key");
        ASSERT_TRUE(head);
        EXPECT_TRUE(head->object_deleted);
    }
}

TEST_F(ObjectStorageMasterTest, DeferredMoveRetriesWithinPendingBudget) {
    for (bool occupied : {false, true}) {
        SCOPED_TRACE(occupied);
        kv = std::make_shared<ObjectCatalogKv>();
        MasterService service(
            MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
        Configure(service, "offload_on_evict", true);
        const auto source = PrepareSimpleSegment(service, "source");
        const auto destination = PrepareSimpleSegment(
            service, "destination", kDefaultSegmentBase + kDefaultSegmentSize);
        ASSERT_TRUE(service.RegisterObjectStorageClient(source.client_id));
        ASSERT_TRUE(service.RegisterObjectStorageClient(destination.client_id));
        ReplicateConfig config;
        config.os_replica_num = 1;
        config.preferred_segment = "source";
        std::string blocker_name;
        if (occupied) {
            auto blocker = service.PutStart(source.client_id, "blocker",
                                            TenantId::Default(), 6, config);
            ASSERT_TRUE(blocker);
            blocker_name =
                Target(*blocker).get_object_storage_descriptor().object_key;
            ASSERT_TRUE(
                service.PutEnd(source.client_id,
                               ObjectMeta{"blocker", {}, Target(*blocker).id},
                               TenantId::Default(), ReplicaType::ALL));
            MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
        }
        auto start = service.PutStart(source.client_id, "key",
                                      TenantId::Default(), 6, config);
        ASSERT_TRUE(start);
        const auto old_name =
            Target(*start).get_object_storage_descriptor().object_key;
        ASSERT_TRUE(service.PutEnd(source.client_id,
                                   ObjectMeta{"key", 42, Target(*start).id},
                                   TenantId::Default(), ReplicaType::ALL));
        ASSERT_TRUE(service.MoveStart(source.client_id, "key",
                                      TenantId::Default(), "source",
                                      "destination"));
        ASSERT_TRUE(
            service.MoveEnd(source.client_id, "key", TenantId::Default()));
        auto& catalog =
            MasterServiceTestPeer(service).ObjectStorageCatalogForTesting();
        MasterServiceTestPeer(service).RunObjectStorageMaintenance();
        if (occupied) {
            auto tasks = service.PollObjectStorageTasks(source.client_id);
            ASSERT_TRUE(tasks);
            ASSERT_EQ(tasks->size(), 1u);
            EXPECT_EQ(tasks->front().descriptor().object_key, blocker_name);
            EXPECT_TRUE(catalog.TasksFor(destination.client_id).empty());
            auto retired = catalog.Find(old_name);
            ASSERT_TRUE(retired);
            EXPECT_EQ(retired->phase, ObjectStoragePhase::RETIRED);
            EXPECT_TRUE(retired->upload_finished);
            EXPECT_FALSE(retired->upload_started);
            ASSERT_TRUE(service.BeginObjectStorageUpload(source.client_id,
                                                         blocker_name));
            ASSERT_TRUE(service.FinishObjectStorageUpload(
                source.client_id, blocker_name, ObjectUploadOutcome::COMPLETE));
            // The retired, never-started intent must not keep the slot
            // occupied.
            EXPECT_TRUE(catalog.CanReserve(6));
            MasterServiceTestPeer(service).RunObjectStorageMaintenance();
        }
        auto tasks = service.PollObjectStorageTasks(destination.client_id);
        ASSERT_TRUE(tasks);
        ASSERT_EQ(tasks->size(), 1u);
        const auto next = tasks->front();
        EXPECT_NE(next.descriptor().object_key, old_name);
        EXPECT_EQ(next.source.get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  "destination");
        EXPECT_FALSE(catalog.CanReserve(6));
        EXPECT_TRUE(catalog.TasksFor(source.client_id).empty());
        ASSERT_TRUE(service.BeginObjectStorageUpload(
            destination.client_id, next.descriptor().object_key));
        ASSERT_TRUE(service.FinishObjectStorageUpload(
            destination.client_id, next.descriptor().object_key,
            ObjectUploadOutcome::COMPLETE));
        EXPECT_TRUE(catalog.CanReserve(6));
        auto query = service.GetReplicaList("key", TenantId::Default());
        ASSERT_TRUE(query);
        EXPECT_EQ(query->object_checksum, 42u);
        EXPECT_EQ(Target(query->replicas).status, ReplicaStatus::COMPLETE);
    }
}

TEST_F(ObjectStorageMasterTest, DeferredMoveSurvivesOpLogPromotion) {
    for (const bool stale_cleanup : {false, true}) {
        SCOPED_TRACE(stale_cleanup);
        kv = std::make_shared<ObjectCatalogKv>();
        const std::string cluster = "deferred-move-test";
        const auto directory = MakeSnapshotTestTempDir("object_move_oplog_");
        ScopedEnvVar local_path(kSnapshotLocalPathEnv, directory);
        auto log_kv = std::make_shared<ObjectCatalogKv>();
        MockMetadataStore standby;
        OpLogApplier applier(&standby, cluster);
        MountedSegmentContext destination;
        std::string name;
        {
            MasterService service(
                MasterServiceConfigBuilder()
                    .set_default_kv_lease_ttl(60000)
                    .set_enable_ha(true)
                    .set_enable_oplog(true)
                    .set_cluster_id(cluster)
                    // Leave capacity for the bounded sequence of
                    // log entries before the durable barrier.
                    .set_oplog_batch_max_entries(16)
                    .build());
            ASSERT_EQ(
                MasterServiceTestPeer(service).SetBatchOpLogBackendForTesting(
                    log_kv),
                ErrorCode::OK);
            Configure(service, "offload_on_evict");
            const auto source = PrepareSimpleSegment(service, "source");
            destination =
                PrepareSimpleSegment(service, "destination",
                                     kDefaultSegmentBase + kDefaultSegmentSize);
            ASSERT_TRUE(service.RegisterObjectStorageClient(source.client_id));
            ReplicateConfig config;
            config.os_replica_num = 1;
            config.preferred_segment = "source";
            auto start = service.PutStart(source.client_id, "key",
                                          TenantId::Default(), 6, config);
            ASSERT_TRUE(start);
            name = Target(*start).get_object_storage_descriptor().object_key;
            ASSERT_TRUE(service.PutEnd(source.client_id,
                                       ObjectMeta{"key", 42, Target(*start).id},
                                       TenantId::Default(), ReplicaType::ALL));
            ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()));
            if (stale_cleanup) {
                ASSERT_TRUE(service.CopyStart(source.client_id, "key",
                                              TenantId::Default(), "source",
                                              {"destination"}));
                ASSERT_TRUE(service.CopyEnd(source.client_id, "key",
                                            TenantId::Default()));
                ASSERT_TRUE(service.UnmountSegment(source.segment_id,
                                                   source.client_id));
                ASSERT_TRUE(service.GetReplicaList("key", TenantId::Default()));
            } else {
                ASSERT_TRUE(service.MoveStart(source.client_id, "key",
                                              TenantId::Default(), "source",
                                              "destination"));
                ASSERT_TRUE(service.MoveEnd(source.client_id, "key",
                                            TenantId::Default()));
            }
            // A later no-op deletion provides a durable barrier for every entry
            // above without depending on batch timing or cleanup callbacks.
            auto barrier =
                MasterServiceTestPeer(service).AppendOpLogVisibleBeforeDurable(
                    OpType::REMOVE, TenantId::Default().value(), "barrier", "");
            ASSERT_TRUE(barrier) << toString(barrier.error());
            auto done = std::make_shared<std::promise<ErrorCode>>();
            auto ready = done->get_future();
            auto durable = MasterServiceTestPeer::OrderedOplogWriter(service)
                               ->AwaitDurable(*barrier);
            std::move(durable).thenValue(
                [done](ErrorCode error) { done->set_value(error); });
            ASSERT_EQ(ready.wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
            ASSERT_EQ(ready.get(), ErrorCode::OK);
            OpLogBatchStandbyReader reader(cluster, *log_kv, applier);
            const auto applied = reader.PollOnce();
            ASSERT_EQ(applied.error, ErrorCode::OK);
            ASSERT_GE(applied.applied_entries, 4u);
        }
        auto metadata = standby.GetMetadata(TenantId::Default().value(), "key");
        ASSERT_TRUE(metadata);
        EXPECT_EQ(metadata->replicas.size(), 2u);
        MasterService promoted;
        Configure(promoted, "offload_on_evict", false, false);
        Segment live = MakeSegment("destination",
                                   kDefaultSegmentBase + kDefaultSegmentSize);
        live.id = destination.segment_id;
        ASSERT_TRUE(promoted.MountSegment(live, destination.client_id));
        ASSERT_TRUE(promoted.RestoreFromStandbySnapshot(
            {{TenantId::Default().value(), "key", *metadata}},
            applier.GetExpectedSequenceId() - 1,
            applier.GetSegmentRegistry().GetAllSegments()));
        auto query = promoted.GetReplicaList("key", TenantId::Default());
        ASSERT_TRUE(query);
        EXPECT_EQ(query->object_checksum, 42u);
        ASSERT_EQ(query->replicas.size(), 1u);
        EXPECT_TRUE(query->replicas.front().is_memory_replica());
        ExpectPendingObjectTarget(promoted);
        EXPECT_FALSE(MasterServiceTestPeer(promoted)
                         .ObjectStorageCatalogForTesting()
                         .Find(name)
                         ->object_deleted);
        std::filesystem::remove_all(directory);
    }
}

TEST_F(ObjectStorageMasterTest, ObjectAdmissionDoesNotConsumeDfsReservations) {
    for (const std::string allocator_type : {"shard", "bucket"}) {
        for (bool batch_upsert : {false, true}) {
            SCOPED_TRACE(allocator_type);
            SCOPED_TRACE(batch_upsert);
            const auto directory =
                MakeSnapshotTestTempDir("object_dfs_admission_");
            {
                ScopedEnvVar enable("MOONCAKE_ENABLE_DFS", "1");
                ScopedEnvVar adapter_type("MOONCAKE_DFS_FS_ADAPTER", "posix");
                ScopedEnvVar root("MOONCAKE_DFS_ROOT_DIR", directory);
                ScopedEnvVar type("MOONCAKE_DFS_ALLOCATOR", allocator_type);
                ScopedEnvVar shards("MOONCAKE_DFS_SHARD_COUNT", "1");
                ScopedEnvVar shard_size("MOONCAKE_DFS_SHARD_CAPACITY", "32768");
                ScopedEnvVar bucket_size("MOONCAKE_DFS_BUCKET_CAPACITY",
                                         "32768");
                ScopedEnvVar buckets("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
                ScopedEnvVar alignment("MOONCAKE_DFS_ALIGNMENT", "4096");
                ScopedEnvVar eviction("MOONCAKE_DFS_EVICTION_ENABLED", "0");
                ScopedEnvVar deferred_free("MOONCAKE_DFS_DEFERRED_FREE_SECONDS",
                                           "0");
                ScopedEnvVar single_tenant("MOONCAKE_DFS_SINGLE_TENANT",
                                           "true");
                kv = std::make_shared<ObjectCatalogKv>();
                MasterService service(MasterServiceConfigBuilder()
                                          .set_default_kv_lease_ttl(0)
                                          .build());
                const auto mounted = PrepareSimpleSegment(service);
                auto& dfs =
                    MasterServiceTestPeer(service).DfsAllocatorForTesting();
                ASSERT_TRUE(dfs.IsInitialized());
                ASSERT_EQ(dfs.Type(), allocator_type == "shard"
                                          ? DfsAllocatorType::SHARD
                                          : DfsAllocatorType::BUCKET);
                const auto initial_dfs_bytes = dfs.GetUsedBytes();
                ReplicateConfig config;
                config.os_replica_num = config.dfs_replica_num = 1;
                auto start = [&](const std::string& key, uint64_t bytes,
                                 const ReplicateConfig& requested) {
                    if (batch_upsert) {
                        auto results = service.BatchUpsertStart(
                            mounted.client_id, {key}, TenantId::Default(),
                            {bytes}, requested);
                        return std::move(results.at(0));
                    }
                    return service.PutStart(mounted.client_id, key,
                                            TenantId::Default(), bytes,
                                            requested);
                };
                auto expect_rejected =
                    [&](const std::string& key, uint64_t bytes,
                        const ReplicateConfig& requested, ErrorCode error) {
                        const auto memory_before =
                            service.GetStorageUsageSnapshot().memory.used_bytes;
                        auto result = start(key, bytes, requested);
                        ASSERT_FALSE(result);
                        EXPECT_EQ(result.error(), error);
                        EXPECT_EQ(dfs.GetUsedBytes(), initial_dfs_bytes);
                        EXPECT_EQ(
                            service.GetStorageUsageSnapshot().memory.used_bytes,
                            memory_before);
                        if (auto* bucket =
                                dynamic_cast<ImmutableBucketAllocator*>(&dfs)) {
                            EXPECT_FALSE(bucket->GetBucketIdForKey(key));
                        }
                    };
                // All admission early exits must precede non-RAII DFS space.
                expect_rejected("disabled-tier", 6, config,
                                ErrorCode::INVALID_PARAMS);
                Configure(service, "sync", true);
                auto invalid = config;
                invalid.os_replica_num = 2;
                expect_rejected("invalid-count", 6, invalid,
                                ErrorCode::INVALID_PARAMS);
                expect_rejected("too-large", 7, config,
                                ErrorCode::NO_AVAILABLE_HANDLE);
                auto os_only = config;
                os_only.dfs_replica_num = 0;
                auto occupier =
                    service.PutStart(mounted.client_id, "occupier",
                                     TenantId::Default(), 6, os_only);
                ASSERT_TRUE(occupier);
                auto& catalog = MasterServiceTestPeer(service)
                                    .ObjectStorageCatalogForTesting();
                ASSERT_FALSE(catalog.CanReserve(6));
                expect_rejected("retry-key", 6, config,
                                ErrorCode::NO_AVAILABLE_HANDLE);
                expect_rejected("retry-key", 6, config,
                                ErrorCode::NO_AVAILABLE_HANDLE);
                ASSERT_TRUE(service.RevokeObjectStorageWrite(
                    mounted.client_id, "occupier", TenantId::Default(),
                    Target(*occupier).id));
                MasterServiceTestPeer(service).RunObjectStorageMaintenance();
                ASSERT_TRUE(catalog.CanReserve(6));
                auto retry = start("retry-key", 6, config);
                ASSERT_TRUE(retry) << toString(retry.error());
                ASSERT_EQ(retry->size(), 3u);
                EXPECT_EQ(std::count_if(
                              retry->begin(), retry->end(),
                              [](const auto& r) { return r.is_dfs_replica(); }),
                          1);
                EXPECT_EQ(
                    std::count_if(retry->begin(), retry->end(),
                                  [](const auto& r) {
                                      return r.is_object_storage_replica();
                                  }),
                    1);
                const auto allocated_dfs_bytes = dfs.GetUsedBytes();
                EXPECT_GT(allocated_dfs_bytes, initial_dfs_bytes);
                ASSERT_TRUE(service.RevokeObjectStorageWrite(
                    mounted.client_id, "retry-key", TenantId::Default(),
                    Target(*retry).id));
                if (auto* bucket =
                        dynamic_cast<ImmutableBucketAllocator*>(&dfs)) {
                    EXPECT_FALSE(bucket->GetBucketIdForKey("retry-key"));
                }
                MasterServiceTestPeer(service).RunObjectStorageMaintenance();
                auto after_revoke = start("retry-key", 6, config);
                ASSERT_TRUE(after_revoke) << toString(after_revoke.error());
                // Shard Free queues deferred release; Allocate reclaims it.
                if (allocator_type == "shard") {
                    EXPECT_EQ(dfs.GetUsedBytes(), allocated_dfs_bytes);
                }
                {
                    const auto shard_index =
                        MasterServiceTestPeer(service).getShardIndex(
                            TenantId::Default(), "retry-key");
                    MasterServiceTestPeer::MetadataShardAccessorRW shard(
                        &service, shard_index);
                    MasterServiceTestPeer(service)
                        .DiscardExpiredProcessingReplicas(
                            shard, std::chrono::system_clock::now() +
                                       std::chrono::hours(1));
                }
                if (auto* bucket =
                        dynamic_cast<ImmutableBucketAllocator*>(&dfs)) {
                    EXPECT_FALSE(bucket->GetBucketIdForKey("retry-key"));
                }
                MasterServiceTestPeer(service).RunObjectStorageMaintenance();
                auto after_timeout = start("retry-key", 6, config);
                ASSERT_TRUE(after_timeout) << toString(after_timeout.error());
                if (allocator_type == "shard") {
                    EXPECT_EQ(dfs.GetUsedBytes(), allocated_dfs_bytes);
                }
                ASSERT_TRUE(service.RevokeObjectStorageWrite(
                    mounted.client_id, "retry-key", TenantId::Default(),
                    Target(*after_timeout).id));
            }
            std::filesystem::remove_all(directory);
        }
    }
}

TEST_F(ObjectStorageMasterTest, PendingUploadsDoNotStopUnrelatedEviction) {
    for (bool grouped : {false, true}) {
        for (bool single_slot : {false, true}) {
            SCOPED_TRACE(grouped);
            SCOPED_TRACE(single_slot);
            kv = std::make_shared<ObjectCatalogKv>();
            MasterService service(MasterServiceConfigBuilder()
                                      .set_default_kv_lease_ttl(0)
                                      .build());
            Configure(service, "offload_on_evict", single_slot);
            const auto mounted = PrepareSimpleSegment(service);
            ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
            ReplicateConfig config;
            config.os_replica_num = 1;
            if (grouped)
                config.group_ids = std::vector<std::string>{"mixed-group"};
            for (int i = 0; i < 3; ++i) {
                const auto key = "pending-" + std::to_string(i);
                auto start = service.PutStart(mounted.client_id, key,
                                              TenantId::Default(), 6, config);
                ASSERT_TRUE(start);
                ASSERT_TRUE(service.PutEnd(
                    mounted.client_id, ObjectMeta{key, {}, Target(*start).id},
                    TenantId::Default(), ReplicaType::ALL));
            }
            config.os_replica_num = 0;
            for (int i = 0; i < 3; ++i) {
                const auto key = "memory-" + std::to_string(i);
                ASSERT_TRUE(service.PutStart(mounted.client_id, key,
                                             TenantId::Default(), 6, config));
                ASSERT_TRUE(service.PutEnd(mounted.client_id, key,
                                           TenantId::Default(),
                                           ReplicaType::ALL));
            }
            // One scan must visit every key, regardless of shard/key ordering.
            // With one slot, the remaining intents must not stop useful work.
            MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
            auto records = MasterServiceTestPeer(service)
                               .ObjectStorageCatalogForTesting()
                               .Records();
            EXPECT_EQ(std::count_if(records.begin(), records.end(),
                                    [](const ObjectStorageRecord& r) {
                                        return r.phase ==
                                               ObjectStoragePhase::READY;
                                    }),
                      single_slot ? 1 : 3);
            for (int i = 0; i < 3; ++i) {
                auto query = service.GetReplicaList(
                    "pending-" + std::to_string(i), TenantId::Default());
                ASSERT_TRUE(query);
                ASSERT_EQ(query->replicas.size(), 1u);
                EXPECT_TRUE(query->replicas.front().is_memory_replica());
                EXPECT_FALSE(service.GetReplicaList(
                    "memory-" + std::to_string(i), TenantId::Default()));
            }
        }
    }
}

TEST_F(ObjectStorageMasterTest, PressureSchedulesBeforeEvictingLastMemory) {
    MasterService service(
        MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
    Configure(service, "offload_on_evict");
    const auto mounted = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "pressure",
                                  TenantId::Default(), 4096, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"pressure", {}, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    auto tasks = service.PollObjectStorageTasks(mounted.client_id);
    ASSERT_TRUE(tasks);
    EXPECT_TRUE(tasks->empty());
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    tasks = service.PollObjectStorageTasks(mounted.client_id);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    auto query = service.GetReplicaList("pressure", TenantId::Default());
    ASSERT_TRUE(query);
    EXPECT_TRUE(
        std::any_of(query->replicas.begin(), query->replicas.end(),
                    [](const auto& r) { return r.is_memory_replica(); }));
    const auto name = tasks->front().descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    query = service.GetReplicaList("pressure", TenantId::Default());
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1u);
    EXPECT_TRUE(query->replicas.front().is_object_storage_replica());
}

TEST_F(ObjectStorageMasterTest,
       BatchClearPropagatesFailureAndCannotRecoverDeletedKey) {
    MasterService service(
        MasterServiceConfigBuilder().set_default_kv_lease_ttl(0).build());
    Configure(service, "sync");
    const auto mounted = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    const auto name = target.get_object_storage_descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"key", {}, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    kv->fail = true;
    auto cleared = service.BatchReplicaClear({"key"}, mounted.client_id, "");
    ASSERT_TRUE(cleared);
    EXPECT_TRUE(cleared->empty());
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
    kv->fail = false;
    cleared = service.BatchReplicaClear({"key"}, mounted.client_id, "");
    ASSERT_TRUE(cleared);
    EXPECT_EQ(*cleared, std::vector<std::string>{"key"});
    auto& catalog =
        MasterServiceTestPeer(service).ObjectStorageCatalogForTesting();
    ASSERT_EQ(catalog.Reclaim(*adapter, std::numeric_limits<uint64_t>::max()),
              ErrorCode::OK);
    EXPECT_FALSE(catalog.Find(name));
    EXPECT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    ASSERT_EQ(MasterServiceTestPeer(service).RecoverObjectStorage(),
              ErrorCode::OK);
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
}

TEST_F(ObjectStorageMasterTest,
       LogicalTombstoneRemovesStaleMemorySnapshotAfterGc) {
    MasterService service;
    Configure(service, "sync");
    const auto mounted = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    const auto name = target.get_object_storage_descriptor().object_key;
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    ASSERT_TRUE(service.PutEnd(mounted.client_id,
                               ObjectMeta{"key", {}, target.id},
                               TenantId::Default(), ReplicaType::ALL));
    // Model a lagging snapshot with MEMORY only, while durable deletion and
    // physical GC have both completed outside that snapshot.
    for (auto& shard : MasterServiceTestPeer::MetadataShards(service)) {
        for (auto& [tenant, state] : shard.tenants) {
            auto it = state.metadata.find("key");
            if (it != state.metadata.end())
                it->second.EraseReplicaByID(target.id);
        }
    }
    auto& catalog =
        MasterServiceTestPeer(service).ObjectStorageCatalogForTesting();
    ASSERT_EQ(catalog.Retire(name, 0), ErrorCode::OK);
    ASSERT_EQ(catalog.Reclaim(*adapter, std::numeric_limits<uint64_t>::max()),
              ErrorCode::OK);
    ASSERT_EQ(MasterServiceTestPeer(service).RecoverObjectStorage(),
              ErrorCode::OK);
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
    // Reusing the logical key with a newer MEMORY generation must still work.
    ReplicateConfig memory_only;
    ASSERT_TRUE(service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                 6, memory_only));
    ASSERT_TRUE(service.PutEnd(mounted.client_id, ObjectMeta{"key", {}},
                               TenantId::Default(), ReplicaType::ALL));
    ASSERT_EQ(MasterServiceTestPeer(service).RecoverObjectStorage(),
              ErrorCode::OK);
    EXPECT_TRUE(service.GetReplicaList("key", TenantId::Default()));
}

TEST_F(ObjectStorageMasterTest,
       UnknownRetirementCannotBeUndoneByLateCompletion) {
    MasterService service;
    Configure(service, "sync");
    const auto mounted = PrepareSimpleSegment(service);
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    const auto name = target.get_object_storage_descriptor().object_key;
    const ObjectMeta meta{"key", {}, target.id};
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    ASSERT_TRUE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                               ReplicaType::ALL));
    kv->lose_reply = kv->lose_reconciliation = true;
    EXPECT_FALSE(service.Remove("key", TenantId::Default(), true));
    kv->lose_reply = kv->lose_reconciliation = false;
    // The local catalog still has COMPLETE, but the deletion intent in
    // metadata must dominate a late completion or End retry.
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
    EXPECT_FALSE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                                ReplicaType::ALL));
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
    auto removed = service.Remove("key", TenantId::Default(), true);
    if (!removed) removed = service.Remove("key", TenantId::Default(), true);
    ASSERT_TRUE(removed);
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
}

TEST_F(ObjectStorageMasterTest,
       LostCommitAndReconciliationRepliesRecoverReferences) {
    MasterService service;
    Configure(service, "async");
    const auto mounted = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.RegisterObjectStorageClient(mounted.client_id));
    ReplicateConfig config;
    config.os_replica_num = 1;
    auto start = service.PutStart(mounted.client_id, "key", TenantId::Default(),
                                  6, config);
    ASSERT_TRUE(start);
    const auto target = Target(*start);
    const auto name = target.get_object_storage_descriptor().object_key;
    const ObjectMeta meta{"key", {}, target.id};
    kv->lose_reply = kv->lose_reconciliation = true;
    EXPECT_FALSE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                                ReplicaType::ALL));
    kv->lose_reply = kv->lose_reconciliation = false;
    ASSERT_TRUE(service.PutEnd(mounted.client_id, meta, TenantId::Default(),
                               ReplicaType::ALL));
    kv->lose_reply = kv->lose_reconciliation = true;
    EXPECT_FALSE(service.BeginObjectStorageUpload(mounted.client_id, name));
    kv->lose_reply = kv->lose_reconciliation = false;
    // The first cancellation discovers the previously committed claim; the
    // second uses its current revision. Neither reissues PUT.
    EXPECT_FALSE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::NOT_STARTED));
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, name, ObjectUploadOutcome::NOT_STARTED));
    MasterServiceTestPeer(service).RunObjectStorageMaintenance();
    auto tasks = service.PollObjectStorageTasks(mounted.client_id);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(tasks->size(), 1u);
    const auto next_name = tasks->front().descriptor().object_key;
    ASSERT_NE(next_name, name);
    ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, next_name));
    kv->lose_reply = kv->lose_reconciliation = true;
    EXPECT_FALSE(service.FinishObjectStorageUpload(
        mounted.client_id, next_name, ObjectUploadOutcome::COMPLETE));
    kv->lose_reply = kv->lose_reconciliation = false;
    ASSERT_TRUE(service.FinishObjectStorageUpload(
        mounted.client_id, next_name, ObjectUploadOutcome::COMPLETE));
    // A leaked reference would reject this replacement as OBJECT_REPLICA_BUSY.
    ASSERT_TRUE(service.UpsertStart(mounted.client_id, "key",
                                    TenantId::Default(), 6, config));
}

TEST_F(ObjectStorageMasterTest,
       CompletedObjectRestoresWithoutOwnerAndRetirementDoesNotResurrect) {
    StandbyObjectEntry snapshot;
    snapshot.key = "key";
    snapshot.metadata.size = 6;
    {
        MasterService service;
        Configure(service, "sync");
        const auto mounted = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.os_replica_num = 1;
        auto start = service.PutStart(mounted.client_id, "key",
                                      TenantId::Default(), 6, config);
        ASSERT_TRUE(start);
        auto target = Target(*start);
        const auto name = target.get_object_storage_descriptor().object_key;
        auto restored_target = target;
        restored_target.status = ReplicaStatus::COMPLETE;
        snapshot.metadata.client_id = mounted.client_id;
        snapshot.metadata.replicas = {restored_target};
        Replica original(target.get_object_storage_descriptor(),
                         ReplicaStatus::COMPLETE);
        msgpack::sbuffer bytes;
        MsgpackPacker packer(bytes);
        const auto view =
            MasterServiceTestPeer::SegmentManager(service).getView();
        ASSERT_TRUE(Serializer<Replica>::serialize(original, view, packer));
        const auto object = msgpack::unpack(bytes.data(), bytes.size());
        auto decoded = Serializer<Replica>::deserialize(object.get(), view);
        ASSERT_TRUE(decoded);
        EXPECT_EQ((*decoded)->get_object_storage_descriptor().namespace_id,
                  original.get_object_storage_descriptor().namespace_id);
        EXPECT_EQ((*decoded)->get_object_storage_descriptor().object_key, name);

        ASSERT_TRUE(service.BeginObjectStorageUpload(mounted.client_id, name));
        ASSERT_TRUE(service.FinishObjectStorageUpload(
            mounted.client_id, name, ObjectUploadOutcome::COMPLETE));
        ASSERT_TRUE(service.PutEnd(mounted.client_id,
                                   ObjectMeta{"key", 42, target.id},
                                   TenantId::Default(), ReplicaType::ALL));
    }
    {
        MasterService service;
        Configure(service, "sync");
        ASSERT_TRUE(service.RestoreFromStandbySnapshot({snapshot}, 0, {}));
        auto query = service.GetReplicaList("key", TenantId::Default());
        ASSERT_TRUE(query);
        ASSERT_EQ(query->replicas.size(), 1u);
        EXPECT_TRUE(query->replicas.front().is_object_storage_replica());
        EXPECT_EQ(query->object_checksum, 42u);
        ASSERT_TRUE(service.Remove("key", TenantId::Default(), true));
    }
    MasterService service;
    Configure(service, "sync");
    ASSERT_EQ(MasterServiceTestPeer(service).RecoverObjectStorage(),
              ErrorCode::OK);
    EXPECT_FALSE(service.GetReplicaList("key", TenantId::Default()));
}
}  // namespace
}  // namespace mooncake::test
