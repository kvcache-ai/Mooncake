#include "ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.h"

#include <gtest/gtest.h>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#ifdef STORE_USE_ETCD
#include <unistd.h>
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#endif

namespace mooncake::test {
namespace {
class FakeBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        ++delete_count;
        EXPECT_TRUE(values.contains(
            ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster")));
        if (before_delete) before_delete();
        return delete_error;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        const std::string owned_key(key);
        values[owned_key] = std::string(value);
        if (!create_revisions.contains(owned_key)) {
            create_revisions[owned_key] = next_create_revision++;
        }
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>&) override {
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        ++txn_count;
        if (!mutate_key.empty()) {
            values[mutate_key] = mutate_value;
            mutate_key.clear();
        }
        if (next_txn_error != ErrorCode::OK) {
            const auto error = next_txn_error;
            next_txn_error = ErrorCode::OK;
            return error;
        }
        for (const auto& compare : txn.compares) {
            const auto it = values.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != values.end()) return ErrorCode::ETCD_TRANSACTION_FAIL;
            } else if (compare.kind == KvCompareKind::kCreateRevisionEquals) {
                auto revision = create_revisions.find(compare.key);
                if (revision == create_revisions.end() ||
                    revision->second != compare.expected_revision) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == values.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            values[put.key] = put.value;
            if (!create_revisions.contains(put.key)) {
                create_revisions[put.key] = next_create_revision++;
            }
        }
        if (after_txn) after_txn();
        return ErrorCode::OK;
    }

    size_t delete_count{0};
    ErrorCode delete_error{ErrorCode::OK};
    std::function<void()> before_delete;
    std::function<void()> after_txn;
    std::map<std::string, std::string> values;
    std::map<std::string, EtcdRevisionId> create_revisions;
    ErrorCode next_txn_error{ErrorCode::OK};
    std::string mutate_key;
    std::string mutate_value;
    size_t txn_count{0};
    EtcdRevisionId next_create_revision{1};
};

class RecordingObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string&, const std::vector<uint8_t>&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& bytes) override {
        ++buffer_downloads;
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        bytes = it->second;
        return {};
    }

    tl::expected<void, std::string> UploadString(const std::string&,
                                                 const std::string&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& value) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        value.assign(it->second.begin(), it->second.end());
        return {};
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        deleted.push_back(prefix);
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& keys) override {
        for (const auto& [key, _] : objects)
            if (key.starts_with(prefix)) keys.push_back(key);
        return {};
    }

    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        if (inspection_error) return tl::make_unexpected("unknown metadata");
        return SnapshotObjectInspection{
            .stored_size = it->second.size(),
            .crc32c = checksums ? std::optional<uint32_t>(
                                      Crc32cValue(it->second.data(),
                                                  it->second.size()) ^
                                      (bad_checksum ? 1 : 0))
                                : std::nullopt};
    }

    std::string GetConnectionInfo() const override { return "test"; }

    bool checksums{true}, bad_checksum{false}, inspection_error{false};
    size_t buffer_downloads{0};
    std::vector<std::string> deleted;
    std::map<std::string, std::vector<uint8_t>> objects;
};

std::string AddSnapshot(RecordingObjectStore& store, uint64_t batch,
                        int64_t lease_id = 101) {
    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = ha::BuildBatchOpLogSnapshotId(batch, lease_id);
    manifest.last_included_batch_id = batch;
    manifest.last_included_seq = batch * 10;
    manifest.producer_view_version = 7;
    const std::string root = "snapshots";
    manifest.segments = {
        ha::BuildBatchOpLogSnapshotSegmentsKey(root, manifest.snapshot_id), 1,
        Crc32cValue("s", 1)};
    manifest.nof_segments = {
        ha::BuildBatchOpLogSnapshotNoFSegmentsKey(root, manifest.snapshot_id),
        1, Crc32cValue("n", 1)};
    manifest.object_chunks.push_back({0,
                                      ha::BuildBatchOpLogSnapshotObjectChunkKey(
                                          root, manifest.snapshot_id, 0),
                                      1, 1, Crc32cValue("o", 1)});
    store.objects[manifest.segments.key] = {'s'};
    store.objects[manifest.nof_segments.key] = {'n'};
    store.objects[manifest.object_chunks[0].key] = {'o'};
    const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = manifest.snapshot_id;
    descriptor.last_included_batch_id = batch;
    descriptor.last_included_seq = manifest.last_included_seq;
    descriptor.producer_view_version = manifest.producer_view_version;
    descriptor.manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey(root, manifest.snapshot_id);
    descriptor.manifest_size = bytes.size();
    descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
    descriptor.created_at_ms = 1;
    store.objects[descriptor.manifest_key] = {bytes.begin(), bytes.end()};
    const auto json = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    store.objects[ha::BuildBatchOpLogSnapshotDescriptorKey(
        root, manifest.snapshot_id)] = {json.begin(), json.end()};
    return json;
}

class PruningTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(ErrorCode::OK, backend.Put(lock_key, "101"));
        latest = AddSnapshot(store, 20);
        fallback = AddSnapshot(store, 10);
        backend.Put(latest_key, latest);
        backend.Put(fallback_key, fallback);
    }
    ErrorCode Run() { return pruning.Run(*lease); }
    const std::string latest_key =
        ha::BuildBatchOpLogSnapshotLatestKey("cluster");
    const std::string fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey("cluster");
    const std::string lock_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster");
    const std::string floor_key =
        ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster");
    FakeBackend backend;
    RecordingObjectStore store;
    std::unique_ptr<SnapshotMaintenanceLease> lease =
        SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    BatchOpLogPruningCoordinator pruning{backend, store, "cluster",
                                         "snapshots"};
    std::string latest, fallback;
};

TEST_F(PruningTest, FirstSnapshotDoesNotPublishFloor) {
    backend.values.erase(fallback_key);
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_FALSE(backend.values.contains(floor_key));
    EXPECT_EQ(0u, backend.txn_count);
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, PublishesFloorBeforeDeletionAndRotates) {
    backend.before_delete = [&] { EXPECT_EQ("10", backend.values[floor_key]); };
    ASSERT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(1u, backend.delete_count);
    backend.before_delete = [&] { EXPECT_EQ("20", backend.values[floor_key]); };
    ASSERT_EQ(ErrorCode::OK, BatchOpLogSnapshotPublisher(backend, "cluster")
                                 .Publish(*lease, AddSnapshot(store, 30)));
    ASSERT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(2u, backend.delete_count);
}

TEST_F(PruningTest, RequiresIndependentOrderedSnapshots) {
    for (const auto& value :
         {latest, AddSnapshot(store, 20, 102), AddSnapshot(store, 30)}) {
        backend.values[fallback_key] = value;
        EXPECT_EQ(ErrorCode::OK, Run());
        EXPECT_FALSE(backend.values.contains(floor_key));
    }
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, RejectsCorruptPointersAndArtifacts) {
    const auto original_objects = store.objects;
    for (const auto& pointer_key : {latest_key, fallback_key}) {
        const auto pointer = backend.values[pointer_key];
        backend.values[pointer_key] = "invalid";
        EXPECT_NE(ErrorCode::OK, Run());
        backend.values[pointer_key] = pointer;
    }
    // Exercise every artifact of both snapshots, including descriptor raw
    // bytes, manifest CRC, memory and NoF segments, and chunks.
    for (const auto& [key, bytes] : original_objects) {
        SCOPED_TRACE(key);
        store.objects.erase(key);
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
        store.objects[key].push_back('x');
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
        store.objects[key][0] ^= 1;
        EXPECT_NE(ErrorCode::OK, Run());
        store.objects = original_objects;
    }
    EXPECT_FALSE(backend.values.contains(floor_key));
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, RejectsManifestIdentityDespiteValidCrc) {
    const auto original_objects = store.objects;
    for (const auto& key : {latest_key, fallback_key}) {
        store.objects = original_objects;
        const auto original = backend.values[key];
        auto descriptor = *ha::DecodeBatchOpLogSnapshotDescriptor(original);
        const auto manifest_bytes = store.objects[descriptor.manifest_key];
        auto manifest = *ha::DecodeBatchOpLogSnapshotManifest(
            std::string(manifest_bytes.begin(), manifest_bytes.end()));
        ++manifest.last_included_seq;
        const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
        store.objects[descriptor.manifest_key] = {bytes.begin(), bytes.end()};
        descriptor.manifest_size = bytes.size();
        descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
        const auto json = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
        backend.values[key] = json;
        store.objects[ha::BuildBatchOpLogSnapshotDescriptorKey(
            "snapshots", descriptor.snapshot_id)] = {json.begin(), json.end()};
        EXPECT_NE(ErrorCode::OK, Run());
        backend.values[key] = original;
    }
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, InspectionUnknownOrChecksumMismatchFailsClosed) {
    store.bad_checksum = true;
    EXPECT_NE(ErrorCode::OK, Run());
    store.bad_checksum = false;
    store.inspection_error = true;
    EXPECT_NE(ErrorCode::OK, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, UnsupportedChecksumNeverDownloadsChunkBodies) {
    store.checksums = false;
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(0u, store.buffer_downloads);
    EXPECT_EQ(1u, backend.delete_count);
}

TEST_F(PruningTest, CasFencesPointersLeaseAndRawFloor) {
    for (const auto& key : {latest_key, fallback_key, lock_key, floor_key}) {
        SCOPED_TRACE(key);
        const auto values = backend.values;
        backend.mutate_key = key;
        backend.mutate_value = "changed";
        EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
        EXPECT_EQ(0u, backend.delete_count);
        backend.values = values;
    }
    backend.values[floor_key] = "0009";
    backend.mutate_key = floor_key;
    backend.mutate_value = "9";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    backend.create_revisions[lock_key] = 99;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

TEST_F(PruningTest, FailedOrUnknownTxnDoesNotDelete) {
    for (auto error :
         {ErrorCode::ETCD_OPERATION_ERROR, ErrorCode::ETCD_TRANSACTION_FAIL}) {
        backend.next_txn_error = error;
        EXPECT_EQ(error, Run());
        EXPECT_EQ(0u, backend.delete_count);
    }
}

TEST_F(PruningTest, DeleteFailureKeepsFloorAndRetries) {
    backend.delete_error = ErrorCode::ETCD_OPERATION_ERROR;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, Run());
    EXPECT_EQ("10", backend.values[floor_key]);
    backend.delete_error = ErrorCode::OK;
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(2u, backend.delete_count);
    EXPECT_EQ("10", backend.values[floor_key]);
}

TEST_F(PruningTest, CrashAfterCasLeavesReaderVisibleFloorForRetry) {
    bool stopped = false;
    backend.after_txn = [&] { stopped = true; };
    EXPECT_EQ(ErrorCode::OK, pruning.Run(*lease, [&] { return stopped; }));
    EXPECT_EQ("10", backend.values[floor_key]);
    EXPECT_EQ(0u, backend.delete_count);
    backend.after_txn = {};
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ(1u, backend.delete_count);
}

TEST_F(PruningTest, FloorNeverRegressesAndParsingIsStrict) {
    backend.values[floor_key] = "11";
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ("11", backend.values[floor_key]);
    EXPECT_EQ(0u, backend.delete_count);
    for (const auto& value :
         {"", "-1", "+1", " 1", "1x", "18446744073709551616"}) {
        backend.values[floor_key] = value;
        EXPECT_NE(ErrorCode::OK, Run());
    }
    EXPECT_EQ(0u, backend.txn_count);
    backend.values[floor_key] = "0009";
    EXPECT_EQ(ErrorCode::OK, Run());
    EXPECT_EQ("10", backend.values[floor_key]);
}

TEST_F(PruningTest, CancelledOrReleasedLeaseDoesNotDelete) {
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              pruning.Run(*lease, [] { return true; }));
    ASSERT_EQ(ErrorCode::OK, lease->Release());
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, Run());
    EXPECT_EQ(0u, backend.delete_count);
}

#ifdef STORE_USE_ETCD
TEST(PruningRealEtcdTest, FloorPrecedesInclusiveDeleteAndProtectsControlKeys) {
    constexpr char endpoints[] = "127.0.0.1:2379";
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::ConnectToEtcdStoreClient(endpoints));
    const std::string cluster = "n12-test-" + std::to_string(getpid());
    EtcdHaKvBackend backend;
    const std::string root = "/oplog/" + cluster + "/";
    std::string ignored;
    const auto probe = backend.Get(root + "probe", ignored);
    if (probe != ErrorCode::OK && probe != ErrorCode::ETCD_KEY_NOT_EXIST)
        GTEST_SKIP() << "etcd is not reachable at " << endpoints;
    ASSERT_EQ(ErrorCode::OK, backend.DeleteRange(root, root + "\xff"));
    SnapshotMaintenanceLease lease(cluster);
    ASSERT_EQ(ErrorCode::OK, lease.Acquire());
    RecordingObjectStore store;
    BatchOpLogSnapshotPublisher publisher(backend, cluster);
    ASSERT_EQ(
        ErrorCode::OK,
        publisher.Publish(lease, AddSnapshot(store, 10, lease.lease_id())));
    ASSERT_EQ(
        ErrorCode::OK,
        publisher.Publish(lease, AddSnapshot(store, 20, lease.lease_id())));
    for (uint64_t id : {0, 1, 10, 11, 20, 21})
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put(BuildBatchRecordKey(cluster, id), "batch"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey(cluster), "durable"));
    ASSERT_EQ(ErrorCode::OK, backend.Put(BuildProducerViewKey(cluster), "7"));
    BatchOpLogPruningCoordinator pruning(backend, store, cluster, "snapshots");
    ASSERT_EQ(ErrorCode::OK, pruning.Run(lease));
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Get(ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster),
                    ignored));
    EXPECT_EQ("10", ignored);
    for (uint64_t id : {0, 1, 10})
        EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
                  backend.Get(BuildBatchRecordKey(cluster, id), ignored));
    for (const auto& key :
         {BuildBatchRecordKey(cluster, 11), BuildBatchRecordKey(cluster, 20),
          BuildBatchRecordKey(cluster, 21), BuildDurablePrefixKey(cluster),
          BuildProducerViewKey(cluster),
          ha::BuildBatchOpLogSnapshotLatestKey(cluster),
          ha::BuildBatchOpLogSnapshotFallbackKey(cluster), lease.lock_key()})
        EXPECT_EQ(ErrorCode::OK, backend.Get(key, ignored));
    EXPECT_EQ(ErrorCode::OK, pruning.Run(lease));
    ASSERT_EQ(ErrorCode::OK, lease.Release());
    ASSERT_EQ(ErrorCode::OK, backend.DeleteRange(root, root + "\xff"));
}
#endif

}  // namespace
}  // namespace mooncake::test
