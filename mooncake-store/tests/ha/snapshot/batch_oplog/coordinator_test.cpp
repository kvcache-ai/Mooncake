#include "ha_metric_manager.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_types.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "hot_standby_service.h"

namespace mooncake::test {
namespace {

class EmptyBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        if (get_error != ErrorCode::OK) return get_error;
        auto it = values.find(std::string(key));
        if (it == values.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        values[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>& output) override {
        output.clear();
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }

    ErrorCode get_error{ErrorCode::OK};
    std::map<std::string, std::string> values;
};

class UnusedObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string&, const std::vector<uint8_t>&) override {
        return {};
    }
    tl::expected<void, std::string> DownloadBuffer(
        const std::string&, std::vector<uint8_t>&) override {
        return tl::make_unexpected("unused");
    }
    tl::expected<void, std::string> UploadString(const std::string&,
                                                 const std::string&) override {
        return {};
    }
    tl::expected<void, std::string> DownloadString(const std::string&,
                                                   std::string&) override {
        return tl::make_unexpected("unused");
    }
    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string&) override {
        return {};
    }
    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string&, std::vector<std::string>& output) override {
        output.clear();
        return {};
    }
    std::string GetConnectionInfo() const override { return "unused"; }
};

class RecordingBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        std::lock_guard<std::mutex> lock(mutex_);
        EXPECT_EQ(
            "1",
            values_[ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster")]);
        ++delete_count;
        values_.erase(values_.lower_bound(std::string(begin)),
                      values_.lower_bound(std::string(end)));
        return ErrorCode::OK;
    }
    size_t delete_count{0};

    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = values_.find(std::string(key));
        if (it == values_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::string owned_key(key);
        values_[owned_key] = std::string(value);
        create_revisions_.try_emplace(owned_key, next_revision_++);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& output) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pause_replay.load()) return ErrorCode::ETCD_OPERATION_ERROR;
        output.clear();
        for (const auto& [key, value] : values_) {
            if (key >= begin && key < end &&
                (limit == 0 || output.size() < limit)) {
                output.push_back({key, value});
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& compare : txn.compares) {
            auto it = values_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != values_.end())
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
            } else if (compare.kind == KvCompareKind::kCreateRevisionEquals) {
                auto revision = create_revisions_.find(compare.key);
                if (revision == create_revisions_.end() ||
                    revision->second != compare.expected_revision) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == values_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            values_[put.key] = put.value;
            create_revisions_.try_emplace(put.key, next_revision_++);
        }
        return ErrorCode::OK;
    }

    bool Contains(std::string_view key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return values_.contains(std::string(key));
    }

    std::atomic<bool> pause_replay{false};

   private:
    mutable std::mutex mutex_;
    std::map<std::string, std::string> values_;
    std::map<std::string, EtcdRevisionId> create_revisions_;
    EtcdRevisionId next_revision_{1};
};

class RecordingObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override {
        if (on_upload) on_upload();
        objects_[key] = buffer;
        return {};
    }
    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override {
        auto it = objects_.find(key);
        if (it == objects_.end()) return tl::make_unexpected("not found");
        buffer = it->second;
        return {};
    }
    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& value) override {
        objects_[key] = std::vector<uint8_t>(value.begin(), value.end());
        return {};
    }
    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& value) override {
        std::vector<uint8_t> bytes;
        auto result = DownloadBuffer(key, bytes);
        if (!result) return result;
        value.assign(bytes.begin(), bytes.end());
        return {};
    }
    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        for (auto it = objects_.begin(); it != objects_.end();) {
            if (it->first.starts_with(prefix))
                it = objects_.erase(it);
            else
                ++it;
        }
        return {};
    }
    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& output) override {
        if (prefix == "snapshots/batch-oplog/") {
            if (gc_failure == 1) return tl::make_unexpected("GC list failed");
            if (gc_failure == 2) throw std::runtime_error("GC list threw");
        }
        output.clear();
        for (const auto& [key, value] : objects_) {
            (void)value;
            if (key.starts_with(prefix)) output.push_back(key);
        }
        return {};
    }
    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override {
        auto it = objects_.find(key);
        if (it == objects_.end()) return tl::make_unexpected("not found");
        return SnapshotObjectInspection{.stored_size = it->second.size(),
                                        .crc32c = std::nullopt};
    }
    std::string GetConnectionInfo() const override { return "recording"; }
    int gc_failure{0};
    std::function<void()> on_upload;

   private:
    std::map<std::string, std::vector<uint8_t>> objects_;
};

OpLogBatchRecord MakeBatch() {
    OpLogEntry entry;
    entry.sequence_id = 1;
    entry.op_type = OpType::REMOVE;
    entry.tenant_id = "tenant";
    entry.object_key = "key";
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    OpLogBatchRecord batch;
    batch.batch_id = 1;
    batch.first_seq = 1;
    batch.last_seq = 1;
    batch.entries.push_back(std::move(entry));
    return batch;
}

}  // namespace

TEST(BatchOpLogSnapshotCoordinatorTest, EmptyStandbySkipsWithoutLease) {
    HotStandbyConfig standby_config;
    standby_config.enable_verification = false;
    HotStandbyService standby(standby_config);
    EmptyBackend backend;
    UnusedObjectStore object_store;
    size_t lease_factory_calls = 0;
    BatchOpLogSnapshotCoordinatorConfig config;
    config.snapshot_root = "snapshots";
    config.clock = [] { return std::chrono::steady_clock::now(); };
    BatchOpLogSnapshotCoordinator coordinator(
        standby, backend, object_store, "cluster", std::move(config), [&] {
            ++lease_factory_calls;
            return std::unique_ptr<SnapshotMaintenanceLease>();
        });

    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_EQ(0u, lease_factory_calls);
    EXPECT_FALSE(coordinator.IsAttemptInFlight());
    EXPECT_EQ(0u, coordinator.GetStatus().attempts);
    EXPECT_EQ(HAMetricManager::SnapshotSkipReason::NoNewBatch,
              HAMetricManager::instance().get_snapshot_runtime().skip_reason);

    auto& metrics = HAMetricManager::instance();
    const auto before = metrics.get_snapshot_operation(
        HAMetricManager::SnapshotOperation::Schedule);
    backend.get_error = ErrorCode::ETCD_OPERATION_ERROR;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, coordinator.RunOnce());
    EXPECT_EQ(before.errors + 1,
              metrics
                  .get_snapshot_operation(
                      HAMetricManager::SnapshotOperation::Schedule)
                  .errors);
    EXPECT_EQ(0u, lease_factory_calls);
    backend.get_error = ErrorCode::OK;
    coordinator.Start();
    EXPECT_TRUE(coordinator.IsRunning());
    coordinator.Stop();
    EXPECT_FALSE(coordinator.IsRunning());
}

TEST(BatchOpLogSnapshotCoordinatorTest,
     LeaseLossAndCatchUpAreNotPublicationSuccess) {
    auto backend = std::make_shared<RecordingBackend>();
    const auto add_batch = [&](uint64_t id) {
        auto batch = MakeBatch();
        batch.batch_id = batch.first_seq = batch.last_seq = id;
        batch.entries[0].sequence_id = id;
        backend->Put(BuildBatchRecordKey("cluster", id),
                     EncodeOpLogBatchRecord(batch));
        backend->Put(BuildDurablePrefixKey("cluster"),
                     EncodeDurablePrefix({.batch_id = id, .last_seq = id}));
    };
    add_batch(1);
    backend->Put(BuildProducerViewKey("cluster"), "7");
    backend->Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"), "101");
    HotStandbyConfig config;
    config.enable_verification = false;
    config.oplog_poll_interval_ms = 1;
    HotStandbyService standby(config);
    standby.SetCatchUpBatchKvBackendForTesting(backend);
    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", "cluster"));
    const auto wait_for_batch = [&](uint64_t id) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            auto prefix = standby.GetLastAppliedBatchOpLogSnapshotPrefix();
            if (prefix && prefix->batch_id == id) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    };
    ASSERT_TRUE(wait_for_batch(1));
    RecordingObjectStore objects;
    SnapshotMaintenanceLease* active_lease = nullptr;
    BatchOpLogSnapshotCoordinatorConfig coordinator_config;
    coordinator_config.snapshot_root = "snapshots";
    coordinator_config.snapshot_interval_seconds = 0;
    BatchOpLogSnapshotCoordinator coordinator(
        standby, *backend, objects, "cluster", coordinator_config, [&] {
            auto lease =
                SnapshotMaintenanceLease::MakeForTesting("cluster", "101", 4);
            active_lease = lease.get();
            return lease;
        });
    auto& metrics = HAMetricManager::instance();
    using Operation = HAMetricManager::SnapshotOperation;
    const auto before = metrics.get_snapshot_operation(Operation::Publish);
    const auto lost = metrics.get_snapshot_runtime().lease_lost_total;
    objects.on_upload = [] {
        throw std::runtime_error("upload failed with held lease");
    };
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, coordinator.RunOnce());
    EXPECT_EQ(lost, metrics.get_snapshot_runtime().lease_lost_total);
    objects.on_upload = [&] {
        active_lease->Release();
        throw std::runtime_error("upload failed after lease loss");
    };
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, coordinator.RunOnce());
    EXPECT_EQ(lost + 1, metrics.get_snapshot_runtime().lease_lost_total);

    objects.on_upload = [&] {
        backend->pause_replay = true;
        add_batch(2);
        active_lease->Release();
    };
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, coordinator.RunOnce());
    EXPECT_EQ(lost + 2, metrics.get_snapshot_runtime().lease_lost_total);
    EXPECT_EQ(before.errors + 1,
              metrics.get_snapshot_operation(Operation::Publish).errors);
    EXPECT_EQ(2u, metrics.get_snapshot_runtime().catch_up_target_batch);
    EXPECT_FALSE(
        backend->Contains(ha::BuildBatchOpLogSnapshotLatestKey("cluster")));
    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_EQ(HAMetricManager::SnapshotSkipReason::CatchUp,
              metrics.get_snapshot_runtime().skip_reason);
    EXPECT_EQ(before.total + 1,
              metrics.get_snapshot_operation(Operation::Publish).total);
    objects.on_upload = {};
    backend->pause_replay = false;
    ASSERT_TRUE(wait_for_batch(2));
    EXPECT_EQ(0u, metrics.get_snapshot_runtime().catch_up_target_batch);
    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_EQ(before.total + 2,
              metrics.get_snapshot_operation(Operation::Publish).total);
    EXPECT_EQ(before.errors + 1,
              metrics.get_snapshot_operation(Operation::Publish).errors);
    standby.Stop();
}

class SnapshotMaintenanceTest : public ::testing::TestWithParam<int> {};

TEST_P(SnapshotMaintenanceTest, PublishesAndPrunesDespiteGcFailure) {
    auto backend = std::make_shared<RecordingBackend>();
    ASSERT_EQ(ErrorCode::OK, backend->Put(BuildBatchRecordKey("cluster", 1),
                                          EncodeOpLogBatchRecord(MakeBatch())));
    ASSERT_EQ(
        ErrorCode::OK,
        backend->Put(BuildDurablePrefixKey("cluster"),
                     EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    ASSERT_EQ(ErrorCode::OK,
              backend->Put(BuildProducerViewKey("cluster"), "7"));
    const auto maintenance_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster");
    ASSERT_EQ(ErrorCode::OK, backend->Put(maintenance_key, "101"));

    HotStandbyConfig standby_config;
    standby_config.enable_verification = false;
    standby_config.oplog_poll_interval_ms = 1;
    HotStandbyService standby(standby_config);
    standby.SetCatchUpBatchKvBackendForTesting(backend);
    ASSERT_EQ(ErrorCode::OK, standby.Start("", "", "cluster"));

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        auto prefix = standby.GetLastAppliedBatchOpLogSnapshotPrefix();
        if (prefix && prefix->batch_id == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    RecordingObjectStore object_store;
    object_store.gc_failure = GetParam();
    BatchOpLogSnapshotCoordinatorConfig config;
    config.snapshot_root = "snapshots";
    config.snapshot_interval_seconds = 0;
    bool lease_busy = true;
    BatchOpLogSnapshotCoordinator coordinator(
        standby, *backend, object_store, "cluster", std::move(config), [&] {
            return lease_busy ? nullptr
                              : SnapshotMaintenanceLease::MakeForTesting(
                                    "cluster", "101", 4);
        });
    auto& metrics = HAMetricManager::instance();
    using Operation = HAMetricManager::SnapshotOperation;
    const auto gc_before = metrics.get_snapshot_operation(Operation::Gc);
    const auto publish_before =
        metrics.get_snapshot_operation(Operation::Publish);
    const auto prune_before = metrics.get_snapshot_operation(Operation::Prune);
    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_EQ(HAMetricManager::SnapshotSkipReason::LeaseBusy,
              metrics.get_snapshot_runtime().skip_reason);
    EXPECT_EQ(publish_before.total,
              metrics.get_snapshot_operation(Operation::Publish).total);
    lease_busy = false;

    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_TRUE(
        backend->Contains(ha::BuildBatchOpLogSnapshotLatestKey("cluster")));
    EXPECT_EQ(1u, coordinator.GetStatus().attempts);
    EXPECT_TRUE(coordinator.GetStatus().catch_up_target.has_value());
    EXPECT_FALSE(backend->Contains(
        ha::BuildBatchOpLogSnapshotCompactionFloorKey("cluster")));
    auto second = MakeBatch();
    second.batch_id = second.first_seq = second.last_seq = 2;
    second.entries[0].sequence_id = 2;
    ASSERT_EQ(ErrorCode::OK, backend->Put(BuildBatchRecordKey("cluster", 2),
                                          EncodeOpLogBatchRecord(second)));
    ASSERT_EQ(
        ErrorCode::OK,
        backend->Put(BuildDurablePrefixKey("cluster"),
                     EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    const auto next_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < next_deadline) {
        const auto prefix = standby.GetLastAppliedBatchOpLogSnapshotPrefix();
        if (prefix && prefix->batch_id == 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(2u, standby.GetLastAppliedBatchOpLogSnapshotPrefix()->batch_id);
    ASSERT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_EQ(2u, coordinator.GetStatus().attempts);
    EXPECT_EQ(1u, backend->delete_count);
    const auto runtime = metrics.get_snapshot_runtime();
    EXPECT_TRUE(runtime.latest_created_at_ms.has_value());
    EXPECT_TRUE(runtime.fallback_created_at_ms.has_value());
    EXPECT_EQ(1u, runtime.compaction_floor);
    EXPECT_EQ(1u, runtime.candidate_floor);
    EXPECT_GT(runtime.snapshot_bytes, 0u);
    EXPECT_EQ(publish_before.total + 2,
              metrics.get_snapshot_operation(Operation::Publish).total);
    EXPECT_EQ(publish_before.errors,
              metrics.get_snapshot_operation(Operation::Publish).errors);
    EXPECT_EQ(gc_before.total + 2,
              metrics.get_snapshot_operation(Operation::Gc).total);
    EXPECT_EQ(gc_before.errors + (GetParam() == 0 ? 0 : 2),
              metrics.get_snapshot_operation(Operation::Gc).errors);
    EXPECT_EQ(prune_before.errors,
              metrics.get_snapshot_operation(Operation::Prune).errors);
    EXPECT_FALSE(backend->Contains(BuildBatchRecordKey("cluster", 1)));
    EXPECT_TRUE(backend->Contains(BuildBatchRecordKey("cluster", 2)));
    standby.Stop();
}

INSTANTIATE_TEST_SUITE_P(GcOutcomes, SnapshotMaintenanceTest,
                         ::testing::Values(0, 1, 2));

}  // namespace mooncake::test
