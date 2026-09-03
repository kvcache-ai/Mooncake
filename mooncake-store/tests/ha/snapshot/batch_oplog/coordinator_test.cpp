#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
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
    ErrorCode Get(std::string_view key, std::string& value) override {
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

    coordinator.Start();
    EXPECT_TRUE(coordinator.IsRunning());
    coordinator.Stop();
    EXPECT_FALSE(coordinator.IsRunning());
}

TEST(BatchOpLogSnapshotCoordinatorTest, PublishesAfterCaptureAndResumesApply) {
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
    BatchOpLogSnapshotCoordinatorConfig config;
    config.snapshot_root = "snapshots";
    config.snapshot_interval_seconds = 0;
    BatchOpLogSnapshotCoordinator coordinator(
        standby, *backend, object_store, "cluster", std::move(config), [] {
            return SnapshotMaintenanceLease::MakeForTesting("cluster", "101",
                                                            4);
        });

    EXPECT_EQ(ErrorCode::OK, coordinator.RunOnce());
    EXPECT_TRUE(
        backend->Contains(ha::BuildBatchOpLogSnapshotLatestKey("cluster")));
    EXPECT_EQ(1u, coordinator.GetStatus().attempts);
    EXPECT_TRUE(coordinator.GetStatus().catch_up_target.has_value());
    standby.Stop();
}

}  // namespace mooncake::test
