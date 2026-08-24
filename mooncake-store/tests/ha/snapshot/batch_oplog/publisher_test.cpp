#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
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
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        values[std::string(key)] = std::string(value);
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
            } else if (it == values.end() || it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) values[put.key] = put.value;
        return ErrorCode::OK;
    }

    std::map<std::string, std::string> values;
    ErrorCode next_txn_error{ErrorCode::OK};
    std::string mutate_key;
    std::string mutate_value;
    size_t txn_count{0};
};

std::string MakeDescriptor(uint64_t batch_id, int64_t lease_id = 101,
                           ViewVersionId producer_view = 7) {
    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = ha::BuildBatchOpLogSnapshotId(batch_id, lease_id);
    descriptor.last_included_seq = batch_id * 10;
    descriptor.last_included_batch_id = batch_id;
    descriptor.producer_view_version = producer_view;
    descriptor.manifest_key = "snapshots/batch-oplog/" + descriptor.snapshot_id +
                              "/manifest.json";
    descriptor.manifest_size = 1;
    descriptor.created_at_ms = 1234;
    return ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
}

}  // namespace

TEST(BatchOpLogSnapshotPublisherTest, PublishesAndRotatesPointersAtomically) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");

    const auto first = MakeDescriptor(1);
    EXPECT_EQ(ErrorCode::OK, publisher.Publish(*lease, first));
    EXPECT_EQ(first, backend.values[ha::BuildBatchOpLogSnapshotLatestKey("cluster")]);
    EXPECT_FALSE(backend.values.contains(
        ha::BuildBatchOpLogSnapshotFallbackKey("cluster")));

    const auto second = MakeDescriptor(2);
    EXPECT_EQ(ErrorCode::OK, publisher.Publish(*lease, second));
    EXPECT_EQ(second, backend.values[ha::BuildBatchOpLogSnapshotLatestKey("cluster")]);
    EXPECT_EQ(first, backend.values[ha::BuildBatchOpLogSnapshotFallbackKey("cluster")]);
}

TEST(BatchOpLogSnapshotPublisherTest, RejectsStaleAndLostLeaseWithoutTxn) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    const auto lock_key = ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster");
    ASSERT_EQ(ErrorCode::OK, backend.Put(lock_key, "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    const auto first = MakeDescriptor(4);
    ASSERT_EQ(ErrorCode::OK, publisher.Publish(*lease, first));
    const auto txn_count = backend.txn_count;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, publisher.Publish(*lease, first));
    EXPECT_EQ(txn_count, backend.txn_count);

    lease->Release();
    const auto next = MakeDescriptor(5);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, publisher.Publish(*lease, next));
    EXPECT_EQ(txn_count, backend.txn_count);
}

TEST(BatchOpLogSnapshotPublisherTest, LeavesPointersUnchangedOnCasFailure) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    const auto first = MakeDescriptor(1);
    backend.next_txn_error = ErrorCode::ETCD_TRANSACTION_FAIL;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, publisher.Publish(*lease, first));
    EXPECT_FALSE(backend.values.contains(
        ha::BuildBatchOpLogSnapshotLatestKey("cluster")));
}

TEST(BatchOpLogSnapshotPublisherTest, FencesLatestAndFallbackRaces) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    const auto first = MakeDescriptor(1);
    ASSERT_EQ(ErrorCode::OK, publisher.Publish(*lease, first));

    const auto latest_racer = MakeDescriptor(4);
    backend.mutate_key = ha::BuildBatchOpLogSnapshotLatestKey("cluster");
    backend.mutate_value = latest_racer;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              publisher.Publish(*lease, MakeDescriptor(2)));
    EXPECT_EQ(latest_racer,
              backend.values[ha::BuildBatchOpLogSnapshotLatestKey("cluster")]);

    backend.values.erase(ha::BuildBatchOpLogSnapshotFallbackKey("cluster"));
    backend.values[ha::BuildBatchOpLogSnapshotLatestKey("cluster")] = first;
    backend.mutate_key = ha::BuildBatchOpLogSnapshotFallbackKey("cluster");
    backend.mutate_value = first;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              publisher.Publish(*lease, MakeDescriptor(2)));
    EXPECT_EQ(first,
              backend.values[ha::BuildBatchOpLogSnapshotFallbackKey("cluster")]);
}

TEST(BatchOpLogSnapshotPublisherTest, RejectsCandidateFromAnotherLease) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "102");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "102"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              publisher.Publish(*lease, MakeDescriptor(1)));
    EXPECT_EQ(0u, backend.txn_count);
}

TEST(BatchOpLogSnapshotPublisherTest, AllowsOlderProducerViewWhenCursorAdvances) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    ASSERT_EQ(ErrorCode::OK, publisher.Publish(*lease, MakeDescriptor(1, 101, 9)));
    EXPECT_EQ(ErrorCode::OK, publisher.Publish(*lease, MakeDescriptor(2, 101, 3)));
}

TEST(BatchOpLogSnapshotPublisherTest, LockOwnerRaceCannotPublish) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    backend.mutate_key = ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster");
    backend.mutate_value = "102";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              publisher.Publish(*lease, MakeDescriptor(1)));
    EXPECT_FALSE(backend.values.contains(
        ha::BuildBatchOpLogSnapshotLatestKey("cluster")));
}

TEST(BatchOpLogSnapshotPublisherTest, RejectsCorruptFallback) {
    FakeBackend backend;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("cluster", "101");
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotMaintenanceKey("cluster"),
                          "101"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotFallbackKey("cluster"),
                          "not-json"));
    BatchOpLogSnapshotPublisher publisher(backend, "cluster");
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR,
              publisher.Publish(*lease, MakeDescriptor(1)));
    EXPECT_EQ(0u, backend.txn_count);
}

#ifdef STORE_USE_ETCD
TEST(BatchOpLogSnapshotPublisherTest, RealEtcdLeaseAndPublisherLifecycle) {
    constexpr char kEndpoints[] = "127.0.0.1:2379";
    if (EtcdHelper::ConnectToEtcdStoreClient(kEndpoints) != ErrorCode::OK) {
        GTEST_SKIP() << "etcd is not available at " << kEndpoints;
    }

    const std::string cluster =
        "n04-real-" + std::to_string(static_cast<long long>(getpid()));
    SnapshotMaintenanceLease first(cluster);
    const auto first_acquire = first.Acquire();
    if (first_acquire != ErrorCode::OK) {
        GTEST_SKIP() << "etcd lease unavailable: "
                     << static_cast<int>(first_acquire);
    }
    SnapshotMaintenanceLease second(cluster);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, second.Acquire());

    EtcdHaKvBackend backend;
    BatchOpLogSnapshotPublisher publisher(backend, cluster);
    const auto first_descriptor = MakeDescriptor(1, first.lease_id());
    ASSERT_EQ(ErrorCode::OK, publisher.Publish(first, first_descriptor));
    const auto second_descriptor = MakeDescriptor(2, first.lease_id());
    ASSERT_EQ(ErrorCode::OK, publisher.Publish(first, second_descriptor));

    ASSERT_EQ(ErrorCode::OK, first.Release());
    ASSERT_EQ(ErrorCode::OK, second.Acquire());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              publisher.Publish(second, second_descriptor));
    ASSERT_EQ(ErrorCode::OK, second.Release());
}
#endif

}  // namespace mooncake::test
