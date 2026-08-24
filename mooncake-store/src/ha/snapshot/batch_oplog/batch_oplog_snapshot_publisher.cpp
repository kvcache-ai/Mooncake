#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.h"

#include <utility>

#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"

namespace mooncake {
namespace {

ErrorCode ReadPointer(HaKvBackend& backend, const std::string& key,
                      std::string& value, bool& exists) {
    const ErrorCode err = backend.Get(key, value);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        value.clear();
        exists = false;
        return ErrorCode::OK;
    }
    if (err != ErrorCode::OK) {
        return err;
    }
    exists = true;
    return ErrorCode::OK;
}

}  // namespace

BatchOpLogSnapshotPublisher::BatchOpLogSnapshotPublisher(
    HaKvBackend& backend, std::string cluster_id)
    : backend_(backend), cluster_id_(std::move(cluster_id)) {}

ErrorCode BatchOpLogSnapshotPublisher::Publish(
    const SnapshotMaintenanceLease& lease, std::string_view descriptor_json) {
    if (!lease.IsHeld()) {
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }
    return PublishImpl(lease.owner_token(), descriptor_json, lease);
}

ErrorCode BatchOpLogSnapshotPublisher::PublishImpl(
    std::string_view owner_token, std::string_view descriptor_json,
    const SnapshotMaintenanceLease& lease) {
    if (owner_token.empty() || !lease.IsHeld() ||
        !backend_.SupportsTxn() || cluster_id_.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto candidate = ha::DecodeBatchOpLogSnapshotDescriptor(descriptor_json);
    if (!candidate) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (candidate->snapshot_id !=
        std::to_string(candidate->last_included_batch_id) + "-" +
            std::string(owner_token)) {
        return ErrorCode::INVALID_PARAMS;
    }

    const std::string latest_key =
        ha::BuildBatchOpLogSnapshotLatestKey(cluster_id_);
    const std::string fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey(cluster_id_);
    const std::string maintenance_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_);
    if (latest_key.empty() || fallback_key.empty() || maintenance_key.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    std::string latest;
    std::string fallback;
    bool latest_exists = false;
    bool fallback_exists = false;
    ErrorCode err = ReadPointer(backend_, latest_key, latest, latest_exists);
    if (err != ErrorCode::OK) {
        return err;
    }
    err = ReadPointer(backend_, fallback_key, fallback, fallback_exists);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!latest_exists && fallback_exists) {
        return ErrorCode::INTERNAL_ERROR;
    }
    if (fallback_exists &&
        !ha::DecodeBatchOpLogSnapshotDescriptor(fallback)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    if (latest_exists) {
        auto decoded_latest =
            ha::DecodeBatchOpLogSnapshotDescriptor(latest);
        if (!decoded_latest) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (candidate->last_included_batch_id <=
            decoded_latest->last_included_batch_id) {
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
    }

    // Re-check immediately before the fenced transaction. A keepalive can
    // stop while object metadata is being read; the compare remains the final
    // etcd-side fence.
    if (!lease.IsHeld()) {
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }
    KvTxn txn;
    txn.compares.push_back({.key = maintenance_key,
                            .kind = KvCompareKind::kValueEquals,
                            .expected_value = std::string(owner_token)});
    txn.compares.push_back(
        {.key = latest_key,
         .kind = latest_exists ? KvCompareKind::kValueEquals
                               : KvCompareKind::kKeyNotExists,
         .expected_value = latest});
    txn.compares.push_back(
        {.key = fallback_key,
         .kind = fallback_exists ? KvCompareKind::kValueEquals
                                 : KvCompareKind::kKeyNotExists,
         .expected_value = fallback});
    if (latest_exists) {
        txn.puts.push_back({.key = fallback_key, .value = latest});
    }
    txn.puts.push_back({.key = latest_key,
                        .value = std::string(descriptor_json)});
    return backend_.Txn(txn);
}

}  // namespace mooncake
