#include "ha/snapshot/snapshot_maintenance_lease.h"

#include <utility>

#include "etcd_helper.h"
#include "ha/snapshot/batch_oplog/metadata.h"

namespace mooncake {

SnapshotMaintenanceLease::SnapshotMaintenanceLease(std::string cluster_id)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)) {}

SnapshotMaintenanceLease::SnapshotMaintenanceLease(std::string cluster_id,
                                                   std::string owner_token,
                                                   bool testing)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)),
      owner_token_(std::move(owner_token)),
      testing_(testing),
      lock_created_(testing),
      keepalive_stopped_(!testing) {}

SnapshotMaintenanceLease::~SnapshotMaintenanceLease() { (void)Release(); }

std::unique_ptr<SnapshotMaintenanceLease>
SnapshotMaintenanceLease::MakeForTesting(std::string cluster_id,
                                         std::string owner_token) {
    return std::unique_ptr<SnapshotMaintenanceLease>(
        new SnapshotMaintenanceLease(std::move(cluster_id),
                                     std::move(owner_token), true));
}

ErrorCode SnapshotMaintenanceLease::Acquire() {
    if (testing_) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (lock_key_.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (lock_created_ || keepalive_thread_.joinable()) {
            return ErrorCode::INVALID_PARAMS;
        }
        shutdown_requested_ = false;
        keepalive_result_ = ErrorCode::OK;
        keepalive_stopped_ = false;
    }

    EtcdLeaseId lease_id = 0;
    ErrorCode err = EtcdHelper::GrantLease(kTtlSeconds, lease_id);
    if (err != ErrorCode::OK) {
        std::lock_guard<std::mutex> lock(mutex_);
        keepalive_stopped_ = true;
        keepalive_result_ = err;
        return err;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lease_id_ = lease_id;
        owner_token_ = std::to_string(lease_id);
    }
    keepalive_thread_ = std::thread([this, lease_id] {
        const ErrorCode result = EtcdHelper::KeepAlive(lease_id);
        std::lock_guard<std::mutex> lock(mutex_);
        keepalive_result_ = result;
        keepalive_stopped_ = true;
    });

    err = EtcdHelper::WaitKeepAliveReady(lease_id_, kKeepAliveReadyTimeoutMs);
    if (err != ErrorCode::OK) {
        (void)StopKeepAlive(true);
        return err;
    }
    EtcdRevisionId revision = 0;
    err = EtcdHelper::CreateWithLease(lock_key_.c_str(), lock_key_.size(),
                                      owner_token_.c_str(), owner_token_.size(),
                                      lease_id_, revision);
    if (err != ErrorCode::OK) {
        (void)StopKeepAlive(true);
        return err;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lock_created_ = true;
    }
    if (!IsHeld()) {
        (void)StopKeepAlive(true);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

bool SnapshotMaintenanceLease::IsHeld() const {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        return lock_created_ && !owner_token_.empty();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return lock_created_ && !keepalive_stopped_ &&
           keepalive_result_ == ErrorCode::OK && !shutdown_requested_;
}

EtcdLeaseId SnapshotMaintenanceLease::lease_id() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lease_id_;
}

std::string SnapshotMaintenanceLease::owner_token() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return owner_token_;
}

ErrorCode SnapshotMaintenanceLease::Release() {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        lock_created_ = false;
        return ErrorCode::OK;
    }
    return StopKeepAlive(true);
}

ErrorCode SnapshotMaintenanceLease::StopKeepAlive(bool revoke) {
    std::thread thread_to_join;
    EtcdLeaseId lease_id = 0;
    bool should_cancel = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_requested_ = true;
        lease_id = lease_id_;
        should_cancel = keepalive_thread_.joinable() && !keepalive_stopped_;
        if (keepalive_thread_.joinable()) {
            thread_to_join = std::move(keepalive_thread_);
        }
        lock_created_ = false;
    }
    ErrorCode result = ErrorCode::OK;
    if (should_cancel && lease_id != 0) {
        result = EtcdHelper::CancelKeepAlive(lease_id);
        if (result == ErrorCode::ETCD_OPERATION_ERROR) {
            result = ErrorCode::OK;
        }
    }
    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }
    if (revoke && lease_id != 0) {
        const ErrorCode revoke_result = EtcdHelper::RevokeLease(lease_id);
        if (result == ErrorCode::OK) {
            result = revoke_result;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lease_id_ = 0;
        owner_token_.clear();
        keepalive_stopped_ = true;
    }
    return result;
}

}  // namespace mooncake
