#include "ha/snapshot/snapshot_maintenance_lease.h"

#include <utility>

#include "etcd_helper.h"
#include "ha/snapshot/batch_oplog/metadata.h"

namespace mooncake {

SnapshotMaintenanceLease::SnapshotMaintenanceLease(std::string cluster_id)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)) {}

SnapshotMaintenanceLease::SnapshotMaintenanceLease(
    std::string cluster_id, std::string owner_token, bool testing,
    EtcdRevisionId create_revision)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)),
      owner_token_(std::move(owner_token)),
      lock_create_revision_(create_revision),
      testing_(testing),
      lock_created_(testing) {}

SnapshotMaintenanceLease::~SnapshotMaintenanceLease() { (void)Release(); }

std::unique_ptr<SnapshotMaintenanceLease>
SnapshotMaintenanceLease::MakeForTesting(std::string cluster_id,
                                         std::string owner_token,
                                         EtcdRevisionId create_revision) {
    return std::unique_ptr<SnapshotMaintenanceLease>(
        new SnapshotMaintenanceLease(std::move(cluster_id),
                                     std::move(owner_token), true,
                                     create_revision));
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
        if (lock_created_ || session_handle_ != 0) {
            return ErrorCode::INVALID_PARAMS;
        }
    }

    int64_t session_handle = 0;
    EtcdLeaseId lease_id = 0;
    EtcdRevisionId create_revision = 0;
    ErrorCode err = EtcdHelper::AcquireMaintenanceSession(
        lock_key_, kTtlSeconds, session_handle, lease_id, create_revision);
    if (err != ErrorCode::OK) {
        return err;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        session_handle_ = session_handle;
        lease_id_ = lease_id;
        lock_create_revision_ = create_revision;
        owner_token_ = std::to_string(lease_id);
        lock_created_ = true;
    }
    if (!IsHeld()) {
        (void)Release();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

bool SnapshotMaintenanceLease::IsHeld() const {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        return lock_created_ && !owner_token_.empty();
    }
    int64_t session_handle = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!lock_created_) {
            return false;
        }
        session_handle = session_handle_;
    }
    auto alive = EtcdHelper::MaintenanceSessionAlive(session_handle);
    return alive && *alive;
}

EtcdLeaseId SnapshotMaintenanceLease::lease_id() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lease_id_;
}

std::string SnapshotMaintenanceLease::owner_token() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return owner_token_;
}

EtcdRevisionId SnapshotMaintenanceLease::lock_create_revision() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lock_create_revision_;
}

ErrorCode SnapshotMaintenanceLease::Release() {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        lock_created_ = false;
        return ErrorCode::OK;
    }
    int64_t session_handle = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        session_handle = session_handle_;
        session_handle_ = 0;
        lease_id_ = 0;
        lock_create_revision_ = 0;
        owner_token_.clear();
        lock_created_ = false;
    }
    if (session_handle == 0) {
        return ErrorCode::OK;
    }
    return EtcdHelper::CloseMaintenanceSession(session_handle);
}

}  // namespace mooncake
