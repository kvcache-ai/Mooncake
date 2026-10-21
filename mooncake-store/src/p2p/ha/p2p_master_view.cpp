#include "p2p/ha/p2p_master_view.h"

#include <chrono>
#include <thread>

#include <glog/logging.h>

#include "etcd_helper.h"
#include "types.h"

namespace mooncake {

std::string BuildP2PEtcdMasterViewKey(std::string_view cluster_id) {
    std::string normalized_cluster_id =
        cluster_id.empty() ? DEFAULT_CLUSTER_ID : std::string(cluster_id);
    if (normalized_cluster_id.back() != '/') {
        normalized_cluster_id += '/';
    }
    return "mooncake-store/" + normalized_cluster_id + "master_view";
}

P2PEtcdMasterView::P2PEtcdMasterView(std::string_view cluster_id)
    : master_view_key_(BuildP2PEtcdMasterViewKey(cluster_id)) {
    LOG(INFO) << "P2P master view key: " << master_view_key_;
}

ErrorCode P2PEtcdMasterView::Connect(const std::string& etcd_endpoints) {
    return EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints);
}

void P2PEtcdMasterView::ElectLeader(const std::string& master_address,
                                    ViewVersionId& version,
                                    EtcdLeaseId& lease_id) {
    while (true) {
        ViewVersionId current_version = 0;
        std::string current_master;
        auto result =
            EtcdHelper::Get(master_view_key_.c_str(), master_view_key_.size(),
                            current_master, current_version);
        if (result != ErrorCode::OK &&
            result != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(ERROR) << "Failed to get current P2P leader: " << result;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (result != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(INFO) << "CurrentLeader=" << current_master
                      << ", CurrentVersion=" << current_version;
            result = EtcdHelper::WatchUntilDeleted(master_view_key_.c_str(),
                                                   master_view_key_.size());
            if (result != ErrorCode::OK) {
                LOG(ERROR) << "Etcd error while waiting for P2P leadership "
                              "change: "
                           << result;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
        }

        result = EtcdHelper::GrantLease(ETCD_MASTER_VIEW_LEASE_TTL, lease_id);
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Failed to grant P2P leader lease: " << result;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        result = EtcdHelper::CreateWithLease(
            master_view_key_.c_str(), master_view_key_.size(),
            master_address.c_str(), master_address.size(), lease_id, version);
        if (result == ErrorCode::ETCD_TRANSACTION_FAIL) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Failed to create P2P master view: " << result;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        return;
    }
}

void P2PEtcdMasterView::KeepLeader(EtcdLeaseId lease_id) {
    EtcdHelper::KeepAlive(lease_id);
}

void P2PEtcdMasterView::CancelKeepAlive(EtcdLeaseId lease_id) {
    auto result = EtcdHelper::CancelKeepAlive(lease_id);
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Failed to cancel P2P etcd keep-alive, lease_id="
                   << lease_id << ", error=" << result;
    }
}

int P2PEtcdMasterView::GetLeaderLeaseTTLSeconds() const {
    return static_cast<int>(ETCD_MASTER_VIEW_LEASE_TTL);
}

ErrorCode P2PEtcdMasterView::GetMasterView(std::string& master_address,
                                           ViewVersionId& version) {
    return EtcdHelper::Get(master_view_key_.c_str(), master_view_key_.size(),
                           master_address, version);
}

#ifdef STORE_USE_REDIS
P2PRedisMasterView::P2PRedisMasterView(const std::string& cluster_id,
                                       const std::string& redis_endpoint,
                                       const std::string& password,
                                       int db_index, int ttl_seconds,
                                       int heartbeat_interval_seconds,
                                       const std::string& username)
    : redis_election_helper_(cluster_id, redis_endpoint, password, db_index,
                             ttl_seconds, heartbeat_interval_seconds, username),
      ttl_seconds_(ttl_seconds) {}

ErrorCode P2PRedisMasterView::Connect() {
    return redis_election_helper_.Connect();
}

void P2PRedisMasterView::ElectLeader(const std::string& master_address,
                                     ViewVersionId& version,
                                     EtcdLeaseId& lease_id) {
    int redis_lease_id = 0;
    redis_election_helper_.ElectLeader(master_address, version, redis_lease_id);
    lease_id = static_cast<EtcdLeaseId>(redis_lease_id);
}

void P2PRedisMasterView::KeepLeader(EtcdLeaseId lease_id) {
    redis_election_helper_.KeepLeader(static_cast<int>(lease_id));
}

void P2PRedisMasterView::CancelKeepAlive(EtcdLeaseId lease_id) {
    (void)lease_id;
    redis_election_helper_.CancelKeepAlive();
}

int P2PRedisMasterView::GetLeaderLeaseTTLSeconds() const {
    return ttl_seconds_;
}

ErrorCode P2PRedisMasterView::GetMasterView(std::string& master_address,
                                            ViewVersionId& version) {
    return redis_election_helper_.GetMasterView(master_address, version);
}
#endif

}  // namespace mooncake
