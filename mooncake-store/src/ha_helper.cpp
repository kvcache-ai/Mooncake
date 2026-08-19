#include "ha_helper.h"

#include <glog/logging.h>

#include <cstdlib>
#include <thread>

#include "etcd_helper.h"
#ifdef STORE_USE_REDIS
#include "p2p/redis_master_view_helper.h"
#endif

namespace mooncake {

MasterViewHelper::MasterViewHelper() {
    std::string cluster_id;
    const char* cluster_id_env = std::getenv("MC_STORE_CLUSTER_ID");
    if (cluster_id_env != nullptr && strlen(cluster_id_env) > 0) {
        cluster_id = cluster_id_env;
    } else {
        cluster_id = "mooncake";
    }
    if (!cluster_id.empty() && cluster_id.back() != '/') {
        cluster_id += '/';
    }
    master_view_key_ = "mooncake-store/" + cluster_id + "master_view";
    LOG(INFO) << "Master view key: " << master_view_key_;
}

MasterViewHelper::~MasterViewHelper() = default;

ErrorCode MasterViewHelper::ConnectToEtcd(const std::string& etcd_endpoints) {
    return EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints);
}

void MasterViewHelper::ElectLeader(const std::string& master_address,
                                   ViewVersionId& version,
                                   EtcdLeaseId& lease_id) {
    while (true) {
        ViewVersionId current_version = 0;
        std::string current_master;
        auto ret =
            EtcdHelper::Get(master_view_key_.c_str(), master_view_key_.size(),
                            current_master, current_version);
        if (ret != ErrorCode::OK && ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(ERROR) << "Failed to get current leader: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        } else if (ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(INFO) << "CurrentLeader=" << current_master
                      << ", CurrentVersion=" << current_version;
            LOG(INFO) << "Waiting for leadership change...";
            auto ret = EtcdHelper::WatchUntilDeleted(master_view_key_.c_str(),
                                                     master_view_key_.size());
            if (ret != ErrorCode::OK) {
                LOG(ERROR) << "Etcd error when waiting for leadership change: "
                           << ret;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
        } else {
            LOG(INFO) << "No leader found, trying to elect self as leader";
        }

        ret = EtcdHelper::GrantLease(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC, lease_id);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "Failed to grant lease: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        ret = EtcdHelper::CreateWithLease(
            master_view_key_.c_str(), master_view_key_.size(),
            master_address.c_str(), master_address.size(), lease_id, version);
        if (ret == ErrorCode::ETCD_TRANSACTION_FAIL) {
            LOG(INFO) << "Failed to elect self as leader: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        } else if (ret != ErrorCode::OK) {
            LOG(ERROR) << "Failed to create key with lease: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        } else {
            LOG(INFO) << "Successfully elected self as leader";
            return;
        }
    }
}

void MasterViewHelper::KeepLeader(EtcdLeaseId lease_id) {
    EtcdHelper::KeepAlive(lease_id);
}

void MasterViewHelper::CancelKeepAlive(EtcdLeaseId lease_id) {
    auto ret = EtcdHelper::CancelKeepAlive(lease_id);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "Failed to cancel etcd keep-alive, lease_id=" << lease_id
                   << ", error=" << ret;
    }
}

int MasterViewHelper::GetLeaderLeaseTTLSeconds() const {
    return static_cast<int>(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC);
}

ErrorCode MasterViewHelper::GetMasterView(std::string& master_address,
                                          ViewVersionId& version) {
    auto err_code =
        EtcdHelper::Get(master_view_key_.c_str(), master_view_key_.size(),
                        master_address, version);
    if (err_code != ErrorCode::OK) {
        if (err_code == ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(ERROR) << "No master is available";
        } else {
            LOG(ERROR) << "Failed to get master address due to etcd error";
        }
        return err_code;
    } else {
        LOG(INFO) << "Get master address: " << master_address
                  << ", version: " << version;
        return ErrorCode::OK;
    }
}

std::unique_ptr<MasterViewHelper> CreateMasterViewHelper(
    const MasterServiceSupervisorConfig& config) {
    if (config.election_backend == ElectionBackend::REDIS) {
#ifdef STORE_USE_REDIS
        auto helper = std::make_unique<RedisMasterViewHelper>(
            config.cluster_id, config.redis_endpoint, config.redis_password,
            config.redis_db_index, config.redis_master_view_ttl_sec,
            config.redis_heartbeat_interval_sec, config.redis_username);
        auto rc = helper->Connect();
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to Redis at: "
                       << config.redis_endpoint;
            return nullptr;
        }
        return helper;
#else
        LOG(ERROR) << "Redis election backend requested but STORE_USE_REDIS "
                      "is not enabled at compile time";
        return nullptr;
#endif
    }

    auto helper = std::make_unique<MasterViewHelper>();
    if (helper->ConnectToEtcd(config.etcd_endpoints) != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd endpoints: "
                   << config.etcd_endpoints;
        return nullptr;
    }
    return helper;
}

}  // namespace mooncake
