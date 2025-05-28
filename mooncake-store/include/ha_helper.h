#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>

#include <string>
#include <thread>
#include <chrono>
#include <cstdint>

#include <libetcd_wrapper.h>

#include "types.h"

namespace mooncake {

inline const char* const MASTER_ELECTION_KEY = "store_master_address";

class LeaderElectionHelper {
public:
    LeaderElectionHelper(const LeaderElectionHelper&) = delete;
    LeaderElectionHelper& operator=(const LeaderElectionHelper&) = delete;
    LeaderElectionHelper() = default;

    LeaderElectionHelper(const std::string& etcd_address, const std::string& local_hostname)
        : local_hostname_(local_hostname) {
        char* err_msg_ = nullptr;
        int ret = NewStoreEtcdClient((char*)etcd_address.c_str(), &err_msg_);
        if (ret) {
            LOG(FATAL) << "Failed to initialize etcd client: " << err_msg_;
            free(err_msg_);
            err_msg_ = nullptr;
        } else {
            LOG(INFO) << "initialized etcd client";
        }
    }

    void ElectSelfAsLeader() {
        const int64_t lease_ttl = 20; // 20 seconds TTL
        GoInt64 lease_id = 0;
        char* err_msg = nullptr;

        while (true) {
            // Check if there is already a leader
            char* current_leader = nullptr;
            int ret = EtcdStoreGetWrapper((char*)MASTER_ELECTION_KEY,
                         &current_leader, &err_msg);
            if (ret != 0) {
                LOG(ERROR) << "Failed to get current leader: " << err_msg;
                free(err_msg);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            } else if (current_leader != nullptr) {
                LOG(INFO) << "Current leader is: " << current_leader;
                free(current_leader);
                // In rare cases, the leader may be ourselves, but it does not matter.
                // We will watch the key until it's deleted.
                LOG(INFO) << "Waiting for leadership change...";
                ret = EtcdStoreWatchUntilDeletedWrapper((char*)MASTER_ELECTION_KEY, &err_msg);
                if (ret != 0) {
                    LOG(ERROR) << "Error watching key: " << err_msg;
                    free(err_msg);
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
            } else {
                LOG(INFO) << "No leader found, trying to elect self as leader";
            }

            // Here, the key is either deleted or not set. We can 
            // try to elect ourselves as the leader. We vote ourselfves
            // as the leader by trying to creating the key in a transaction.
            // The one who successfully creates the key is the leader.
            ret = EtcdStoreGrantLeaseWrapper(lease_ttl, &lease_id, &err_msg);
            if (ret != 0) {
                LOG(ERROR) << "Failed to create lease: " << err_msg;
                free(err_msg);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            err_msg = nullptr;
            GoInt tx_success = 0;
            ret = EtcdStoreCreateWithLeaseWrapper((char*)MASTER_ELECTION_KEY,
                    (char*)local_hostname_.c_str(), lease_id, &tx_success, &err_msg);
            if (ret != 0) {
                LOG(ERROR) << "Failed to put key with lease: " << err_msg;
                free(err_msg);
            } else if (tx_success == 0) {
                LOG(INFO) << "Failed to elect self as leader";
            } else {
                LOG(INFO) << "Successfully elected self as leader";
                lease_id_ = lease_id;
                return;
            } 
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void KeepSelfAsLeader() {
        char* err_msg = nullptr;
        EtcdStoreKeepAliveWrapper(lease_id_, &err_msg);
        if (err_msg != nullptr) {
            LOG(ERROR) << "Failed to keep self as leader: " << err_msg;
            free(err_msg);
        }
    }
private:
    std::string local_hostname_;
    GoInt64 lease_id_;
};

class ClientHaHelper {
public:
    ClientHaHelper(const ClientHaHelper&) = delete;
    ClientHaHelper& operator=(const ClientHaHelper&) = delete;
    ClientHaHelper() = default;

    ErrorCode ConnectToEtcd(const std::string& etcd_address) {
        char* err_msg = nullptr;
        int ret = NewStoreEtcdClient((char*)etcd_address.c_str(), &err_msg);
        if (ret != 0) {
            LOG(ERROR) << "Failed to initialize etcd client: " << err_msg;
            free(err_msg);
            err_msg = nullptr;
            return ErrorCode::ETCD_CONNECT_FAIL;
        }
        return ErrorCode::OK;
    }

    ErrorCode GetMasterAddress(std::string& master_address) {
        char* err_msg = nullptr;
        char* current_leader = nullptr;
        int ret = EtcdStoreGetWrapper((char*)MASTER_ELECTION_KEY,
                        &current_leader, &err_msg);
        if (ret != 0) {
            LOG(ERROR) << "Failed to get master address: " << err_msg;
            free(err_msg);
            err_msg = nullptr;
            return ErrorCode::MASTER_CONNECT_FAIL;
        } else {
            LOG(INFO) << "Get master address: " << current_leader;
            master_address = current_leader;
            free(current_leader);
            return ErrorCode::OK;
        }
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_