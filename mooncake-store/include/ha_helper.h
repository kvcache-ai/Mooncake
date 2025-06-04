#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>

#include <string>
#include <thread>
#include <chrono>
#include <cstdint>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "rpc_service.h"
#include "etcd_helper.h"
#include "types.h"

namespace mooncake {

inline const char* const MASTER_VIEW_KEY = "mooncake-store/master_view";

class MasterViewHelper {
public:
    MasterViewHelper(const MasterViewHelper&) = delete;
    MasterViewHelper& operator=(const MasterViewHelper&) = delete;
    MasterViewHelper() = default;

    ErrorCode ConnectToEtcd(const std::string& etcd_address) {
        return ConnectToEtcdStoreClient(etcd_address);
    }

    void ElectLeader(const std::string& master, ViewVersion& version, EtcdLeaseId& lease_id) {
        while (true) {
            // Check if there is already a leader
            ViewVersion current_version = 0;
            std::string current_master;
            auto ret = EtcdGet(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY), current_master, current_version);
            if (ret != ErrorCode::OK && ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
                LOG(ERROR) << "Failed to get current leader: " << ret;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            } else if (ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
                LOG(INFO) << "CurrentLeader=" << current_master << ", CurrentVersion=" << current_version;
                // In rare cases, the leader may be ourselves, but it does not matter.
                // We will watch the key until it's deleted.
                LOG(INFO) << "Waiting for leadership change...";
                auto ret = EtcdWatchUntilDeleted(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY));
                if (ret != ErrorCode::OK) {
                    LOG(ERROR) << "ETCD error when waiting for leadership change: " << ret;
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
            ret = EtcdGrantLease(ETCD_MASTER_VIEW_LEASE_TTL, lease_id);
            if (ret != ErrorCode::OK) {
                LOG(ERROR) << "Failed to grant lease: " << ret;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            ret = EtcdCreateWithLease(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY),
                master.c_str(), master.size(), lease_id, version);
            if (ret == ErrorCode::ETCD_TRANSACTION_FAIL) {
                LOG(INFO) << "Failed to elect self as leader";
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
    void KeepLeader(EtcdLeaseId lease_id) {
        EtcdKeepAlive(lease_id);
    }

    ErrorCode GetMasterView(std::string& master, ViewVersion& version) {
        auto ret = EtcdGet(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY), master, version);
        if (ret != ErrorCode::OK) {
            if (ret == ErrorCode::ETCD_KEY_NOT_EXIST) {
                LOG(ERROR) << "No master is available";
                return ErrorCode::ETCD_KEY_NOT_EXIST;
            } else {
                LOG(ERROR) << "Failed to get master address due to ETCD error";
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
        } else {
            LOG(INFO) << "Get master address: " << master << ", version: " << version;
            return ErrorCode::OK;
        }
    }
};

class MasterServiceSupervisor {
public:
    MasterServiceSupervisor(int port, int server_thread_num, bool enable_gc,
                          bool enable_metric_reporting, int metrics_port,
                          int64_t default_kv_lease_ttl, double eviction_ratio,
                          const std::string& etcd_endpoints = "0.0.0.0:2379")
        : port_(port),
          server_thread_num_(server_thread_num),
          enable_gc_(enable_gc),
          enable_metric_reporting_(enable_metric_reporting),
          metrics_port_(metrics_port),
          default_kv_lease_ttl_(default_kv_lease_ttl),
          eviction_ratio_(eviction_ratio),
          etcd_endpoints_(etcd_endpoints) {}

    int Start() {
        while (true) {
            LOG(INFO) << "Init master service...";
            coro_rpc::coro_rpc_server server(server_thread_num_, port_);
            LOG(INFO) << "Init leader election helper...";
            MasterViewHelper mv_helper;
            if (mv_helper.ConnectToEtcd(etcd_endpoints_) != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to ETCD endpoints: " << etcd_endpoints_;
                return -1;
            }
            LOG(INFO) << "Trying to elect self as leader...";
            ViewVersion version = 0;
            EtcdLeaseId lease_id = 0;
            mv_helper.ElectLeader("localhost:" + std::to_string(port_), version, lease_id);
            auto leader_watch_thread = std::thread([&server, &mv_helper, lease_id]() {            
                mv_helper.KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });
            mooncake::WrappedMasterService wrapped_master_service(
            enable_gc_, default_kv_lease_ttl_,
            enable_metric_reporting_, metrics_port_, eviction_ratio_, version);

            mooncake::RegisterRpcService(server, wrapped_master_service);

            // Metric reporting is now handled by WrappedMasterService.
            async_simple::Future<coro_rpc::err_code> ec = server.async_start();  /*won't block here */
            if (ec.hasResult()) {
                LOG(ERROR) << "Failed to start master service: " << ec.result().value();
                return -1;
            }
            // Block until the server is stopped
            auto err = std::move(ec).get();
            LOG(ERROR) << "Master service stopped: " << err;
            leader_watch_thread.join();
        }
        return 0;
    }

    ~MasterServiceSupervisor() {
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

private:
    // Master service parameters
    int port_;
    int server_thread_num_;
    bool enable_gc_;
    bool enable_metric_reporting_;
    int metrics_port_;
    int64_t default_kv_lease_ttl_;
    double eviction_ratio_;

    // Server thread
    std::thread server_thread_;

    // ETCD parameters
    std::string etcd_endpoints_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_