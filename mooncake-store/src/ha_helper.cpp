#include "ha_helper.h"

namespace mooncake {

ErrorCode MasterViewHelper::ConnectToEtcd(const std::string& etcd_endpoints) {
    return EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints);
}

void MasterViewHelper::ElectLeader(const std::string& master_address,
                                   ViewVersionId& version,
                                   EtcdLeaseId& lease_id) {
    while (true) {
        // Check if there is already a leader
        ViewVersionId current_version = 0;
        std::string current_master;
        auto ret = EtcdHelper::Get(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY),
                                   current_master, current_version);
        if (ret != ErrorCode::OK && ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(ERROR) << "Failed to get current leader: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        } else if (ret != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(INFO) << "CurrentLeader=" << current_master
                      << ", CurrentVersion=" << current_version;
            // In rare cases, the leader may be ourselves, but it does not
            // matter. We will watch the key until it's deleted.
            LOG(INFO) << "Waiting for leadership change...";
            auto ret = EtcdHelper::WatchUntilDeleted(MASTER_VIEW_KEY,
                                                     strlen(MASTER_VIEW_KEY));
            if (ret != ErrorCode::OK) {
                LOG(ERROR) << "Etcd error when waiting for leadership change: "
                           << ret;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            // From now, the key is deleted
        } else {
            LOG(INFO) << "No leader found, trying to elect self as leader";
        }

        // Here, the key is either deleted or not set. We can
        // try to elect ourselves as the leader. We vote ourselfves
        // as the leader by trying to creating the key in a transaction.
        // The one who successfully creates the key is the leader.
        ret = EtcdHelper::GrantLease(ETCD_MASTER_VIEW_LEASE_TTL, lease_id);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "Failed to grant lease: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        ret = EtcdHelper::CreateWithLease(
            MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY), master_address.c_str(),
            master_address.size(), lease_id, version);
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

ErrorCode MasterViewHelper::GetMasterView(std::string& master_address,
                                          ViewVersionId& version) {
    auto err_code = EtcdHelper::Get(MASTER_VIEW_KEY, strlen(MASTER_VIEW_KEY),
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

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterConfig& master_config)
    : enable_gc_(master_config.enable_gc),
      enable_metric_reporting_(master_config.enable_metric_reporting),
      metrics_port_(master_config.metrics_port),
      default_kv_lease_ttl_(master_config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(master_config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(
          master_config.allow_evict_soft_pinned_objects),
      eviction_ratio_(master_config.eviction_ratio),
      eviction_high_watermark_ratio_(
          master_config.eviction_high_watermark_ratio),
      client_live_ttl_sec_(master_config.client_live_ttl_sec),
      rpc_port_(master_config.rpc_port),
      rpc_thread_num_(master_config.rpc_thread_num),
      rpc_address_(master_config.rpc_address),
      rpc_conn_timeout_(
          std::chrono::seconds(master_config.rpc_conn_timeout_seconds)),
      rpc_enable_tcp_no_delay_(master_config.rpc_enable_tcp_no_delay),
      etcd_endpoints_(master_config.etcd_endpoints),
      local_hostname_(master_config.rpc_address + ":" +
                      std::to_string(master_config.rpc_port)),
      cluster_id_(master_config.cluster_id),
      root_fs_dir_(master_config.root_fs_dir) {
    if (master_config.memory_allocator == "cachelib") {
        memory_allocator_ = BufferAllocatorType::CACHELIB;
    } else {
        memory_allocator_ = BufferAllocatorType::OFFSET;
    }
}

int MasterServiceSupervisor::Start() {
    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(rpc_thread_num_, rpc_port_,
                                         rpc_address_, rpc_conn_timeout_,
                                         rpc_enable_tcp_no_delay_);
        LOG(INFO) << "Init leader election helper...";
        MasterViewHelper mv_helper;
        if (mv_helper.ConnectToEtcd(etcd_endpoints_) != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd endpoints: "
                       << etcd_endpoints_;
            return -1;
        }
        LOG(INFO) << "Trying to elect self as leader...";
        ViewVersionId version = 0;
        EtcdLeaseId lease_id = 0;
        mv_helper.ElectLeader(local_hostname_, version, lease_id);

        // Start a thread to keep the leader alive
        auto keep_leader_thread =
            std::thread([&server, &mv_helper, lease_id]() {
                mv_helper.KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        // To prevent potential split-brain, wait long enough for the old leader
        // to retire.
        const int waiting_time = ETCD_MASTER_VIEW_LEASE_TTL;
        std::this_thread::sleep_for(std::chrono::seconds(waiting_time));

        LOG(INFO) << "Starting master service...";
        bool enable_ha = true;
        mooncake::WrappedMasterService wrapped_master_service(
            enable_gc_, default_kv_lease_ttl_, default_kv_soft_pin_ttl_,
            allow_evict_soft_pinned_objects_, enable_metric_reporting_,
            metrics_port_, eviction_ratio_, eviction_high_watermark_ratio_,
            version, client_live_ttl_sec_, enable_ha, cluster_id_, root_fs_dir_,
            memory_allocator_);
        mooncake::RegisterRpcService(server, wrapped_master_service);
        // Metric reporting is now handled by WrappedMasterService.

        async_simple::Future<coro_rpc::err_code> ec =
            server.async_start();  // won't block here
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            auto etcd_err = EtcdHelper::CancelKeepAlive(lease_id);
            if (etcd_err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to cancel keep leader alive: "
                           << etcd_err;
            }
            // Even if CancelKeepAlive fails, the keep alive context are closed.
            // We can safely join the keep leader thread.
            keep_leader_thread.join();
            return -1;
        }
        // Block until the server is stopped
        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        // If the server is closed due to internal errors, we need to manually
        // stop keep leader alive.
        auto etcd_err = EtcdHelper::CancelKeepAlive(lease_id);
        // The error here is predicatable, no need to log it as ERROR.
        LOG(INFO) << "Cancel keep leader alive: " << etcd_err;
        keep_leader_thread.join();
    }
    return 0;
}

MasterServiceSupervisor::~MasterServiceSupervisor() {
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

}  // namespace mooncake