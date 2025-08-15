#include "ha_helper.h"
#include "etcd_helper.h"
#include "rpc_service.h"

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

WrappedMasterServiceConfig
MasterServiceSupervisorConfig::createWrappedMasterServiceConfig(
    ViewVersionId view_version) const {
    WrappedMasterServiceConfig config;

    // Set required parameters using assignment operator
    config.enable_gc = enable_gc;
    config.default_kv_lease_ttl = default_kv_lease_ttl;

    // Set optional parameters (these have default values)
    config.default_kv_soft_pin_ttl = default_kv_soft_pin_ttl;
    config.allow_evict_soft_pinned_objects = allow_evict_soft_pinned_objects;
    config.enable_metric_reporting = enable_metric_reporting;
    config.http_port = static_cast<uint16_t>(metrics_port);
    config.eviction_ratio = eviction_ratio;
    config.eviction_high_watermark_ratio = eviction_high_watermark_ratio;
    config.view_version = view_version;
    config.client_live_ttl_sec = client_live_ttl_sec;
    config.enable_ha =
        true;  // This is used in HA mode, so enable_ha should be true
    config.cluster_id = cluster_id;
    config.root_fs_dir = root_fs_dir;
    config.memory_allocator = memory_allocator;

    return config;
}

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {
    // Validate that all required parameters are set
    if (!config.enable_gc.IsSet()) {
        throw std::runtime_error("enable_gc is not set");
    }
    if (!config.enable_metric_reporting.IsSet()) {
        throw std::runtime_error("enable_metric_reporting is not set");
    }
    if (!config.metrics_port.IsSet()) {
        throw std::runtime_error("metrics_port is not set");
    }
    if (!config.default_kv_lease_ttl.IsSet()) {
        throw std::runtime_error("default_kv_lease_ttl is not set");
    }
    if (!config.default_kv_soft_pin_ttl.IsSet()) {
        throw std::runtime_error("default_kv_soft_pin_ttl is not set");
    }
    if (!config.allow_evict_soft_pinned_objects.IsSet()) {
        throw std::runtime_error("allow_evict_soft_pinned_objects is not set");
    }
    if (!config.eviction_ratio.IsSet()) {
        throw std::runtime_error("eviction_ratio is not set");
    }
    if (!config.eviction_high_watermark_ratio.IsSet()) {
        throw std::runtime_error("eviction_high_watermark_ratio is not set");
    }
    if (!config.client_live_ttl_sec.IsSet()) {
        throw std::runtime_error("client_live_ttl_sec is not set");
    }
    if (!config.rpc_port.IsSet()) {
        throw std::runtime_error("rpc_port is not set");
    }
    if (!config.rpc_thread_num.IsSet()) {
        throw std::runtime_error("rpc_thread_num is not set");
    }
}

int MasterServiceSupervisor::Start() {
    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(config_.rpc_thread_num, config_.rpc_port,
                                         config_.rpc_address, config_.rpc_conn_timeout,
                                         config_.rpc_enable_tcp_no_delay);
        LOG(INFO) << "Init leader election helper...";
        MasterViewHelper mv_helper;
        if (mv_helper.ConnectToEtcd(config_.etcd_endpoints) != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd endpoints: "
                       << config_.etcd_endpoints;
            return -1;
        }
        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        // view_version will be updated by ElectLeader and then used in
        // WrappedMasterService
        ViewVersionId view_version = 0;
        mv_helper.ElectLeader(config_.local_hostname, view_version, lease_id);

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
        mooncake::WrappedMasterService wrapped_master_service(
            config_.createWrappedMasterServiceConfig(view_version));
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