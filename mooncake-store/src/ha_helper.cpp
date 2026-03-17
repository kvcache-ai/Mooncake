#include "ha_helper.h"
#include "etcd_helper.h"
#include "k8s_ha_helper.h"
#include "rpc_service.h"

namespace mooncake {

EtcdMasterViewHelper::EtcdMasterViewHelper(const std::string& etcd_endpoints)
    : etcd_endpoints_(etcd_endpoints), lease_id_(0) {
    std::string cluster_id;
    const char* cluster_id_env = std::getenv("MC_STORE_CLUSTER_ID");
    if (cluster_id_env != nullptr && strlen(cluster_id_env) > 0) {
        cluster_id = cluster_id_env;
    } else {
        cluster_id = "mooncake";
    }
    // Ensure the cluster_id ends with '/' if not empty
    if (!cluster_id.empty() && cluster_id.back() != '/') {
        cluster_id += '/';
    }
    master_view_key_ = "mooncake-store/" + cluster_id + "master_view";
    LOG(INFO) << "Master view key: " << master_view_key_;
}

ErrorCode EtcdMasterViewHelper::ConnectToCoordinator() {
    return EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints_);
}

ErrorCode EtcdMasterViewHelper::ElectLeader(const std::string& master_address,
                                            ViewVersionId& version) {
    while (true) {
        // Check if there is already a leader
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
            // In rare cases, the leader may be ourselves, but it does not
            // matter. We will watch the key until it's deleted.
            LOG(INFO) << "Waiting for leadership change...";
            auto ret = EtcdHelper::WatchUntilDeleted(master_view_key_.c_str(),
                                                     master_view_key_.size());
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
        ret = EtcdHelper::GrantLease(MASTER_VIEW_LEASE_TTL, lease_id_);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "Failed to grant lease: " << ret;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        ret = EtcdHelper::CreateWithLease(
            master_view_key_.c_str(), master_view_key_.size(),
            master_address.c_str(), master_address.size(), lease_id_, version);
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
            return ErrorCode::OK;
        }
    }
}

void EtcdMasterViewHelper::KeepLeader() { EtcdHelper::KeepAlive(lease_id_); }

ErrorCode EtcdMasterViewHelper::GetMasterView(std::string& master_address,
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

ErrorCode EtcdMasterViewHelper::CancelKeepLeader() {
    return EtcdHelper::CancelKeepAlive(lease_id_);
}

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

std::unique_ptr<MasterViewHelper> MasterViewHelper::CreateForMaster(
    const std::string& coordinator_type, const std::string& coordinator_info) {
    std::unique_ptr<MasterViewHelper> helper;
    if (coordinator_type == "etcd") {
        auto etcd_helper  = std::make_unique<EtcdMasterViewHelper>(coordinator_info);
        auto err = etcd_helper->ConnectToCoordinator();
        if ( err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd, endpoints: " << coordinator_info
            << " err: " << err;
            return nullptr;
        }
        helper =std::unique_ptr<MasterViewHelper>(std::move(etcd_helper));
    } else if (coordinator_type == "k8s") {
        helper = std::make_unique<K8sMasterViewHelper>();
    } else {
        LOG(ERROR) << "Unsupported coordinator type: " << coordinator_type;
        return nullptr;
    }
    return helper;
}

std::unique_ptr<MasterViewHelper> MasterViewHelper::CreateForClient(
    const std::string& coordinator_type, const std::string& coordinator_info) {
    std::unique_ptr<MasterViewHelper> helper;
    if (coordinator_type == "etcd") {
        auto etcd_helper = std::make_unique<EtcdMasterViewHelper>(coordinator_info);
        auto err = etcd_helper->ConnectToCoordinator();
        if ( err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd, endpoints: " << coordinator_info
            << " err: " << err;
            return nullptr;
        }
        helper =std::unique_ptr<MasterViewHelper>(std::move(etcd_helper));
    } else if (coordinator_type == "k8s") {
        helper = std::make_unique<K8sMasterViewHelper>(coordinator_info);
    } else {
        LOG(ERROR) << "Unsupported coordinator type: " << coordinator_type;
        return nullptr;
    }
    return helper;
}

int MasterServiceSupervisor::Start() {
    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server.init_ibv();
        }

        LOG(INFO) << "Init leader election helper...";
        std::unique_ptr<MasterViewHelper> mv_helper =
            MasterViewHelper::CreateForMaster(config_.ha_coordinator,
                                              config_.etcd_endpoints);

        if (mv_helper == nullptr) {
            LOG(ERROR) << "Failed to init leader election helper, type: "
                       << config_.ha_coordinator << "endpoint: " << config_.etcd_endpoints;
            return -1;
        }

        LOG(INFO) << "Trying to elect self as leader...";
        // view_version will be updated by ElectLeader and then used in
        // WrappedMasterService
        ViewVersionId view_version = 0;
        mv_helper->ElectLeader(config_.local_hostname, view_version);

        // Start a thread to keep the leader alive
        auto keep_leader_thread =
            std::thread([&server, &mv_helper]() {
                mv_helper->KeepLeader();
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        // To prevent potential split-brain, wait long enough for the old leader
        // to retire.
        const int waiting_time = MASTER_VIEW_LEASE_TTL;
        std::this_thread::sleep_for(std::chrono::seconds(waiting_time));

        LOG(INFO) << "Starting master service...";
        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(config_, view_version));
        mooncake::RegisterRpcService(server, wrapped_master_service);
        // Metric reporting is now handled by WrappedMasterService.

        async_simple::Future<coro_rpc::err_code> ec =
            server.async_start();  // won't block here
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            auto etcd_err = mv_helper->CancelKeepLeader();
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
        auto etcd_err = mv_helper->CancelKeepLeader();
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