#include "ha_helper.h"
#include "etcd_helper.h"
#include "centralized_rpc_service.h"
#include "ha/oplog/p2p_hot_standby_service.h"
#include "p2p_rpc_service.h"
#ifdef STORE_USE_REDIS
#include "redis_master_view_helper.h"
#endif

#include <optional>

namespace mooncake {

MasterViewHelper::MasterViewHelper() {
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

MasterViewHelper::~MasterViewHelper() = default;

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
        ret = EtcdHelper::GrantLease(ETCD_MASTER_VIEW_LEASE_TTL, lease_id);
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
    return static_cast<int>(ETCD_MASTER_VIEW_LEASE_TTL);
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

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

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

        LOG(INFO) << "Init leader election helper (backend="
                  << (config_.election_backend == ElectionBackend::REDIS
                          ? "redis"
                          : "etcd")
                  << ")...";

        auto mv_helper = CreateMasterViewHelper(config_);
        if (!mv_helper) {
            LOG(ERROR) << "Failed to create leader election helper, backend="
                       << (config_.election_backend == ElectionBackend::REDIS
                               ? "redis"
                               : "etcd");
            return -1;
        }

        std::unique_ptr<P2PHotStandbyService> p2p_standby;
        if (config_.deployment_mode == DeploymentMode::P2P &&
            config_.enable_oplog) {
            P2PHotStandbyConfig standby_config;
            standby_config.cluster_id = config_.cluster_id;
            standby_config.oplog_store_type =
                ParseOpLogStoreType(config_.oplog_store_type);
            standby_config.oplog_store_root_dir = config_.oplog_data_dir;
            standby_config.redis_endpoint = config_.redis_endpoint;
            standby_config.redis_username = config_.redis_username;
            standby_config.redis_password = config_.redis_password;

            p2p_standby =
                std::make_unique<P2PHotStandbyService>(standby_config);
            auto standby_start = p2p_standby->Start();
            if (standby_start != ErrorCode::OK) {
                LOG(ERROR) << "Failed to start P2P hot standby service"
                           << ", error=" << toString(standby_start);
                return -1;
            }
        }

        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        // view_version will be updated by ElectLeader and then used in
        // WrappedMasterService
        ViewVersionId view_version = 0;
        mv_helper->ElectLeader(config_.local_hostname, view_version, lease_id);

        // Start a thread to keep the leader alive
        auto keep_leader_thread =
            std::thread([&server, mv_helper = mv_helper.get(), lease_id]() {
                mv_helper->KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        // To prevent potential split-brain, wait long enough for the old leader
        // to retire.
        std::this_thread::sleep_for(
            std::chrono::seconds(mv_helper->GetLeaderLeaseTTLSeconds()));

        std::optional<P2PStandbyMetadataStore::ExportedMetadata>
            p2p_promoted_metadata;
        uint64_t p2p_promoted_sequence_id = 0;
        if (p2p_standby) {
            auto promote_err = p2p_standby->Promote();
            if (promote_err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to promote P2P hot standby service"
                           << ", error=" << toString(promote_err);
                mv_helper->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
            p2p_promoted_sequence_id =
                p2p_standby->GetLatestAppliedSequenceId();
            p2p_promoted_metadata = p2p_standby->ExportMetadata();
            p2p_standby.reset();
        }

        LOG(INFO) << "Starting master service...";
        std::unique_ptr<WrappedMasterService> wrapped_master_service;
        if (config_.deployment_mode == DeploymentMode::CENTRALIZATION) {
            wrapped_master_service =
                std::make_unique<WrappedCentralizedMasterService>(
                    WrappedMasterServiceConfig(config_, view_version));
            RegisterCentralizedRpcService(
                server, static_cast<WrappedCentralizedMasterService&>(
                            *wrapped_master_service));
        } else {
            auto p2p_wrapped_service =
                std::make_unique<WrappedP2PMasterService>(
                    WrappedMasterServiceConfig(config_, view_version));
            if (p2p_promoted_metadata.has_value()) {
                auto& p2p_master_service = static_cast<P2PMasterService&>(
                    p2p_wrapped_service->GetMasterService());
                auto restore_err =
                    p2p_master_service.RestoreFromStandbyMetadata(
                        p2p_promoted_metadata.value(),
                        p2p_promoted_sequence_id);
                if (restore_err != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to restore P2P promoted metadata"
                               << ", error=" << toString(restore_err);
                    mv_helper->CancelKeepAlive(lease_id);
                    keep_leader_thread.join();
                    return -1;
                }
            }
            RegisterP2PRpcService(server, *p2p_wrapped_service);
            wrapped_master_service = std::move(p2p_wrapped_service);
        }
        // Metric reporting is now handled by WrappedMasterService.

        async_simple::Future<coro_rpc::err_code> ec =
            server.async_start();  // won't block here
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            mv_helper->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            return -1;
        }
        // Block until the server is stopped
        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        // If the server is closed due to internal errors, we need to manually
        // stop keep leader alive.
        mv_helper->CancelKeepAlive(lease_id);
        LOG(INFO) << "Cancel keep leader alive requested";
        keep_leader_thread.join();
    }
    return 0;
}

MasterServiceSupervisor::~MasterServiceSupervisor() {
    if (server_thread_.joinable()) {
        server_thread_.join();
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

    // Default: etcd backend
    auto helper = std::make_unique<MasterViewHelper>();
    if (helper->ConnectToEtcd(config.etcd_endpoints) != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd endpoints: "
                   << config.etcd_endpoints;
        return nullptr;
    }
    return helper;
}

}  // namespace mooncake
