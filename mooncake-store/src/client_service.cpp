#include "client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>

#include "config.h"
#include "types.h"
#include "p2p_client_service.h"
#include "centralized_client_service.h"

namespace mooncake {

size_t ClientService::CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

size_t ClientService::CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

ClientService::ClientService(const std::string& local_ip, uint16_t te_port,
                             const std::string& metadata_connstring,
                             const std::map<std::string, std::string>& labels)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      local_ip_(local_ip),
      te_port_(te_port),
      metadata_connstring_(metadata_connstring) {
    LOG(INFO) << "client_id=" << client_id_;

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const CentralizedClientConfig& config) {
    auto client = std::make_shared<CentralizedClientService>(
        config.local_ip, config.te_port, config.metadata_connstring,
        config.labels);

    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize centralized client service"
                   << ", ret = " << err;
        return std::nullopt;
    }

    return client;
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const P2PClientConfig& config) {
    auto client = std::make_shared<P2PClientService>(
        config.local_ip, config.te_port, config.metadata_connstring,
        config.labels);

    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize P2P client service"
                   << ", ret = " << err;
        return std::nullopt;
    }

    return client;
}

ClientService::~ClientService() {
    Stop();
    Destroy();
}

void ClientService::Stop() { StopHeartbeat(); }

void ClientService::StopHeartbeat() {
    // Stop ping thread only after no need to contact master anymore
    if (heartbeat_running_) {
        {
            std::lock_guard<std::mutex> lock(heartbeat_mtx_);
            heartbeat_running_ = false;
        }
        heartbeat_cv_.notify_all();
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
    }
}

void ClientService::Destroy() {
    // Free global segment memory
    segment_ptrs_.clear();
    ascend_segment_ptrs_.clear();
}

void ClientService::StartHeartbeat(const std::string& master_server_entry) {
    if (heartbeat_running_) {
        LOG(WARNING) << "Heartbeat thread already running, skip starting";
        return;
    }

    bool is_ha_mode = (master_server_entry.find("etcd://") == 0);
    std::string current_master_address;

    if (is_ha_mode) {
        // For HA mode, resolve the actual address from etcd
        ViewVersionId master_version = 0;
        auto err = master_view_helper_.GetMasterView(current_master_address,
                                                     master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address for heartbeat";
            return;
        }
    } else {
        current_master_address = master_server_entry;
    }

    heartbeat_running_ = true;
    heartbeat_thread_ =
        std::thread([this, is_ha_mode, current_master_address]() mutable {
            this->HeartbeatThreadMain(is_ha_mode,
                                      std::move(current_master_address));
        });
}

ErrorCode ClientService::ConnectToMaster(
    const std::string& master_server_entry) {
    if (master_server_entry.find("etcd://") == 0) {
        std::string etcd_entry = master_server_entry.substr(strlen("etcd://"));

        // Get master address from etcd
        auto err = master_view_helper_.ConnectToEtcd(etcd_entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd";
            return err;
        }
        std::string master_address;
        ViewVersionId master_version = 0;
        err = master_view_helper_.GetMasterView(master_address, master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address";
            return err;
        }

        err = GetMasterClient().Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        return ErrorCode::OK;
    } else {
        auto err = GetMasterClient().Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        return ErrorCode::OK;
    }
}

static std::optional<bool> get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        int iv = std::stoi(ev_ad);
        if (iv == 1) {
            LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
            return true;
        } else if (iv == 0) {
            LOG(INFO) << "auto discovery not set by env MC_MS_AUTO_DISC";
            return false;
        } else {
            LOG(WARNING)
                << "invalid MC_MS_AUTO_DISC value: " << ev_ad
                << ", should be 0 or 1, using default: auto discovery not set";
        }
    }
    return std::nullopt;
}

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

static std::vector<std::string> get_auto_discover_filters() {
    std::vector<std::string> whitelst_filters;
    char* ev_ad = std::getenv("MC_MS_FILTERS");
    if (ev_ad) {
        LOG(INFO) << "whitelist filters: " << ev_ad;
        char delimiter = ',';
        char* end = ev_ad + std::strlen(ev_ad);
        char *start = ev_ad, *pos = ev_ad;
        while ((pos = std::find(start, end, delimiter)) != end) {
            std::string str(start, pos);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
            start = pos + 1;
        }
        if (start != (end + 1)) {
            std::string str(start, end);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
        }
    }
    return whitelst_filters;
}

tl::expected<void, ErrorCode> ClientService::CheckRegisterMemoryParams(
    const void* addr, size_t length) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Tcp is not limited by max_mr_size, but we ignore it for now.
    auto max_mr_size = globalConfig().max_mr_size;  // Max segment size
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode ClientService::InitTransferEngine(
    const std::string& endpoint, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    if (!transfer_engine_) {
        LOG(ERROR) << "Transfer engine pointer is null";
        return ErrorCode::INVALID_PARAMS;
    }

    // get auto_discover and filters from env
    std::optional<bool> env_auto_discover = get_auto_discover();
    bool auto_discover = false;
    if (env_auto_discover.has_value()) {
        // Use user-specified auto-discover setting
        auto_discover = env_auto_discover.value();
    } else {
        // Enable auto-discover for RDMA if no devices are specified
        if (protocol == "rdma" && !device_names.has_value()) {
            LOG(INFO) << "Set auto discovery ON by default for RDMA protocol, "
                         "since no "
                         "device names provided";
            auto_discover = true;
        }
    }
    transfer_engine_->setAutoDiscover(auto_discover);

    // Honor filters when auto-discovery is enabled; otherwise warn once
    if (auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is enabled for protocol: "
                  << protocol;
        auto filters = get_auto_discover_filters();
        transfer_engine_->setWhitelistFilters(std::move(filters));
    } else {
        const char* env_filters = std::getenv("MC_MS_FILTERS");
        if (env_filters && *env_filters != '\0') {
            LOG(WARNING)
                << "MC_MS_FILTERS is set but auto discovery is disabled; "
                << "ignoring whitelist: " << env_filters;
        }
    }

    auto [hostname, port] = parseHostNameWithPort(endpoint);
    int rc =
        transfer_engine_->init(metadata_connstring, endpoint, hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (!auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is disabled for protocol: "
                  << protocol;

        Transport* transport = nullptr;

        if (protocol == "rdma") {
            if (!device_names.has_value() || device_names.value().empty()) {
                LOG(ERROR) << "RDMA protocol requires device names when auto "
                              "discovery is disabled";
                return ErrorCode::INVALID_PARAMS;
            }

            LOG(INFO) << "Using specified RDMA devices: "
                      << device_names.value();

            std::vector<std::string> devices =
                splitString(device_names.value(), ',', /*skip_empty=*/true);

            // Manually discover topology with specified devices only
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }

            transport = transfer_engine_->installTransport("rdma", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install RDMA transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "tcp") {
            if (device_names.has_value()) {
                LOG(WARNING)
                    << "TCP protocol does not use device names, ignoring";
            }

            try {
                transport = transfer_engine_->installTransport("tcp", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ascend") {
            if (device_names.has_value()) {
                LOG(WARNING) << "Ascend protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport =
                    transfer_engine_->installTransport("ascend", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "ascend_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install Ascend transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
ClientService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().BatchQueryIp(client_ids);
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
ClientService::QueryByRegex(const std::string& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().GetReplicaListByRegex(str);
}

tl::expected<void, ErrorCode> ClientService::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> ClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

void ClientService::HeartbeatThreadMain(bool is_ha_mode,
                                        std::string current_master_address) {
    // How many failed heartbeats before getting latest master view from etcd
    const int max_heartbeat_fail_count = 3;
    // How long to wait for next heartbeat after success
    const int success_heartbeat_interval_ms = 1000;
    // How long to wait for next heartbeat after failure
    const int fail_heartbeat_interval_ms = 1000;
    // Increment after a heartbeat failure, reset after a heartbeat success
    int heartbeat_fail_count = 0;

    auto register_client = [this]() {
        LOG(INFO) << "Sending RegisterClientRequest"
                  << ", client_id=" << client_id_;
        auto res = RegisterClient();
        if (!res) {
            LOG(ERROR) << "Failed to register client"
                       << ", client_id=" << client_id_
                       << ", error=" << res.error();
        } else {
            LOG(INFO) << "Client registered successfully"
                      << ", client_id=" << client_id_
                      << ", view_version=" << res.value().view_version;
        }
    };
    // Use another thread to register client to avoid blocking the heartbeat
    // thread
    std::future<void> register_client_future;

    while (heartbeat_running_) {
        // Join the register client thread if it is ready
        if (register_client_future.valid() &&
            register_client_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            register_client_future = std::future<void>();
        }

        // Send heartbeat to master
        HeartbeatRequest req = build_heartbeat_request();
        auto heartbeat_result = GetMasterClient().Heartbeat(req);
        if (heartbeat_result) {  // Heartbeat success
            heartbeat_fail_count = 0;
            HandleHeartbeatResponse(heartbeat_result.value(),
                                    current_master_address, register_client,
                                    register_client_future);
            WaitForNextHeartbeat(success_heartbeat_interval_ms);
        } else {  // Heartbeat failed
            heartbeat_fail_count++;
            if (heartbeat_fail_count < max_heartbeat_fail_count) {
                // just retry
                LOG(ERROR) << "Failed to send heartbeat to master";
            } else {
                // Exceeded failure threshold, attempt reconnect
                if (ReconnectToMaster(is_ha_mode, current_master_address)) {
                    heartbeat_fail_count = 0;
                    // Do NOT sleep here, immediately loop back to send
                    // heartbeat so the client can discover UNDEFINED status and
                    // re-register as fast as possible.
                    continue;
                }
            }
            WaitForNextHeartbeat(fail_heartbeat_interval_ms);
        }
    }  // end while

    // Wait for any pending register client thread to finish
    if (register_client_future.valid()) {
        register_client_future.wait();
    }
}

void ClientService::WaitForNextHeartbeat(int interval_ms) {
    std::unique_lock<std::mutex> lock{heartbeat_mtx_};
    heartbeat_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                           [this] { return !heartbeat_running_; });
}

bool ClientService::HandleHeartbeatResponse(
    const HeartbeatResponse& response,
    const std::string& current_master_address,
    const std::function<void()>& register_client,
    std::future<void>& register_client_future) {
    if (response.view_version != view_version_) {
        LOG(WARNING) << "Master view_version changed"
                     << ", client status in master: " << (int)response.status
                     << ", master address: " << current_master_address
                     << ", Master version: " << response.view_version
                     << ", Client version: " << view_version_;
    }
    for (auto& task_result : response.task_results) {
        HandleHeartbeatTaskResult(task_result);
    }
    if (response.status == ClientStatus::UNDEFINED &&
        !register_client_future.valid()) {
        // Ensure at most one register client thread is running
        register_client_future =
            std::async(std::launch::async, register_client);
    }
    return true;
}

void ClientService::HandleHeartbeatTaskResult(
    const HeartbeatTaskResult& task_result) {
    if (task_result.error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to process task"
                   << ", task_type=" << (int)task_result.type
                   << ", error=" << toString(task_result.error);
    }

    if (std::holds_alternative<SyncSegmentMetaResult>(task_result.detail)) {
        auto& sync_res = std::get<SyncSegmentMetaResult>(task_result.detail);
        for (auto& sub : sync_res.sub_results) {
            if (sub.error != ErrorCode::OK) {
                LOG(WARNING) << "Failed to sync segment usage"
                             << ", segment_id=" << sub.segment_id
                             << ", error=" << toString(sub.error);
            }
        }
    }
}

bool ClientService::ReconnectToMaster(bool is_ha_mode,
                                      std::string& current_master_address) {
    if (is_ha_mode) {
        LOG(ERROR) << "Heartbeat failure threshold exceeded;"
                   << " fetching latest master view and reconnecting";
        std::string master_address;
        ViewVersionId next_version = 0;
        auto err =
            master_view_helper_.GetMasterView(master_address, next_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get new master view: " << toString(err);
            return false;
        }

        err = GetMasterClient().Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master " << master_address
                       << ": " << toString(err);
            return false;
        }

        current_master_address = master_address;
        LOG(INFO) << "Reconnected to master " << master_address;
        return true;
    } else {
        LOG(ERROR) << "Heartbeat failure threshold exceeded (non-HA);"
                   << " reconnecting to " << current_master_address;
        auto err = GetMasterClient().Connect(current_master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Reconnect failed to " << current_master_address
                       << ": " << toString(err);
            return false;
        }
        LOG(INFO) << "Reconnected to master " << current_master_address;
        return true;
    }
}

}  // namespace mooncake
