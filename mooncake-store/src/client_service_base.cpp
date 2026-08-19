#include "client_service_base.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include <csignal>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>

#include "config.h"
#include "runtime_config_store.h"
#include "transfer_engine.h"
#include "types.h"
#include "p2p_client_service.h"
#include "centralized_client_service.h"
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

void ClientService::initTeEndpoint() {
    if (metadata_connstring_ == P2PHANDSHAKE) {
        te_endpoint_ = transfer_engine_->getLocalIpAndPort();
    } else {
        te_endpoint_ = local_endpoint();
    }
}

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

ClientService::ClientService(const std::string& metadata_connstring,
                             uint16_t http_port, bool enable_http_server,
                             const std::map<std::string, std::string>& labels,
                             bool enable_metric_collection)
    : client_id_(generate_uuid()),
      metadata_connstring_(metadata_connstring),
      http_port_(http_port),
      enable_http_server_(enable_http_server),
      enable_metric_collection_(enable_metric_collection),
      labels_(labels) {
    LOG(INFO) << "client_id=" << client_id_;
    if (enable_http_server) {
        try {
            http_server_ =
                std::make_unique<coro_http::coro_http_server>(1, http_port_);
            LOG(INFO) << "Client HTTP server created on port " << http_port_;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create client HTTP server: " << e.what();
            http_server_.reset();
            http_port_ = 0;
        }
    } else {
        LOG(INFO) << "Client HTTP server disabled";
        http_port_ = 0;
    }
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const CentralizedClientConfig& config) {
    auto client = std::make_shared<CentralizedClientService>(
        config.metadata_connstring, config.protocol, config.http_port,
        config.enable_http_server, config.labels,
        config.enable_metric_collection);

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
        config.metadata_connstring, config.http_port, config.enable_http_server,
        config.labels, config.enable_metric_collection);

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

void ClientService::Stop() {
    StopHttpServer();
    if (client_buffer_allocator_) {
        unregisterLocalMemory(client_buffer_allocator_->getBase(), false);
        client_buffer_allocator_.reset();
    }
    StopHeartbeat();
}

void ClientService::Destroy() {}

ErrorCode ClientService::ConnectToMaster(
    const std::string& master_server_entry) {
    auto conn = GetMasterClient().Connect(master_server_entry);
    if (!conn) {
        LOG(ERROR) << "Failed to connect to master";
        return conn.error();
    }
    return ErrorCode::OK;
}

/*
tl::expected<RegisterClientResponse, ErrorCode>
ClientService::RegisterClient() {
    MutexLocker lk(&registration_mutex_);
    InflightTracker::Guard guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(WARNING) << "inflight guard invalid";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return InnerRegisterClient();
}
*/

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
    auto max_mr_size = globalConfig().max_mr_size;
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode ClientService::InitTransferEngine(
    uint16_t te_port, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    te_port_ = te_port;
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return ErrorCode::OK;
    }

    bool use_tent = (std::getenv("MC_USE_TENT") != nullptr) ||
                    (std::getenv("MC_USE_TEV1") != nullptr);

    bool auto_discover = false;
    if (!use_tent) {
        std::optional<bool> env_auto_discover = get_auto_discover();
        if (env_auto_discover.has_value()) {
            auto_discover = env_auto_discover.value();
        } else {
            if (protocol == "rdma" && !device_names.has_value()) {
                LOG(INFO)
                    << "Set auto discovery ON by default for RDMA protocol, "
                       "since no "
                       "device names provided";
                auto_discover = true;
            }
        }
        if (!auto_discover) {
            const char* env_filters = std::getenv("MC_MS_FILTERS");
            if (env_filters && *env_filters != '\0') {
                LOG(WARNING)
                    << "MC_MS_FILTERS is set but auto discovery is disabled; "
                    << "ignoring whitelist: " << env_filters;
            }
        }
    }

    if (protocol == "ascend") {
        const char* ascend_use_fabric_mem =
            std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
        if (ascend_use_fabric_mem) {
            globalConfig().ascend_use_fabric_mem = true;
        }
    }

    const bool is_auto_port = (te_port_ == 0);
    ErrorCode err = ErrorCode::OK;

    const int kMaxRetries =
        is_auto_port ? Environ::GetInt("MC_STORE_CLIENT_SETUP_RETRIES", 20) : 1;

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            int new_port = auto_port_binder_->getPort();  // reuse existing port
            if (new_port < 0) {
                LOG(WARNING)
                    << "Failed to rebind port"
                    << ", port=" << std::to_string(new_port)
                    << ", retry=" << (attempt + 1) << "/" << kMaxRetries;
                continue;
            }
            te_port_ = static_cast<uint16_t>(new_port);
        } else if (is_auto_port) {
            auto_port_binder_ = std::make_unique<AutoPortBinder>();
            int new_port = auto_port_binder_->getPort();
            if (new_port < 0) {
                LOG(ERROR) << "Failed to bind available port"
                           << ", port=" << std::to_string(new_port);
                continue;
            }
            te_port_ = static_cast<uint16_t>(new_port);
        }

        err = InnerInitTransferEngine(auto_discover, protocol, device_names);
        if (err == ErrorCode::OK) {
            if (use_tent) {
                LOG(INFO) << "Using TENT mode - transport configuration "
                             "handled internally";
                if (device_names.has_value()) {
                    LOG(INFO) << "Note: device_names parameter is ignored in "
                                 "TENT mode. "
                              << "Configure devices via TENT config file or "
                                 "environment "
                                 "variables.";
                }
                return ErrorCode::OK;
            }
            if (attempt > 0) {
                LOG(INFO) << "TE init succeeded on port " << te_port_
                          << " after " << (attempt + 1) << " attempt(s)";
            }
            return ErrorCode::OK;
        }

        if (is_auto_port) {
            LOG(WARNING) << "TE init failed on port " << te_port_ << ", retry "
                         << (attempt + 1) << "/" << kMaxRetries;
        }
    }

    LOG(ERROR) << "Failed to initialize transfer engine"
               << (is_auto_port ? " after all retries" : "") << ", err=" << err;
    return err;
}

ErrorCode ClientService::InnerInitTransferEngine(
    bool auto_discover, const std::string& protocol,
    const std::optional<std::string>& device_names) {
    transfer_engine_ = std::make_shared<TransferEngine>();
    transfer_engine_->setAutoDiscover(auto_discover);
    if (auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is enabled for protocol: "
                  << protocol;
        auto filters = get_auto_discover_filters();
        transfer_engine_->setWhitelistFilters(std::move(filters));
    }

    int rc = transfer_engine_->init(metadata_connstring_, local_endpoint(),
                                     local_ip_, te_port_);
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

            std::vector<std::string> devices;
            boost::split(devices, device_names.value(), boost::is_any_of(","),
                         boost::token_compress_on);

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
        } else if (protocol == "cxl") {
            if (device_names.has_value()) {
                LOG(WARNING) << "CXL protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport = transfer_engine_->installTransport("cxl", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxl_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install CXL transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

tl::expected<std::vector<std::string>, ErrorCode>
ClientService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetMasterClient().BatchQueryIp(client_ids);
}

tl::expected<std::vector<Replica>, ErrorCode>
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
        LOG(ERROR) << "RegisterLocalMemory param check failed, addr=" << addr
                   << ", length=" << length << ", location=" << location
                   << ", error=" << toString(check_result.error());
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        LOG(ERROR) << "transfer_engine registerLocalMemory failed, addr="
                   << addr << ", length=" << length << ", location=" << location
                   << ", remote_accessible=" << remote_accessible;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> ClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        LOG(ERROR) << "transfer_engine unregisterLocalMemory failed, addr="
                   << addr;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

void ClientService::RegisterHttpMethods() {
    if (!http_server_) return;

    using namespace coro_http;

    http_server_->set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            ClientMetric* metrics = GetMetrics();
            if (!metrics) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "Metrics not available");
                return;
            }
            std::string metrics_str;
            metrics->serialize(metrics_str);
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok,
                                        std::move(metrics_str));
        });

    http_server_->set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            ClientMetric* metrics = GetMetrics();
            if (!metrics) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "Metrics not available");
                return;
            }
            std::string summary = metrics->summary_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    http_server_->set_http_handler<GET>(
        "/health", [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, GetHealthStatus());
        });

    RegisterRuntimeConfigHttpMethods();
}

void ClientService::RegisterRuntimeConfigHttpMethods() {
    return;
}


void ClientService::StartHttpServer() {
    if (!http_server_) return;
    try {
        RegisterHttpMethods();
        http_server_->async_start();
        http_port_ = http_server_->port();
        LOG(INFO) << "Client HTTP server started on port " << http_port_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start client HTTP server: " << e.what();
        http_server_.reset();
        http_port_ = 0;
    }
}

void ClientService::StopHttpServer() {
    if (http_server_) {
        LOG(INFO) << "Stopping client HTTP server on port " << http_port_;
        http_server_->stop();
        http_server_.reset();
    }
}

void ClientService::InitLocalBufferAllocator(size_t pool_size,
                                             const std::string& protocol,
                                             bool use_hugepage) {
    if (pool_size == 0) {
        LOG(INFO) << "Buffer allocator pool size is 0, skip initialization";
        return;
    }
    client_buffer_allocator_ =
        ClientBufferAllocator::create(pool_size, protocol, use_hugepage);
    if (client_buffer_allocator_) {
        auto result = RegisterLocalMemory(client_buffer_allocator_->getBase(),
                                          client_buffer_allocator_->size(), "*",
                                          false, true);
        if (!result) {
            LOG(ERROR) << "Failed to register buffer allocator memory: "
                       << toString(result.error());
            client_buffer_allocator_.reset();
        } else {
            LOG(INFO) << "Buffer allocator initialized: " << pool_size
                      << " bytes";
        }
    }
}

tl::expected<UUID, ErrorCode> ClientService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<UUID, ErrorCode> ClientService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<QueryTaskResponse, ErrorCode> ClientService::QueryTask(
    const UUID& task_id) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> ClientService::FetchTasks(
    size_t batch_size) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> ClientService::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

}  // namespace mooncake