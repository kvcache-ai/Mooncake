#include "client_resources.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <thread>
#include <utility>

#include "client_buffer.hpp"
#include "config.h"
#include "transfer_engine.h"
#include "utils.h"

namespace mooncake {
namespace {

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

}  // namespace

ClientResources::ClientResources() = default;

ClientResources::~ClientResources() {
    auto error = ReleaseLocalBuffer(true);
    if (error != ErrorCode::OK) {
        LOG(ERROR)
            << "Failed to release local buffer during resource destruction: "
            << error;
    }
}

tl::expected<void, ErrorCode> ClientResources::CheckRegisterMemoryParams(
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

void ClientResources::ParseLocalHostname(const std::string& local_hostname) {
    te_port_ = 0;
    const auto ipv6 = extractIPv6HostAndPort(local_hostname);
    if (ipv6.matched) {
        local_ip_ = ipv6.host;
        if (!ipv6.port.empty()) {
            // E.g. "[::1]:12345:npu_2": read the port, preserve the device.
            te_port_ =
                getPortFromString(ipv6.port.substr(0, ipv6.port.find(':')), 0);
        }
    } else {
        const auto colon = local_hostname.find(':');
        local_ip_ = local_hostname.substr(0, colon);
        if (colon != std::string::npos) {
            // E.g. "127.0.0.1:12345:npu_2" or "localhost:12345:npu_2".
            const auto port_end = local_hostname.find(':', colon + 1);
            te_port_ = getPortFromString(
                local_hostname.substr(colon + 1, port_end - colon - 1), 0);
        }
    }
}

void ClientResources::InitTransferEngine(
    const std::string& local_hostname,
    const std::shared_ptr<TransferEngine>& engine) {
    ParseLocalHostname(local_hostname);
    transfer_engine_ = engine;
}

std::string ClientResources::GetLocalEndpoint() const {
    return maybeWrapIpV6(local_ip_) + ":" + std::to_string(te_port_);
}

ErrorCode ClientResources::InitTransferEngine(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    ParseLocalHostname(local_hostname);
    // this only performs RPC calls
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return ErrorCode::OK;
    }

    // Check if using TENT mode - TENT handles transport configuration
    // internally
    use_tent_ = (std::getenv("MC_USE_TENT") != nullptr) ||
                (std::getenv("MC_USE_TEV1") != nullptr);

    bool auto_discover = false;
    if (!use_tent_) {
        if (protocol != "tcp" && protocol != "rdma" && protocol != "ascend" &&
            protocol != "cxl") {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
        // Get auto_discover and filters from env (non-TENT only)
        std::optional<bool> env_auto_discover = get_auto_discover();
        if (env_auto_discover.has_value()) {
            // Use user-specified auto-discover setting
            auto_discover = env_auto_discover.value();
        } else {
            // Enable auto-discover for RDMA if no devices are specified
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
    // Never report success without a successful initialization attempt.
    ErrorCode err = ErrorCode::INTERNAL_ERROR;

    const int kMaxRetries =
        is_auto_port ? GetEnvOr<int>("MC_STORE_CLIENT_SETUP_RETRIES", 20) : 1;

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            int new_port = port_binder_->rebind();
            if (new_port < 0) {
                LOG(WARNING)
                    << "Failed to rebind port"
                    << ", port=" << std::to_string(new_port)
                    << ", retry=" << (attempt + 1) << "/" << kMaxRetries;
                continue;
            }
            te_port_ = static_cast<uint16_t>(new_port);
        } else if (is_auto_port) {
            port_binder_ = std::make_unique<AutoPortBinder>();
            int new_port = port_binder_->getPort();
            if (new_port < 0) {
                LOG(ERROR) << "Failed to bind available port"
                           << ", port=" << std::to_string(new_port);
                continue;
            }
            te_port_ = static_cast<uint16_t>(new_port);
        }

        auto local_server_name = local_hostname;
        if (is_auto_port) {
            const auto host_size =
                local_ip_.size() + (local_hostname.starts_with('[') ? 2 : 0);
            if (local_hostname.size() == host_size) {
                // E.g. port 12345: "localhost" -> "localhost:12345",
                // "::1" or "[::1]" -> "[::1]:12345".
                local_server_name = GetLocalEndpoint();
            } else {
                // E.g. port 12345: "localhost:0" -> "localhost:12345",
                // "[::1]:0:npu_2" -> "[::1]:12345:npu_2".
                const auto port_end = local_hostname.find(':', host_size + 1);
                local_server_name.replace(host_size + 1,
                                          port_end - host_size - 1,
                                          std::to_string(te_port_));
            }
        }
        err = InnerInitTransferEngine(auto_discover, protocol, device_names,
                                      metadata_connstring, local_server_name);
        if (err == ErrorCode::OK) {
            // TENT mode: Skip manual transport installation - TENT handles this
            // internally
            if (use_tent_) {
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

ErrorCode ClientResources::InnerInitTransferEngine(
    bool auto_discover, const std::string& protocol,
    const std::optional<std::string>& device_names,
    const std::string& metadata_connstring, const std::string& local_hostname) {
    transfer_engine_ = std::make_shared<TransferEngine>();
    if (!use_tent_) {
        transfer_engine_->setAutoDiscover(auto_discover);
    }
    if (auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is enabled for protocol: "
                  << protocol;
        auto filters = get_auto_discover_filters();
        transfer_engine_->setWhitelistFilters(std::move(filters));
    }

    int rc = transfer_engine_->init(metadata_connstring, local_hostname);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (use_tent_) {
        return ErrorCode::OK;
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
        }
    }

    return ErrorCode::OK;
}

tl::expected<void, ErrorCode> ClientResources::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length);
    if (!check_result) {
        LOG(ERROR) << "RegisterLocalMemory param check failed, addr=" << addr
                   << ", length=" << length << ", location=" << location
                   << ", error=" << toString(check_result.error());
        return tl::unexpected(check_result.error());
    }
    if (!transfer_engine_) {
        LOG(ERROR) << "Memory registration requires a transfer engine";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        LOG(ERROR) << "transfer_engine registerLocalMemory failed, addr="
                   << addr << ", length=" << length << ", location=" << location
                   << ", remote_accessible=" << remote_accessible;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> ClientResources::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (transfer_engine_->unregisterLocalMemory(addr, update_metadata) != 0) {
        LOG(ERROR) << "transfer_engine unregisterLocalMemory failed, addr="
                   << addr;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode ClientResources::InitLocalBufferAllocator(size_t pool_size,
                                                    const std::string& protocol,
                                                    bool use_hugepage) {
    if (pool_size == 0) {
        LOG(INFO) << "Local buffer size is 0, skip registering local memory";
        return ErrorCode::OK;
    }
    try {
        local_buffer_allocator_ =
            ClientBufferAllocator::create(pool_size, protocol, use_hugepage);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to allocate local buffer, size=" << pool_size
                   << ", protocol=" << protocol << ": " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
    if (local_buffer_allocator_) {
        auto result = RegisterLocalMemory(local_buffer_allocator_->getBase(),
                                          local_buffer_allocator_->size(), "*",
                                          false, true);
        if (!result) {
            LOG(ERROR) << "Failed to register buffer allocator memory: "
                       << toString(result.error());
            local_buffer_allocator_.reset();
            return result.error();
        }
        LOG(INFO) << "Buffer allocator initialized: " << pool_size << " bytes";
        return ErrorCode::OK;
    }
    LOG(ERROR) << "Failed to create local buffer allocator, size=" << pool_size
               << ", protocol=" << protocol;
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode ClientResources::ReleaseLocalBuffer(bool update_metadata) {
    if (!local_buffer_allocator_) {
        return ErrorCode::OK;
    }
    try {
        auto result = unregisterLocalMemory(local_buffer_allocator_->getBase(),
                                            update_metadata);
        local_buffer_allocator_.reset();
        if (!result) {
            LOG(ERROR) << "ReleaseLocalBuffer: unregister memory failed, error="
                       << result.error();
            return result.error();
        }
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "ReleaseLocalBuffer failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "ReleaseLocalBuffer failed with unknown exception";
    }
    return ErrorCode::INTERNAL_ERROR;
}

}  // namespace mooncake
