#pragma once

#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "http_metadata_server.h"
#include "master_config.h"
#include "rpc_service.h"
#include "types.h"
#include "utils.h"
#include <ylt/util/tl/expected.hpp>
#include "utils.h"

namespace mooncake {

// Configuration for InProcMaster (in-process master server for testing)
struct InProcMasterConfig {
    std::optional<int> rpc_port;
    std::optional<int> http_metrics_port;
    std::optional<int> http_metadata_port;
    std::optional<uint64_t> default_kv_lease_ttl;
};

// Builder class for InProcMasterConfig
class InProcMasterConfigBuilder {
   private:
    std::optional<int> rpc_port_ = std::nullopt;
    std::optional<int> http_metrics_port_ = std::nullopt;
    std::optional<int> http_metadata_port_ = std::nullopt;
    std::optional<uint64_t> default_kv_lease_ttl_ = std::nullopt;

   public:
    InProcMasterConfigBuilder() = default;

    InProcMasterConfigBuilder& set_rpc_port(int port) {
        rpc_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_http_metrics_port(int port) {
        http_metrics_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_http_metadata_port(int port) {
        http_metadata_port_ = port;
        return *this;
    }

    InProcMasterConfigBuilder& set_default_kv_lease_ttl(uint64_t ttl) {
        default_kv_lease_ttl_ = ttl;
        return *this;
    }

    InProcMasterConfig build() const {
        InProcMasterConfig config;
        config.rpc_port = rpc_port_;
        config.http_metrics_port = http_metrics_port_;
        config.http_metadata_port = http_metadata_port_;
        config.default_kv_lease_ttl = default_kv_lease_ttl_;
        return config;
    };
};

namespace testing {
// Lightweight in-process master server for tests (non-HA).
// Optionally starts embedded HTTP metadata server for transfer engine
// (default off).
class InProcMaster {
   public:
    InProcMaster() = default;
    ~InProcMaster() { Stop(); }

    bool Start(InProcMasterConfig config) {
        try {
            // Choose ports if not provided
            rpc_port_ = config.rpc_port.has_value() ? config.rpc_port.value()
                                                    : getFreeTcpPort();
            http_metrics_port_ = config.http_metrics_port.has_value()
                                     ? config.http_metrics_port.value()
                                     : getFreeTcpPort();
            http_metadata_port_ = config.http_metadata_port.has_value()
                                      ? config.http_metadata_port.value()
                                      : getFreeTcpPort();

            // Optional HTTP metadata server
            if (http_metadata_port_ > 0) {
                meta_server_ = std::make_unique<HttpMetadataServer>(
                    static_cast<uint16_t>(http_metadata_port_), "127.0.0.1");
                if (!meta_server_->start()) {
                    return false;
                }
            }

            // RPC server + master service
            server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"0.0.0.0",
                std::chrono::seconds(0), /*tcp_no_delay=*/true);
            const char* value = std::getenv("MC_RPC_PROTOCOL");
            if (value && std::string_view(value) == "rdma") {
                server_->init_ibv();
            }

            uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
            if (config.default_kv_lease_ttl.has_value()) {
                default_kv_lease_ttl = config.default_kv_lease_ttl.value();
            } else if (const char* ttl_env =
                           std::getenv("DEFAULT_KV_LEASE_TTL")) {
                char* endptr = nullptr;
                unsigned long parsed = std::strtoul(ttl_env, &endptr, 10);
                if (endptr != ttl_env && endptr && *endptr == '\0') {
                    default_kv_lease_ttl = static_cast<uint64_t>(parsed);
                }
            }

            MasterConfig ms_cfg;
            ms_cfg.default_kv_lease_ttl = default_kv_lease_ttl;
            ms_cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
            ms_cfg.allow_evict_soft_pinned_objects = true;
            ms_cfg.enable_metric_reporting = false;
            ms_cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
            ms_cfg.eviction_high_watermark_ratio =
                DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
            // Use default client_live_ttl_sec to align with production
            // defaults
            ms_cfg.metrics_port = static_cast<uint16_t>(http_metrics_port_);
            ms_cfg.cluster_id = DEFAULT_CLUSTER_ID;
            ms_cfg.root_fs_dir = DEFAULT_ROOT_FS_DIR;
            ms_cfg.memory_allocator = "offset";

            wrapped_ = std::make_unique<WrappedMasterService>(ms_cfg);
            RegisterRpcService(*server_, *wrapped_);

            auto ec = server_->async_start();
            if (ec.hasResult()) {
                return false;
            }
            // Allow server to bind
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return true;
        } catch (...) {
            return false;
        }
    }

    void Stop() {
        if (server_) {
            server_->stop();
            server_.reset();
            wrapped_.reset();
        }
        if (meta_server_) {
            meta_server_->stop();
            meta_server_.reset();
        }
    }

    // Accessors
    int rpc_port() const { return rpc_port_; }
    int http_metrics_port() const { return http_metrics_port_; }
    int http_metadata_port() const { return http_metadata_port_; }
    std::string master_address() const {
        return std::string("127.0.0.1:") + std::to_string(rpc_port_);
    }
    std::string metadata_url() const {
        if (http_metadata_port_ <= 0) return {};
        return std::string("http://127.0.0.1:") +
               std::to_string(http_metadata_port_) + "/metadata";
    }
    std::string http_metrics_base() const {
        return std::string("http://127.0.0.1:") +
               std::to_string(http_metrics_port_);
    }

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<WrappedMasterService> wrapped_;
    std::unique_ptr<HttpMetadataServer> meta_server_;
    int rpc_port_ = 0;
    int http_metrics_port_ = 0;
    int http_metadata_port_ = 0;
};

// Helper: return all segments body or error
inline tl::expected<std::string, int> GetAllSegments(const InProcMaster& m) {
    return httpGet(m.http_metrics_base() + "/get_all_segments");
}

// Helper: check if a client hostname appears in segment list
inline tl::expected<bool, int> CheckSegmentVisible(
    const InProcMaster& m, const std::string& local_hostname) {
    auto r = GetAllSegments(m);
    if (!r) return tl::unexpected(r.error());
    const std::string& body = r.value();
    if (body.empty() || body.find(local_hostname) == std::string::npos) {
        return false;
    }
    return true;
}

}  // namespace testing
}  // namespace mooncake
