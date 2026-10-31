#pragma once

#include <cstdlib>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <csignal>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "http_metadata_server.h"
#include "master_config.h"
#include "rpc_service.h"
#include "types.h"
#include "utils.h"
#include <ylt/util/tl/expected.hpp>
#include "utils.h"

namespace mooncake {
namespace testing {

// Lightweight in-process master server for tests (non-HA).
// An explicit metadata port of zero disables the embedded HTTP metadata server.
class InProcMaster {
   public:
    InProcMaster() = default;
    ~InProcMaster() { Stop(); }

    bool Start(InProcMasterConfig config) {
        if (server_) {
            LOG(ERROR) << "In-process test Master is already running on port "
                       << rpc_port_;
            return false;
        }
        try {
            // Let each listener bind port zero so the kernel allocates and
            // holds its port atomically. Probing free ports before binding
            // races with other clients and processes.
            rpc_port_ = config.rpc_port.value_or(0);
            http_metrics_port_ = config.http_metrics_port.value_or(0);
            http_metadata_port_ = config.http_metadata_port.value_or(0);

            // An explicit metadata port of zero disables this optional
            // listener.
            if (!config.http_metadata_port || http_metadata_port_ > 0) {
                meta_server_ = std::make_unique<HttpMetadataServer>(
                    static_cast<uint16_t>(http_metadata_port_), "127.0.0.1");
                if (!meta_server_->start()) {
                    LOG(ERROR)
                        << "Failed to start test HTTP metadata server on "
                        << http_metadata_port_;
                    Stop();
                    return false;
                }
                http_metadata_port_ = meta_server_->port();
            }

            // RPC server + master service
            server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"127.0.0.1",
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

            WrappedMasterServiceConfig wms_cfg;
            wms_cfg.default_kv_lease_ttl = default_kv_lease_ttl;
            wms_cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
            wms_cfg.allow_evict_soft_pinned_objects = true;
            wms_cfg.enable_metric_reporting = false;
            wms_cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
            wms_cfg.eviction_high_watermark_ratio =
                DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
            wms_cfg.view_version = 0;
            wms_cfg.enable_ha = false;
            wms_cfg.http_port = static_cast<uint16_t>(http_metrics_port_);
            wms_cfg.cluster_id = DEFAULT_CLUSTER_ID;
            wms_cfg.root_fs_dir = DEFAULT_ROOT_FS_DIR;
            wms_cfg.memory_allocator = BufferAllocatorType::OFFSET;

            wms_cfg.enable_cxl = config.enable_cxl.has_value()
                                     ? config.enable_cxl.value()
                                     : false;
            if (config.cxl_path.has_value()) {
                wms_cfg.cxl_path = config.cxl_path.value();
            } else if (const char* cxl_path_env =
                           std::getenv("MC_CXL_DEV_PATH")) {
                wms_cfg.cxl_path = cxl_path_env;
            }

            if (config.cxl_size.has_value()) {
                wms_cfg.cxl_size = config.cxl_size.value();
            } else if (const char* cxl_size_env =
                           std::getenv("MC_CXL_DEV_SIZE")) {
                char* endptr = nullptr;
                unsigned long long val =
                    std::strtoull(cxl_size_env, &endptr, 10);
                if (endptr != cxl_size_env && *endptr == '\0') {
                    wms_cfg.cxl_size = static_cast<size_t>(val);
                }
            }

            wrapped_ = std::make_unique<WrappedMasterService>(wms_cfg);
            if (!wrapped_->http_server_started()) {
                LOG(ERROR)
                    << "Failed to start test Master HTTP metrics server on "
                    << http_metrics_port_;
                Stop();
                return false;
            }
            http_metrics_port_ = wrapped_->http_port();
            RegisterRpcService(*server_, *wrapped_);

            auto ec = server_->async_start();
            if (ec.hasResult()) {
                LOG(ERROR) << "Failed to start test Master RPC server on "
                           << rpc_port_;
                Stop();
                return false;
            }
            rpc_port_ = server_->port();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return true;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to start in-process test Master: "
                       << e.what();
            Stop();
            return false;
        } catch (...) {
            LOG(ERROR)
                << "Failed to start in-process test Master: unknown exception";
            Stop();
            return false;
        }
    }

    void Stop() {
        if (server_) {
            server_->stop();
            server_.reset();
        }
        wrapped_.reset();
        if (meta_server_) {
            meta_server_->stop();
            meta_server_.reset();
        }
        // Preserve the last bound addresses: callers use these accessors after
        // Stop() to restart on the same ports for reconnection.
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

    WrappedMasterService& GetWrapped() { return *wrapped_; }

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
