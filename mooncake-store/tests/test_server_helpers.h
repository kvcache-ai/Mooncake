#pragma once

#include <memory>
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
namespace testing {

// Lightweight in-process master server for tests (non-HA).
// Optionally starts embedded HTTP metadata server for transfer engine.
class InProcMaster {
   public:
    InProcMaster() = default;
    ~InProcMaster() { Stop(); }

    bool Start(int rpc_port = 0, int http_metrics_port = 0,
               bool enable_http_metadata = true, int http_metadata_port = 0) {
        try {
            // Choose ports if not provided
            rpc_port_ = rpc_port > 0 ? rpc_port : getFreeTcpPort();
            http_metrics_port_ =
                http_metrics_port > 0 ? http_metrics_port : getFreeTcpPort();
            if (enable_http_metadata) {
                http_metadata_port_ = http_metadata_port > 0
                                          ? http_metadata_port
                                          : getFreeTcpPort();
            } else {
                http_metadata_port_ = 0;
            }

            // Optional HTTP metadata server
            if (enable_http_metadata) {
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

            WrappedMasterServiceConfig cfg;
            cfg.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
            cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
            cfg.allow_evict_soft_pinned_objects = true;
            cfg.enable_metric_reporting = false;
            cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
            cfg.eviction_high_watermark_ratio =
                DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
            cfg.view_version = 0;
            // Use default client_live_ttl_sec to align with production defaults
            cfg.enable_ha = false;
            cfg.http_port = static_cast<uint16_t>(http_metrics_port_);
            cfg.cluster_id = DEFAULT_CLUSTER_ID;
            cfg.root_fs_dir = DEFAULT_ROOT_FS_DIR;
            cfg.memory_allocator = BufferAllocatorType::OFFSET;

            wrapped_ = std::make_unique<WrappedMasterService>(cfg);
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
