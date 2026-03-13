#pragma once

#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "master_config.h"
#include "p2p_rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

/**
 * @brief Lightweight in-process P2P master server for tests (non-HA).
 *
 * Mirrors InProcMaster but uses WrappedP2PMasterService and
 * RegisterP2PRpcService so that P2P-specific RPCs (GetWriteRoute,
 * AddReplica, RemoveReplica) are registered alongside the base RPCs.
 */
class InProcP2PMaster {
   public:
    InProcP2PMaster() = default;
    ~InProcP2PMaster() { Stop(); }

    bool Start(InProcMasterConfig config = {}) {
        try {
            rpc_port_ = config.rpc_port.has_value() ? config.rpc_port.value()
                                                    : getFreeTcpPort();

            server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"0.0.0.0",
                std::chrono::seconds(0), /*tcp_no_delay=*/true);

            WrappedMasterServiceConfig wms_cfg;
            wms_cfg.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
            wms_cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
            wms_cfg.allow_evict_soft_pinned_objects = true;
            wms_cfg.enable_metric_reporting = false;
            wms_cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
            wms_cfg.eviction_high_watermark_ratio =
                DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
            wms_cfg.view_version = 0;
            wms_cfg.enable_ha = false;
            wms_cfg.cluster_id = DEFAULT_CLUSTER_ID;
            wms_cfg.root_fs_dir = DEFAULT_ROOT_FS_DIR;
            wms_cfg.memory_allocator = BufferAllocatorType::OFFSET;
            wms_cfg.max_replicas_per_key = 0;  // no limit for P2P

            if (config.client_live_ttl_sec.has_value()) {
                wms_cfg.client_live_ttl_sec =
                    config.client_live_ttl_sec.value();
            } else {
                wms_cfg.client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
            }

            if (config.client_crashed_ttl_sec.has_value()) {
                wms_cfg.client_crashed_ttl_sec =
                    config.client_crashed_ttl_sec.value();
            } else {
                wms_cfg.client_crashed_ttl_sec = DEFAULT_CLIENT_CRASHED_TTL_SEC;
            }

            wrapped_ = std::make_unique<WrappedP2PMasterService>(wms_cfg);
            RegisterP2PRpcService(*server_, *wrapped_);

            auto ec = server_->async_start();
            if (ec.hasResult()) {
                return false;
            }
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
    }

    int rpc_port() const { return rpc_port_; }
    std::string master_address() const {
        return std::string("127.0.0.1:") + std::to_string(rpc_port_);
    }
    WrappedP2PMasterService& GetWrapped() { return *wrapped_; }

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<WrappedP2PMasterService> wrapped_;
    int rpc_port_ = 0;
};

}  // namespace testing
}  // namespace mooncake
