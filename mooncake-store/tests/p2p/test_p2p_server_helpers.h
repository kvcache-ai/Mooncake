#pragma once

#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/master/p2p_rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

struct InProcP2PMasterConfig {
    std::optional<int> rpc_port;
    std::optional<int64_t> client_live_ttl_sec;
    std::optional<int64_t> client_crashed_ttl_sec;
    std::optional<int> heartbeat_rpc_port;
    std::optional<uint32_t> heartbeat_rpc_thread_num;
};

class InProcP2PMasterConfigBuilder {
   public:
    InProcP2PMasterConfigBuilder& set_rpc_port(int value) {
        config_.rpc_port = value;
        return *this;
    }

    InProcP2PMasterConfigBuilder& set_client_live_ttl_sec(int64_t value) {
        config_.client_live_ttl_sec = value;
        return *this;
    }

    InProcP2PMasterConfigBuilder& set_client_crashed_ttl_sec(int64_t value) {
        config_.client_crashed_ttl_sec = value;
        return *this;
    }

    InProcP2PMasterConfigBuilder& set_heartbeat_rpc_port(int value) {
        config_.heartbeat_rpc_port = value;
        return *this;
    }

    InProcP2PMasterConfigBuilder& set_heartbeat_rpc_thread_num(
        uint32_t value) {
        config_.heartbeat_rpc_thread_num = value;
        return *this;
    }

    InProcP2PMasterConfig build() const { return config_; }

   private:
    InProcP2PMasterConfig config_;
};

/**
 * @brief Lightweight in-process P2P master server for tests (non-HA).
 *
 * Mirrors InProcMaster but uses P2PMasterRpcService and
 * RegisterP2PRpcService so that P2P-specific RPCs (GetWriteRoute,
 * AddReplica, RemoveReplica) are registered alongside the base RPCs.
 */
class InProcP2PMaster {
   public:
    InProcP2PMaster() = default;
    ~InProcP2PMaster() { Stop(); }

    bool Start(InProcP2PMasterConfig config = {}) {
        try {
            rpc_port_ = config.rpc_port.has_value() ? config.rpc_port.value()
                                                    : getFreeTcpPort();

            server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"0.0.0.0",
                std::chrono::seconds(0), /*tcp_no_delay=*/true);

            P2PMasterConfig wms_cfg;
            wms_cfg.metrics.enable_reporting = false;
            wms_cfg.rpc.heartbeat_port =
                config.heartbeat_rpc_port.value_or(0);
            wms_cfg.routes.max_clients_per_key = 0;  // no limit for P2P

            if (config.client_live_ttl_sec.has_value()) {
                wms_cfg.client_lifecycle.live_ttl_seconds =
                    config.client_live_ttl_sec.value();
            } else {
                wms_cfg.client_lifecycle.live_ttl_seconds =
                    DEFAULT_CLIENT_LIVE_TTL_SEC;
            }

            if (config.client_crashed_ttl_sec.has_value()) {
                wms_cfg.client_lifecycle.crashed_ttl_seconds =
                    config.client_crashed_ttl_sec.value();
            } else {
                wms_cfg.client_lifecycle.crashed_ttl_seconds =
                    DEFAULT_CLIENT_CRASHED_TTL_SEC;
            }

            wrapped_ = std::make_unique<P2PMasterRpcService>(wms_cfg);
            wrapped_->init();
            const bool dedicated_heartbeat =
                config.heartbeat_rpc_port.has_value() &&
                config.heartbeat_rpc_port.value() > 0;
            const bool main_includes_heartbeat = !dedicated_heartbeat;
            RegisterP2PRpcService(
                *server_, *wrapped_,
                /*include_heartbeat=*/main_includes_heartbeat);
            if (dedicated_heartbeat) {
                heartbeat_rpc_port_ = config.heartbeat_rpc_port.value();
                uint32_t hb_threads =
                    config.heartbeat_rpc_thread_num.has_value()
                        ? config.heartbeat_rpc_thread_num.value()
                        : 1u;
                if (hb_threads == 0) hb_threads = 1;
                heartbeat_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                    /*thread_num=*/hb_threads, /*port=*/heartbeat_rpc_port_,
                    /*address=*/"0.0.0.0", std::chrono::seconds(0),
                    /*tcp_no_delay=*/true);
                RegisterP2PHeartbeatRpcService(*heartbeat_server_, *wrapped_);
            }

            auto ec = server_->async_start();
            if (ec.hasResult()) {
                return false;
            }
            if (heartbeat_server_) {
                auto hb_ec = heartbeat_server_->async_start();
                if (hb_ec.hasResult()) {
                    return false;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return true;
        } catch (...) {
            return false;
        }
    }

    void Stop() {
        if (heartbeat_server_) {
            heartbeat_server_->stop();
            heartbeat_server_.reset();
        }
        if (server_) {
            server_->stop();
            server_.reset();
        }
        wrapped_.reset();
    }

    int rpc_port() const { return rpc_port_; }
    int heartbeat_rpc_port() const { return heartbeat_rpc_port_; }
    std::string master_address() const {
        return std::string("127.0.0.1:") + std::to_string(rpc_port_);
    }
    P2PMasterRpcService& GetWrapped() { return *wrapped_; }

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<coro_rpc::coro_rpc_server> heartbeat_server_;
    std::unique_ptr<P2PMasterRpcService> wrapped_;
    int rpc_port_ = 0;
    int heartbeat_rpc_port_ = 0;
};

}  // namespace testing
}  // namespace mooncake
