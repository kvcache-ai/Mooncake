#pragma once

#include <cstdlib>
#include <exception>
#include <glog/logging.h>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/master/p2p_rpc_service.h"
#include "p2p/master/p2p_master_client.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

// Init publishes the local state; Master sees it on the next heartbeat.
// Data-path fixtures wait for that advertisement instead of assuming Init's
// return synchronously changes the Master's route visibility.
inline bool WaitForRoutableClient(P2PMasterClient& master,
                                  const UUID& client_id) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        auto ips = master.BatchQueryIp({client_id});
        if (ips && ips->contains(client_id)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

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

    InProcP2PMasterConfigBuilder& set_heartbeat_rpc_thread_num(uint32_t value) {
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
            rpc_port_ = config.rpc_port.value_or(0);

            server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                /*thread_num=*/4, /*port=*/rpc_port_, /*address=*/"0.0.0.0",
                std::chrono::seconds(0), /*tcp_no_delay=*/true);

            P2PMasterConfig wms_cfg;
            wms_cfg.metrics.enable_reporting = false;
            wms_cfg.metrics.http_port = 0;
            wms_cfg.rpc.heartbeat_port = config.heartbeat_rpc_port.value_or(0);
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
                if (hb_threads == 0) {
                    hb_threads = 1;
                }
                heartbeat_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
                    /*thread_num=*/hb_threads, /*port=*/heartbeat_rpc_port_,
                    /*address=*/"0.0.0.0", std::chrono::seconds(0),
                    /*tcp_no_delay=*/true);
                RegisterP2PHeartbeatRpcService(*heartbeat_server_, *wrapped_);
            }

            auto ec = server_->async_start();
            if (ec.hasResult()) {
                LOG(ERROR) << "Failed to start test P2P master on port "
                           << rpc_port_ << ": "
                           << ec.result().value().message();
                return false;
            }
            rpc_port_ = server_->port();
            if (heartbeat_server_) {
                auto hb_ec = heartbeat_server_->async_start();
                if (hb_ec.hasResult()) {
                    LOG(ERROR)
                        << "Failed to start test heartbeat server on port "
                        << heartbeat_rpc_port_ << ": "
                        << hb_ec.result().value().message();
                    return false;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return true;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to start test P2P master: " << e.what();
            return false;
        } catch (...) {
            LOG(ERROR) << "Failed to start test P2P master: unknown exception";
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
