#include "p2p/ha/p2p_status_http_server.h"

#include <utility>

#include <glog/logging.h>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "version.h"

namespace mooncake {

void RegisterStatusHttpHandlers(coro_http::coro_http_server& server) {
    using namespace coro_http;

    server.set_http_handler<GET>(
        "/ha/status", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "application/json; charset=utf-8");
            resp.set_status_and_content(
                status_type::ok, HAStatusManager::instance().SerializeJson());
        });

    server.set_http_handler<GET>("/ha/doctor", [](coro_http_request& req,
                                                  coro_http_response& resp) {
        resp.add_header("Content-Type", "application/json; charset=utf-8");
        resp.set_status_and_content(
            status_type::ok, HAStatusManager::instance().SerializeDoctorJson());
    });

    server.set_http_handler<GET>("/metrics", [](coro_http_request& req,
                                                coro_http_response& resp) {
        auto metrics = P2PMasterMetricManager::instance().serialize_metrics();
        metrics += HAMetricManager::instance().serialize_metrics();
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        resp.set_status_and_content(status_type::ok, std::move(metrics));
    });

    server.set_http_handler<GET>(
        "/server_info", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok,
                                        GetMooncakeStoreBuildInfo());
        });

    server.set_http_handler<GET>(
        "/metrics/summary",
        [](coro_http_request& req, coro_http_response& resp) {
            std::string summary =
                P2PMasterMetricManager::instance().get_summary_string();
            summary += " | ";
            summary += HAMetricManager::instance().get_summary_string();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    server.set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

void P2PStatusHttpServer::Start() {
    if (server_) {
        return;
    }

    server_ = std::make_unique<coro_http::coro_http_server>(
        /*thread_num=*/1, port_, host_);
    RegisterStatusHttpHandlers(*server_);
    // Component info is process scoped. Initialize it before the supervisor
    // HTTP server becomes reachable so standby /metrics scrapes are complete.
    P2PMasterMetricManager::instance().set_component_info(
        "p2p_master", GetMooncakeStoreVersion(), MOONCAKE_STORE_ARCHITECTURE);

    auto result = server_->async_start();
    if (result.hasResult()) {
        LOG(WARNING) << "Failed to start P2P status HTTP server"
                     << ", port=" << port_
                     << ", error=" << result.result().value();
        HAStatusManager::instance().SetLastError(
            "Failed to start P2P status HTTP server");
        server_.reset();
        return;
    }
    LOG(INFO) << "P2P status HTTP server started on port " << server_->port();
}

void P2PStatusHttpServer::Stop() {
    if (!server_) {
        return;
    }
    const uint16_t bound_port = server_->port();
    server_->stop();
    LOG(INFO) << "P2P status HTTP server stopped on port " << bound_port;
    server_.reset();
}

uint16_t P2PStatusHttpServer::port() const {
    return server_ ? server_->port() : port_;
}

}  // namespace mooncake
