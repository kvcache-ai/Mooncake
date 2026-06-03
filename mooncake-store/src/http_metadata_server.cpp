#include "http_metadata_server.h"

#include <csignal>
#include <ylt/coro_http/coro_http_server.hpp>
#include <glog/logging.h>

#include <mutex>
#include <string>

namespace mooncake {

HttpMetadataServer::HttpMetadataServer(uint16_t port, const std::string& host)
    : port_(port),
      host_(host),
      server_(std::make_unique<coro_http::coro_http_server>(4, port)),
      running_(false) {
    init_server();
}

HttpMetadataServer::~HttpMetadataServer() { stop(); }

void HttpMetadataServer::init_server() {
    using namespace coro_http;

    // GET /metadata?key=<key>
    server_->set_http_handler<GET>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(store_mutex_);
            auto it = store_.find(std::string(key));
            if (it == store_.end()) {
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, it->second);
        });

    // PUT /metadata?key=<key>
    server_->set_http_handler<PUT>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::string body(req.get_body());
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                if (key.find("rpc_meta") != std::string::npos &&
                    store_.find(std::string(key)) != store_.end()) {
                    resp.set_status_and_content(
                        status_type::bad_request,
                        "Duplicate rpc_meta key not allowed");
                    return;
                }
                store_[std::string(key)] = body;
            }

            resp.set_status_and_content(status_type::ok, "metadata updated");
        });

    // DELETE /metadata?key=<key>
    server_->set_http_handler<coro_http::http_method::DEL>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(store_mutex_);
            auto it = store_.find(std::string(key));
            if (it == store_.end()) {
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            store_.erase(it);
            resp.set_status_and_content(status_type::ok, "metadata deleted");
        });

    // Health check endpoint
    server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

bool HttpMetadataServer::start() {
    if (running_) {
        return true;
    }

    server_->async_start();
    running_ = true;
    LOG(INFO) << "HTTP metadata server started on " << host_ << ":" << port_;
    return true;
}

void HttpMetadataServer::stop() {
    if (!running_) {
        return;
    }

    server_->stop();
    running_ = false;
    LOG(INFO) << "HTTP metadata server stopped";
}

KVPoll HttpMetadataServer::poll() const {
    if (!running_) {
        return KVPoll::Failed;
    }
    return KVPoll::Success;
}

}  // namespace mooncake
