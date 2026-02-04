#include "http_metadata_server.h"
#include "rpc_service.h"
#include <ylt/coro_http/coro_http_server.hpp>
#include <glog/logging.h>

#include <mutex>
#include <string>
#include <regex>
#include <thread>
#include <chrono>

namespace mooncake {

HttpMetadataServer::HttpMetadataServer(uint16_t port, const std::string& host)
    : port_(port),
      host_(host),
      server_(std::make_unique<coro_http::coro_http_server>(4, port)),
      wrapped_master_service_(nullptr),
      running_(false) {
    init_server();
}

HttpMetadataServer::HttpMetadataServer(
    uint16_t port, const std::string& host,
    std::shared_ptr<WrappedMasterService> wrapped_master_service)
    : port_(port),
      host_(host),
      server_(std::make_unique<coro_http::coro_http_server>(4, port)),
      wrapped_master_service_(wrapped_master_service),
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

    // Start health monitoring thread if master service is provided
    if (wrapped_master_service_) {
        health_monitor_running_ = true;
        health_monitor_thread_ =
            std::thread(&HttpMetadataServer::health_monitor_thread_func, this);
    }

    LOG(INFO) << "HTTP metadata server started on " << host_ << ":" << port_;
    return true;
}

void HttpMetadataServer::stop() {
    if (!running_) {
        return;
    }

    // Stop health monitoring thread
    health_monitor_running_ = false;
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }

    server_->stop();
    running_ = false;
    LOG(INFO) << "HTTP metadata server stopped";
}

void HttpMetadataServer::health_monitor_thread_func() {
    while (health_monitor_running_) {
        check_and_cleanup_metadata();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kHealthMonitorSleepMs));
    }
}

void HttpMetadataServer::check_and_cleanup_metadata() {
    if (!wrapped_master_service_) {
        return;
    }

    // Get all segments once from master service
    auto segments_result = wrapped_master_service_->GetAllSegments();
    if (!segments_result.has_value()) {
        LOG(WARNING) << "Failed to get all segments for metadata cleanup";
        return;
    }
    const auto& all_segments = segments_result.value();

    // Convert to unordered_set for O(1) lookup
    std::unordered_set<std::string> segment_set(all_segments.begin(),
                                                all_segments.end());

    // Get all current keys from the metadata store
    std::vector<std::string> keys_to_check;
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        for (const auto& pair : store_) {
            keys_to_check.push_back(pair.first);
        }
    }

    // Check each key to see if it corresponds to segment metadata
    // that should be cleaned up
    for (const auto& key : keys_to_check) {
        // Check if this key corresponds to segment metadata (e.g., keys
        // containing "segment")
        if (key.find("segment") != std::string::npos) {
            std::string segment_name = key;
            // Extract segment name from key if needed
            if (key.find("rpc_meta_") == 0) {
                segment_name = key.substr(9);  // Remove "rpc_meta_" prefix
            }
            if (!is_segment_healthy(segment_name, segment_set)) {
                cleanup_segment_metadata(segment_name);
            }
        }
        // Note: Removed client health check as it has side effects.
        // Client metadata cleanup should be handled by the master service
        // based on actual client liveness tracking.
    }
}

bool HttpMetadataServer::is_segment_healthy(
    const std::string& segment_name,
    const std::unordered_set<std::string>& all_segments) {
    // Check if the segment exists in the provided set
    return all_segments.find(segment_name) != all_segments.end();
}

void HttpMetadataServer::cleanup_segment_metadata(
    const std::string& segment_name) {
    std::lock_guard<std::mutex> lock(store_mutex_);

    // Find and remove all metadata entries related to this segment
    for (auto it = store_.begin(); it != store_.end();) {
        if (it->first.find(segment_name) != std::string::npos) {
            LOG(INFO) << "Cleaning up metadata for segment: " << segment_name
                      << ", key: " << it->first;
            it = store_.erase(it);
        } else {
            ++it;
        }
    }
}

KVPoll HttpMetadataServer::poll() const {
    if (!running_) {
        return KVPoll::Failed;
    }
    return KVPoll::Success;
}

}  // namespace mooncake
