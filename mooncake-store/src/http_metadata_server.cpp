#include "http_metadata_server.h"

#include <async_simple/coro/SyncAwait.h>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <sstream>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <glog/logging.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace mooncake {

class HttpMetadataServerImpl {
   public:
    HttpMetadataServerImpl(uint16_t port, std::string host)
        : port(port),
          host(std::move(host)),
          server(std::make_unique<coro_http::coro_http_server>(4, port)) {}

    uint16_t port;
    std::string host;
    std::unique_ptr<coro_http::coro_http_server> server;
    std::unordered_map<std::string, std::string> store;
    mutable std::mutex store_mutex;
    bool running{false};
};

HttpMetadataClient::HttpMetadataClient(std::string metadata_uri)
    : metadata_uri_(std::move(metadata_uri)) {}

std::string HttpMetadataClient::encodeQueryValue(const std::string& value) {
    std::ostringstream encoded;
    encoded << std::uppercase << std::hex;
    for (const unsigned char ch : value) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' ||
            ch == '~') {
            encoded << static_cast<char>(ch);
        } else {
            encoded << '%' << std::setw(2) << std::setfill('0')
                    << static_cast<unsigned int>(ch);
        }
    }
    return encoded.str();
}

bool HttpMetadataClient::removeKey(const std::string& key) const {
    try {
        coro_http::coro_http_client client;
        client.set_conn_timeout(std::chrono::milliseconds(1500));
        client.set_req_timeout(std::chrono::milliseconds(3000));
        const char separator =
            metadata_uri_.find('?') == std::string::npos ? '?' : '&';
        const std::string url =
            metadata_uri_ + separator + "key=" + encodeQueryValue(key);
        auto response = async_simple::coro::syncAwait(
            client.async_delete(url, "", coro_http::req_content_type::json));
        return response.status == 200;
    } catch (const std::exception& error) {
        LOG(ERROR) << "HTTP metadata DELETE failed: " << error.what();
        return false;
    }
}

HttpMetadataServer::HttpMetadataServer(uint16_t port, const std::string& host)
    : impl_(std::make_unique<HttpMetadataServerImpl>(port, host)) {
    init_server();
}

HttpMetadataServer::~HttpMetadataServer() { stop(); }

void HttpMetadataServer::init_server() {
    using namespace coro_http;

    // GET /metadata?key=<key>
    impl_->server->set_http_handler<GET>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(impl_->store_mutex);
            auto it = impl_->store.find(std::string(key));
            if (it == impl_->store.end()) {
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, it->second);
        });

    // PUT /metadata?key=<key>
    impl_->server->set_http_handler<PUT>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::string body(req.get_body());
            {
                std::lock_guard<std::mutex> lock(impl_->store_mutex);
                std::string key_str(key);
                if (key_str.find("rpc_meta") != std::string::npos) {
                    auto it = impl_->store.find(key_str);
                    if (it != impl_->store.end()) {
                        if (it->second == body) {
                            resp.set_status_and_content(status_type::ok,
                                                        "metadata unchanged");
                            return;
                        }
                        resp.set_status_and_content(
                            status_type::bad_request,
                            "Duplicate rpc_meta key not allowed");
                        return;
                    }
                }
                impl_->store[std::move(key_str)] = body;
            }

            resp.set_status_and_content(status_type::ok, "metadata updated");
        });

    // DELETE /metadata?key=<key>
    impl_->server->set_http_handler<coro_http::http_method::DEL>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(impl_->store_mutex);
            auto it = impl_->store.find(std::string(key));
            if (it == impl_->store.end()) {
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            impl_->store.erase(it);
            resp.set_status_and_content(status_type::ok, "metadata deleted");
        });

    // Health check endpoint
    impl_->server->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

bool HttpMetadataServer::start() {
    if (impl_->running) {
        return true;
    }

    // async_start() binds synchronously and hands back a future that is already
    // resolved (hasResult()) when the bind failed; otherwise the server keeps
    // running. Mirror MasterAdminServer::Start() so a failed bind is surfaced
    // instead of reporting a healthy server that never came up.
    auto ec = impl_->server->async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "Failed to start HTTP metadata server on " << impl_->host
                   << ":" << impl_->port;
        return false;
    }
    impl_->running = true;
    LOG(INFO) << "HTTP metadata server started on " << impl_->host << ":"
              << impl_->port;
    return true;
}

void HttpMetadataServer::stop() {
    if (!impl_->running) {
        return;
    }

    impl_->server->stop();
    impl_->running = false;
    LOG(INFO) << "HTTP metadata server stopped";
}

KVPoll HttpMetadataServer::poll() const {
    if (!impl_->running) {
        return KVPoll::Failed;
    }
    return KVPoll::Success;
}

bool HttpMetadataServer::is_running() const { return impl_->running; }

bool HttpMetadataServer::removeKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(impl_->store_mutex);
    if (impl_->store.erase(key) > 0) {
        LOG(INFO) << "HttpMetadataServer: removed key=" << key;
        return true;
    }
    return false;
}

size_t HttpMetadataServer::removeKeys(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lock(impl_->store_mutex);
    size_t removed = 0;
    for (const auto& key : keys) {
        if (impl_->store.erase(key) > 0) {
            LOG(INFO) << "HttpMetadataServer: removed key=" << key;
            ++removed;
        }
    }
    return removed;
}

}  // namespace mooncake
