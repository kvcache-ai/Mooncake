#include "http_metadata_server.h"

#include <charconv>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <ylt/coro_http/coro_http_server.hpp>
#include <glog/logging.h>

#include <mutex>
#include <string>

namespace mooncake {
namespace {

std::string env_string(const char* name) {
    const char* value = std::getenv(name);
    return value ? value : "";
}

size_t positive_size_env(const char* name, size_t fallback) {
    const auto raw = env_string(name);
    if (raw.empty()) return fallback;
    size_t value = 0;
    const auto [end, ec] =
        std::from_chars(raw.data(), raw.data() + raw.size(), value);
    // YLT's Content-Length limit is signed; use the same range for all quotas.
    if (ec != std::errc{} || end != raw.data() + raw.size() || value == 0 ||
        value > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw std::invalid_argument(std::string(name) +
                                    " must be a positive integer <= INT64_MAX");
    }
    return value;
}

bool insecure_opted_in() {
    const auto value = env_string("MC_METADATA_HTTP_ALLOW_INSECURE");
    return value == "1" || coro_http::iequal0(value, "true") ||
           coro_http::iequal0(value, "yes");
}

bool is_loopback(const std::string& host) {
    std::error_code ec;
    const auto address = asio::ip::make_address(host, ec);
    return !ec && address.is_loopback();
}

bool token_equal(std::string_view presented, std::string_view expected) {
    if (presented.size() != expected.size()) return false;
    // Inspect every byte for equal-length tokens, without a content-dependent
    // early exit. Volatile prevents replacing this loop with an early-out
    // memcmp.
    volatile unsigned char difference = 0;
    for (size_t i = 0; i < expected.size(); ++i) {
        difference = difference | (presented[i] ^ expected[i]);
    }
    return difference == 0;
}

}  // namespace

HttpMetadataServer::HttpMetadataServer(uint16_t port, const std::string& host)
    : port_(port),
      // Pin localhost to loopback rather than trusting DNS for the bind gate.
      host_(host == "localhost" ? "127.0.0.1" : host),
      auth_token_(env_string("MC_METADATA_HTTP_TOKEN")),
      allow_insecure_(insecure_opted_in()),
      max_keys_(positive_size_env("MC_METADATA_MAX_KEYS", 65536)),
      max_total_bytes_(
          positive_size_env("MC_METADATA_MAX_TOTAL_BYTES", 1ULL << 30)),
      max_body_bytes_(
          positive_size_env("MC_METADATA_MAX_BODY_BYTES", 1ULL << 20)),
      server_(std::make_unique<coro_http::coro_http_server>(4, port, host_)),
      running_(false) {
    init_server();
}

HttpMetadataServer::~HttpMetadataServer() { stop(); }

bool HttpMetadataServer::authorize(coro_http::coro_http_request& req,
                                   coro_http::coro_http_response& resp) const {
    if (auth_token_.empty()) return true;
    const auto header = req.get_header_value("Authorization");
    const auto space = header.find(' ');
    if (space != std::string_view::npos &&
        coro_http::iequal0(header.substr(0, space), "bearer") &&
        token_equal(header.substr(space + 1), auth_token_)) {
        return true;
    }
    resp.add_header("WWW-Authenticate", "Bearer");
    resp.set_status_and_content(coro_http::status_type::unauthorized,
                                "unauthorized");
    return false;
}

void HttpMetadataServer::init_server() {
    using namespace coro_http;

    // Enforced by YLT before allocating/reading a declared body (currently
    // returns HTTP 400). A handler-only check would be too late to bound reads.
    server_->set_max_http_body_size(static_cast<int64_t>(max_body_bytes_));

    // GET /metadata?key=<key>
    server_->set_http_handler<GET>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            if (!authorize(req, resp)) return;
            auto key = req.get_decode_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(store_mutex_);
            auto it = store_.find(key);
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
            if (!authorize(req, resp)) return;
            // HTTPStoragePlugin sends fixed-length bodies. YLT leaves streamed
            // bodies to handlers; do not silently store an empty value or read
            // an unbounded stream on this endpoint.
            if (!req.get_header_value("Transfer-Encoding").empty() ||
                req.get_content_type() == content_type::multipart) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "Streamed metadata bodies are not supported");
                return;
            }
            auto key = req.get_decode_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            auto body = req.get_body();
            if (body.size() > max_body_bytes_) {
                resp.set_status_and_content(status_type::bad_request,
                                            "request body too large");
                return;
            }
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                auto it = store_.find(key);
                if (key.find("rpc_meta") != std::string::npos) {
                    if (it != store_.end()) {
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
                // The pinned YLT serializer does not support HTTP 507; use
                // 503 for quota exhaustion instead of its fallback HTTP 501.
                if (it == store_.end() && store_.size() >= max_keys_) {
                    resp.set_status_and_content(
                        status_type::service_unavailable,
                        "metadata key budget exhausted");
                    return;
                }
                const size_t old_bytes =
                    it == store_.end() ? 0
                                       : it->first.size() + it->second.size();
                const size_t retained_bytes = total_bytes_ - old_bytes;
                const size_t available_bytes =
                    max_total_bytes_ - retained_bytes;
                // Subtraction avoids overflow, including at configured limits.
                if (key.size() > available_bytes ||
                    body.size() > available_bytes - key.size()) {
                    resp.set_status_and_content(
                        status_type::service_unavailable,
                        "metadata size budget exhausted");
                    return;
                }
                const size_t new_total =
                    retained_bytes + key.size() + body.size();
                store_.insert_or_assign(std::move(key), std::string(body));
                total_bytes_ = new_total;
            }

            resp.set_status_and_content(status_type::ok, "metadata updated");
        });

    // DELETE /metadata?key=<key>
    server_->set_http_handler<coro_http::http_method::DEL>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            if (!authorize(req, resp)) return;
            auto key = req.get_decode_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::lock_guard<std::mutex> lock(store_mutex_);
            if (!erase_key_locked(key)) {
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

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

    if (auth_token_.empty() && !is_loopback(host_)) {
        if (!allow_insecure_) {
            LOG(ERROR)
                << "HTTP metadata server refuses a non-loopback bind without "
                   "MC_METADATA_HTTP_TOKEN. Set the token on the server and "
                   "every engine, bind loopback, or explicitly opt into "
                   "MC_METADATA_HTTP_ALLOW_INSECURE=1 on a trusted network.";
            return false;
        }
        LOG(WARNING)
            << "HTTP metadata server has no authentication "
               "(MC_METADATA_HTTP_ALLOW_INSECURE opt-in): anyone who can "
               "reach this port can read and rewrite cluster metadata.";
    }

    // async_start() binds synchronously and hands back a future that is already
    // resolved (hasResult()) when the bind failed; otherwise the server keeps
    // running. Mirror MasterAdminServer::Start() so a failed bind is surfaced
    // instead of reporting a healthy server that never came up.
    auto ec = server_->async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "Failed to start HTTP metadata server on " << host_ << ":"
                   << port_;
        return false;
    }
    port_ = server_->port();
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

bool HttpMetadataServer::erase_key_locked(const std::string& key) {
    auto it = store_.find(key);
    if (it == store_.end()) return false;
    total_bytes_ -= it->first.size() + it->second.size();
    store_.erase(it);
    return true;
}

bool HttpMetadataServer::removeKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(store_mutex_);
    if (!erase_key_locked(key)) return false;
    LOG(INFO) << "HttpMetadataServer: removed key=" << key;
    return true;
}

size_t HttpMetadataServer::removeKeys(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lock(store_mutex_);
    size_t removed = 0;
    for (const auto& key : keys) {
        if (erase_key_locked(key)) {
            LOG(INFO) << "HttpMetadataServer: removed key=" << key;
            ++removed;
        }
    }
    return removed;
}

}  // namespace mooncake
