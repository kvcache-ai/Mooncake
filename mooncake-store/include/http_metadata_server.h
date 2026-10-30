#ifndef MOONCAKE_HTTP_METADATA_SERVER_H
#define MOONCAKE_HTTP_METADATA_SERVER_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>

#include <ylt/coro_http/coro_http_server.hpp>

namespace mooncake {

enum class KVPoll {
    Failed = 0,
    Bootstrapping = 1,
    WaitingForInput = 2,
    Transferring = 3,
    Success = 4
};

class HttpMetadataServer {
   public:
    // Reads MC_METADATA_HTTP_TOKEN / MC_METADATA_HTTP_ALLOW_INSECURE and
    // MC_METADATA_MAX_{KEYS,TOTAL_BYTES,BODY_BYTES} at construction. Invalid
    // quotas throw std::invalid_argument. Non-loopback binds without a token
    // require the explicit insecure opt-in; otherwise start() returns false.
    HttpMetadataServer(uint16_t port, const std::string& host = "0.0.0.0");
    ~HttpMetadataServer();

    uint16_t port() const { return server_->port(); }

    // Start the HTTP metadata server
    bool start();

    // Stop the HTTP metadata server
    void stop();

    // Poll the server status
    KVPoll poll() const;

    // Check if the server is running
    bool is_running() const { return running_; }

    // Remove a key from the metadata store (for internal use by MasterService)
    // Returns true if key was found and removed, false if key did not exist
    bool removeKey(const std::string& key);

    // Remove multiple keys from the metadata store
    // Returns the number of keys that were successfully removed
    size_t removeKeys(const std::vector<std::string>& keys);

    // Non-copyable
    HttpMetadataServer(const HttpMetadataServer&) = delete;
    HttpMetadataServer& operator=(const HttpMetadataServer&) = delete;

   private:
    void init_server();
    bool authorize(coro_http::coro_http_request& req,
                   coro_http::coro_http_response& resp) const;
    // Caller holds store_mutex_. All deletion paths share this accounting.
    bool erase_key_locked(const std::string& key);

    uint16_t port_;
    std::string host_;
    const std::string auth_token_;
    const bool allow_insecure_;
    const size_t max_keys_;
    const size_t max_total_bytes_;
    const size_t max_body_bytes_;
    std::unique_ptr<coro_http::coro_http_server> server_;
    std::unordered_map<std::string, std::string> store_;
    // store_.size() and total_bytes_ are guarded by store_mutex_. Only key
    // and value payload bytes are counted, not allocator/HTTP overhead or RSS.
    size_t total_bytes_ = 0;
    mutable std::mutex store_mutex_;
    bool running_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HTTP_METADATA_SERVER_H
