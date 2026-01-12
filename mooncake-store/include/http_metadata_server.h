#ifndef MOONCAKE_HTTP_METADATA_SERVER_H
#define MOONCAKE_HTTP_METADATA_SERVER_H

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
    HttpMetadataServer(uint16_t port, const std::string& host = "0.0.0.0");
    ~HttpMetadataServer();

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

    uint16_t port_;
    std::string host_;
    std::unique_ptr<coro_http::coro_http_server> server_;
    std::unordered_map<std::string, std::string> store_;
    mutable std::mutex store_mutex_;
    bool running_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HTTP_METADATA_SERVER_H
