#ifndef MOONCAKE_HTTP_METADATA_SERVER_H
#define MOONCAKE_HTTP_METADATA_SERVER_H

#include <memory>
#include <string>
#include <vector>

namespace mooncake {

class HttpMetadataServerImpl;

class HttpMetadataClient {
   public:
    explicit HttpMetadataClient(std::string metadata_uri);

    bool removeKey(const std::string& key) const;

   private:
    static std::string encodeQueryValue(const std::string& value);

    std::string metadata_uri_;
};

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
    bool is_running() const;

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

    std::unique_ptr<HttpMetadataServerImpl> impl_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HTTP_METADATA_SERVER_H
