#ifndef MOONCAKE_HTTP_METADATA_SERVER_H
#define MOONCAKE_HTTP_METADATA_SERVER_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>

#include <ylt/coro_http/coro_http_server.hpp>
#include "types.h"
#include "rpc_service.h"

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
    HttpMetadataServer(
        uint16_t port, const std::string& host,
        std::shared_ptr<WrappedMasterService> wrapped_master_service);
    ~HttpMetadataServer();

    // Start the HTTP metadata server
    bool start();

    // Stop the HTTP metadata server
    void stop();

    // Poll the server status
    KVPoll poll() const;

    // Check if the server is running
    bool is_running() const { return running_; }

    // Non-copyable
    HttpMetadataServer(const HttpMetadataServer&) = delete;
    HttpMetadataServer& operator=(const HttpMetadataServer&) = delete;

   private:
    void init_server();
    void health_monitor_thread_func();
    void check_and_cleanup_metadata();
    bool is_segment_healthy(
        const std::string& segment_name,
        const std::unordered_set<std::string>& all_segments);
    void cleanup_segment_metadata(const std::string& segment_name);

    uint16_t port_;
    std::string host_;
    std::unique_ptr<coro_http::coro_http_server> server_;
    std::unordered_map<std::string, std::string> store_;
    mutable std::mutex store_mutex_;
    bool running_;

    // Health monitoring
    std::shared_ptr<WrappedMasterService> wrapped_master_service_;
    std::thread health_monitor_thread_;
    std::atomic<bool> health_monitor_running_{false};
    static constexpr uint64_t kHealthMonitorSleepMs = 600000;  // 10 minutes
};

}  // namespace mooncake

#endif  // MOONCAKE_HTTP_METADATA_SERVER_H
