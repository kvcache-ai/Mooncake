#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_lifecycle_event.h"

namespace mooncake {

class HttpMetadataServer;

// Best-effort asynchronous cleanup of Transfer Engine metadata left behind by
// clients whose leases expired. The worker owns the remote metadata client but
// not a co-located HttpMetadataServer.
class HttpMetadataCleanupWorker {
   public:
    using RemoveKeyFn = std::function<bool(const std::string&)>;
    using CreateResult =
        tl::expected<std::unique_ptr<HttpMetadataCleanupWorker>, std::string>;

    // Validates the configured backend and returns either a ready-to-start
    // worker or an actionable initialization error.
    static CreateResult Create(
        HttpMetadataServer* http_metadata_server,
        const std::string& http_metadata_remote_url = "");

    HttpMetadataCleanupWorker(RemoveKeyFn remove_key,
                              std::string metadata_prefix);
    ~HttpMetadataCleanupWorker();

    HttpMetadataCleanupWorker(const HttpMetadataCleanupWorker&) = delete;
    HttpMetadataCleanupWorker& operator=(const HttpMetadataCleanupWorker&) =
        delete;

    // Starts the worker if a usable local or remote backend was configured.
    // Returns false when cleanup is disabled or backend initialization failed.
    bool Start();

    // Stops accepting work, finishes the batch already claimed by the worker,
    // and discards work that has not started.
    void Stop();

    // Copies an event into the queue. This method is intentionally fast so it
    // can be called directly from a MasterService lifecycle callback.
    void Enqueue(const ClientLeaseExpiredEvent& event);

   private:
    void ThreadFunc();
    bool RemoveKey(const std::string& key);
    static std::string BuildMetadataPrefix();

    RemoveKeyFn remove_key_;
    std::string http_metadata_prefix_;

    // Serializes Start/Stop and protects thread_ from concurrent access.
    std::mutex lifecycle_mutex_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<ClientLeaseExpiredEvent> queue_;
};

}  // namespace mooncake
