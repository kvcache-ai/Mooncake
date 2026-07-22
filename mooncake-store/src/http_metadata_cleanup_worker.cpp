#include "http_metadata_cleanup_worker.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <utility>

#include <glog/logging.h>

#include "http_metadata_server.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {

HttpMetadataCleanupWorker::CreateResult HttpMetadataCleanupWorker::Create(
    HttpMetadataServer* http_metadata_server,
    const std::string& http_metadata_remote_url) {
    RemoveKeyFn remove_key;
    if (http_metadata_server) {
        remove_key = [http_metadata_server](const std::string& key) {
            return http_metadata_server->removeKey(key);
        };
        LOG(INFO) << "HTTP metadata cleanup on client lease expiry: enabled "
                     "(co-located metadata server)";
        return std::make_unique<HttpMetadataCleanupWorker>(
            std::move(remove_key), BuildMetadataPrefix());
    }

    if (http_metadata_remote_url.empty()) {
        return tl::make_unexpected(std::string(
            "no local or remote HTTP metadata backend was configured"));
    }

    // MetadataStoragePlugin::Create() terminates for unsupported backends, so
    // reject non-HTTP connection strings before calling it.
    if (http_metadata_remote_url.rfind("http://", 0) != 0 &&
        http_metadata_remote_url.rfind("https://", 0) != 0) {
        return tl::make_unexpected("configured metadata server '" +
                                   http_metadata_remote_url +
                                   "' is not an HTTP endpoint");
    }

#ifdef USE_HTTP
    try {
        auto http_metadata_remote =
            MetadataStoragePlugin::Create(http_metadata_remote_url);
        if (!http_metadata_remote) {
            return tl::make_unexpected(
                "metadata plugin factory returned null for '" +
                http_metadata_remote_url + "'");
        }
        remove_key =
            [remote = std::move(http_metadata_remote)](const std::string& key) {
                return remote->remove(key);
            };
        LOG(INFO) << "HTTP metadata cleanup on client lease expiry: enabled "
                     "(remote metadata server "
                  << http_metadata_remote_url << ")";
        return std::make_unique<HttpMetadataCleanupWorker>(
            std::move(remove_key), BuildMetadataPrefix());
    } catch (const std::exception& e) {
        return tl::make_unexpected(
            "failed to initialize remote HTTP metadata client for '" +
            http_metadata_remote_url + "': " + e.what());
    }
#else
    return tl::make_unexpected(
        std::string("remote HTTP metadata cleanup requires USE_HTTP=ON"));
#endif
}

HttpMetadataCleanupWorker::HttpMetadataCleanupWorker(
    RemoveKeyFn remove_key, std::string metadata_prefix)
    : remove_key_(std::move(remove_key)),
      http_metadata_prefix_(std::move(metadata_prefix)) {}

HttpMetadataCleanupWorker::~HttpMetadataCleanupWorker() { Stop(); }

bool HttpMetadataCleanupWorker::Start() {
    if (!remove_key_) {
        return false;
    }

    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (running_) {
        return true;
    }
    running_ = true;
    try {
        thread_ = std::thread(&HttpMetadataCleanupWorker::ThreadFunc, this);
    } catch (...) {
        std::lock_guard<std::mutex> queue_lock(mutex_);
        running_ = false;
        queue_.clear();
        throw;
    }
    return true;
}

void HttpMetadataCleanupWorker::Stop() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> queue_lock(mutex_);
        if (!running_) {
            return;
        }
        running_ = false;
        queue_.clear();
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void HttpMetadataCleanupWorker::Enqueue(const ClientLeaseExpiredEvent& event) {
    if (event.unmounted_memory_segment_names.empty()) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        queue_.push_back(event);
    }
    cv_.notify_one();
}

void HttpMetadataCleanupWorker::ThreadFunc() {
    LOG(INFO) << "HTTP metadata cleanup worker started";
    while (true) {
        std::vector<ClientLeaseExpiredEvent> batch;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return !queue_.empty() || !running_.load(); });
            if (!running_) {
                break;
            }
            batch.swap(queue_);
        }

        for (const auto& event : batch) {
            for (const auto& segment_name :
                 event.unmounted_memory_segment_names) {
                const std::string ram_key =
                    http_metadata_prefix_ + "ram/" + segment_name;
                const std::string rpc_key =
                    http_metadata_prefix_ + "rpc_meta/" + segment_name;

                bool ram_removed = false;
                bool rpc_removed = false;
                try {
                    ram_removed = RemoveKey(ram_key);
                } catch (const std::exception& e) {
                    LOG(WARNING) << "HTTP metadata cleanup failed for key "
                                 << ram_key << ": " << e.what();
                } catch (...) {
                    LOG(WARNING) << "HTTP metadata cleanup failed for key "
                                 << ram_key << ": unknown exception";
                }
                try {
                    rpc_removed = RemoveKey(rpc_key);
                } catch (const std::exception& e) {
                    LOG(WARNING) << "HTTP metadata cleanup failed for key "
                                 << rpc_key << ": " << e.what();
                } catch (...) {
                    LOG(WARNING) << "HTTP metadata cleanup failed for key "
                                 << rpc_key << ": unknown exception";
                }
                LOG(INFO) << "Cleaned up HTTP metadata for client_id="
                          << event.client_id
                          << ", segment_name=" << segment_name
                          << ", ram_key_removed=" << ram_removed
                          << ", rpc_key_removed=" << rpc_removed;
            }
        }
    }
    LOG(INFO) << "HTTP metadata cleanup worker stopped";
}

bool HttpMetadataCleanupWorker::RemoveKey(const std::string& key) {
    return remove_key_(key);
}

std::string HttpMetadataCleanupWorker::BuildMetadataPrefix() {
    const char* custom_prefix = std::getenv("MC_METADATA_CLUSTER_ID");
    if (!custom_prefix || std::strlen(custom_prefix) == 0) {
        return "mooncake/";
    }

    std::string prefix = "mooncake/" + std::string(custom_prefix);
    if (prefix.back() != '/') {
        prefix += '/';
    }
    return prefix;
}

}  // namespace mooncake
