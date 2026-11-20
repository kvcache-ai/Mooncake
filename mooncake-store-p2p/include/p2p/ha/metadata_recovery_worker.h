#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include <boost/functional/hash.hpp>

#include "p2p/client/async_metadata_notifier.h"
#include "p2p/client/data_manager.h"

namespace mooncake {

// Replays local replica metadata. Service owns registration, service state and
// notifier lifetime; this worker owns only one cancellable recovery task.
class MetadataRecoveryWorker {
   public:
    enum class RecoveryMode { FullMetadataSync, RegisterOnly };
    enum class Status {
        IDLE,
        RUNNING,
        COMPLETED,
        CANCELLED,
        TIMED_OUT,
        FAILED
    };
    using PublishReplica = std::function<tl::expected<void, ErrorCode>(
        std::string_view, const UUID&, size_t)>;

    // Dependencies must outlive the worker. publish_replica is the existing
    // synchronous publication path, used only when notifier is disabled.
    MetadataRecoveryWorker(
        DataManager& data_manager, AsyncMetadataNotifier* notifier,
        PublishReplica publish_replica,
        RecoveryMode recovery_mode = RecoveryMode::FullMetadataSync);
    ~MetadataRecoveryWorker();

    MetadataRecoveryWorker(const MetadataRecoveryWorker&) = delete;
    MetadataRecoveryWorker& operator=(const MetadataRecoveryWorker&) = delete;

    ErrorCode Start();
    void Stop();
    Status GetStatus() const { return status_.load(std::memory_order_acquire); }

   private:
    using AbortToken = std::shared_ptr<std::atomic<bool>>;
    void StopLocked();
    Status RecoveryPipelineMain(AbortToken need_abort);
    bool EnqueueWithRetry(const std::string& key, const UUID& tier_id,
                          size_t size, bool is_hot,
                          const AbortToken& need_abort);

    DataManager& data_manager_;
    AsyncMetadataNotifier* notifier_;
    PublishReplica publish_replica_;
    const RecoveryMode recovery_mode_;
    std::atomic<Status> status_{Status::IDLE};
    std::mutex mutex_;
    std::mutex retry_mutex_;
    std::condition_variable retry_cv_;
    std::thread recovery_thread_;
    AbortToken need_abort_;
};

}  // namespace mooncake
