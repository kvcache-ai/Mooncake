#pragma once

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <optional>
#include <string>
#include <thread>
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

class LMCacheNotifier {
   public:
    // Event types for notifications
    enum class NotificationEventType { ADMIT, EVICT };

    /**
     * @brief Construct a new LMCache Notifier
     * @param controller_url The URL of the LMCache controller
     */
    explicit LMCacheNotifier(std::string controller_url);

    /**
     * @brief Destructor - stops the worker thread and cleans up pending tasks
     */
    ~LMCacheNotifier();

    // Prevent copying and assignment
    LMCacheNotifier(const LMCacheNotifier&) = delete;
    LMCacheNotifier& operator=(const LMCacheNotifier&) = delete;

    /**
     * @brief Enqueue a notification task
     * @param event_type Type of event (ADMIT or EVICT)
     * @param key Object key
     * @param location_str Location string (e.g., "mooncake_cpu_ram")
     * @param instance_id Optional LMCache instance ID
     * @param worker_id Optional LMCache worker ID
     */
    void EnqueueTask(
        NotificationEventType event_type, const std::string& key,
        const std::string& location_str,
        const std::optional<std::string>& instance_id = std::nullopt,
        const std::optional<int>& worker_id = std::nullopt);

   private:
    // Structure to hold notification task details
    struct NotificationTask {
        NotificationEventType type;
        std::string key;
        std::string location_str;
        std::optional<std::string> instance_id;
        std::optional<int> worker_id;
    };

    // Constants
    static constexpr size_t kQueueSize =
        10 * 1024;  // Size of the notification queue
    static constexpr uint64_t kWorkerThreadSleepMs =
        10;  // 10 ms sleep between notification checks

    // Main worker thread function
    void WorkerLoop();

    // Internal notification sending function
    void SendNotificationInternal(cinatra::coro_http_client& client,
                                  NotificationTask* task) const;

    // Member variables
    const std::string lmcache_controller_url_;
    boost::lockfree::queue<NotificationTask*> notification_queue_{kQueueSize};
    std::thread worker_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace mooncake
