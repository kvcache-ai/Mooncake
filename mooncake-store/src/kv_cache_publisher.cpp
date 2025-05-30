#include "kv_cache_publisher.h"

#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include <iguana/json_writer.hpp>
#include <utility>
#include <ylt/coro_http/coro_http_client.hpp>

#include "kv_cache_event.h"

namespace mooncake {

LMCacheNotifier::LMCacheNotifier(std::string controller_url)
    : lmcache_controller_url_(std::move(controller_url)) {
    running_ = true;
    worker_thread_ = std::thread(&LMCacheNotifier::WorkerLoop, this);
}

LMCacheNotifier::~LMCacheNotifier() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    // Clean up any remaining notification tasks
    NotificationTask* task = nullptr;
    while (notification_queue_.pop(task)) {
        delete task;
    }
    VLOG(1) << "action=notification_thread_stopped";
}

void LMCacheNotifier::EnqueueTask(NotificationEventType event_type,
                                  const std::string& key,
                                  const std::string& location_str,
                                  const std::optional<std::string>& instance_id,
                                  const std::optional<int>& worker_id) {
    // Create a new notification task
    auto* task = new NotificationTask();
    task->type = event_type;
    task->key = key;
    task->location_str = location_str;
    task->instance_id = instance_id;
    task->worker_id = worker_id;

    // Push the task to the queue
    if (!notification_queue_.push(task)) {
        // Queue is full, delete the task to avoid memory leak
        delete task;
        LOG(ERROR) << "key=" << key << ", error=notification_queue_full";
    } else {
        VLOG(1) << "key=" << key << ", event_type="
                << (event_type == NotificationEventType::ADMIT ? "ADMIT"
                                                               : "EVICT")
                << ", action=notification_task_enqueued";
    }
}

void LMCacheNotifier::WorkerLoop() {
    VLOG(1) << "action=notification_thread_started";
    cinatra::coro_http_client client{};
    async_simple::coro::syncAwait(client.connect(lmcache_controller_url_));

    while (running_) {
        NotificationTask* task = nullptr;
        bool has_task = notification_queue_.pop(task);

        if (has_task && task) {
            // Send notification
            SendNotificationInternal(client, task);

            // Clean up task
            delete task;
        }

        // Sleep for a short time to avoid busy waiting
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kWorkerThreadSleepMs));
    }

    VLOG(1) << "action=notification_thread_stopped";
}

void LMCacheNotifier::SendNotificationInternal(
    cinatra::coro_http_client& client, NotificationTask* task) const {
    // if (!task->instance_id || !task->worker_id) {
    //     LOG(WARNING) << "action=lmcache_notification_skipped, "
    //                     "error=missing_instance_id_or_worker_id";
    //     return;
    // }

    // Create the appropriate message based on event type
    std::string json_str;
    if (task->type == NotificationEventType::ADMIT) {
        mooncake::KVAdmitMsg msg;
        msg.key = task->key;
        msg.location = task->location_str;
        msg.instance_id = (task->instance_id ? task->instance_id.value() : "");
        msg.worker_id = (task->worker_id ? task->worker_id.value() : 0);

        struct_json::to_json(msg, json_str);

    } else if (task->type == NotificationEventType::EVICT) {
        mooncake::KVEvictMsg msg;
        msg.key = task->key;
        msg.location = task->location_str;
        msg.instance_id = (task->instance_id ? task->instance_id.value() : "");
        msg.worker_id = (task->worker_id ? task->worker_id.value() : 0);

        struct_json::to_json(msg, json_str);
    } else {
        LOG(ERROR) << "action=lmcache_notification_skipped, "
                      "error=unknown_event_type, type="
                   << static_cast<int>(task->type);
    }

    auto resp = async_simple::coro::syncAwait(client.async_post(
        lmcache_controller_url_, json_str, cinatra::req_content_type::text));
    VLOG(1) << "action=lmcache_notification_sent, "
               "url="
            << lmcache_controller_url_ << ", status=" << resp.status;
}

}  // namespace mooncake
