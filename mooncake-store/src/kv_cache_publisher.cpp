#include "kv_cache_publisher.h"

#include <glog/logging.h>
#include <zmq.h>

#include <iguana/json_writer.hpp>
#include <utility>

#include "kv_cache_event.h"

namespace mooncake {

LMCacheNotifier::LMCacheNotifier(std::string controller_zmq_address)
    : controller_address_(std::move(controller_zmq_address)),
      zmq_context_(1),
      zmq_socket_(zmq_context_, zmq::socket_type::push) {
    running_ = true;
    zmq_socket_.connect(controller_address_);
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
                                  const std::optional<std::string>& worker_id) {
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
    LOG(INFO) << "action=notification_thread_started";
    LOG(INFO)
        << "LMCacheNotifier::WorkerLoop started, PUSH socket connected to "
        << controller_address_;
    while (running_) {
        NotificationTask* task = nullptr;
        bool has_task = notification_queue_.pop(task);

        if (has_task && task) {
            SendNotificationInternal(task);

            // Clean up task
            delete task;
        }

        // Sleep for a short time to avoid busy waiting
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kWorkerThreadSleepMs));
    }

    LOG(INFO) << "action=notification_thread_stopped";
}

void LMCacheNotifier::SendNotificationInternal(NotificationTask* task) {
    // Create the appropriate message based on event type
    std::string json_str;
    if (task->type == NotificationEventType::ADMIT) {
        mooncake::KVAdmitMsg msg;
        msg.key = task->key;
        msg.location = task->location_str;
        msg.instance_id = (task->instance_id ? task->instance_id.value() : "");
        msg.worker_id =
            (task->worker_id ? std::stoi(task->worker_id.value()) : 0);

        struct_json::to_json(msg, json_str);

    } else if (task->type == NotificationEventType::EVICT) {
        mooncake::KVEvictMsg msg;
        msg.key = task->key;
        msg.location = task->location_str;
        msg.instance_id = (task->instance_id ? task->instance_id.value() : "");
        msg.worker_id =
            (task->worker_id ? std::stoi(task->worker_id.value()) : 0);

        struct_json::to_json(msg, json_str);
    } else {
        LOG(ERROR) << "action=lmcache_notification_skipped, "
                      "error=unknown_event_type, type="
                   << static_cast<int>(task->type);
    }

    if (json_str.empty()) {
        return;
    }

    zmq::message_t zmq_msg(json_str.begin(), json_str.end());
    try {
        auto res = zmq_socket_.send(zmq_msg, zmq::send_flags::none);
        if (res.has_value()) {
            VLOG(1) << "action=lmcache_notification_sent, address="
                    << controller_address_ << ", bytes_sent=" << res.value();
        } else {
            LOG(WARNING)
                << "action=lmcache_notification_failed_to_send, address="
                << controller_address_;
        }
    } catch (const zmq::error_t& e) {
        LOG(WARNING) << "action=lmcache_notification_zmq_error, address="
                     << controller_address_ << ", error=" << e.what()
                     << ", errno=" << e.num();
    }
}
}  // namespace mooncake
