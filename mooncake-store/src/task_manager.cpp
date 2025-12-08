#include "task_manager.h"
#include <glog/logging.h>

namespace mooncake {

UUID ClientTaskManager::submit_task(const std::string& localhost_name,
                                    TaskType type, const std::string& payload) {
    MutexLocker lock(&mutex_);
    auto now = std::chrono::steady_clock::now();
    Task task = {.id = generate_uuid(),
                 .type = type,
                 .status = TaskStatus::PENDING,
                 .payload = payload,
                 .created_at = now,
                 .last_updated_at = now,
                 .error_message = "",
                 .assigned_client = localhost_name};
    if (all_tasks_.find(task.id) != all_tasks_.end()) {
        LOG(WARNING) << "Task " << task.id << " already exists. Overwriting.";
    }
    all_tasks_[task.id] = task;
    pending_tasks_[localhost_name].push(task.id);
    return task.id;
}

std::vector<Task> ClientTaskManager::pop_tasks(
    const std::string& localhost_name, size_t batch_size) {
    MutexLocker lock(&mutex_);
    std::vector<Task> result;

    if (pending_tasks_.find(localhost_name) == pending_tasks_.end()) {
        return result;
    }

    auto& queue = pending_tasks_[localhost_name];
    auto& processing_set = processing_tasks_[localhost_name];

    while (!queue.empty() && result.size() < batch_size) {
        UUID task_id = queue.front();
        queue.pop();

        auto it = all_tasks_.find(task_id);
        if (it != all_tasks_.end()) {
            it->second.status = TaskStatus::PROCESSING;
            processing_set.insert(task_id);
            result.push_back(it->second);
        } else {
            LOG(ERROR) << "Task " << task_id
                       << " found in queue but not in all_tasks_";
        }
    }

    LOG(INFO) << "Popped " << result.size() << " tasks for client "
              << localhost_name;

    return result;
}

std::optional<Task> ClientTaskManager::find_task_by_id(
    const UUID& task_id) const {
    MutexLocker lock(&mutex_);
    auto it = all_tasks_.find(task_id);
    if (it != all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
}

void ClientTaskManager::mark_failed(const std::string& localhost_name,
                                    const UUID& task_id,
                                    const std::string& error_message) {
    MutexLocker lock(&mutex_);
    update_task_status(task_id, TaskStatus::FAILED, error_message);
    // Remove from processing set
    processing_tasks_[localhost_name].erase(task_id);
    // Add to finished task history for pruning
    finished_task_history_.push_back(task_id);
    prune_finished_tasks();
}

void ClientTaskManager::mark_success(const std::string& localhost_name,
                                     const UUID& task_id) {
    MutexLocker lock(&mutex_);
    update_task_status(task_id, TaskStatus::SUCCESS);
    // Remove from processing set
    processing_tasks_[localhost_name].erase(task_id);
    // Add to finished task history for pruning
    finished_task_history_.push_back(task_id);
    prune_finished_tasks();
}

void ClientTaskManager::update_task_status(const UUID& task_id,
                                           TaskStatus status,
                                           const std::string& error_message) {
    auto it = all_tasks_.find(task_id);
    if (it != all_tasks_.end()) {
        it->second.status = status;
        it->second.error_message = error_message;
    } else {
        LOG(ERROR) << "Task " << task_id << " not found in all_tasks_";
    }
}

void ClientTaskManager::prune_finished_tasks() {
    while (finished_task_history_.size() > max_finished_tasks_) {
        UUID oldest_task_id = finished_task_history_.front();
        finished_task_history_.pop_front();
        all_tasks_.erase(oldest_task_id);
    }
}
}  // namespace mooncake
