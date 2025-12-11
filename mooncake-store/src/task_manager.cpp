#include "task_manager.h"
#include <glog/logging.h>

namespace mooncake {

std::optional<Task> ScopedTaskReadAccess::find_task_by_id(
    const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
}

UUID ScopedTaskWriteAccess::submit_task(const UUID& client_id, TaskType type,
                                        const std::string& payload) {
    auto now = std::chrono::steady_clock::now();
    Task task = {.id = generate_uuid(),
                 .type = type,
                 .status = TaskStatus::PENDING,
                 .payload = payload,
                 .created_at = now,
                 .last_updated_at = now,
                 .error_message = "",
                 .assigned_client = client_id};
    if (manager_->all_tasks_.find(task.id) != manager_->all_tasks_.end()) {
        LOG(WARNING) << "Task " << task.id << " already exists. Overwriting.";
    }
    manager_->all_tasks_[task.id] = task;
    manager_->pending_tasks_[client_id].push(task.id);
    return task.id;
}

std::vector<Task> ScopedTaskWriteAccess::pop_tasks(const UUID& client_id,
                                                   size_t batch_size) {
    std::vector<Task> result;

    if (manager_->pending_tasks_.find(client_id) ==
        manager_->pending_tasks_.end()) {
        return result;
    }

    auto& queue = manager_->pending_tasks_[client_id];
    auto& processing_set = manager_->processing_tasks_[client_id];

    while (!queue.empty() && result.size() < batch_size) {
        UUID task_id = queue.front();
        queue.pop();

        auto it = manager_->all_tasks_.find(task_id);
        if (it != manager_->all_tasks_.end()) {
            it->second.status = TaskStatus::PROCESSING;
            processing_set.insert(task_id);
            result.push_back(it->second);
        } else {
            LOG(ERROR) << "Task " << task_id
                       << " found in queue but not in all_tasks_";
        }
    }

    LOG(INFO) << "Popped " << result.size() << " tasks for client "
              << client_id;

    return result;
}

std::optional<Task> ScopedTaskWriteAccess::find_task_by_id(
    const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
}

void ScopedTaskWriteAccess::mark_failed(const UUID& client_id,
                                        const UUID& task_id,
                                        const std::string& error_message) {
    update_task_status(task_id, TaskStatus::FAILED, error_message);
    // Remove from processing set
    manager_->processing_tasks_[client_id].erase(task_id);
    // Add to finished task history for pruning
    manager_->finished_task_history_.push_back(task_id);
    prune_finished_tasks();
}

void ScopedTaskWriteAccess::mark_success(const UUID& client_id,
                                         const UUID& task_id) {
    update_task_status(task_id, TaskStatus::SUCCESS);
    // Remove from processing set
    manager_->processing_tasks_[client_id].erase(task_id);
    // Add to finished task history for pruning
    manager_->finished_task_history_.push_back(task_id);
    prune_finished_tasks();
}

void ScopedTaskWriteAccess::update_task_status(
    const UUID& task_id, TaskStatus status, const std::string& error_message) {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        it->second.status = status;
        it->second.error_message = error_message;
    } else {
        LOG(ERROR) << "Task " << task_id << " not found in all_tasks_";
    }
}

void ScopedTaskWriteAccess::prune_finished_tasks() {
    while (manager_->finished_task_history_.size() >
           manager_->max_finished_tasks_) {
        UUID oldest_task_id = manager_->finished_task_history_.front();
        manager_->finished_task_history_.pop_front();
        manager_->all_tasks_.erase(oldest_task_id);
    }
}
}  // namespace mooncake
