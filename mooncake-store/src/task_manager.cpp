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

tl::expected<UUID, ErrorCode> ScopedTaskWriteAccess::submit_task(
    const UUID& client_id, TaskType type, const std::string& payload) {
    if (manager_->total_pending_tasks_ >= manager_->max_total_pending_tasks_) {
        LOG(ERROR) << "Cannot submit new task: pending task limit reached ("
                   << manager_->total_pending_tasks_ << "/"
                   << manager_->max_total_pending_tasks_ << ")";
        return tl::make_unexpected(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    }
    UUID id = generate_uuid();
    while (manager_->all_tasks_.find(id) != manager_->all_tasks_.end()) {
        id = generate_uuid();
    }
    auto now = std::chrono::system_clock::now();
    Task task = {.id = id,
                 .type = type,
                 .status = TaskStatus::PENDING,
                 .payload = payload,
                 .created_at = now,
                 .last_updated_at = now,
                 .message = "",
                 .assigned_client = client_id};
    manager_->total_pending_tasks_++;
    manager_->all_tasks_[task.id] = task;
    manager_->pending_tasks_[client_id].push(task.id);

    return task.id;
}

std::vector<Task> ScopedTaskWriteAccess::pop_tasks(const UUID& client_id,
                                                   size_t batch_size) {
    std::vector<Task> result;

    auto pit = manager_->pending_tasks_.find(client_id);
    if (pit == manager_->pending_tasks_.end()) {
        return result;
    }

    auto& queue = pit->second;
    auto& processing_set = manager_->processing_tasks_[client_id];

    while (!queue.empty() && result.size() < batch_size) {
        if (manager_->total_processing_tasks_ >=
            manager_->max_total_processing_tasks_) {
            break;
        }

        const UUID task_id = queue.front();
        queue.pop();

        if (manager_->total_pending_tasks_ > 0) {
            manager_->total_pending_tasks_--;
        }

        auto it = manager_->all_tasks_.find(task_id);
        if (it == manager_->all_tasks_.end()) {
            LOG(ERROR) << "Task " << task_id
                       << " not found in all_tasks_ while popping";
            continue;
        }

        Task& task = it->second;
        task.mark_processing();

        const auto [_, inserted] = processing_set.insert(task_id);
        if (!inserted) {
            LOG(WARNING) << "Task " << task_id
                         << " is already in processing set for client "
                         << client_id;
        } else {
            manager_->total_processing_tasks_++;
        }

        result.push_back(task);
    }

    return result;
}

ErrorCode ScopedTaskWriteAccess::complete_task(const UUID& client_id,
                                               const UUID& task_id,
                                               TaskStatus status,
                                               const std::string& message) {
    if (!is_finished_status(status)) {
        LOG(ERROR) << "complete_task: invalid completion status=" << status
                   << ", task_id=" << task_id;
        return ErrorCode::INVALID_PARAMS;
    }

    auto it = manager_->all_tasks_.find(task_id);
    if (it == manager_->all_tasks_.end()) {
        LOG(ERROR) << "Task " << task_id << " not found for update";
        return ErrorCode::TASK_NOT_FOUND;
    }

    Task& task = it->second;

    if (task.assigned_client != client_id) {
        LOG(ERROR) << "Client " << client_id << " is not assigned to task "
                   << task_id;
        return ErrorCode::ILLEGAL_CLIENT;
    }

    if (task.is_finished()) {
        LOG(WARNING) << "Task " << task_id
                     << " is already finished with status " << task.status;
        return ErrorCode::OK;
    }

    task.mark_complete(status, message);

    auto ps_it = manager_->processing_tasks_.find(client_id);
    if (ps_it != manager_->processing_tasks_.end()) {
        auto& processing_set = ps_it->second;
        const size_t erased = processing_set.erase(task_id);
        if (erased == 1 && manager_->total_processing_tasks_ > 0) {
            manager_->total_processing_tasks_--;
        }
    }

    manager_->finished_task_history_.push_back(task_id);

    return ErrorCode::OK;
}

void ScopedTaskWriteAccess::prune_finished_tasks() {
    while (manager_->finished_task_history_.size() >
           manager_->max_total_finished_tasks_) {
        UUID oldest_task_id = manager_->finished_task_history_.front();
        manager_->finished_task_history_.pop_front();
        manager_->all_tasks_.erase(oldest_task_id);
    }
}

void ScopedTaskWriteAccess::prune_expired_tasks() {
    const auto now = std::chrono::system_clock::now();
    // Pending timeout: based on created_at
    if (manager_->pending_task_timeout_sec_ > 0) {
        const auto pending_timeout_duration =
            std::chrono::seconds(manager_->pending_task_timeout_sec_);

        for (auto& [client_id, task_queue] : manager_->pending_tasks_) {
            std::queue<UUID> keep_queue;
            while (!task_queue.empty()) {
                const UUID task_id = task_queue.front();
                task_queue.pop();

                auto it = manager_->all_tasks_.find(task_id);
                if (it == manager_->all_tasks_.end()) {
                    // Drop dangling id;
                    if (manager_->total_pending_tasks_ > 0) {
                        manager_->total_pending_tasks_--;
                    }
                    continue;
                }

                Task& task = it->second;

                // If this task is no longer pending don't keep it in pending
                // queue.
                if (task.status != TaskStatus::PENDING) {
                    continue;
                }

                if (now - task.created_at > pending_timeout_duration) {
                    task.mark_complete(TaskStatus::FAILED, "pending timeout");
                    if (manager_->total_pending_tasks_ > 0) {
                        manager_->total_pending_tasks_--;
                    }
                    manager_->finished_task_history_.push_back(task_id);
                    continue;
                }

                keep_queue.push(task_id);
            }
            task_queue = std::move(keep_queue);
        }
    }

    // Processing timeout: based on last_updated_at
    if (manager_->processing_task_timeout_sec_ > 0) {
        const auto processing_timeout =
            std::chrono::seconds(manager_->processing_task_timeout_sec_);
        for (auto& [client_id, processing_task_set] :
             manager_->processing_tasks_) {
            std::vector<UUID> to_remove;
            for (const UUID& task_id : processing_task_set) {
                auto it = manager_->all_tasks_.find(task_id);
                if (it == manager_->all_tasks_.end()) {
                    // Drop dangling id.
                    to_remove.push_back(task_id);
                    if (manager_->total_processing_tasks_ > 0) {
                        manager_->total_processing_tasks_--;
                    }
                    continue;
                }

                Task& task = it->second;

                // If task is no longer processing remove it from processing
                // set.
                if (task.status != TaskStatus::PROCESSING) {
                    to_remove.push_back(task_id);
                    if (manager_->total_processing_tasks_ > 0) {
                        manager_->total_processing_tasks_--;
                    }
                    continue;
                }

                if (now - task.last_updated_at > processing_timeout) {
                    task.mark_complete(TaskStatus::FAILED,
                                       "processing timeout");
                    to_remove.push_back(task_id);
                    if (manager_->total_processing_tasks_ > 0) {
                        manager_->total_processing_tasks_--;
                    }
                    manager_->finished_task_history_.push_back(task_id);
                }
            }
            for (const UUID& task_id : to_remove) {
                processing_task_set.erase(task_id);
            }
        }
    }
}
}  // namespace mooncake
