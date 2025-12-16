#include "task_manager.h"
#include <glog/logging.h>

namespace mooncake {

std::optional<Task> ScopedTaskReadAccess::find_task_by_id(const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
}

tl::expected<UUID, ErrorCode> ScopedTaskWriteAccess::submit_task(const UUID& client_id, TaskType type, const std::string& payload) {
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
    Task task = {
        .id = id,
        .type = type,
        .status = TaskStatus::PENDING,
        .payload = payload,
        .created_at = now,
        .last_updated_at = now,
        .message = "",
        .assigned_client = client_id
    };
    if (manager_->all_tasks_.find(task.id) != manager_->all_tasks_.end()) {
        LOG(WARNING) << "Task " << task.id << " already exists. Overwriting.";
    }
    manager_->total_pending_tasks_++;
    manager_->all_tasks_[task.id] = task;
    manager_->pending_tasks_[client_id].push(task.id);

    return task.id;
}

std::vector<Task> ScopedTaskWriteAccess::pop_tasks(const UUID& client_id, size_t batch_size) {
    std::vector<Task> result;
    
    auto pit = manager_->pending_tasks_.find(client_id);
    if (pit == manager_->pending_tasks_.end()) {
        return result;
    }

    auto& queue = pit->second;
    auto& processing_set = manager_->processing_tasks_[client_id];

    while (!queue.empty() && result.size() < batch_size) {
        if (manager_->total_processing_tasks_ >= manager_->max_total_processing_tasks_) {
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
                         << " is already in processing set for client " << client_id;
        } else {
            manager_->total_processing_tasks_++;
        }

        result.push_back(task);
    }

    return result;
}

ErrorCode ScopedTaskWriteAccess::complete_task(const UUID& client_id, const UUID& task_id, TaskStatus status, const std::string& message) {
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
        LOG(ERROR) << "Client " << client_id << " is not assigned to task " << task_id;
        return ErrorCode::ILLEGAL_CLIENT;
    }

    if (task.is_finished()) {
        LOG(WARNING) << "Task " << task_id << " is already finished with status " << task.status;
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
    while (manager_->finished_task_history_.size() > manager_->max_total_finished_tasks_) {
        UUID oldest_task_id = manager_->finished_task_history_.front();
        manager_->finished_task_history_.pop_front();
        manager_->all_tasks_.erase(oldest_task_id);
    }
}
} // namespace mooncake

