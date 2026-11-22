#include "task_manager.h"
#include <algorithm>
#include <glog/logging.h>
#include <queue>

namespace mooncake {
namespace {

bool MatchesTaskPayload(const Task& task, const TenantId& tenant,
                        const std::optional<std::string>& key = std::nullopt) {
    try {
        // Validate a complete object before using the typed reader's defaults.
        if (task.payload.empty()) {
            return false;
        }
        struct_json::jvalue value;
        auto it = task.payload.begin();
        auto end = task.payload.end();
        struct_json::parse<true>(value, it, end, true);
        if (it != end || !std::holds_alternative<struct_json::jobject>(value)) {
            return false;
        }
        const auto matches = [&](const auto& payload) {
            const TenantId payload_tenant(payload.tenant_id);
            return payload_tenant.IsValid() && payload_tenant == tenant &&
                   (!key || payload.key == *key);
        };
        switch (task.type) {
            case TaskType::REPLICA_COPY: {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, task.payload);
                return matches(payload);
            }
            case TaskType::REPLICA_MOVE: {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, task.payload);
                return matches(payload);
            }
        }
    } catch (const std::exception&) {
        // A malformed payload cannot be attributed to a tenant.
    }
    return false;
}

}  // namespace

std::optional<Task> ScopedTaskReadAccess::find_task_by_id(
    const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
}

tl::expected<bool, ErrorCode> ScopedTaskReadAccess::is_cancellation_requested(
    const UUID& client_id, const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it == manager_->all_tasks_.end()) {
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    if (it->second.assigned_client != client_id) {
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }
    return it->second.cancellation_requested;
}

tl::expected<TaskListPage, ErrorCode> ScopedTaskReadAccess::list_tasks(
    const TaskListRequest& request) const {
    const TenantId tenant(request.tenant_id);
    if (!tenant.IsValid() || request.limit == 0 || request.limit > 1000 ||
        (request.status && *request.status != TaskStatus::PENDING &&
         *request.status != TaskStatus::PROCESSING &&
         !is_finished_status(*request.status)) ||
        (request.type && *request.type != TaskType::REPLICA_COPY &&
         *request.type != TaskType::REPLICA_MOVE)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::optional<UUID> after;
    constexpr std::string_view prefix = "v1:";
    if (!request.page_token.empty()) {
        UUID id;
        const auto token = std::string_view(request.page_token);
        if (!token.starts_with(prefix) ||
            !StringToUuid(std::string(token.substr(prefix.size())), id) ||
            token.substr(prefix.size()) != UuidToString(id)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        after = id;
    }
    const auto less = [](const Task* lhs, const Task* rhs) {
        return lhs->id < rhs->id;
    };
    std::priority_queue<const Task*, std::vector<const Task*>, decltype(less)>
        selected(less);
    for (const auto& [id, task] : manager_->all_tasks_) {
        if ((after && id <= *after) ||
            (request.status && task.status != *request.status) ||
            (request.type && task.type != *request.type) ||
            (request.assigned_client &&
             task.assigned_client != *request.assigned_client)) {
            continue;
        }
        if (MatchesTaskPayload(task, tenant, request.key)) {
            selected.push(&task);
            if (selected.size() > static_cast<size_t>(request.limit) + 1) {
                selected.pop();
            }
        }
    }
    const bool has_more = selected.size() > request.limit;
    if (has_more) {
        selected.pop();
    }
    std::vector<const Task*> ordered;
    ordered.reserve(selected.size());
    while (!selected.empty()) {
        ordered.push_back(selected.top());
        selected.pop();
    }
    std::reverse(ordered.begin(), ordered.end());
    TaskListPage page;
    page.tasks.reserve(ordered.size());
    for (const Task* task : ordered) {
        page.tasks.push_back(*task);
    }
    if (has_more) {
        page.next_page_token =
            std::string(prefix) + UuidToString(ordered.back()->id);
    }
    return page;
}

ScopedTaskReadAccess::task_iterator ScopedTaskReadAccess::begin() const {
    return manager_->all_tasks_.cbegin();
}

ScopedTaskReadAccess::task_iterator ScopedTaskReadAccess::end() const {
    return manager_->all_tasks_.cend();
}

size_t ScopedTaskReadAccess::size() const {
    return manager_->all_tasks_.size();
}

tl::expected<UUID, ErrorCode> ScopedTaskWriteAccess::submit_task(
    const UUID& client_id, TaskType type, const std::string& payload,
    bool retained_by_drain) {
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
                 .assigned_client = client_id,
                 .max_retry_attempts = manager_->max_retry_attempts_,
                 .retained_by_drain = retained_by_drain};
    manager_->total_pending_tasks_++;
    manager_->all_tasks_[task.id] = task;
    manager_->pending_tasks_[client_id].push_back(task.id);

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
        queue.pop_front();

        auto it = manager_->all_tasks_.find(task_id);
        if (it == manager_->all_tasks_.end()) {
            // Terminal tasks can be pruned before their queued IDs are
            // consumed.
            VLOG(1) << "Skipping stale pending queue entry for task "
                    << task_id;
            continue;
        }

        Task& task = it->second;
        if (task.status != TaskStatus::PENDING) {
            continue;
        }
        if (manager_->total_pending_tasks_ > 0) {
            manager_->total_pending_tasks_--;
        }
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

tl::expected<ScopedTaskWriteAccess::ReplayBatch, ErrorCode>
ScopedTaskWriteAccess::fetch_tasks_replay(const ClientSessionSharedPtr& session,
                                          const UUID& token, uint64_t sequence,
                                          size_t batch_size, bool closing) {
    const auto& client_id = session->ClientId();
    auto& states = manager_->task_fetch_replay_;
    auto entry = states.find(client_id);
    if (sequence == 0) {
        if (token != UUID{} || batch_size != 0 || closing) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (entry == states.end() || entry->second.session != session) {
            entry =
                states
                    .insert_or_assign(client_id,
                                      ClientTaskManager::TaskFetchReplayState{
                                          session, generate_uuid(), 0, 0, {}})
                    .first;
        }
        const auto& state = entry->second;
        return ReplayBatch{state.token, state.sequence, {}};
    }

    if (entry == states.end() || entry->second.session != session ||
        entry->second.token != token) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // Match the legacy client's maximum batch while bounding replay state
    // independently of the caller's local credit validation.
    constexpr size_t kMaxReplayBatchSize = 16;
    if (batch_size == 0 || batch_size > kMaxReplayBatchSize) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& state = entry->second;
    if (state.closed && (!closing || sequence != state.sequence)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (sequence == state.sequence) {
        if (batch_size != state.batch_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        state.closed = closing;
        ReplayBatch batch{token, sequence, {}};
        batch.tasks.reserve(state.task_ids.size());
        for (const auto& id : state.task_ids) {
            const auto task = manager_->all_tasks_.find(id);
            if (task != manager_->all_tasks_.end() &&
                task->second.status == TaskStatus::PROCESSING) {
                batch.tasks.push_back(task->second);
            }
        }
        return batch;
    }
    if (sequence < state.sequence || sequence - state.sequence != 1) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (closing) {
        state.sequence = sequence;
        state.batch_size = batch_size;
        state.task_ids.clear();
        state.closed = true;
        return ReplayBatch{token, sequence, {}};
    }
    if (state.sequence == 0) {
        const auto processing = manager_->processing_tasks_.find(client_id);
        if (processing != manager_->processing_tasks_.end() &&
            !processing->second.empty()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    // Install the grant under the same lock that charges processing quota.
    // Reserve the bookkeeping before claiming tasks.
    std::vector<UUID> task_ids;
    task_ids.reserve(batch_size);
    auto tasks = pop_tasks(client_id, batch_size);
    for (const auto& task : tasks) {
        task_ids.push_back(task.id);
    }
    state.task_ids = std::move(task_ids);
    state.sequence = sequence;
    state.batch_size = batch_size;
    return ReplayBatch{token, sequence, std::move(tasks)};
}

bool ScopedTaskWriteAccess::task_fetch_replay_active(
    const ClientSessionSharedPtr& session) const {
    const auto entry = manager_->task_fetch_replay_.find(session->ClientId());
    return entry != manager_->task_fetch_replay_.end() &&
           entry->second.session == session && entry->second.sequence != 0;
}

void ScopedTaskWriteAccess::retire_task_fetch_replay(
    const ClientSessionSharedPtr& session) {
    auto& states = manager_->task_fetch_replay_;
    const auto entry = states.find(session->ClientId());
    if (entry != states.end() && entry->second.session == session) {
        states.erase(entry);
    }
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

    if (task.status == TaskStatus::PENDING &&
        manager_->total_pending_tasks_ > 0) {
        manager_->total_pending_tasks_--;
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
    size_t examined = 0;
    const size_t initial_size = manager_->finished_task_history_.size();
    while (manager_->finished_task_history_.size() >
               manager_->max_total_finished_tasks_ &&
           examined++ < initial_size) {
        UUID oldest_task_id = manager_->finished_task_history_.front();
        manager_->finished_task_history_.pop_front();
        auto it = manager_->all_tasks_.find(oldest_task_id);
        if (it != manager_->all_tasks_.end() && it->second.retained_by_drain) {
            manager_->finished_task_history_.push_back(oldest_task_id);
            continue;
        }
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
            std::deque<UUID> keep_queue;
            while (!task_queue.empty()) {
                const UUID task_id = task_queue.front();
                task_queue.pop_front();

                auto it = manager_->all_tasks_.find(task_id);
                if (it == manager_->all_tasks_.end()) {
                    // The terminal transition already released the pending
                    // slot.
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

                keep_queue.push_back(task_id);
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
                    if (task.retained_by_drain) {
                        // A timeout cannot prove the worker stopped. Ask it
                        // to cancel and wait for acknowledgment before retry.
                        task.cancellation_requested = true;
                        continue;
                    }
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

ErrorCode ScopedTaskWriteAccess::cancel_task(const UUID& task_id,
                                             const TenantId& tenant_id) {
    auto it = manager_->all_tasks_.find(task_id);
    if (it == manager_->all_tasks_.end() ||
        !MatchesTaskPayload(it->second, tenant_id)) {
        return ErrorCode::TASK_NOT_FOUND;
    }

    Task& task = it->second;
    switch (task.status) {
        case TaskStatus::PENDING: {
            auto queue = manager_->pending_tasks_.find(task.assigned_client);
            if (queue != manager_->pending_tasks_.end()) {
                std::erase(queue->second, task_id);
            }
            return complete_task(task.assigned_client, task_id,
                                 TaskStatus::CANCELED, "Task canceled");
        }
        case TaskStatus::PROCESSING:
            task.cancellation_requested = true;
            return ErrorCode::OK;
        case TaskStatus::CANCELED:
            return ErrorCode::OK;
        default:
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
}

void ScopedTaskWriteAccess::release_drain_task(const UUID& task_id) {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        it->second.retained_by_drain = false;
    }
}

ErrorCode ScopedTaskWriteAccess::fail_task_if_pending(
    const UUID& task_id, const std::string& message) {
    auto it = manager_->all_tasks_.find(task_id);
    if (it == manager_->all_tasks_.end()) {
        return ErrorCode::TASK_NOT_FOUND;
    }

    Task& task = it->second;
    if (task.status != TaskStatus::PENDING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    task.mark_complete(TaskStatus::FAILED, message);
    if (manager_->total_pending_tasks_ > 0) {
        manager_->total_pending_tasks_--;
    }
    manager_->finished_task_history_.push_back(task_id);
    return ErrorCode::OK;
}

}  // namespace mooncake
