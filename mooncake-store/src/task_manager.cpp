#include "task_manager.h"
#include <glog/logging.h>
#include <msgpack.hpp>
#include "utils/zstd_util.h"

namespace mooncake {

std::optional<Task> ScopedTaskReadAccess::find_task_by_id(
    const UUID& task_id) const {
    auto it = manager_->all_tasks_.find(task_id);
    if (it != manager_->all_tasks_.end()) {
        return it->second;
    }
    return std::nullopt;
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

void ScopedTaskWriteAccess::restore_task(Task&& task) {
    const UUID task_id = task.id;
    manager_->all_tasks_[task_id] = std::move(task);
    Task& stored_task = manager_->all_tasks_[task_id];

    const UUID client_id = stored_task.assigned_client;
    const TaskStatus status = stored_task.status;

    switch (status) {
        case TaskStatus::PENDING: {
            manager_->pending_tasks_[client_id].push(task_id);
            manager_->total_pending_tasks_++;
            break;
        }
        case TaskStatus::PROCESSING: {
            manager_->processing_tasks_[client_id].insert(task_id);
            manager_->total_processing_tasks_++;
            break;
        }
        case TaskStatus::FAILED:
        case TaskStatus::SUCCESS: {
            manager_->finished_task_history_.push_back(task_id);
            break;
        }
    }
}

void ScopedTaskWriteAccess::clear_all() {
    manager_->all_tasks_.clear();
    manager_->pending_tasks_.clear();
    manager_->processing_tasks_.clear();
    manager_->finished_task_history_.clear();
    manager_->total_pending_tasks_ = 0;
    manager_->total_processing_tasks_ = 0;
}

tl::expected<std::vector<uint8_t>, SerializationError>
TaskManagerSerializer::Serialize() {
    if (!task_manager_) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::SERIALIZE_FAIL,
                               "serialize TaskManager task_manager_ is null"));
    }

    auto read_access = task_manager_->get_read_access();

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    packer.pack_array(read_access.size());

    for (const auto& [_, task] : read_access) {
        // Serialize Task as array: [id, type, status, payload, created_at,
        // last_updated_at, message, assigned_client]
        packer.pack_array(TaskManagerSerializer::kTaskSerializedFields);
        packer.pack(UuidToString(task.id));
        packer.pack(static_cast<int32_t>(task.type));
        packer.pack(static_cast<int32_t>(task.status));
        packer.pack(task.payload);
        packer.pack(static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                task.created_at.time_since_epoch())
                .count()));
        packer.pack(static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                task.last_updated_at.time_since_epoch())
                .count()));
        packer.pack(task.message);
        packer.pack(UuidToString(task.assigned_client));
    }

    // Compress entire data
    std::vector<uint8_t> compressed_data = zstd_compress(
        reinterpret_cast<const uint8_t*>(sbuf.data()), sbuf.size(), 3);

    // Return compressed data
    return std::vector<uint8_t>(
        std::make_move_iterator(compressed_data.begin()),
        std::make_move_iterator(compressed_data.end()));
}

tl::expected<void, SerializationError> TaskManagerSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    if (!task_manager_) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize TaskManager task_manager_ is null"));
    }

    // Decompress data
    std::vector<uint8_t> decompressed_data;
    try {
        decompressed_data = zstd_decompress(
            reinterpret_cast<const uint8_t*>(data.data()), data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "failed to decompress MessagePack data: " + std::string(e.what())));
    }

    // Parse MessagePack data
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(
            reinterpret_cast<const char*>(decompressed_data.data()),
            decompressed_data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "failed to unpack msgpack: " + std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();
    if (obj.type != msgpack::type::ARRAY) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "invalid MessagePack format: expected array of tasks"));
    }

    auto write_access = task_manager_->get_write_access();

    // Clear existing tasks
    write_access.clear_all();

    // Deserialize each task
    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        const msgpack::object& task_obj = obj.via.array.ptr[i];
        if (task_obj.type != msgpack::type::ARRAY ||
            task_obj.via.array.size !=
                TaskManagerSerializer::kTaskSerializedFields) {
            LOG(WARNING) << "Invalid task format for task index " << i
                         << ", skipping deserialization";
            continue;
        }

        const msgpack::object* arr = task_obj.via.array.ptr;

        Task task;
        std::string id_str = arr[0].as<std::string>();
        if (!StringToUuid(id_str, task.id)) {
            LOG(WARNING) << "Invalid UUID format for task id " << id_str
                         << ", skipping deserialization";
            continue;
        }
        try {
            task.type = static_cast<TaskType>(arr[1].as<int32_t>());
            task.status = static_cast<TaskStatus>(arr[2].as<int32_t>());
            task.payload = arr[3].as<std::string>();
            task.created_at = std::chrono::system_clock::time_point(
                std::chrono::milliseconds(arr[4].as<int64_t>()));
            task.last_updated_at = std::chrono::system_clock::time_point(
                std::chrono::milliseconds(arr[5].as<int64_t>()));
            task.message = arr[6].as<std::string>();
        } catch (const std::exception& e) {
            LOG(WARNING) << "Invalid task field types for task index " << i
                         << ": " << e.what() << ", skipping deserialization";
            continue;
        }

        std::string assigned_str = arr[7].as<std::string>();
        if (!StringToUuid(assigned_str, task.assigned_client)) {
            LOG(WARNING) << "Invalid UUID format for assigned_client "
                         << assigned_str << ", skipping deserialization";
            continue;
        }
        write_access.restore_task(std::move(task));
    }
    return {};
}

void TaskManagerSerializer::Reset() {
    if (!task_manager_) {
        return;
    }
    auto write_access = task_manager_->get_write_access();
    write_access.clear_all();
}
}  // namespace mooncake
