#include "task_executor.h"

#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>
#include <set>
#include <string>
#include "client_service.h"
#include "client_buffer.hpp"
#include "utils.h"
#include "rpc_types.h"

namespace mooncake {

// Maximum number of retry attempts for failed tasks
constexpr uint32_t MAX_RETRY_COUNT = 10;

ErrorCode ReplicaCopyExecutor::Execute(const ReplicaCopyPayload& payload) {
    const std::string& key = payload.key;
    const std::vector<std::string>& targets = payload.targets;

    LOG(INFO) << "action=replica_copy_start"
              << ", key=" << key << ", targets_count=" << targets.size();

    // Query only to find source segment (not for existence validation,
    // CopyStart will validate existence)
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) {
        ErrorCode error = query_result.error();
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=query_failed"
                   << ", error_code=" << error;
        return error;
    }

    // Find a source replica that is not in targets
    Replica::Descriptor source_replica;
    bool found_source = false;
    std::set<std::string> target_set(targets.begin(), targets.end());
    const auto& replicas = query_result.value().replicas;

    for (const auto& replica : replicas) {
        if (replica.is_memory_replica()) {
            const auto& mem_desc = replica.get_memory_descriptor();
            std::string replica_segment =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (target_set.find(replica_segment) == target_set.end()) {
                source_replica = replica;
                found_source = true;
                break;
            }
        }
    }

    if (!found_source) {
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=cannot_determine_src_segment";
        return ErrorCode::INTERNAL_ERROR;
    }

    std::string src_segment = source_replica.get_memory_descriptor()
                                  .buffer_descriptor.transport_endpoint_;

    // Call CopyStart first - it validates existence and allocates replicas
    auto copy_start_result =
        master_client_->CopyStart(key, src_segment, targets);
    if (!copy_start_result.has_value()) {
        ErrorCode error = copy_start_result.error();
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", src_segment=" << src_segment
                   << ", error=copy_start_failed"
                   << ", error_code=" << error;
        return error;
    }

    const auto& target_replicas = copy_start_result.value();
    if (target_replicas.empty()) {
        LOG(INFO) << "action=replica_copy_skipped"
                  << ", key=" << key
                  << ", info=all_target_segments_already_have_replicas";
        return ErrorCode::OK;
    }

    // Get object size from target replica descriptor
    uint64_t total_size = calculate_total_size(target_replicas[0]);
    if (total_size == 0) {
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=invalid_replica_size";
        auto revoke_result = master_client_->CopyRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_copy_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return ErrorCode::INVALID_PARAMS;
    }

    // Prepare slices for data transfer
    std::vector<Slice> slices;
    std::vector<uint8_t> temp_buffer(total_size);
    slices.reserve((total_size + kMaxSliceSize - 1) / kMaxSliceSize);

    size_t offset = 0;
    while (offset < total_size) {
        size_t chunk_size = std::min(total_size - offset, kMaxSliceSize);
        slices.emplace_back(temp_buffer.data() + offset, chunk_size);
        offset += chunk_size;
    }

    // Read data from source replica
    ErrorCode read_result = client_->TransferRead(source_replica, slices);
    if (read_result != ErrorCode::OK) {
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=transfer_read_failed"
                   << ", error_code=" << read_result;
        auto revoke_result = master_client_->CopyRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_copy_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return read_result;
    }

    // Write data to target replicas
    bool transfer_failed = false;
    ErrorCode transfer_error = ErrorCode::OK;
    for (const auto& target_replica : target_replicas) {
        ErrorCode transfer_result =
            client_->TransferWrite(target_replica, slices);
        if (transfer_result != ErrorCode::OK) {
            transfer_error = transfer_result;
            LOG(ERROR) << "action=replica_copy_transfer_failed"
                       << ", key=" << key
                       << ", target_replica_id=" << target_replica.id
                       << ", error_code=" << transfer_error;
            transfer_failed = true;
            break;
        }
    }

    if (transfer_failed) {
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=transfer_write_failed";
        auto revoke_result = master_client_->CopyRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_copy_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return transfer_error;
    }

    auto copy_end_result = master_client_->CopyEnd(key);
    if (!copy_end_result.has_value()) {
        ErrorCode error = copy_end_result.error();
        LOG(ERROR) << "action=replica_copy_failed"
                   << ", key=" << key << ", error=copy_end_failed"
                   << ", error_code=" << error;
        auto revoke_result = master_client_->CopyRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_copy_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return error;
    }

    LOG(INFO) << "action=replica_copy_success"
              << ", key=" << key << ", target_count=" << target_replicas.size();

    return ErrorCode::OK;
}

ErrorCode ReplicaMoveExecutor::Execute(const ReplicaMovePayload& payload) {
    const std::string& key = payload.key;
    const std::string& source = payload.source;
    const std::string& target = payload.target;

    LOG(INFO) << "action=replica_move_start"
              << ", key=" << key << ", source_segment=" << source
              << ", target_segment=" << target;

    // Call MoveStart first - it validates existence and allocates replica if
    // needed
    auto move_start_result = master_client_->MoveStart(key, source, target);
    if (!move_start_result.has_value()) {
        ErrorCode error = move_start_result.error();
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=move_start_failed"
                   << ", error_code=" << error;
        // MoveStart already validated existence, so we just return the error
        return error;
    }

    const auto& target_replica_opt = move_start_result.value();
    if (!target_replica_opt.has_value()) {
        LOG(INFO) << "action=replica_move_skipped"
                  << ", key=" << key << ", info=target_replica_already_exists";
        // Target already exists, consider it success
        return ErrorCode::OK;
    }

    const auto& target_replica = target_replica_opt.value();

    // Get object size from target replica descriptor
    uint64_t total_size = calculate_total_size(target_replica);
    if (total_size == 0) {
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=invalid_replica_size";
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return ErrorCode::INVALID_PARAMS;
    }

    // Query to get source replica descriptor for reading data
    // (MoveStart already validated source exists, so this is just for getting
    // descriptor)
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) {
        ErrorCode error = query_result.error();
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=query_failed"
                   << ", error_code=" << error;
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return error;
    }

    // Find source replica descriptor
    Replica::Descriptor source_replica;
    bool found_source = false;
    for (const auto& replica : query_result.value().replicas) {
        std::string segment_name;
        if (replica.is_memory_replica()) {
            const auto& mem_desc = replica.get_memory_descriptor();
            segment_name = mem_desc.buffer_descriptor.transport_endpoint_;
        } else if (replica.is_disk_replica()) {
            segment_name = source;
        } else {
            continue;
        }

        if (segment_name == source) {
            source_replica = replica;
            found_source = true;
            break;
        }
    }

    if (!found_source) {
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key
                   << ", error=source_replica_not_found_in_list";
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return ErrorCode::REPLICA_NOT_FOUND;
    }

    // Prepare slices for data transfer
    std::vector<Slice> slices;
    std::vector<uint8_t> temp_buffer(total_size);
    slices.reserve((total_size + kMaxSliceSize - 1) / kMaxSliceSize);

    size_t offset = 0;
    while (offset < total_size) {
        size_t chunk_size = std::min(total_size - offset, kMaxSliceSize);
        slices.emplace_back(temp_buffer.data() + offset, chunk_size);
        offset += chunk_size;
    }

    // Read data from source replica
    ErrorCode read_result = client_->TransferRead(source_replica, slices);
    if (read_result != ErrorCode::OK) {
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=transfer_read_failed"
                   << ", error_code=" << read_result;
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return read_result;
    }

    // Write data to target replica
    ErrorCode transfer_result = client_->TransferWrite(target_replica, slices);
    if (transfer_result != ErrorCode::OK) {
        ErrorCode error = transfer_result;
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=transfer_write_failed"
                   << ", error_code=" << error;
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return error;
    }

    auto move_end_result = master_client_->MoveEnd(key);
    if (!move_end_result.has_value()) {
        ErrorCode error = move_end_result.error();
        LOG(ERROR) << "action=replica_move_failed"
                   << ", key=" << key << ", error=move_end_failed"
                   << ", error_code=" << error;
        auto revoke_result = master_client_->MoveRevoke(key);
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_move_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
        return error;
    }

    LOG(INFO) << "action=replica_move_success"
              << ", key=" << key << ", source_segment=" << source
              << ", target_segment=" << target;

    return ErrorCode::OK;
}

void TaskExecutor::SubmitTask(const ClientTask& client_task,
                              const UUID& client_id) {
    if (!running_.load()) {
        LOG(WARNING) << "action=task_rejected"
                     << ", task_id=" << client_task.assignment.id
                     << ", reason=executor_stopped";
        return;
    }

    thread_pool_.enqueue([this, client_task, client_id]() {
        ExecuteTask(client_task, client_id);
    });
}

void TaskExecutor::ExecuteTask(const ClientTask& client_task,
                               const UUID& client_id) {
    const auto& assignment = client_task.assignment;
    ErrorCode result = ErrorCode::OK;

    if (!client_ || !master_client_) {
        LOG(WARNING) << "action=task_execution_skipped"
                     << ", task_id=" << assignment.id
                     << ", reason=null_client_or_master_client";
        result = ErrorCode::INTERNAL_ERROR;
    } else {
        try {
            switch (assignment.type) {
                case TaskType::REPLICA_COPY: {
                    ReplicaCopyPayload payload;
                    struct_json::from_json(payload, assignment.payload);
                    result = copy_executor_.Execute(payload);
                    break;
                }
                case TaskType::REPLICA_MOVE: {
                    ReplicaMovePayload payload;
                    struct_json::from_json(payload, assignment.payload);
                    result = move_executor_.Execute(payload);
                    break;
                }
                default:
                    LOG(ERROR) << "action=task_execution_failed"
                               << ", task_id=" << assignment.id
                               << ", error=unknown_task_type"
                               << ", task_type=" << assignment.type;
                    result = ErrorCode::INVALID_PARAMS;
                    break;
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "action=task_execution_failed"
                       << ", task_id=" << assignment.id << ", error=exception"
                       << ", exception=" << e.what();
            result = ErrorCode::INTERNAL_ERROR;
        }
    }

    if (master_client_) {
        if (result == ErrorCode::OK) {
            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::SUCCESS;
            complete_request.message = "Task completed successfully";
            auto complete_result =
                master_client_->MarkTaskToComplete(client_id, complete_request);
            if (!complete_result.has_value()) {
                LOG(WARNING) << "action=task_complete_failed"
                             << ", task_id=" << assignment.id
                             << ", error_code=" << complete_result.error();
            }
        } else {
            uint32_t current_retry_count = client_task.retry_count;
            // Only retry on allocation failures (NO_AVAILABLE_HANDLE)
            // Other errors (e.g., OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should
            // not be retried
            bool should_retry = (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
                                (current_retry_count < MAX_RETRY_COUNT);

            if (should_retry) {
                ClientTask retry_task = client_task;
                retry_task.increment_retry();

                const auto retry_delay =
                    std::chrono::milliseconds(50 * (current_retry_count + 1));
                std::this_thread::sleep_for(retry_delay);

                LOG(WARNING) << "action=task_execution_failed_retry"
                             << ", task_id=" << assignment.id
                             << ", error_code=" << result
                             << ", retry_count=" << current_retry_count
                             << ", max_retry_count=" << MAX_RETRY_COUNT
                             << ", will_retry=true"
                             << ", retry_delay=" << retry_delay.count() << "ms";

                thread_pool_.enqueue([this, retry_task, client_id]() {
                    ExecuteTask(retry_task, client_id);
                });
            } else {
                LOG(ERROR) << "action=task_execution_failed_max_retries_reached"
                           << ", task_id=" << assignment.id
                           << ", error_code=" << result
                           << ", retry_count=" << current_retry_count
                           << ", max_retry_count=" << MAX_RETRY_COUNT;
                TaskCompleteRequest complete_request;
                complete_request.id = assignment.id;
                complete_request.status = TaskStatus::FAILED;
                complete_request.message =
                    toString(result) + " (max retries reached: " +
                    std::to_string(MAX_RETRY_COUNT) + ")";
                auto complete_result = master_client_->MarkTaskToComplete(
                    client_id, complete_request);
                if (!complete_result.has_value()) {
                    LOG(WARNING) << "action=task_complete_failed"
                                 << ", task_id=" << assignment.id
                                 << ", error_code=" << complete_result.error();
                }
            }
        }
    }
}

void TaskExecutor::Stop() {
    if (!running_.exchange(false)) {
        return;
    }

    thread_pool_.stop();
}

}  // namespace mooncake
