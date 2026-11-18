#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"
#include "nof/nof_runtime.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <limits>
#if defined(__linux__)
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "common.h"
#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.h"
#include "common/network.h"
#include "config/client_host_identity_config.h"
#include "config/client_object_checksum_config.h"
#include "replica_selection.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "config/client_auto_discovery_config.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "config/client_numa_config.h"
#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, tenant_id, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return master_client_.CreateMoveTask(key, source, target);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    return master_client_.CreateMoveTask(key, tenant_id, source, target);
}

bool Client::IsTaskCancellationRequested(const UUID* task_id) {
    if (!task_id) {
        return false;
    }
    // A failed check is best effort, including calls to older masters.
    return master_client_.IsTaskCancellationRequested(*task_id).value_or(false);
}

tl::expected<void, ErrorCode> Client::ExecuteReplicaTransfer(
    const std::string& key, const std::string& action_name,
    std::function<tl::expected<void, ErrorCode>()> end_fn,
    std::function<tl::expected<void, ErrorCode>()> revoke_fn,
    const Replica::Descriptor& source,
    const std::vector<Replica::Descriptor>& targets, const UUID* task_id) {
    auto revoke_lambda = [&]() {
        auto revoke_result = revoke_fn();
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_" << action_name << "_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
    };
    auto cancel_and_revoke = [&]() -> tl::expected<void, ErrorCode> {
        auto result = revoke_fn();
        if (!result &&
            result.error() != ErrorCode::OBJECT_NO_REPLICATION_TASK &&
            result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(WARNING) << "action=replica_" << action_name
                         << "_cancel_revoke_failed, key=" << key
                         << ", error_code=" << result.error();
            // Do not report a successful cancellation, or retry the transfer,
            // when the master's cleanup has not been confirmed.
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return tl::unexpected(ErrorCode::TASK_CANCELED);
    };

    std::vector<Slice> slices;
    if (!targets.empty()) {
        // currently only memory source replica is supported
        if (!source.is_memory_replica()) {
            LOG(ERROR) << "action=replica_" << action_name << "_failed"
                       << ", key=" << key << ", error=invalid_replica_type";
            revoke_lambda();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Validate that source replica is in local memory
        if (!IsReplicaOnLocalMemory(source)) {
            LOG(ERROR) << "action=replica_" << action_name << "_failed"
                       << ", key=" << key
                       << ", error=source_replica_not_in_local_memory";
            revoke_lambda();
            return tl::unexpected(ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
        }

        // Split the source replica into slices for transfer
        // This avoids data copy because the source replica is already in memory
        const auto& buffer_descriptor =
            source.get_memory_descriptor().buffer_descriptor;
        void* buffer =
            reinterpret_cast<void*>(buffer_descriptor.buffer_address_);
        slices = split_into_slices(buffer, buffer_descriptor.size_);
    }

    for (const auto& target : targets) {
        if (IsTaskCancellationRequested(task_id)) {
            return cancel_and_revoke();
        }
        if (task_id && slices.size() > 1 && target.is_memory_replica()) {
            uint64_t offset = 0;
            std::vector<Slice> chunk(1);
            for (const auto& slice : slices) {
                if (offset != 0 && IsTaskCancellationRequested(task_id)) {
                    return cancel_and_revoke();
                }
                chunk.front() = slice;
                if (TransferWriteRange(target, chunk, offset,
                                       TransferIntent::kMigration) !=
                    ErrorCode::OK) {
                    revoke_lambda();
                    return tl::unexpected(ErrorCode::TRANSFER_FAIL);
                }
                offset += slice.size;
            }
        } else if (TransferWrite(target, slices, TransferIntent::kMigration) !=
                   ErrorCode::OK) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
    }

    // A cancellation observed before commit can safely revoke the allocation.
    if (IsTaskCancellationRequested(task_id)) {
        return cancel_and_revoke();
    }
    auto end_result = end_fn();
    if (!end_result.has_value()) {
        // Start creates a replication task even when the target already
        // exists, so a failed end must release the source reservation.
        revoke_lambda();
        return tl::unexpected(end_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& source,
    const std::vector<std::string>& targets) {
    return Copy(key, master_client_.tenant_id(), source, targets);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets) {
    return Copy(key, tenant_id, source, targets, UUID{}, 0);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch, const UUID* task_id) {
    if (IsTaskCancellationRequested(task_id)) {
        return tl::unexpected(ErrorCode::TASK_CANCELED);
    }
    LOG(INFO) << "action=replica_copy_start" << ", key=" << key
              << ", targets_count=" << targets.size();

    // Call CopyStart first - it validates existence and allocates replicas
    auto start_result =
        dynamic_replication_lease_id == UUID{}
            ? master_client_.CopyStart(key, tenant_id, source, targets)
            : master_client_.DynamicReplicaCopyStart(
                  key, tenant_id, source, targets, dynamic_replication_lease_id,
                  dynamic_replication_version_epoch);
    if (!start_result.has_value()) {
        ErrorCode error = start_result.error();
        if (dynamic_replication_lease_id != UUID{} &&
            error == ErrorCode::OBJECT_ALREADY_EXISTS) {
            LOG(INFO) << "action=replica_copy_skipped"
                      << ", key=" << key
                      << ", info=target_replicas_already_exist";
            return {};
        }
        LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                   << ", source=" << source << ", error=copy_start_failed"
                   << ", error_code=" << error;
        return tl::unexpected(error);
    }

    const auto& response = start_result.value();
    auto copy_end = [&]() {
        return dynamic_replication_lease_id == UUID{}
                   ? master_client_.CopyEnd(key, tenant_id)
                   : master_client_.DynamicReplicaCopyEnd(
                         key, tenant_id, dynamic_replication_lease_id,
                         dynamic_replication_version_epoch);
    };
    auto copy_revoke = [&]() {
        return dynamic_replication_lease_id == UUID{}
                   ? master_client_.CopyRevoke(key, tenant_id)
                   : master_client_.DynamicReplicaCopyRevoke(
                         key, tenant_id, dynamic_replication_lease_id,
                         dynamic_replication_version_epoch);
    };
    auto result =
        ExecuteReplicaTransfer(key, "copy", copy_end, copy_revoke,
                               response.source, response.targets, task_id);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_copy_success" << ", key=" << key
                  << ", target_count=" << response.targets.size();
    }

    return result;
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& source,
                                           const std::string& target) {
    return Move(key, master_client_.tenant_id(), source, target);
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& tenant_id,
                                           const std::string& source,
                                           const std::string& target,
                                           const UUID* task_id) {
    if (IsTaskCancellationRequested(task_id)) {
        return tl::unexpected(ErrorCode::TASK_CANCELED);
    }
    LOG(INFO) << "action=replica_move_start" << ", key=" << key
              << ", source_segment=" << source << ", target_segment=" << target;

    // Call MoveStart first - it validates existence and allocates replica if
    // needed
    auto move_start_result =
        master_client_.MoveStart(key, tenant_id, source, target);
    if (!move_start_result.has_value()) {
        ErrorCode error = move_start_result.error();
        LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                   << ", error=move_start_failed" << ", error_code=" << error;
        // MoveStart already validated existence, so we just return the error
        return tl::unexpected(error);
    }

    const auto& response = move_start_result.value();
    std::vector<Replica::Descriptor> targets;
    if (response.target) {
        targets.push_back(*response.target);
    } else {
        LOG(INFO) << "action=replica_move_skipped" << ", key=" << key
                  << ", info=target_replica_already_exists";
    }

    auto result = ExecuteReplicaTransfer(
        key, "move", [&]() { return master_client_.MoveEnd(key, tenant_id); },
        [&]() { return master_client_.MoveRevoke(key, tenant_id); },
        response.source, targets, task_id);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_move_success" << ", key=" << key
                  << ", source_segment=" << source
                  << ", target_segment=" << target;
    }

    return result;
}

tl::expected<UUID, ErrorCode> Client::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    return master_client_.CreateDrainJob(request);
}

tl::expected<UUID, ErrorCode> Client::CreateTimedDrainJob(
    const CreateDrainJobRequest& request, uint64_t timeout_ms) {
    return master_client_.CreateTimedDrainJob(request, timeout_ms);
}

tl::expected<QueryJobResponse, ErrorCode> Client::QueryDrainJob(
    const UUID& job_id) {
    return master_client_.QueryDrainJob(job_id);
}

tl::expected<void, ErrorCode> Client::CancelDrainJob(const UUID& job_id) {
    return master_client_.CancelDrainJob(job_id);
}

tl::expected<ListDrainJobsResponse, ErrorCode> Client::ListDrainJobs(
    const DrainJobListRequest& request) {
    return master_client_.ListDrainJobs(request);
}

tl::expected<ListTasksResponse, ErrorCode> Client::ListTasks(
    const TaskListRequest& request) {
    return master_client_.ListTasks(request);
}

tl::expected<void, ErrorCode> Client::CancelTask(const UUID& task_id) {
    return master_client_.CancelTask(task_id);
}

tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
    const UUID& task_id) {
    return master_client_.QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> Client::FetchTasks(
    size_t batch_size) {
    return master_client_.FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_.MarkTaskToComplete(update_request);
}

void Client::PollAndDispatchTasks() {
    if (task_running_.load()) {
        auto fetch_result = FetchTasks(kTaskBatchSize);
        if (fetch_result.has_value()) {
            const auto& tasks = fetch_result.value();
            if (!tasks.empty()) {
                LOG(INFO) << "action=task_poll_success"
                          << ", task_count=" << tasks.size();
                for (const auto& task_assignment : tasks) {
                    SubmitTask(task_assignment);
                }
            }
        } else {
            ErrorCode error = fetch_result.error();
            // Only log if it's not an RPC failure (which is expected
            // during connection failures)
            if (error != ErrorCode::RPC_FAIL) {
                LOG(WARNING)
                    << "action=task_poll_failed" << ", error_code=" << error;
            }
        }
    }
}

void Client::TaskPollThreadMain() {
    const auto poll_interval = std::chrono::milliseconds(1000);

    while (task_poll_running_.load()) {
        PollAndDispatchTasks();
        std::this_thread::sleep_for(poll_interval);
    }
}

void Client::SubmitTask(const TaskAssignment& assignment) {
    if (!task_running_.load()) {
        LOG(WARNING) << "action=task_rejected" << ", task_id=" << assignment.id
                     << ", reason=executor_stopped";
        return;
    }

    // Construct ClientTask from TaskAssignment
    ClientTask client_task;
    client_task.assignment = assignment;
    client_task.retry_count = 0;

    task_thread_pool_.enqueue(
        [this, client_task]() { ExecuteTask(client_task); });
}

void Client::ExecuteTask(const ClientTask& client_task) {
    const auto& assignment = client_task.assignment;
    ErrorCode result = ErrorCode::OK;

    try {
        switch (assignment.type) {
            case TaskType::REPLICA_COPY: {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, assignment.payload);
                const UUID dynamic_lease_id{
                    payload.dynamic_replication_lease_id_high,
                    payload.dynamic_replication_lease_id_low};
                auto copy_result = Copy(
                    payload.key, payload.tenant_id, payload.source,
                    payload.targets, dynamic_lease_id,
                    payload.dynamic_replication_version_epoch, &assignment.id);
                if (copy_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = copy_result.error();
                }
                break;
            }
            case TaskType::REPLICA_MOVE: {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto move_result =
                    Move(payload.key, payload.tenant_id, payload.source,
                         payload.target, &assignment.id);
                if (move_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = move_result.error();
                }
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

    auto report_completion = [&](const TaskCompleteRequest& request) {
        for (;;) {
            auto completion = master_client_.MarkTaskToComplete(request);
            if (completion) {
                return;
            }
            LOG(WARNING) << "action=task_complete_failed"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << completion.error();
            if (!task_running_.load() ||
                (completion.error() != ErrorCode::RPC_FAIL &&
                 completion.error() != ErrorCode::RPC_TIMEOUT &&
                 completion.error() !=
                     ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS)) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    };

    if (result == ErrorCode::OK || result == ErrorCode::TASK_CANCELED) {
        TaskCompleteRequest complete_request;
        complete_request.id = assignment.id;
        complete_request.status = result == ErrorCode::OK
                                      ? TaskStatus::SUCCESS
                                      : TaskStatus::CANCELED;
        complete_request.message = result == ErrorCode::OK
                                       ? "Task completed successfully"
                                       : "Task canceled";
        report_completion(complete_request);
    } else {
        uint32_t current_retry_count = client_task.retry_count;
        // Only retry on allocation failures (NO_AVAILABLE_HANDLE)
        // Other errors (e.g., OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should
        // not be retried
        bool should_retry =
            (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
            (current_retry_count < assignment.max_retry_attempts);

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
                         << ", max_retry_count="
                         << assignment.max_retry_attempts << ", will_retry=true"
                         << ", retry_delay=" << retry_delay.count() << "ms";

            task_thread_pool_.enqueue(
                [this, retry_task]() { ExecuteTask(retry_task); });
        } else {
            LOG(ERROR) << "action=task_execution_failed"
                       << ", task_id=" << assignment.id
                       << ", error_code=" << result
                       << ", retry_count=" << current_retry_count
                       << ", max_retry_count=" << assignment.max_retry_attempts;
            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message =
                toString(result) + " (max retries reached: " +
                std::to_string(assignment.max_retry_attempts) + ")";
            report_completion(complete_request);
        }
    }
}

}  // namespace mooncake
