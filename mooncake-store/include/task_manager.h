#pragma once

#include <boost/functional/hash.hpp>
#include <deque>
#include <queue>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <ostream>
#include "types.h"
#include "mutex.h"
#include "master_config.h"

namespace mooncake {
enum class TaskType {
    REPLICA_COPY,
    REPLICA_MOVE,
};

inline std::ostream& operator<<(std::ostream& os, const TaskType& type) {
    switch (type) {
        case TaskType::REPLICA_COPY:
            os << "REPLICA_COPY";
            break;
        case TaskType::REPLICA_MOVE:
            os << "REPLICA_MOVE";
            break;
        default:
            os << "UNKNOWN_TASK_TYPE";
            break;
    }
    return os;
}

enum class TaskStatus {
    PENDING,
    PROCESSING,
    FAILED,
    SUCCESS,
};

inline bool is_finished_status(TaskStatus status) {
    return status == TaskStatus::FAILED || status == TaskStatus::SUCCESS;
}

inline std::ostream& operator<<(std::ostream& os, const TaskStatus& status) {
    switch (status) {
        case TaskStatus::PENDING:
            os << "PENDING";
            break;
        case TaskStatus::PROCESSING:
            os << "PROCESSING";
            break;
        case TaskStatus::FAILED:
            os << "FAILED";
            break;
        case TaskStatus::SUCCESS:
            os << "SUCCESS";
            break;
        default:
            os << "UNKNOWN_TASK_STATUS";
            break;
    }
    return os;
}

struct Task {
    UUID id;
    TaskType type;
    TaskStatus status;
    std::string payload;  // JSON or other serialized data
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_updated_at;

    std::string message;
    UUID assigned_client;
    uint32_t max_retry_attempts;

    bool is_finished() const { return is_finished_status(status); }

    void mark_processing() {
        status = TaskStatus::PROCESSING;
        last_updated_at = std::chrono::system_clock::now();
    }

    void mark_complete(TaskStatus final_status, const std::string& msg) {
        status = final_status;
        message = msg;
        last_updated_at = std::chrono::system_clock::now();
    }
};

struct ReplicaCopyPayload {
    std::string key;
    std::string source;
    std::vector<std::string> targets;
};
YLT_REFL(ReplicaCopyPayload, key, source, targets);

struct ReplicaMovePayload {
    std::string key;
    std::string source;
    std::string target;
};
YLT_REFL(ReplicaMovePayload, key, source, target);

template <TaskType T>
struct TaskPayloadTraits;

template <>
struct TaskPayloadTraits<TaskType::REPLICA_COPY> {
    using type = ReplicaCopyPayload;
    static constexpr const char* name = "ReplicaCopyPayload";
};

template <>
struct TaskPayloadTraits<TaskType::REPLICA_MOVE> {
    using type = ReplicaMovePayload;
    static constexpr const char* name = "ReplicaMovePayload";
};

template <typename T>
std::string serialize_payload(const T& payload) {
    std::string json;
    struct_json::to_json(payload, json);
    return json;
};

class ClientTaskManager;

// RAII Accessor: Holds a shared lock and provides read-only access
class ScopedTaskReadAccess {
   public:
    ScopedTaskReadAccess(const ClientTaskManager* manager, SharedMutex& mutex)
        : manager_(manager), lock_(&mutex, shared_lock) {}

    ~ScopedTaskReadAccess() = default;

    std::optional<Task> find_task_by_id(const UUID& task_id) const;

   private:
    const ClientTaskManager* manager_;
    SharedMutexLocker lock_;
};

// RAII Accessor: Holds an exclusive lock and provides read-write access
class ScopedTaskWriteAccess {
   public:
    ScopedTaskWriteAccess(ClientTaskManager* manager, SharedMutex& mutex)
        : manager_(manager), lock_(&mutex) {}

    ~ScopedTaskWriteAccess() = default;

    template <TaskType Type>
    tl::expected<UUID, ErrorCode> submit_task_typed(
        const UUID& client_id,
        const typename TaskPayloadTraits<Type>::type& payload) {
        std::string json = serialize_payload(payload);
        return submit_task(client_id, Type, json);
    }

    std::vector<Task> pop_tasks(const UUID& client_id, size_t batch_size);

    ErrorCode complete_task(const UUID& client_id, const UUID& task_id,
                            TaskStatus status, const std::string& message);

    void prune_finished_tasks();

    void prune_expired_tasks();

   private:
    tl::expected<UUID, ErrorCode> submit_task(const UUID& client_id,
                                              TaskType type,
                                              const std::string& payload);

    ClientTaskManager* manager_;
    SharedMutexLocker lock_;
};

class ClientTaskManager {
   public:
    friend class ScopedTaskReadAccess;
    friend class ScopedTaskWriteAccess;

    explicit ClientTaskManager(const TaskManagerConfig& config)
        : max_total_finished_tasks_(config.max_total_finished_tasks),
          max_total_pending_tasks_(config.max_total_pending_tasks),
          max_total_processing_tasks_(config.max_total_processing_tasks),
          pending_task_timeout_sec_(config.pending_task_timeout_sec),
          processing_task_timeout_sec_(config.processing_task_timeout_sec),
          max_retry_attempts_(config.max_retry_attempts) {}

    ~ClientTaskManager() = default;

    // Accessors
    ScopedTaskReadAccess get_read_access() const {
        return ScopedTaskReadAccess(this, mutex_);
    }

    ScopedTaskWriteAccess get_write_access() {
        return ScopedTaskWriteAccess(this, mutex_);
    }

   private:
    mutable SharedMutex mutex_;
    size_t max_total_finished_tasks_;
    size_t max_total_pending_tasks_;
    size_t max_total_processing_tasks_;

    // 0 = no timeout
    uint64_t pending_task_timeout_sec_;
    uint64_t processing_task_timeout_sec_;
    uint32_t max_retry_attempts_;

    size_t total_pending_tasks_ GUARDED_BY(mutex_) = 0;
    size_t total_processing_tasks_ GUARDED_BY(mutex_) = 0;

    // Map: task_id -> Task
    std::unordered_map<UUID, Task, boost::hash<UUID>> all_tasks_
        GUARDED_BY(mutex_);

    // Dispatch Queue (Pending)
    // Map: client_id -> queue of pending task_ids
    std::unordered_map<UUID, std::queue<UUID>, boost::hash<UUID>> pending_tasks_
        GUARDED_BY(mutex_);

    // Active Set (Processing)
    // Map: client_id -> set of task_ids currently being processed
    std::unordered_map<UUID, std::unordered_set<UUID, boost::hash<UUID>>,
                       boost::hash<UUID>>
        processing_tasks_ GUARDED_BY(mutex_);

    // Tracks the order of finished tasks (Oldest -> Newest)
    // Used to implement LRU eviction for completed tasks
    std::deque<UUID> finished_task_history_ GUARDED_BY(mutex_);
};
}  // namespace mooncake
