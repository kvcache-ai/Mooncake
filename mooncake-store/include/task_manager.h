#pragma once

#include <boost/functional/hash.hpp>
#include <queue>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <ostream>
#include "types.h"
#include "mutex.h"

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
        std::chrono::steady_clock::time_point created_at;
        std::chrono::steady_clock::time_point last_updated_at;

        std::string error_message;  // message for FAILED status
        UUID assigned_client;
    };

    struct ReplicaCopyPayload {
        std::string key;
        std::vector<std::string> targets;
    };
    YLT_REFL(ReplicaCopyPayload, key, targets);

    struct ReplicaMovePayload {
        std::string key;
        std::string source;
        std::string target;
    };
    YLT_REFL(ReplicaMovePayload, key, source, target);

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

            UUID submit_task(const UUID& client_id, TaskType type, const std::string& payload);
            
            std::vector<Task> pop_tasks(const UUID& client_id, size_t batch_size);

            std::optional<Task> find_task_by_id(const UUID& task_id) const;

            void mark_success(const UUID& client_id, const UUID& task_id);
            void mark_failed(const UUID& client_id, const UUID& task_id, const std::string& error_message);

        private:
            void update_task_status(const UUID& task_id, TaskStatus status, const std::string& error_message = "");
            void prune_finished_tasks();

            ClientTaskManager* manager_;
            SharedMutexLocker lock_;
    };

    class ClientTaskManager {
        public:
            friend class ScopedTaskReadAccess;
            friend class ScopedTaskWriteAccess;

            explicit ClientTaskManager(size_t max_finished_tasks = 1000) :
             max_finished_tasks_(max_finished_tasks) {}

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
            size_t max_finished_tasks_;

            // Map: task_id -> Task
            std::unordered_map<UUID, Task, boost::hash<UUID>> all_tasks_ GUARDED_BY(mutex_);

            // Dispatch Queue (Pending)
            // Map: client_id -> queue of pending task_ids
            std::unordered_map<UUID, std::queue<UUID>, boost::hash<UUID>> pending_tasks_ GUARDED_BY(mutex_);

            // Active Set (Processing)
            // Map: client_id -> set of task_ids currently being processed
            std::unordered_map<UUID, std::unordered_set<UUID, boost::hash<UUID>>, boost::hash<UUID>> processing_tasks_ GUARDED_BY(mutex_);

            // Tracks the order of finished tasks (Oldest -> Newest)
            // Used to implement LRU eviction for completed tasks
            std::deque<UUID> finished_task_history_ GUARDED_BY(mutex_);
    };
}