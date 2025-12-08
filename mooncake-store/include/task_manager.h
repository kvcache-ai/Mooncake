#include <boost/functional/hash.hpp>
#include <queue>
#include <vector>
#include <string>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include "types.h"
#include "mutex.h"

namespace mooncake {
enum class TaskType {
    REPLICA_COPY,
    REPLICA_MOVE,
};

enum class TaskStatus {
    PENDING,
    PROCESSING,
    FAILED,
    SUCCESS,
};

struct Task {
    UUID id;
    TaskType type;
    TaskStatus status;
    std::string payload;  // JSON or other serialized data
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_updated_at;

    std::string error_message;  // message for FAILED status
    std::string assigned_client;
};

struct ReplicaCopyPayload {
    std::string key;
    std::string source;
    std::list<std::string> targets;
};

struct ReplicaMovePayload {
    std::string key;
    std::string source;
    std::string target;
};

class ClientTaskManager {
   public:
    explicit ClientTaskManager(size_t max_finished_tasks = 1000)
        : max_finished_tasks_(max_finished_tasks) {}
    ~ClientTaskManager() = default;

    UUID submit_task(const std::string& localhost_name, TaskType type,
                     const std::string& payload);

    std::vector<Task> pop_tasks(const std::string& localhost_name,
                                size_t batch_size);

    std::optional<Task> find_task_by_id(const UUID& task_id) const;

    void mark_success(const std::string& localhost_name, const UUID& task_id);

    void mark_failed(const std::string& localhost_name, const UUID& task_id,
                     const std::string& error_message);

   private:
    void update_task_status(const UUID& task_id, TaskStatus status,
                            const std::string& error_message = "");
    void prune_finished_tasks();

   private:
    mutable Mutex mutex_;
    size_t max_finished_tasks_ = 1000;

    // Map: task_id -> Task
    std::unordered_map<UUID, Task, boost::hash<UUID>> all_tasks_
        GUARDED_BY(mutex_);

    // Dispatch Queue (Pending)
    // Map: localhost_name -> queue of pending task_ids
    std::unordered_map<std::string, std::queue<UUID>> pending_tasks_
        GUARDED_BY(mutex_);

    // Active Set (Processing)
    // Map: localhost_name -> set of task_ids currently being processed
    std::unordered_map<std::string, std::unordered_set<UUID, boost::hash<UUID>>>
        processing_tasks_ GUARDED_BY(mutex_);

    // Tracks the order of finished tasks (Oldest -> Newest)
    // Used to implement LRU eviction for completed tasks
    std::deque<UUID> finished_task_history_ GUARDED_BY(mutex_);
};
}  // namespace mooncake
