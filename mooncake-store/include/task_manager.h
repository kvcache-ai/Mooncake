#include <boost/functional/hash.hpp>
#include <queue>
#include <vector>
#include <string>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include "types.h"

namespace mooncake {
enum class TaskType {
    REPLICA_COPY,
    REPLICA_MOVE,
};

enum class TaskStatus {
    PENDING,
    INITIATED,
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
    ClientTaskManager() = default;
    ~ClientTaskManager() = default;

    void submit_task(const std::string& localhost_name, Task task);

    // Pop a batch of tasks, up to batch_size
    std::vector<Task> pop_tasks(const std::string& localhost_name,
                                size_t batch_size);

    // Get task status
    std::optional<TaskStatus> get_task_status(const UUID& task_id);

    // Update task status
    void update_task_status(const UUID& task_id, TaskStatus new_status);

   private:
    std::mutex mutex_;

    // Map: task_id -> Task
    std::unordered_map<UUID, Task, boost::hash<UUID>> all_tasks_;

    // Map: localhost_name -> queue of pending task_ids
    std::unordered_map<std::string, std::queue<UUID>> pending_tasks_;

    // Map: localhost_name -> set of task_ids currently being processed
    std::unordered_map<std::string, std::unordered_set<UUID, boost::hash<UUID>>>
        processing_tasks_;
};
}  // namespace mooncake
