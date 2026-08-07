#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake {

class LocalDeleteRegistry {
   public:
    class Reservation {
       public:
        ~Reservation();
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;

        const std::vector<LocalDeleteTask>& tasks() const { return tasks_; }
        void Publish();

       private:
        friend class LocalDeleteRegistry;
        Reservation(LocalDeleteRegistry* registry,
                    std::vector<LocalDeleteTask> tasks);

        LocalDeleteRegistry* registry_;
        std::vector<LocalDeleteTask> tasks_;
        bool released_{false};
    };

    explicit LocalDeleteRegistry(size_t capacity = 50000)
        : capacity_(capacity) {}

    tl::expected<std::shared_ptr<Reservation>, ErrorCode> Reserve(
        std::vector<LocalDeleteTask> tasks);

    LocalDiskMountInfo Mount(const UUID& client_id,
                             const std::string& local_disk_segment_id,
                             uint32_t capabilities);
    void Unmount(const UUID& client_id);

    tl::expected<std::vector<LocalDeleteTask>, ErrorCode> Fetch(
        const UUID& client_id, const std::string& local_disk_segment_id,
        uint64_t mount_epoch, uint32_t limit) const;

    tl::expected<void, ErrorCode> ValidateMount(
        const UUID& client_id, const std::string& local_disk_segment_id,
        uint64_t mount_epoch) const;

    size_t Erase(const std::string& local_disk_segment_id,
                 const std::vector<LocalDeleteTaskId>& task_ids);

    bool ApplyDurableTasks(const std::vector<LocalDeleteTask>& tasks);
    std::vector<LocalDeleteTask> Snapshot() const;
    bool Restore(const std::vector<LocalDeleteTask>& tasks);
    void Reset();
    size_t Size() const;

   private:
    struct MountState {
        UUID client_id{0, 0};
        uint64_t mount_epoch{0};
        uint32_t capabilities{0};
    };

    void ReleaseReservation(size_t count);
    void PublishReservation(std::vector<LocalDeleteTask> tasks);

    mutable std::mutex mutex_;
    const size_t capacity_;
    size_t reserved_{0};
    std::unordered_map<std::string, MountState> mounts_;
    std::map<UUID, std::string> storage_by_client_;
    std::unordered_map<std::string,
                       std::map<LocalDeleteTaskId, LocalDeleteTask>>
        pending_;
};

ObjectIncarnation GenerateObjectIncarnation();
LocalDeleteTaskId GenerateLocalDeleteTaskId();

}  // namespace mooncake
