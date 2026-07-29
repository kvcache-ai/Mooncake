#include "local_delete.h"

#include <algorithm>
#include <set>

namespace mooncake {

namespace {

template <typename T>
T GenerateNamedId() {
    UUID id{0, 0};
    do {
        id = generate_uuid();
    } while (id == UUID{0, 0});
    return T{.high = id.first, .low = id.second};
}

size_t PendingCount(
    const std::unordered_map<
        std::string, std::map<LocalDeleteTaskId, LocalDeleteTask>>& pending) {
    size_t count = 0;
    for (const auto& entry : pending) {
        count += entry.second.size();
    }
    return count;
}

bool IsValidTask(const LocalDeleteTask& task) {
    return !task.local_disk_segment_id.empty() &&
           (task.task_id.high != 0 || task.task_id.low != 0);
}

}  // namespace

ObjectIncarnation GenerateObjectIncarnation() {
    return GenerateNamedId<ObjectIncarnation>();
}

LocalDeleteTaskId GenerateLocalDeleteTaskId() {
    return GenerateNamedId<LocalDeleteTaskId>();
}

LocalDeleteRegistry::Reservation::Reservation(
    LocalDeleteRegistry* registry, std::vector<LocalDeleteTask> tasks)
    : registry_(registry), tasks_(std::move(tasks)) {}

LocalDeleteRegistry::Reservation::~Reservation() {
    if (!released_) {
        registry_->ReleaseReservation(tasks_.size());
    }
}

void LocalDeleteRegistry::Reservation::Publish() {
    if (released_) {
        return;
    }
    released_ = true;
    registry_->PublishReservation(std::move(tasks_));
}

tl::expected<std::shared_ptr<LocalDeleteRegistry::Reservation>, ErrorCode>
LocalDeleteRegistry::Reserve(std::vector<LocalDeleteTask> tasks) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!std::all_of(tasks.begin(), tasks.end(), IsValidTask)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const size_t pending_count = PendingCount(pending_);
    if (pending_count > capacity_ || reserved_ > capacity_ - pending_count ||
        tasks.size() > capacity_ - pending_count - reserved_) {
        return tl::unexpected(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    }
    reserved_ += tasks.size();
    return std::shared_ptr<Reservation>(
        new Reservation(this, std::move(tasks)));
}

LocalDiskMountInfo LocalDeleteRegistry::Mount(
    const UUID& client_id, const std::string& local_disk_segment_id,
    uint32_t capabilities) {
    if (local_disk_segment_id.empty()) {
        return {};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto previous_storage = storage_by_client_.find(client_id);
    if (previous_storage != storage_by_client_.end() &&
        previous_storage->second != local_disk_segment_id) {
        auto previous_mount = mounts_.find(previous_storage->second);
        if (previous_mount != mounts_.end() &&
            previous_mount->second.client_id == client_id) {
            mounts_.erase(previous_mount);
        }
        storage_by_client_.erase(previous_storage);
    }

    auto& state = mounts_[local_disk_segment_id];
    if (state.mount_epoch == 0) {
        do {
            const auto epoch_id = generate_uuid();
            state.mount_epoch = epoch_id.first ^ epoch_id.second;
        } while (state.mount_epoch == 0);
    } else if (state.client_id != client_id) {
        storage_by_client_.erase(state.client_id);
        ++state.mount_epoch;
        if (state.mount_epoch == 0) state.mount_epoch = 1;
    }
    state.client_id = client_id;
    state.capabilities = capabilities;
    storage_by_client_[client_id] = local_disk_segment_id;
    return {state.mount_epoch, state.capabilities};
}

void LocalDeleteRegistry::Unmount(const UUID& client_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto storage_it = storage_by_client_.find(client_id);
    if (storage_it == storage_by_client_.end()) {
        return;
    }
    auto mount_it = mounts_.find(storage_it->second);
    if (mount_it != mounts_.end() && mount_it->second.client_id == client_id) {
        mounts_.erase(mount_it);
    }
    storage_by_client_.erase(storage_it);
}

tl::expected<void, ErrorCode> LocalDeleteRegistry::ValidateMount(
    const UUID& client_id, const std::string& local_disk_segment_id,
    uint64_t mount_epoch) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = mounts_.find(local_disk_segment_id);
    if (it == mounts_.end() || it->second.client_id != client_id ||
        it->second.mount_epoch != mount_epoch ||
        (it->second.capabilities & kLocalDiskCapabilityObjectTombstoneV1) ==
            0) {
        return tl::unexpected(ErrorCode::ILLEGAL_CLIENT);
    }
    return {};
}

tl::expected<std::vector<LocalDeleteTask>, ErrorCode>
LocalDeleteRegistry::Fetch(const UUID& client_id,
                           const std::string& local_disk_segment_id,
                           uint64_t mount_epoch, uint32_t limit) const {
    if (limit == 0) {
        return std::vector<LocalDeleteTask>{};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto mount_it = mounts_.find(local_disk_segment_id);
    if (mount_it == mounts_.end() || mount_it->second.client_id != client_id ||
        mount_it->second.mount_epoch != mount_epoch ||
        (mount_it->second.capabilities &
         kLocalDiskCapabilityObjectTombstoneV1) == 0) {
        return tl::unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    std::vector<LocalDeleteTask> result;
    auto pending_it = pending_.find(local_disk_segment_id);
    if (pending_it == pending_.end()) {
        return result;
    }
    result.reserve(std::min<size_t>(limit, pending_it->second.size()));
    for (const auto& entry : pending_it->second) {
        if (result.size() == limit) {
            break;
        }
        result.push_back(entry.second);
    }
    return result;
}

size_t LocalDeleteRegistry::Erase(
    const std::string& local_disk_segment_id,
    const std::vector<LocalDeleteTaskId>& task_ids) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto pending_it = pending_.find(local_disk_segment_id);
    if (pending_it == pending_.end()) {
        return 0;
    }
    size_t erased = 0;
    for (const auto& task_id : task_ids) {
        erased += pending_it->second.erase(task_id);
    }
    if (pending_it->second.empty()) {
        pending_.erase(pending_it);
    }
    return erased;
}

bool LocalDeleteRegistry::ApplyDurableTasks(
    const std::vector<LocalDeleteTask>& tasks) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!std::all_of(tasks.begin(), tasks.end(), IsValidTask)) {
        return false;
    }
    const size_t pending_count = PendingCount(pending_);
    std::set<std::pair<std::string, LocalDeleteTaskId>> new_tasks;
    for (const auto& task : tasks) {
        auto storage_it = pending_.find(task.local_disk_segment_id);
        if (storage_it == pending_.end() ||
            !storage_it->second.contains(task.task_id)) {
            new_tasks.emplace(task.local_disk_segment_id, task.task_id);
        }
    }
    const size_t new_count = new_tasks.size();
    if (pending_count > capacity_ || new_count > capacity_ - pending_count) {
        return false;
    }
    for (const auto& task : tasks) {
        pending_[task.local_disk_segment_id].try_emplace(task.task_id, task);
    }
    return true;
}

std::vector<LocalDeleteTask> LocalDeleteRegistry::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LocalDeleteTask> result;
    result.reserve(PendingCount(pending_));
    for (const auto& storage_entry : pending_) {
        for (const auto& task_entry : storage_entry.second) {
            result.push_back(task_entry.second);
        }
    }
    return result;
}

bool LocalDeleteRegistry::Restore(const std::vector<LocalDeleteTask>& tasks) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (tasks.size() > capacity_ ||
        !std::all_of(tasks.begin(), tasks.end(), IsValidTask)) {
        return false;
    }
    pending_.clear();
    mounts_.clear();
    storage_by_client_.clear();
    reserved_ = 0;
    for (const auto& task : tasks) {
        pending_[task.local_disk_segment_id].try_emplace(task.task_id, task);
    }
    return true;
}

void LocalDeleteRegistry::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.clear();
    mounts_.clear();
    storage_by_client_.clear();
    reserved_ = 0;
}

size_t LocalDeleteRegistry::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return PendingCount(pending_);
}

void LocalDeleteRegistry::ReleaseReservation(size_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    reserved_ -= std::min(reserved_, count);
}

void LocalDeleteRegistry::PublishReservation(
    std::vector<LocalDeleteTask> tasks) {
    std::lock_guard<std::mutex> lock(mutex_);
    reserved_ -= std::min(reserved_, tasks.size());
    for (auto& task : tasks) {
        pending_[task.local_disk_segment_id].try_emplace(task.task_id,
                                                         std::move(task));
    }
}

}  // namespace mooncake
