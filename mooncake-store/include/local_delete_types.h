#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "config/offset_allocator_backend_config.h"
#include "ha/oplog/oplog_types.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

inline constexpr size_t kMaxPendingLocalDeletes = 50'000;
inline constexpr uint32_t kMaxLocalDeleteTasks = 256;

struct LocalDeleteTask {
    UUID task_id;
    UUID storage_id;
    std::string tenant_id;
    std::string key;
    ObjectIncarnation incarnation;

    bool operator==(const LocalDeleteTask&) const = default;
};
YLT_REFL(LocalDeleteTask, task_id, storage_id, tenant_id, key, incarnation);

inline bool CanPersistLocalDeleteKey(std::string_view tenant_id,
                                     std::string_view key) {
    // Include the tenant separator in the OffsetAllocator record size.
    return !tenant_id.empty() && !key.empty() &&
           key.size() <= kMaxOpLogObjectKeySize &&
           tenant_id.size() < kMaxOffsetStorageKeySize - key.size();
}

inline bool IsValidLocalDeleteTask(const LocalDeleteTask& task) {
    return task.task_id != UUID{} && task.storage_id != UUID{} &&
           CanPersistLocalDeleteKey(task.tenant_id, task.key) &&
           TenantId(task.tenant_id).IsValid() && !task.incarnation.IsZero();
}

struct LocalDeleteTarget {
    UUID task_id;
    UUID storage_id;
};
YLT_REFL(LocalDeleteTarget, task_id, storage_id);

struct LocalDeleteRemove {
    uint32_t version{1};
    ObjectIncarnation incarnation;
    std::vector<LocalDeleteTarget> targets;
};
YLT_REFL(LocalDeleteRemove, version, incarnation, targets);

struct LocalDeleteAck {
    uint32_t version{1};
    UUID storage_id;
    std::vector<UUID> task_ids;
};
YLT_REFL(LocalDeleteAck, version, storage_id, task_ids);

}  // namespace mooncake
