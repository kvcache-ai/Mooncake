#include "offload_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "master_metric_manager.h"

namespace mooncake {

OffloadManager::OffloadManager(LocalSsdManager& local_ssd, const HostView& host,
                               const OffloadConfig& config,
                               std::chrono::seconds task_timeout)
    : local_ssd_(local_ssd),
      host_(host),
      config_(config),
      task_timeout_(task_timeout) {
    config_.Validate();
}

// --- Self-contained offload task state machine ------------------------------

bool OffloadManager::HasTask(const TenantId& tenant_id,
                             std::string_view key) const {
    return tasks_.Contains(tenant_id, key);
}

bool OffloadManager::CollectTargets(const Replica& replica,
                                    TargetList& out) const {
    const auto& segment_names = replica.get_segment_names();
    boost::container::small_vector<std::string_view, 4> valid_segments;
    for (const auto& name : segment_names) {
        if (name.has_value()) {
            valid_segments.emplace_back(*name);
        }
    }
    if (valid_segments.empty()) {
        return false;
    }

    Replica::Descriptor source_descriptor;
    if (!host_.IsReplicaReadable(replica) ||
        !replica.getDescriptorIfAvailable(source_descriptor) ||
        !source_descriptor.is_memory_replica()) {
        return false;
    }
    const auto source_session = replica.getClientSession();
    if (!source_session || !source_session->IsServing()) {
        return false;
    }

    boost::container::small_vector<UUID, 4> owners(valid_segments.size());
    if (!host_.ResolveSegmentOwners(valid_segments, owners)) {
        return false;
    }

    const int64_t size = static_cast<int64_t>(
        source_descriptor.get_memory_descriptor().buffer_descriptor.size_);
    for (const UUID& client_id : owners) {
        out.push_back(OffloadTarget{
            .source_id = replica.id(),
            .client_id = client_id,
            .size = size,
        });
    }
    return true;
}

bool OffloadManager::StartOffloadTargets(
    const TenantId& tenant_id, const std::string& key,
    std::span<const OffloadTarget> targets,
    const std::function<ReplicaPin(ReplicaID)>& pin_source) {
    if (!config_.enable_offload || targets.empty()) {
        return false;
    }

    return tasks_.WithShardLocked(tenant_id, key, [&](auto& shard) -> bool {
        if (shard.Contains(tenant_id, key)) {
            return false;
        }

        std::optional<ReplicaID> pinned_source_id;
        std::vector<UUID> mirror_clients;

        std::optional<ReplicaID> current_source_id;
        bool current_source_enqueued = false;
        UUID current_source_last_client{};
        auto flush_source_metric = [&]() {
            if (current_source_enqueued) {
                MasterMetricManager::instance().inc_offload_enqueued(
                    UuidToString(current_source_last_client));
                current_source_enqueued = false;
            }
        };

        for (const auto& target : targets) {
            if (!current_source_id || *current_source_id != target.source_id) {
                flush_source_metric();
                current_source_id = target.source_id;
            }
            const ErrorCode err = local_ssd_.EnqueueOffload(
                target.client_id,
                OffloadTaskItem{.tenant_id = tenant_id.value(),
                                .key = key,
                                .size = target.size},
                config_.offloading_queue_limit);
            if (err == ErrorCode::KEYS_ULTRA_LIMIT) {
                MasterMetricManager::instance().inc_offload_enqueue_rejected(
                    UuidToString(target.client_id));
            }
            if (err != ErrorCode::OK) {
                continue;
            }
            if (mirror_clients.empty()) {
                mirror_clients.reserve(targets.size());
            }
            mirror_clients.push_back(target.client_id);
            if (!pinned_source_id.has_value()) {
                pinned_source_id = target.source_id;
            }
            current_source_last_client = target.client_id;
            current_source_enqueued = true;
        }
        flush_source_metric();

        if (!pinned_source_id.has_value()) {
            return false;
        }
        // Pin before making the task visible in `tasks_` so any concurrent
        // eviction/move check observes refcnt >= 1.
        ReplicaPin source_pin = pin_source(*pinned_source_id);
        shard.Emplace(tenant_id, key, std::move(source_pin),
                      std::chrono::system_clock::now(),
                      std::move(mirror_clients));
        return true;
    });
}

bool OffloadManager::StartOffload(const TenantId& tenant_id,
                                  const std::string& key, Replica& source) {
    if (!config_.enable_offload) {
        return false;
    }
    TargetList targets;
    if (!CollectTargets(source, targets)) {
        return false;
    }
    return StartOffloadTargets(tenant_id, key, targets,
                               [&source](ReplicaID) { return source.Pin(); });
}

void OffloadManager::OnPutEnd(const TenantId& tenant_id, const std::string& key,
                              ReplicaType replica_type,
                              ObjectMetadata& metadata) {
    if (replica_type == ReplicaType::DFS || !config_.ShouldOffloadOnPutEnd() ||
        metadata.HasReplica([](const Replica& replica) {
            return replica.is_dfs_replica() && replica.is_processing();
        })) {
        return;
    }

    TargetList targets;
    metadata.VisitReplicas(
        [](const Replica& replica) {
            return replica.is_completed() && replica.is_memory_replica();
        },
        [this, &targets](const Replica& replica) {
            (void)CollectTargets(replica, targets);
        });
    (void)StartOffloadTargets(
        tenant_id, key, targets, [&metadata](ReplicaID source_id) -> ReplicaPin {
            if (auto* source = metadata.GetReplicaByID(source_id);
                source != nullptr) {
                return source->Pin();
            }
            return {};
        });
}

bool OffloadManager::TryCancelQueuedOffload(const TenantId& tenant_id,
                                            std::string_view key) {
    bool cancelled = true;
    std::optional<UUID> cancelled_client;

    tasks_.WithExistingLocked(
        tenant_id, key, [&](OffloadingTask& task, bool& erase_entry) {
            if (task.mirror_clients.empty() ||
                !local_ssd_.CancelOffloadsIfAllPending(task.mirror_clients,
                                                       tenant_id, key)) {
                cancelled = false;
                return;
            }
            cancelled_client = task.mirror_clients[0];
            erase_entry = true;
        });

    if (cancelled_client.has_value()) {
        MasterMetricManager::instance().inc_offload_cancelled(
            UuidToString(*cancelled_client));
    }
    return cancelled;
}

tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
OffloadManager::HandleOffloadHeartbeat(const UUID& client_id,
                                       bool enable_offloading) {
    auto pending =
        local_ssd_.SetOffloadingAndTakePending(client_id, enable_offloading);
    if (!pending) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(pending.error());
    }
    if (enable_offloading) {
        return std::move(*pending);
    }

    for (const auto& task : *pending) {
        // Synchronize against any concurrent StartOffload holding this key's
        // shard lock (which enqueues into local_ssd_ before Emplace).
        // Destroying the erased OffloadingTask releases its ReplicaPin directly
        // without acquiring any key lock.
        (void)tasks_.EraseSync(TenantId(task.tenant_id), task.key);
    }
    return std::vector<OffloadTaskItem>{};
}

bool OffloadManager::FailOffload(const UUID& client_id,
                                 const TenantId& tenant_id,
                                 std::string_view key) {
    auto removed = tasks_.Erase(tenant_id, key);
    if (!removed.has_value()) {
        return false;
    }
    removed.reset();
    MasterMetricManager::instance().inc_offload_failed(UuidToString(client_id));
    return true;
}

OffloadManager::CompleteStatus OffloadManager::CompleteOffload(
    const UUID& client_id, const TenantId& tenant_id, std::string_view key,
    ObjectMetadata& metadata, Replica& local_disk_replica) {
    auto removed = tasks_.Erase(tenant_id, key);
    if (!removed.has_value()) {
        return CompleteStatus::kNotTracked;
    }
    removed.reset();

    if (!local_ssd_.HasClient(client_id)) {
        return CompleteStatus::kRefusedUnmounted;
    }

    const int64_t object_size =
        static_cast<int64_t>(local_disk_replica.get_descriptor()
                                 .get_local_disk_descriptor()
                                 .object_size);
    if (metadata.ApplyLocalDiskReplica(client_id, local_disk_replica)) {
        MasterMetricManager::instance().inc_offload_completed(
            UuidToString(client_id));
        if (object_size > 0) {
            local_ssd_.AdjustUsedBytes(client_id, object_size);
        }
    }
    return CompleteStatus::kCommitted;
}

void OffloadManager::OnObjectErased(const TenantId& tenant_id,
                                    std::string_view key) {
    if (auto removed = tasks_.Erase(tenant_id, key); removed.has_value()) {
        // Drop any pending mailbox entry so the next heartbeat does not drain a
        // task-less key back to the client.
        for (const UUID& client_id : removed->mirror_clients) {
            local_ssd_.RemoveOffload(client_id, tenant_id, key);
        }
    }
}

void OffloadManager::DiscardExpiredTasks(
    const std::chrono::system_clock::time_point& now) {
    tasks_.EraseIf([&](const ScopedObjectKey& scoped_key,
                       OffloadingTask& task) -> bool {
        if (task.start_time + task_timeout_ > now) {
            return false;
        }
        LOG(WARNING) << "Offloading task expired for key: " << scoped_key.key
                     << " tenant=" << scoped_key.tenant_id;
        // Remove pending mailbox entries while holding the shard lock (same
        // lock order as StartOffloadTargets / TryCancelQueuedOffload:
        // tasks_ shard lock -> local_ssd_ lock) so a newly re-enqueued task
        // for the same key cannot have its mailbox entry wiped.
        for (const UUID& client_id : task.mirror_clients) {
            local_ssd_.RemoveOffload(client_id, scoped_key.tenant_id,
                                     scoped_key.key);
        }
        return true;
    });
}

}  // namespace mooncake
