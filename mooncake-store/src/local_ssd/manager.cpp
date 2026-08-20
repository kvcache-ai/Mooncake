#include "local_ssd/manager.h"

#include <algorithm>
#include <iterator>
#include <mutex>
#include <utility>

namespace mooncake {

LocalSsdTaskMailbox::LocalSsdTaskMailbox(bool enable_offloading)
    : enable_offloading_(enable_offloading) {}

ErrorCode LocalSsdTaskMailbox::EnqueueOffload(OffloadTaskItem task,
                                              size_t limit) {
    MutexLocker lock(&mutex_);
    if (!enable_offloading_) {
        return ErrorCode::UNABLE_OFFLOADING;
    }
    if (pending_offloads_.size() >= limit) {
        return ErrorCode::KEYS_ULTRA_LIMIT;
    }
    auto index = TenantId(task.tenant_id).MakeScopedKey(task.key);
    if (!pending_offloads_.emplace(std::move(index), std::move(task)).second) {
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    return ErrorCode::OK;
}

std::vector<OffloadTaskItem> LocalSsdTaskMailbox::SetOffloadingAndTakePending(
    bool enabled) {
    MutexLocker lock(&mutex_);
    enable_offloading_ = enabled;
    std::vector<OffloadTaskItem> tasks;
    tasks.reserve(pending_offloads_.size());
    for (auto& [_, task] : pending_offloads_) {
        tasks.push_back(std::move(task));
    }
    pending_offloads_.clear();
    return tasks;
}

bool LocalSsdTaskMailbox::RemoveOffload(const TenantId& tenant_id,
                                        std::string_view key) {
    MutexLocker lock(&mutex_);
    return pending_offloads_.erase(tenant_id.MakeScopedKey(key)) != 0;
}

ErrorCode LocalSsdTaskMailbox::EnqueuePromotion(PromotionTaskItem task) {
    MutexLocker lock(&mutex_);
    auto index = TenantId(task.tenant_id).MakeScopedKey(task.key);
    if (!pending_promotions_.emplace(std::move(index), std::move(task))
             .second) {
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    return ErrorCode::OK;
}

std::vector<PromotionTaskItem> LocalSsdTaskMailbox::TakePromotions(
    size_t max_items) {
    MutexLocker lock(&mutex_);
    std::vector<PromotionTaskItem> tasks;
    tasks.reserve(std::min(max_items, pending_promotions_.size()));
    while (!pending_promotions_.empty() && tasks.size() < max_items) {
        auto node = pending_promotions_.extract(pending_promotions_.begin());
        tasks.push_back(std::move(node.mapped()));
    }
    return tasks;
}

bool LocalSsdTaskMailbox::RemovePromotion(const TenantId& tenant_id,
                                          std::string_view key) {
    MutexLocker lock(&mutex_);
    return pending_promotions_.erase(tenant_id.MakeScopedKey(key)) != 0;
}

void LocalSsdTaskMailbox::RequestRemoveAll() {
    MutexLocker lock(&mutex_);
    pending_remove_all_ = true;
}

bool LocalSsdTaskMailbox::ConsumeRemoveAll() {
    MutexLocker lock(&mutex_);
    return std::exchange(pending_remove_all_, false);
}

ErrorCode LocalSsdManager::RegisterClient(const UUID& client_id,
                                          bool enable_offloading) {
    std::unique_lock lock(mutex_);
    if (clients_.contains(client_id)) {
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    clients_.emplace(client_id,
                     std::make_shared<ClientRecord>(enable_offloading));
    return ErrorCode::OK;
}

std::optional<LocalSsdManager::ClientAccess> LocalSsdManager::FindClient(
    const UUID& client_id) const {
    std::shared_lock lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        return std::nullopt;
    }
    return std::optional<ClientAccess>(std::in_place, client_id, it->second);
}

std::vector<LocalSsdManager::ClientAccess> LocalSsdManager::SnapshotClients()
    const {
    std::vector<ClientAccess> clients;
    std::shared_lock lock(mutex_);
    clients.reserve(clients_.size());
    for (const auto& [client_id, record] : clients_) {
        clients.emplace_back(client_id, record);
    }
    return clients;
}

void LocalSsdManager::WaitForOperations(const ClientMap& clients) {
    for (const auto& [_, record] : clients) {
        std::unique_lock lifecycle_lock(record->lifecycle_mutex);
    }
}

std::optional<int64_t> LocalSsdManager::UnregisterClient(
    const UUID& client_id) {
    std::shared_ptr<ClientRecord> record;
    {
        std::unique_lock lock(mutex_);
        auto it = clients_.find(client_id);
        if (it == clients_.end()) {
            return std::nullopt;
        }
        record = std::move(it->second);
        clients_.erase(it);
    }

    std::unique_lock lifecycle_lock(record->lifecycle_mutex);
    MutexLocker stats_lock(&record->stats_mutex);
    return record->total_capacity_bytes;
}

tl::expected<LocalSsdManager::CapacityChange, ErrorCode>
LocalSsdManager::ReportCapacity(const UUID& client_id, int64_t bytes) {
    if (bytes < 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto client = FindClient(client_id);
    if (!client) {
        return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto& record = *client->record;
    MutexLocker stats_lock(&record.stats_mutex);
    CapacityChange change{record.total_capacity_bytes, bytes};
    record.total_capacity_bytes = bytes;
    return change;
}

std::optional<LocalSsdManager::Usage> LocalSsdManager::GetUsage(
    const UUID& client_id) const {
    auto client = FindClient(client_id);
    if (!client) {
        return std::nullopt;
    }
    auto& record = *client->record;
    MutexLocker stats_lock(&record.stats_mutex);
    return Usage{record.total_capacity_bytes,
                 record.used_bytes.load(std::memory_order_relaxed)};
}

bool LocalSsdManager::AdjustUsedBytes(const UUID& client_id, int64_t delta) {
    auto client = FindClient(client_id);
    if (!client) {
        return false;
    }
    client->record->used_bytes.fetch_add(delta, std::memory_order_relaxed);
    return true;
}

ErrorCode LocalSsdManager::EnqueueOffload(const UUID& client_id,
                                          OffloadTaskItem task, size_t limit) {
    auto client = FindClient(client_id);
    if (!client) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    return client->record->mailbox.EnqueueOffload(std::move(task), limit);
}

tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
LocalSsdManager::SetOffloadingAndTakePending(const UUID& client_id,
                                             bool enabled) {
    auto client = FindClient(client_id);
    if (!client) {
        return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return client->record->mailbox.SetOffloadingAndTakePending(enabled);
}

bool LocalSsdManager::RemoveOffload(const UUID& client_id,
                                    const TenantId& tenant_id,
                                    std::string_view key) {
    auto client = FindClient(client_id);
    return client && client->record->mailbox.RemoveOffload(tenant_id, key);
}

size_t LocalSsdManager::RemoveOffloadFromAll(const TenantId& tenant_id,
                                             std::string_view key) {
    auto clients = SnapshotClients();
    size_t removed = 0;
    for (auto& client : clients) {
        removed += client.record->mailbox.RemoveOffload(tenant_id, key);
    }
    return removed;
}

bool LocalSsdManager::CancelOffloadsIfAllPending(
    const std::vector<UUID>& client_ids, const TenantId& tenant_id,
    std::string_view key) {
    if (client_ids.empty()) {
        return false;
    }

    std::vector<UUID> sorted_ids = client_ids;
    std::sort(sorted_ids.begin(), sorted_ids.end());
    sorted_ids.erase(std::unique(sorted_ids.begin(), sorted_ids.end()),
                     sorted_ids.end());

    std::vector<ClientAccess> clients;
    clients.reserve(sorted_ids.size());
    for (const auto& client_id : sorted_ids) {
        auto client = FindClient(client_id);
        if (!client) {
            return false;
        }
        clients.push_back(std::move(*client));
    }

    // Hold every mailbox lock before checking any entry. A heartbeat can
    // therefore either drain a mailbox before this operation starts or after
    // it commits, but cannot leave a partially cancelled set of mirrors.
    std::vector<std::unique_lock<Mutex>> mailbox_locks;
    mailbox_locks.reserve(clients.size());
    for (auto& client : clients) {
        mailbox_locks.emplace_back(client.record->mailbox.mutex_);
    }

    const std::string index = tenant_id.MakeScopedKey(key);
    for (const auto& client : clients) {
        if (!client.record->mailbox.pending_offloads_.contains(index)) {
            return false;
        }
    }
    for (auto& client : clients) {
        client.record->mailbox.pending_offloads_.erase(index);
    }
    return true;
}

ErrorCode LocalSsdManager::EnqueuePromotion(const UUID& client_id,
                                            PromotionTaskItem task) {
    auto client = FindClient(client_id);
    if (!client) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    return client->record->mailbox.EnqueuePromotion(std::move(task));
}

tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
LocalSsdManager::TakePromotions(const UUID& client_id, size_t max_items) {
    auto client = FindClient(client_id);
    if (!client) {
        return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return client->record->mailbox.TakePromotions(max_items);
}

bool LocalSsdManager::RemovePromotion(const UUID& client_id,
                                      const TenantId& tenant_id,
                                      std::string_view key) {
    auto client = FindClient(client_id);
    return client && client->record->mailbox.RemovePromotion(tenant_id, key);
}

void LocalSsdManager::RequestRemoveAll() {
    auto clients = SnapshotClients();
    for (auto& client : clients) {
        client.record->mailbox.RequestRemoveAll();
    }
}

void LocalSsdManager::RequestRemoveAll(const std::vector<UUID>& clients) {
    for (const auto& client_id : clients) {
        auto client = FindClient(client_id);
        if (client) {
            client->record->mailbox.RequestRemoveAll();
        }
    }
}

tl::expected<bool, ErrorCode> LocalSsdManager::ConsumeRemoveAll(
    const UUID& client_id) {
    auto client = FindClient(client_id);
    if (!client) {
        return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return client->record->mailbox.ConsumeRemoveAll();
}

LocalSsdPersistedState LocalSsdManager::ExportPersistedState() const {
    auto clients = SnapshotClients();
    LocalSsdPersistedState state;
    for (const auto& client_access : clients) {
        const auto& record = client_access.record;
        LocalSsdPersistedClient client;
        {
            MutexLocker stats_lock(&record->stats_mutex);
            MutexLocker mailbox_lock(&record->mailbox.mutex_);
            client.total_capacity_bytes = record->total_capacity_bytes;
            client.enable_offloading = record->mailbox.enable_offloading_;
            client.pending_offloads.insert(
                record->mailbox.pending_offloads_.begin(),
                record->mailbox.pending_offloads_.end());
        }
        state.emplace(client_access.client_id, std::move(client));
    }
    return state;
}

void LocalSsdManager::RestorePersistedState(LocalSsdPersistedState state) {
    ClientMap clients;
    for (auto& [client_id, persisted] : state) {
        auto record =
            std::make_shared<ClientRecord>(persisted.enable_offloading);
        record->total_capacity_bytes = persisted.total_capacity_bytes;
        record->mailbox.pending_offloads_.insert(
            std::make_move_iterator(persisted.pending_offloads.begin()),
            std::make_move_iterator(persisted.pending_offloads.end()));
        clients.emplace(client_id, std::move(record));
    }
    ClientMap previous;
    {
        std::unique_lock lock(mutex_);
        previous.swap(clients_);
        clients_ = std::move(clients);
    }
    WaitForOperations(previous);
}

void LocalSsdManager::Clear() {
    ClientMap clients;
    {
        std::unique_lock lock(mutex_);
        clients.swap(clients_);
    }
    WaitForOperations(clients);
}

}  // namespace mooncake
