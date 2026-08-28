#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "local_ssd/persisted_state.h"
#include "mutex.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

class LocalSsdTaskMailbox {
   public:
    explicit LocalSsdTaskMailbox(bool enable_offloading);

    ErrorCode EnqueueOffload(OffloadTaskItem task, size_t limit);
    std::vector<OffloadTaskItem> SetOffloadingAndTakePending(bool enabled);
    bool RemoveOffload(const TenantId& tenant_id, std::string_view key);

    ErrorCode EnqueuePromotion(PromotionTaskItem task);
    std::vector<PromotionTaskItem> TakePromotions(size_t max_items);
    bool RemovePromotion(const TenantId& tenant_id, std::string_view key);
    // Re-mark a queued promotion as most-recently-touched. No-op (returns
    // false) when the key is not queued, e.g. already taken by a heartbeat.
    bool TouchPromotion(const TenantId& tenant_id, std::string_view key);

    void RequestRemoveAll();
    bool ConsumeRemoveAll();

   private:
    mutable Mutex mutex_;
    bool enable_offloading_ GUARDED_BY(mutex_);
    std::unordered_map<std::string, OffloadTaskItem> pending_offloads_
        GUARDED_BY(mutex_);
    // Dedup map + recency order. seq is monotone; enqueue, a duplicate
    // enqueue, and TouchPromotion all re-mark the entry with a fresh seq, so
    // TakePromotions can deliver the most-recently-touched key first instead
    // of an arbitrary (hash-order) one.
    struct PromotionEntry {
        PromotionTaskItem task;
        uint64_t seq;
    };
    std::unordered_map<std::string, PromotionEntry> pending_promotions_
        GUARDED_BY(mutex_);
    uint64_t promotion_seq_ GUARDED_BY(mutex_){0};
    bool pending_remove_all_ GUARDED_BY(mutex_){false};

    friend class LocalSsdManager;
};

class LocalSsdManager {
   public:
    struct CapacityChange {
        int64_t previous_bytes;
        int64_t current_bytes;
    };

    struct Usage {
        int64_t total_capacity_bytes;
        int64_t used_bytes;
    };

    ErrorCode RegisterClient(const UUID& client_id, bool enable_offloading);
    std::optional<int64_t> UnregisterClient(const UUID& client_id);

    tl::expected<CapacityChange, ErrorCode> ReportCapacity(
        const UUID& client_id, int64_t bytes);

    std::optional<Usage> GetUsage(const UUID& client_id) const;
    bool AdjustUsedBytes(const UUID& client_id, int64_t delta);

    ErrorCode EnqueueOffload(const UUID& client_id, OffloadTaskItem task,
                             size_t limit);
    tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    SetOffloadingAndTakePending(const UUID& client_id, bool enabled);
    bool RemoveOffload(const UUID& client_id, const TenantId& tenant_id,
                       std::string_view key);
    size_t RemoveOffloadFromAll(const TenantId& tenant_id,
                                std::string_view key);
    bool CancelOffloadsIfAllPending(const std::vector<UUID>& client_ids,
                                    const TenantId& tenant_id,
                                    std::string_view key);

    ErrorCode EnqueuePromotion(const UUID& client_id, PromotionTaskItem task);
    tl::expected<std::vector<PromotionTaskItem>, ErrorCode> TakePromotions(
        const UUID& client_id, size_t max_items);
    bool RemovePromotion(const UUID& client_id, const TenantId& tenant_id,
                         std::string_view key);
    bool TouchPromotion(const UUID& client_id, const TenantId& tenant_id,
                        std::string_view key);

    void RequestRemoveAll();
    void RequestRemoveAll(const std::vector<UUID>& clients);
    tl::expected<bool, ErrorCode> ConsumeRemoveAll(const UUID& client_id);

    LocalSsdPersistedState ExportPersistedState() const;
    void RestorePersistedState(LocalSsdPersistedState state);
    void Clear();

   private:
    struct ClientRecord {
        explicit ClientRecord(bool enable_offloading)
            : mailbox(enable_offloading) {}

        // Unregistration removes the record from clients_ first, then takes
        // this lock exclusively to wait for operations that already found it.
        mutable std::shared_mutex lifecycle_mutex;
        mutable Mutex stats_mutex;
        int64_t total_capacity_bytes GUARDED_BY(stats_mutex){0};
        std::atomic<int64_t> used_bytes{0};
        LocalSsdTaskMailbox mailbox;
    };

    struct ClientAccess {
        ClientAccess(UUID id, std::shared_ptr<ClientRecord> client)
            : client_id(id),
              record(std::move(client)),
              lifecycle_lock(record->lifecycle_mutex) {}
        ClientAccess(ClientAccess&&) noexcept = default;
        ClientAccess& operator=(ClientAccess&&) noexcept = default;
        ClientAccess(const ClientAccess&) = delete;
        ClientAccess& operator=(const ClientAccess&) = delete;

        UUID client_id;
        std::shared_ptr<ClientRecord> record;
        std::shared_lock<std::shared_mutex> lifecycle_lock;
    };

    using ClientMap = std::unordered_map<UUID, std::shared_ptr<ClientRecord>,
                                         boost::hash<UUID>>;

    std::optional<ClientAccess> FindClient(const UUID& client_id) const;
    std::vector<ClientAccess> SnapshotClients() const;
    static void WaitForOperations(const ClientMap& clients);

    mutable std::shared_mutex mutex_;
    ClientMap clients_;
};

}  // namespace mooncake
