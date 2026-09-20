#pragma once

#include <functional>

#include "ha/oplog/oplog_types.h"
#include "types.h"
#include "object_runtime_state.h"
#include "weight_metadata_store.h"

namespace mooncake {

struct WeightGroupMemberSnapshot {
    std::string key;
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    bool readable{false};
    bool has_memory{false};
    bool has_cold{false};
};

class WeightStoreBackend {
   public:
    using DurableResult = tl::expected<OpLogEntry, ErrorCode>;
    // A stop can report failure before an in-flight successful write completes.
    // The consumer must arbitrate completion before publishing metadata.
    using DurableFinalize = std::function<void(const DurableResult&)>;
    virtual ~WeightStoreBackend() = default;
    virtual bool CanPublishWeightMutations() const = 0;
    virtual PromotionQueueResult PromoteWeightObject(
        const TenantId& tenant_id, const std::string& key) = 0;
    virtual std::vector<std::string> GetGroupMemberKeys(
        const TenantId& tenant_id, const std::string& group_id) const = 0;
    virtual tl::expected<void, ErrorCode> RemoveObject(
        const std::string& key, const TenantId& tenant_id, bool force,
        bool allow_managed_weight) = 0;
    virtual void EvictManagedWeightGroupToCold(
        const WeightRevisionMetadata& revision) = 0;
    virtual bool IsOpLogEnabled() const = 0;
    virtual bool IsTenantSupported(const std::string& tenant_id) const = 0;
    virtual tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalize finalize) = 0;
    virtual WeightMetadataStore::Result<std::vector<WeightGroupMemberSnapshot>>
    SnapshotWeightGroup(const WeightRevisionIdentity& identity,
                        const std::string& payload_group_id) const = 0;
};

class MasterService;

class MasterStoreBackend final : public WeightStoreBackend {
   public:
    explicit MasterStoreBackend(MasterService& master) : master_(master) {}
    bool CanPublishWeightMutations() const override;
    PromotionQueueResult PromoteWeightObject(
        const TenantId& tenant_id, const std::string& key) override;
    std::vector<std::string> GetGroupMemberKeys(
        const TenantId& tenant_id, const std::string& group_id) const override;
    tl::expected<void, ErrorCode> RemoveObject(
        const std::string& key, const TenantId& tenant_id, bool force,
        bool allow_managed_weight) override;
    void EvictManagedWeightGroupToCold(
        const WeightRevisionMetadata& revision) override;
    bool IsOpLogEnabled() const override;
    bool IsTenantSupported(const std::string& tenant_id) const override;
    tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalize finalize) override;
    WeightMetadataStore::Result<std::vector<WeightGroupMemberSnapshot>>
    SnapshotWeightGroup(const WeightRevisionIdentity& identity,
                        const std::string& payload_group_id) const override;

   private:
    MasterService& master_;
};

}  // namespace mooncake
