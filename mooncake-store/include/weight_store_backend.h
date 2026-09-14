#pragma once

#include "types.h"
#include "weight_metadata_store.h"

namespace mooncake {

struct WeightGroupMemberSnapshot {
    std::string key;
    uint64_t size{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    bool readable{false};
};

class WeightStoreBackend {
   public:
    virtual ~WeightStoreBackend() = default;
    virtual bool IsTenantSupported(const std::string& tenant_id) const = 0;
    virtual WeightMetadataStore::Result<std::vector<WeightGroupMemberSnapshot>>
    SnapshotWeightGroup(const WeightRevisionIdentity& identity,
                        const std::string& payload_group_id) const = 0;
};

class MasterService;

class MasterStoreBackend final : public WeightStoreBackend {
   public:
    explicit MasterStoreBackend(MasterService& master) : master_(master) {}
    bool IsTenantSupported(const std::string& tenant_id) const override;
    WeightMetadataStore::Result<std::vector<WeightGroupMemberSnapshot>>
    SnapshotWeightGroup(const WeightRevisionIdentity& identity,
                        const std::string& payload_group_id) const override;

   private:
    MasterService& master_;
};

}  // namespace mooncake
