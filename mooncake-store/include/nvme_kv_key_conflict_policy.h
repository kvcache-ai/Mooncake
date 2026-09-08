#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv_connector.h"
#include "nvme_kv_key_codec.h"
#include "nvme_kv_object_layout.h"
#include "types.h"

namespace mooncake {

class NvmeKvKeyConflictPolicy {
   public:
    using PhysicalKey = NvmeKvPhysicalKey;

    enum class ExistingObjectDecision {
        kNotFound,
        kSameObject,
        kDifferentObject,
    };

    struct WritePlan {
        NvmeKvObjectIdentity identity;
        PhysicalKey root_key{};
        bool store_inline = false;
        std::string root_blob;
        std::vector<std::pair<PhysicalKey, std::string_view>> chunk_values;
        std::vector<NvmeKvManifestChunkRecord> manifest_records;
    };

    static bool ValidateResolvedRootPlacement(
        const NvmeKvObjectIdentity& identity,
        const NvmeKvStoredIdentityView& stored_view,
        const NvmeKvPhysicalKey& observed_physical_key);
    static tl::expected<WritePlan, ErrorCode> BuildWritePlan(
        const NvmeKvObjectIdentity& identity, std::string_view payload,
        uint32_t slot, uint32_t max_value_size);
    static tl::expected<ExistingObjectDecision, ErrorCode>
    ResolveExistingObject(NvmeKvConnector& connector,
                          const PhysicalKey& physical_key,
                          std::string_view expected_blob);
};

}  // namespace mooncake
