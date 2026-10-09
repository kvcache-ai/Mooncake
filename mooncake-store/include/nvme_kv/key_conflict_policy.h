#pragma once

#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv/connector.h"
#include "nvme_kv/key_codec.h"
#include "nvme_kv/object_layout.h"
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

    static bool ValidateResolvedRootPlacement(
        const NvmeKvObjectIdentity& identity,
        const NvmeKvStoredIdentityView& stored_view,
        const NvmeKvPhysicalKey& observed_physical_key);
    static tl::expected<ExistingObjectDecision, ErrorCode>
    ResolveExistingObject(NvmeKvConnector& connector,
                          const PhysicalKey& physical_key,
                          std::string_view expected_blob);
};

}  // namespace mooncake
