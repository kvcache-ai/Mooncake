#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "weight_management.h"

namespace mooncake {

/**
 * @brief In-memory registry for revision-level weight metadata (RFC #4017 PR1).
 *
 * Thread-safe. Does not mutate Store object maps; MasterService performs any
 * object-existence checks before calling CommitImport.
 */
class WeightMetadataStore {
   public:
    tl::expected<WeightRevisionMetadata, ErrorCode> BeginImport(
        const BeginWeightImportRequest& request);

    tl::expected<WeightRevisionMetadata, ErrorCode> CommitImport(
        const CommitWeightImportRequest& request);

    tl::expected<WeightRevisionMetadata, ErrorCode> Get(
        const WeightRevisionIdentity& identity) const;

    ListWeightRevisionsResponse List(
        const ListWeightRevisionsRequest& request) const;

    tl::expected<WeightRevisionMetadata, ErrorCode> UpdatePolicy(
        const UpdateWeightPolicyRequest& request);

    // Test helper: number of tracked revisions including IMPORTING/DELETED.
    size_t SizeForTesting() const;

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, WeightRevisionMetadata> revisions_;
};

}  // namespace mooncake
